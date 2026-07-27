# -*- coding: utf-8 -*-
"""Plus 试用提链后台队列。"""
from __future__ import annotations

import json
import logging
import os
import base64
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:
    from curl_cffi import requests as curl_requests
except Exception:  # WebUI 环境未装 curl_cffi 时使用标准库兜底
    curl_requests = None

from config import extract_link as cfg
from core import db

logger = logging.getLogger(__name__)


def _runtime_setting(name: str, default=None):
    """
    提链配置多数保存在 .env。服务模块会在 WebUI 启动时较早 import，
    因此每次实际读取时都重新加载 .env，避免“页面已保存但当前进程仍读到空值”。
    """
    try:
        from config.env_loader import load_env
        load_env(override=True)
    except Exception:
        pass
    raw = os.getenv(name)
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip()
    return getattr(cfg, name, default)


def _int_setting(name: str, default: int, lower: int, upper: int) -> int:
    try:
        value = int(_runtime_setting(name, default) or default)
    except (TypeError, ValueError):
        value = default
    return max(lower, min(upper, value))


def _float_setting(name: str, default: float, lower: float, upper: float) -> float:
    try:
        value = float(_runtime_setting(name, default) or default)
    except (TypeError, ValueError):
        value = default
    return max(lower, min(upper, value))


def _bool_setting(name: str, default: bool = False) -> bool:
    value = _runtime_setting(name, default)
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "on", "y"}


def _link_type(value: str | None = None) -> str:
    t = str(value or _runtime_setting("EXTRACT_LINK_TYPE", "pix") or "pix").strip().lower()
    if t not in {"pix", "upi", "ideal", "kakao"}:
        raise ValueError("提链类型无效，仅支持 pix / upi / ideal / kakao")
    return t


def _api_base() -> str:
    base = str(_runtime_setting("EXTRACT_LINK_API_BASE", "") or "").strip().rstrip("/")
    if not base:
        raise ValueError("EXTRACT_LINK_API_BASE 为空")
    return base


def _provider() -> str:
    provider = str(_runtime_setting("EXTRACT_LINK_PROVIDER", "auto") or "auto").strip().lower()
    if provider == "auto":
        return "cccy" if "pay.cccy.me" in _api_base().lower() else "legacy"
    if provider not in {"legacy", "cccy"}:
        raise ValueError("EXTRACT_LINK_PROVIDER 仅支持 auto / legacy / cccy")
    return provider


_CDK_ROTATION_LOCK = threading.Lock()
_CDK_ROTATION_INDEX = 0


def _cdk_candidates(value=None) -> list[str]:
    """解析 CDK 池；支持多行、英文/中文逗号和分号分隔。"""
    raw = value if value is not None else _runtime_setting("EXTRACT_LINK_CDK", "")
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item or "").strip() for item in raw]
    else:
        parts = [item.strip() for item in re.split(r"[\r\n,;，；]+", str(raw or ""))]
    # 去重但保持配置顺序，避免同一个 CDK 被重复分配。
    return list(dict.fromkeys(item for item in parts if item))


def _cdk(value=None) -> str:
    candidates = _cdk_candidates(value)
    if not candidates:
        raise ValueError("EXTRACT_LINK_CDK/CDK 为空")
    return candidates[0]


def _ordered_cdks(value=None) -> list[str]:
    """按轮询起点返回本次任务的候选 CDK 顺序。"""
    global _CDK_ROTATION_INDEX
    candidates = _cdk_candidates(value)
    if not candidates:
        raise ValueError("EXTRACT_LINK_CDK/CDK 为空")
    with _CDK_ROTATION_LOCK:
        start = _CDK_ROTATION_INDEX % len(candidates)
        _CDK_ROTATION_INDEX = (_CDK_ROTATION_INDEX + 1) % len(candidates)
    return candidates[start:] + candidates[:start]


def _next_cdk(value=None) -> str:
    """从配置的 CDK 池中线程安全地轮询取一个，供并发/批量提链使用。"""
    return _ordered_cdks(value)[0]


def _remove_cdk(candidate: str) -> bool:
    """Persistently remove an exhausted or invalid CDK from the configured pool."""
    candidate = str(candidate or "").strip()
    if not candidate:
        return False
    with _CDK_ROTATION_LOCK:
        current = _cdk_candidates()
        remaining = [item for item in current if item != candidate]
        if len(remaining) == len(current):
            return False
        try:
            from config.env_loader import write_env_values, load_env
            write_env_values({"EXTRACT_LINK_CDK": "\n".join(remaining)})
            load_env(override=True)
        except Exception:
            logger.exception("Failed to auto-remove exhausted CDK: %s", candidate)
            return False
        logger.warning("CDK exhausted or invalid; removed automatically: %s", candidate)
        return True


def _cdk_has_enough_points(info: dict) -> bool:
    """根据 verify-cdk 响应判断点数是否足够支付一次提链。"""
    if info.get("valid") is False:
        return False
    remaining = info.get("points", info.get("cdk_remaining", info.get("count")))
    cost = info.get("cost_points", 1)
    if remaining is None:
        return True
    try:
        return float(remaining) >= max(1.0, float(cost))
    except (TypeError, ValueError):
        return True


def _is_cdk_unusable_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    markers = (
        "cdk 无效", "cdk无效", "invalid cdk", "invalid_cdk",
        "点数不足", "次数不足", "余额不足", "积分不足",
        "insufficient point", "not enough point", "no point",
        "exhausted", "已用完", "已耗尽",
    )
    return any(marker in text for marker in markers)


_WORKERS = _int_setting("EXTRACT_LINK_WORKERS", 3, 1, 16)
_QUEUE_LIMIT = _int_setting("EXTRACT_LINK_QUEUE_LIMIT", 500, _WORKERS, 5000)
_EXECUTOR = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="extract-link")
_QUEUE_SLOTS = threading.BoundedSemaphore(_QUEUE_LIMIT)


def queue_settings() -> dict:
    return {"workers": _WORKERS, "queue_limit": _QUEUE_LIMIT}


def _session():
    if curl_requests is None:
        return None
    return curl_requests.Session()


def _json_response(resp) -> dict:
    try:
        payload = resp.json()
    except Exception:
        text = getattr(resp, "text", "") or ""
        payload = {"error": text[:500]}
    return payload if isinstance(payload, dict) else {}


def _cccy_key() -> bytes:
    encoded = str(_runtime_setting("EXTRACT_LINK_CCCY_AES_KEY", "") or "").strip()
    if not encoded:
        raise ValueError("EXTRACT_LINK_CCCY_AES_KEY 为空")
    try:
        key = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("EXTRACT_LINK_CCCY_AES_KEY 不是有效 Base64") from exc
    if len(key) not in {16, 24, 32}:
        raise ValueError("EXTRACT_LINK_CCCY_AES_KEY 解码后长度必须为 16/24/32 字节")
    return key


def _cccy_encrypt(payload: dict, *, timestamp: int | None = None) -> dict:
    iv = os.urandom(12)
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    encrypted = AESGCM(_cccy_key()).encrypt(iv, raw, None)
    return {
        "v": base64.b64encode(iv).decode("ascii"),
        "d": base64.b64encode(encrypted).decode("ascii"),
        "t": int(timestamp or time.time()),
    }


def _cccy_decode(payload: dict) -> dict:
    """解密 CCCY 的 {v,d} 响应；明文 JSON 原样返回。"""
    if not isinstance(payload, dict):
        return {}
    if not payload.get("v") or not payload.get("d"):
        return payload
    try:
        iv = base64.b64decode(str(payload["v"]), validate=True)
        encrypted = base64.b64decode(str(payload["d"]), validate=True)
        raw = AESGCM(_cccy_key()).decrypt(iv, encrypted, None)
        decoded = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"CCCY 响应解密失败: {type(exc).__name__}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("CCCY 解密响应不是 JSON object")
    return decoded


def _cccy_headers(*, timestamp: int | None = None) -> dict:
    headers = {
        "Accept": "application/json",
        "Origin": _api_base(),
        "Referer": f"{_api_base()}/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
        ),
    }
    if timestamp is not None:
        headers["X-Timestamp"] = str(int(timestamp))
    return headers


def _cccy_region(link_type: str) -> str:
    link = _link_type(link_type)
    key = {
        "upi": "EXTRACT_LINK_CCCY_UPI_REGION",
        "ideal": "EXTRACT_LINK_CCCY_IDEAL_REGION",
        "kakao": "EXTRACT_LINK_CCCY_KAKAO_REGION",
    }.get(link, "EXTRACT_LINK_CCCY_PIX_REGION")
    fallback = {"upi": "IN", "ideal": "NL", "kakao": "KR"}.get(link, "BR")
    return str(_runtime_setting(key, fallback) or fallback).strip().upper()


def _cccy_start_payload(*, token: str, link_type: str, cdk: str) -> dict:
    link = _link_type(link_type)
    region = _cccy_region(link)
    promotion_region = str(
        _runtime_setting("EXTRACT_LINK_CCCY_PROMOTION_REGION", "VN") or "VN"
    ).strip().upper()
    return {
        "accessToken": token,
        "cdk": _cdk(cdk),
        "link_type": link,
        "proxy": str(_runtime_setting("EXTRACT_LINK_CCCY_PROXY", "") or "").strip(),
        "proxy_chain_strategy": str(
            _runtime_setting("EXTRACT_LINK_CCCY_PROXY_CHAIN_STRATEGY", "") or ""
        ).strip(),
        "diagnostic_enabled": _bool_setting("EXTRACT_LINK_CCCY_DIAGNOSTIC", False),
        "approve_proxy_region": region,
        "checkout_proxy_region": region,
        "promotion_proxy_region": promotion_region,
        "provider_proxy_region": region,
        "billing_country": region,
        "checkout_ui_mode": str(
            _runtime_setting("EXTRACT_LINK_CCCY_CHECKOUT_UI_MODE", "custom") or "custom"
        ).strip(),
        "payment_locale": str(
            _runtime_setting("EXTRACT_LINK_CCCY_PAYMENT_LOCALE", "auto") or "auto"
        ).strip(),
        "client_fingerprint": str(
            _runtime_setting("EXTRACT_LINK_CCCY_CLIENT_FINGERPRINT", "apple-safari")
            or "apple-safari"
        ).strip(),
    }


def _query_cdk_legacy(*, cdk: str | None = None) -> dict:
    base = _api_base()
    code = _cdk(cdk)
    timeout = _int_setting("EXTRACT_LINK_REQUEST_TIMEOUT", 30, 5, 300)
    s = _session()
    try:
        if s is None:
            req = Request(f"{base}/api/cdk?{urlencode({'code': code})}", headers={"Accept": "application/json"})
            with urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace") or "{}")
            return payload if isinstance(payload, dict) else {}
        resp = s.get(f"{base}/api/cdk?{urlencode({'code': code})}", timeout=timeout)
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": (resp.text or "")[:300]}
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(payload.get("error") or f"HTTP {resp.status_code}")
        return payload if isinstance(payload, dict) else {}
    finally:
        try:
            s.close()
        except Exception:
            pass


def _query_cdk_cccy(*, cdk: str | None = None) -> dict:
    base = _api_base()
    timeout = _int_setting("EXTRACT_LINK_REQUEST_TIMEOUT", 30, 5, 300)
    payload = {"cdk": _cdk(cdk)}
    s = _session()
    try:
        if s is None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            req = Request(
                f"{base}/api/verify-cdk",
                data=body,
                headers={**_cccy_headers(), "Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace") or "{}")
        else:
            resp = s.post(
                f"{base}/api/verify-cdk",
                json=payload,
                headers=_cccy_headers(),
                timeout=timeout,
                impersonate="chrome",
            )
            data = _json_response(resp)
            if resp.status_code < 200 or resp.status_code >= 300:
                raise RuntimeError(_extract_error_message(data) or f"HTTP {resp.status_code}")
        if not isinstance(data, dict):
            return {}
        # 统一给现有 WebUI/数据库提供 cdk_remaining，同时保留 CCCY 原字段。
        if data.get("points") is not None and data.get("cdk_remaining") is None:
            data["cdk_remaining"] = data.get("points")
        if data.get("valid") is False:
            raise RuntimeError(_extract_error_message(data) or "CCCY CDK 无效")
        return data
    finally:
        try:
            s.close()
        except Exception:
            pass


def query_cdk(*, cdk: str | None = None) -> dict:
    if _provider() == "cccy":
        return _query_cdk_cccy(cdk=cdk)
    return _query_cdk_legacy(cdk=cdk)


def _create_extract_job_legacy(*, token: str, link_type: str, cdk: str) -> dict:
    base = _api_base()
    timeout = _int_setting("EXTRACT_LINK_REQUEST_TIMEOUT", 30, 5, 300)
    payload = {"link_type": _link_type(link_type), "cdk": _cdk(cdk), "token": token}
    s = _session()
    try:
        if s is None:
            body = json.dumps(payload).encode("utf-8")
            req = Request(
                f"{base}/api/extract",
                data=body,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace") or "{}")
            if not isinstance(data, dict) or not data.get("job_id"):
                raise RuntimeError(f"提链服务未返回 job_id: {data}")
            return data
        resp = s.post(f"{base}/api/extract", json=payload, timeout=timeout)
        try:
            data = resp.json()
        except Exception:
            data = {"error": (resp.text or "")[:300]}
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(data.get("error") or f"HTTP {resp.status_code}")
        if not isinstance(data, dict) or not data.get("job_id"):
            raise RuntimeError(f"提链服务未返回 job_id: {data}")
        return data
    finally:
        try:
            s.close()
        except Exception:
            pass


def _create_extract_job_cccy(*, token: str, link_type: str, cdk: str) -> dict:
    base = _api_base()
    timeout = _int_setting("EXTRACT_LINK_REQUEST_TIMEOUT", 30, 5, 300)
    payload = _cccy_encrypt(_cccy_start_payload(token=token, link_type=link_type, cdk=cdk))
    s = _session()
    try:
        if s is None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            req = Request(
                f"{base}/api/long-link/start",
                data=body,
                headers={**_cccy_headers(), "Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace") or "{}")
        else:
            resp = s.post(
                f"{base}/api/long-link/start",
                json=payload,
                headers=_cccy_headers(),
                timeout=timeout,
                impersonate="chrome",
            )
            data = _json_response(resp)
            # CCCY 的错误响应也可能使用 AES-GCM 包装；先尝试解密，
            # 这样上层才能识别“点数不足”并自动切换下一个 CDK。
            try:
                decoded = _cccy_decode(data)
            except Exception:
                decoded = data
            if resp.status_code < 200 or resp.status_code >= 300:
                raise RuntimeError(_extract_error_message(decoded) or f"HTTP {resp.status_code}")
            data = decoded
        if s is None:
            data = _cccy_decode(data)
        if not data.get("job_id"):
            raise RuntimeError(f"CCCY 提链服务未返回 job_id: {data}")
        return data
    finally:
        try:
            s.close()
        except Exception:
            pass


def _create_extract_job(*, token: str, link_type: str, cdk: str) -> dict:
    if _provider() == "cccy":
        return _create_extract_job_cccy(token=token, link_type=link_type, cdk=cdk)
    return _create_extract_job_legacy(token=token, link_type=link_type, cdk=cdk)


def _iter_sse_events_legacy(*, job_id: str, cdk: str):
    base = _api_base()
    timeout = _int_setting("EXTRACT_LINK_EVENT_TIMEOUT", 600, 30, 3600)
    url = f"{base}/api/jobs/{quote(job_id, safe='')}/events?{urlencode({'cdk': _cdk(cdk)})}"
    s = _session()
    try:
        if s is None:
            req = Request(url, headers={"Accept": "text/event-stream"})
            with urlopen(req, timeout=timeout) as resp:
                event = "message"
                data_lines: list[str] = []
                for raw in resp:
                    line = raw.decode("utf-8", "replace").rstrip("\r\n")
                    if line == "":
                        if data_lines:
                            text = "\n".join(data_lines)
                            try:
                                data = json.loads(text)
                            except Exception:
                                data = {"raw": text}
                            yield event, data
                        event = "message"
                        data_lines = []
                        continue
                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event = line.split(":", 1)[1].strip() or "message"
                    elif line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
                if data_lines:
                    text = "\n".join(data_lines)
                    try:
                        data = json.loads(text)
                    except Exception:
                        data = {"raw": text}
                    yield event, data
            return
        resp = s.get(url, timeout=timeout, stream=True)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"监听提链事件失败 HTTP {resp.status_code}: {(resp.text or '')[:300]}")
        event = "message"
        data_lines: list[str] = []
        for raw in resp.iter_lines():
            if raw is None:
                continue
            if isinstance(raw, bytes):
                line = raw.decode("utf-8", "replace")
            else:
                line = str(raw)
            line = line.rstrip("\r")
            if line == "":
                if data_lines:
                    text = "\n".join(data_lines)
                    try:
                        data = json.loads(text)
                    except Exception:
                        data = {"raw": text}
                    yield event, data
                event = "message"
                data_lines = []
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event = line.split(":", 1)[1].strip() or "message"
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
        if data_lines:
            text = "\n".join(data_lines)
            try:
                data = json.loads(text)
            except Exception:
                data = {"raw": text}
            yield event, data
    finally:
        try:
            s.close()
        except Exception:
            pass


def _iter_cccy_events(*, job_id: str, cdk: str):
    del cdk  # CCCY 轮询使用 job_id + X-Timestamp；CDK 已在创建任务时提交。
    base = _api_base()
    event_timeout = _int_setting("EXTRACT_LINK_EVENT_TIMEOUT", 600, 30, 3600)
    request_timeout = _int_setting("EXTRACT_LINK_REQUEST_TIMEOUT", 30, 5, 300)
    interval = _float_setting("EXTRACT_LINK_CCCY_POLL_INTERVAL", 1.0, 0.3, 10.0)
    url = f"{base}/api/long-link/jobs/{quote(job_id, safe='')}"
    deadline = time.monotonic() + event_timeout
    seen_steps = 0
    s = _session()
    try:
        while time.monotonic() < deadline:
            timestamp = int(time.time())
            if s is None:
                req = Request(url, headers=_cccy_headers(timestamp=timestamp))
                with urlopen(req, timeout=request_timeout) as resp:
                    encrypted = json.loads(resp.read().decode("utf-8", "replace") or "{}")
            else:
                resp = s.get(
                    url,
                    headers=_cccy_headers(timestamp=timestamp),
                    timeout=request_timeout,
                    impersonate="chrome",
                )
                encrypted = _json_response(resp)
                if resp.status_code < 200 or resp.status_code >= 300:
                    raise RuntimeError(_extract_error_message(encrypted) or f"HTTP {resp.status_code}")
            data = _cccy_decode(encrypted)
            steps = data.get("steps") if isinstance(data.get("steps"), list) else []
            for step in steps[seen_steps:]:
                if not isinstance(step, dict):
                    continue
                name = str(step.get("name") or "").strip()
                detail = str(step.get("detail") or "").strip()
                message = f"{name}: {detail}".strip(": ")
                yield "log", {"message": message, "step": step}
            seen_steps = max(seen_steps, len(steps))

            status = str(data.get("status") or "").strip().lower()
            if status in {"done", "success", "completed"}:
                result = data.get("result")
                if not isinstance(result, dict) or not result.get("ok", True):
                    raise RuntimeError(_extract_error_message(data) or "CCCY 任务完成但没有返回 result")
                yield "result", {"result": result, "response": data}
                return
            if status in {"error", "failed", "failure", "cancelled", "canceled"}:
                yield "error", data
                return
            time.sleep(interval)
        raise TimeoutError(f"CCCY 提链任务轮询超时（{event_timeout}s）")
    finally:
        try:
            s.close()
        except Exception:
            pass


def _iter_extract_events(*, job_id: str, cdk: str):
    if _provider() == "cccy":
        yield from _iter_cccy_events(job_id=job_id, cdk=cdk)
        return
    yield from _iter_sse_events_legacy(job_id=job_id, cdk=cdk)


def _extract_error_message(data) -> str:
    """尽量从提链服务返回的任意错误结构中提取用户可读原因。"""
    if data is None:
        return ""
    if isinstance(data, str):
        return data.strip()
    if not isinstance(data, dict):
        return str(data)
    err = data.get("error")
    if isinstance(err, dict):
        for key in ("message", "detail", "reason", "error", "msg", "description"):
            value = err.get(key)
            if value:
                return str(value).strip()
        return json.dumps(err, ensure_ascii=False)[:500]
    if err:
        return str(err).strip()
    for key in ("message", "detail", "reason", "msg", "description", "raw"):
        value = data.get(key)
        if value:
            return str(value).strip()
    return json.dumps(data, ensure_ascii=False)[:500]


def _format_failure_reason(exc: Exception, logs: list[str] | None = None, last_event: dict | None = None) -> str:
    reason = f"{type(exc).__name__}: {str(exc)}".strip()
    if (not str(exc).strip()) and logs:
        reason = str(logs[-1])
    if last_event and "提链事件流结束但未返回 result" in reason:
        extracted = _extract_error_message(last_event.get("data"))
        if extracted:
            reason = f"提链事件流结束但未返回 result；最后事件 {last_event.get('event')}: {extracted}"
    return reason[:500]


def _run_extract(*, account_id: int, email: str, access_token: str, link_type: str, cdks: list[str], trigger: str) -> dict:
    logs: list[str] = []
    last_event = None
    try:
        if not db.mark_account_extract_running(account_id):
            return {"ok": False, "error": "账号已删除或提链状态已被重置"}

        # 依次检查本次任务的候选 CDK。点数不足/无效时自动切换下一个；
        # 网络或服务端异常不误判为 CDK 耗尽，避免重复创建同一提链任务。
        job = None
        cdk = ""
        rejected: list[str] = []
        for candidate in cdks:
            try:
                info = query_cdk(cdk=candidate)
                if not _cdk_has_enough_points(info):
                    remaining = info.get("points", info.get("cdk_remaining", info.get("count", 0)))
                    cost = info.get("cost_points", 1)
                    rejected.append(f"{candidate}(剩余{remaining},需要{cost})")
                    _remove_cdk(candidate)
                    continue
                job = _create_extract_job(token=access_token, link_type=link_type, cdk=candidate)
                cdk = candidate
                break
            except Exception as exc:
                if _is_cdk_unusable_error(exc):
                    rejected.append(f"{candidate}({str(exc)[:120]})")
                    _remove_cdk(candidate)
                    continue
                raise
        if job is None:
            detail = "；".join(rejected) if rejected else "没有可用 CDK"
            raise RuntimeError(f"所有 CDK 点数均不足或无效：{detail}")

        job_id = str(job.get("job_id") or "")
        db.update_account_extract(account_id, {
            "ok": False,
            "status": "running",
            "job_id": job_id,
            "link_type": link_type,
            "message": "提链任务已创建，等待结果",
            "cdk_remaining": job.get("cdk_remaining"),
        })
        for event, data in _iter_extract_events(job_id=job_id, cdk=cdk):
            last_event = {"event": event, "data": data}
            if event == "log":
                msg = str((data or {}).get("message") or "")[:300]
                if msg:
                    logs.append(msg)
                    db.update_account_extract(account_id, {
                        "ok": False,
                        "status": "running",
                        "job_id": job_id,
                        "link_type": link_type,
                        "message": msg,
                    })
            elif event == "result":
                result = (data or {}).get("result") if isinstance(data, dict) else None
                if not isinstance(result, dict):
                    result = {}
                final = {"ok": True, "status": "success", "job_id": job_id, "link_type": link_type, "result": result, "logs": logs}
                db.update_account_extract(account_id, final)
                logger.info("[提链] 成功: %s type=%s job=%s", email, link_type, job_id)
                return final
            elif event == "error":
                msg = _extract_error_message(data)
                raise RuntimeError(msg or "提链任务失败")
            elif event == "done":
                break
        raise RuntimeError(f"提链事件流结束但未返回 result: {last_event}")
    except Exception as exc:
        reason = _format_failure_reason(exc, logs=logs, last_event=last_event)
        result = {
            "ok": False,
            "status": "failed",
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "error": reason,
            "message": reason,
        }
        try:
            db.update_account_extract(account_id, result)
        except Exception:
            logger.exception("[提链] 写入失败状态异常: account_id=%s", account_id)
        logger.exception("[提链] 失败: %s", email)
        return result
    finally:
        _QUEUE_SLOTS.release()


def enqueue_account_extract(*, account_id: int, email: str, access_token: str, trigger: str = "manual", link_type: str | None = None, cdk=None) -> dict:
    if not _QUEUE_SLOTS.acquire(blocking=False):
        return {"accepted": False, "busy": False, "error": "提链队列已满"}
    try:
        lt = _link_type(link_type)
        # 每个任务只在入队时选择一次 CDK，创建任务和后续事件监听始终
        # 使用同一个值。批量/并发任务会按配置顺序轮询分配。
        codes = _ordered_cdks(cdk)
        if not db.claim_account_extract(account_id, trigger=trigger, link_type=lt):
            _QUEUE_SLOTS.release()
            return {"accepted": False, "busy": True, "error": "该账号正在提链中"}
        fut = _EXECUTOR.submit(_run_extract, account_id=account_id, email=email, access_token=access_token, link_type=lt, cdks=codes, trigger=trigger)
        return {"accepted": True, "busy": False, "future": fut, "link_type": lt}
    except Exception:
        _QUEUE_SLOTS.release()
        raise


def run_external_extract(*, email: str, access_token: str, link_type: str = "pix", cdk=None) -> dict:
    """Run extraction for an account that is not stored in the local account DB.

    This is used by the standalone operator server.  The account/token is held
    by that server's storage layer; only the link-provider protocol is shared
    with the registration application.
    """
    logs: list[str] = []
    last_event = None
    selected_cdk = ""
    try:
        codes = _ordered_cdks(cdk)
        job = None
        for candidate in codes:
            try:
                info = query_cdk(cdk=candidate)
                if not _cdk_has_enough_points(info):
                    _remove_cdk(candidate)
                    continue
                job = _create_extract_job(token=access_token, link_type=_link_type(link_type), cdk=candidate)
                selected_cdk = candidate
                break
            except Exception as exc:
                if _is_cdk_unusable_error(exc):
                    _remove_cdk(candidate)
                    continue
                raise
        if not job:
            raise RuntimeError("没有可用的提链 CDK")
        job_id = str(job.get("job_id") or "")
        for event, data in _iter_extract_events(job_id=job_id, cdk=selected_cdk):
            last_event = {"event": event, "data": data}
            if event == "log":
                message = str((data or {}).get("message") or "")[:500]
                if message:
                    logs.append(message)
            elif event == "result":
                result = (data or {}).get("result") if isinstance(data, dict) else None
                return {"ok": True, "status": "success", "job_id": job_id,
                        "link_type": _link_type(link_type), "result": result or {}, "logs": logs}
            elif event == "error":
                raise RuntimeError(_extract_error_message(data) or "提链任务失败")
            elif event == "done":
                break
        raise RuntimeError(f"提链事件结束但没有 result: {last_event}")
    except Exception as exc:
        return {"ok": False, "status": "failed", "error": _format_failure_reason(exc, logs, last_event),
                "message": _format_failure_reason(exc, logs, last_event), "logs": logs}
