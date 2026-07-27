# -*- coding: utf-8 -*-
"""Plus 试用提链服务配置。"""
from config.env_loader import apply_env_overrides

# 提链服务地址
EXTRACT_LINK_API_BASE: str = ""

# 提链服务协议：
#   auto   = pay.cccy.me 自动使用 CCCY 协议，其他地址沿用原 SSE 协议
#   legacy = /api/cdk + /api/extract + SSE /api/jobs/{id}/events
#   cccy   = /api/verify-cdk + /api/long-link/start + 轮询 /api/long-link/jobs/{id}
EXTRACT_LINK_PROVIDER: str = "auto"

# 提链 CDK；可在 .env/WebUI 中每行配置一个，任务会轮询使用。
EXTRACT_LINK_CDK: str = ""

# 提链类型：pix / upi / ideal / kakao
EXTRACT_LINK_TYPE: str = "pix"

# 后台提链并发与超时
EXTRACT_LINK_WORKERS: int = 3
EXTRACT_LINK_QUEUE_LIMIT: int = 500
EXTRACT_LINK_REQUEST_TIMEOUT: int = 30
# 提链任务总等待时间。CCCY 在繁忙时排队可能超过 3 分钟，
# 默认等待 10 分钟，仍可通过 WebUI 或 .env 调整。
EXTRACT_LINK_EVENT_TIMEOUT: int = 600

# pay.cccy.me / CCCY 参数。AES key 来自该服务公开页面中的 WebCrypto 配置；
# 可在服务端轮换后通过 WebUI/.env 更新，不需要改代码。
EXTRACT_LINK_CCCY_AES_KEY: str = "e7WPnFKSYz9gbRPiErcCt3iYMMgyv4R9xdkxiwAAcXk="
EXTRACT_LINK_CCCY_POLL_INTERVAL: float = 1.0
EXTRACT_LINK_CCCY_DIAGNOSTIC: bool = False
EXTRACT_LINK_CCCY_CHECKOUT_UI_MODE: str = "custom"
EXTRACT_LINK_CCCY_PAYMENT_LOCALE: str = "auto"
EXTRACT_LINK_CCCY_CLIENT_FINGERPRINT: str = "apple-safari"
EXTRACT_LINK_CCCY_PROXY: str = ""
EXTRACT_LINK_CCCY_PROXY_CHAIN_STRATEGY: str = ""
EXTRACT_LINK_CCCY_PIX_REGION: str = "BR"
EXTRACT_LINK_CCCY_UPI_REGION: str = "IN"
EXTRACT_LINK_CCCY_IDEAL_REGION: str = "NL"
EXTRACT_LINK_CCCY_KAKAO_REGION: str = "KR"
EXTRACT_LINK_CCCY_PROMOTION_REGION: str = "VN"

apply_env_overrides(globals(), {
    'EXTRACT_LINK_PROVIDER': 'str',
    'EXTRACT_LINK_API_BASE': 'str',
    'EXTRACT_LINK_CDK': 'str',
    'EXTRACT_LINK_TYPE': 'str',
    'EXTRACT_LINK_WORKERS': 'int',
    'EXTRACT_LINK_QUEUE_LIMIT': 'int',
    'EXTRACT_LINK_REQUEST_TIMEOUT': 'int',
    'EXTRACT_LINK_EVENT_TIMEOUT': 'int',
    'EXTRACT_LINK_CCCY_AES_KEY': 'str',
    'EXTRACT_LINK_CCCY_POLL_INTERVAL': 'float',
    'EXTRACT_LINK_CCCY_DIAGNOSTIC': 'bool',
    'EXTRACT_LINK_CCCY_CHECKOUT_UI_MODE': 'str',
    'EXTRACT_LINK_CCCY_PAYMENT_LOCALE': 'str',
    'EXTRACT_LINK_CCCY_CLIENT_FINGERPRINT': 'str',
    'EXTRACT_LINK_CCCY_PROXY': 'str',
    'EXTRACT_LINK_CCCY_PROXY_CHAIN_STRATEGY': 'str',
    'EXTRACT_LINK_CCCY_PIX_REGION': 'str',
    'EXTRACT_LINK_CCCY_UPI_REGION': 'str',
    'EXTRACT_LINK_CCCY_IDEAL_REGION': 'str',
    'EXTRACT_LINK_CCCY_KAKAO_REGION': 'str',
    'EXTRACT_LINK_CCCY_PROMOTION_REGION': 'str',
})
