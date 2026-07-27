from __future__ import annotations

import hmac
import os

from flask import Flask, jsonify, request

from storage import Store

app = Flask(__name__)


def authorized() -> bool:
    auth = (request.headers.get("Authorization") or "").strip()
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    expected = (os.getenv("OPERATOR_SYNC_TOKEN") or "").strip()
    return bool(expected and token and hmac.compare_digest(expected, token))


@app.get("/healthz")
def healthz():
    return jsonify(ok=True, service="extract-worker")


@app.get("/api/jobs/<job_id>")
def job_status(job_id: str):
    if not authorized():
        return jsonify(ok=False, error="unauthorized"), 401
    job = Store().get(f"pending/{job_id}.json") or Store().get(f"results/{job_id}.json")
    if not job:
        return jsonify(ok=False, error="job not found"), 404
    job.pop("access_token", None)
    return jsonify(ok=True, job=job)
