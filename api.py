from __future__ import annotations

import hmac
import os

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)


def authorized() -> bool:
    auth = (request.headers.get("Authorization") or "").strip()
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    expected = (os.getenv("OPERATOR_SYNC_TOKEN") or "").strip()
    return bool(expected and token and hmac.compare_digest(expected, token))


@app.get("/")
def root():
    return jsonify(ok=True, service="extract-worker", status="online")


@app.get("/healthz")
def healthz():
    return jsonify(ok=True, service="extract-worker")


@app.get("/api/jobs/<job_id>")
def job_status(job_id: str):
    if not authorized():
        return jsonify(ok=False, error="unauthorized"), 401
    base = os.environ["OPERATOR_INTERNAL_URL"].rstrip("/")
    response = requests.get(f"{base}/internal/jobs/{job_id}", headers={"Authorization": f"Bearer {os.environ['EXTRACT_INTERNAL_TOKEN']}"}, timeout=30)
    return (response.text, response.status_code, {"Content-Type": "application/json"})
