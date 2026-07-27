from __future__ import annotations

import os
import time

import requests
from dotenv import load_dotenv

from core.extract_link_service import run_external_extract


def main() -> None:
    load_dotenv()
    base = os.environ["OPERATOR_INTERNAL_URL"].rstrip("/")
    headers = {"Authorization": f"Bearer {os.environ['EXTRACT_INTERNAL_TOKEN']}"}
    while True:
        try:
            response = requests.post(f"{base}/internal/claim", headers=headers, timeout=30)
            payload = response.json() if response.content else {}
            job = payload.get("job") if response.ok else None
            if job:
                result = run_external_extract(email=job.get("email", ""), access_token=job["access_token"], link_type=job.get("channel", "pix"))
                requests.post(f"{base}/internal/complete", headers=headers, json={"job_id": job["job_id"], "result": result}, timeout=30)
        except Exception:
            time.sleep(5)
        time.sleep(float(os.getenv("OPERATOR_WORKER_INTERVAL", "2")))


if __name__ == "__main__":
    main()
