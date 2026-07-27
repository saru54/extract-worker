from __future__ import annotations

import os
import time

from dotenv import load_dotenv

from core.extract_link_service import run_external_extract
from storage import Store


def main() -> None:
    load_dotenv()
    store = Store()
    while True:
        for key in store.list("pending/"):
            job = store.get(key)
            if not job or job.get("status") != "extracting":
                continue
            started = float(job.get("worker_started_at") or 0)
            if started and time.time() - started < float(os.getenv("OPERATOR_JOB_LEASE", "900")):
                continue
            job["worker_started_at"] = time.time(); store.put(key, job)
            result = run_external_extract(email=job.get("email", ""), access_token=job["access_token"], link_type=job.get("channel", "pix"))
            job.pop("access_token", None)
            job.update(result); job["status"] = result.get("status", "failed"); job["completed_at"] = time.time(); store.put(key, job)
            if isinstance(result.get("result"), dict):
                job.update(result["result"])
                store.put(key, job)
            store.event({"event": "extract", "job_id": job["job_id"], "status": job["status"]})
        time.sleep(float(os.getenv("OPERATOR_WORKER_INTERVAL", "2")))


if __name__ == "__main__":
    main()
