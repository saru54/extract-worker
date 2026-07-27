from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

from cryptography.fernet import Fernet


class Store:
    """Encrypted object storage backed by Cloudflare R2's S3 API."""

    def __init__(self):
        self.bucket = os.environ["R2_BUCKET"]
        self.client = self._client()
        self.fernet = Fernet(os.environ["OPERATOR_DATA_KEY"].encode())

    def _client(self):
        import boto3
        return boto3.client(
            "s3", endpoint_url=os.environ["R2_ENDPOINT"].rstrip("/"),
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto")

    def put(self, key: str, value: dict) -> None:
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        self.client.put_object(Bucket=self.bucket, Key=key, Body=self.fernet.encrypt(raw),
                               ContentType="application/octet-stream")

    def get(self, key: str) -> dict | None:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        return json.loads(self.fernet.decrypt(obj["Body"].read()))

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def list(self, prefix: str) -> list[str]:
        out, token = [], None
        while True:
            args = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                args["ContinuationToken"] = token
            page = self.client.list_objects_v2(**args)
            out.extend(x["Key"] for x in page.get("Contents", []))
            if not page.get("IsTruncated"):
                return out
            token = page.get("NextContinuationToken")

    def event(self, event: dict) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        self.put(f"events/{stamp}/{uuid.uuid4().hex}.json", event)
