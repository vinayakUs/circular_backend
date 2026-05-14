import logging
from typing import Optional

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from config import Config


class S3StorageClient:
    """Wrapper for S3 operations used by the storage layer."""

    def __init__(
        self,
        bucket: str | None = None,
        region: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.bucket = bucket or Config.AWS_S3_BUCKET
        self.region = region or Config.AWS_S3_REGION

        key_id = aws_access_key_id or Config.AWS_ACCESS_KEY_ID
        secret = aws_secret_access_key or Config.AWS_SECRET_ACCESS_KEY

        session_kwargs: dict = {"region_name": self.region}
        if key_id and secret:
            session_kwargs["aws_access_key_id"] = key_id
            session_kwargs["aws_secret_access_key"] = secret

        session = boto3.Session(**session_kwargs)

        client_kwargs: dict = {
            "service_name": "s3",
            "config": BotocoreConfig(
                retries={"max_attempts": 3, "mode": "standard"}
            ),
        }

        endpoint = endpoint_url or Config.AWS_S3_ENDPOINT_URL
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint

        self.client = session.client(**client_kwargs)

    def _make_key(self, path: str) -> str:
        if path.startswith("s3://"):
            path = path[len("s3://") :]
            parts = path.split("/", 1)
            if parts[0] == self.bucket:
                return parts[1] if len(parts) > 1 else ""
            return path
        return path.lstrip("/")

    def upload_bytes(self, key: str, content: bytes) -> str:
        """Upload bytes to S3 and return the s3:// URL."""
        clean_key = self._make_key(key)
        self.client.put_object(Bucket=self.bucket, Key=clean_key, Body=content)
        return f"s3://{self.bucket}/{clean_key}"

    def download_bytes(self, key: str) -> bytes:
        """Download content from S3 by key or s3:// URL."""
        clean_key = self._make_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=clean_key)
        return response["Body"].read()

    def download_as_text(self, key: str) -> str:
        """Download content from S3 as string."""
        return self.download_bytes(key).decode("utf-8")

    def exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        clean_key = self._make_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=clean_key)
            return True
        except ClientError:
            return False

    def delete(self, key: str) -> None:
        """Delete a single object from S3."""
        clean_key = self._make_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=clean_key)

    def delete_prefix(self, prefix: str) -> None:
        """Delete all objects under a prefix."""
        clean_prefix = self._make_key(prefix).rstrip("/") + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=clean_prefix):
            contents = page.get("Contents", [])
            if contents:
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={
                        "Objects": [{"Key": obj["Key"]} for obj in contents],
                        "Quiet": True,
                    },
                )

    def generate_presigned_url(self, bucket: str, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for an S3 object."""
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )