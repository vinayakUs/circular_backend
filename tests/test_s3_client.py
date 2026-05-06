from __future__ import annotations

import os
import unittest

from storage.s3_client import S3StorageClient


class S3StorageClientTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.bucket = os.getenv("AWS_S3_BUCKET", "my-app-dev-bucket")
        self.endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL", "http://localhost:4566")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "test")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    def _make_client(self, **kwargs) -> S3StorageClient:
        return S3StorageClient(
            bucket=self.bucket,
            region="us-east-1",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            **kwargs,
        )

    def test_upload_and_download_bytes(self) -> None:
        client = self._make_client()
        test_key = "test/upload_download.bin"
        test_content = b"Hello from LocalStack!"

        url = client.upload_bytes(test_key, test_content)
        self.assertEqual(url, f"s3://{self.bucket}/{test_key}")

        self.assertTrue(client.exists(test_key))

        downloaded = client.download_bytes(test_key)
        self.assertEqual(downloaded, test_content)

        client.delete(test_key)
        self.assertFalse(client.exists(test_key))

    def test_upload_download_pdf(self) -> None:
        client = self._make_client()
        test_key = "test/sample.pdf"
        test_content = b"%PDF-1.4 fake pdf content"

        url = client.upload_bytes(test_key, test_content)
        self.assertEqual(url, f"s3://{self.bucket}/{test_key}")

        downloaded = client.download_bytes(test_key)
        self.assertEqual(downloaded, test_content)

        client.delete(test_key)
        self.assertFalse(client.exists(test_key))

    def test_delete_prefix(self) -> None:
        client = self._make_client()
        prefix = "test/prefix_cleanup"

        for i in range(3):
            client.upload_bytes(f"{prefix}/file{i}.pdf", f"content{i}".encode())

        for i in range(3):
            self.assertTrue(client.exists(f"{prefix}/file{i}.pdf"))

        client.delete_prefix(prefix)

        for i in range(3):
            self.assertFalse(client.exists(f"{prefix}/file{i}.pdf"))

    def test_exists_false_for_missing(self) -> None:
        client = self._make_client()
        self.assertFalse(client.exists("nonexistent/path/file.pdf"))


if __name__ == "__main__":
    unittest.main()