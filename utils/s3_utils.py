import re
# /root/circular_backend/utils/s3_utils.py

def build_safe_filename(value: str) -> str:
    """Sanitize a string for use in S3 keys."""
    safe_value = re.sub(r'[<>:"/\\|?*]+', "_", value).strip()
    safe_value = re.sub(r"\s+", "_", safe_value)
    safe_value = re.sub(r"_+", "_", safe_value).strip("._")
    return safe_value or "document"
