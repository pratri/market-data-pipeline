"""
S3 helpers shared by the ingestion scripts.

Config comes from environment variables, or a .env file in the project root.
"""

import io
import os
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).parent.parent
ENV_PATH = PROJECT_ROOT / ".env"


def load_env() -> None:
    """Load KEY=VALUE lines from .env. Variables already set in the environment win."""
    if not ENV_PATH.exists():
        return

    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_bucket() -> str:
    """Return S3_BUCKET, or exit with a hint on how to set it."""
    load_env()
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit(
            "ERROR: S3_BUCKET is not set.\n"
            "Create a .env file in the project root containing:\n"
            "  S3_BUCKET=market-data-pipeline-raw-xxxxxxxx\n"
            "Get the name with: terraform output s3_bucket_name"
        )
    return bucket


def get_s3_client():
    """S3 client. boto3 finds credentials in env vars, ~/.aws, or the EC2 instance profile."""
    load_env()
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def write_parquet_to_s3(df: pd.DataFrame, key: str, bucket: str | None = None) -> str:
    """Write a DataFrame to S3 as parquet, in memory. Returns the s3:// URI."""
    bucket = bucket or get_bucket()
    client = get_s3_client()

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)

    try:
        client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("AccessDenied", "AccessDeniedException"):
            raise SystemExit(
                f"ERROR: access denied writing to s3://{bucket}/{key}\n"
                "Check that your AWS credentials are configured "
                "(aws sts get-caller-identity) and that the bucket name "
                "in .env matches your Terraform output."
            ) from exc
        if code == "NoSuchBucket":
            raise SystemExit(
                f"ERROR: bucket '{bucket}' does not exist.\n"
                "Check S3_BUCKET in .env against: terraform output s3_bucket_name"
            ) from exc
        raise

    return f"s3://{bucket}/{key}"


def s3_key_exists(key: str, bucket: str | None = None) -> bool:
    """True if the object exists."""
    bucket = bucket or get_bucket()
    client = get_s3_client()

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def list_s3_keys(prefix: str, bucket: str | None = None) -> list[str]:
    """All keys under a prefix. Paginated, list_objects_v2 returns 1000 at most."""
    bucket = bucket or get_bucket()
    client = get_s3_client()

    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
