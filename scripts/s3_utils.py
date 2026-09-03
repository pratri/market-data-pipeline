"""
Shared S3 helpers.

Both ingestion scripts need to write Parquet to S3, so the logic lives
here rather than being duplicated.

Config comes from environment variables, loaded from a .env file at the
project root. The bucket name has a random suffix from Terraform, so
hardcoding it would make the code work only for one specific bucket.
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
    """Read KEY=VALUE lines from .env into os.environ.

    Written by hand rather than pulling in python-dotenv: it's twenty
    lines and one fewer dependency to install on the EC2 box.

    Existing environment variables win, so a value set in the shell
    overrides the file. That's the conventional precedence and it's what
    lets Airflow inject config without editing files.
    """
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
    """Return the target bucket name, or exit with a useful message."""
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
    """Build an S3 client.

    boto3 finds credentials automatically, in this order:
      1. environment variables
      2. ~/.aws/credentials  (your laptop, from `aws configure`)
      3. the EC2 instance profile  (the box, no keys needed)

    That last one is why the same code runs unchanged locally and on the
    instance. Nothing to configure, nothing to leak.
    """
    load_env()
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def write_parquet_to_s3(df: pd.DataFrame, key: str, bucket: str | None = None) -> str:
    """Serialize a DataFrame to Parquet in memory and upload it.

    Writing to a BytesIO buffer instead of a temp file avoids touching
    disk at all, which matters on a small EC2 box with limited space.

    Returns the s3:// URI of the written object.
    """
    bucket = bucket or get_bucket()
    client = get_s3_client()

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)  # rewind, or you upload zero bytes

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
    """Check whether an object already exists.

    Used to skip re-downloading data that's already landed, which is the
    basis of incremental loading.
    """
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
    """List every key under a prefix, handling pagination.

    list_objects_v2 returns at most 1000 keys per call. A paginator
    handles the continuation tokens so you don't silently miss data
    once the bucket grows past that.
    """
    bucket = bucket or get_bucket()
    client = get_s3_client()

    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
