import os
from pathlib import Path
import boto3
from botocore.client import Config

S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "warehouse")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(bucket: str = S3_BUCKET) -> None:
    client = _client()
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def upload_seed(path: Path, bucket: str = S3_BUCKET) -> None:
    client = _client()
    ensure_bucket(bucket)
    key = f"seeds/{path.name}"
    client.upload_file(str(path), bucket, key)


def download_seeds(dest: Path, bucket: str = S3_BUCKET) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="seeds/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].split("/", 1)[1]
            target = dest / name
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, obj["Key"], str(target))
