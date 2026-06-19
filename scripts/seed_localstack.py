"""Seed LocalStack S3 + Secrets Manager so the DAG can run on a laptop."""
from __future__ import annotations

import json
import os

import boto3

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
KW = {"endpoint_url": ENDPOINT, "region_name": "eu-central-1"}


def main() -> None:
    boto3.client("s3", **KW).create_bucket(
        Bucket="softgames-payments",
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )
    boto3.client("secretsmanager", **KW).create_secret(
        Name="softgames/payment-provider",
        SecretString=json.dumps({"token": "dev-token"}),
    )
    print("seeded.")


if __name__ == "__main__":
    main()
