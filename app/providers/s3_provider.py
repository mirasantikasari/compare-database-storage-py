from urllib.parse import quote, urlsplit

import boto3
from botocore.config import Config

from app.config import env

_client_cache: dict[str, "boto3.client"] = {}


def list_s3_providers() -> list[dict]:
    return [
        {
            "key": provider.key,
            "label": provider.label,
            "isDefault": provider.key == env.s3_default_provider_key,
        }
        for provider in env.s3_providers.values()
    ]


def get_s3_client(provider_key: str | None = None):
    """Lazily creates (and caches) one boto3 S3 client per configured provider."""
    key = provider_key or env.s3_default_provider_key
    if not key:
        raise RuntimeError(
            "No Object Storage provider is configured. Set S3_<PROVIDER>_ACCESS_KEY / "
            "_SECRET_KEY in .env (see .env.example)."
        )

    cached = _client_cache.get(key)
    if cached is not None:
        return cached

    config = env.s3_providers.get(key)
    if config is None:
        available = ", ".join(env.s3_providers.keys()) or "(none configured)"
        raise RuntimeError(f'Unknown storage provider "{key}". Available: {available}')

    client = boto3.client(
        "s3",
        region_name=config.region,
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if config.force_path_style else "virtual"},
            retries={"max_attempts": 5, "mode": "standard"},
            max_pool_connections=env.s3_max_pool_connections,
        ),
    )
    _client_cache[key] = client
    return client


def build_bucket_url(provider_key: str | None, bucket: str) -> str:
    """Base URL for a bucket itself (path-style or virtual-hosted, per the provider's config),
    with no trailing slash — the same addressing rules as build_object_presigned_url, minus an
    object key (and minus a signature, since there's no single object to sign)."""
    resolved_key = provider_key or env.s3_default_provider_key
    config = env.s3_providers.get(resolved_key) if resolved_key else None

    if config is None:
        return bucket

    if config.endpoint:
        parts = urlsplit(config.endpoint)
        if config.force_path_style:
            return f"{parts.scheme}://{parts.netloc}/{bucket}"
        return f"{parts.scheme}://{bucket}.{parts.netloc}"

    host = (
        "s3.amazonaws.com"
        if config.region == "us-east-1"
        else f"s3.{config.region}.amazonaws.com"
    )
    return f"https://{bucket}.{host}"


OBJECT_URL_TTL_SECONDS = 7 * 24 * 60 * 60


def build_object_presigned_url(
    provider_key: str | None, bucket: str, key: str, expires_in: int = OBJECT_URL_TTL_SECONDS
) -> str:
    """Sign a private object download link so it still opens when the bucket/object isn't public."""
    return get_s3_client(provider_key).generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in, HttpMethod="GET",
    )


ARCHIVE_URL_TTL_SECONDS = OBJECT_URL_TTL_SECONDS


def build_archive_presigned_url(provider_key: str | None, bucket: str, key: str) -> str:
    """Sign a private archive download for seven days without fetching its contents."""
    return build_object_presigned_url(provider_key, bucket, key, expires_in=ARCHIVE_URL_TTL_SECONDS)
