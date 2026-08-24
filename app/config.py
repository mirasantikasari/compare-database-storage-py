import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

PROVIDER_DEFS = [
    ("aws", "S3_AWS", "AWS S3"),
    ("wasabi", "S3_WASABI", "Wasabi"),
    ("do", "S3_DO", "DigitalOcean Spaces"),
]


@dataclass
class S3ProviderConfig:
    key: str
    label: str
    access_key_id: str
    secret_access_key: str
    region: str
    endpoint: str | None
    force_path_style: bool


def _read_s3_provider(key: str, prefix: str, label: str) -> S3ProviderConfig | None:
    access_key_id = os.environ.get(f"{prefix}_ACCESS_KEY")
    secret_access_key = os.environ.get(f"{prefix}_SECRET_KEY")
    if not access_key_id or not secret_access_key:
        return None

    endpoint = os.environ.get(f"{prefix}_ENDPOINT") or None
    force_path_style_raw = os.environ.get(f"{prefix}_FORCE_PATH_STYLE")
    force_path_style = (
        force_path_style_raw == "true" if force_path_style_raw else bool(endpoint)
    )

    return S3ProviderConfig(
        key=key,
        label=label,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=os.environ.get(f"{prefix}_REGION") or "us-east-1",
        endpoint=endpoint,
        force_path_style=force_path_style,
    )


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str | None


@dataclass
class Env:
    port: int
    s3_providers: dict[str, S3ProviderConfig]
    s3_default_provider_key: str | None
    db: DbConfig
    reports_dir: str
    storage_summary_concurrency: int
    storage_copy_concurrency: int
    db_scan_concurrency: int
    max_content_scan_table_rows: int


def _load_env() -> Env:
    s3_providers: dict[str, S3ProviderConfig] = {}
    for key, prefix, label in PROVIDER_DEFS:
        config = _read_s3_provider(key, prefix, label)
        if config:
            s3_providers[key] = config

    requested_default = os.environ.get("S3_DEFAULT_PROVIDER")
    default_provider_key = (
        requested_default
        if requested_default and requested_default in s3_providers
        else (next(iter(s3_providers), None))
    )

    return Env(
        port=int(os.environ.get("PORT", "3000")),
        s3_providers=s3_providers,
        s3_default_provider_key=default_provider_key,
        db=DbConfig(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            port=int(os.environ.get("DB_PORT", "3306")),
            user=os.environ.get("DB_USER", "root"),
            password=os.environ.get("DB_PASSWORD", ""),
            database=os.environ.get("DB_DATABASE") or None,
        ),
        reports_dir=os.environ.get("REPORTS_DIR", "reports"),
        storage_summary_concurrency=int(os.environ.get("STORAGE_SUMMARY_CONCURRENCY", "8")),
        # Separate from storage_summary_concurrency: that one only ever lists metadata, while a
        # cross-provider copy streams each object's full bytes through the app (GET from source,
        # PUT to destination) — too much parallel data transfer can saturate the app's own network
        # link rather than either provider, so this defaults lower.
        storage_copy_concurrency=int(os.environ.get("STORAGE_COPY_CONCURRENCY", "4")),
        # Deliberately separate from storage_summary_concurrency (which only ever calls out to
        # S3-compatible APIs — safe to parallelize aggressively). This one bounds how many
        # simultaneous full-column table scans hit the *production* MySQL server at once, so it
        # defaults low: a handful of parallel LONGTEXT/JSON column scans against a live production
        # database is enough to exhaust its connection pool or saturate its CPU/IOPS.
        db_scan_concurrency=int(os.environ.get("DB_SCAN_CONCURRENCY", "2")),
        # Tables at or above this (approximate, from information_schema — no COUNT(*) needed)
        # row count are skipped by the rich-text/JSON content scan entirely. This is squarely
        # aimed at append-only audit/activity-log tables: by far the largest tables in a typical
        # schema, and the least valuable to scan for embedded file references (they're historical
        # snapshots, not the live content columns that actually need protecting).
        max_content_scan_table_rows=int(os.environ.get("MAX_CONTENT_SCAN_TABLE_ROWS", "50000")),
    )


env = _load_env()
