import time
from collections.abc import Callable
from urllib.parse import unquote, urlparse

from app.config import env
from app.services.checkpoint_service import (
    checkpoint_id_for,
    clear_checkpoint,
    db_reference_from_dict,
    db_reference_to_dict,
    load_checkpoint,
    save_checkpoint,
    storage_object_from_dict,
    storage_object_to_dict,
)
from app.services.mysql_service import fetch_file_references
from app.services.storage_service import is_region_mismatch_error, iterate_bucket_objects, list_buckets
from app.types import (
    DbFileReference,
    MatchedFile,
    MissingFile,
    OrphanFile,
    ReconciliationRequest,
    ReconciliationResult,
    ReconciliationSummary,
    StorageObject,
)

ProgressCallback = Callable[[str, int, int, str, str | None], None]

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0


def _with_retries(fn: Callable[[], list]) -> list:
    """Retries a whole bucket/mapping fetch a few times (with backoff) before giving up on it —
    a dropped connection partway through a huge bucket/table is transient far more often than
    it's a real, permanent problem."""
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as error:  # noqa: BLE001 - re-raised below once retries are exhausted
            last_error = error
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise last_error  # type: ignore[misc]


def _collect_mapping_refs(mapping, database: str | None) -> list[DbFileReference]:
    return list(fetch_file_references(mapping, database))


def _collect_bucket_objects(bucket: str, prefix: str | None, provider: str | None) -> list[StorageObject]:
    return list(iterate_bucket_objects(bucket, prefix, provider))


def _provider_hosts() -> dict[str, str]:
    """netloc (host[:port]) of each configured provider's endpoint, lowercased, keyed by provider key."""
    return {
        key: urlparse(config.endpoint).netloc.lower()
        for key, config in env.s3_providers.items()
        if config.endpoint
    }


def _belongs_to_other_provider(value: str, own_provider: str | None, provider_hosts: dict[str, str]) -> bool:
    """
    True when `value` is a full URL whose host matches a *different* configured provider than the
    one this scan is running against (e.g. a Wasabi URL left in a DB column that's now served from
    DigitalOcean, from before a migration). Such a ref will never be found in the bucket being
    scanned, on any provider, so it must be excluded up front rather than reported as "missing" —
    otherwise every provider migration permanently pollutes the missing list with files that were
    never lost, just moved. Bare paths/keys and URLs to unrecognized hosts have no provider
    to contradict the scan, so they're left alone and checked as before.
    """
    trimmed = value.strip()
    if not trimmed.lower().startswith(("http://", "https://")):
        return False
    try:
        host = urlparse(trimmed).netloc.lower()
    except ValueError:
        return False

    resolved_own = own_provider or env.s3_default_provider_key
    own_host = provider_hosts.get(resolved_own) if resolved_own else None
    if host == own_host:
        return False
    return any(key != resolved_own and host == other_host for key, other_host in provider_hosts.items())


def _normalize_path(value: str, buckets: list[str]) -> str:
    """
    DB columns don't always store a raw object key. Common variants seen in practice:
     - a leading slash the S3 key never has
     - a full URL through a CDN in front of the bucket (path == key, once host is stripped)
     - a full URL straight to the provider, with the bucket name as the first path segment
    Bare filenames with no folder structure at all can't be recovered generically (the app
    must be reconstructing the real key from other columns) — those are left as-is and will
    legitimately show up as missing.
    """
    trimmed = value.strip()

    if not trimmed.lower().startswith(("http://", "https://")):
        return trimmed.lstrip("/")

    try:
        parsed = urlparse(trimmed)
        pathname = unquote(parsed.path).lstrip("/")
        for bucket in buckets:
            if pathname == bucket or pathname.startswith(f"{bucket}/"):
                pathname = pathname[len(bucket) :].lstrip("/")
                break
        return pathname
    except ValueError:
        return trimmed.lstrip("/")


def run_reconciliation(
    request: ReconciliationRequest,
    on_progress: ProgressCallback | None = None,
) -> ReconciliationResult:
    """
    Meant to be run via asyncio.to_thread from the async route handlers — every S3/MySQL call
    inside it is blocking, so keeping it off the event loop is what keeps the server responsive
    (heartbeats, other requests) while a scan is in progress, however long it takes.

    Each bucket/mapping is retried a few times on transient failure; if it still fails, that one
    item is reported as an error but every other bucket/mapping that already succeeded is kept in
    a checkpoint file (reports/.checkpoints/<id>.json). Re-running the exact same request (same
    buckets/mappings/prefix/database/provider) skips everything the checkpoint already has and
    only (re)does what's missing — so a scan that dies partway through a 1TB bucket doesn't mean
    starting over from zero. The checkpoint is deleted once a run finishes with no errors at all.
    """
    checkpoint_id = checkpoint_id_for(request)
    checkpoint = load_checkpoint(checkpoint_id)
    any_error = False

    buckets = request.buckets if request.buckets is not None else list_buckets(request.provider)
    db_files: dict[str, DbFileReference] = {}
    database_file_count = 0
    other_provider_count = 0
    provider_hosts = _provider_hosts()

    for i, mapping in enumerate(request.mappings):
        mapping_key = f"{mapping.table}.{mapping.column}"
        mapping_error: str | None = None
        cached = checkpoint["mappings"].get(mapping_key)
        if cached is not None:
            refs = [db_reference_from_dict(d) for d in cached]
        else:
            try:
                refs = _with_retries(lambda: _collect_mapping_refs(mapping, request.database))
                checkpoint["mappings"][mapping_key] = [db_reference_to_dict(r) for r in refs]
                save_checkpoint(checkpoint_id, checkpoint)
            except Exception as error:  # noqa: BLE001
                # One bad table/column (missing permission, renamed column, ...) shouldn't throw
                # away everything already read from the other mappings.
                refs = []
                mapping_error = str(error)
                any_error = True

        for ref in refs:
            if _belongs_to_other_provider(ref.value, request.provider, provider_hosts):
                other_provider_count += 1
                continue
            database_file_count += 1
            normalized = _normalize_path(ref.value, buckets)
            db_files.setdefault(normalized, ref)
        if on_progress:
            on_progress("database", i + 1, len(request.mappings), mapping_key, mapping_error)

    storage_objects: dict[str, StorageObject] = {}
    storage_object_count = 0

    for i, bucket in enumerate(buckets):
        bucket_error: str | None = None
        cached = checkpoint["buckets"].get(bucket)
        if cached is not None:
            objs = [storage_object_from_dict(d) for d in cached]
        else:
            try:
                objs = _with_retries(lambda: _collect_bucket_objects(bucket, request.prefix, request.provider))
                checkpoint["buckets"][bucket] = [storage_object_to_dict(o) for o in objs]
                save_checkpoint(checkpoint_id, checkpoint)
            except Exception as error:  # noqa: BLE001
                objs = []
                message = str(error)
                if not is_region_mismatch_error(message):
                    bucket_error = message
                    any_error = True

        for obj in objs:
            storage_object_count += 1
            storage_objects[obj.key] = obj
        if on_progress:
            on_progress("storage", i + 1, len(buckets), bucket, bucket_error)

    if not any_error:
        clear_checkpoint(checkpoint_id)

    # A single bucket in scope makes the bucket for every missing file unambiguous up front.
    single_bucket_hint = buckets[0] if len(buckets) == 1 else None

    matched: list[MatchedFile] = []
    unmatched: list[tuple[str, DbFileReference]] = []

    for path, ref in db_files.items():
        obj = storage_objects.get(path)
        if obj:
            matched.append(
                MatchedFile(
                    path=path,
                    bucket=obj.bucket,
                    size=obj.size,
                    last_modified=obj.last_modified,
                    table=ref.table,
                    column=ref.column,
                    id=ref.id,
                )
            )
        else:
            unmatched.append((path, ref))

    # A missing file was never found in storage, so there's no real bucket to attribute it to —
    # but when several buckets were scanned, one DB column almost always still points at exactly
    # one of them in practice, so whichever bucket its *matched* siblings (same table.column)
    # landed in is a reliable guess, letting the report build a (dead, but informative) full link
    # instead of a bare relative path even in a multi-bucket scan.
    bucket_by_mapping: dict[tuple[str, str], str] = {}
    for m in matched:
        bucket_by_mapping.setdefault((m.table, m.column), m.bucket)

    missing: list[MissingFile] = [
        MissingFile(
            path=path,
            table=ref.table,
            column=ref.column,
            id=ref.id,
            bucket=single_bucket_hint or bucket_by_mapping.get((ref.table, ref.column)),
        )
        for path, ref in unmatched
    ]

    orphan: list[OrphanFile] = [
        OrphanFile(path=key, bucket=obj.bucket, size=obj.size, last_modified=obj.last_modified)
        for key, obj in storage_objects.items()
        if key not in db_files
    ]

    return ReconciliationResult(
        matched=matched,
        missing=missing,
        orphan=orphan,
        summary=ReconciliationSummary(
            matched_count=len(matched),
            missing_count=len(missing),
            orphan_count=len(orphan),
            database_file_count=database_file_count,
            storage_object_count=storage_object_count,
            other_provider_count=other_provider_count,
        ),
    )
