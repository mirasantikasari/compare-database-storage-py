import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlparse

from app.config import env
from app.services.checkpoint_service import (
    checkpoint_id_for,
    clear_checkpoint,
    db_reference_from_dict,
    db_reference_to_dict,
    load_bucket_checkpoint,
    load_mapping_checkpoint,
    save_bucket_checkpoint,
    save_mapping_checkpoint,
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
    TableColumnMapping,
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


_TICK_EVERY_MAPPING_ROWS = 5000  # matches mysql_service.BATCH_SIZE, so one tick per DB round-trip
_TICK_EVERY_BUCKET_OBJECTS = 1000  # matches MAX_KEYS_PER_PAGE, so one tick per S3 page


def _collect_mapping_refs(
    mapping: TableColumnMapping,
    database: str | None,
    on_tick: Callable[[int], None] | None = None,
) -> list[DbFileReference]:
    refs: list[DbFileReference] = []
    for ref in fetch_file_references(mapping, database):
        refs.append(ref)
        if on_tick and len(refs) % _TICK_EVERY_MAPPING_ROWS == 0:
            on_tick(len(refs))
    return refs


def _collect_bucket_objects(
    bucket: str,
    prefix: str | None,
    provider: str | None,
    on_tick: Callable[[int], None] | None = None,
) -> list[StorageObject]:
    objects: list[StorageObject] = []
    for obj in iterate_bucket_objects(bucket, prefix, provider):
        objects.append(obj)
        if on_tick and len(objects) % _TICK_EVERY_BUCKET_OBJECTS == 0:
            on_tick(len(objects))
    return objects


_AWS_S3_HOST_RE = re.compile(r"(?:^|\.)s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com$", re.I)


def _split_s3_style_url(pathname: str, host: str) -> tuple[str, str] | None:
    """
    Best-effort split of an S3-style URL's (host, path) into (bucket, key). Tries every
    configured provider's endpoint first — respecting its actual path-style vs virtual-hosted
    setting, the same distinction build_object_url uses — then falls back to AWS's well-known
    *.amazonaws.com pattern even when AWS isn't a configured provider here, since DB columns
    often keep old or foreign references to buckets this app was never given credentials for.
    """
    host_lower = host.lower()

    for config in env.s3_providers.values():
        if not config.endpoint:
            continue
        endpoint_host = urlparse(config.endpoint).netloc.lower()
        if not endpoint_host:
            continue
        if config.force_path_style and host_lower == endpoint_host:
            bucket, _, rest = pathname.partition("/")
            if bucket:
                return bucket, rest
        elif not config.force_path_style and host_lower.endswith(f".{endpoint_host}"):
            bucket = host_lower[: -(len(endpoint_host) + 1)]
            if bucket:
                return bucket, pathname

    match = _AWS_S3_HOST_RE.search(host_lower)
    if match:
        prefix = host_lower[: match.start()].rstrip(".")
        if prefix:
            return prefix, pathname
        bucket, _, rest = pathname.partition("/")
        if bucket:
            return bucket, rest

    return None


def _split_object_reference(value: str) -> tuple[str | None, str]:
    """
    Splits a raw DB column value into (bucket, key). Common variants seen in practice:
     - a bare relative key, possibly with a leading slash the S3 key never has -> (None, key)
     - a full URL through a CDN in front of the bucket (path == key, once host is stripped, since
       there's no reliable way to recover a bucket name from an arbitrary CDN domain) -> (None, key)
     - a full URL straight to a provider, bucket as the first path segment (path-style) or as a
       subdomain (virtual-hosted) -> (bucket, key)
    Bucket is what the caller uses to tell "genuinely missing from the buckets we checked" apart
    from "this reference points at some other bucket/provider we were never asked to scan" —
    those shouldn't be reported as missing at all, since we have no way to confirm or deny it.
    """
    trimmed = value.strip()

    if not trimmed.lower().startswith(("http://", "https://")):
        return None, trimmed.lstrip("/")

    try:
        parsed = urlparse(trimmed)
    except ValueError:
        return None, trimmed.lstrip("/")

    pathname = unquote(parsed.path).lstrip("/")
    split = _split_s3_style_url(pathname, parsed.netloc)
    if split:
        bucket, key = split
        return bucket, key.lstrip("/")
    return None, pathname


def _fetch_mappings(
    mappings: list[TableColumnMapping],
    database: str | None,
    checkpoint_id: str,
    on_progress: ProgressCallback | None,
) -> tuple[list[list[DbFileReference]], list[str | None]]:
    """
    Fetches every mapping's DB rows several at a time (bounded by STORAGE_SUMMARY_CONCURRENCY,
    and by the MySQL pool's own connection limit) instead of one table at a time. Auto
    reconciliation can easily discover 50-100+ candidate columns; running those fully
    sequentially made that phase the single biggest chunk of a scan's wall time.
    """
    results: list[list[DbFileReference] | None] = [None] * len(mappings)
    errors: list[str | None] = [None] * len(mappings)
    completed = 0
    lock = threading.Lock()

    def work(i: int) -> None:
        nonlocal completed
        mapping = mappings[i]
        mapping_key = f"{mapping.table}.{mapping.column}"
        error: str | None = None
        cached = load_mapping_checkpoint(checkpoint_id, mapping_key)
        if cached is not None:
            refs = [db_reference_from_dict(d) for d in cached]
        else:
            started_at = time.monotonic()

            def on_tick(count: int, _key=mapping_key, _started=started_at) -> None:
                # completed/total stay pinned to *fully finished* mappings (unchanged meaning
                # for the progress bar's percent) — only the label ticks up live, so a table with
                # millions of rows doesn't just sit frozen until it's entirely done.
                if on_progress:
                    elapsed = max(time.monotonic() - _started, 0.001)
                    with lock:
                        done_so_far = completed
                    on_progress(
                        "database",
                        done_so_far,
                        len(mappings),
                        f"{_key} — {count:,} row(s) read so far ({count / elapsed:,.0f}/s)",
                        None,
                    )

            try:
                refs = _with_retries(lambda: _collect_mapping_refs(mapping, database, on_tick))
                save_mapping_checkpoint(checkpoint_id, mapping_key, [db_reference_to_dict(r) for r in refs])
            except Exception as err:  # noqa: BLE001
                # One bad table/column (missing permission, renamed column, ...) shouldn't throw
                # away everything already read from the other mappings.
                refs = []
                error = str(err)

        results[i] = refs
        errors[i] = error
        with lock:
            completed += 1
            done = completed
        if on_progress:
            on_progress("database", done, len(mappings), mapping_key, error)

    if mappings:
        workers = min(env.storage_summary_concurrency, len(mappings))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, range(len(mappings))))

    return results, errors


def _fetch_buckets(
    buckets: list[str],
    prefix: str | None,
    provider: str | None,
    checkpoint_id: str,
    on_progress: ProgressCallback | None,
) -> tuple[list[list[StorageObject]], list[str | None]]:
    """Same idea as _fetch_mappings, for bucket listings — helps most when several buckets are
    in scope; a single bucket's own pagination is still inherently sequential (S3 API design)."""
    results: list[list[StorageObject] | None] = [None] * len(buckets)
    errors: list[str | None] = [None] * len(buckets)
    completed = 0
    lock = threading.Lock()

    def work(i: int) -> None:
        nonlocal completed
        bucket = buckets[i]
        error: str | None = None
        cached = load_bucket_checkpoint(checkpoint_id, bucket)
        if cached is not None:
            objs = [storage_object_from_dict(d) for d in cached]
        else:
            started_at = time.monotonic()

            def on_tick(count: int, _bucket=bucket, _started=started_at) -> None:
                if on_progress:
                    elapsed = max(time.monotonic() - _started, 0.001)
                    with lock:
                        done_so_far = completed
                    on_progress(
                        "storage",
                        done_so_far,
                        len(buckets),
                        f"{_bucket} — {count:,} object(s) scanned so far ({count / elapsed:,.0f}/s)",
                        None,
                    )

            try:
                objs = _with_retries(lambda: _collect_bucket_objects(bucket, prefix, provider, on_tick))
                save_bucket_checkpoint(checkpoint_id, bucket, [storage_object_to_dict(o) for o in objs])
            except Exception as err:  # noqa: BLE001
                objs = []
                message = str(err)
                if not is_region_mismatch_error(message):
                    error = message

        results[i] = objs
        errors[i] = error
        with lock:
            completed += 1
            done = completed
        if on_progress:
            on_progress("storage", done, len(buckets), bucket, error)

    if buckets:
        workers = min(env.storage_summary_concurrency, len(buckets))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, range(len(buckets))))

    return results, errors


def run_reconciliation(
    request: ReconciliationRequest,
    on_progress: ProgressCallback | None = None,
) -> ReconciliationResult:
    """
    Meant to be run via asyncio.to_thread from the async route handlers — every S3/MySQL call
    inside it is blocking, so keeping it off the event loop is what keeps the server responsive
    (heartbeats, other requests) while a scan is in progress, however long it takes.

    Mapping fetches and bucket scans each run several at a time (see _fetch_mappings /
    _fetch_buckets) instead of one at a time. Each is also retried a few times on transient
    failure; if one still fails, it's reported as an error but every other bucket/mapping that
    already succeeded is kept in a checkpoint (reports/.checkpoints/<id>/ — one small file per
    finished item). Re-running the exact same request (same buckets/mappings/prefix/database/
    provider) skips everything the checkpoint already has and only (re)does what's missing — so
    a scan that dies partway through doesn't mean starting over from zero. The checkpoint is
    deleted once a run finishes with no errors at all.
    """
    checkpoint_id = checkpoint_id_for(request)

    buckets = request.buckets if request.buckets is not None else list_buckets(request.provider)

    mapping_results, mapping_errors = _fetch_mappings(request.mappings, request.database, checkpoint_id, on_progress)
    any_error = any(e is not None for e in mapping_errors)

    db_files: dict[str, DbFileReference] = {}
    database_file_count = 0
    other_provider_count = 0
    for refs in mapping_results:
        for ref in refs or []:
            database_file_count += 1
            url_bucket, key = _split_object_reference(ref.value)
            if url_bucket is not None and url_bucket not in buckets:
                # Names a real bucket, just not one of the ones we're scanning right now (a
                # different provider/account, or simply out of scope for this run) — we have no
                # way to confirm or deny it exists, so it's excluded rather than reported missing.
                other_provider_count += 1
                continue
            db_files.setdefault(key, ref)

    bucket_results, bucket_errors = _fetch_buckets(
        buckets, request.prefix, request.provider, checkpoint_id, on_progress
    )
    if any(e is not None for e in bucket_errors):
        any_error = True

    storage_objects: dict[str, StorageObject] = {}
    storage_object_count = 0
    for objs in bucket_results:
        for obj in objs or []:
            storage_object_count += 1
            storage_objects[obj.key] = obj

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
