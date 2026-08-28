import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar
from urllib.parse import unquote, urlparse

from app.config import env
from app.services.checkpoint_service import (
    checkpoint_id_for,
    clear_checkpoint,
    db_reference_from_dict,
    db_reference_to_dict,
    load_bucket_checkpoint,
    load_content_checkpoint,
    load_mapping_checkpoint,
    save_bucket_checkpoint,
    save_content_checkpoint,
    save_mapping_checkpoint,
    storage_object_from_dict,
    storage_object_to_dict,
)
from app.services.mysql_service import CONTENT_BATCH_SIZE, fetch_file_references
from app.services.storage_service import is_region_mismatch_error, iterate_bucket_objects, list_buckets
from app.types import (
    CleanupCandidate,
    DbFileReference,
    DoCleanupResult,
    DoCleanupSummary,
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

_T = TypeVar("_T")


def _with_retries(fn: Callable[[], _T]) -> _T:
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


_AWS_PROVIDER_HINT = "aws"


def _split_s3_style_url(pathname: str, host: str) -> tuple[str, str, str | None] | None:
    """
    Best-effort split of an S3-style URL's (host, path) into (bucket, key, provider_hint). Tries
    every configured provider's endpoint first — respecting its actual path-style vs
    virtual-hosted setting, the same distinction build_object_url uses — then falls back to AWS's
    well-known *.amazonaws.com pattern even when AWS isn't a configured provider here, since DB
    columns often keep old or foreign references to buckets this app was never given credentials
    for. provider_hint is that matched provider's key (e.g. "do-sfo2"), or the literal "aws" for the
    generic-pattern fallback — confident enough to tell "this URL names a *different* provider
    than the one being scanned" apart from "the bucket just isn't in this scan's list", even when
    the bucket name itself happens to coincide (the same tenant bucket name often exists on more
    than one provider after a migration).
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
                return bucket, rest, config.key
        elif not config.force_path_style and host_lower.endswith(f".{endpoint_host}"):
            bucket = host_lower[: -(len(endpoint_host) + 1)]
            if bucket:
                return bucket, pathname, config.key

    match = _AWS_S3_HOST_RE.search(host_lower)
    if match:
        prefix = host_lower[: match.start()].rstrip(".")
        if prefix:
            return prefix, pathname, _AWS_PROVIDER_HINT
        bucket, _, rest = pathname.partition("/")
        if bucket:
            return bucket, rest, _AWS_PROVIDER_HINT

    return None


def _split_object_reference(value: str) -> tuple[str | None, str, str | None]:
    """
    Splits a raw DB column value into (bucket, key, provider_hint). Common variants seen in
    practice:
     - a bare relative key, possibly with a leading slash the S3 key never has -> (None, key, None)
     - a full URL through a CDN in front of the bucket (path == key, once host is stripped, since
       there's no reliable way to recover a bucket name from an arbitrary CDN domain)
       -> (None, key, None)
     - a full URL straight to a provider, bucket as the first path segment (path-style) or as a
       subdomain (virtual-hosted) -> (bucket, key, provider_hint)
    Bucket is what the caller uses to tell "genuinely missing from the buckets we checked" apart
    from "this reference points at some other bucket/provider we were never asked to scan" —
    those shouldn't be reported as missing at all, since we have no way to confirm or deny it.
    provider_hint lets the caller catch the case where the bucket *name* happens to coincide with
    one being scanned, but the URL's own host makes clear it's actually a different provider (a
    migrated tenant bucket that still exists, under the same name, on its old provider too).
    """
    trimmed = value.strip()

    if not trimmed.lower().startswith(("http://", "https://")):
        return None, trimmed.lstrip("/"), None

    try:
        parsed = urlparse(trimmed)
    except ValueError:
        return None, trimmed.lstrip("/"), None

    pathname = unquote(parsed.path).lstrip("/")
    split = _split_s3_style_url(pathname, parsed.netloc)
    if split:
        bucket, key, provider_hint = split
        return bucket, key.lstrip("/"), provider_hint
    return None, pathname, None


_EMBEDDED_URL_RE = re.compile(r"""https?://[^\s"'<>)]+""", re.I)


def _extract_embedded_keys(text: str, buckets: set[str]) -> set[str]:
    """
    Pulls every Object Storage key referenced *inside* a blob of rich-text/HTML — e.g. the
    <img src="..."> a CKEditor/CKFinder upload leaves behind in a lesson's content column —
    rather than a value that's itself nothing but a single file reference (which
    _split_object_reference alone already handles for dedicated path columns).
    """
    keys: set[str] = set()
    for match in _EMBEDDED_URL_RE.finditer(text):
        url_bucket, key, _url_provider = _split_object_reference(match.group(0))
        if not key:
            continue
        if url_bucket is not None and url_bucket not in buckets:
            continue
        keys.add(key)
    return keys


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
        workers = min(env.db_scan_concurrency, len(mappings))
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
    content_mappings: list[TableColumnMapping] | None = None,
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

    When content_mappings is given (rich-text/JSON columns, from discover_content_columns), an
    object that would otherwise land in `orphan` — absent from every dedicated file-path column —
    but whose key is found embedded inside one of those columns instead (e.g. a CKEditor/CKFinder
    upload referenced only as an <img> inside a lesson's HTML body) is moved to `protected`
    instead. This is what keeps "orphan" from over-claiming "safe to delete" for a file that's
    still genuinely in use, just not through a column this scan would otherwise know to check.
    """
    checkpoint_id = checkpoint_id_for(request)

    buckets = request.buckets if request.buckets is not None else list_buckets(request.provider)

    mapping_results, mapping_errors = _fetch_mappings(request.mappings, request.database, checkpoint_id, on_progress)
    any_error = any(e is not None for e in mapping_errors)

    effective_provider = request.provider or env.s3_default_provider_key

    db_files: dict[str, DbFileReference] = {}
    database_file_count = 0
    other_provider_count = 0
    different_provider_count = 0
    for refs in mapping_results:
        for ref in refs or []:
            database_file_count += 1
            url_bucket, key, url_provider = _split_object_reference(ref.value)
            if url_provider is not None and effective_provider is not None and url_provider != effective_provider:
                # The URL's own host names a *specific*, different provider (e.g. an old AWS S3
                # link) than the one being scanned — even when the bucket name happens to coincide
                # with one being scanned here, because the same tenant bucket name often exists on
                # more than one provider after a migration. Reporting this as Missing would be
                # wrong: the file may well still exist, just not on the provider this run checked.
                different_provider_count += 1
                continue
            if url_bucket is not None and url_bucket not in buckets:
                # Names a real bucket, just not one of the ones we're scanning right now (out of
                # scope for this run, or the host wasn't recognized as any specific provider) — we
                # have no way to confirm or deny it exists, so it's excluded rather than reported missing.
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
                    raw_value=ref.value,
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
            raw_value=ref.value,
        )
        for path, ref in unmatched
    ]

    raw_orphan: list[OrphanFile] = [
        OrphanFile(path=key, bucket=obj.bucket, size=obj.size, last_modified=obj.last_modified)
        for key, obj in storage_objects.items()
        if key not in db_files
    ]

    orphan = raw_orphan
    protected: list[OrphanFile] = []
    content_reference_count = 0

    if content_mappings:
        content_checkpoint_id = checkpoint_id_for(
            ReconciliationRequest(
                mappings=content_mappings,
                buckets=request.buckets,
                prefix=request.prefix,
                database=request.database,
                provider=request.provider,
            )
        )
        content_keys, content_reference_count, content_errors = _fetch_content_keys(
            content_mappings, request.database, set(buckets), content_checkpoint_id, on_progress
        )
        if not any(e is not None for e in content_errors):
            clear_checkpoint(content_checkpoint_id)

        orphan = []
        for item in raw_orphan:
            (protected if item.path in content_keys else orphan).append(item)

    return ReconciliationResult(
        matched=matched,
        missing=missing,
        orphan=orphan,
        protected=protected,
        summary=ReconciliationSummary(
            matched_count=len(matched),
            missing_count=len(missing),
            orphan_count=len(orphan),
            database_file_count=database_file_count,
            storage_object_count=storage_object_count,
            other_provider_count=other_provider_count,
            different_provider_count=different_provider_count,
            protected_count=len(protected),
            content_reference_count=content_reference_count,
        ),
    )


def _collect_content_keys(
    mapping: TableColumnMapping,
    database: str | None,
    buckets: set[str],
    on_tick: Callable[[int], None] | None = None,
) -> tuple[set[str], int]:
    """
    Streams a rich-text column row by row (never buffering the raw HTML), extracting and keeping
    only the (much smaller) set of Object Storage keys it references — so a column full of large
    lesson content doesn't blow up memory or the checkpoint file the way collecting the raw values
    the way _collect_mapping_refs does would.
    """
    keys: set[str] = set()
    rows_scanned = 0
    for ref in fetch_file_references(mapping, database, batch_size=CONTENT_BATCH_SIZE):
        rows_scanned += 1
        keys |= _extract_embedded_keys(ref.value, buckets)
        if on_tick and rows_scanned % _TICK_EVERY_MAPPING_ROWS == 0:
            on_tick(rows_scanned)
    return keys, rows_scanned


def _fetch_content_keys(
    mappings: list[TableColumnMapping],
    database: str | None,
    buckets: set[str],
    checkpoint_id: str,
    on_progress: ProgressCallback | None,
) -> tuple[set[str], int, list[str | None]]:
    """Same fan-out/retry/checkpoint shape as _fetch_mappings, but for rich-text columns scanned
    for embedded Object Storage references instead of dedicated file-path columns."""
    results: list[set[str] | None] = [None] * len(mappings)
    row_counts: list[int] = [0] * len(mappings)
    errors: list[str | None] = [None] * len(mappings)
    completed = 0
    lock = threading.Lock()

    def work(i: int) -> None:
        nonlocal completed
        mapping = mappings[i]
        mapping_key = f"{mapping.table}.{mapping.column}"
        error: str | None = None
        cached = load_content_checkpoint(checkpoint_id, mapping_key)
        if cached is not None:
            keys = set(cached["keys"])
            rows_scanned = cached["rowsScanned"]
        else:
            started_at = time.monotonic()

            def on_tick(count: int, _key=mapping_key, _started=started_at) -> None:
                if on_progress:
                    elapsed = max(time.monotonic() - _started, 0.001)
                    with lock:
                        done_so_far = completed
                    on_progress(
                        "content",
                        done_so_far,
                        len(mappings),
                        f"{_key} — {count:,} row(s) scanned so far ({count / elapsed:,.0f}/s)",
                        None,
                    )

            try:
                keys, rows_scanned = _with_retries(lambda: _collect_content_keys(mapping, database, buckets, on_tick))
                save_content_checkpoint(checkpoint_id, mapping_key, sorted(keys), rows_scanned)
            except Exception as err:  # noqa: BLE001 - one bad column shouldn't abort the whole scan
                keys, rows_scanned = set(), 0
                error = str(err)

        results[i] = keys
        row_counts[i] = rows_scanned
        errors[i] = error
        with lock:
            completed += 1
            done = completed
        if on_progress:
            on_progress("content", done, len(mappings), mapping_key, error)

    if mappings:
        workers = min(env.db_scan_concurrency, len(mappings))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, range(len(mappings))))

    all_keys: set[str] = set()
    for keys in results:
        if keys:
            all_keys |= keys
    return all_keys, sum(row_counts), errors


def run_do_cleanup_scan(
    database: str,
    file_mappings: list[TableColumnMapping],
    content_mappings: list[TableColumnMapping],
    buckets: list[str] | None,
    prefix: str | None,
    provider: str | None,
    on_progress: ProgressCallback | None = None,
) -> DoCleanupResult:
    """
    Finds Object Storage objects that are safe cleanup candidates: present in storage, absent
    from every discovered file-reference column (the normal reconciliation "orphan" set) — AND
    not found embedded inside any rich-text/HTML column either (content_mappings). That second
    check is what keeps a CKEditor/CKFinder-uploaded file (referenced only as an <img>/<a> inside
    a big HTML blob, never as its own dedicated path column) from being reported as a cleanup
    candidate — it would otherwise look identical to a genuinely unused file to the plain
    reconciliation orphan check.

    Deletion itself is intentionally out of scope: this only ever produces a report (via
    excel_service.generate_do_cleanup_report) for manual review, the same as the rest of this
    module.
    """
    request = ReconciliationRequest(
        buckets=buckets, prefix=prefix, mappings=file_mappings, database=database, provider=provider
    )
    result = run_reconciliation(request, on_progress, content_mappings=content_mappings)

    def as_candidates(items: list[OrphanFile]) -> list[CleanupCandidate]:
        return [CleanupCandidate(path=o.path, bucket=o.bucket, size=o.size, last_modified=o.last_modified) for o in items]

    candidates = as_candidates(result.orphan)
    protected = as_candidates(result.protected)

    return DoCleanupResult(
        candidates=candidates,
        protected=protected,
        summary=DoCleanupSummary(
            candidate_count=len(candidates),
            protected_count=len(protected),
            orphan_count=result.summary.orphan_count + result.summary.protected_count,
            matched_count=result.summary.matched_count,
            missing_count=result.summary.missing_count,
            database_file_count=result.summary.database_file_count,
            storage_object_count=result.summary.storage_object_count,
            content_reference_count=result.summary.content_reference_count,
            other_provider_count=result.summary.other_provider_count,
            different_provider_count=result.summary.different_provider_count,
        ),
    )
