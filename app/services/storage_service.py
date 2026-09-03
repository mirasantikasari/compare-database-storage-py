import re
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from botocore.exceptions import ClientError

from app.config import env
from app.providers.s3_provider import get_s3_client
from app.types import BucketSummary, ListObjectsPage, StorageObject, StorageSummary

MAX_KEYS_PER_PAGE = 1000
BUCKET_SUMMARY_CONCURRENCY = env.storage_summary_concurrency

_REGION_MISMATCH_RE = re.compile(r"must be addressed using the specified endpoint", re.I)


def list_buckets(provider: str | None = None) -> list[str]:
    result = get_s3_client(provider).list_buckets()
    return [b["Name"] for b in result.get("Buckets", []) if b.get("Name")]


def list_objects_page(
    bucket: str,
    prefix: str | None = None,
    max_keys: int | None = None,
    continuation_token: str | None = None,
    provider: str | None = None,
) -> ListObjectsPage:
    kwargs = {
        "Bucket": bucket,
        "MaxKeys": max_keys or MAX_KEYS_PER_PAGE,
    }
    if prefix:
        kwargs["Prefix"] = prefix
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    result = get_s3_client(provider).list_objects_v2(**kwargs)

    objects = [
        StorageObject(
            bucket=bucket,
            key=obj["Key"],
            size=obj.get("Size", 0),
            last_modified=obj.get("LastModified"),
            etag=obj.get("ETag"),
        )
        for obj in result.get("Contents", [])
    ]

    return ListObjectsPage(
        objects=objects,
        is_truncated=result.get("IsTruncated", False),
        next_continuation_token=result.get("NextContinuationToken"),
    )


def iterate_bucket_objects(
    bucket: str,
    prefix: str | None = None,
    provider: str | None = None,
) -> Iterator[StorageObject]:
    """Streams every object in a bucket, following pagination, without buffering it in memory."""
    continuation_token: str | None = None

    while True:
        page = list_objects_page(
            bucket, prefix=prefix, continuation_token=continuation_token, provider=provider
        )
        yield from page.objects
        continuation_token = page.next_continuation_token if page.is_truncated else None
        if not continuation_token:
            break


def get_bucket_summary(
    bucket: str,
    prefix: str | None = None,
    provider: str | None = None,
) -> BucketSummary:
    object_count = 0
    total_size = 0
    for obj in iterate_bucket_objects(bucket, prefix, provider):
        object_count += 1
        total_size += obj.size
    return BucketSummary(bucket=bucket, object_count=object_count, total_size=total_size)


def is_region_mismatch_error(message: str) -> bool:
    return bool(_REGION_MISMATCH_RE.search(message))


def get_storage_summary(
    buckets: list[str] | None = None,
    prefix: str | None = None,
    provider: str | None = None,
    on_bucket_done: Callable[[int, int, BucketSummary], None] | None = None,
) -> StorageSummary:
    """
    Summarizes every accessible bucket, bounding concurrent bucket scans in a thread pool so
    one slow/stuck bucket never blocks the others (or the caller's event loop, since this whole
    function is meant to be run via asyncio.to_thread from the async route handlers).
    """
    target_buckets = buckets if buckets is not None else list_buckets(provider)
    summaries: list[BucketSummary | None] = [None] * len(target_buckets)
    completed = 0
    lock = threading.Lock()

    def scan_one(index: int, bucket: str) -> None:
        nonlocal completed
        try:
            summary = get_bucket_summary(bucket, prefix, provider)
        except Exception as error:  # noqa: BLE001 - one bad bucket shouldn't abort the scan
            summary = BucketSummary(bucket=bucket, object_count=0, total_size=0, error=str(error))
        summaries[index] = summary
        with lock:
            completed += 1
            done = completed
        if on_bucket_done:
            on_bucket_done(done, len(target_buckets), summary)

    if target_buckets:
        with ThreadPoolExecutor(max_workers=min(BUCKET_SUMMARY_CONCURRENCY, len(target_buckets))) as pool:
            list(pool.map(lambda item: scan_one(*item), enumerate(target_buckets)))

    reportable = [
        s for s in summaries if s is not None and not (s.error and is_region_mismatch_error(s.error))
    ]

    return StorageSummary(
        buckets=reportable,
        bucket_count=len(reportable),
        object_count=sum(b.object_count for b in reportable),
        total_size=sum(b.total_size for b in reportable),
    )


_DELETE_BATCH_SIZE = 1000  # S3 DeleteObjects hard limit per request
# Deliberately smaller than the S3 max when progress is being streamed: a human reviewing a
# checkbox list before confirming rarely selects anywhere near 1000 files, and batching at 1000
# would mean the progress bar jumps straight from 0% to 100% with nothing in between for exactly
# the runs where watching it matters most. 50 keeps every batch's blast radius small too.
_DELETE_STREAM_BATCH_SIZE = 50

_PUBLIC_URL_CHECK_CONCURRENCY = 20
_PUBLIC_URL_CHECK_TIMEOUT_SECONDS = 10


def _is_configured_storage_url(url: str) -> bool:
    """Restricts uploaded report URLs to configured provider hosts (including bucket subdomains)."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    hostname = parsed.hostname.lower()
    for config in env.s3_providers.values():
        endpoint_host = urlsplit(config.endpoint).hostname if config.endpoint else None
        if endpoint_host:
            endpoint_host = endpoint_host.lower()
            if hostname == endpoint_host or hostname.endswith(f".{endpoint_host}"):
                return True
        elif config.key == "aws" and hostname.endswith(".amazonaws.com"):
            return True
    return False


def validate_public_destination_urls(
    items: list[dict],
    concurrency: int = _PUBLIC_URL_CHECK_CONCURRENCY,
    on_progress: Callable[[int, int, bool, str | None], None] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Verifies that each migration destination URL is anonymously readable without downloading it.
    A one-byte range GET tests the same public path a browser uses while keeping bandwidth tiny.
    Returns (valid_items, failures), preserving report order in both collections.
    """
    outcomes: list[tuple[bool, str | None] | None] = [None] * len(items)
    completed = 0
    lock = threading.Lock()

    def check(index: int) -> None:
        nonlocal completed
        url = str(items[index].get("destinationUrl") or "")
        valid = False
        error: str | None = None
        if not _is_configured_storage_url(url):
            error = "Destination URL is not an HTTPS URL for a configured storage provider"
        else:
            try:
                request = Request(
                    url,
                    headers={"Range": "bytes=0-0", "User-Agent": "object-storage-reconciler/1.0"},
                    method="GET",
                )
                with urlopen(request, timeout=_PUBLIC_URL_CHECK_TIMEOUT_SECONDS) as response:  # noqa: S310 - host allowlisted above
                    status = response.status
                    if status in {200, 206}:
                        response.read(1)
                        valid = True
                    else:
                        error = f"HTTP {status}"
            except HTTPError as exc:
                error = f"HTTP {exc.code}: {exc.reason}"
            except URLError as exc:
                error = f"Connection error: {exc.reason}"
            except (OSError, TimeoutError) as exc:
                error = f"Connection error: {exc}"

        outcomes[index] = (valid, error)
        with lock:
            completed += 1
            done = completed
        if on_progress:
            on_progress(done, len(items), valid, error)

    if items:
        workers = min(max(1, concurrency), len(items))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(check, range(len(items))))

    valid_items = [item for item, outcome in zip(items, outcomes) if outcome and outcome[0]]
    failures = [
        {**item, "error": outcome[1]}
        for item, outcome in zip(items, outcomes)
        if outcome and not outcome[0]
    ]
    return valid_items, failures


def delete_objects(
    items: list[tuple[str, str]],
    provider: str | None = None,
    batch_size: int = _DELETE_BATCH_SIZE,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[dict]:
    """
    Permanently deletes objects — irreversible, no confirmation or safety check happens here;
    the caller (the /storage/delete route) is where that belongs. Grouped by bucket since
    DeleteObjects is a per-bucket batch call; returns one result per requested (bucket, key),
    success or error, in the same order they were given — the caller's audit trail of exactly
    what happened to each item. on_progress(completed, total, bucket), when given, fires after
    every batch actually returns from the provider — never optimistically before.
    """
    client = get_s3_client(provider)

    by_bucket: dict[str, list[str]] = {}
    for bucket, key in items:
        by_bucket.setdefault(bucket, []).append(key)

    total = len(items)
    completed = 0
    outcome_by_item: dict[tuple[str, str], dict] = {}
    for bucket, keys in by_bucket.items():
        for start in range(0, len(keys), batch_size):
            chunk = keys[start : start + batch_size]
            try:
                resp = client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": False},
                )
            except Exception as error:  # noqa: BLE001 - one bad batch shouldn't lose the whole request's audit trail
                message = str(error)
                for key in chunk:
                    outcome_by_item[(bucket, key)] = {
                        "bucket": bucket, "key": key, "success": False, "error": message
                    }
                completed += len(chunk)
                if on_progress:
                    on_progress(completed, total, bucket)
                continue

            for deleted in resp.get("Deleted", []):
                outcome_by_item[(bucket, deleted["Key"])] = {
                    "bucket": bucket, "key": deleted["Key"], "success": True, "error": None
                }
            for err in resp.get("Errors", []):
                outcome_by_item[(bucket, err["Key"])] = {
                    "bucket": bucket,
                    "key": err["Key"],
                    "success": False,
                    "error": err.get("Message") or err.get("Code") or "Unknown error",
                }
            completed += len(chunk)
            if on_progress:
                on_progress(completed, total, bucket)

    return [
        outcome_by_item.get(
            (bucket, key),
            {"bucket": bucket, "key": key, "success": False, "error": "No response from provider"},
        )
        for bucket, key in items
    ]


_COPY_CONCURRENCY = env.storage_copy_concurrency


def _ensure_dest_buckets(dst_client, dest_provider: str, buckets: set[str]) -> dict[str, str | None]:
    """
    Creates any of `buckets` that don't already exist on the destination provider, before any
    item-level copy is attempted. Without this, a first-time migration to a bucket that's never
    existed on the destination (the common case — a tenant's bucket name being unchanged doesn't
    mean the bucket itself was ever created there) fails every single item individually with the
    same NoSuchBucket error, which is both slower (one failed round-trip per item) and a worse
    error message than catching it once up front. Returns {bucket: error_message_or_None} so the
    caller can short-circuit every item destined for a bucket that couldn't be created (e.g. the
    credential lacks CreateBucket permission) instead of letting each one fail the same way again.

    Calls CreateBucket directly rather than checking existence with HeadBucket first — the error
    a *missing* bucket produces on HeadBucket isn't standardized across S3-compatible providers
    (AWS: a clean 404/NoSuchBucket; Wasabi: a bare, code-less 400 that's indistinguishable from a
    real problem), so there's no reliable way to tell "doesn't exist yet" apart from "something's
    actually wrong" from that response alone. CreateBucket's "you already own this" response is
    far more consistent — and on Wasabi specifically, re-creating a bucket already owned by the
    same account is simply a silent no-op, not even an error.
    """
    region = None
    config = env.s3_providers.get(dest_provider)
    if config:
        region = config.region

    results: dict[str, str | None] = {}
    for bucket in buckets:
        try:
            if region and region != "us-east-1":
                dst_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
            else:
                dst_client.create_bucket(Bucket=bucket)
            results[bucket] = None
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            # Already exists and this credential owns it -> nothing to do, not a real failure.
            # (BucketAlreadyExists, by contrast, means someone else owns that name — a genuine
            # failure, left as an error below.)
            results[bucket] = None if code == "BucketAlreadyOwnedByYou" else str(error)

    return results


_ITEM_PROGRESS_MIN_INTERVAL = 0.2  # seconds between item_progress callbacks for one file


def copy_objects(
    items: list[tuple[str, str]],
    source_provider: str,
    dest_provider: str,
    dest_bucket: str | None = None,
    overwrite: bool = False,
    make_public: bool = True,
    concurrency: int | None = None,
    on_progress: Callable[[int, int, str, bool, bool], None] | None = None,
    on_item_progress: Callable[[str, int, int], None] | None = None,
) -> list[dict]:
    """
    Copies objects from one S3-compatible provider to another (e.g. DigitalOcean Spaces ->
    Wasabi, for a provider migration). Unlike delete_objects, this never touches the source —
    intentionally: the caller is expected to update the app's own DB references to the new
    provider first and confirm the migration worked before anything at the old location is
    considered for removal (see the separate, human-gated delete flow).

    S3's server-side CopyObject only works within a single endpoint, so a cross-provider copy
    can't use it — instead each object is streamed through the app (GetObject from source,
    then a managed multipart-aware upload to destination), which is slower but the only option
    across providers. dest_bucket, when given, sends every item into that one bucket regardless
    of its source bucket name; otherwise each item keeps its own source bucket name on the
    destination side too (the common case when a tenant's bucket name is unchanged by the
    migration). Any destination bucket that doesn't exist yet is created automatically (see
    _ensure_dest_buckets) — a bucket name being unchanged across providers doesn't mean the
    bucket itself was ever created on the new one.

    overwrite=False (the default) HeadObjects the destination first and skips the actual
    GetObject/upload when the key is already there — so re-uploading the same source report (or
    one that overlaps a previous run) doesn't redo already-finished transfers. This is intentionally
    keyed off what's actually sitting at the destination rather than which report/file the caller
    used, since that's the only thing that can't go stale or miss a re-copy from a different
    report that happens to cover the same objects. Set overwrite=True to force a fresh copy of
    every item regardless.

    make_public=True (the default) uploads with ACL=public-read. It also reapplies public-read to
    an already-present object before marking it Skipped, so re-running a report repairs objects
    left private by an earlier migration without transferring their bytes again. This app's whole
    premise is files a browser fetches directly by URL (every report's links assume that), and a
    fresh upload otherwise lands with the destination provider's own default ACL — typically
    private, even when the *source* object was public (an object's ACL is never preserved by a
    GetObject+PutObject copy the way it would be by a same-provider CopyObject) — which silently
    breaks every link pointing at it. Set make_public=False to leave the destination object's ACL
    alone instead. Note this only sets the object's own ACL: a destination bucket with its own
    "Block Public Access" style setting enabled (a provider console setting, not something this
    app can see or change) can still keep objects unreachable regardless of their ACL.
    on_progress(completed, total, key, success, skipped), when given, fires after each item
    finishes — never optimistically before. on_item_progress(key, bytes_transferred, total_bytes),
    when given, fires *during* an in-progress upload (throttled to roughly once every
    _ITEM_PROGRESS_MIN_INTERVAL seconds per file) — on_progress alone only moves once per whole
    file, which for a single large file (or a small selection of them) means no feedback at all
    until it's already done; this is what lets a caller show real "how much longer" progress
    instead of a bar stuck at 0% the entire time.
    """
    src_client = get_s3_client(source_provider)
    dst_client = get_s3_client(dest_provider)

    target_buckets = {dest_bucket or bucket for bucket, _key in items}
    bucket_errors = _ensure_dest_buckets(dst_client, dest_provider, target_buckets) if target_buckets else {}

    total = len(items)
    results: list[dict | None] = [None] * total
    completed = 0
    lock = threading.Lock()

    def copy_one(index: int) -> None:
        nonlocal completed
        bucket, key = items[index]
        target_bucket = dest_bucket or bucket
        skipped = False

        bucket_error = bucket_errors.get(target_bucket)
        if bucket_error:
            outcome = {
                "bucket": bucket, "key": key, "destBucket": target_bucket,
                "success": False, "skipped": False, "error": f"Destination bucket unavailable: {bucket_error}",
            }
            results[index] = outcome
            with lock:
                completed += 1
                done = completed
            if on_progress:
                on_progress(done, total, key, False, False)
            return

        try:
            if not overwrite:
                try:
                    dst_client.head_object(Bucket=target_bucket, Key=key)
                    skipped = True
                except ClientError:
                    skipped = False  # not found at the destination (or a transient error) -> copy for real

            if skipped:
                # Existing destination objects may have been created privately by an earlier run.
                # "Skipped" only means their bytes do not need transferring; it must not bypass
                # the explicitly requested visibility setting.
                if make_public:
                    dst_client.put_object_acl(Bucket=target_bucket, Key=key, ACL="public-read")
                outcome = {
                    "bucket": bucket, "key": key, "destBucket": target_bucket,
                    "success": True, "skipped": True, "error": None,
                }
            else:
                obj = src_client.get_object(Bucket=bucket, Key=key)
                extra_args = {}
                if obj.get("ContentType"):
                    extra_args["ContentType"] = obj["ContentType"]
                if make_public:
                    extra_args["ACL"] = "public-read"

                callback = None
                if on_item_progress:
                    total_bytes = obj.get("ContentLength") or 0
                    transferred = 0
                    last_emit = 0.0

                    def callback(bytes_amount: int) -> None:
                        nonlocal transferred, last_emit
                        transferred += bytes_amount
                        now = time.monotonic()
                        if now - last_emit >= _ITEM_PROGRESS_MIN_INTERVAL or transferred >= total_bytes:
                            last_emit = now
                            on_item_progress(key, transferred, total_bytes)

                dst_client.upload_fileobj(
                    obj["Body"], target_bucket, key, ExtraArgs=extra_args or None, Callback=callback
                )
                outcome = {
                    "bucket": bucket, "key": key, "destBucket": target_bucket,
                    "success": True, "skipped": False, "error": None,
                }
        except Exception as error:  # noqa: BLE001 - one bad object shouldn't abort the whole batch
            outcome = {
                "bucket": bucket, "key": key, "destBucket": target_bucket,
                "success": False, "skipped": False, "error": str(error),
            }
        results[index] = outcome
        with lock:
            completed += 1
            done = completed
        if on_progress:
            on_progress(done, total, key, outcome["success"], outcome["skipped"])

    if items:
        workers = min(concurrency or _COPY_CONCURRENCY, len(items))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(copy_one, range(total)))

    return results
