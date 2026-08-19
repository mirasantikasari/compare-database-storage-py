import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor

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
