import hashlib
import json
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
from app.providers.s3_provider import get_s3_client, build_archive_presigned_url
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
    on_progress: Callable[[int, int, int, bool, str | None], None] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Verifies that each migration destination URL is anonymously readable without downloading it.
    A one-byte range GET tests the same public path a browser uses while keeping bandwidth tiny.
    Returns (valid_items, failures), preserving report order in both collections.

    on_progress(index, completed, total, valid, error), when given, fires as each item's own
    check finishes — checks run concurrently, so completion order does not match `items` order,
    and `index` (into `items`) is what lets a caller identify exactly which item just finished
    rather than only how many have finished so far. That, in turn, is what makes a caller-side
    resume possible: after a dropped connection, only the items whose index was never reported
    back need to be re-sent, instead of re-checking the whole list from the start.
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
            on_progress(index, done, len(items), valid, error)

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


_DELETE_CONCURRENCY = env.storage_delete_concurrency


def delete_objects(
    items: list[tuple[str, str]],
    provider: str | None = None,
    batch_size: int = _DELETE_BATCH_SIZE,
    concurrency: int | None = None,
    on_progress: Callable[[int, int, str, list[dict]], None] | None = None,
) -> list[dict]:
    """
    Permanently deletes objects — irreversible, no confirmation or safety check happens here;
    the caller (the /storage/delete route) is where that belongs. Grouped by bucket since
    DeleteObjects is a per-bucket batch call; batches (possibly several per bucket) run across a
    bounded thread pool since, like listing, this only ever exchanges metadata — no object bytes
    flow through the app — so it's safe to parallelize aggressively rather than sending one batch
    at a time. Returns one result per requested (bucket, key), success or error, in the same order
    they were given — the caller's audit trail of exactly what happened to each item.
    on_progress(completed, total, bucket, batch_results), when given, fires after every batch
    actually returns from the provider — never optimistically before — with batch_results being
    that batch's own {bucket, key, success, error} outcomes, so a caller can react to (and persist)
    partial progress as it happens rather than only once the whole request finishes: if the
    connection drops or the request is retried after a crash, whatever batches already completed
    don't need to be redone from scratch.
    """
    client = get_s3_client(provider)

    by_bucket: dict[str, list[str]] = {}
    for bucket, key in items:
        by_bucket.setdefault(bucket, []).append(key)

    chunks: list[tuple[str, list[str]]] = [
        (bucket, keys[start : start + batch_size])
        for bucket, keys in by_bucket.items()
        for start in range(0, len(keys), batch_size)
    ]

    total = len(items)
    completed = 0
    lock = threading.Lock()
    outcome_by_item: dict[tuple[str, str], dict] = {}

    def delete_chunk(bucket: str, chunk: list[str]) -> None:
        nonlocal completed
        try:
            resp = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": False},
            )
        except Exception as error:  # noqa: BLE001 - one bad batch shouldn't lose the whole request's audit trail
            message = str(error)
            batch_results = [
                {"bucket": bucket, "key": key, "success": False, "error": message} for key in chunk
            ]
        else:
            batch_results = [
                {"bucket": bucket, "key": deleted["Key"], "success": True, "error": None}
                for deleted in resp.get("Deleted", [])
            ] + [
                {
                    "bucket": bucket,
                    "key": err["Key"],
                    "success": False,
                    "error": err.get("Message") or err.get("Code") or "Unknown error",
                }
                for err in resp.get("Errors", [])
            ]

        for result in batch_results:
            outcome_by_item[(result["bucket"], result["key"])] = result

        with lock:
            completed += len(chunk)
            done = completed
        if on_progress:
            on_progress(done, total, bucket, batch_results)

    if chunks:
        workers = min(concurrency or _DELETE_CONCURRENCY, len(chunks))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(lambda item: delete_chunk(*item), chunks))

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

    AWS distinguishes "you already own this" (BucketAlreadyOwnedByYou) from "someone else owns
    this name" (BucketAlreadyExists), but DigitalOcean Spaces has been observed returning the
    latter, bare "BucketAlreadyExists", even for a bucket this exact credential already owns — so
    that code alone can't be trusted to mean "genuine failure" the way it does on AWS. Once
    CreateBucket says the bucket exists at all (either code), HeadBucket is what actually
    disambiguates owned-by-this-credential from owned-by-someone-else here: unlike detecting a
    *missing* bucket, a 200 vs. 403/404 on a bucket that's confirmed to exist is consistent across
    providers.
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
            if code == "BucketAlreadyOwnedByYou":
                results[bucket] = None
            elif code == "BucketAlreadyExists":
                try:
                    dst_client.head_bucket(Bucket=bucket)
                    results[bucket] = None  # exists and this credential can access it -> owned by us
                except ClientError:
                    results[bucket] = str(error)  # exists, but this credential can't reach it -> someone else's
            else:
                results[bucket] = str(error)

    return results


_ITEM_PROGRESS_MIN_INTERVAL = 0.2  # seconds between item_progress callbacks for one file

# Modern buckets (AWS S3 with Object Ownership set to "Bucket owner enforced" — the default for
# any bucket created since April 2023 — and some S3-compatible providers) reject any PutObject /
# PutObjectAcl call that carries an ACL at all, rather than just ignoring it. Detected by code
# where the provider gives one; by message otherwise, since some providers surface this as a
# generic error code with only the message actually saying ACLs are unsupported.
_ACL_UNSUPPORTED_CODES = {
    "UnsupportedAclConfigurationException", "AccessControlListNotSupported", "InvalidBucketAclWithObjectOwnership",
}


def _is_acl_unsupported_error(error: ClientError) -> bool:
    response_error = error.response.get("Error", {})
    if str(response_error.get("Code", "")) in _ACL_UNSUPPORTED_CODES:
        return True
    message = str(response_error.get("Message", "")).lower()
    return "acl" in message and any(word in message for word in ("unsupported", "not support", "disabled"))


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
    dest_key_fn: Callable[[str, str], str] | None = None,
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

    A destination bucket with ACLs disabled entirely (AWS's "Bucket owner enforced" Object
    Ownership setting, or an equivalent on another provider) rejects any request that carries an
    ACL at all — so once that's detected for a given target bucket, every subsequent item into
    that same bucket in this call skips the ACL automatically instead of failing the same way
    over and over; making it public then falls to that bucket's own policy/settings instead,
    outside this app's control.
    on_progress(completed, total, key, success, skipped), when given, fires after each item
    finishes — never optimistically before. on_item_progress(key, bytes_transferred, total_bytes),
    when given, fires *during* an in-progress upload (throttled to roughly once every
    _ITEM_PROGRESS_MIN_INTERVAL seconds per file) — on_progress alone only moves once per whole
    file, which for a single large file (or a small selection of them) means no feedback at all
    until it's already done; this is what lets a caller show real "how much longer" progress
    instead of a bar stuck at 0% the entire time.

    dest_key_fn(bucket, key), when given, computes the destination key instead of reusing the
    source key as-is — e.g. archive_and_delete_objects uses it to file every object under a
    "<source bucket>/<source key>" path, so objects from different source buckets never collide
    once dest_bucket points them all at one shared bucket. Every result still reports the
    (unchanged) source key under "key" plus the resolved destination key under "destKey".
    """
    src_client = get_s3_client(source_provider)
    dst_client = get_s3_client(dest_provider)

    target_buckets = {dest_bucket or bucket for bucket, _key in items}
    bucket_errors = _ensure_dest_buckets(dst_client, dest_provider, target_buckets) if target_buckets else {}

    total = len(items)
    results: list[dict | None] = [None] * total
    completed = 0
    lock = threading.Lock()
    # Once a target bucket is found to reject ACLs outright, every later item into that same
    # bucket skips the ACL up front instead of repeating the same failed attempt (see
    # _is_acl_unsupported_error). Plain dict writes are safe enough here without a lock: worst
    # case under a race is a handful of redundant retries, never a wrong result.
    acl_supported: dict[str, bool] = {}

    def copy_one(index: int) -> None:
        nonlocal completed
        bucket, key = items[index]
        target_bucket = dest_bucket or bucket
        target_key = dest_key_fn(bucket, key) if dest_key_fn else key
        skipped = False

        bucket_error = bucket_errors.get(target_bucket)
        if bucket_error:
            outcome = {
                "bucket": bucket, "key": key, "destBucket": target_bucket, "destKey": target_key,
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
                    dst_client.head_object(Bucket=target_bucket, Key=target_key)
                    skipped = True
                except ClientError:
                    skipped = False  # not found at the destination (or a transient error) -> copy for real

            if skipped:
                # Existing destination objects may have been created privately by an earlier run.
                # "Skipped" only means their bytes do not need transferring; it must not bypass
                # the explicitly requested visibility setting.
                if make_public and acl_supported.get(target_bucket, True):
                    try:
                        dst_client.put_object_acl(Bucket=target_bucket, Key=target_key, ACL="public-read")
                    except ClientError as acl_error:
                        if not _is_acl_unsupported_error(acl_error):
                            raise
                        acl_supported[target_bucket] = False  # bucket has ACLs disabled entirely -> nothing to reapply
                outcome = {
                    "bucket": bucket, "key": key, "destBucket": target_bucket, "destKey": target_key,
                    "success": True, "skipped": True, "error": None,
                }
            else:
                obj = src_client.get_object(Bucket=bucket, Key=key)
                extra_args = {}
                if obj.get("ContentType"):
                    extra_args["ContentType"] = obj["ContentType"]
                if make_public and acl_supported.get(target_bucket, True):
                    extra_args["ACL"] = "public-read"

                def make_callback():
                    if not on_item_progress:
                        return None
                    total_bytes = obj.get("ContentLength") or 0
                    state = {"transferred": 0, "last_emit": 0.0}

                    def callback(bytes_amount: int) -> None:
                        state["transferred"] += bytes_amount
                        now = time.monotonic()
                        if now - state["last_emit"] >= _ITEM_PROGRESS_MIN_INTERVAL or state["transferred"] >= total_bytes:
                            state["last_emit"] = now
                            on_item_progress(key, state["transferred"], total_bytes)

                    return callback

                try:
                    dst_client.upload_fileobj(
                        obj["Body"], target_bucket, target_key, ExtraArgs=extra_args or None, Callback=make_callback()
                    )
                except ClientError as acl_error:
                    if "ACL" not in extra_args or not _is_acl_unsupported_error(acl_error):
                        raise
                    # The bucket rejects ACLs outright — remember that for the rest of this run,
                    # then retry this one item without it. The failed attempt may have already
                    # consumed part of the source stream, so it's re-fetched for a clean retry
                    # rather than reusing the same (now potentially partial) body.
                    acl_supported[target_bucket] = False
                    extra_args.pop("ACL", None)
                    obj = src_client.get_object(Bucket=bucket, Key=key)
                    dst_client.upload_fileobj(
                        obj["Body"], target_bucket, target_key, ExtraArgs=extra_args or None, Callback=make_callback()
                    )
                outcome = {
                    "bucket": bucket, "key": key, "destBucket": target_bucket, "destKey": target_key,
                    "success": True, "skipped": False, "error": None,
                }
        except Exception as error:  # noqa: BLE001 - one bad object shouldn't abort the whole batch
            outcome = {
                "bucket": bucket, "key": key, "destBucket": target_bucket, "destKey": target_key,
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


def _archive_dest_key(bucket: str, key: str) -> str:
    return f"{bucket}/{key}"


def ensure_public_read_bucket_policy(provider: str, bucket: str) -> str | None:
    """Ensure anonymous object reads while preserving other bucket policy statements.

    Return a visible error if the provider refuses the public policy.
    """
    client = get_s3_client(provider)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket}/*",
            }
        ],
    }
    try:
        bucket_error = _ensure_dest_buckets(client, provider, {bucket}).get(bucket)
        if bucket_error:
            return bucket_error
        try:
            existing = json.loads(client.get_bucket_policy(Bucket=bucket)["Policy"])
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in {"NoSuchBucketPolicy", "NoSuchPolicy", "404"}:
                raise
        else:
            statements = existing.get("Statement", [])
            if isinstance(statements, dict):
                statements = [statements]
            public_statement = policy["Statement"][0]
            if public_statement not in statements:
                # Do not reuse a Sid that belongs to an existing policy statement.
                public_statement.pop("Sid", None)
                if public_statement not in statements:
                    statements.append(public_statement)
            existing["Statement"] = statements
            policy = existing
        client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
        return None
    except ClientError as error:
        return str(error)


def archive_and_delete_objects(
    items: list[tuple[str, str]],
    source_provider: str | None,
    archive_provider: str,
    archive_bucket: str,
    concurrency: int | None = None,
    delete_batch_size: int = _DELETE_BATCH_SIZE,
    on_copy_progress: Callable[[int, int, str, bool, bool], None] | None = None,
    on_item_progress: Callable[[str, int, int], None] | None = None,
    on_delete_progress: Callable[[int, int, str, list[dict]], None] | None = None,
) -> list[dict]:
    """Delete sources only after a signed archive download matches their full contents."""
    if archive_bucket != "scola-school-archives":
        raise ValueError("Source deletion requires archive bucket scola-school-archives")
    if any(bucket == archive_bucket for bucket, _ in items):
        raise ValueError("Archive bucket must never be used as a deletion source")
    resolved_source = source_provider or env.s3_default_provider_key

    copy_results = copy_objects(
        items,
        resolved_source,
        archive_provider,
        dest_bucket=archive_bucket,
        overwrite=False,
        # Keep transfer outcomes independent from unsupported visibility operations.
        make_public=False,
        concurrency=concurrency,
        on_progress=on_copy_progress,
        on_item_progress=on_item_progress,
        dest_key_fn=_archive_dest_key,
    )

    merged = []
    for (bucket, key), copy_result in zip(items, copy_results):
        merged.append(
            {
                "bucket": bucket,
                "key": key,
                "archiveBucket": copy_result.get("destBucket"),
                "archiveKey": copy_result.get("destKey"),
                "copySuccess": copy_result["success"],
                "copySkipped": copy_result.get("skipped", False),
                "copyError": copy_result.get("error"),
                "publicAccessStatus": "Private (presigned URL)",
                "publicAccessError": None,
                "deleteAttempted": False,
                "deleteSuccess": False,
                "deleteError": None,
            }
        )
    source_client = get_s3_client(resolved_source) if items else None
    archive_client = get_s3_client(archive_provider) if items else None
    for index, result in enumerate(merged):
        result["archiveVerified"] = False
        result["verificationError"] = None
        if not result["copySuccess"]:
            continue
        bucket, key = result["bucket"], result["key"]
        try:
            # Use the expected destination, never a URL supplied by the uploaded report.
            dest_key = _archive_dest_key(bucket, key)
            source_meta = source_client.head_object(Bucket=bucket, Key=key)
            result["sizeBytes"] = source_meta["ContentLength"]
            archive_meta = archive_client.head_object(Bucket=archive_bucket, Key=dest_key)
            if source_meta["ContentLength"] != archive_meta["ContentLength"]:
                raise ValueError("Archive size differs from source; source retained")
            source = source_client.get_object(Bucket=bucket, Key=key, IfMatch=source_meta["ETag"])
            def digest(stream):
                sha = hashlib.sha256()
                size = 0
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    sha.update(chunk)
                    size += len(chunk)
                return size, sha.digest()
            try:
                source_digest = digest(source["Body"])
            finally:
                source["Body"].close()
            signed_url = build_archive_presigned_url(archive_provider, archive_bucket, dest_key)
            with urlopen(Request(signed_url), timeout=60) as response:
                if response.status != 200:
                    raise ValueError("Archive download did not return HTTP 200")
                archive_digest = digest(response)
            if source_digest != archive_digest or source_digest[0] != source_meta["ContentLength"]:
                raise ValueError("Archive contents differ from source; source retained")
            current = source_client.head_object(Bucket=bucket, Key=key)
            if any(current.get(field) != source_meta.get(field) for field in ("ETag", "ContentLength", "LastModified", "VersionId")):
                raise ValueError("Source changed during verification; source retained")
            result["archiveVerified"] = True
        except Exception as error:
            # Do not persist exception text containing signed URL credentials.
            result["verificationError"] = f"Archive verification failed ({type(error).__name__}); source retained"
            continue
        result["deleteAttempted"] = True
        outcomes = delete_objects([(bucket, key)], resolved_source, batch_size=1)
        outcome = outcomes[0]
        result["deleteSuccess"] = outcome["success"]
        result["deleteError"] = outcome.get("error")
        if on_delete_progress:
            on_delete_progress(index + 1, len(merged), bucket, outcomes)
    return merged
