from app.services.archive_jobs import archive_jobs
from app.services.archive_summary import archive_size_summary
import asyncio
import io
import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import env
from app.providers.s3_provider import build_bucket_url, list_s3_providers
from app.services.excel_service import (
    ReportProgressCallback,
    build_report_file_name,
    buckets_label,
    generate_archive_deletion_report,
    generate_bucket_links_report,
    generate_bucket_objects_report,
    generate_copy_report,
    generate_storage_report,
    parse_copy_report_for_delete_details,
    parse_copy_report_for_db_update,
    parse_deletable_report,
    parse_deletion_excluded_report,
    parse_matched_report,
)
from app.services.mysql_service import list_databases, update_migrated_urls
from app.services.sse import sse_stream
from app.services.storage_service import (
    _DELETE_STREAM_BATCH_SIZE,
    archive_and_delete_objects,
    copy_objects,
    ensure_public_read_bucket_policy,
    get_storage_summary,
    is_region_mismatch_error,
    iterate_bucket_objects,
    list_buckets,
    list_objects_page,
    validate_public_destination_urls,
)
from app.types import BucketSummary, StorageObject, StorageSummary

router = APIRouter(prefix="/storage")

_DELETE_CONFIRM_PHRASE = "HAPUS"
_UPDATE_URL_CONFIRM_PHRASE = "UPDATE URL"


def _split_buckets(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [b.strip() for b in raw.split(",") if b.strip()]


@router.get("/providers")
async def get_providers():
    return {"status": True, "data": {"providers": list_s3_providers()}}


@router.get("/databases")
async def get_databases():
    databases = await asyncio.to_thread(list_databases)
    return {
        "status": True,
        "data": {"databases": databases, "count": len(databases), "defaultDatabase": env.db.database},
    }


@router.get("/buckets")
async def get_buckets(provider: str | None = Query(default=None), export: str | None = Query(default=None)):
    buckets = await asyncio.to_thread(list_buckets, provider)
    links = [{"bucket": b, "url": build_bucket_url(provider, b)} for b in buckets]

    report_file = None
    if export == "true":
        report_file = await asyncio.to_thread(
            generate_bucket_links_report, links, build_report_file_name(["bucket-links", provider])
        )

    return {
        "status": True,
        "data": {"buckets": buckets, "links": links, "count": len(buckets), "reportFile": report_file},
    }


@router.get("/summary")
async def get_summary(
    buckets: str | None = Query(default=None),
    prefix: str | None = Query(default=None),
    export: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    bucket_list = _split_buckets(buckets)
    summary = await asyncio.to_thread(
        get_storage_summary, bucket_list, prefix, provider, None
    )

    report_file = None
    if export == "true":
        report_file = await asyncio.to_thread(
            generate_storage_report, summary, build_report_file_name(["storage", buckets_label(bucket_list)])
        )

    return {
        "status": True,
        "data": {
            "buckets": [
                {
                    "bucket": b.bucket,
                    "objectCount": b.object_count,
                    "totalSize": b.total_size,
                    "error": b.error,
                }
                for b in summary.buckets
            ],
            "bucketCount": summary.bucket_count,
            "objectCount": summary.object_count,
            "totalSize": summary.total_size,
            "reportFile": report_file,
        },
    }


@router.get("/summary/stream")
async def get_summary_stream(
    buckets: str | None = Query(default=None),
    prefix: str | None = Query(default=None),
    export: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    bucket_list = _split_buckets(buckets)
    should_export = export == "true"
    report_file = build_report_file_name(["storage", buckets_label(bucket_list)]) if should_export else None

    def work(emit):
        completed_buckets: list[BucketSummary] = []

        def on_bucket_done(completed: int, total: int, bucket: BucketSummary) -> None:
            if not (bucket.error and is_region_mismatch_error(bucket.error)):
                completed_buckets.append(bucket)

            if should_export:
                partial = StorageSummary(
                    buckets=completed_buckets,
                    bucket_count=len(completed_buckets),
                    object_count=sum(b.object_count for b in completed_buckets),
                    total_size=sum(b.total_size for b in completed_buckets),
                )
                generate_storage_report(partial, report_file)

            emit(
                "progress",
                {
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100),
                    "bucket": bucket.bucket,
                    "objectCount": bucket.object_count,
                    "totalSize": bucket.total_size,
                    "error": bucket.error,
                    "reportFile": report_file,
                },
            )

        summary_final = get_storage_summary(bucket_list, prefix, provider, on_bucket_done)
        emit(
            "done",
            {
                "buckets": [
                    {"bucket": b.bucket, "objectCount": b.object_count, "totalSize": b.total_size, "error": b.error}
                    for b in summary_final.buckets
                ],
                "bucketCount": summary_final.bucket_count,
                "objectCount": summary_final.object_count,
                "totalSize": summary_final.total_size,
                "reportFile": report_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/{bucket}/objects")
async def get_objects(
    bucket: str,
    prefix: str | None = Query(default=None),
    maxKeys: str | None = Query(default=None),
    continuationToken: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    page = await asyncio.to_thread(
        list_objects_page,
        bucket,
        prefix,
        int(maxKeys) if maxKeys else None,
        continuationToken,
        provider,
    )
    return {
        "status": True,
        "data": {
            "objects": [
                {
                    "bucket": o.bucket,
                    "key": o.key,
                    "size": o.size,
                    "lastModified": o.last_modified.isoformat() if o.last_modified else None,
                    "etag": o.etag,
                }
                for o in page.objects
            ],
            "isTruncated": page.is_truncated,
            "nextContinuationToken": page.next_continuation_token,
        },
    }


@router.get("/{bucket}/objects/export/stream")
async def export_bucket_objects_stream(
    bucket: str,
    prefix: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    """
    Lists every object in one bucket and writes it to a downloadable .xlsx — Key, Size, Last
    Modified, and a clickable public URL per row. Streamed via SSE, same as every other
    potentially-slow scan in this app: a bucket can hold anywhere from a handful of objects to
    millions, so the scan itself reports progress before the report-writing phase gets a chance to.
    """

    def work(emit):
        objects: list[StorageObject] = []
        for obj in iterate_bucket_objects(bucket, prefix, provider):
            objects.append(obj)
            if len(objects) % 500 == 0:
                emit("progress", {"phase": "scan", "scanned": len(objects)})

        def on_report_progress(_phase: str, written: int, total: int, message: str, _extra: str | None) -> None:
            emit(
                "progress",
                {
                    "phase": "report",
                    "scanned": len(objects),
                    "completed": written,
                    "total": total,
                    "percent": round((written / total) * 100) if total else 100,
                    "message": message,
                },
            )

        report_file = generate_bucket_objects_report(bucket, objects, provider, on_progress=on_report_progress)
        emit("done", {"bucket": bucket, "count": len(objects), "reportFile": report_file})

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/parse-report")
async def parse_report(file: UploadFile = File(...)):
    """
    Reads a previously-downloaded Orphan/Cleanup Candidates .xlsx back in, so its rows can be
    shown with checkboxes in the delete UI — a human reviews and picks exactly which ones to
    remove, rather than any bulk "delete everything currently flagged" action.
    """
    content = await file.read()
    try:
        rows = await asyncio.to_thread(parse_deletable_report, io.BytesIO(content))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return {"status": True, "data": {"rows": rows, "count": len(rows)}}


@router.post("/parse-matched-report")
async def parse_matched_report_route(file: UploadFile = File(...)):
    """
    Reads a previously-downloaded reconciliation report's "Matched" sheet back in, so its rows
    can be shown with checkboxes in a copy-to-another-provider UI (e.g. migrating confirmed,
    in-use files from DO to Wasabi) — same upload-then-select pattern as /parse-report, just
    against files that are known-good rather than candidates for deletion.
    """
    content = await file.read()
    try:
        rows = await asyncio.to_thread(parse_matched_report, io.BytesIO(content))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return {"status": True, "data": {"rows": rows, "count": len(rows)}}


@router.post("/parse-copy-report")
async def parse_copy_report_route(file: UploadFile = File(...)):
    """Validates a storage-migration report and extracts safe database-update candidates."""
    content = await file.read()
    try:
        rows, stats = await asyncio.to_thread(parse_copy_report_for_db_update, io.BytesIO(content))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"status": True, "data": {"rows": rows, **stats}}


@router.post("/parse-copy-delete-report")
async def parse_copy_delete_report_route(file: UploadFile = File(...)):
    """
    Reads a storage-migration report's Copied sheet back in — the fast, network-free half of
    "Delete files already migrated", split out from the public-URL check (see
    /validate-destination-urls/stream) specifically so a slow or dropped link check never means
    re-uploading and re-parsing the file too: the client keeps this response around and only
    resends whichever candidate rows the link check hasn't confirmed yet.
    """
    content = await file.read()
    try:
        rows, stats, excluded = await asyncio.to_thread(parse_copy_report_for_delete_details, io.BytesIO(content))
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"status": True, "data": {"rows": rows, "excluded": excluded, **stats}}


def _categorize_validation_failure(error: str | None) -> str:
    error = str(error or "Unknown error")
    if error.startswith("HTTP 403"):
        return "accessDenied"
    if error.startswith("HTTP 404"):
        return "notFound"
    if error.startswith("Destination URL"):
        return "invalidUrl"
    return "connectionError"


class ValidateUrlItem(BaseModel):
    bucket: str
    key: str
    destinationUrl: str
    sizeMb: str | float | int | None = None
    status: str | None = None


class ValidateUrlsBody(BaseModel):
    items: list[ValidateUrlItem]


@router.post("/validate-destination-urls/stream")
async def validate_destination_urls_stream(body: ValidateUrlsBody):
    """
    Publicly verifies that every given destination URL is still readable — the slow,
    network-dependent half of "Delete files already migrated". Deliberately takes the candidate
    rows directly (rather than re-parsing an uploaded file) so a dropped connection partway
    through a large batch can be retried by resending only the items whose outcome never made it
    back to the client, instead of starting the whole check over from row one.

    Progress events carry each just-finished item's own (bucket, key, valid) outcome, batched
    rather than one per item (a report with tens of thousands of rows would otherwise mean tens
    of thousands of SSE frames) — but still fine-grained enough that a client can track exactly
    which items are confirmed done and compute "everything not yet accounted for" as the retry set.
    """
    items = [i.model_dump() for i in body.items]

    def work(emit):
        if not items:
            emit(
                "done",
                {"rows": [], "excludedRows": [], "eligible": 0, "destinationInvalid": 0, "validationFailures": {}},
            )
            return

        # Emitting every item would create tens of thousands of SSE frames for large reports.
        # Batch item outcomes and flush at roughly 100-200 visible steps, plus the final item —
        # frequent enough that a dropped connection loses at most a small, cheap-to-redo batch.
        tick_every = max(1, len(items) // 150)
        pending_results: list[dict] = []

        def on_progress(index: int, completed: int, total: int, valid: bool, error: str | None) -> None:
            pending_results.append(
                {"bucket": items[index]["bucket"], "key": items[index]["key"], "valid": valid, "error": error}
            )
            if completed == total or completed % tick_every == 0:
                emit(
                    "progress",
                    {
                        "completed": completed,
                        "total": total,
                        "percent": round((completed / total) * 100) if total else 100,
                        "results": pending_results,
                    },
                )
                pending_results.clear()

        valid_rows, validation_failures = validate_public_destination_urls(items, on_progress=on_progress)
        excluded_rows = [
            {**failure, "status": "Excluded", "reason": failure.get("error")}
            for failure in validation_failures
        ]
        safe_rows = [
            {key: row.get(key) for key in ("bucket", "key", "sizeMb", "status")}
            for row in valid_rows
        ]
        failure_counts: dict[str, int] = {}
        for failure in validation_failures:
            category = _categorize_validation_failure(failure.get("error"))
            failure_counts[category] = failure_counts.get(category, 0) + 1

        emit(
            "done",
            {
                "rows": safe_rows,
                "eligible": len(safe_rows),
                "destinationInvalid": len(validation_failures),
                "validationFailures": failure_counts,
                "excludedRows": excluded_rows,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/validate-deletion-excluded-report/stream")
async def validate_deletion_excluded_report_stream(file: UploadFile = File(...)):
    """
    Re-checks the "Excluded" sheet of a deletion report this app generated earlier: many
    exclusions there are transient (a connection timeout while checking the destination URL was
    publicly readable, not the file being genuinely unsafe to delete), so a later retry can turn
    out fine even though nothing about the file itself changed. Same public-URL check and
    eligible/excluded split as /validate-destination-urls/stream, just sourced from a deletion
    report's Excluded sheet instead of a migration report's Copied sheet.
    """
    content = await file.read()

    def work(emit):
        candidates, unretryable = parse_deletion_excluded_report(io.BytesIO(content))
        total = len(candidates) + len(unretryable)
        emit(
            "parsed",
            {"total": total, "eligible": len(candidates), "unretryable": len(unretryable)},
        )

        # Emitting every item would create tens of thousands of SSE frames for large reports.
        # Update at roughly 100 visible steps, plus the final item.
        tick_every = max(1, len(candidates) // 100)

        def on_progress(_index: int, completed: int, total: int, valid: bool, error: str | None) -> None:
            if completed == total or completed % tick_every == 0:
                emit(
                    "progress",
                    {
                        "completed": completed,
                        "total": total,
                        "percent": round((completed / total) * 100) if total else 100,
                        "lastValid": valid,
                        "lastError": error,
                    },
                )

        valid_rows, validation_failures = validate_public_destination_urls(candidates, on_progress=on_progress)
        excluded_rows = unretryable + [
            {**failure, "status": "Excluded", "reason": failure.get("error")}
            for failure in validation_failures
        ]
        safe_rows = [
            {key: row.get(key) for key in ("bucket", "key", "status")}
            for row in valid_rows
        ]
        failure_counts: dict[str, int] = {}
        for failure in validation_failures:
            category = _categorize_validation_failure(failure.get("error"))
            failure_counts[category] = failure_counts.get(category, 0) + 1

        emit(
            "done",
            {
                "rows": safe_rows,
                "eligible": len(safe_rows),
                "stillExcluded": len(excluded_rows),
                "validationFailures": failure_counts,
                "excludedRows": excluded_rows,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


class DeleteItem(BaseModel):
    bucket: str
    key: str
    path: str | None = None
    # Purely informational (goes into the audit log, never used to decide what gets deleted —
    # only bucket/key drive that), so it stays untyped rather than a strict float: a report a
    # human hand-edited in Excel can easily carry "19 MB" as literal cell text instead of a
    # clean number, and that shouldn't be able to block an otherwise-valid delete request.
    sizeMb: str | float | int | None = None


class DeleteBody(BaseModel):
    items: list[DeleteItem]
    excluded: list[dict] = Field(default_factory=list)
    provider: str | None = None
    confirm: str

    archiveJobId: str | None = None


def _write_deletion_audit_log(
    provider: str | None,
    requested: list[dict],
    results: list[dict],
    excluded: list[dict] | None = None,
) -> str:
    audit_dir = os.path.join(env.reports_dir, ".deletions")
    os.makedirs(audit_dir, exist_ok=True)
    audit_path = os.path.join(audit_dir, f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "provider": provider,
                "archiveProvider": env.archive_provider,
                "archiveBucket": env.archive_bucket,
                "requested": requested,
                "results": results,
                "excluded": excluded or [],
            },
            f,
            indent=2,
        )
    return os.path.basename(audit_path)


def _validate_delete_body(body: DeleteBody) -> None:
    if len(body.items) == 0:
        raise HTTPException(status_code=400, detail="No items to delete")
    if body.confirm != _DELETE_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400, detail=f'Confirmation phrase must be exactly "{_DELETE_CONFIRM_PHRASE}"'
        )


def _archive_deletion_counts(results: list[dict]) -> dict:
    deleted = sum(1 for r in results if r["deleteSuccess"])
    copy_failed = sum(1 for r in results if not r["copySuccess"])
    archived_not_deleted = sum(1 for r in results if r["copySuccess"] and not r["deleteSuccess"])
    return {
        "deletedCount": deleted,
        "copyFailedCount": copy_failed,
        "archivedNotDeletedCount": archived_not_deleted,
        "publicAccessWarningCount": sum(1 for r in results if r.get("publicAccessError")),
    }


@router.post("/delete")
async def delete(body: DeleteBody):
    """Archive report objects and delete sources only after signed-download verification."""
    _validate_delete_body(body)

    # Best-effort: the archive bucket may have ACLs disabled entirely (see
    # ensure_public_read_bucket_policy), in which case a bucket policy is the only way its objects
    # can be made publicly readable at all. Never blocks archiving on failure — a policy
    # that can't be set just means the report's archive links may not resolve until it's fixed.
    policy_error = None  # Private archives use signed links; no public policy needed.

    items = [(i.bucket, i.key) for i in body.items]
    results = await asyncio.to_thread(
        archive_and_delete_objects, items, body.provider, env.archive_provider, env.archive_bucket
    )
    audit_file = _write_deletion_audit_log(
        body.provider, [i.model_dump() for i in body.items], results, body.excluded
    )
    report_file = await asyncio.to_thread(
        generate_archive_deletion_report,
        [i.model_dump() for i in body.items],
        results,
        body.excluded,
        env.archive_provider,
    )

    return {
        "status": True,
        "data": {
            "results": results,
            **_archive_deletion_counts(results),
            "sizeSummary": archive_size_summary(results, [i.model_dump() for i in body.items], body.excluded or []),
            "auditFile": audit_file,
            "reportFile": report_file,
            "archiveBucketPolicyError": policy_error,
        },
    }


@router.post("/delete/stream")
async def delete_stream(body: DeleteBody):
    """Stream archiving and deletion after signed-download verification."""
    _validate_delete_body(body)

    def work(emit):
        items = [(i.bucket, i.key) for i in body.items]
        total = len(items)

        def on_copy_progress(completed: int, total_: int, key: str, success: bool, skipped: bool) -> None:
            emit(
                "copy_progress",
                {
                    "completed": completed,
                    "total": total_,
                    "percent": round((completed / total_) * 100) if total_ else 100,
                    "key": key,
                    "success": success,
                    "skipped": skipped,
                },
            )

        def on_delete_progress(completed: int, total_: int, bucket: str, batch_results: list[dict]) -> None:
            emit(
                "delete_progress",
                {
                    "completed": completed,
                    "total": total_,
                    "percent": round((completed / total_) * 100) if total_ else 100,
                    "bucket": bucket,
                    "results": batch_results,
                },
            )

        # Best-effort: see the matching comment in POST /delete. Never blocks archiving.
        policy_error = None  # Private archives use signed links.

        emit("archive_phase", {"phase": "prepare", "message": "Menyiapkan arsip dan mengecek file yang sudah ada..."})
        results = archive_and_delete_objects(
            items, body.provider, env.archive_provider, env.archive_bucket,
            delete_batch_size=_DELETE_STREAM_BATCH_SIZE,
            on_copy_progress=on_copy_progress, on_delete_progress=on_delete_progress,
            on_item_progress=lambda key, transferred, total_bytes: emit("archive_bytes", {
                "key": key, "transferred": transferred, "totalBytes": total_bytes}),
            on_archive_progress=lambda event: emit("archive_progress", event),
        )
        audit_file = _write_deletion_audit_log(
            body.provider, [i.model_dump() for i in body.items], results, body.excluded
        )
        emit("archive_phase", {"phase": "report", "message": "Membuat report dan presigned URL..."})
        report_file = generate_archive_deletion_report(
            [i.model_dump() for i in body.items], results, body.excluded, env.archive_provider,
        )

        emit(
            "done",
            {
                "results": results,
                **_archive_deletion_counts(results),
            "sizeSummary": archive_size_summary(results, [i.model_dump() for i in body.items], body.excluded or []),
                "auditFile": audit_file,
                "reportFile": report_file,
                "archiveBucketPolicyError": policy_error,
            },
        )

    try:
        job = archive_jobs.connect(
            body.provider or env.s3_default_provider_key,
            [(item.bucket, item.key) for item in body.items], work, body.archiveJobId)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return StreamingResponse(
        archive_jobs.stream(job),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


class CopyItem(BaseModel):
    bucket: str
    key: str
    # Purely informational, same reasoning as DeleteItem.sizeMb — shown in the generated report
    # and carried into the audit log, never used to decide what gets copied.
    sizeMb: str | float | int | None = None
    path: str | None = None
    # Database-reference metadata from the uploaded reconciliation report. It does not affect
    # the object copy itself, but is preserved in both the audit log and migration report so a
    # migrated object can still be traced back to the exact source row.
    table: str | None = None
    column: str | None = None
    rowId: str | float | int | None = None


class CopyBody(BaseModel):
    items: list[CopyItem]
    sourceProvider: str
    destProvider: str
    # Sends every item into this one bucket regardless of its source bucket name. Omit to keep
    # each item's own source bucket name on the destination side too (the common case: a
    # tenant's bucket name is unchanged by a DO -> Wasabi migration).
    destBucket: str | None = None
    # False (default): an item already present at the destination is left alone instead of
    # re-transferred — so re-uploading the same report (or one that overlaps a previous run)
    # doesn't redo already-finished copies. True forces every item to be copied again regardless.
    overwrite: bool = False
    # True (default): uploads with ACL=public-read, since a GetObject+PutObject copy never
    # preserves the source's own ACL — without this, files that were publicly readable on the
    # source silently become private (default ACL) on the destination and every link to them breaks.
    makePublic: bool = True


class DatabaseUrlUpdateItem(BaseModel):
    sourcePath: str
    destinationUrl: str
    table: str
    column: str
    rowId: str | float | int


class DatabaseUrlUpdateBody(BaseModel):
    database: str
    items: list[DatabaseUrlUpdateItem]
    confirm: str


def _validate_copy_body(body: CopyBody) -> None:
    if len(body.items) == 0:
        raise HTTPException(status_code=400, detail="No items to copy")
    if body.sourceProvider == body.destProvider:
        raise HTTPException(status_code=400, detail="Source and destination provider must be different")


def _write_copy_audit_log(
    source_provider: str,
    dest_provider: str,
    dest_bucket: str | None,
    requested: list[dict],
    results: list[dict],
    report_file: str | None,
) -> str:
    audit_dir = os.path.join(env.reports_dir, ".copies")
    os.makedirs(audit_dir, exist_ok=True)
    audit_path = os.path.join(audit_dir, f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sourceProvider": source_provider,
                "destProvider": dest_provider,
                "destBucket": dest_bucket,
                "reportFile": report_file,
                "requested": requested,
                "results": results,
            },
            f,
            indent=2,
        )
    return os.path.basename(audit_path)


def _build_copy_report(
    body: CopyBody, results: list[dict], on_progress: ReportProgressCallback | None = None
) -> str:
    entries = []
    for item, result in zip(body.items, results):
        status = "Failed" if not result["success"] else ("Skipped" if result.get("skipped") else "Copied")
        entries.append(
            {
                "sourcePath": item.path,
                "bucket": result["bucket"],
                "key": result["key"],
                "destBucket": result.get("destBucket"),
                "sizeMb": item.sizeMb,
                "table": item.table,
                "column": item.column,
                "rowId": item.rowId,
                "status": status,
                "error": result.get("error"),
            }
        )
    return generate_copy_report(
        entries, body.destProvider, build_report_file_name(["copy", body.sourceProvider, body.destProvider]),
        on_progress=on_progress,
    )


@router.post("/copy")
async def copy(body: CopyBody):
    """
    Copies the given objects from sourceProvider to destProvider (e.g. DO -> Wasabi). Never
    touches the source — this is meant for migrating files that are still in active use (the
    "Matched" list, not orphans), ahead of the app's own DB references being repointed at the
    new provider separately. Every request is written to reports/.copies/ as an audit trail, and
    an .xlsx report is generated the same way every other report in this app is — one row per
    item, with the new Destination URL so you can see exactly what now lives where on Wasabi.
    """
    _validate_copy_body(body)

    items = [(i.bucket, i.key) for i in body.items]
    results = await asyncio.to_thread(
        copy_objects,
        items, body.sourceProvider, body.destProvider, body.destBucket, body.overwrite, body.makePublic,
    )
    report_file = await asyncio.to_thread(_build_copy_report, body, results)
    audit_file = _write_copy_audit_log(
        body.sourceProvider, body.destProvider, body.destBucket, [i.model_dump() for i in body.items], results, report_file
    )

    succeeded = sum(1 for r in results if r["success"])
    skipped = sum(1 for r in results if r.get("skipped"))
    return {
        "status": True,
        "data": {
            "results": results,
            "succeededCount": succeeded,
            "skippedCount": skipped,
            "failedCount": len(results) - succeeded,
            "auditFile": audit_file,
            "reportFile": report_file,
        },
    }


@router.post("/copy/stream")
async def copy_stream(body: CopyBody):
    """
    Same cross-provider copy as POST /copy, but reports progress as each object finishes instead
    of one blocking call for the whole batch — copying streams full object bytes through the app
    (there's no server-side CopyObject across providers), so this can take a while for a large
    selection. Also emits `item_progress` events mid-transfer (bytes uploaded so far for whichever
    file is currently in flight) — `progress` alone only moves once a whole file finishes, which
    for one large file (or a small selection) means no feedback at all until it's already done.
    Same audit trail and .xlsx report as the non-streaming endpoint.
    """
    _validate_copy_body(body)

    def work(emit):
        items = [(i.bucket, i.key) for i in body.items]

        def on_progress(completed: int, total: int, key: str, success: bool, skipped: bool) -> None:
            emit(
                "progress",
                {
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 100,
                    "key": key,
                    "success": success,
                    "skipped": skipped,
                },
            )

        def on_item_progress(key: str, bytes_transferred: int, total_bytes: int) -> None:
            emit(
                "item_progress",
                {
                    "key": key,
                    "bytesTransferred": bytes_transferred,
                    "totalBytes": total_bytes,
                    "percent": round((bytes_transferred / total_bytes) * 100) if total_bytes else 100,
                },
            )

        results = copy_objects(
            items, body.sourceProvider, body.destProvider, body.destBucket, body.overwrite, body.makePublic,
            on_progress=on_progress, on_item_progress=on_item_progress,
        )

        # Every item is done at this point, but generating the .xlsx (and, for a very large
        # run, writing it to disk) is not instant — without an explicit signal here, the UI has
        # nothing to show between the last `progress` event and `done` except a bar stuck at 100%.
        def on_report_progress(_phase: str, written: int, total: int, message: str, _extra: str | None) -> None:
            emit(
                "report_progress",
                {
                    "completed": written,
                    "total": total,
                    "percent": round((written / total) * 100) if total else 100,
                    "message": message,
                },
            )

        on_report_progress("report", 0, len(results), "Generating Excel report…", None)
        report_file = _build_copy_report(body, results, on_report_progress)
        audit_file = _write_copy_audit_log(
            body.sourceProvider, body.destProvider, body.destBucket, [i.model_dump() for i in body.items], results, report_file
        )

        succeeded = sum(1 for r in results if r["success"])
        skipped_count = sum(1 for r in results if r.get("skipped"))
        emit(
            "done",
            {
                "results": results,
                "succeededCount": succeeded,
                "skippedCount": skipped_count,
                "failedCount": len(results) - succeeded,
                "auditFile": audit_file,
                "reportFile": report_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/update-database-urls/stream")
async def update_database_urls_stream(body: DatabaseUrlUpdateBody):
    """
    Repoints DB file URLs from a completed copy report. Every row is guarded (see
    update_migrated_urls) and commits independently, so a run that gets cut off partway can be
    resumed by resending only the rows that never got a `progress` event back.
    """
    if body.confirm != _UPDATE_URL_CONFIRM_PHRASE:
        raise HTTPException(
            status_code=400,
            detail=f'Type "{_UPDATE_URL_CONFIRM_PHRASE}" exactly to confirm database updates',
        )
    if not body.database.strip():
        raise HTTPException(status_code=400, detail="Database name is required")
    if not body.items:
        raise HTTPException(status_code=400, detail="No eligible migration rows to update")

    def work(emit):
        def on_progress(completed: int, total: int, outcome: str) -> None:
            emit(
                "progress",
                {
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 100,
                    "outcome": outcome,
                },
            )

        counts = update_migrated_urls(
            body.database.strip(),
            [item.model_dump() for item in body.items],
            on_progress,
        )
        emit("done", counts)

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


def _copies_dir() -> str:
    return os.path.join(env.reports_dir, ".copies")


def _safe_audit_path(directory: str, filename: str) -> str:
    """Rejects anything but a bare filename (no `..`, no path separators) before it touches
    the filesystem — filename comes straight from the URL path, and this file is later opened
    and its contents returned to the caller, so path traversal here would be a real read gadget."""
    if filename != os.path.basename(filename) or not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid audit file name")
    path = os.path.join(directory, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Audit file not found")
    return path


@router.get("/copies")
async def list_copies():
    """
    Lists past /storage/copy(/stream) runs from their reports/.copies/ audit trail, newest
    first — the entry point for "delete what I already moved to Wasabi": pick a run here, then
    GET /storage/copies/{filename} for the exact items it copied successfully.
    """
    directory = _copies_dir()
    if not os.path.isdir(directory):
        return {"status": True, "data": {"copies": []}}

    entries = []
    for name in os.listdir(directory):
        if not name.endswith(".json"):
            continue
        path = os.path.join(directory, name)
        try:
            with open(path, encoding="utf-8") as f:
                record = json.load(f)
        except (OSError, json.JSONDecodeError):  # noqa: PERF203 - a bad file shouldn't hide the rest
            continue
        results = record.get("results") or []
        entries.append(
            {
                "file": name,
                "timestamp": record.get("timestamp"),
                "sourceProvider": record.get("sourceProvider"),
                "destProvider": record.get("destProvider"),
                "destBucket": record.get("destBucket"),
                "reportFile": record.get("reportFile"),
                "itemCount": len(results),
                "succeededCount": sum(1 for r in results if r.get("success")),
                "skippedCount": sum(1 for r in results if r.get("skipped")),
                "failedCount": sum(1 for r in results if not r.get("success")),
            }
        )

    entries.sort(key=lambda e: e["timestamp"] or "", reverse=True)
    return {"status": True, "data": {"copies": entries}}


@router.get("/copies/{filename}")
async def get_copy(filename: str):
    """
    Reads one copy run's full audit record back, split into `succeeded` (safe to feed straight
    into POST /storage/delete with provider=sourceProvider from this same record, to remove the
    now-migrated originals from DO) and `failed` (left alone — never present a partially-copied
    object as deletable). Deleting is still a separate, explicit step through the existing
    delete flow — this endpoint only reads the copy record, it never deletes anything itself.
    """
    path = _safe_audit_path(_copies_dir(), filename)
    try:
        with open(path, encoding="utf-8") as f:
            record = json.load(f)
    except (OSError, json.JSONDecodeError) as error:
        raise HTTPException(status_code=500, detail=f"Could not read audit file: {error}") from error

    results = record.get("results") or []
    succeeded = [
        {"bucket": r["bucket"], "key": r["key"], "destBucket": r.get("destBucket")}
        for r in results
        if r.get("success")
    ]
    failed = [r for r in results if not r.get("success")]

    return {
        "status": True,
        "data": {
            "file": filename,
            "timestamp": record.get("timestamp"),
            "sourceProvider": record.get("sourceProvider"),
            "destProvider": record.get("destProvider"),
            "destBucket": record.get("destBucket"),
            "reportFile": record.get("reportFile"),
            "succeeded": succeeded,
            "failed": failed,
        },
    }
