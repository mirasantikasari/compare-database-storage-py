import asyncio
import io
import json
import os
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import env
from app.providers.s3_provider import list_s3_providers
from app.services.excel_service import (
    build_report_file_name,
    buckets_label,
    generate_storage_report,
    parse_deletable_report,
)
from app.services.sse import sse_stream
from app.services.storage_service import (
    _DELETE_STREAM_BATCH_SIZE,
    delete_objects,
    get_storage_summary,
    is_region_mismatch_error,
    list_buckets,
    list_objects_page,
)
from app.types import BucketSummary, StorageSummary

router = APIRouter(prefix="/storage")

_DELETE_CONFIRM_PHRASE = "HAPUS"


def _split_buckets(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [b.strip() for b in raw.split(",") if b.strip()]


@router.get("/providers")
async def get_providers():
    return {"status": True, "data": {"providers": list_s3_providers()}}


@router.get("/buckets")
async def get_buckets(provider: str | None = Query(default=None)):
    buckets = await asyncio.to_thread(list_buckets, provider)
    return {"status": True, "data": {"buckets": buckets, "count": len(buckets)}}


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
    provider: str | None = None
    confirm: str


def _write_deletion_audit_log(provider: str | None, requested: list[dict], results: list[dict]) -> str:
    audit_dir = os.path.join(env.reports_dir, ".deletions")
    os.makedirs(audit_dir, exist_ok=True)
    audit_path = os.path.join(audit_dir, f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "provider": provider,
                "requested": requested,
                "results": results,
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


@router.post("/delete")
async def delete(body: DeleteBody):
    """
    Permanently deletes the given objects. Irreversible, so this refuses to run unless `confirm`
    is the exact phrase the UI makes a human type out by hand first — a request built by hand or
    replayed from a saved payload without that phrase gets rejected before anything is deleted.
    Every successful attempt is written to reports/.deletions/ as an audit trail, since there is
    no undo for whatever this deletes.
    """
    _validate_delete_body(body)

    items = [(i.bucket, i.key) for i in body.items]
    results = await asyncio.to_thread(delete_objects, items, body.provider)
    audit_file = _write_deletion_audit_log(body.provider, [i.model_dump() for i in body.items], results)

    succeeded = sum(1 for r in results if r["success"])
    return {
        "status": True,
        "data": {
            "results": results,
            "succeededCount": succeeded,
            "failedCount": len(results) - succeeded,
            "auditFile": audit_file,
        },
    }


@router.post("/delete/stream")
async def delete_stream(body: DeleteBody):
    """
    Same permanent-delete-with-confirmation as POST /delete, but reports progress as it goes
    (batches of _DELETE_STREAM_BATCH_SIZE, not the whole request in one blocking call) — for
    watching a larger selection get deleted without the UI just sitting there with no feedback
    until it's entirely done. Same audit trail as the non-streaming endpoint.
    """
    _validate_delete_body(body)

    def work(emit):
        items = [(i.bucket, i.key) for i in body.items]

        def on_progress(completed: int, total: int, bucket: str) -> None:
            emit(
                "progress",
                {
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 100,
                    "bucket": bucket,
                },
            )

        results = delete_objects(
            items, body.provider, batch_size=_DELETE_STREAM_BATCH_SIZE, on_progress=on_progress
        )
        audit_file = _write_deletion_audit_log(body.provider, [i.model_dump() for i in body.items], results)

        succeeded = sum(1 for r in results if r["success"])
        emit(
            "done",
            {
                "results": results,
                "succeededCount": succeeded,
                "failedCount": len(results) - succeeded,
                "auditFile": audit_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
