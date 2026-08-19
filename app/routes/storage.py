import asyncio

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.providers.s3_provider import list_s3_providers
from app.services.excel_service import build_report_file_name, buckets_label, generate_storage_report
from app.services.sse import sse_stream
from app.services.storage_service import (
    get_storage_summary,
    is_region_mismatch_error,
    list_buckets,
    list_objects_page,
)
from app.types import BucketSummary, StorageSummary

router = APIRouter(prefix="/storage")


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
