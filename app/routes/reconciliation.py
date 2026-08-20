import asyncio
import os

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from app.config import env
from app.services.excel_service import (
    build_report_file_name,
    buckets_label,
    generate_do_cleanup_report,
    generate_reconciliation_report,
)
from app.services.mysql_service import discover_content_columns, discover_file_columns
from app.services.reconciliation_service import run_do_cleanup_scan, run_reconciliation
from app.services.sse import sse_stream
from app.types import ReconciliationRequest, TableColumnMapping

router = APIRouter()


class MappingBody(BaseModel):
    table: str
    column: str
    idColumn: str | None = None


class ReconciliationBody(BaseModel):
    buckets: list[str] | None = None
    prefix: str | None = None
    mappings: list[MappingBody]
    database: str | None = None
    provider: str | None = None


@router.get("/reconciliation/auto/stream")
async def reconciliation_auto_stream(
    database: str = Query(...),
    buckets: str | None = Query(default=None),
    prefix: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    bucket_list = [b.strip() for b in buckets.split(",") if b.strip()] if buckets else None
    # Computed upfront (depends only on the request, not the scan's outcome) and sent with
    # every event from here on, so a client that stalls or disconnects mid-scan still knows
    # which filename to poll GET /reports/{filename} for — the scan itself keeps running
    # server-side regardless of whether anyone is still listening.
    report_file = build_report_file_name([database, buckets_label(bucket_list)])

    def work(emit):
        mappings = discover_file_columns(database)
        # Cross-checked against every Orphan candidate below (see run_reconciliation's
        # content_mappings) so a file still referenced only from inside a rich-text/JSON
        # column — a CKEditor/CKFinder upload embedded as an <img> in a lesson body, say — never
        # gets reported as Orphan just because it has no dedicated path column of its own. This
        # is what "Orphan" being a trustworthy basis for an outside-the-system delete depends on.
        content_mappings = discover_content_columns(database)
        emit(
            "discovered",
            {
                "mappingCount": len(mappings),
                "mappings": [f"{m.table}.{m.column}" for m in mappings],
                "contentMappingCount": len(content_mappings),
                "reportFile": report_file,
            },
        )

        if not mappings:
            raise RuntimeError(f'No candidate file-reference columns found in database "{database}"')

        def on_progress(phase, completed, total, label, error):
            emit(
                "progress",
                {
                    "phase": phase,
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 100,
                    "label": label,
                    "error": error,
                    "reportFile": report_file,
                },
            )

        request = ReconciliationRequest(
            buckets=bucket_list,
            prefix=prefix,
            mappings=mappings,
            database=database,
            provider=provider,
        )
        result = run_reconciliation(request, on_progress, content_mappings=content_mappings)

        generate_reconciliation_report(result, report_file, provider, on_progress)
        emit(
            "done",
            {
                "summary": {
                    "matchedCount": result.summary.matched_count,
                    "missingCount": result.summary.missing_count,
                    "orphanCount": result.summary.orphan_count,
                    "protectedCount": result.summary.protected_count,
                    "databaseFileCount": result.summary.database_file_count,
                    "contentReferenceCount": result.summary.content_reference_count,
                    "storageObjectCount": result.summary.storage_object_count,
                    "otherProviderCount": result.summary.other_provider_count,
                    "differentProviderCount": result.summary.different_provider_count,
                },
                "mappings": len(mappings),
                "reportFile": report_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/reconciliation/do-cleanup/stream")
async def do_cleanup_stream(
    database: str = Query(...),
    buckets: str | None = Query(default=None),
    prefix: str | None = Query(default=None),
    provider: str | None = Query(default=None),
):
    """
    Finds Object Storage objects that are candidates for cleanup: unreferenced by any discovered
    file-path column AND not found embedded inside any rich-text/HTML column either (which is
    where CKEditor/CKFinder uploads actually live — as an <img>/<a> inside a big content blob,
    not a dedicated path column). Only ever produces an .xlsx report for manual review; nothing
    is deleted here.
    """
    bucket_list = [b.strip() for b in buckets.split(",") if b.strip()] if buckets else None
    report_file = build_report_file_name([database, buckets_label(bucket_list), "do-cleanup"])

    def work(emit):
        file_mappings = discover_file_columns(database)
        content_mappings = discover_content_columns(database)
        emit(
            "discovered",
            {
                "fileMappingCount": len(file_mappings),
                "contentMappingCount": len(content_mappings),
                "reportFile": report_file,
            },
        )

        if not file_mappings and not content_mappings:
            raise RuntimeError(f'No candidate file-reference or rich-text columns found in database "{database}"')

        def on_progress(phase, completed, total, label, error):
            emit(
                "progress",
                {
                    "phase": phase,
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 100,
                    "label": label,
                    "error": error,
                    "reportFile": report_file,
                },
            )

        result = run_do_cleanup_scan(
            database, file_mappings, content_mappings, bucket_list, prefix, provider, on_progress
        )

        generate_do_cleanup_report(result, report_file, provider, on_progress)
        emit(
            "done",
            {
                "summary": {
                    "candidateCount": result.summary.candidate_count,
                    "protectedCount": result.summary.protected_count,
                    "orphanCount": result.summary.orphan_count,
                    "matchedCount": result.summary.matched_count,
                    "missingCount": result.summary.missing_count,
                    "databaseFileCount": result.summary.database_file_count,
                    "contentReferenceCount": result.summary.content_reference_count,
                    "storageObjectCount": result.summary.storage_object_count,
                    "otherProviderCount": result.summary.other_provider_count,
                    "differentProviderCount": result.summary.different_provider_count,
                },
                "reportFile": report_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/reconciliation/stream")
async def reconciliation_stream(body: ReconciliationBody):
    if len(body.mappings) == 0:
        raise HTTPException(status_code=400, detail="At least one table/column mapping is required")

    report_file = build_report_file_name([body.database, buckets_label(body.buckets)])

    def work(emit):
        # Same Orphan-vs-Protected cross-check as Auto reconciliation, whenever a database is
        # given (it's optional here since mappings can be typed by hand) — otherwise there's no
        # schema to introspect for rich-text/JSON columns and Orphan falls back to the plain
        # "not in the file-path columns I was given" check.
        content_mappings = discover_content_columns(body.database) if body.database else []

        def on_progress(phase, completed, total, label, error):
            emit(
                "progress",
                {
                    "phase": phase,
                    "completed": completed,
                    "total": total,
                    "percent": round((completed / total) * 100) if total else 0,
                    "label": label,
                    "error": error,
                    "reportFile": report_file,
                },
            )

        request = ReconciliationRequest(
            buckets=body.buckets,
            prefix=body.prefix,
            mappings=[
                TableColumnMapping(table=m.table, column=m.column, id_column=m.idColumn) for m in body.mappings
            ],
            database=body.database,
            provider=body.provider,
        )
        result = run_reconciliation(request, on_progress, content_mappings=content_mappings)

        generate_reconciliation_report(result, report_file, body.provider, on_progress)
        emit(
            "done",
            {
                "summary": {
                    "matchedCount": result.summary.matched_count,
                    "missingCount": result.summary.missing_count,
                    "orphanCount": result.summary.orphan_count,
                    "protectedCount": result.summary.protected_count,
                    "databaseFileCount": result.summary.database_file_count,
                    "contentReferenceCount": result.summary.content_reference_count,
                    "storageObjectCount": result.summary.storage_object_count,
                    "otherProviderCount": result.summary.other_provider_count,
                    "differentProviderCount": result.summary.different_provider_count,
                },
                "reportFile": report_file,
            },
        )

    return StreamingResponse(
        sse_stream(work),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/reconciliation")
async def reconciliation_post(body: ReconciliationBody):
    if len(body.mappings) == 0:
        raise HTTPException(status_code=400, detail="At least one table/column mapping is required")

    content_mappings = (
        await asyncio.to_thread(discover_content_columns, body.database) if body.database else []
    )

    request = ReconciliationRequest(
        buckets=body.buckets,
        prefix=body.prefix,
        mappings=[
            TableColumnMapping(table=m.table, column=m.column, id_column=m.idColumn) for m in body.mappings
        ],
        database=body.database,
        provider=body.provider,
    )
    result = await asyncio.to_thread(run_reconciliation, request, None, content_mappings)
    report_file = await asyncio.to_thread(
        generate_reconciliation_report,
        result,
        build_report_file_name([body.database, buckets_label(body.buckets)]),
        body.provider,
    )

    return {
        "status": True,
        "data": {
            "summary": {
                "matchedCount": result.summary.matched_count,
                "missingCount": result.summary.missing_count,
                "orphanCount": result.summary.orphan_count,
                "protectedCount": result.summary.protected_count,
                "databaseFileCount": result.summary.database_file_count,
                "contentReferenceCount": result.summary.content_reference_count,
                "storageObjectCount": result.summary.storage_object_count,
                "otherProviderCount": result.summary.other_provider_count,
                "differentProviderCount": result.summary.different_provider_count,
            },
            "reportFile": report_file,
        },
    }


@router.get("/reports/{filename}")
async def get_report(filename: str):
    safe_name = os.path.basename(filename)
    file_path = os.path.join(env.reports_dir, safe_name)

    if not safe_name.endswith(".xlsx") or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Report not found")

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=safe_name,
    )
