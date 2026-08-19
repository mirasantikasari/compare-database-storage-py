import asyncio
import os

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from app.config import env
from app.services.excel_service import build_report_file_name, buckets_label, generate_reconciliation_report
from app.services.mysql_service import discover_file_columns
from app.services.reconciliation_service import run_reconciliation
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
        emit(
            "discovered",
            {
                "mappingCount": len(mappings),
                "mappings": [f"{m.table}.{m.column}" for m in mappings],
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
                    "percent": round((completed / total) * 100),
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
        result = run_reconciliation(request, on_progress)

        generate_reconciliation_report(result, report_file, provider)
        emit(
            "done",
            {
                "summary": {
                    "matchedCount": result.summary.matched_count,
                    "missingCount": result.summary.missing_count,
                    "orphanCount": result.summary.orphan_count,
                    "databaseFileCount": result.summary.database_file_count,
                    "storageObjectCount": result.summary.storage_object_count,
                    "otherProviderCount": result.summary.other_provider_count,
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


@router.post("/reconciliation/stream")
async def reconciliation_stream(body: ReconciliationBody):
    if len(body.mappings) == 0:
        raise HTTPException(status_code=400, detail="At least one table/column mapping is required")

    report_file = build_report_file_name([body.database, buckets_label(body.buckets)])

    def work(emit):
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
        result = run_reconciliation(request, on_progress)

        generate_reconciliation_report(result, report_file, body.provider)
        emit(
            "done",
            {
                "summary": {
                    "matchedCount": result.summary.matched_count,
                    "missingCount": result.summary.missing_count,
                    "orphanCount": result.summary.orphan_count,
                    "databaseFileCount": result.summary.database_file_count,
                    "storageObjectCount": result.summary.storage_object_count,
                    "otherProviderCount": result.summary.other_provider_count,
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

    request = ReconciliationRequest(
        buckets=body.buckets,
        prefix=body.prefix,
        mappings=[
            TableColumnMapping(table=m.table, column=m.column, id_column=m.idColumn) for m in body.mappings
        ],
        database=body.database,
        provider=body.provider,
    )
    result = await asyncio.to_thread(run_reconciliation, request, None)
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
                "databaseFileCount": result.summary.database_file_count,
                "storageObjectCount": result.summary.storage_object_count,
                "otherProviderCount": result.summary.other_provider_count,
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
