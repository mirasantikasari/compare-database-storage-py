import os
import re
import tempfile
import uuid
from datetime import date, datetime

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.worksheet.worksheet import Worksheet

from app.config import env
from app.providers.s3_provider import build_object_url
from app.types import ReconciliationResult, StorageSummary

_BOLD = Font(bold=True)
_RED = Font(color="FFCC0000")


def _format_bytes(num_bytes: float) -> str:
    if num_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    exponent = 0
    value = float(num_bytes)
    while value >= 1024 and exponent < len(units) - 1:
        value /= 1024
        exponent += 1
    return f"{value:.2f} {units[exponent]}"


def _mb(num_bytes: float) -> float:
    return round(num_bytes / (1024 * 1024), 2)


def _gb(num_bytes: float) -> float:
    return round(num_bytes / (1024 * 1024 * 1024), 2)


def _naive(dt: datetime | None) -> datetime | None:
    """openpyxl/Excel cells can't hold a timezone-aware datetime ('Excel does not support
    timezones in datetimes') — boto3 always returns LastModified as UTC-aware, so this strips
    tzinfo while keeping the same UTC wall-clock value."""
    return dt.replace(tzinfo=None) if dt is not None else None


def _style_header_row(sheet: Worksheet) -> None:
    for cell in sheet[1]:
        cell.font = _BOLD


def _today_date_stamp() -> str:
    return date.today().isoformat()


def buckets_label(buckets: list[str] | None) -> str:
    """One bucket -> its name; several -> joined; none/"all buckets" -> "all-buckets"."""
    if not buckets:
        return "all-buckets"
    if len(buckets) <= 3:
        return "+".join(buckets)
    return f"{len(buckets)}-buckets"


def build_report_file_name(parts: list[str | None]) -> str:
    """Builds a `part-part-...-YYYY-MM-DD.xlsx` filename, skipping empty parts."""
    segments = []
    for part in parts:
        if not part:
            continue
        cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", part).strip("-")
        if cleaned:
            segments.append(cleaned)
    segments.append(_today_date_stamp())
    return f"{'-'.join(segments)}.xlsx"


def _save_workbook(workbook: Workbook, file_name: str) -> str:
    """
    Writes to a temp file and renames it into place, so a concurrent read (e.g. downloading a
    partial report mid-scan) always sees either the previous or the fully-written new version,
    never a torn/corrupt file from an in-progress write.
    """
    os.makedirs(env.reports_dir, exist_ok=True)
    file_path = os.path.join(env.reports_dir, file_name)
    tmp_path = os.path.join(env.reports_dir, f".{file_name}.{uuid.uuid4().hex}.tmp")
    workbook.save(tmp_path)
    os.replace(tmp_path, file_path)
    return file_name


def generate_storage_report(summary: StorageSummary, file_name: str | None = None) -> str:
    file_name = file_name or build_report_file_name(["storage"])
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Storage Summary"

    headers = ["Bucket", "Object Count", "Total Size (Bytes)", "Total Size", "Error"]
    sheet.append(headers)
    widths = [30, 16, 20, 16, 40]
    for idx, width in enumerate(widths, start=1):
        sheet.column_dimensions[sheet.cell(row=1, column=idx).column_letter].width = width

    for bucket in summary.buckets:
        sheet.append(
            [
                bucket.bucket,
                bucket.object_count,
                bucket.total_size,
                _format_bytes(bucket.total_size),
                bucket.error or "",
            ]
        )
        if bucket.error:
            sheet.cell(row=sheet.max_row, column=5).font = _RED

    sheet.append([])
    sheet.append(
        ["TOTAL", summary.object_count, summary.total_size, _format_bytes(summary.total_size), ""]
    )
    for cell in sheet[sheet.max_row]:
        cell.font = _BOLD

    _style_header_row(sheet)

    return _save_workbook(workbook, file_name)


def generate_reconciliation_report(
    result: ReconciliationResult,
    file_name: str | None = None,
    provider: str | None = None,
) -> str:
    file_name = file_name or build_report_file_name(["reconciliation"])
    workbook = Workbook()

    matched_sheet = workbook.active
    matched_sheet.title = "Matched"
    matched_sheet.append(["Path", "Bucket", "Size (MB)", "Last Modified", "Table", "Column", "Row ID"])
    for idx, width in enumerate([60, 20, 14, 22, 16, 16, 12], start=1):
        matched_sheet.column_dimensions[matched_sheet.cell(row=1, column=idx).column_letter].width = width
    for file in result.matched:
        url = build_object_url(provider, file.bucket, file.path)
        matched_sheet.append(
            [url, file.bucket, _mb(file.size), _naive(file.last_modified), file.table, file.column, file.id]
        )
        cell = matched_sheet.cell(row=matched_sheet.max_row, column=1)
        cell.hyperlink = url
        cell.style = "Hyperlink"
    _style_header_row(matched_sheet)

    missing_sheet = workbook.create_sheet("Missing")
    missing_sheet.append(["Path", "Table", "Column", "Row ID"])
    for idx, width in enumerate([60, 16, 16, 12], start=1):
        missing_sheet.column_dimensions[missing_sheet.cell(row=1, column=idx).column_letter].width = width
    for file in result.missing:
        # A missing file was never found in storage, so this link (when we have enough info to
        # build one at all) points at where it *would* be — it will 404, but that's still more
        # useful for tracking it down than a bare relative key.
        if file.bucket:
            url = build_object_url(provider, file.bucket, file.path)
            missing_sheet.append([url, file.table, file.column, file.id])
            cell = missing_sheet.cell(row=missing_sheet.max_row, column=1)
            cell.hyperlink = url
            cell.style = "Hyperlink"
        else:
            missing_sheet.append([file.path, file.table, file.column, file.id])
    _style_header_row(missing_sheet)

    orphan_sheet = workbook.create_sheet("Orphan")
    orphan_sheet.append(["Path", "Bucket", "Size (MB)", "Last Modified"])
    for idx, width in enumerate([60, 20, 14, 22], start=1):
        orphan_sheet.column_dimensions[orphan_sheet.cell(row=1, column=idx).column_letter].width = width
    for file in result.orphan:
        url = build_object_url(provider, file.bucket, file.path)
        orphan_sheet.append([url, file.bucket, _mb(file.size), _naive(file.last_modified)])
        cell = orphan_sheet.cell(row=orphan_sheet.max_row, column=1)
        cell.hyperlink = url
        cell.style = "Hyperlink"
    _style_header_row(orphan_sheet)

    # Missing files were never found in storage, so they have no size to report; matched +
    # orphan together account for every object actually seen in storage, so their sizes sum to
    # the same total as "Object Storage Objects".
    matched_size = sum(f.size for f in result.matched)
    orphan_size = sum(f.size for f in result.orphan)

    summary_sheet = workbook.create_sheet("Summary")
    summary_sheet.append(["Metric", "Count", "Size (GB)"])
    for idx, width in enumerate([30, 16, 16], start=1):
        summary_sheet.column_dimensions[summary_sheet.cell(row=1, column=idx).column_letter].width = width
    summary_sheet.append(["Matched", result.summary.matched_count, _gb(matched_size)])
    summary_sheet.append(["Missing", result.summary.missing_count, ""])
    summary_sheet.append(["Orphan", result.summary.orphan_count, _gb(orphan_size)])
    summary_sheet.append(["Database File References", result.summary.database_file_count, ""])
    summary_sheet.append(
        ["Object Storage Objects", result.summary.storage_object_count, _gb(matched_size + orphan_size)]
    )
    summary_sheet.append(["Skipped (other provider)", result.summary.other_provider_count, ""])
    _style_header_row(summary_sheet)

    return _save_workbook(workbook, file_name)
