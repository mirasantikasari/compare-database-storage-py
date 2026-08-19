import os
import re
import tempfile
import time
import uuid
from collections.abc import Callable
from datetime import date, datetime

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

from app.config import env
from app.providers.s3_provider import build_object_url
from app.types import ReconciliationResult, StorageSummary

_BOLD = Font(bold=True)
_RED = Font(color="FFCC0000")
# Applied directly instead of `cell.style = "Hyperlink"` — the named-style path re-resolves the
# style from the workbook's style registry on every single cell, which measurably adds up across
# tens/hundreds of thousands of rows. A plain Font gets the same visual result (and still a real,
# clickable hyperlink) for a fraction of the per-cell cost.
_HYPERLINK_FONT = Font(color="0563C1", underline="single")

# How often generate_reconciliation_report reports progress while writing rows — frequent enough
# to feel alive on a huge report, not so frequent that the progress calls themselves add overhead.
_REPORT_TICK_EVERY_ROWS = 2000

ReportProgressCallback = Callable[[str, int, int, str, str | None], None]


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


def _header_row(ws, headers: list[str]) -> list[WriteOnlyCell]:
    cells = []
    for h in headers:
        cell = WriteOnlyCell(ws, value=h)
        cell.font = _BOLD
        cells.append(cell)
    return cells


def _hyperlink_cell(ws, url: str) -> WriteOnlyCell:
    cell = WriteOnlyCell(ws, value=url)
    cell.hyperlink = url
    cell.font = _HYPERLINK_FONT
    return cell


def generate_reconciliation_report(
    result: ReconciliationResult,
    file_name: str | None = None,
    provider: str | None = None,
    on_progress: ReportProgressCallback | None = None,
) -> str:
    """
    Uses openpyxl's write_only mode: a normal Workbook re-registers a cell's style against the
    workbook's shared style table on every assignment, which gets measurably slower as that
    table grows — noticeable well before "thousands and thousands of objects" territory, since
    every Matched/Orphan row sets a font for its hyperlink. write_only sidesteps that (several
    times faster in practice), at the cost of being append-only — no going back to re-read or
    restyle a row after it's written, which this function never needs to do anyway.
    """
    file_name = file_name or build_report_file_name(["reconciliation"])
    workbook = Workbook(write_only=True)

    total_rows = len(result.matched) + len(result.missing) + len(result.orphan)
    written = 0
    started_at = time.monotonic()

    def tick(sheet_label: str) -> None:
        nonlocal written
        written += 1
        if on_progress and written % _REPORT_TICK_EVERY_ROWS == 0:
            elapsed = max(time.monotonic() - started_at, 0.001)
            on_progress(
                "report",
                written,
                total_rows,
                f"Writing {sheet_label} sheet — {written:,}/{total_rows:,} row(s) "
                f"({written / elapsed:,.0f}/s)",
                None,
            )

    matched_sheet = workbook.create_sheet("Matched")
    for idx, width in enumerate([60, 20, 14, 22, 16, 16, 12], start=1):
        matched_sheet.column_dimensions[get_column_letter(idx)].width = width
    matched_sheet.append(
        _header_row(matched_sheet, ["Path", "Bucket", "Size (MB)", "Last Modified", "Table", "Column", "Row ID"])
    )
    for file in result.matched:
        url = build_object_url(provider, file.bucket, file.path)
        matched_sheet.append(
            [
                _hyperlink_cell(matched_sheet, url),
                file.bucket,
                _mb(file.size),
                _naive(file.last_modified),
                file.table,
                file.column,
                file.id,
            ]
        )
        tick("Matched")

    missing_sheet = workbook.create_sheet("Missing")
    for idx, width in enumerate([60, 16, 16, 12], start=1):
        missing_sheet.column_dimensions[get_column_letter(idx)].width = width
    missing_sheet.append(_header_row(missing_sheet, ["Path", "Table", "Column", "Row ID"]))
    for file in result.missing:
        # A missing file was never found in storage, so this link (when we have enough info to
        # build one at all) points at where it *would* be — it will 404, but that's still more
        # useful for tracking it down than a bare relative key.
        if file.bucket:
            url = build_object_url(provider, file.bucket, file.path)
            missing_sheet.append([_hyperlink_cell(missing_sheet, url), file.table, file.column, file.id])
        else:
            missing_sheet.append([file.path, file.table, file.column, file.id])
        tick("Missing")

    orphan_sheet = workbook.create_sheet("Orphan")
    for idx, width in enumerate([60, 20, 14, 22], start=1):
        orphan_sheet.column_dimensions[get_column_letter(idx)].width = width
    orphan_sheet.append(_header_row(orphan_sheet, ["Path", "Bucket", "Size (MB)", "Last Modified"]))
    for file in result.orphan:
        url = build_object_url(provider, file.bucket, file.path)
        orphan_sheet.append(
            [_hyperlink_cell(orphan_sheet, url), file.bucket, _mb(file.size), _naive(file.last_modified)]
        )
        tick("Orphan")

    if on_progress:
        on_progress("report", total_rows, total_rows, "Saving workbook to disk…", None)

    # Missing files were never found in storage, so they have no size to report; matched +
    # orphan together account for every object actually seen in storage, so their sizes sum to
    # the same total as "Object Storage Objects".
    matched_size = sum(f.size for f in result.matched)
    orphan_size = sum(f.size for f in result.orphan)

    summary_sheet = workbook.create_sheet("Summary")
    for idx, width in enumerate([30, 16, 16], start=1):
        summary_sheet.column_dimensions[get_column_letter(idx)].width = width
    summary_sheet.append(_header_row(summary_sheet, ["Metric", "Count", "Size (GB)"]))
    summary_sheet.append(["Matched", result.summary.matched_count, _gb(matched_size)])
    summary_sheet.append(["Missing", result.summary.missing_count, ""])
    summary_sheet.append(["Orphan", result.summary.orphan_count, _gb(orphan_size)])
    summary_sheet.append(["Database File References", result.summary.database_file_count, ""])
    summary_sheet.append(
        ["Object Storage Objects", result.summary.storage_object_count, _gb(matched_size + orphan_size)]
    )
    summary_sheet.append(["Skipped (other provider)", result.summary.other_provider_count, ""])

    return _save_workbook(workbook, file_name)
