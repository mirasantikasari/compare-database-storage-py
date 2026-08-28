# Object Storage & MySQL Reconciler

A read-only (plus one carefully-gated delete feature) audit tool for multi-tenant deployments
that store files in S3-compatible object storage (AWS S3, Wasabi, DigitalOcean Spaces) and keep
references to those files in per-tenant MySQL databases. It answers three questions:

- **Is this file in storage actually referenced anywhere in the database?** (Orphan)
- **Is this database reference actually backed by a file in storage?** (Missing)
- **Is a file that looks unreferenced actually still embedded in a lesson's rich-text/HTML or
  JSON content (CKEditor/CKFinder-style uploads)?** (Protected)

Built for a school-LMS platform with hundreds of tenant databases/buckets, where table and
column names vary per school and some tables can be enormous — most of the design decisions
below exist because of real incidents hit while building this (a missed column, a production
database taken down by an over-eager scan, a delete aimed at the wrong storage provider).

## Features

### Storage summary
Object count and total size per bucket, optionally exported to `.xlsx`.

### Auto reconciliation
Point it at a database name; it introspects `information_schema` to find candidate
file-reference columns itself (see [How column discovery works](#how-column-discovery-works)),
scans the configured bucket(s), and produces a report with four categories: **Matched**,
**Missing**, **Orphan**, **Protected**.

### DO cleanup candidates
The same scan as Auto reconciliation, but the deliverable is explicitly framed as a cleanup
worklist: **Cleanup Candidates** (safe-to-review orphans) vs **Protected** (orphans saved by a
rich-text/JSON match).

### Manual reconciliation
Hand-type table/column mappings instead of relying on auto-discovery — useful for one-off checks
or when discovery misses something. Database is optional; when given, the same rich-text/JSON
protection check still runs.

### Delete files from report
Upload a previously-downloaded Orphan/Cleanup Candidates report, tick the rows you actually want
gone, type a literal confirmation phrase, and delete — with a live progress bar and a permanent
JSON audit trail. See [Delete safety design](#delete-safety-design).

### Copy files to another provider
Upload a previously-downloaded reconciliation report and read back its `Matched` sheet — files
already confirmed to exist in both the DB and storage — then copy the selected rows from one
provider to another (e.g. DigitalOcean -> Wasabi ahead of a migration). `POST /storage/copy` /
`/storage/copy/stream` never touch the source: nothing is deleted from the old provider, since
the app's own DB references still point there until they're repointed separately. S3's
server-side `CopyObject` only works within a single endpoint, so a cross-provider copy streams
each object through the app instead (`GetObject` from source, a multipart-aware upload to
destination) — bounded by `STORAGE_COPY_CONCURRENCY`, kept low by default since this moves full
object bytes rather than just metadata.

**Live per-file progress, not just per-item.** `POST /storage/copy/stream` emits an
`item_progress` event repeatedly *during* a single file's upload (bytes transferred so far),
not only once per whole file the way `progress` does — without it, a run with just one large
file (or a small handful) would leave the UI's progress bar sitting at 0% with no sign of life
until the transfer was already done.

**The destination bucket doesn't need to exist beforehand.** A tenant's bucket name being
unchanged across providers doesn't mean the bucket was ever created on the new one — before any
item is copied, every distinct destination bucket is `HeadBucket`-checked and auto-created if
missing (region taken from that provider's own config). If creation itself fails (e.g. the
credential lacks `CreateBucket` permission), every item bound for that bucket fails once with a
clear "Destination bucket unavailable" message instead of a `NoSuchBucket` error repeated per item.

**Re-uploading the same report is safe.** By default (`overwrite: false`) every item is
`HeadObject`-checked against the destination first; anything already there is left alone and
marked `Skipped` instead of re-downloaded/re-uploaded. This is keyed off what's actually sitting
at the destination, not off which report or file was uploaded, so it also covers a second report
that happens to overlap an earlier run — not just a literal re-upload of the same `.xlsx`. Pass
`overwrite: true` to force a fresh copy of every item regardless.

Every run produces two artifacts: a JSON audit trail in `reports/.copies/` (same shape as the
delete audit log, plus which `.xlsx` report belongs to it) and an `.xlsx` report in
`reports/` — one row per item with the object's new **Destination URL** (its actual Wasabi link,
built with `build_object_url` the same way every other report's links are) alongside its status
(`Copied` / `Skipped` / `Failed`), so it's a direct answer to "what actually moved and where does
it live now".

To later remove the now-migrated originals from the old provider, `GET /storage/copies` lists
past copy runs and `GET /storage/copies/{filename}` reads one back split into `succeeded`/
`failed` items — `succeeded` is exactly the `{bucket, key}` shape `POST /storage/delete` expects
(pass `provider` as that record's `sourceProvider`), so deleting what was copied still goes
through the same human-confirmed delete flow as everything else, it just skips re-typing the
list by hand. This intentionally does *not* re-verify against the database — it only reflects
what the copy step actually wrote to the destination — so only delete once the app's own DB
references have actually been repointed at the new provider; re-running Auto reconciliation /
DO cleanup candidates against the old provider is the safer, DB-verified alternative.

## How reconciliation works

1. **Discover columns.** `discover_file_columns()` looks for `VARCHAR`/`CHAR`/`TEXT`-family
   columns whose name suggests a stored file (`image`, `attachment`, `foto`, `dokumen`, `link`
   in a file-ish table, etc.) — see `ASSET_COLUMN_PATTERN` / `GENERIC_PATH_COLUMN_PATTERN` in
   `app/services/mysql_service.py`. It's a heuristic: false positives are harmless noise, false
   negatives mean reduced coverage, so it deliberately casts a wide net.
2. **Read the database.** Every discovered column is read in batches (keyset pagination), in
   parallel across a bounded worker pool (`DB_SCAN_CONCURRENCY`).
3. **List storage.** Every target bucket is listed via `ListObjectsV2`.
4. **Compare.**
   - In both DB and storage → **Matched**.
   - In DB but not storage → **Missing** (a broken reference — nothing to delete, the *file* is
     what's gone).
   - In storage but not any discovered DB column → candidate **Orphan**.
5. **Protect.** `discover_content_columns()` separately finds every `TEXT`/`JSON`-family column
   (no name filtering — see below) and scans it for embedded Object Storage URLs (e.g. an
   `<img src="...">` a CKEditor/CKFinder upload leaves in a lesson's HTML body). Any candidate
   Orphan whose key turns up there moves to **Protected** instead.
6. **Cross-provider references.** A DB value can be a full URL naming a *specific* provider
   (e.g. an old `amazonaws.com` link) even while the current scan targets a different provider
   (e.g. DigitalOcean). Such references are excluded from Missing entirely (counted separately as
   "different provider host") rather than wrongly reported as broken — the file may well still
   exist, just not on the provider this run checked.

## How column discovery works

Two independent discovery passes, because they protect against different failure modes:

| | `discover_file_columns` | `discover_content_columns` |
|---|---|---|
| Targets | `VARCHAR`/`CHAR`/`TEXT`-family | `TEXT`/`MEDIUMTEXT`/`LONGTEXT`/`JSON` only |
| Filtering | By column/table **name** (heuristic) | **No name filtering** — type alone qualifies |
| Finds | Dedicated path columns (`cover`, `attachment_quiz`, …) | Rich-text/JSON blobs that *embed* a URL somewhere inside |
| Misses looks like | A file wrongly shows as Orphan | A file wrongly shows as Orphan |

The no-name-filtering design for content columns is deliberate: real production data showed
genuinely-used files referenced only from columns named `value` (a generic settings table) or
`favicon`/`slider_1` (nothing an English/Indonesian keyword list would ever guess) — the file's
*type* being large text was the only reliable signal.

Two tables are excluded from content scanning regardless of size or name match:
- `EXCLUDE_TABLE_PATTERN` — tables with "menu" in the name (their `icon` columns hold static
  frontend asset filenames, not storage uploads).
- `EXCLUDE_CONTENT_TABLE_PATTERN` — an **exact-name allowlist** of confirmed pure audit/log
  tables (`activity_log`, `authentication_log`, `course_quiz_log`, …). Deliberately not a
  "contains 'log'" pattern: `course_assignment_log` *looks* like a log table but actually holds
  live submission content, while `course_quiz_log` *doesn't* look like one but is genuinely just
  a per-attempt event log whose `question` column is a redundant snapshot of
  `course_quiz_detail.question` (already scanned there).

## Production-safety design

This tool queries a live production database and can permanently delete production files. Every
one of these exists because of a real problem hit while building it:

- **`DB_SCAN_CONCURRENCY`** (default `2`) is separate from `STORAGE_SUMMARY_CONCURRENCY`
  (default `8`). S3-compatible API calls are safe to parallelize aggressively; MySQL queries
  against a live primary are not — a handful of parallel `LONGTEXT` column scans took the
  production database down once.
- **`MAX_CONTENT_SCAN_TABLE_ROWS`** (default `50000`) skips content-scanning any table at or
  above this estimated row count (from `information_schema.TABLES`, not a real `COUNT(*)`).
  Append-only log tables are consistently the largest tables in the schema and the least
  valuable to scan.
- **Smaller batch size for content columns** (`CONTENT_BATCH_SIZE = 500` vs the normal `5000`) —
  a `LONGTEXT` row can be hundreds of KB, so the same row count moves far more data.
- **Column values are never rewritten for display.** The report's "Value (as stored in DB)"
  column always shows the literal raw DB string — never a URL reconstructed from a parsed
  bucket/key, which can look nothing like the original (different host, different
  path-vs-virtual-hosted style) even when it points at the same object.
- **Deletion never happens off a plain Orphan list.** See the next section.

## Delete safety design

- **Human-in-the-loop only.** The only way to delete anything is: download a report → open it
  and actually look at it → upload it back → tick specific rows → type the exact confirmation
  phrase → confirm again in a browser dialog. There is no "delete all current orphans" button.
- **Server-side confirmation, not just UI gating.** `POST /storage/delete` rejects any request
  whose `confirm` field isn't the exact phrase, regardless of what called it — a replayed or
  hand-built request without going through the UI is rejected the same way.
- **Resilient to hand-edited reports.** A human reviewing a report in Excel will often trim it
  down to a shortlist — which can lose the hidden `Bucket`/`Key` columns the report normally
  carries (Excel rewrites the whole file on save). `parse_deletable_report` falls back to
  re-deriving `(bucket, key)` from the visible, clickable `Path` URL when those columns are gone.
- **Full audit trail.** Every delete request, successful or not, is written to
  `reports/.deletions/<timestamp>.json` — what was requested and the exact per-item outcome.
  There is no undo.
- **Streamed progress.** `POST /storage/delete/stream` reports progress in batches of 50 (not
  the S3 max of 1000) specifically so a human-reviewed, human-sized selection shows meaningful
  incremental progress instead of jumping straight from 0% to 100%.

## Architecture

- **Backend:** FastAPI (Python), blocking S3/MySQL calls run via `asyncio.to_thread` /
  dedicated thread pools so they never block the event loop.
- **Object storage:** boto3, one client per configured provider (`app/providers/s3_provider.py`).
  Supports AWS S3, Wasabi, and DigitalOcean Spaces simultaneously — a request can target any
  configured provider, and multiple providers can hold buckets with the *same* tenant name after
  a migration (handled explicitly, see "Cross-provider references" above).
- **Database:** PyMySQL + DBUtils `PooledDB`, read-only queries, keyset-paginated batches.
- **Reports:** openpyxl, `write_only` mode workbooks for large reports (write_only skips
  per-cell style-table lookups, which matters once a sheet has hundreds of thousands of rows).
- **Streaming:** Server-Sent Events (`app/services/sse.py`) for anything that can take minutes —
  a background thread emits `progress`/`done`/`error` events onto an `asyncio.Queue`, with a
  15-second heartbeat comment so a quiet stream doesn't look hung to a proxy/browser.
- **Frontend:** a single static `public/index.html` — vanilla JS, no build step, no framework.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in credentials
uvicorn app.main:app --reload
```

Open `http://localhost:3000` (or whatever `PORT` is set to).

## Configuration reference

All settings are environment variables (see `.env.example` for the full annotated version).

| Variable | Default | Notes |
|---|---|---|
| `PORT` | `3000` | |
| `S3_DEFAULT_PROVIDER` | — | Which provider is used when a request doesn't specify one |
| `S3_AWS_*` / `S3_WASABI_*` / `S3_DO_*` | — | `_ACCESS_KEY` + `_SECRET_KEY` required for a provider to show up at all. Only AWS needs `_REGION`/`_ENDPOINT` too — Wasabi and DO's region/endpoint are fixed in `app/config.py` (Wasabi `ap-southeast-1`; DO exposes both `do-sfo2` and `do-sgp1` off the same `S3_DO_*` credential, since DO Spaces keys are account-wide) |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` | — | A **read-only** MySQL user is strongly recommended |
| `DB_DATABASE` | — | Optional fixed default; Auto reconciliation can also target a database per-request |
| `REPORTS_DIR` | `reports` | Where `.xlsx` reports and `.deletions/*.json` audit logs are written |
| `STORAGE_SUMMARY_CONCURRENCY` | `8` | Parallel S3-compatible API calls |
| `STORAGE_COPY_CONCURRENCY` | `4` | Parallel object copies between providers (streams full bytes, unlike the summary scan) |
| `DB_SCAN_CONCURRENCY` | `2` | Parallel MySQL table/column scans — keep low against a production primary |
| `MAX_CONTENT_SCAN_TABLE_ROWS` | `50000` | Tables at/above this estimated row count skip content scanning |

## API reference

| Method | Path | Purpose |
|---|---|---|
| GET | `/storage/providers` | List configured providers |
| GET | `/storage/buckets` | List buckets for a provider |
| GET | `/storage/summary` / `/storage/summary/stream` | Object count/size per bucket |
| GET | `/storage/{bucket}/objects` | Paginated object listing |
| POST | `/storage/parse-report` | Parse an uploaded Orphan/Cleanup Candidates report |
| POST | `/storage/delete` | Delete objects (blocking, single response) |
| POST | `/storage/delete/stream` | Delete objects (SSE, live progress) |
| POST | `/storage/parse-matched-report` | Parse an uploaded reconciliation report's Matched sheet |
| POST | `/storage/copy` | Copy objects to another provider, e.g. DO -> Wasabi (blocking, single response) |
| POST | `/storage/copy/stream` | Copy objects to another provider (SSE, live progress) |
| GET | `/storage/copies` | List past copy runs from their `reports/.copies/` audit trail |
| GET | `/storage/copies/{filename}` | Read one copy run's audit record, split into succeeded/failed items |
| GET | `/reconciliation/auto/stream` | Auto-discovery reconciliation (SSE) |
| GET | `/reconciliation/do-cleanup/stream` | DO cleanup candidates scan (SSE) |
| POST | `/reconciliation/stream` / `/reconciliation` | Manual reconciliation (SSE / blocking) |
| GET | `/reports/{filename}` | Download a generated report |

All JSON responses share the shape `{"status": bool, "data" | "message": ...}`. SSE endpoints
emit `discovered` → repeated `progress` → `done` (or `error`) events.

## Reports

Every run writes a `.xlsx` to `REPORTS_DIR` (default `reports/`) named
`{database}-{buckets}-{YYYY-MM-DD}.xlsx`. Sheets: `Matched`, `Missing`, `Orphan`,
`Protected (rich-text, JSON)`, `Summary` (Auto/Manual reconciliation) or `Cleanup Candidates`,
`Protected`, `Summary` (DO cleanup). A scan interrupted partway through resumes from a checkpoint
in `REPORTS_DIR/.checkpoints/` rather than restarting; delete audit logs live in
`REPORTS_DIR/.deletions/`.
