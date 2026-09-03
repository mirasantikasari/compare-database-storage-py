import re
from collections.abc import Callable, Iterator

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor

from app.config import env
from app.types import DbFileReference, TableColumnMapping

IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")
BATCH_SIZE = 5000
# Rich-text/JSON columns can carry a payload many times heavier per row than a short VARCHAR path
# (a LONGTEXT lesson body can run into the hundreds of KB) — 5000 of those in a single SELECT can
# mean tens/hundreds of MB moved in one query. A smaller batch keeps each round-trip bounded and
# gives a busy production MySQL server more, smaller breathing points instead of one huge pull.
CONTENT_BATCH_SIZE = 500

_pool = PooledDB(
    creator=pymysql,
    maxconnections=10,
    blocking=True,
    host=env.db.host,
    port=env.db.port,
    user=env.db.user,
    password=env.db.password,
    # Omitted entirely (not passed) when unset, same reasoning as the TS version: MySQL
    # validates access to a named default database as part of the handshake itself, so a
    # wrong/inaccessible DB_DATABASE would break every connection.
    **({"database": env.db.database} if env.db.database else {}),
    cursorclass=DictCursor,
    autocommit=True,
)

_SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def _assert_valid_identifier(name: str, label: str) -> None:
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(f'Invalid {label} name: "{name}"')


def _escape_id(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def list_databases() -> list[str]:
    """Lists non-system databases visible to the configured MySQL credential."""
    conn = _pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            rows = cursor.fetchall()
    finally:
        conn.close()

    names = [next(iter(row.values())) for row in rows]
    return sorted(name for name in names if name not in _SYSTEM_DATABASES)


def update_migrated_urls(
    database: str,
    items: list[dict],
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, int]:
    """
    Repoints dedicated file-reference columns using a completed storage-copy report.

    Every update is guarded by both Row ID and the original Source Path. A row whose value has
    changed since the report was generated is classified as a conflict and is never overwritten.
    Single-column primary keys are discovered from information_schema; for legacy tables without
    a declared primary key, an `id` column is accepted as the fallback used by reconciliation.

    Each row commits on its own (the pooled connection's default autocommit, rather than one
    transaction wrapping the whole run) so a run that gets interrupted partway — a dropped
    connection, a huge report — can safely be resumed by resending only the rows that never got
    a progress event back: this same per-row guard makes reprocessing an already-applied row
    self-correcting, since its current value will already equal Destination URL and it is simply
    reported "alreadyUpdated" rather than reapplied or flagged as a conflict. The trade-off is
    that a run stopped by an unexpected error can leave some rows updated and others not, rather
    than all-or-nothing.
    """
    _assert_valid_identifier(database, "database")
    if not items:
        return {"updated": 0, "alreadyUpdated": 0, "conflict": 0, "missing": 0, "total": 0}

    for item in items:
        _assert_valid_identifier(str(item["table"]), "table")
        _assert_valid_identifier(str(item["column"]), "column")

    conn = _pool.connection()
    try:
        # Resolve and validate every target before starting any writes.
        targets: dict[str, tuple[str, set[str]]] = {}
        with conn.cursor() as cursor:
            for table in sorted({str(item["table"]) for item in items}):
                cursor.execute(
                    """
                    SELECT COLUMN_NAME AS columnName, COLUMN_KEY AS columnKey
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    [database, table],
                )
                columns = cursor.fetchall()
                if not columns:
                    raise ValueError(f'Table "{database}.{table}" does not exist or is not accessible')
                names = {row["columnName"] for row in columns}
                primary_keys = [row["columnName"] for row in columns if row["columnKey"] == "PRI"]
                if len(primary_keys) == 1:
                    id_column = primary_keys[0]
                elif not primary_keys and "id" in names:
                    id_column = "id"
                else:
                    raise ValueError(
                        f'Table "{database}.{table}" needs one primary key (or an id column) '
                        "to safely match the report's Row ID"
                    )
                targets[table] = (id_column, names)

            for item in items:
                table = str(item["table"])
                column = str(item["column"])
                if column not in targets[table][1]:
                    raise ValueError(f'Column "{database}.{table}.{column}" does not exist')

        counts = {"updated": 0, "alreadyUpdated": 0, "conflict": 0, "missing": 0, "total": len(items)}
        with conn.cursor() as cursor:
            for index, item in enumerate(items, start=1):
                table = str(item["table"])
                column = str(item["column"])
                id_column = targets[table][0]
                qualified_table = f"{_escape_id(database)}.{_escape_id(table)}"
                escaped_column = _escape_id(column)
                escaped_id = _escape_id(id_column)

                cursor.execute(
                    f"SELECT {escaped_column} AS currentValue FROM {qualified_table} WHERE {escaped_id} = %s",
                    [item["rowId"]],
                )
                row = cursor.fetchone()
                if row is None:
                    outcome = "missing"
                else:
                    current = row["currentValue"]
                    if current == item["destinationUrl"]:
                        outcome = "alreadyUpdated"
                    elif current != item["sourcePath"]:
                        outcome = "conflict"
                    else:
                        cursor.execute(
                            f"UPDATE {qualified_table} SET {escaped_column} = %s "
                            f"WHERE {escaped_id} = %s AND {escaped_column} = %s",
                            [item["destinationUrl"], item["rowId"], item["sourcePath"]],
                        )
                        outcome = "updated" if cursor.rowcount == 1 else "conflict"
                counts[outcome] += 1
                if on_progress:
                    on_progress(index, len(items), outcome)
        return counts
    finally:
        conn.close()


def fetch_file_references(
    mapping: TableColumnMapping,
    database: str | None = None,
    batch_size: int = BATCH_SIZE,
) -> Iterator[DbFileReference]:
    """
    Reads non-empty values from a table/column in id-ordered batches (keyset pagination)
    instead of one big SELECT, so tables with millions of rows stay memory-safe. Intended to
    be driven from a worker thread (via asyncio.to_thread) so a slow/huge table never blocks
    the server's event loop.
    """
    id_column = mapping.id_column or "id"
    _assert_valid_identifier(mapping.table, "table")
    _assert_valid_identifier(mapping.column, "column")
    _assert_valid_identifier(id_column, "id column")
    if database:
        _assert_valid_identifier(database, "database")

    table = f"{_escape_id(database)}.{_escape_id(mapping.table)}" if database else _escape_id(mapping.table)
    column = _escape_id(mapping.column)
    id_col = _escape_id(id_column)

    last_id: str | int | None = None
    has_more = True

    conn = _pool.connection()
    try:
        with conn.cursor() as cursor:
            while has_more:
                cursor_clause = f"AND {id_col} > %s" if last_id is not None else ""
                sql = f"""
                    SELECT {id_col} AS id, {column} AS value
                    FROM {table}
                    WHERE {column} IS NOT NULL AND {column} <> ''
                        {cursor_clause}
                    ORDER BY {id_col} ASC
                    LIMIT {batch_size}
                """
                params = [last_id] if last_id is not None else []
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                for row in rows:
                    yield DbFileReference(
                        table=mapping.table, column=mapping.column, id=row["id"], value=row["value"]
                    )
                    last_id = row["id"]

                has_more = len(rows) == batch_size
    finally:
        conn.close()


STRING_DATA_TYPES = {"varchar", "char", "text", "tinytext", "mediumtext", "longtext"}

# Column names that plausibly hold a stored asset (image/attachment/document/...). "capture"
# earned its place after a real production dump showed a `course_quiz_log.url_capture` column
# (proctoring screenshot evidence) that no other keyword here would have matched.
ASSET_COLUMN_PATTERN = re.compile(
    r"(image|photo|avatar|attachment|logo|banner|cover|picture|thumbnail|document|foto|gambar|lampiran|dokumen|file|capture)",
    re.I,
)
# "url"/"path"/"link" alone are too generic (nav links, API endpoints, external CDN refs,
# YouTube links, ...) — only trust them when the table itself is clearly a file/attachment/media
# table. "link" earned its place here after a real production dump showed a `course_assignment`
# table storing its (dominant, 100k+ row) file reference in a `link` column — `filename` in the
# same table only ever holds the bare basename, not the actual object key/path.
GENERIC_PATH_COLUMN_PATTERN = re.compile(r"^(url|path|link)$", re.I)
ASSET_TABLE_HINT_PATTERN = re.compile(r"(file|document|attachment|media|asset|assignment)", re.I)
# Metadata columns that happen to contain an asset keyword but aren't themselves a path
# (sizes, extensions, flags, timestamps, foreign keys into a separate files table, ...).
EXCLUDE_COLUMN_PATTERN = re.compile(
    r"(size|extension|limit|style|count|status|width|height|duration|mime|position|alignment|"
    r"logout|^type$|_type$|_by$|_at$|^uploaded$|^total_|^max_|^show_|^print_|^id_|_id$)",
    re.I,
)
EXCLUDE_TABLE_PATTERN = re.compile(r"menu", re.I)
# `^id_`/`_id$` in EXCLUDE_COLUMN_PATTERN assumes an "id_x" column is a numeric foreign key, which
# is true for the vast majority — but a couple of real schemas store this as a VARCHAR alongside
# genuinely-int id_ siblings in the same table (`group_files.id_file`, `message_room.id_file`),
# which is exactly the shape of a mis-named file reference rather than a real foreign key. Excluded
# by name here so those two specific columns aren't silently dropped by the generic `id_` rule.
EXCLUDE_COLUMN_OVERRIDE = {"id_file"}

# TEXT-family columns big enough to plausibly hold rich-text/HTML (CKEditor/CKFinder-style)
# content, as opposed to a short VARCHAR that's almost certainly a single file path. Native
# JSON columns are included too — a JSON array/object of attachments embeds file references the
# same "not a single dedicated path column" way rich text does, just structured instead of HTML.
CONTENT_DATA_TYPES = {"text", "tinytext", "mediumtext", "longtext", "json"}

# Pure audit/activity-log tables: append-only history, never the live source a lesson's content
# actually lives in — scanning them for embedded file references adds real DB load (they're
# consistently among the largest tables in the schema) for close to zero protective value, since
# anything still genuinely in use also shows up in the *current* content column that logs it.
# Deliberately an exact-name allowlist rather than a "contains 'log'" pattern: names alone don't
# reliably say which — `course_assignment_log` holds real submission content (description/
# file_upload) despite the name, while `course_quiz_log` is genuinely just a per-attempt event
# log (its `question` is a redundant snapshot of course_quiz_detail.question, already scanned
# there) — confirmed against this schema, not guessed from naming convention alone.
EXCLUDE_CONTENT_TABLE_PATTERN = re.compile(
    r"^(activity_log|authentication_log|log_activity|log_score|log_score_final|"
    r"course_quiz_log|users_notification_log|mod_penilaian_log_nilai|"
    r"mod_penilaian_log_aktifitas)$",
    re.I,
)


def _introspect_columns(database: str) -> tuple[list[dict], dict[str, str], dict[str, int]]:
    conn = _pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME AS tableName, COLUMN_NAME AS columnName, DATA_TYPE AS dataType
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                [database],
            )
            columns = cursor.fetchall()

            cursor.execute(
                """
                SELECT TABLE_NAME AS tableName, COLUMN_NAME AS columnName
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                [database],
            )
            primary_keys = cursor.fetchall()

            # TABLE_ROWS is an estimate from index statistics, not a real COUNT(*) — cheap
            # metadata-only lookup (InnoDB doesn't need to touch a single data page for it),
            # which is the whole point: telling a 500k-row audit-log table apart from a normal
            # one shouldn't itself require scanning that table.
            cursor.execute(
                """
                SELECT TABLE_NAME AS tableName, TABLE_ROWS AS tableRows
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                """,
                [database],
            )
            table_row_stats = cursor.fetchall()
    finally:
        conn.close()

    primary_key_by_table: dict[str, str] = {}
    for row in primary_keys:
        primary_key_by_table.setdefault(row["tableName"], row["columnName"])

    row_count_by_table: dict[str, int] = {
        row["tableName"]: row["tableRows"] or 0 for row in table_row_stats
    }

    return columns, primary_key_by_table, row_count_by_table


def discover_file_columns(database: str) -> list[TableColumnMapping]:
    """
    Introspects a database's `information_schema` for columns that look like they store an
    Object Storage key/path, using column name + type heuristics. Best-effort: a false
    positive just shows up as noise in the report, a false negative just means reduced
    coverage — neither corrupts the result, so a wide net is an acceptable trade-off for not
    having to hand-configure every table/column on every database.
    """
    _assert_valid_identifier(database, "database")
    columns, primary_key_by_table, _row_count_by_table = _introspect_columns(database)

    mappings: list[TableColumnMapping] = []
    for col in columns:
        if col["dataType"].lower() not in STRING_DATA_TYPES:
            continue
        if EXCLUDE_TABLE_PATTERN.search(col["tableName"]):
            continue
        column_lower = col["columnName"].lower()
        if column_lower not in EXCLUDE_COLUMN_OVERRIDE and EXCLUDE_COLUMN_PATTERN.search(col["columnName"]):
            continue

        is_asset_column = bool(ASSET_COLUMN_PATTERN.search(col["columnName"]))
        is_generic_path_in_asset_table = bool(
            GENERIC_PATH_COLUMN_PATTERN.match(col["columnName"])
            and ASSET_TABLE_HINT_PATTERN.search(col["tableName"])
        )

        if is_asset_column or is_generic_path_in_asset_table:
            mappings.append(
                TableColumnMapping(
                    table=col["tableName"],
                    column=col["columnName"],
                    id_column=primary_key_by_table.get(col["tableName"], "id"),
                )
            )

    return mappings


def discover_content_columns(database: str) -> list[TableColumnMapping]:
    """
    Introspects a database for TEXT/MEDIUMTEXT/LONGTEXT columns, treated as candidate rich-text
    (CKEditor/CKFinder-style) content that may embed Object Storage URLs inline — e.g. an
    `<img src="...">` inside a lesson's HTML body — rather than storing a path in its own
    dedicated column the way discover_file_columns looks for.

    Unlike discover_file_columns, this applies no column-name filtering: a false negative here
    would make a still-referenced file look like a safe-to-delete orphan, which is exactly the
    mistake this scan exists to prevent — so it deliberately errs toward scanning more text
    columns than strictly necessary rather than risk missing one.

    Two exceptions, both aimed at the same problem (this hammering a *production* database):
    - Tables at or above env.max_content_scan_table_rows (by estimated row count, not a real
      scan) are skipped outright.
    - Known pure audit/activity-log tables (EXCLUDE_CONTENT_TABLE_PATTERN) are skipped by name
      regardless of size — they're historical snapshots, not where a lesson's live, still-editable
      content actually lives, so scanning them buys close to nothing over scanning the *current*
      content column that they're a log of.
    """
    _assert_valid_identifier(database, "database")
    columns, primary_key_by_table, row_count_by_table = _introspect_columns(database)

    return [
        TableColumnMapping(
            table=col["tableName"],
            column=col["columnName"],
            id_column=primary_key_by_table.get(col["tableName"], "id"),
        )
        for col in columns
        if col["dataType"].lower() in CONTENT_DATA_TYPES
        and not EXCLUDE_TABLE_PATTERN.search(col["tableName"])
        and not EXCLUDE_CONTENT_TABLE_PATTERN.match(col["tableName"])
        and row_count_by_table.get(col["tableName"], 0) < env.max_content_scan_table_rows
    ]
