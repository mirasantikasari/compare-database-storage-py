import re
from collections.abc import Iterator

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor

from app.config import env
from app.types import DbFileReference, TableColumnMapping

IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
BATCH_SIZE = 5000

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


def _assert_valid_identifier(name: str, label: str) -> None:
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(f'Invalid {label} name: "{name}"')


def _escape_id(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def fetch_file_references(
    mapping: TableColumnMapping,
    database: str | None = None,
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
                    LIMIT {BATCH_SIZE}
                """
                params = [last_id] if last_id is not None else []
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                for row in rows:
                    yield DbFileReference(
                        table=mapping.table, column=mapping.column, id=row["id"], value=row["value"]
                    )
                    last_id = row["id"]

                has_more = len(rows) == BATCH_SIZE
    finally:
        conn.close()


STRING_DATA_TYPES = {"varchar", "char", "text", "tinytext", "mediumtext", "longtext"}

# Column names that plausibly hold a stored asset (image/attachment/document/...).
ASSET_COLUMN_PATTERN = re.compile(
    r"(image|photo|avatar|attachment|logo|banner|cover|picture|thumbnail|document|foto|gambar|lampiran|dokumen|file)",
    re.I,
)
# "url"/"path" alone are too generic (nav links, API endpoints, external CDN refs) — only
# trust them when the table itself is clearly a file/attachment/media table.
GENERIC_PATH_COLUMN_PATTERN = re.compile(r"^(url|path)$", re.I)
ASSET_TABLE_HINT_PATTERN = re.compile(r"(file|document|attachment|media|asset)", re.I)
# Metadata columns that happen to contain an asset keyword but aren't themselves a path
# (sizes, extensions, flags, timestamps, foreign keys into a separate files table, ...).
EXCLUDE_COLUMN_PATTERN = re.compile(
    r"(size|extension|limit|style|count|status|width|height|duration|mime|position|alignment|"
    r"logout|^type$|_type$|_by$|_at$|^uploaded$|^total_|^max_|^show_|^print_|^id_|_id$)",
    re.I,
)
EXCLUDE_TABLE_PATTERN = re.compile(r"menu", re.I)


def discover_file_columns(database: str) -> list[TableColumnMapping]:
    """
    Introspects a database's `information_schema` for columns that look like they store an
    Object Storage key/path, using column name + type heuristics. Best-effort: a false
    positive just shows up as noise in the report, a false negative just means reduced
    coverage — neither corrupts the result, so a wide net is an acceptable trade-off for not
    having to hand-configure every table/column on every database.
    """
    _assert_valid_identifier(database, "database")

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
    finally:
        conn.close()

    primary_key_by_table: dict[str, str] = {}
    for row in primary_keys:
        primary_key_by_table.setdefault(row["tableName"], row["columnName"])

    mappings: list[TableColumnMapping] = []
    for col in columns:
        if col["dataType"].lower() not in STRING_DATA_TYPES:
            continue
        if EXCLUDE_TABLE_PATTERN.search(col["tableName"]):
            continue
        if EXCLUDE_COLUMN_PATTERN.search(col["columnName"]):
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
