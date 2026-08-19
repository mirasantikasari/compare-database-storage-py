from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class StorageObject:
    bucket: str
    key: str
    size: int
    last_modified: datetime | None = None
    etag: str | None = None


@dataclass
class BucketSummary:
    bucket: str
    object_count: int
    total_size: int
    error: str | None = None


@dataclass
class StorageSummary:
    buckets: list[BucketSummary]
    bucket_count: int
    object_count: int
    total_size: int


@dataclass
class ListObjectsPage:
    objects: list[StorageObject]
    is_truncated: bool
    next_continuation_token: str | None = None


@dataclass
class TableColumnMapping:
    table: str
    column: str
    id_column: str | None = None


@dataclass
class ReconciliationRequest:
    mappings: list[TableColumnMapping]
    buckets: list[str] | None = None
    prefix: str | None = None
    database: str | None = None
    provider: str | None = None


@dataclass
class DbFileReference:
    table: str
    column: str
    id: str | int | None
    value: str


@dataclass
class MatchedFile:
    path: str
    bucket: str
    size: int
    table: str
    column: str
    id: str | int | None
    last_modified: datetime | None = None


@dataclass
class MissingFile:
    path: str
    table: str
    column: str
    id: str | int | None
    bucket: str | None = None


@dataclass
class OrphanFile:
    path: str
    bucket: str
    size: int
    last_modified: datetime | None = None


@dataclass
class ReconciliationSummary:
    matched_count: int
    missing_count: int
    orphan_count: int
    database_file_count: int
    storage_object_count: int
    other_provider_count: int = 0


@dataclass
class ReconciliationResult:
    matched: list[MatchedFile]
    missing: list[MissingFile]
    orphan: list[OrphanFile]
    summary: ReconciliationSummary
