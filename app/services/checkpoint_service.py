import hashlib
import json
import os
from datetime import datetime

from app.config import env
from app.types import DbFileReference, ReconciliationRequest, StorageObject

_CHECKPOINT_DIR_NAME = ".checkpoints"


def _checkpoint_dir() -> str:
    path = os.path.join(env.reports_dir, _CHECKPOINT_DIR_NAME)
    os.makedirs(path, exist_ok=True)
    return path


def checkpoint_id_for(request: ReconciliationRequest) -> str:
    """
    Stable id for "this exact reconciliation request", so re-running it (e.g. after fixing a
    transient DB/S3 error) resumes from whatever buckets/mappings already finished last time,
    while a request with different buckets/mappings/prefix/database/provider always gets its
    own checkpoint instead of accidentally reusing unrelated data.
    """
    key = json.dumps(
        {
            "buckets": sorted(request.buckets) if request.buckets else None,
            "prefix": request.prefix,
            "database": request.database,
            "provider": request.provider,
            "mappings": sorted(f"{m.table}.{m.column}.{m.id_column or 'id'}" for m in request.mappings),
        },
        sort_keys=True,
    )
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _path_for(checkpoint_id: str) -> str:
    return os.path.join(_checkpoint_dir(), f"{checkpoint_id}.json")


def load_checkpoint(checkpoint_id: str) -> dict:
    path = _path_for(checkpoint_id)
    if not os.path.exists(path):
        return {"buckets": {}, "mappings": {}}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_checkpoint(checkpoint_id: str, data: dict) -> None:
    """Temp file + rename so a crash mid-write can never corrupt the checkpoint a retry resumes from."""
    path = _path_for(checkpoint_id)
    tmp_path = f"{path}.{os.getpid()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp_path, path)


def clear_checkpoint(checkpoint_id: str) -> None:
    path = _path_for(checkpoint_id)
    if os.path.exists(path):
        os.remove(path)


def storage_object_to_dict(obj: StorageObject) -> dict:
    return {
        "bucket": obj.bucket,
        "key": obj.key,
        "size": obj.size,
        "lastModified": obj.last_modified.isoformat() if obj.last_modified else None,
        "etag": obj.etag,
    }


def storage_object_from_dict(d: dict) -> StorageObject:
    return StorageObject(
        bucket=d["bucket"],
        key=d["key"],
        size=d["size"],
        last_modified=datetime.fromisoformat(d["lastModified"]) if d.get("lastModified") else None,
        etag=d.get("etag"),
    )


def db_reference_to_dict(ref: DbFileReference) -> dict:
    return {"table": ref.table, "column": ref.column, "id": ref.id, "value": ref.value}


def db_reference_from_dict(d: dict) -> DbFileReference:
    return DbFileReference(table=d["table"], column=d["column"], id=d["id"], value=d["value"])
