import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime

from app.config import env
from app.types import DbFileReference, ReconciliationRequest, StorageObject

_CHECKPOINT_DIR_NAME = ".checkpoints"
_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_.-]")


def _checkpoints_root() -> str:
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


def _run_dir(checkpoint_id: str) -> str:
    """
    One directory per reconciliation request, holding one small file per finished bucket/mapping
    — instead of a single ever-growing JSON. Auto reconciliation can easily discover 50-100+
    mapping columns; rewriting *everything* already finished just to record item #80 would make
    each save slower than the last (and the whole scan slower the more of it succeeds).
    """
    path = os.path.join(_checkpoints_root(), checkpoint_id)
    os.makedirs(path, exist_ok=True)
    return path


def _safe_name(name: str) -> str:
    return _SAFE_NAME_RE.sub("_", name)


def _item_path(checkpoint_id: str, kind: str, name: str) -> str:
    return os.path.join(_run_dir(checkpoint_id), f"{kind}__{_safe_name(name)}.json")


def _write_json_atomic(path: str, data) -> None:
    """Temp file + rename so a crash mid-write can never corrupt the checkpoint a retry resumes from."""
    tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp_path, path)


def _read_json(path: str) -> list | None:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_mapping_checkpoint(checkpoint_id: str, mapping_key: str) -> list[dict] | None:
    return _read_json(_item_path(checkpoint_id, "mapping", mapping_key))


def save_mapping_checkpoint(checkpoint_id: str, mapping_key: str, refs: list[dict]) -> None:
    _write_json_atomic(_item_path(checkpoint_id, "mapping", mapping_key), refs)


def load_bucket_checkpoint(checkpoint_id: str, bucket: str) -> list[dict] | None:
    return _read_json(_item_path(checkpoint_id, "bucket", bucket))


def save_bucket_checkpoint(checkpoint_id: str, bucket: str, objs: list[dict]) -> None:
    _write_json_atomic(_item_path(checkpoint_id, "bucket", bucket), objs)


def clear_checkpoint(checkpoint_id: str) -> None:
    shutil.rmtree(os.path.join(_checkpoints_root(), checkpoint_id), ignore_errors=True)


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
