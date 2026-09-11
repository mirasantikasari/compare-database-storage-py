"""Shared archive size totals for API responses and Excel reports."""
import math


def archive_size_summary(results, requested=(), excluded=()):
    requests = {(r.get("bucket"), r.get("key")): r for r in requested}
    totals = {name: {"count": 0, "sizeGb": 0.0, "unknownSizeCount": 0, "estimatedSizeCount": 0}
              for name in ("total", "deleted", "archivedNotDeleted", "copyFailed", "excluded")}

    def add(group, result, request):
        group["count"] += 1
        raw = result.get("sizeBytes")
        estimated = raw is None
        if estimated:
            raw = request.get("sizeMb")
        try:
            size = float(str(raw).replace(",", "."))
            if not math.isfinite(size) or size < 0:
                raise ValueError()
        except (TypeError, ValueError):
            group["unknownSizeCount"] += 1
            return
        group["sizeGb"] += size / (1024 if estimated else 1024 ** 3)
        group["estimatedSizeCount"] += int(estimated)

    for result in results:
        request = requests.get((result.get("bucket"), result.get("key")), {})
        category = "deleted" if result.get("deleteSuccess") else (
            "archivedNotDeleted" if result.get("copySuccess") else "copyFailed")
        add(totals[category], result, request)
        add(totals["total"], result, request)
    for row in excluded:
        add(totals["excluded"], {}, row)
    return totals
