"""Reconnectable archive streams. Workers live independently of HTTP connections."""
import asyncio
import threading
import uuid
from .sse import sse_format


class ArchiveJobs:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}

    def connect(self, provider, items, work, job_id=None):
        keys = frozenset(items)
        with self.lock:
            if job_id:
                job = self.jobs.get(job_id)
                if job is None:
                    raise ValueError("Proses tidak tersedia setelah server restart. Muat ulang report untuk melanjutkan.")
                if job["provider"] != provider or not keys.issubset(job["keys"]):
                    raise ValueError("Report/provider berbeda dari proses yang ingin disambungkan.")
                return job
            for job in self.jobs.values():
                if not job["finished"]:
                    if job["provider"] == provider and keys.issubset(job["keys"]):
                        return job
                    raise ValueError("Proses arsip untuk report/provider lain masih berjalan.")
            # Retain recent completed jobs for reconnect after the terminal event was lost.
            if len(self.jobs) >= 8:
                self.jobs.pop(next(iter(self.jobs)))
            job = dict(id=uuid.uuid4().hex, provider=provider, keys=keys,
                       events={}, sequence=0, finished=False)
            self.jobs[job["id"]] = job
            threading.Thread(target=self._run, args=(job, work), daemon=True).start()
            return job

    def _run(self, job, work):
        def emit(event, data):
            with self.lock:
                job["sequence"] += 1
                sequence = job["sequence"]
                # Byte updates and in-progress verification phases are snapshots, keyed per item
                # so concurrent items don't overwrite each other's in-flight state; outcomes
                # (item_done, and the global archive_phase messages) remain available for replay.
                if event == "archive_bytes":
                    key = (event, data.get("key"))
                elif event == "archive_progress" and data.get("phase") != "item_done":
                    key = (event, data.get("bucket"), data.get("key"))
                elif event == "archive_phase" and data.get("phase") != "item_done":
                    key = (event, "current")
                else:
                    key = sequence
                job["events"][key] = (sequence, event, data)
        try:
            work(emit)
        except Exception as error:
            emit("archive_failed", {"message": str(error)})
            emit("error", {"message": str(error)})
        finally:
            with self.lock:
                job["finished"] = True

    async def stream(self, job):
        yield sse_format("archive_job", {"jobId": job["id"], "total": len(job["keys"])})
        cursor = 0
        idle = 0
        while True:
            with self.lock:
                events = sorted((e for e in job["events"].values() if e[0] > cursor), key=lambda e: e[0])
                finished = job["finished"]
            for sequence, event, data in events:
                cursor = sequence
                yield sse_format(event, data)
            if finished:
                break
            idle = 0 if events else idle + 1
            if idle >= 40:
                yield b": heartbeat\n\n"
                idle = 0
            await asyncio.sleep(0.25)


archive_jobs = ArchiveJobs()
