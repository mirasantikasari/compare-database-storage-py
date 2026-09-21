import asyncio
import threading
import unittest
from app.services.archive_jobs import ArchiveJobs


class ArchiveJobTests(unittest.TestCase):
    def test_reconnect_uses_same_worker_and_replays_terminal_result(self):
        jobs = ArchiveJobs()
        release = threading.Event()
        started = threading.Event()
        calls = []
        def work(emit):
            calls.append(1)
            emit("copy_progress", {"completed": 1})
            started.set()
            release.wait(2)
            emit("done", {"deletedCount": 1})
        job = jobs.connect("source", [("b", "k")], work)
        self.assertTrue(started.wait(2))
        self.assertIs(jobs.connect("source", [("b", "k")], work), job)
        self.assertIs(jobs.connect("source", [("b", "k")], work, job["id"]), job)
        release.set()
        async def read():
            return b"".join([part async for part in jobs.stream(job)])
        first = asyncio.run(read())
        second = asyncio.run(read())
        self.assertIn(b"event: done", first)
        self.assertIn(b"event: copy_progress", second)
        self.assertEqual(calls, [1])

    def test_other_report_cannot_attach(self):
        jobs = ArchiveJobs()
        release = threading.Event()
        job = jobs.connect("one", [("b", "k")], lambda emit: release.wait(2))
        try:
            with self.assertRaises(ValueError):
                jobs.connect("two", [("b", "k")], lambda emit: None, job["id"])
        finally:
            release.set()

    def test_independent_reports_run_concurrently(self):
        jobs = ArchiveJobs()
        release_one = threading.Event()
        release_two = threading.Event()
        job_one = jobs.connect("one", [("b", "k1")], lambda emit: release_one.wait(2))
        job_two = jobs.connect("two", [("b", "k2")], lambda emit: release_two.wait(2))
        try:
            self.assertNotEqual(job_one["id"], job_two["id"])
        finally:
            release_one.set()
            release_two.set()

    def test_overlapping_keys_cannot_start_new_job(self):
        jobs = ArchiveJobs()
        release = threading.Event()
        jobs.connect("one", [("b", "k")], lambda emit: release.wait(2))
        try:
            with self.assertRaises(ValueError):
                jobs.connect("two", [("b", "k")], lambda emit: None)
        finally:
            release.set()

    def test_failed_job_emits_terminal_failure(self):
        jobs = ArchiveJobs()
        def work(emit):
            raise RuntimeError("failed")
        job = jobs.connect("one", [("b", "k")], work)
        async def read():
            return b"".join([part async for part in jobs.stream(job)])
        output = asyncio.run(read())
        self.assertIn(b"event: archive_failed", output)
        self.assertIn(b"event: error", output)
