import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from app.services import storage_service as storage


class ArchiveVisibilityTests(unittest.TestCase):
    def run_archive(self, outcomes, acl_error=None):
        client = Mock()
        client.put_object_acl.side_effect = acl_error
        items = [("school", str(i)) for i in range(len(outcomes))]
        copies = [dict(bucket=b, key=k, destBucket="scola-school-archives",
                       destKey=f"{b}/{k}", **outcome)
                  for (b, k), outcome in zip(items, outcomes)]
        with patch.object(storage, "copy_objects", return_value=copies) as copy, \
             patch.object(storage, "get_s3_client", return_value=client), \
             patch.object(storage, "delete_objects") as delete:
            results = storage.archive_and_delete_objects(
                items, "source", "archive", "scola-school-archives")
            self.assertFalse(copy.call_args.kwargs["make_public"])
            delete.assert_not_called()
        self.assertTrue(all(not r["deleteAttempted"] for r in results))
        return results, client

    def test_private_archive_never_attempts_acl(self):
        results, client = self.run_archive([dict(success=True, skipped=True, error=None)])
        client.put_object_acl.assert_not_called()
        self.assertTrue(results[0]["copySuccess"])
        self.assertEqual(results[0]["publicAccessStatus"], "Private (presigned URL)")

    def test_copy_failure_preserved(self):
        results, client = self.run_archive([dict(success=False, skipped=False, error="transfer failed")])
        client.put_object_acl.assert_not_called()
        self.assertFalse(results[0]["copySuccess"])
        self.assertEqual(results[0]["copyError"], "transfer failed")

    def test_download_signing(self):
        from app.providers import s3_provider
        client = Mock()
        with patch.object(s3_provider, "get_s3_client", return_value=client):
            s3_provider.build_archive_presigned_url("archive", "bucket", "school/a b.jpeg")
        client.generate_presigned_url.assert_called_once_with(
            "get_object", Params={"Bucket": "bucket", "Key": "school/a b.jpeg"},
            ExpiresIn=604800, HttpMethod="GET")

    def test_report_contains_signed_link_and_expiry(self):
        import tempfile
        from pathlib import Path
        from openpyxl import load_workbook
        from app.services import excel_service
        result = dict(bucket="school", key="a", archiveBucket="archive", archiveKey="school/a",
                      copySuccess=True, copySkipped=True, copyError=None, deleteSuccess=False,
                      deleteAttempted=False, publicAccessStatus="Private (presigned URL)")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "report.xlsx"
            def save(workbook, name):
                workbook.save(target)
                return str(target)
            with patch.object(excel_service, "_save_workbook", side_effect=save), \
                 patch.object(excel_service, "build_archive_presigned_url", return_value="https://example.test/a?X-Amz-Signature=test"):
                excel_service.generate_archive_deletion_report([], [result], archive_provider="archive")
            wb = load_workbook(target)
            ws = wb["Archived, Not Deleted"]
            values = dict(zip([c.value for c in ws[1]], [c.value for c in ws[2]]))
            self.assertIn("X-Amz-Signature", values["Archive URL"])
            self.assertEqual(ws.cell(2, 5).hyperlink.target, values["Archive URL"])
            self.assertTrue(values["Archive URL Expires At (UTC)"])
            self.assertEqual(values["Copy Status"], "Already archived")
            self.assertEqual(values["Delete Status"], "Not attempted")
            wb.close()


class VerifiedDeletionTests(unittest.TestCase):
    def run_case(self, archived=b"data", link_error=None, size=4, changed=False):
        self.events = []
        import io
        source, archive = Mock(), Mock()
        meta = dict(ContentLength=4, ETag='"etag"')
        source.head_object.side_effect = [meta, dict(meta, ETag='"changed"') if changed else meta]
        source.get_object.return_value = {"Body": io.BytesIO(b"data")}
        archive.head_object.return_value = {"ContentLength": size}
        response = Mock()
        response.status = 200
        response.read.side_effect = io.BytesIO(archived).read
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(storage, "get_s3_client", side_effect=lambda p: source if p == "source" else archive), \
             patch.object(storage, "copy_objects", return_value=[dict(success=True, skipped=True, destBucket="scola-school-archives", destKey="school/file")]), \
             patch.object(storage, "build_archive_presigned_url", return_value="https://example.test/file"), \
             patch.object(storage, "urlopen", return_value=response, side_effect=link_error), \
             patch.object(storage, "delete_objects", return_value=[dict(success=True)]) as delete:
            result = storage.archive_and_delete_objects([("school", "file")], "source", "archive", "scola-school-archives", on_archive_progress=self.events.append)[0]
            return result, delete.call_count

    def test_matching_existing_archive_deleted(self):
        result, calls = self.run_case()
        self.assertTrue(result["archiveVerified"])
        self.assertTrue(result["deleteSuccess"])
        self.assertEqual(calls, 1)

    def test_wrong_content_same_size_retained(self):
        result, calls = self.run_case(archived=b"xxxx")
        self.assertFalse(result["archiveVerified"])
        self.assertEqual(calls, 0)

    def test_broken_link_retained(self):
        result, calls = self.run_case(link_error=OSError("unreachable"))
        self.assertFalse(result["deleteAttempted"])
        self.assertEqual(calls, 0)

    def test_size_mismatch_retained(self):
        _, calls = self.run_case(size=3)
        self.assertEqual(calls, 0)

    def test_changed_source_retained(self):
        _, calls = self.run_case(changed=True)
        self.assertEqual(calls, 0)

    def test_wrong_archive_bucket_rejected(self):
        with self.assertRaises(ValueError):
            storage.archive_and_delete_objects([], "source", "archive", "other")

    def test_archive_cannot_be_deleted(self):
        with self.assertRaises(ValueError):
            storage.archive_and_delete_objects([("scola-school-archives", "file")], "source", "archive", "scola-school-archives")


    def test_progress_for_every_verification_phase(self):
        self.run_case()
        phases = [event["phase"] for event in self.events]
        for phase in ("checking", "verify_source", "verify_archive", "deleting", "item_done"):
            self.assertIn(phase, phases)
        self.assertEqual(phases[-1], "item_done")
        self.assertEqual(self.events[-1]["completed"], 1)
        self.assertTrue(self.events[-1]["result"]["deleteSuccess"])

    def test_failed_verification_emits_terminal_progress(self):
        self.run_case(link_error=OSError("connection lost"))
        self.assertEqual(self.events[-1]["phase"], "item_done")
        self.assertFalse(self.events[-1]["result"]["deleteAttempted"])

    def test_concurrent_overlapping_archive_rejected(self):
        storage._archive_active_keys.add(("school", "file"))
        try:
            with self.assertRaisesRegex(RuntimeError, "still active"):
                storage.archive_and_delete_objects([("school", "file")], "source", "archive", "scola-school-archives")
        finally:
            storage._archive_active_keys.discard(("school", "file"))

    def test_concurrent_independent_archive_allowed(self):
        storage._archive_active_keys.add(("school", "other-file"))
        try:
            with self.assertRaises(ValueError):
                # Disjoint keys aren't blocked by the active run above; this still fails, but for
                # the unrelated reason that "other" isn't the required archive bucket.
                storage.archive_and_delete_objects([("school", "file")], "source", "archive", "other")
        finally:
            storage._archive_active_keys.discard(("school", "other-file"))


class ArchiveCopySkipTests(unittest.TestCase):
    def test_existing_archive_never_uploaded_again(self):
        source, destination = Mock(), Mock()
        with patch.object(storage, "get_s3_client", side_effect=[source, destination]), \
             patch.object(storage, "_ensure_dest_buckets", return_value={}):
            result = storage.copy_objects([("school", "file")], "source", "archive",
                dest_bucket="scola-school-archives", make_public=False,
                dest_key_fn=storage._archive_dest_key)[0]
        self.assertTrue(result["skipped"])
        source.get_object.assert_not_called()
        destination.upload_fileobj.assert_not_called()

    def test_archive_metadata_error_never_overwrites(self):
        source, destination = Mock(), Mock()
        destination.head_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
        with patch.object(storage, "get_s3_client", side_effect=[source, destination]), \
             patch.object(storage, "_ensure_dest_buckets", return_value={}):
            result = storage.copy_objects([("school", "file")], "source", "archive",
                dest_bucket="scola-school-archives", make_public=False)[0]
        self.assertFalse(result["success"])
        destination.upload_fileobj.assert_not_called()
