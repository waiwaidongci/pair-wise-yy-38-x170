import json
import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied
from src.repository import Repository
from src.service import Service


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.repo = Repository(self.db_path)
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _make_item(self, ref, severity="urgent", quantity=12, threshold=6):
        return self.service.create_item(
            {"title": f"item {ref}", "description": "d", "severity": severity,
             "quantity": quantity, "threshold": threshold, "external_ref": ref},
            "creator", "duty_officer")

    def _check(self, item):
        return self.service.transition(
            item["id"], "checked", item["version"], "reviewer", "duty_officer")

    def test_batch_freezes_version_and_open_records(self):
        item = self._make_item("B-1")
        item = self._check(item)
        # add an open record
        self.service.add_record(
            item["id"], {"kind": "evidence", "detail": "ev", "status": "open",
                         "external_ref": "R-1"}, "recorder", "duty_officer")
        batch = self.service.create_batch(
            {"batch_no": "BATCH-1", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        self.assertEqual(batch["status"], "open")
        self.assertEqual(len(batch["items"]), 1)
        bi = batch["items"][0]
        self.assertEqual(bi["frozen_version"], item["version"])
        self.assertEqual(bi["frozen_status"], "checked")
        self.assertEqual(bi["result"], "pending")
        # open record frozen
        self.assertEqual(len(batch["records"]), 1)
        self.assertEqual(batch["records"][0]["record_id"], 1)

    def test_confirm_completes_unchanged_defers_modified(self):
        item1 = self._make_item("C-1")
        item2 = self._make_item("C-2")
        self._check(item1)
        self._check(item2)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-2", "item_ids": [item1["id"], item2["id"]],
             "target": "authorized"}, "chief", "chief_engineer")
        # concurrently add a record to item2 (bumps version)
        self.service.add_record(
            item2["id"], {"kind": "evidence", "detail": "new ev", "status": "open",
                          "external_ref": "R-2"}, "recorder", "duty_officer")
        result = self.service.confirm_batch(batch["id"], "chief", "chief_engineer")
        self.assertEqual(result["status"], "open")
        self.assertEqual(result["done"], [item1["id"]])
        self.assertEqual(result["pending"], [item2["id"]])
        # item1 authorized, item2 still checked
        self.assertEqual(self.service.get_item(item1["id"], "viewer")["status"], "authorized")
        self.assertEqual(self.service.get_item(item2["id"], "viewer")["status"], "checked")

    def test_idempotent_retry_no_duplicate_audit(self):
        item = self._make_item("D-1")
        self._check(item)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-3", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        r1 = self.service.confirm_batch(batch["id"], "chief", "chief_engineer")
        audit_count_1 = len(self.service.audit("viewer", item["id"]))
        # retry with same batch number -> reuse first result
        r2 = self.service.confirm_batch(batch["id"], "chief", "chief_engineer")
        audit_count_2 = len(self.service.audit("viewer", item["id"]))
        self.assertEqual(r1, r2)
        self.assertEqual(audit_count_1, audit_count_2)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_resume_processes_pending_items(self):
        item = self._make_item("E-1")
        self._check(item)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-4", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        # modify item -> pending
        self.service.add_record(
            item["id"], {"kind": "evidence", "detail": "ev", "status": "open",
                         "external_ref": "R-4"}, "recorder", "duty_officer")
        r1 = self.service.confirm_batch(batch["id"], "chief", "chief_engineer")
        self.assertEqual(r1["pending"], [item["id"]])
        # resume (continue confirming) -> re-freeze and authorize
        r2 = self.service.confirm_batch(batch["id"], "chief", "chief_engineer", resume=True)
        self.assertEqual(r2["status"], "confirmed")
        self.assertEqual(r2["done"], [item["id"]])
        self.assertEqual(r2["pending"], [])
        self.assertEqual(self.service.get_item(item["id"], "viewer")["status"], "authorized")
        self.assertTrue(self.repo.verify_audit_chain())

    def test_restart_persistence(self):
        item = self._make_item("F-1")
        self._check(item)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-5", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        # simulate restart: new repo/service on same db file
        self.repo.close()
        repo2 = Repository(self.db_path)
        service2 = Service(repo2)
        result = service2.confirm_batch(batch["id"], "chief", "chief_engineer")
        self.assertEqual(result["status"], "confirmed")
        self.assertEqual(service2.get_item(item["id"], "viewer")["status"], "authorized")
        self.assertTrue(repo2.verify_audit_chain())
        repo2.close()

    def test_create_batch_idempotent_by_batch_no(self):
        item = self._make_item("G-1")
        self._check(item)
        payload = {"batch_no": "BATCH-6", "item_ids": [item["id"]], "target": "authorized"}
        b1 = self.service.create_batch(payload, "chief", "chief_engineer")
        b2 = self.service.create_batch(payload, "chief", "chief_engineer")
        self.assertEqual(b1["id"], b2["id"])

    def test_permission_denied_for_non_chief(self):
        item = self._make_item("H-1")
        self._check(item)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-7", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        with self.assertRaises(PermissionDenied):
            self.service.confirm_batch(batch["id"], "viewer", "viewer")

    def test_audit_references_frozen_snapshot(self):
        item = self._make_item("I-1")
        item = self._check(item)
        batch = self.service.create_batch(
            {"batch_no": "BATCH-8", "item_ids": [item["id"]], "target": "authorized"},
            "chief", "chief_engineer")
        self.service.confirm_batch(batch["id"], "chief", "chief_engineer")
        events = self.service.audit("viewer", item["id"])
        transition_events = [e for e in events
                            if e["action"] == "transition" and e["detail"].get("to") == "authorized"]
        self.assertEqual(len(transition_events), 1)
        detail = transition_events[0]["detail"]
        self.assertEqual(detail["batch_id"], batch["id"])
        self.assertEqual(detail["frozen_version"], item["version"])
        self.assertEqual(detail["new_version"], item["version"] + 1)
        self.assertEqual(detail["frozen_record_ids"], [])


if __name__ == "__main__":
    unittest.main()
