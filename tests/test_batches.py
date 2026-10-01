import json
import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied, ValidationError
from src.repository import Repository
from src.service import Service


CHIEF = ("chief", "chief_engineer")
DUTY = ("officer", "duty_officer")


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.repo = Repository(self.db_path)
        self.service = Service(self.repo)
        self.items = []
        for idx in range(3):
            item = self.service.create_item(
                {"title": f"order {idx}", "description": "flood dispatch",
                 "severity": "urgent", "quantity": 8 + idx, "threshold": 10,
                 "external_ref": f"B-{idx}"},
                *DUTY)
            item = self.service.transition(item["id"], "checked", item["version"], *DUTY)
            self.items.append(item)
        self.ids = [i["id"] for i in self.items]

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _audit_snapshot_ids(self):
        events = self.service.audit("viewer")
        return {e["id"]: e.get("snapshot_id") for e in events}

    def test_freeze_confirm_and_single_snapshot(self):
        # 冻结：版本与未关闭操作记录进入同一份快照
        self.service.add_record(
            self.ids[1], {"kind": "check", "detail": "reviewed", "status": "closed"},
            *DUTY)
        self.service.add_record(
            self.ids[1], {"kind": "pending", "detail": "open issue", "status": "open"},
            *DUTY)

        batch = self.service.create_batch({"item_ids": self.ids}, *CHIEF)
        self.assertEqual(batch["status"], "open")
        self.assertEqual(batch["counts"]["pending"], 3)
        snapshot_id = batch["snapshot_id"]
        self.assertTrue(snapshot_id.startswith("SNAP-"))

        entries = {e["item_id"]: e for e in batch["items"]}
        for item in self.items:
            self.assertEqual(entries[item["id"]]["frozen_version"], item["version"])
        frozen_open = entries[self.ids[1]]["frozen_open_records"]
        self.assertEqual([r["detail"] for r in frozen_open], ["open issue"])
        # 同一份快照同时落在批次、批次指令、审计记录上
        self.assertEqual(entries[self.ids[0]]["frozen_open_records"], [])
        snapshots = self._audit_snapshot_ids()
        freeze_events = [e for e in self.service.audit("viewer")
                         if e["action"] == "batch_freeze"]
        self.assertEqual(len(freeze_events), 1)
        self.assertEqual(freeze_events[0]["snapshot_id"], snapshot_id)

        result = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["counts"]["authorized"], 3)
        for item_id in self.ids:
            self.assertEqual(self.service.get_item(item_id, "viewer")["status"],
                             "authorized")
        authorize_events = [e for e in self.service.audit("viewer")
                            if e["action"] == "batch_authorize"
                            and e["entity_id"] in self.ids]
        self.assertEqual(len(authorize_events), 3)
        self.assertTrue(all(e["snapshot_id"] == snapshot_id
                            for e in authorize_events))
        self.assertTrue(self.repo.verify_audit_chain())

    def test_concurrent_record_leaves_affected_item_others_complete(self):
        batch = self.service.create_batch({"item_ids": self.ids}, *CHIEF)
        # 冻结后值班员对第一条指令并发提交了未关闭操作记录
        self.service.add_record(
            self.ids[0], {"kind": "field", "detail": "newly opened", "status": "open"},
            *DUTY)

        result = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(result["status"], "open")
        by_id = {r["item_id"]: r for r in result["results"]}
        self.assertEqual(by_id[self.ids[0]]["outcome"], "affected")
        for item_id in self.ids[1:]:
            self.assertEqual(by_id[item_id]["outcome"], "authorized")
        # 受影响指令留在批次内，仍是 checked；其余照常完成授权
        self.assertEqual(self.service.get_item(self.ids[0], "viewer")["status"],
                         "checked")
        for item_id in self.ids[1:]:
            self.assertEqual(self.service.get_item(item_id, "viewer")["status"],
                             "authorized")
        self.assertEqual(result["counts"]["affected"], 1)
        self.assertEqual(result["counts"]["authorized"], 2)

        audit_count = len(self.service.audit("viewer"))
        authorize_for_first = [e for e in self.service.audit("viewer")
                               if e["action"] == "batch_authorize"
                               and e["entity_id"] == self.ids[0]]
        self.assertEqual(authorize_for_first, [])

        # 立即重放：新增记录仍未闭环 → 仍然 affected，且审计一条不多
        replay = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(replay["counts"]["affected"], 1)
        self.assertEqual(len(self.service.audit("viewer")), audit_count)

        # 值班员闭环新增操作记录后，继续确认同一批次即可授权
        self.repo.conn.execute(
            "UPDATE records SET status='closed' WHERE item_id=? AND status='open'",
            (self.ids[0],))
        self.repo.conn.commit()
        finished = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["counts"]["authorized"], 3)
        self.assertEqual(self.service.get_item(self.ids[0], "viewer")["status"],
                         "authorized")
        self.assertEqual(len(self.service.audit("viewer")), audit_count + 2)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_retry_by_batch_no_reuses_first_result(self):
        batch = self.service.create_batch({"item_ids": self.ids}, *CHIEF)
        first = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        audit_rows = self.service.audit("viewer")
        # 写入失败后按批次号重试：整体完成，审计不重放
        second = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        third = self.service.confirm_batch(batch["id"], *CHIEF)
        self.assertEqual(second["status"], "completed")
        self.assertEqual(third["status"], "completed")
        self.assertEqual(second["results"], first["results"])
        self.assertEqual(len(self.service.audit("viewer")), len(audit_rows))
        # 每条指令的首次结果（含审计事件ID）被原样复用
        for result in third["results"]:
            stored = next(r for r in audit_rows
                          if r["id"] == result["audit_event_id"])
            self.assertEqual(stored["entity_id"], result["item_id"])

    def test_unfinished_batch_continues_after_restart(self):
        batch = self.service.create_batch({"item_ids": self.ids}, *CHIEF)
        # 冻结后第二条指令出现并发操作记录
        self.service.add_record(
            self.ids[1], {"kind": "field", "detail": "open after freeze",
                          "status": "open"}, *DUTY)
        partial = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(partial["status"], "open")

        # 模拟服务重启：新进程/新连接打开同一份数据库文件
        self.repo.close()
        self.repo = Repository(self.db_path)
        self.service = Service(self.repo)

        recovered = self.service.get_batch(batch["batch_no"], "viewer")
        self.assertEqual(recovered["snapshot_id"], batch["snapshot_id"])
        self.assertEqual(recovered["counts"]["authorized"], 2)
        self.assertEqual(recovered["counts"]["affected"], 1)
        # 未完成项仍可继续确认：先因未闭环而保留
        again = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(again["counts"]["affected"], 1)
        self.repo.conn.execute(
            "UPDATE records SET status='closed' WHERE item_id=? AND status='open'",
            (self.ids[1],))
        self.repo.conn.commit()
        done = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(done["status"], "completed")
        self.assertTrue(self.repo.verify_audit_chain())

    def test_batch_validation_and_permissions(self):
        with self.assertRaises(PermissionDenied):
            self.service.create_batch({"item_ids": self.ids}, *DUTY)
        with self.assertRaises(PermissionDenied):
            self.service.create_batch({"item_ids": self.ids},
                                      "viewer", "viewer")
        with self.assertRaises(ValidationError):
            self.service.create_batch({"item_ids": []}, *CHIEF)
        with self.assertRaises(ValidationError):
            self.service.create_batch({"item_ids": [1, 1]}, *CHIEF)
        with self.assertRaises(ValidationError):
            self.service.create_batch({"item_ids": ["1"]}, *CHIEF)

        batch = self.service.create_batch({"item_ids": [self.ids[0]]}, *CHIEF)
        with self.assertRaises(PermissionDenied):
            self.service.confirm_batch(batch["batch_no"], *DUTY)
        # 未复核的指令不能入批次
        fresh = self.service.create_item(
            {"title": "draft only", "description": "x", "severity": "routine",
             "quantity": 1, "threshold": 2, "external_ref": "B-DRAFT"}, *DUTY)
        with self.assertRaises(ConflictError):
            self.service.create_batch({"item_ids": [fresh["id"]]}, *CHIEF)
        # 已在未完成批次中的指令不能重复冻结
        with self.assertRaises(ConflictError):
            self.service.create_batch({"item_ids": [self.ids[0]]}, *CHIEF)
        with self.assertRaises(Exception):
            self.service.get_batch("NO-SUCH-BATCH", "viewer")

    def test_frozen_version_conflict_is_affected(self):
        batch = self.service.create_batch({"item_ids": self.ids}, *CHIEF)
        # 冻结后指令版本被推进（例如其他路径的更新），仍以冻结基线判定
        self.repo.conn.execute(
            "UPDATE items SET version=version+1 WHERE id=?", (self.ids[2],))
        self.repo.conn.commit()
        result = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        by_id = {r["item_id"]: r for r in result["results"]}
        self.assertEqual(by_id[self.ids[2]]["outcome"], "affected")
        self.assertIn("版本", by_id[self.ids[2]]["reason"])
        # 回到冻结版本（基线重放一致）后可授权
        self.repo.conn.execute(
            "UPDATE items SET version=? WHERE id=?",
            (batch["items"][2]["frozen_version"], self.ids[2]))
        self.repo.conn.commit()
        done = self.service.confirm_batch(batch["batch_no"], *CHIEF)
        self.assertEqual(done["status"], "completed")


if __name__ == "__main__":
    unittest.main()
