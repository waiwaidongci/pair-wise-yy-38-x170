from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError, ValidationError
from .rules import (BATCH_REQUIRED_STATUS, BATCH_TARGET, ENTITY, BATCH_ENTITY,
                    ID_PREFIX, STATES)


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    snapshot_id TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL UNIQUE,
                    snapshot_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','completed')),
                    created_by TEXT NOT NULL,
                    confirmed_by TEXT,
                    created_at TEXT NOT NULL,
                    confirmed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS batch_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL REFERENCES items(id),
                    frozen_version INTEGER NOT NULL,
                    frozen_open_records TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','affected','authorized')),
                    result TEXT,
                    processed_at TEXT,
                    UNIQUE(batch_id, item_id)
                );
                CREATE INDEX IF NOT EXISTS ix_batch_items_item
                    ON batch_items(item_id);
            """)
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        """为早期数据库补齐快照列，保证重启后批次数据仍可继续确认。"""
        cols = {row["name"] for row in self.conn.execute(
            "PRAGMA table_info(audit_events)").fetchall()}
        if "snapshot_id" not in cols:
            self.conn.execute(
                "ALTER TABLE audit_events ADD COLUMN snapshot_id TEXT")

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def _insert_audit_locked(self, action: str, entity_type: str, entity_id: int,
                             actor: str, detail: dict,
                             snapshot_id: Optional[str] = None) -> Dict[str, Any]:
        """调用方必须持有 self._lock 且处于事务中。"""
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous,
                           snapshot_id)
        cur = self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, snapshot_id, created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"],
             event.get("snapshot_id"), event["created_at"]),
        )
        event["id"] = int(cur.lastrowid)
        return event

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict,
                     snapshot_id: Optional[str] = None) -> Dict[str, Any]:
        with self._lock, self.conn:
            return self._insert_audit_locked(action, entity_type, entity_id, actor,
                                             detail, snapshot_id)

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    # ------------------------------------------------------------------
    # 授权批次：冻结快照、确认授权、按批次号幂等重放
    # ------------------------------------------------------------------

    @staticmethod
    def _snapshot_id() -> str:
        return "SNAP-" + uuid.uuid4().hex

    @staticmethod
    def _batch_no(snapshot_id: str) -> str:
        return "AUTH-" + snapshot_id.replace("SNAP-", "")[:16].upper()

    def _freeze_records(self, item_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM records WHERE item_id=? AND status='open' ORDER BY id",
            (item_id,),
        ).fetchall()
        return [{"id": int(r["id"]), "kind": r["kind"], "detail": r["detail"],
                 "external_ref": r["external_ref"], "created_by": r["created_by"],
                 "created_at": r["created_at"]} for r in rows]

    def freeze_batch(self, item_ids: List[int], actor: str) -> Dict[str, Any]:
        """在同一事务/同一份快照下冻结指令版本与未关闭操作记录。"""
        if not item_ids:
            raise ValidationError("批次至少包含一条调度指令")
        if len(item_ids) != len(set(item_ids)):
            raise ValidationError("批次内指令不能重复")
        from .rules import MAX_BATCH_ITEMS
        if len(item_ids) > MAX_BATCH_ITEMS:
            raise ValidationError(f"批次最多包含{MAX_BATCH_ITEMS}条指令")
        snapshot_id = self._snapshot_id()
        batch_no = self._batch_no(snapshot_id)
        now = utc_now()
        frozen = []
        with self._lock, self.conn:
            for item_id in item_ids:
                row = self.conn.execute(
                    "SELECT id, status, version FROM items WHERE id=?", (item_id,)
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"调度指令{item_id}不存在")
                if row["status"] != BATCH_REQUIRED_STATUS:
                    raise ConflictError(
                        f"调度指令{item_id}当前状态为{row['status']}，"
                        f"须为{BATCH_REQUIRED_STATUS}才能加入授权批次")
                overlap = self.conn.execute(
                    """SELECT b.id FROM batch_items bi
                       JOIN batches b ON b.id=bi.batch_id
                       WHERE bi.item_id=? AND b.status='open' LIMIT 1""",
                    (item_id,),
                ).fetchone()
                if overlap is not None:
                    raise ConflictError(f"调度指令{item_id}已在未完成的授权批次中")
                open_records = self._freeze_records(item_id)
                frozen.append({
                    "item_id": item_id, "frozen_version": int(row["version"]),
                    "frozen_open_records": open_records,
                })
            cur = self.conn.execute(
                """INSERT INTO batches(batch_no, snapshot_id, status, created_by,
                   created_at) VALUES(?,?, 'open', ?, ?)""",
                (batch_no, snapshot_id, actor, now),
            )
            batch_id = int(cur.lastrowid)
            for entry in frozen:
                self.conn.execute(
                    """INSERT INTO batch_items(batch_id, item_id, frozen_version,
                       frozen_open_records, status) VALUES(?,?,?,?,'pending')""",
                    (batch_id, entry["item_id"], entry["frozen_version"],
                     json.dumps(entry["frozen_open_records"], ensure_ascii=False,
                                sort_keys=True)),
                )
            self._insert_audit_locked(
                "batch_freeze", BATCH_ENTITY, batch_id, actor,
                {"batch_no": batch_no, "item_ids": list(item_ids),
                 "item_count": len(item_ids),
                 "frozen": [{"item_id": e["item_id"],
                             "version": e["frozen_version"],
                             "open_records": [r["id"] for r
                                              in e["frozen_open_records"]]}
                            for e in frozen]},
                snapshot_id,
            )
        return self.get_batch(batch_id)

    def _batch_row(self, batch_id: int) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("授权批次不存在")
        return row

    @staticmethod
    def _batch_item(row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        result["frozen_open_records"] = json.loads(result["frozen_open_records"])
        if result.get("result"):
            result["result"] = json.loads(result["result"])
        return result

    def get_batch(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self._batch_row(batch_id)
            batch = dict(row)
            items = self.conn.execute(
                "SELECT * FROM batch_items WHERE batch_id=? ORDER BY id",
                (batch_id,),
            ).fetchall()
        batch["items"] = [self._batch_item(r) for r in items]
        return batch

    def get_batch_by_no(self, batch_no: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT id FROM batches WHERE batch_no=?", (batch_no,)).fetchone()
            if row is None:
                raise NotFoundError("授权批次不存在")
            batch_id = int(row["id"])
        return self.get_batch(batch_id)

    def list_batches(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT id FROM batches ORDER BY id DESC").fetchall()
        return [self.get_batch(int(r["id"])) for r in rows]

    def _complete_batch_locked(self, batch_id: int, actor: str,
                               snapshot_id: str, now: str) -> None:
        pending = self.conn.execute(
            "SELECT COUNT(*) AS n FROM batch_items WHERE batch_id=? AND status!='authorized'",
            (batch_id,),
        ).fetchone()["n"]
        if pending:
            return
        self.conn.execute(
            """UPDATE batches SET status='completed', confirmed_by=?, confirmed_at=?
               WHERE id=? AND status='open'""",
            (actor, now, batch_id),
        )
        self._insert_audit_locked(
            "batch_complete", BATCH_ENTITY, batch_id, actor,
            {"outcome": "all_authorized"}, snapshot_id,
        )

    def confirm_batch_item(self, batch_id: int, item_id: int, actor: str
                           ) -> Tuple[str, Dict[str, Any], Optional[Dict[str, Any]]]:
        """确认批次中的单条指令。

        返回 (新状态, 结果字典, 审计事件)；重放已授权项时审计事件为 None，
        保证按批次号重试不会多出审计记录。
        """
        now = utc_now()
        with self._lock, self.conn:
            batch = self._batch_row(batch_id)
            snapshot_id = batch["snapshot_id"]
            bi = self.conn.execute(
                "SELECT * FROM batch_items WHERE batch_id=? AND item_id=?",
                (batch_id, item_id),
            ).fetchone()
            if bi is None:
                raise NotFoundError(f"调度指令{item_id}不在该批次中")

            # 幂等：重放只复用首次结果，不重复写状态/审计
            if bi["status"] == "authorized":
                return "authorized", json.loads(bi["result"]), None

            item_row = self.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            # 冻结后值班员并发提交操作记录或版本发生变化 → 受影响，留在批次内等待处理
            if item_row is None:
                outcome = "affected"
                reason = "指令已被删除"
            elif item_row["status"] == BATCH_TARGET:
                # 已在批次之外被授权：直接确认，无需再写审计
                outcome = "authorized"
                reason = "already_authorized"
            elif item_row["status"] != BATCH_REQUIRED_STATUS:
                outcome = "affected"
                reason = f"状态已变为{item_row['status']}"
            elif int(item_row["version"]) != int(bi["frozen_version"]):
                outcome = "affected"
                reason = "指令版本与冻结快照不一致"
            else:
                current_open = self._freeze_records(item_id)
                frozen_open = json.loads(bi["frozen_open_records"])
                if {r["id"] for r in current_open} != {r["id"] for r in frozen_open}:
                    outcome = "affected"
                    reason = "存在冻结后提交或关闭的未关闭操作记录"
                else:
                    outcome = None
                    reason = None

            if outcome == "affected":
                # 始终以首次冻结基线为准重判，受影响项留在批次内等待值班员闭环处理；
                # 不刷新基线，保证相同输入重放得到相同结果且不会越权放行
                current_open = (self._freeze_records(item_id)
                                if item_row is not None else [])
                current_version = int(item_row["version"]) if item_row is not None \
                    else int(bi["frozen_version"])
                result = {"item_id": item_id, "outcome": "affected", "reason": reason,
                          "current_version": current_version,
                          "current_open_record_ids": [r["id"] for r in current_open],
                          "frozen_version": int(bi["frozen_version"]),
                          "frozen_open_record_ids":
                              [r["id"] for r in json.loads(bi["frozen_open_records"])]}
                self.conn.execute(
                    """UPDATE batch_items SET status='affected', result=?, processed_at=?
                       WHERE id=?""",
                    (json.dumps(result, ensure_ascii=False, sort_keys=True), now,
                     bi["id"]),
                )
                return "affected", result, None

            if outcome == "authorized":
                result = {"item_id": item_id, "outcome": "authorized",
                          "reason": "already_authorized",
                          "version": int(item_row["version"])}
                self.conn.execute(
                    """UPDATE batch_items SET status='authorized', result=?,
                       processed_at=? WHERE id=?""",
                    (json.dumps(result, ensure_ascii=False, sort_keys=True), now,
                     bi["id"]),
                )
                self._complete_batch_locked(batch_id, actor, snapshot_id, now)
                return "authorized", result, None

            # 条件更新 + 同事务审计：只在冻结版本仍匹配时授权
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=? AND status=?""",
                (BATCH_TARGET, now, item_id, int(bi["frozen_version"]),
                 BATCH_REQUIRED_STATUS),
            )
            if cur.rowcount == 0:  # 事务内并发兜底
                result = {"item_id": item_id, "outcome": "affected",
                          "reason": "确认时发生并发变化"}
                self.conn.execute(
                    "UPDATE batch_items SET status='affected', result=?, processed_at=? WHERE id=?",
                    (json.dumps(result, ensure_ascii=False, sort_keys=True), now,
                     bi["id"]),
                )
                return "affected", result, None

            event = self._insert_audit_locked(
                "batch_authorize", ENTITY, item_id, actor,
                {"batch_id": batch_id, "from": BATCH_REQUIRED_STATUS,
                 "to": BATCH_TARGET, "frozen_version": int(bi["frozen_version"]),
                 "frozen_open_records":
                     [r["id"] for r in json.loads(bi["frozen_open_records"])]},
                snapshot_id,
            )
            result = {"item_id": item_id, "outcome": "authorized",
                      "version": int(bi["frozen_version"]) + 1,
                      "audit_event_id": event["id"]}
            self.conn.execute(
                """UPDATE batch_items SET status='authorized', result=?, processed_at=?
                   WHERE id=?""",
                (json.dumps(result, ensure_ascii=False, sort_keys=True), now,
                 bi["id"]),
            )
            self._complete_batch_locked(batch_id, actor, snapshot_id, now)
            return "authorized", result, event

    def close(self) -> None:
        with self._lock:
            self.conn.close()
