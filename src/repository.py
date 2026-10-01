from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, ID_PREFIX, STATES


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
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','confirmed')),
                    target_status TEXT NOT NULL DEFAULT 'authorized',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    confirmed_by TEXT,
                    confirmed_at TEXT,
                    confirm_result TEXT
                );
                CREATE TABLE IF NOT EXISTS batch_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL
                        REFERENCES items(id) ON DELETE CASCADE,
                    frozen_version INTEGER NOT NULL,
                    frozen_status TEXT NOT NULL,
                    result TEXT NOT NULL DEFAULT 'pending'
                        CHECK(result IN ('pending','done')),
                    processed_at TEXT,
                    UNIQUE(batch_id, item_id)
                );
                CREATE TABLE IF NOT EXISTS batch_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    record_id INTEGER NOT NULL
                        REFERENCES records(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL,
                    UNIQUE(batch_id, record_id)
                );
            """)
            cols = [r[1] for r in self.conn.execute("PRAGMA table_info(audit_events)").fetchall()]
            if "batch_id" not in cols:
                self.conn.execute(
                    "ALTER TABLE audit_events ADD COLUMN batch_id INTEGER REFERENCES batches(id)"
                )

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

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

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

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # ------------------------------------------------------------------
    # 授权批次
    # ------------------------------------------------------------------
    @staticmethod
    def _batch(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def _batch_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def _batch_record(self, row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def get_batch_by_no(self, batch_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE batch_no=?", (batch_no,)
            ).fetchone()
        if row is None:
            return None
        return self._batch(row)

    def create_batch(self, batch_no: str, item_ids: List[int], target: str,
                     actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                existing = self.conn.execute(
                    "SELECT * FROM batches WHERE batch_no=?", (batch_no,)
                ).fetchone()
                if existing is not None:
                    return self._batch(existing)
                cur = self.conn.execute(
                    """INSERT INTO batches(batch_no, status, target_status, created_by, created_at)
                       VALUES(?,?,?,?,?)""",
                    (batch_no, "open", target, actor, now),
                )
                batch_id = int(cur.lastrowid)
                for item_id in item_ids:
                    item = self.conn.execute(
                        "SELECT * FROM items WHERE id=?", (item_id,)
                    ).fetchone()
                    if item is None:
                        raise NotFoundError("项目不存在")
                    self.conn.execute(
                        """INSERT INTO batch_items(batch_id, item_id, frozen_version,
                           frozen_status, result) VALUES(?,?,?,?,?)""",
                        (batch_id, item_id, item["version"], item["status"], "pending"),
                    )
                    records = self.conn.execute(
                        """SELECT * FROM records WHERE item_id=? AND status='open'
                           ORDER BY id""",
                        (item_id,),
                    ).fetchall()
                    for rec in records:
                        self.conn.execute(
                            """INSERT INTO batch_records(batch_id, record_id, item_id,
                               kind, detail, status) VALUES(?,?,?,?,?,?)""",
                            (batch_id, rec["id"], item_id, rec["kind"],
                             rec["detail"], rec["status"]),
                        )
        except sqlite3.IntegrityError:
            existing = self.get_batch_by_no(batch_no)
            if existing is not None:
                return existing
            raise ConflictError("批次号已存在或指令已在批次中")
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        batch = self._batch(row)
        with self._lock:
            items = self.conn.execute(
                "SELECT * FROM batch_items WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
            records = self.conn.execute(
                "SELECT * FROM batch_records WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        batch["items"] = [self._batch_item(r) for r in items]
        batch["records"] = [self._batch_record(r) for r in records]
        return batch

    def list_batches(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM batches ORDER BY id DESC").fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            batch = self._batch(row)
            with self._lock:
                items = self.conn.execute(
                    "SELECT * FROM batch_items WHERE batch_id=? ORDER BY id",
                    (batch["id"],),
                ).fetchall()
            batch["items"] = [self._batch_item(r) for r in items]
            result.append(batch)
        return result

    def _list_batch_items(self, batch_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM batch_items WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        return [self._batch_item(r) for r in rows]

    def _frozen_record_ids(self, batch_id: int, item_id: int) -> List[int]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT record_id FROM batch_records WHERE batch_id=? AND item_id=?
                   ORDER BY id""",
                (batch_id, item_id),
            ).fetchall()
        return [int(r["record_id"]) for r in rows]

    def _open_record_ids(self, item_id: int) -> List[int]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT id FROM records WHERE item_id=? AND status='open'
                   ORDER BY id""",
                (item_id,),
            ).fetchall()
        return [int(r["id"]) for r in rows]

    def _refreeze_batch_item(self, bi: Dict[str, Any], item: Dict[str, Any]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE batch_items SET frozen_version=?, frozen_status=? WHERE id=?",
                (item["version"], item["status"], bi["id"]),
            )
            self.conn.execute(
                "DELETE FROM batch_records WHERE batch_id=? AND item_id=?",
                (bi["batch_id"], bi["item_id"]),
            )
            records = self.conn.execute(
                """SELECT * FROM records WHERE item_id=? AND status='open'
                   ORDER BY id""",
                (bi["item_id"],),
            ).fetchall()
            for rec in records:
                self.conn.execute(
                    """INSERT INTO batch_records(batch_id, record_id, item_id,
                       kind, detail, status) VALUES(?,?,?,?,?,?)""",
                    (bi["batch_id"], rec["id"], bi["item_id"], rec["kind"],
                     rec["detail"], rec["status"]),
                )

    def _insert_audit(self, action: str, entity_type: str, entity_id: int,
                      actor: str, detail: dict,
                      batch_id: Optional[int] = None) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        cur = self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at, batch_id)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"], batch_id),
        )
        event["id"] = int(cur.lastrowid)
        return event

    def _process_batch_item(self, bi: Dict[str, Any], item: Dict[str, Any],
                            target: str, actor: str,
                            frozen_record_ids: List[int]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                "UPDATE items SET status=?, version=version+1, updated_at=? WHERE id=? AND version=?",
                (target, now, item["id"], item["version"]),
            )
            if cur.rowcount == 0:
                raise ConflictError("版本冲突，请刷新后重试")
            self._insert_audit("transition", ENTITY, item["id"], actor, {
                "from": item["status"], "to": target,
                "batch_id": bi["batch_id"],
                "frozen_version": item["version"],
                "new_version": item["version"] + 1,
                "frozen_record_ids": frozen_record_ids,
            }, bi["batch_id"])
            self.conn.execute(
                "UPDATE batch_items SET result='done', processed_at=? WHERE id=?",
                (now, bi["id"]),
            )

    def _store_confirm_result(self, batch_id: int, result: Dict[str, Any],
                              actor: str) -> None:
        with self._lock, self.conn:
            if result["status"] == "confirmed":
                self.conn.execute(
                    """UPDATE batches SET status='confirmed', confirmed_by=?,
                       confirmed_at=?, confirm_result=? WHERE id=?""",
                    (actor, utc_now(), json.dumps(result, ensure_ascii=False), batch_id),
                )
            else:
                self.conn.execute(
                    "UPDATE batches SET confirm_result=? WHERE id=?",
                    (json.dumps(result, ensure_ascii=False), batch_id),
                )

    def confirm_batch(self, batch_id: int, actor: str,
                      resume: bool = False) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        batch = self._batch(row)
        if batch["status"] == "confirmed":
            return json.loads(batch["confirm_result"])
        target = batch["target_status"]
        batch_items = self._list_batch_items(batch_id)
        done: List[int] = []
        pending: List[int] = []
        for bi in batch_items:
            if bi["result"] == "done":
                done.append(bi["item_id"])
                continue
            item = self.get_item(bi["item_id"])
            if resume:
                self._refreeze_batch_item(bi, item)
                frozen_version = item["version"]
                frozen_status = item["status"]
            else:
                frozen_version = bi["frozen_version"]
                frozen_status = bi["frozen_status"]
            if item["version"] == frozen_version and item["status"] == frozen_status:
                current_record_ids = self._open_record_ids(bi["item_id"])
                frozen_record_ids = self._frozen_record_ids(batch_id, bi["item_id"])
                if current_record_ids == frozen_record_ids:
                    try:
                        self._process_batch_item(bi, item, target, actor, frozen_record_ids)
                    except ConflictError:
                        pending.append(bi["item_id"])
                        continue
                    done.append(bi["item_id"])
                else:
                    pending.append(bi["item_id"])
            else:
                pending.append(bi["item_id"])
        result = {
            "batch_id": batch_id,
            "batch_no": batch["batch_no"],
            "status": "confirmed" if not pending else "open",
            "done": done,
            "pending": pending,
        }
        self._store_confirm_result(batch_id, result, actor)
        return result
