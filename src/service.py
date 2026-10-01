from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import ensure_role, normalize_severity, require_number, require_text
from .repository import Repository
from .rules import (AUDIT_ROLES, BATCH_ITEM_STATUSES, BATCH_ROLES,
                    CREATE_ROLES, ENTITY, RECORD_ROLES, TITLE, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ------------------------------------------------------------------
    # 授权批次：冻结快照 → 总工确认 → 按批次号幂等重试
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_item_view(entry: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "item_id": entry["item_id"],
            "status": entry["status"],
            "frozen_version": entry["frozen_version"],
            "frozen_open_records": entry["frozen_open_records"],
            "result": entry["result"],
            "processed_at": entry["processed_at"],
        }

    def _batch_view(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        items = [self._batch_item_view(e) for e in batch["items"]]
        counts = {status: 0 for status in BATCH_ITEM_STATUSES}
        for entry in items:
            counts[entry["status"]] += 1
        return {
            "id": batch["id"],
            "batch_no": batch["batch_no"],
            "snapshot_id": batch["snapshot_id"],
            "status": batch["status"],
            "created_by": batch["created_by"],
            "confirmed_by": batch["confirmed_by"],
            "created_at": batch["created_at"],
            "confirmed_at": batch["confirmed_at"],
            "items": items,
            "results": [entry["result"] for entry in items if entry["result"]],
            "counts": counts,
        }

    def create_batch(self, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        ensure_role(role, BATCH_ROLES)
        actor = require_text(actor, "actor", 100)
        raw_ids = payload.get("item_ids")
        if not isinstance(raw_ids, list) or not raw_ids:
            from .domain import ValidationError
            raise ValidationError("item_ids必须是非空数组")
        item_ids = []
        for value in raw_ids:
            if isinstance(value, bool) or not isinstance(value, int):
                from .domain import ValidationError
                raise ValidationError("item_ids必须全部是整数")
            if value < 1:
                from .domain import ValidationError
                raise ValidationError("item_ids必须是正整数")
            item_ids.append(value)
        batch = self.repository.freeze_batch(item_ids, actor)
        return self._batch_view(batch)

    def confirm_batch(self, identifier: Any, actor: str, role: str) -> Dict[str, Any]:
        """总工确认批次；可按批次号(batch_no)或数字ID调用，重试天然幂等。"""
        ensure_role(role, BATCH_ROLES)
        actor = require_text(actor, "actor", 100)
        if isinstance(identifier, int) and not isinstance(identifier, bool):
            batch = self.repository.get_batch(identifier)
        elif isinstance(identifier, str) and identifier.strip():
            batch = self.repository.get_batch_by_no(identifier.strip())
        else:
            from .domain import ValidationError
            raise ValidationError("批次号不能为空")

        # 已完成批次：整体复用首次结果，不产生任何写入或审计
        if batch["status"] == "completed":
            return self._batch_view(batch)

        for entry in batch["items"]:
            self.repository.confirm_batch_item(batch["id"], entry["item_id"], actor)
        return self._batch_view(self.repository.get_batch(batch["id"]))

    def get_batch(self, identifier: Any, role: str) -> Dict[str, Any]:
        self._view(role)
        if isinstance(identifier, int) and not isinstance(identifier, bool):
            batch = self.repository.get_batch(identifier)
        elif isinstance(identifier, str) and identifier.strip():
            batch = self.repository.get_batch_by_no(identifier.strip())
        else:
            from .domain import ValidationError
            raise ValidationError("批次号不能为空")
        return self._batch_view(batch)

    def list_batches(self, role: str) -> list:
        self._view(role)
        return [self._batch_view(b) for b in self.repository.list_batches()]

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
