"""处置方案版本化与执行台账服务。

方案（disposal_plans）是纯追加的版本链：初始处置、风险撤回、范围变化都只能
产生新版本；每个版本对应一份执行台账（disposal_ledgers），步骤与仅追加的
哈希链事件完整保留“当时决定了什么、依据哪个风险快照、谁在何时完成了什么”。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Callable

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, InvalidState, NotFound, ValidationFailed
from .risk import violation_probability
from .storage import audit, rows, transaction

STEP_CODES = ("notification", "isolation", "clearing", "waste_manifest", "review", "closeout")
DEFAULT_WINDOWS_HOURS = {
    "notification": 24,
    "isolation": 24,
    "clearing": 72,
    "waste_manifest": 48,
    "review": 72,
    "closeout": 48,
}
# 现场执行类事件的参与人不得独立结案
EXECUTION_EVENTS = {
    "notification.acked",
    "isolation.reported",
    "clearing.reported",
    "waste.reported",
    "review.submitted",
}
GENESIS = "GENESIS"


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    import hashlib

    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class DisposalLedgerService:
    def __init__(self, db: sqlite3.Connection, auth, clock=None) -> None:
        self.db = db
        self.auth = auth
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 基础工具
    def _now_text(self) -> str:
        return utc_text(self.clock.now())

    def _actor(self, token: str, permission: str):
        return self.auth.require(token, permission)

    def _ticket(self, ticket_id: str) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT * FROM treatment_tickets WHERE treatment_ticket_id=?", (ticket_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"处置工单不存在: {ticket_id}")
        return row

    def _ledger_row(self, ledger_id: str) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM disposal_ledgers WHERE ledger_id=?", (ledger_id,)).fetchone()
        if row is None:
            raise NotFound(f"执行台账不存在: {ledger_id}")
        return row

    def _plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM disposal_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFound(f"处置方案不存在: {plan_id}")
        return row

    def _event(self, db, ledger_id, plan_id, plan_version, step_id, event_type, payload, actor,
               idempotency_key: str | None = None, created_at: str | None = None) -> str:
        """向仅追加事件流写入哈希链事件，返回事件哈希。"""
        created_at = created_at or self._now_text()
        prev = db.execute(
            "SELECT event_hash FROM disposal_events WHERE ledger_id=? ORDER BY event_id DESC LIMIT 1",
            (ledger_id,),
        ).fetchone()
        previous_hash = prev[0] if prev else GENESIS
        body = canonical({
            "ledger_id": ledger_id,
            "plan_id": plan_id,
            "plan_version": plan_version,
            "step_id": step_id,
            "event_type": event_type,
            "payload": payload,
            "idempotency_key": idempotency_key,
            "actor": actor,
            "created_at": created_at,
        })
        event_hash = digest(previous_hash + "|" + body)
        db.execute(
            "INSERT INTO disposal_events(ledger_id,plan_id,plan_version,step_id,event_type,payload_json,"
            "idempotency_key,actor,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (ledger_id, plan_id, plan_version, step_id, event_type, canonical(payload),
             idempotency_key, actor, previous_hash, event_hash, created_at),
        )
        return event_hash

    def _idempotent(self, db, ledger_id: str, key: str, request: dict[str, Any],
                    work: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        """同一台账内幂等键去重：同内容重放直接返回首次结果，不同内容拒绝。"""
        if not key or not str(key).strip():
            raise ValidationFailed("外协上报必须携带 idempotency_key")
        request_hash = digest(request)
        row = db.execute(
            "SELECT request_sha256,response_json FROM disposal_idempotency WHERE ledger_id=? AND idempotency_key=?",
            (ledger_id, key),
        ).fetchone()
        if row is not None:
            if row["request_sha256"] != request_hash:
                raise Conflict("同一幂等键对应了不同的上报内容")
            cached = json.loads(row["response_json"])
            cached["duplicate"] = True
            return cached
        result = work()
        result["duplicate"] = False
        db.execute(
            "INSERT INTO disposal_idempotency(ledger_id,idempotency_key,request_sha256,response_json,created_at)"
            " VALUES(?,?,?,?,?)",
            (ledger_id, key, request_hash, canonical(result), self._now_text()),
        )
        return result

    def _steps(self, db, ledger_id: str) -> list[sqlite3.Row]:
        return db.execute(
            "SELECT * FROM disposal_steps WHERE ledger_id=? ORDER BY step_seq,step_id", (ledger_id,)
        ).fetchall()

    def _find_step(self, db, ledger_id, step_code, zone_code=None, statuses=("pending", "in_progress")):
        query = "SELECT * FROM disposal_steps WHERE ledger_id=? AND step_code=? AND status IN ('%s')" \
            % "','".join(statuses)
        args: list[Any] = [ledger_id, step_code]
        if zone_code is not None:
            query += " AND zone_code=?"
            args.append(zone_code)
        query += " ORDER BY attempt DESC,step_seq DESC LIMIT 1"
        row = db.execute(query, args).fetchone()
        if row is None:
            raise InvalidState(f"步骤当前不可上报: {step_code}" + (f"/{zone_code}" if zone_code else ""))
        return row

    def _deadline(self, anchor, windows: dict[str, float], code: str) -> str:
        return utc_text(anchor + timedelta(hours=float(windows[code])))

    # ------------------------------------------------------------------ 校验
    @staticmethod
    def _validate_scope(scope: Any) -> dict[str, Any]:
        if not isinstance(scope, dict):
            raise ValidationFailed("impact_scope 必须是对象")
        species = str(scope.get("species", "")).strip()
        zones = scope.get("zone_codes")
        area = scope.get("area_m2")
        if not species or not isinstance(zones, list) or not zones:
            raise ValidationFailed("impact_scope 需要 species 与非空 zone_codes")
        if any(not str(z).strip() for z in zones) or len(set(zones)) != len(zones):
            raise ValidationFailed("zone_codes 不能为空且不能重复")
        if not isinstance(area, (int, float)) or area <= 0:
            raise ValidationFailed("area_m2 必须为正数")
        return {"species": species, "zone_codes": [str(z).strip() for z in zones],
                "area_m2": float(area), "polygon": scope.get("polygon")}

    @staticmethod
    def _validate_windows(overrides: Any) -> dict[str, float]:
        windows = dict(DEFAULT_WINDOWS_HOURS)
        if overrides is None:
            return windows
        if not isinstance(overrides, dict):
            raise ValidationFailed("windows 必须是对象")
        for key, value in overrides.items():
            if key not in DEFAULT_WINDOWS_HOURS:
                raise ValidationFailed(f"未知时限键: {key}")
            if not isinstance(value, (int, float)) or value <= 0:
                raise ValidationFailed(f"时限必须为正数小时: {key}")
            windows[key] = float(value)
        return windows

    def _risk_snapshot(self, ticket: sqlite3.Row) -> dict[str, Any]:
        """快照当时有效的风险记录（告警）与园区风险概率。"""
        alert = self.db.execute("SELECT * FROM alerts WHERE alert_id=?", (ticket["alert_id"],)).fetchone()
        if alert is None:
            raise NotFound("告警记录不存在，无法生成风险快照")
        alerts = rows(self.db, "SELECT * FROM alerts WHERE zone_record_id=? ORDER BY created_at",
                      (ticket["zone_record_id"],))
        return {
            "alert_id": alert["alert_id"],
            "zone_record_id": alert["zone_record_id"],
            "severity": alert["severity"],
            "score": alert["score"],
            "status": alert["status"],
            "violation_probability": violation_probability(alerts),
            "captured_at": self._now_text(),
        }

    # ------------------------------------------------------------------ 风险撤回
    def withdraw_risk(self, token, alert_id, reason):
        actor = self._actor(token, "approve")
        reason = str(reason or "").strip()
        if not reason:
            raise ValidationFailed("风险撤回必须填写原因")
        with transaction(self.db):
            row = self.db.execute("SELECT status FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            if row is None:
                raise NotFound(f"告警不存在: {alert_id}")
            if row["status"] == "withdrawn":
                raise Conflict("风险记录已撤回，撤回不能重复生效；如范围再变请生成新版本")
            self.db.execute("UPDATE alerts SET status='withdrawn',resolved_at=? WHERE alert_id=?",
                            (self._now_text(), alert_id))
            audit(self.db, "alert", alert_id, "risk_withdrawn", actor.user_id, {"reason": reason})
            self._event_for_alert(alert_id, "risk.withdrawn", {"reason": reason}, actor.user_id)
        return {"alert_id": alert_id, "status": "withdrawn", "withdrawn_at": self._now_text()}

    def _event_for_alert(self, alert_id, event_type, payload, actor):
        """若告警已有方案台账，撤回事件同时记入各活跃台账事件流，保证可溯。"""
        ledgers = self.db.execute(
            "SELECT l.ledger_id,l.plan_id,l.plan_version FROM disposal_ledgers l "
            "JOIN disposal_plans p ON p.plan_id=l.plan_id "
            "JOIN treatment_tickets t ON t.treatment_ticket_id=p.treatment_ticket_id "
            "WHERE t.alert_id=? AND l.state IN ('active','rework')",
            (alert_id,),
        ).fetchall()
        for l in ledgers:
            self._event(self.db, l["ledger_id"], l["plan_id"], l["plan_version"], None,
                        event_type, payload, actor)

    # ------------------------------------------------------------------ 方案版本
    def create_plan(self, token, treatment_ticket_id, impact_scope, windows=None,
                    change_kind: str | None = None, reason: str = ""):
        actor = self._actor(token, "approve")
        ticket = self._ticket(treatment_ticket_id)
        reason = str(reason or "").strip()
        windows = self._validate_windows(windows)
        now = self.clock.now()
        created_at = utc_text(now)

        latest = self.db.execute(
            "SELECT * FROM disposal_plans WHERE treatment_ticket_id=? ORDER BY plan_version DESC LIMIT 1",
            (treatment_ticket_id,),
        ).fetchone()

        if latest is None:
            kind = "initial"
            if change_kind and change_kind != "initial":
                raise ValidationFailed("首版方案必须是 initial")
            scope = self._validate_scope(impact_scope)
            risk = self._risk_snapshot(ticket)
        else:
            kind = change_kind or "scope_changed"
            if kind not in ("scope_changed", "risk_withdrawn"):
                raise ValidationFailed("新版本类型只能是 scope_changed 或 risk_withdrawn")
            risk = self._risk_snapshot(ticket)
            if kind == "risk_withdrawn":
                # 撤回时 scope 记录撤回时仍登记的影响范围（可为空），但风险状态必须已撤回
                if risk["status"] != "withdrawn":
                    raise InvalidState("只有风险记录已撤回才能生成 risk_withdrawn 方案")
                scope = self._validate_scope(impact_scope) if impact_scope else {
                    "species": "", "zone_codes": [], "area_m2": 0.0, "polygon": None}
            else:
                scope = self._validate_scope(impact_scope)

        fingerprint_risk = {"severity": risk["severity"], "score": risk["score"], "status": risk["status"]}
        scope_fingerprint = digest({"risk": fingerprint_risk, "scope": scope})
        if latest is not None and latest["scope_fingerprint"] == scope_fingerprint:
            raise Conflict("风险记录与影响范围均未变化，不能生成新版本")

        version = (latest["plan_version"] + 1) if latest else 1
        plan_id = "plan-" + uuid.uuid4().hex[:16]
        plan_body = {
            "treatment_ticket_id": treatment_ticket_id,
            "plan_version": version,
            "change_kind": kind,
            "supersedes_plan_id": latest["plan_id"] if latest else None,
            "risk_snapshot": risk,
            "impact_scope": scope,
            "windows": windows,
            "scope_fingerprint": scope_fingerprint,
            "reason": reason,
            "created_by": actor.user_id,
            "created_at": created_at,
        }
        content_sha256 = digest(plan_body)
        ledger_id = "ledg-" + uuid.uuid4().hex[:16]

        with transaction(self.db):
            if latest is not None:
                old = self.db.execute(
                    "SELECT * FROM disposal_ledgers WHERE plan_id=?", (latest["plan_id"],)
                ).fetchone()
                if old is not None and old["state"] in ("active", "rework"):
                    self.db.execute("UPDATE disposal_ledgers SET state='superseded' WHERE ledger_id=?",
                                    (old["ledger_id"],))
                    self.db.execute(
                        "UPDATE disposal_steps SET status='cancelled' WHERE ledger_id=? AND status IN ('pending','in_progress')",
                        (old["ledger_id"],),
                    )
                    self._event(self.db, old["ledger_id"], latest["plan_id"], latest["plan_version"], None,
                                "plan.superseded", {"new_plan_id": plan_id, "new_version": version, "reason": reason},
                                actor.user_id, created_at=created_at)

            self.db.execute(
                "INSERT INTO disposal_plans(plan_id,treatment_ticket_id,plan_version,change_kind,"
                "supersedes_plan_id,risk_snapshot_json,impact_scope_json,windows_json,scope_fingerprint,"
                "content_sha256,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (plan_id, treatment_ticket_id, version, kind, plan_body["supersedes_plan_id"],
                 canonical(risk), canonical(scope), canonical(windows), scope_fingerprint,
                 content_sha256, reason, actor.user_id, created_at),
            )
            self.db.execute(
                "INSERT INTO disposal_ledgers(ledger_id,plan_id,treatment_ticket_id,plan_version,state,"
                "created_by,created_at) VALUES(?,?,?,?, 'active',?,?)",
                (ledger_id, plan_id, treatment_ticket_id, version, actor.user_id, created_at),
            )
            self._create_steps(ledger_id, plan_id, version, kind, scope, windows, now, created_at,
                               superseded_plan=latest)
            if kind != "risk_withdrawn":
                self._schedule_review_if_ready(ledger_id, 1, now, windows)
            self._event(self.db, ledger_id, plan_id, version, None, "plan.created",
                        {"change_kind": kind, "version": version, "impact_scope": scope,
                         "risk": fingerprint_risk, "windows": windows, "reason": reason},
                        actor.user_id, created_at=created_at)
            audit(self.db, "disposal_plan", plan_id, "version_created", actor.user_id,
                  {"treatment_ticket_id": treatment_ticket_id, "plan_version": version, "change_kind": kind,
                   "content_sha256": content_sha256})
        return self.ledger(token, ledger_id)

    def _add_step(self, db, ledger_id, plan_id, version, seq, code, windows, now, *,
                  zone_code=None, detail=None, status="pending", deadline=None, carried_from=None,
                  completed_at=None):
        step_id = f"step-{uuid.uuid4().hex[:12]}"
        db.execute(
            "INSERT INTO disposal_steps(step_id,ledger_id,plan_id,plan_version,step_code,step_seq,attempt,"
            "zone_code,detail_json,status,deadline_at,completed_at,carried_from_step_id,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (step_id, ledger_id, plan_id, version, code, seq, 1, zone_code,
             canonical(detail or {}), status, deadline, completed_at, carried_from, utc_text(now)),
        )
        return step_id

    def _create_steps(self, ledger_id, plan_id, version, kind, scope, windows, now, created_at,
                      superseded_plan):
        db = self.db
        seq = 0
        notification_deadline = self._deadline(now, windows, "notification")
        seq += 1
        self._add_step(db, ledger_id, plan_id, version, seq, "notification", windows, now,
                       detail={"change_kind": kind}, deadline=notification_deadline)
        if kind == "risk_withdrawn":
            seq += 1
            self._add_step(db, ledger_id, plan_id, version, seq, "closeout", windows, now)
            return

        # 已在旧版本完成的分区直接结转，保留来历（carried_from_step_id）
        carried_clearing: dict[str, str] = {}
        if superseded_plan is not None:
            done_rows = db.execute(
                "SELECT s1.* FROM disposal_steps s1 JOIN disposal_ledgers l ON l.ledger_id=s1.ledger_id "
                "WHERE l.plan_id=? AND s1.step_code='clearing' AND s1.status='done' "
                "AND s1.zone_code IN (%s)" % ",".join("?" for _ in scope["zone_codes"]),
                (superseded_plan["plan_id"], *scope["zone_codes"]),
            ).fetchall()
            newest_done: dict[str, sqlite3.Row] = {}
            for row in done_rows:
                newest_done[row["zone_code"]] = row
        else:
            newest_done = {}

        seq += 1
        self._add_step(db, ledger_id, plan_id, version, seq, "isolation", windows, now)
        for zone in scope["zone_codes"]:
            seq += 1
            old = newest_done.get(zone)
            if old is not None:
                carried_clearing[zone] = self._add_step(
                    db, ledger_id, plan_id, version, seq, "clearing", windows, now, zone_code=zone,
                    detail={"carried": True}, status="done", carried_from=old["step_id"],
                    completed_at=old["completed_at"])
            else:
                self._add_step(db, ledger_id, plan_id, version, seq, "clearing", windows, now,
                               zone_code=zone)
        for zone in scope["zone_codes"]:
            seq += 1
            old_waste = (
                self._carried_waste(db, superseded_plan, zone)
                if superseded_plan is not None else None
            )
            if old_waste is not None and zone in carried_clearing:
                self._add_step(db, ledger_id, plan_id, version, seq, "waste_manifest", windows, now,
                               zone_code=zone, detail={"carried": True}, status="done",
                               carried_from=old_waste["step_id"], completed_at=old_waste["completed_at"])
            else:
                self._add_step(db, ledger_id, plan_id, version, seq, "waste_manifest", windows, now,
                               zone_code=zone)
        seq += 1
        self._add_step(db, ledger_id, plan_id, version, seq, "review", windows, now)
        seq += 1
        self._add_step(db, ledger_id, plan_id, version, seq, "closeout", windows, now)

    @staticmethod
    def _carried_waste(db, superseded_plan, zone_code):
        return db.execute(
            "SELECT s1.* FROM disposal_steps s1 JOIN disposal_ledgers l ON l.ledger_id=s1.ledger_id "
            "WHERE l.plan_id=? AND s1.step_code='waste_manifest' AND s1.status='done' AND s1.zone_code=? "
            "ORDER BY s1.attempt DESC,s1.step_seq DESC LIMIT 1",
            (superseded_plan["plan_id"], zone_code),
        ).fetchone()

    # ------------------------------------------------------------------ 现场上报
    def ack_notification(self, token, ledger_id, idempotency_key, recipients):
        actor = self._actor(token, "treatment_ticket")
        ledger = self._ledger_row(ledger_id)
        if not isinstance(recipients, list) or not recipients:
            raise ValidationFailed("recipients 必须为非空数组")
        clean = []
        for item in recipients:
            recipient = str(item.get("recipient", "")).strip()
            if not recipient:
                raise ValidationFailed("通知签收人不能为空")
            signed_at = parse_utc(item["signed_at"], "signed_at") if item.get("signed_at") else self.clock.now()
            clean.append({"recipient": recipient, "signed_at": utc_text(signed_at),
                          "evidence": item.get("evidence", {})})
        request = {"recipients": clean}
        with transaction(self.db):
            def work():
                step = self._find_step(self.db, ledger_id, "notification")
                plan = self._plan_row(ledger["plan_id"])
                windows = json.loads(plan["windows_json"])
                now = self.clock.now()
                for item in clean:
                    self.db.execute(
                        "INSERT INTO disposal_receipts(step_id,recipient,signed_at,evidence_json,created_at)"
                        " VALUES(?,?,?,?,?)",
                        (step["step_id"], item["recipient"], item["signed_at"],
                         canonical(item["evidence"]), self._now_text()),
                    )
                self.db.execute("UPDATE disposal_steps SET status='done',completed_at=? WHERE step_id=?",
                                (utc_text(now), step["step_id"]))
                iso = self.db.execute(
                    "SELECT step_id FROM disposal_steps WHERE ledger_id=? AND step_code='isolation' AND status='pending'",
                    (ledger_id,),
                ).fetchone()
                if iso is not None:
                    self.db.execute("UPDATE disposal_steps SET deadline_at=? WHERE step_id=?",
                                    (self._deadline(now, windows, "isolation"), iso["step_id"]))
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], step["step_id"],
                            "notification.acked", {"recipients": clean}, actor.user_id, idempotency_key)
                return {"ledger_id": ledger_id, "step_id": step["step_id"], "signed": len(clean),
                        "plan_version": ledger["plan_version"]}

            return self._idempotent(self.db, ledger_id, idempotency_key, request, work)

    def report_isolation(self, token, ledger_id, idempotency_key, isolated_at=None, evidence=None):
        actor = self._actor(token, "treatment_ticket")
        ledger = self._ledger_row(ledger_id)
        when = parse_utc(isolated_at, "isolated_at") if isolated_at else self.clock.now()
        evidence = evidence or {}
        request = {"isolated_at": utc_text(when), "evidence": evidence}
        with transaction(self.db):
            def work():
                step = self._find_step(self.db, ledger_id, "isolation")
                plan = self._plan_row(ledger["plan_id"])
                windows = json.loads(plan["windows_json"])
                self.db.execute("UPDATE disposal_steps SET status='done',detail_json=?,completed_at=? WHERE step_id=?",
                                (canonical({"evidence": evidence, "isolated_at": utc_text(when)}),
                                 utc_text(when), step["step_id"]))
                cleared_deadline = self._deadline(when, windows, "clearing")
                self.db.execute(
                    "UPDATE disposal_steps SET deadline_at=? WHERE ledger_id=? AND step_code='clearing' AND status='pending'",
                    (cleared_deadline, ledger_id),
                )
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], step["step_id"],
                            "isolation.reported", {"isolated_at": utc_text(when), "evidence": evidence},
                            actor.user_id, idempotency_key)
                return {"ledger_id": ledger_id, "step_id": step["step_id"], "clearing_deadline_at": cleared_deadline,
                        "plan_version": ledger["plan_version"]}

            return self._idempotent(self.db, ledger_id, idempotency_key, request, work)

    def report_clearing(self, token, ledger_id, idempotency_key, zone_code, chemical_batches):
        actor = self._actor(token, "treatment_ticket")
        ledger = self._ledger_row(ledger_id)
        zone_code = str(zone_code or "").strip()
        if not zone_code:
            raise ValidationFailed("zone_code 不能为空")
        if not isinstance(chemical_batches, list) or not chemical_batches:
            raise ValidationFailed("分区清除必须登记至少一个药剂批次")
        batches = []
        for raw in chemical_batches:
            batch_no = str(raw.get("chemical_batch_no", "")).strip()
            agent = str(raw.get("agent_name", "")).strip()
            dosage = str(raw.get("dosage", "")).strip()
            if not batch_no or not agent or not dosage:
                raise ValidationFailed("药剂批次号、药剂名称与用量都不能为空")
            applied_at = parse_utc(raw["applied_at"], "applied_at") if raw.get("applied_at") else self.clock.now()
            batches.append({"chemical_batch_no": batch_no, "agent_name": agent, "dosage": dosage,
                            "applied_at": utc_text(applied_at), "evidence": raw.get("evidence", {})})
        request = {"zone_code": zone_code, "chemical_batches": batches}
        with transaction(self.db):
            def work():
                plan = self._plan_row(ledger["plan_id"])
                scope = json.loads(plan["impact_scope_json"])
                if plan["change_kind"] != "risk_withdrawn" and zone_code not in scope["zone_codes"]:
                    raise ValidationFailed(f"分区不在当前方案范围内: {zone_code}")
                iso_done = self.db.execute(
                    "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code='isolation' AND status='done'",
                    (ledger_id,),
                ).fetchone()
                if iso_done is None:
                    raise InvalidState("现场隔离未完成，不能分区清除")
                step = self._find_step(self.db, ledger_id, "clearing", zone_code=zone_code)
                windows = json.loads(plan["windows_json"])
                now = self.clock.now()
                for batch in batches:
                    self.db.execute(
                        "INSERT INTO disposal_chemical_batches(step_id,chemical_batch_no,agent_name,dosage,"
                        "applied_at,evidence_json,created_at) VALUES(?,?,?,?,?,?,?)",
                        (step["step_id"], batch["chemical_batch_no"], batch["agent_name"], batch["dosage"],
                         batch["applied_at"], canonical(batch["evidence"]), self._now_text()),
                    )
                self.db.execute("UPDATE disposal_steps SET status='done',completed_at=? WHERE step_id=?",
                                (utc_text(now), step["step_id"]))
                waste_deadline = self._deadline(now, windows, "waste_manifest")
                self.db.execute(
                    "UPDATE disposal_steps SET deadline_at=? WHERE ledger_id=? AND step_code='waste_manifest'"
                    " AND zone_code=? AND attempt=? AND status='pending'",
                    (waste_deadline, ledger_id, zone_code, step["attempt"]),
                )
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], step["step_id"],
                            "clearing.reported", {"zone_code": zone_code, "attempt": step["attempt"],
                                                  "chemical_batches": batches},
                            actor.user_id, idempotency_key)
                return {"ledger_id": ledger_id, "step_id": step["step_id"], "zone_code": zone_code,
                        "attempt": step["attempt"], "waste_deadline_at": waste_deadline,
                        "plan_version": ledger["plan_version"]}

            return self._idempotent(self.db, ledger_id, idempotency_key, request, work)

    def report_waste(self, token, ledger_id, idempotency_key, zone_code, manifest_id,
                     carrier, destination, weight_kg, handed_over_at=None, evidence=None):
        actor = self._actor(token, "treatment_ticket")
        ledger = self._ledger_row(ledger_id)
        zone_code = str(zone_code or "").strip()
        manifest_id = str(manifest_id or "").strip()
        carrier = str(carrier or "").strip()
        destination = str(destination or "").strip()
        if not zone_code or not manifest_id or not carrier or not destination:
            raise ValidationFailed("zone_code、联单号、承运方与去向均不能为空")
        if not isinstance(weight_kg, (int, float)) or weight_kg <= 0:
            raise ValidationFailed("废弃物重量必须为正数")
        when = parse_utc(handed_over_at, "handed_over_at") if handed_over_at else self.clock.now()
        evidence = evidence or {}
        request = {"zone_code": zone_code, "manifest_id": manifest_id, "carrier": carrier,
                   "destination": destination, "weight_kg": float(weight_kg),
                   "handed_over_at": utc_text(when), "evidence": evidence}
        with transaction(self.db):
            def work():
                clearing = self._find_step(self.db, ledger_id, "clearing", zone_code=zone_code,
                                           statuses=("done",))
                step = self._find_step(self.db, ledger_id, "waste_manifest", zone_code=zone_code)
                if step["attempt"] != clearing["attempt"]:
                    raise InvalidState("废弃物联单必须对应当前拨次的清除")
                plan = self._plan_row(ledger["plan_id"])
                try:
                    self.db.execute(
                        "INSERT INTO disposal_waste_manifests(manifest_id,step_id,carrier,destination,"
                        "weight_kg,handed_over_at,evidence_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (manifest_id, step["step_id"], carrier, destination, float(weight_kg),
                         utc_text(when), canonical(evidence), self._now_text()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise Conflict("废弃物联单号已存在") from exc
                now = self.clock.now()
                self.db.execute("UPDATE disposal_steps SET status='done',completed_at=? WHERE step_id=?",
                                (utc_text(now), step["step_id"]))
                self._schedule_review_if_ready(ledger_id, step["attempt"], now, json.loads(plan["windows_json"]))
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], step["step_id"],
                            "waste.reported", request, actor.user_id, idempotency_key)
                return {"ledger_id": ledger_id, "step_id": step["step_id"], "manifest_id": manifest_id,
                        "zone_code": zone_code, "attempt": step["attempt"],
                        "plan_version": ledger["plan_version"]}

            return self._idempotent(self.db, ledger_id, idempotency_key, request, work)

    def _schedule_review_if_ready(self, ledger_id, attempt, now, windows):
        pending_waste = self.db.execute(
            "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code='waste_manifest' AND attempt=? "
            "AND status!='done' LIMIT 1",
            (ledger_id, attempt),
        ).fetchone()
        pending_clearing = self.db.execute(
            "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code='clearing' AND attempt=? "
            "AND status!='done' LIMIT 1",
            (ledger_id, attempt),
        ).fetchone()
        if pending_waste is None and pending_clearing is None:
            self.db.execute(
                "UPDATE disposal_steps SET deadline_at=? WHERE ledger_id=? AND step_code='review' "
                "AND attempt=? AND status='pending' AND deadline_at IS NULL",
                (self._deadline(now, windows, "review"), ledger_id, attempt),
            )

    # ------------------------------------------------------------------ 复查与返工
    def submit_review(self, token, ledger_id, idempotency_key, quadrats):
        actor = self._actor(token, "treatment_ticket")
        ledger = self._ledger_row(ledger_id)
        if not isinstance(quadrats, list) or not quadrats:
            raise ValidationFailed("复查样方不能为空")
        clean = []
        for raw in quadrats:
            code = str(raw.get("quadrat_code", "")).strip()
            result = str(raw.get("result", "")).strip()
            reviewer = str(raw.get("reviewer", "")).strip()
            if not code or result not in ("pass", "fail") or not reviewer:
                raise ValidationFailed("样方编号、pass/fail 结果与复查人不能为空")
            residual = raw.get("residual_count", 0)
            if not isinstance(residual, int) or residual < 0:
                raise ValidationFailed("残株数必须为非负整数")
            reviewed_at = parse_utc(raw["reviewed_at"], "reviewed_at") if raw.get("reviewed_at") else self.clock.now()
            clean.append({"quadrat_code": code, "result": result, "residual_count": residual,
                          "reviewed_at": utc_text(reviewed_at), "reviewer": reviewer,
                          "note": str(raw.get("note", ""))})
        request = {"quadrats": clean}
        with transaction(self.db):
            def work():
                review = self._find_step(self.db, ledger_id, "review")
                plan = self._plan_row(ledger["plan_id"])
                zones_in_attempt = {r["zone_code"] for r in self.db.execute(
                    "SELECT zone_code FROM disposal_steps WHERE ledger_id=? AND step_code='clearing' AND attempt=?",
                    (ledger_id, review["attempt"]),
                ).fetchall()}
                submitted = {q["quadrat_code"] for q in clean}
                if submitted != zones_in_attempt:
                    raise ValidationFailed(
                        f"本拨次复查样方必须恰好覆盖 {sorted(zones_in_attempt)}，实际 {sorted(submitted)}")
                open_work = self.db.execute(
                    "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code IN ('clearing','waste_manifest')"
                    " AND attempt=? AND status!='done' LIMIT 1",
                    (ledger_id, review["attempt"]),
                ).fetchone()
                if open_work is not None:
                    raise InvalidState("本拨次清除与废弃物移交尚未全部完成，不能复查")
                windows = json.loads(plan["windows_json"])
                now = self.clock.now()
                for q in clean:
                    self.db.execute(
                        "INSERT INTO disposal_review_quadrats(quadrat_id,step_id,quadrat_code,result,"
                        "residual_count,reviewed_at,reviewer,note) VALUES(?,?,?,?,?,?,?,?)",
                        (f"quad-{uuid.uuid4().hex[:12]}", review["step_id"], q["quadrat_code"], q["result"],
                         q["residual_count"], q["reviewed_at"], q["reviewer"], q["note"]),
                    )
                failed = [q["quadrat_code"] for q in clean if q["result"] == "fail"]
                if not failed:
                    self.db.execute("UPDATE disposal_steps SET status='done',completed_at=? WHERE step_id=?",
                                    (utc_text(now), review["step_id"]))
                    self.db.execute(
                        "UPDATE disposal_ledgers SET state='active' WHERE ledger_id=? AND state='rework'",
                        (ledger_id,),
                    )
                    self.db.execute(
                        "UPDATE disposal_steps SET deadline_at=? WHERE ledger_id=? AND step_code='closeout'"
                        " AND status='pending' AND deadline_at IS NULL",
                        (self._deadline(now, windows, "closeout"), ledger_id),
                    )
                    self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], review["step_id"],
                                "review.submitted", {"attempt": review["attempt"], "passed": True, "quadrats": clean},
                                actor.user_id, idempotency_key)
                    return {"ledger_id": ledger_id, "step_id": review["step_id"], "passed": True,
                            "rework_zones": [], "plan_version": ledger["plan_version"]}

                # 复查不通过：同方案内开新拨次，只返工失败分区
                self.db.execute("UPDATE disposal_steps SET status='failed',completed_at=? WHERE step_id=?",
                                (utc_text(now), review["step_id"]))
                self.db.execute("UPDATE disposal_ledgers SET state='rework' WHERE ledger_id=?", (ledger_id,))
                new_attempt_row = self.db.execute(
                    "SELECT COALESCE(MAX(attempt),0)+1 AS a FROM disposal_steps WHERE ledger_id=? AND step_code='clearing'",
                    (ledger_id,),
                ).fetchone()
                new_attempt = new_attempt_row["a"]
                seq_row = self.db.execute("SELECT COALESCE(MAX(step_seq),0) AS s FROM disposal_steps WHERE ledger_id=?",
                                          (ledger_id,)).fetchone()
                seq = seq_row["s"]
                new_step_ids: dict[str, dict[str, str]] = {"clearing": {}, "waste_manifest": {}, "review": ""}
                for zone in failed:
                    seq += 1
                    new_step_ids["clearing"][zone] = self._add_step(
                        self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq, "clearing",
                        windows, now, zone_code=zone,
                        detail={"rework_of_step_id": review["step_id"]},
                        deadline=self._deadline(now, windows, "clearing"))
                    self._bump_attempt(self.db, new_step_ids["clearing"][zone], new_attempt)
                    seq += 1
                    new_step_ids["waste_manifest"][zone] = self._add_step(
                        self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq, "waste_manifest",
                        windows, now, zone_code=zone, detail={"rework_of_step_id": review["step_id"]})
                    self._bump_attempt(self.db, new_step_ids["waste_manifest"][zone], new_attempt)
                seq += 1
                new_step_ids["review"] = self._add_step(
                    self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq, "review", windows, now,
                    detail={"rework_of_step_id": review["step_id"]})
                self._bump_attempt(self.db, new_step_ids["review"], new_attempt)
                self.db.execute(
                    "UPDATE disposal_steps SET status='cancelled' WHERE ledger_id=? AND step_code='closeout'"
                    " AND status IN ('pending','in_progress')",
                    (ledger_id,),
                )
                seq += 1
                new_close = self._add_step(
                    self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq, "closeout", windows, now,
                    detail={"rework_of_step_id": review["step_id"]})
                self._bump_attempt(self.db, new_close, new_attempt)
                new_step_ids["closeout"] = new_close
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], review["step_id"],
                            "review.submitted", {"attempt": review["attempt"], "passed": False,
                                                  "failed_zones": failed, "quadrats": clean},
                            actor.user_id, idempotency_key)
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], None,
                            "rework.created", {"from_attempt": review["attempt"], "new_attempt": new_attempt,
                                               "zones": failed, "new_step_ids": new_step_ids},
                            actor.user_id)
                return {"ledger_id": ledger_id, "step_id": review["step_id"], "passed": False,
                        "rework_zones": failed, "new_attempt": new_attempt,
                        "plan_version": ledger["plan_version"]}

            return self._idempotent(self.db, ledger_id, idempotency_key, request, work)

    @staticmethod
    def _bump_attempt(db, step_id, attempt):
        db.execute("UPDATE disposal_steps SET attempt=? WHERE step_id=?", (attempt, step_id))

    @staticmethod
    def _actionability(all_steps, step) -> tuple[bool, list[str]]:
        """判断当前步骤是否可上报；不可上报时给出等待项。"""
        if step["status"] not in ("pending", "in_progress"):
            return False, []
        code = step["step_code"]
        attempt = step["attempt"]

        def open_of(codes, zone=None):
            out = []
            for x in all_steps:
                if x["step_code"] not in codes or x["attempt"] != attempt:
                    continue
                if zone is not None and x["zone_code"] != zone:
                    continue
                if x["status"] in ("pending", "in_progress"):
                    out.append(x)
            return out

        waiting: list[str] = []
        if code == "notification":
            actionable = True
        elif code == "isolation":
            actionable = not open_of({"notification"})
            if not actionable:
                waiting.append("notification")
        elif code == "clearing":
            blockers = open_of({"notification", "isolation"})
            actionable = not blockers
            waiting = sorted({b["step_code"] for b in blockers})
        elif code == "waste_manifest":
            blockers = open_of({"clearing"}, zone=step["zone_code"])
            actionable = not blockers
            if not actionable:
                waiting.append(f"clearing/{step['zone_code']}")
        elif code == "review":
            blockers = open_of({"clearing", "waste_manifest"})
            actionable = not blockers
            waiting = sorted({f"{b['step_code']}/{b['zone_code']}" if b["zone_code"] else b["step_code"]
                              for b in blockers})
        elif code == "closeout":
            latest_review = max(
                (x for x in all_steps if x["step_code"] == "review"),
                key=lambda x: (x["attempt"], x["step_seq"]), default=None)
            actionable = latest_review is not None and latest_review["status"] == "done"
            if not actionable:
                waiting.append("review")
        else:
            actionable = False
        return actionable, waiting

    # ------------------------------------------------------------------ 独立结案
    def independent_closeout(self, token, ledger_id, result, note="", idempotency_key=None):
        actor = self._actor(token, "approve")
        result = str(result or "").strip()
        if result not in ("approved", "rejected"):
            raise ValidationFailed("结案结论必须是 approved 或 rejected")
        note = str(note or "").strip()
        ledger = self._ledger_row(ledger_id)
        request = {"result": result, "note": note, "reviewer": actor.user_id}
        key = idempotency_key or f"closeout:{actor.user_id}:{result}"

        def work():
            now = self.clock.now()
            plan = self._plan_row(ledger["plan_id"])
            windows = json.loads(plan["windows_json"])
            closeout = self._find_step(self.db, ledger_id, "closeout")
            if plan["change_kind"] != "risk_withdrawn":
                latest_review = self.db.execute(
                    "SELECT * FROM disposal_steps WHERE ledger_id=? AND step_code='review' "
                    "ORDER BY attempt DESC,step_seq DESC LIMIT 1",
                    (ledger_id,),
                ).fetchone()
                if latest_review is None or latest_review["status"] != "done":
                    raise InvalidState("复查尚未通过，不能独立结案")
                open_steps = self.db.execute(
                    "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code IN "
                    "('clearing','waste_manifest','review') AND status IN ('pending','in_progress') LIMIT 1",
                    (ledger_id,),
                ).fetchone()
                if open_steps is not None:
                    raise InvalidState("仍有清除、废弃物或复查步骤未完成，不能结案")
            else:
                notif = self.db.execute(
                    "SELECT 1 FROM disposal_steps WHERE ledger_id=? AND step_code='notification' AND status='done'",
                    (ledger_id,),
                ).fetchone()
                if notif is None:
                    raise InvalidState("撤回通知尚未签收，不能结案")
            executors = {r["actor"] for r in self.db.execute(
                "SELECT DISTINCT actor FROM disposal_events WHERE ledger_id=? AND event_type IN (%s)"
                % ",".join("?" for _ in EXECUTION_EVENTS),
                (ledger_id, *EXECUTION_EVENTS),
            ).fetchall()}
            if actor.user_id in executors:
                raise InvalidState("独立复核人不得参与过本台账的现场执行")

            self.db.execute(
                "INSERT INTO disposal_closeouts(ledger_id,attempt,reviewer,result,note,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (ledger_id, closeout["attempt"], actor.user_id, result, note, self._now_text()),
            )
            if result == "approved":
                self.db.execute("UPDATE disposal_steps SET status='done',completed_at=? WHERE step_id=?",
                                (utc_text(now), closeout["step_id"]))
                self.db.execute(
                    "UPDATE disposal_ledgers SET state='closed',closed_at=? WHERE ledger_id=?",
                    (utc_text(now), ledger_id),
                )
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], closeout["step_id"],
                            "closeout.approved", {"reviewer": actor.user_id, "note": note}, actor.user_id, key)
                self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], None,
                            "ledger.closed", {"closed_at": utc_text(now), "reviewer": actor.user_id},
                            actor.user_id)
                audit(self.db, "disposal_ledger", ledger_id, "closed", actor.user_id,
                      {"plan_version": ledger["plan_version"]})
                return {"ledger_id": ledger_id, "state": "closed", "closed_at": utc_text(now),
                        "plan_version": ledger["plan_version"]}

            # 结案被驳回：本拨次分区全部返工，并生成新拨次的结案步骤
            self.db.execute("UPDATE disposal_steps SET status='failed',completed_at=? WHERE step_id=?",
                            (utc_text(now), closeout["step_id"]))
            self.db.execute("UPDATE disposal_ledgers SET state='rework' WHERE ledger_id=?", (ledger_id,))
            attempt_row = self.db.execute(
                "SELECT COALESCE(MAX(attempt),0)+1 AS a FROM disposal_steps WHERE ledger_id=? AND step_code='clearing'",
                (ledger_id,),
            ).fetchone()
            new_attempt = attempt_row["a"]
            scope = json.loads(plan["impact_scope_json"])
            seq_row = self.db.execute("SELECT COALESCE(MAX(step_seq),0) AS s FROM disposal_steps WHERE ledger_id=?",
                                      (ledger_id,)).fetchone()
            seq = seq_row["s"]
            for zone in scope["zone_codes"]:
                seq += 1
                step = self._add_step(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq,
                                      "clearing", windows, now, zone_code=zone,
                                      detail={"closeout_rejected_step_id": closeout["step_id"]},
                                      deadline=self._deadline(now, windows, "clearing"))
                self._bump_attempt(self.db, step, new_attempt)
                seq += 1
                step = self._add_step(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq,
                                      "waste_manifest", windows, now, zone_code=zone,
                                      detail={"closeout_rejected_step_id": closeout["step_id"]})
                self._bump_attempt(self.db, step, new_attempt)
            seq += 1
            step = self._add_step(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq,
                                  "review", windows, now,
                                  detail={"closeout_rejected_step_id": closeout["step_id"]})
            self._bump_attempt(self.db, step, new_attempt)
            seq += 1
            new_close = self._add_step(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], seq,
                                       "closeout", windows, now,
                                       detail={"closeout_rejected_step_id": closeout["step_id"]})
            self._bump_attempt(self.db, new_close, closeout["attempt"] + 1)
            self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], closeout["step_id"],
                        "closeout.rejected", {"reviewer": actor.user_id, "note": note,
                                              "new_attempt": new_attempt}, actor.user_id, key)
            self._event(self.db, ledger_id, ledger["plan_id"], ledger["plan_version"], None,
                        "rework.created", {"from_attempt": closeout["attempt"], "new_attempt": new_attempt,
                                           "zones": scope["zone_codes"], "reason": "closeout_rejected"},
                        actor.user_id)
            return {"ledger_id": ledger_id, "state": "rework", "new_attempt": new_attempt,
                    "plan_version": ledger["plan_version"]}

        with transaction(self.db):
            return self._idempotent(self.db, ledger_id, key, request, work)

    # ------------------------------------------------------------------ 查询
    def ledger(self, token, ledger_id):
        self.auth.require(token, "read")
        ledger = self._ledger_row(ledger_id)
        plan = self._plan_row(ledger["plan_id"])
        steps = self._steps(self.db, ledger_id)
        events = self.db.execute(
            "SELECT * FROM disposal_events WHERE ledger_id=? ORDER BY event_id", (ledger_id,)
        ).fetchall()
        now = self.clock.now()

        step_views = []
        done_codes = {s["step_code"] for s in steps if s["status"] == "done"}
        for s in steps:
            overdue = False
            if s["status"] in ("pending", "in_progress") and s["deadline_at"]:
                overdue = parse_utc(s["deadline_at"], "deadline_at") < now
            actionable, waiting_on = self._actionability(steps, s)
            step_views.append({
                "step_id": s["step_id"], "step_code": s["step_code"], "step_seq": s["step_seq"],
                "attempt": s["attempt"], "zone_code": s["zone_code"], "status": s["status"],
                "detail": json.loads(s["detail_json"]), "deadline_at": s["deadline_at"],
                "scheduled": s["deadline_at"] is not None,
                "completed_at": s["completed_at"], "overdue": overdue,
                "actionable": actionable, "waiting_on": waiting_on,
                "carried_from_step_id": s["carried_from_step_id"],
                "plan_version": s["plan_version"],
            })

        missing = [v for v in step_views if v["status"] in ("pending", "in_progress")]
        missing.sort(key=lambda v: (v["step_seq"],))
        with_deadline = [v for v in missing if v["deadline_at"]]
        next_deadline = None
        if with_deadline:
            nxt = min(with_deadline, key=lambda v: v["deadline_at"])
            next_deadline = {"step_id": nxt["step_id"], "step_code": nxt["step_code"],
                             "zone_code": nxt["zone_code"], "attempt": nxt["attempt"],
                             "deadline_at": nxt["deadline_at"],
                             "overdue": parse_utc(nxt["deadline_at"], "deadline_at") < now}

        timeline = [{
            "event_id": e["event_id"], "event_type": e["event_type"], "step_id": e["step_id"],
            "plan_id": e["plan_id"], "plan_version": e["plan_version"], "actor": e["actor"],
            "payload": json.loads(e["payload_json"]), "idempotency_key": e["idempotency_key"],
            "created_at": e["created_at"],
        } for e in events]

        chain_ok = True
        previous = GENESIS
        for e in events:
            body = canonical({
                "ledger_id": e["ledger_id"], "plan_id": e["plan_id"], "plan_version": e["plan_version"],
                "step_id": e["step_id"], "event_type": e["event_type"], "payload": json.loads(e["payload_json"]),
                "idempotency_key": e["idempotency_key"], "actor": e["actor"], "created_at": e["created_at"],
            })
            if e["previous_hash"] != previous or e["event_hash"] != digest(previous + "|" + body):
                chain_ok = False
                break
            previous = e["event_hash"]

        evidence = {
            "receipts": rows(self.db,
                "SELECT r.*,s.plan_version,s.attempt,s.zone_code FROM disposal_receipts r "
                "JOIN disposal_steps s ON s.step_id=r.step_id WHERE s.ledger_id=? ORDER BY r.receipt_id",
                (ledger_id,)),
            "chemical_batches": rows(self.db,
                "SELECT b.*,s.plan_version,s.attempt,s.zone_code FROM disposal_chemical_batches b "
                "JOIN disposal_steps s ON s.step_id=b.step_id WHERE s.ledger_id=? ORDER BY b.batch_use_id",
                (ledger_id,)),
            "waste_manifests": rows(self.db,
                "SELECT m.*,s.plan_version,s.attempt FROM disposal_waste_manifests m "
                "JOIN disposal_steps s ON s.step_id=m.step_id WHERE s.ledger_id=? ORDER BY m.handed_over_at",
                (ledger_id,)),
            "review_quadrats": rows(self.db,
                "SELECT q.*,s.plan_version,s.attempt FROM disposal_review_quadrats q "
                "JOIN disposal_steps s ON s.step_id=q.step_id WHERE s.ledger_id=? "
                "ORDER BY q.reviewed_at,q.quadrat_code", (ledger_id,)),
            "closeouts": rows(self.db,
                "SELECT * FROM disposal_closeouts WHERE ledger_id=? ORDER BY closeout_id", (ledger_id,)),
        }
        return {
            "ledger_id": ledger_id,
            "treatment_ticket_id": ledger["treatment_ticket_id"],
            "state": ledger["state"],
            "created_at": ledger["created_at"],
            "closed_at": ledger["closed_at"],
            "current_plan_version": ledger["plan_version"],
            "plan": {
                "plan_id": plan["plan_id"], "plan_version": plan["plan_version"],
                "change_kind": plan["change_kind"], "supersedes_plan_id": plan["supersedes_plan_id"],
                "risk_snapshot": json.loads(plan["risk_snapshot_json"]),
                "impact_scope": json.loads(plan["impact_scope_json"]),
                "windows": json.loads(plan["windows_json"]),
                "scope_fingerprint": plan["scope_fingerprint"],
                "content_sha256": plan["content_sha256"],
                "reason": plan["reason"], "created_by": plan["created_by"], "created_at": plan["created_at"],
            },
            "missing_steps": [{"step_id": v["step_id"], "step_code": v["step_code"], "zone_code": v["zone_code"],
                               "attempt": v["attempt"], "status": v["status"], "deadline_at": v["deadline_at"],
                               "scheduled": v["scheduled"], "overdue": v["overdue"],
                               "actionable": v["actionable"], "waiting_on": v["waiting_on"]} for v in missing],
            "next_deadline": next_deadline,
            "steps": step_views,
            "timeline": timeline,
            "evidence": evidence,
            "event_chain_ok": chain_ok,
        }

    def list_versions(self, token, treatment_ticket_id):
        self.auth.require(token, "read")
        self._ticket(treatment_ticket_id)
        out = rows(self.db,
            "SELECT p.plan_id,p.plan_version,p.change_kind,p.supersedes_plan_id,p.scope_fingerprint,"
            "p.content_sha256,p.reason,p.created_by,p.created_at,l.ledger_id,l.state,l.closed_at "
            "FROM disposal_plans p LEFT JOIN disposal_ledgers l ON l.plan_id=p.plan_id "
            "WHERE p.treatment_ticket_id=? ORDER BY p.plan_version", (treatment_ticket_id,))
        return {"treatment_ticket_id": treatment_ticket_id, "versions": out}

    def ticket_overview(self, token, treatment_ticket_id):
        self.auth.require(token, "read")
        self._ticket(treatment_ticket_id)
        versions = self.list_versions(token, treatment_ticket_id)["versions"]
        if not versions:
            return {"treatment_ticket_id": treatment_ticket_id, "plan_created": False,
                    "current_plan_version": 0, "missing_steps": [], "next_deadline": None, "versions": []}
        current = versions[-1]
        view = self.ledger(token, current["ledger_id"])
        return {
            "treatment_ticket_id": treatment_ticket_id,
            "plan_created": True,
            "ledger_id": current["ledger_id"],
            "ledger_state": current["state"],
            "current_plan_version": current["plan_version"],
            "current_change_kind": current["change_kind"],
            "risk_snapshot": view["plan"]["risk_snapshot"],
            "impact_scope": view["plan"]["impact_scope"],
            "missing_steps": view["missing_steps"],
            "next_deadline": view["next_deadline"],
            "event_chain_ok": view["event_chain_ok"],
            "closed_at": current["closed_at"],
            "versions": [{"plan_version": v["plan_version"], "change_kind": v["change_kind"],
                          "ledger_id": v["ledger_id"], "state": v["state"], "reason": v["reason"],
                          "created_at": v["created_at"], "closed_at": v["closed_at"]} for v in versions],
        }
