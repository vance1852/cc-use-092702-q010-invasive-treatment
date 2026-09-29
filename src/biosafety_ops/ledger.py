"""入侵物种处置执行台账应用服务。

一条台账从“处置方案版本”出发，到独立复核结案（或风险撤回）关闭：

- 方案版本不可覆盖：以当时有效的风险记录与影响范围快照生成，带序号和内容哈希；
  风险撤回或范围变化只能追加新版本，旧版本下的任务与事件永远保留来历。
- 执行按版本内严格顺序推进：通知签收 → 现场隔离 → 分区清除 → 复查样方 →
  废弃物去向 → 独立复核；复查不通过或复核不通过都会开出新一轮“返工 + 复查”。
- 外协队伍进度上报使用 report_id 幂等：同一上报重放不二次推进，同一进度键重复推进被拒绝。
- 所有时限取自可注入时钟；查询视图直接给出当前缺少的步骤、下一截止时刻和
  每次变更所依据的方案版本。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Mapping, Sequence

from .auth import Auth
from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, InvalidState, ValidationFailed
from .plans import (
    REWORK_KEY,
    STEP_LABELS,
    AffectedArea,
    ClearingZone,
    build_plan_content,
    canonical_json,
    content_hash,
    steps_for_version,
)
from .storage import audit, transaction

_TERMINAL_LEDGER = {"closed"}


class TreatmentLedgerService:
    def __init__(self, db, clock=None, auth: Auth | None = None) -> None:
        self.db = db
        self.clock = clock or SystemClock()
        self.auth = auth or Auth(db)

    # ------------------------------------------------------------------ 基础
    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _deadline(self, anchor_text: str, hours: float) -> str:
        return utc_text(parse_utc(anchor_text) + timedelta(hours=float(hours)))

    def _require(self, token: str, permission: str):
        return self.auth.require(token, permission)

    def _ledger_row(self, ledger_id: str) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT * FROM treatment_ledgers WHERE ledger_id=?", (ledger_id,)
        ).fetchone()
        if row is None:
            raise KeyError(ledger_id)
        return row

    def _version_row(self, ledger_id: str, version: int) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT * FROM treatment_plan_versions WHERE ledger_id=? AND version=?",
            (ledger_id, version),
        ).fetchone()
        if row is None:
            raise KeyError(f"{ledger_id}#v{version}")
        return row

    def _content(self, version_row: sqlite3.Row) -> dict[str, Any]:
        return json.loads(version_row["content_json"])

    def _risk_snapshot(self, zone_record_id: str, alert_id: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT alert_id,zone_record_id,severity,score,status FROM alerts "
            "WHERE alert_id=? AND zone_record_id=?",
            (alert_id, zone_record_id),
        ).fetchone()
        if row is None:
            raise KeyError(alert_id)
        return {
            "alert_id": row["alert_id"],
            "zone_record_id": row["zone_record_id"],
            "severity": row["severity"],
            "score": row["score"],
            "status": row["status"],
            "captured_at": self._now(),
        }

    def _event(
        self,
        ledger_id: str,
        plan_version: int,
        event_type: str,
        actor: str,
        payload: Mapping[str, Any],
        step_instance_id: int | None = None,
        step_key: str | None = None,
        rework_round: int = 0,
    ) -> None:
        self.db.execute(
            "INSERT INTO treatment_step_events(ledger_id,plan_version,step_instance_id,step_key,"
            "rework_round,event_type,actor,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                ledger_id,
                plan_version,
                step_instance_id,
                step_key,
                rework_round,
                event_type,
                actor,
                canonical_json(payload),
                self._now(),
            ),
        )

    # ------------------------------------------------------------ 方案版本
    def create_ledger(
        self,
        token: str,
        *,
        alert_id: str,
        zone_record_id: str,
        area: Mapping[str, Any],
        clearing_zones: Sequence[Mapping[str, Any]],
        notice_recipients: Sequence[str],
        chemical_batches: Sequence[str],
        reason: str,
        deadline_hours: Mapping[str, float] | None = None,
    ) -> dict[str, Any]:
        actor = self._require(token, "ledger.plan")
        risk = self._risk_snapshot(zone_record_id, alert_id)
        if risk["status"] in {"withdrawn", "cancelled", "resolved"}:
            raise InvalidState("风险记录已撤回或关闭，不能再生成处置方案")
        plan_content = build_plan_content(
            risk_snapshot=risk,
            area=AffectedArea(
                area["area_id"], zone_record_id, area.get("location", ""),
                float(area["area_m2"]), tuple(area.get("polygon", [])),
                area.get("observed_at", risk["captured_at"]),
            ),
            clearing_zones=[
                ClearingZone(z["zone_code"], z["method"], float(z["area_m2"]))
                for z in clearing_zones
            ],
            notice_recipients=list(notice_recipients),
            chemical_batches=list(chemical_batches),
            reason=reason,
            deadline_hours=deadline_hours,
        )
        ledger_id = "lg-" + uuid.uuid4().hex[:16]
        now = self._now()
        sha = content_hash(plan_content)
        with transaction(self.db):
            self.db.execute(
                "INSERT INTO treatment_ledgers(ledger_id,alert_id,zone_record_id,current_version,"
                "status,created_at,updated_at) VALUES(?,?,?,1,'active',?,?)",
                (ledger_id, alert_id, zone_record_id, now, now),
            )
            self.db.execute(
                "INSERT INTO treatment_plan_versions(ledger_id,version,alert_id,content_json,"
                "content_sha256,change_kind,reason,effective_at,created_by,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (ledger_id, 1, alert_id, canonical_json(plan_content), sha,
                 "initial", reason.strip(), now, actor.user_id, now),
            )
            self._instantiate_steps(ledger_id, 1, plan_content, 0, now)
            self._event(ledger_id, 1, "plan.generated", actor.user_id,
                        {"change_kind": "initial", "sha256": sha, "reason": reason})
            audit(self.db, "treatment_ledger", ledger_id, "plan_generated", actor.user_id,
                  {"version": 1, "alert_id": alert_id, "sha256": sha})
        return self.ledger_view(token, ledger_id)

    def revise_plan(
        self,
        token: str,
        ledger_id: str,
        *,
        change_kind: str,
        reason: str,
        area: Mapping[str, Any] | None = None,
        clearing_zones: Sequence[Mapping[str, Any]] | None = None,
        notice_recipients: Sequence[str] | None = None,
        chemical_batches: Sequence[str] | None = None,
        deadline_hours: Mapping[str, float] | None = None,
    ) -> dict[str, Any]:
        actor = self._require(token, "ledger.plan")
        if change_kind not in {"risk_withdrawn", "scope_changed", "plan_adjusted"}:
            raise ValidationFailed("change_kind 必须是 risk_withdrawn/scope_changed/plan_adjusted")
        if not reason.strip():
            raise ValidationFailed("修订原因不能为空")
        ledger = self._ledger_row(ledger_id)
        if change_kind == "risk_withdrawn" and ledger["closure_kind"] == "risk_withdrawn":
            raise Conflict("风险已撤回，不能重复生成撤回版本")
        if ledger["status"] in _TERMINAL_LEDGER:
            raise InvalidState("台账已关闭，不能再生成新版本")
        old_version_row = self._version_row(ledger_id, ledger["current_version"])
        old_content = self._content(old_version_row)
        with transaction(self.db):
            if change_kind == "risk_withdrawn":
                self.db.execute(
                    "UPDATE alerts SET status='withdrawn' WHERE alert_id=? AND zone_record_id=?",
                    (ledger["alert_id"], ledger["zone_record_id"]),
                )
            risk = self._risk_snapshot(ledger["zone_record_id"], ledger["alert_id"])
            old_area = old_content["area"]
            area_raw = dict(old_area)
            if area:
                area_raw.update({k: v for k, v in area.items() if v is not None})
            new_area = AffectedArea(
                area_raw["area_id"],
                ledger["zone_record_id"],
                area_raw.get("location", ""),
                float(area_raw["area_m2"]),
                tuple(area_raw.get("polygon", [])),
                area_raw.get("observed_at", old_area.get("observed_at", risk["captured_at"])),
            )
            zones_source = clearing_zones if clearing_zones is not None else old_content["clearing_zones"]
            new_content = build_plan_content(
                risk_snapshot=risk,
                area=new_area,
                clearing_zones=[
                    z if isinstance(z, ClearingZone)
                    else ClearingZone(z["zone_code"], z["method"], float(z["area_m2"]))
                    for z in zones_source
                ],
                notice_recipients=list(notice_recipients) if notice_recipients is not None
                else old_content["notice_recipients"],
                chemical_batches=list(chemical_batches) if chemical_batches is not None
                else old_content["chemical_batches"],
                reason=reason,
                deadline_hours=deadline_hours if deadline_hours is not None
                else old_content["deadline_hours"],
            )
            semantic_old = _strip_captured(old_content)
            if _strip_captured(new_content) == semantic_old and change_kind != "risk_withdrawn":
                raise Conflict("新方案与当前版本内容一致，无需生成新版本")
            if change_kind == "risk_withdrawn" and semantic_old["risk"]["status"] == "withdrawn":
                raise Conflict("风险已撤回，不能重复生成撤回版本")
            new_version = ledger["current_version"] + 1
            now = self._now()
            sha = content_hash(new_content)
            self.db.execute(
                "UPDATE treatment_plan_versions SET superseded_at=? WHERE ledger_id=? AND version=?",
                (now, ledger_id, ledger["current_version"]),
            )
            self.db.execute(
                "INSERT INTO treatment_plan_versions(ledger_id,version,alert_id,content_json,"
                "content_sha256,change_kind,reason,effective_at,created_by,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (ledger_id, new_version, ledger["alert_id"], canonical_json(new_content), sha,
                 change_kind, reason.strip(), now, actor.user_id, now),
            )
            if change_kind == "risk_withdrawn":
                # 撤回版本是终结版本：不再派生新任务，旧任务全部保留在旧版本之下。
                self.db.execute(
                    "UPDATE treatment_ledgers SET current_version=?,status='closed',"
                    "closure_kind='risk_withdrawn',closed_at=?,closed_by=?,updated_at=? "
                    "WHERE ledger_id=?",
                    (new_version, now, actor.user_id, now, ledger_id),
                )
            else:
                self._instantiate_steps(ledger_id, new_version, new_content, 0, now)
                # 旧版本未完成的步骤让位于新版本，已完成步骤原样保留作为来历。
                self.db.execute(
                    "UPDATE treatment_step_instances SET status='superseded' WHERE ledger_id=? "
                    "AND plan_version=? AND status IN ('pending','in_progress')",
                    (ledger_id, ledger["current_version"]),
                )
                self.db.execute(
                    "UPDATE treatment_ledgers SET current_version=?,updated_at=? WHERE ledger_id=?",
                    (new_version, now, ledger_id),
                )
            self._event(ledger_id, new_version, "plan.generated", actor.user_id,
                        {"change_kind": change_kind, "sha256": sha, "reason": reason})
            audit(self.db, "treatment_ledger", ledger_id, "plan_revised", actor.user_id,
                  {"version": new_version, "change_kind": change_kind, "sha256": sha})
        return self.ledger_view(token, ledger_id)

    def _instantiate_steps(self, ledger_id: str, version: int, content: Mapping[str, Any],
                           rework_round: int, anchor_text: str) -> None:
        """按方案生成步骤实例；前三个步骤以版本生效时刻锚定截止，其余待前置完成再锚定。"""
        deadlines = content["deadline_hours"]
        for seq, item in enumerate(steps_for_version(content, rework_round), start=1):
            key = item["step_key"]
            round_no = item.get("rework_round", 0)
            deadline_at = None
            if key in {"notice", "isolation", "clearing"}:
                deadline_at = self._deadline(anchor_text, deadlines[key])
            self.db.execute(
                "INSERT INTO treatment_step_instances(ledger_id,plan_version,step_key,step_seq,"
                "rework_round,deadline_hours,deadline_at,status,evidence_json) "
                "VALUES(?,?,?,?,?,?,?,'pending','{}')",
                (ledger_id, version, key, self._next_seq(ledger_id), round_no,
                 deadlines[key if key in deadlines else REWORK_KEY], deadline_at),
            )

    def _next_seq(self, ledger_id: str) -> int:
        row = self.db.execute(
            "SELECT COALESCE(MAX(step_seq),0)+1 AS next_seq FROM treatment_step_instances "
            "WHERE ledger_id=?",
            (ledger_id,),
        ).fetchone()
        # 批量插入时每条都取一次序列值
        return int(row["next_seq"])

    # ------------------------------------------------------------ 进度上报
    def report_progress(self, token: str, ledger_id: str, report: Mapping[str, Any]) -> dict[str, Any]:
        actor = self._require(token, "ledger.execute")
        report_id = str(report.get("report_id", "")).strip()
        if not report_id:
            raise ValidationFailed("report_id 必填")
        payload = report.get("payload")
        if not isinstance(payload, Mapping):
            raise ValidationFailed("payload 必须是对象")
        digest = content_hash({"step_key": report.get("step_key"),
                              "rework_round": report.get("rework_round", 0),
                              "payload": dict(payload)})
        stored = self.db.execute(
            "SELECT payload_sha256,response_json FROM treatment_progress_reports WHERE report_id=?",
            (report_id,),
        ).fetchone()
        if stored is not None:
            if stored["payload_sha256"] != digest:
                raise Conflict("同一 report_id 对应不同上报内容")
            # 精确重放：直接返回首次结果，不再推进任何步骤。
            return json.loads(stored["response_json"])

        ledger = self._ledger_row(ledger_id)
        if ledger["status"] in _TERMINAL_LEDGER:
            raise InvalidState("台账已关闭，不能再上报进度")
        version = ledger["current_version"]
        version_row = self._version_row(ledger_id, version)
        content = self._content(version_row)
        gate = self._gate(ledger_id, version)
        step_key = str(report.get("step_key") or gate["step_key"])
        rework_round = int(report.get("rework_round", gate["rework_round"]))
        if step_key != gate["step_key"] or rework_round != gate["rework_round"]:
            raise InvalidState(
                f"当前必须先完成 {gate['step_key']}"
                + (f"（第 {gate['rework_round']} 轮）" if gate["rework_round"] else "")
            )
        if step_key == "review":
            # 独立复核使用单独权限，并在处理器内再次校验独立性。
            self._require(token, "ledger.review")

        handler = {
            "notice": self._handle_notice,
            "isolation": self._handle_isolation,
            "clearing": self._handle_clearing,
            "reinspection": self._handle_reinspection,
            REWORK_KEY: self._handle_rework,
            "disposal": self._handle_disposal,
            "review": self._handle_review,
        }[step_key]

        with transaction(self.db):
            result = handler(ledger, version_row, content, gate, dict(payload), actor.user_id, report_id)
            self.db.execute(
                "INSERT INTO treatment_progress_reports(report_id,ledger_id,plan_version,reporter,"
                "step_key,rework_round,payload_sha256,response_json,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (report_id, ledger_id, version, actor.user_id, step_key, rework_round,
                 digest, canonical_json(result), self._now()),
            )
        return result

    def _gate(self, ledger_id: str, version: int) -> sqlite3.Row:
        # 部分完成的分区清除为 in_progress，仍是门控步骤；failed 步骤已被返工环接替。
        row = self.db.execute(
            "SELECT * FROM treatment_step_instances WHERE ledger_id=? AND plan_version=? "
            "AND status IN ('pending','in_progress') ORDER BY step_seq LIMIT 1",
            (ledger_id, version),
        ).fetchone()
        if row is None:
            raise InvalidState("当前版本没有待执行步骤")
        return row

    def _add_progress(self, instance: sqlite3.Row, key: str, report_id: str,
                      actor: str, payload: Mapping[str, Any]) -> None:
        try:
            self.db.execute(
                "INSERT INTO treatment_step_progress(step_instance_id,progress_key,report_id,"
                "payload_json,created_by,created_at) VALUES(?,?,?,?,?,?)",
                (instance["step_instance_id"], key, report_id, canonical_json(payload),
                 actor, self._now()),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"进度 {key} 已上报，不得重复推进") from exc

    def _complete(self, instance: sqlite3.Row, actor: str, evidence: Mapping[str, Any],
                  completed_at: str | None = None) -> str:
        completed_at = completed_at or self._now()
        self.db.execute(
            "UPDATE treatment_step_instances SET status='completed',evidence_json=?,"
            "completed_at=?,completed_by=? WHERE step_instance_id=?",
            (canonical_json(evidence), completed_at, actor, instance["step_instance_id"]),
        )
        return completed_at

    def _fail_instance(self, instance: sqlite3.Row, evidence: Mapping[str, Any]) -> None:
        self.db.execute(
            "UPDATE treatment_step_instances SET status='failed',evidence_json=? WHERE step_instance_id=?",
            (canonical_json(evidence), instance["step_instance_id"]),
        )

    def _open_rework_loop(self, ledger: sqlite3.Row, content: Mapping[str, Any],
                          failed_round: int, reason_payload: Mapping[str, Any],
                          actor: str) -> int:
        """复查/复核不通过后，开出新一轮返工与复查，截止时刻从当下起算。"""
        new_round = failed_round + 1
        now = self._now()
        deadlines = content["deadline_hours"]
        self.db.execute(
            "INSERT INTO treatment_step_instances(ledger_id,plan_version,step_key,step_seq,"
            "rework_round,deadline_hours,deadline_at,status,evidence_json) "
            "VALUES(?,?,?,?,?,?,?,'pending','{}')",
            (ledger["ledger_id"], ledger["current_version"], REWORK_KEY,
             self._next_seq(ledger["ledger_id"]), new_round, deadlines[REWORK_KEY],
             self._deadline(now, deadlines[REWORK_KEY])),
        )
        rework_row = self.db.execute(
            "SELECT * FROM treatment_step_instances WHERE ledger_id=? AND plan_version=? "
            "AND step_key=? AND rework_round=? ORDER BY step_instance_id DESC LIMIT 1",
            (ledger["ledger_id"], ledger["current_version"], REWORK_KEY, new_round),
        ).fetchone()
        self._event(ledger["ledger_id"], ledger["current_version"], "rework.opened", actor,
                    {"after_round": failed_round, **reason_payload},
                    rework_row["step_instance_id"], REWORK_KEY, new_round)
        self.db.execute(
            "INSERT INTO treatment_step_instances(ledger_id,plan_version,step_key,step_seq,"
            "rework_round,deadline_hours,deadline_at,status,evidence_json) "
            "VALUES(?,?,?,?,?,?,?,'pending','{}')",
            (ledger["ledger_id"], ledger["current_version"], "reinspection",
             self._next_seq(ledger["ledger_id"]), new_round, deadlines["reinspection"], None),
        )
        return new_round

    def _ensure_round_instance(self, ledger_id: str, version: int, key: str, round_no: int,
                               content: Mapping[str, Any]) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT * FROM treatment_step_instances WHERE ledger_id=? AND plan_version=? "
            "AND step_key=? AND rework_round=?",
            (ledger_id, version, key, round_no),
        ).fetchone()
        if row is None:
            self.db.execute(
                "INSERT INTO treatment_step_instances(ledger_id,plan_version,step_key,step_seq,"
                "rework_round,deadline_hours,deadline_at,status,evidence_json) "
                "VALUES(?,?,?,?,?,?,?,'pending','{}')",
                (ledger_id, version, key, self._next_seq(ledger_id), round_no,
                 content["deadline_hours"][key], None),
            )
            row = self.db.execute(
                "SELECT * FROM treatment_step_instances WHERE ledger_id=? AND plan_version=? "
                "AND step_key=? AND rework_round=?",
                (ledger_id, version, key, round_no),
            ).fetchone()
        return row

    # -- 各步骤处理器 -------------------------------------------------------
    def _handle_notice(self, ledger, version_row, content, instance, payload, actor, report_id):
        signer = str(payload.get("signer", "")).strip()
        if not payload.get("signed") or not signer:
            raise ValidationFailed("通知签收需要 signed=true 与签收人 signer")
        unknown = sorted(set(payload.get("signed_recipients", [])) - set(content["notice_recipients"]))
        if unknown:
            raise ValidationFailed(f"签收人不在通知名单: {unknown}")
        self._add_progress(instance, "signoff", report_id, actor, payload)
        completed_at = self._complete(instance, actor, payload)
        self._event(ledger["ledger_id"], version_row["version"], "notice.signed", actor, payload,
                    instance["step_instance_id"], "notice")
        return self._advance_result(ledger, instance, completed_at, {"signer": signer})

    def _handle_isolation(self, ledger, version_row, content, instance, payload, actor, report_id):
        if not payload.get("sealed"):
            raise ValidationFailed("现场隔离需要 sealed=true")
        measures = str(payload.get("measures", "")).strip()
        if not measures:
            raise ValidationFailed("现场隔离需要说明隔离措施 measures")
        self._add_progress(instance, "seal", report_id, actor, payload)
        completed_at = self._complete(instance, actor, payload)
        self._event(ledger["ledger_id"], version_row["version"], "isolation.sealed", actor, payload,
                    instance["step_instance_id"], "isolation")
        return self._advance_result(ledger, instance, completed_at, {"measures": measures})

    def _handle_clearing(self, ledger, version_row, content, instance, payload, actor, report_id):
        zones = payload.get("zones")
        if not isinstance(zones, list) or not zones:
            raise ValidationFailed("分区清除需要上报 zones 列表")
        plan_codes = {z["zone_code"] for z in content["clearing_zones"]}
        incoming = [str(z.get("zone_code", "")).strip() for z in zones]
        if any(not code for code in incoming):
            raise ValidationFailed("清除分区编号不能为空")
        unknown = sorted(set(incoming) - plan_codes)
        if unknown:
            raise ValidationFailed(f"分区不在本版本方案: {unknown}")
        advanced_zones: list[str] = []
        cleared_times: list[str] = []
        for item in zones:
            code = str(item["zone_code"]).strip()
            self._add_progress(instance, f"zone:{code}", report_id, actor, dict(item))
            advanced_zones.append(code)
            if item.get("cleared_at"):
                cleared_times.append(str(item["cleared_at"]))
        self._event(ledger["ledger_id"], version_row["version"], "clearing.progress", actor,
                    {"zones": advanced_zones}, instance["step_instance_id"], "clearing",
                    instance["rework_round"])
        done_codes = {
            row["progress_key"].split(":", 1)[1]
            for row in self.db.execute(
                "SELECT progress_key FROM treatment_step_progress WHERE step_instance_id=?",
                (instance["step_instance_id"],),
            ).fetchall()
            if row["progress_key"].startswith("zone:")
        }
        remaining = sorted(plan_codes - done_codes)
        if remaining:
            self.db.execute(
                "UPDATE treatment_step_instances SET status='in_progress' WHERE step_instance_id=? "
                "AND status='pending'",
                (instance["step_instance_id"],),
            )
            return {"gate_advanced": False, "step_key": "clearing",
                    "status": "in_progress", "cleared_zones": sorted(done_codes),
                    "remaining_zones": remaining, "plan_version": version_row["version"]}
        if not payload.get("all_zones_cleared"):
            raise ValidationFailed("全部分区已清除但未声明 all_zones_cleared=true")
        used_batches = payload.get("chemical_batches_used")
        if not isinstance(used_batches, list) or not used_batches:
            raise ValidationFailed("清除完成必须登记实际使用的药剂批次 chemical_batches_used")
        unknown_batches = sorted(set(used_batches) - set(content["chemical_batches"]))
        if unknown_batches:
            raise ValidationFailed(f"药剂批次不在本版本方案: {unknown_batches}")
        completed_at = max(cleared_times) if cleared_times else self._now()
        parse_utc(completed_at, "cleared_at")
        self._complete(instance, actor, {"zones": sorted(done_codes),
                                         "chemical_batches_used": sorted(set(used_batches))},
                       completed_at)
        self._event(ledger["ledger_id"], version_row["version"], "clearing.completed", actor,
                    {"zones": sorted(done_codes)}, instance["step_instance_id"], "clearing",
                    instance["rework_round"])
        # 锚定本轮复查截止时刻
        reinspection = self._ensure_round_instance(
            ledger["ledger_id"], version_row["version"], "reinspection",
            instance["rework_round"], content,
        )
        deadline = self._deadline(completed_at, content["deadline_hours"]["reinspection"])
        self.db.execute(
            "UPDATE treatment_step_instances SET deadline_at=? WHERE step_instance_id=?",
            (deadline, reinspection["step_instance_id"]),
        )
        return self._advance_result(ledger, instance, completed_at,
                                   {"cleared_zones": sorted(done_codes),
                                    "reinspection_deadline_at": deadline})

    def _handle_reinspection(self, ledger, version_row, content, instance, payload, actor, report_id):
        quadrats = payload.get("quadrat_ids")
        if not isinstance(quadrats, list) or not quadrats or any(not str(q).strip() for q in quadrats):
            raise ValidationFailed("复查必须登记至少一个复查样方 quadrat_ids")
        residual = payload.get("residual_count")
        if not isinstance(residual, int) or residual < 0:
            raise ValidationFailed("residual_count 必须是非负整数")
        if not str(payload.get("inspector", "")).strip():
            raise ValidationFailed("复查需要 inspector")
        passed = bool(payload.get("passed"))
        self._add_progress(instance, f"inspection:{instance['rework_round']}", report_id, actor, payload)
        if passed:
            if residual != 0:
                raise ValidationFailed("复查通过要求 residual_count=0")
            completed_at = self._complete(instance, actor, payload)
            self._event(ledger["ledger_id"], version_row["version"], "reinspection.passed", actor,
                        {"quadrat_ids": quadrats}, instance["step_instance_id"], "reinspection",
                        instance["rework_round"])
            disposal = self._ensure_round_instance(
                ledger["ledger_id"], version_row["version"], "disposal",
                instance["rework_round"], content,
            )
            deadline = self._deadline(completed_at, content["deadline_hours"]["disposal"])
            self.db.execute(
                "UPDATE treatment_step_instances SET deadline_at=? WHERE step_instance_id=?",
                (deadline, disposal["step_instance_id"]),
            )
            return self._advance_result(ledger, instance, completed_at,
                                        {"disposal_deadline_at": deadline})
        self._fail_instance(instance, payload)
        # 同轮尚未到期的废弃物/复核步骤随复查失败一并作废，改由新一轮返工链接替。
        self.db.execute(
            "UPDATE treatment_step_instances SET status='superseded' WHERE ledger_id=? "
            "AND plan_version=? AND step_key IN ('disposal','review') AND rework_round=? "
            "AND status='pending'",
            (ledger["ledger_id"], version_row["version"], instance["rework_round"]),
        )
        new_round = self._open_rework_loop(
            ledger, content, instance["rework_round"],
            {"residual_count": residual, "quadrat_ids": quadrats}, actor,
        )
        self._event(ledger["ledger_id"], version_row["version"], "reinspection.failed", actor,
                    {"residual_count": residual, "quadrat_ids": quadrats, "new_round": new_round},
                    instance["step_instance_id"], "reinspection", instance["rework_round"])
        return {"gate_advanced": True, "step_key": REWORK_KEY, "rework_round": new_round,
                "status": "pending", "plan_version": version_row["version"],
                "because": "reinspection_failed"}

    def _handle_rework(self, ledger, version_row, content, instance, payload, actor, report_id):
        zones = payload.get("zones")
        if not isinstance(zones, list) or not zones:
            raise ValidationFailed("返工必须上报返工分区 zones")
        used_batches = payload.get("chemical_batches_used")
        if used_batches is not None:
            unknown_batches = sorted(set(used_batches) - set(content["chemical_batches"]))
            if unknown_batches:
                raise ValidationFailed(f"药剂批次不在本版本方案: {unknown_batches}")
        self._add_progress(instance, f"rework:{instance['rework_round']}", report_id, actor, payload)
        completed_at = self._complete(instance, actor, payload)
        self._event(ledger["ledger_id"], version_row["version"], "rework.completed", actor,
                    {"zones": zones, "round": instance["rework_round"]},
                    instance["step_instance_id"], REWORK_KEY, instance["rework_round"])
        reinspection = self._ensure_round_instance(
            ledger["ledger_id"], version_row["version"], "reinspection",
            instance["rework_round"], content,
        )
        deadline = self._deadline(completed_at, content["deadline_hours"]["reinspection"])
        self.db.execute(
            "UPDATE treatment_step_instances SET deadline_at=? WHERE step_instance_id=?",
            (deadline, reinspection["step_instance_id"]),
        )
        return self._advance_result(ledger, instance, completed_at,
                                   {"reinspection_deadline_at": deadline})

    def _handle_disposal(self, ledger, version_row, content, instance, payload, actor, report_id):
        for field_name in ("manifest_id", "destination", "carrier"):
            if not str(payload.get(field_name, "")).strip():
                raise ValidationFailed(f"废弃物去向需要 {field_name}")
        mass = payload.get("mass_kg")
        if not isinstance(mass, (int, float)) or mass <= 0:
            raise ValidationFailed("mass_kg 必须是正数")
        self._add_progress(instance, f"manifest:{instance['rework_round']}", report_id, actor, payload)
        completed_at = self._complete(instance, actor, payload)
        self._event(ledger["ledger_id"], version_row["version"], "disposal.manifested", actor,
                    {"manifest_id": payload["manifest_id"], "destination": payload["destination"]},
                    instance["step_instance_id"], "disposal", instance["rework_round"])
        review = self._ensure_round_instance(
            ledger["ledger_id"], version_row["version"], "review",
            instance["rework_round"], content,
        )
        deadline = self._deadline(completed_at, content["deadline_hours"]["review"])
        self.db.execute(
            "UPDATE treatment_step_instances SET deadline_at=? WHERE step_instance_id=?",
            (deadline, review["step_instance_id"]),
        )
        return self._advance_result(ledger, instance, completed_at,
                                   {"review_deadline_at": deadline})

    def _handle_review(self, ledger, version_row, content, instance, payload, actor, report_id):
        finding = str(payload.get("finding", "")).strip()
        if not finding:
            raise ValidationFailed("独立复核必须填写 finding")
        executors = {
            row["completed_by"]
            for row in self.db.execute(
                "SELECT DISTINCT completed_by FROM treatment_step_instances WHERE ledger_id=? "
                "AND status='completed' AND step_key!='review' AND completed_by IS NOT NULL",
                (ledger["ledger_id"],),
            ).fetchall()
        }
        if actor in executors:
            raise InvalidState("复核人不能与执行人员为同一人，必须独立复核")
        approved = bool(payload.get("approved"))
        self._add_progress(instance, f"review:{instance['rework_round']}", report_id, actor, payload)
        if approved:
            completed_at = self._complete(instance, actor, payload)
            now = self._now()
            self.db.execute(
                "UPDATE treatment_ledgers SET status='closed',closure_kind='closed',"
                "closed_at=?,closed_by=?,updated_at=? WHERE ledger_id=?",
                (now, actor, now, ledger["ledger_id"]),
            )
            self._event(ledger["ledger_id"], version_row["version"], "review.approved", actor,
                        {"finding": finding}, instance["step_instance_id"], "review",
                        instance["rework_round"])
            audit(self.db, "treatment_ledger", ledger["ledger_id"], "closed", actor,
                  {"version": version_row["version"], "closure_kind": "closed"})
            return {"gate_advanced": True, "step_key": "review", "status": "closed",
                    "ledger_status": "closed", "plan_version": version_row["version"]}
        self._fail_instance(instance, payload)
        # 复核不通过：同轮链条已走完，直接开新一轮返工（复查通过后会重建该轮废弃物/复核）。
        new_round = self._open_rework_loop(
            ledger, content, instance["rework_round"], {"finding": finding}, actor,
        )
        self._event(ledger["ledger_id"], version_row["version"], "review.rejected", actor,
                    {"finding": finding, "new_round": new_round},
                    instance["step_instance_id"], "review", instance["rework_round"])
        return {"gate_advanced": True, "step_key": REWORK_KEY, "rework_round": new_round,
                "status": "pending", "plan_version": version_row["version"],
                "because": "review_rejected"}

    def _advance_result(self, ledger, instance, completed_at, extra: Mapping[str, Any]) -> dict[str, Any]:
        result = {
            "gate_advanced": True,
            "step_key": instance["step_key"],
            "rework_round": instance["rework_round"],
            "completed_at": completed_at,
            "plan_version": ledger["current_version"],
        }
        result.update(extra)
        return result

    # ------------------------------------------------------------ 查询视图
    def ledger_view(self, token: str, ledger_id: str) -> dict[str, Any]:
        self._require(token, "ledger.read")
        ledger = self._ledger_row(ledger_id)
        now = self.clock.now()
        versions = []
        for vrow in self.db.execute(
            "SELECT * FROM treatment_plan_versions WHERE ledger_id=? ORDER BY version",
            (ledger_id,),
        ).fetchall():
            versions.append({
                "version": vrow["version"],
                "change_kind": vrow["change_kind"],
                "reason": vrow["reason"],
                "content_sha256": vrow["content_sha256"],
                "effective_at": vrow["effective_at"],
                "superseded_at": vrow["superseded_at"],
                "created_by": vrow["created_by"],
                "content": self._content(vrow),
            })
        steps = [
            self._step_view(row, now)
            for row in self.db.execute(
                "SELECT * FROM treatment_step_instances WHERE ledger_id=? ORDER BY step_seq",
                (ledger_id,),
            ).fetchall()
        ]
        for step in steps:
            step["progress"] = [
                {"progress_key": row["progress_key"], "report_id": row["report_id"],
                 "payload": json.loads(row["payload_json"]), "created_by": row["created_by"],
                 "created_at": row["created_at"]}
                for row in self.db.execute(
                    "SELECT progress_key,report_id,payload_json,created_by,created_at "
                    "FROM treatment_step_progress WHERE step_instance_id=? ORDER BY rowid",
                    (step["step_instance_id"],),
                ).fetchall()
            ]
        current_steps = [s for s in steps if s["plan_version"] == ledger["current_version"]]
        outstanding = [s for s in current_steps if s["status"] in ("pending", "in_progress")]
        next_step = outstanding[0] if outstanding else None
        changes = [
            {
                "event_id": row["event_id"],
                "plan_version": row["plan_version"],
                "step_key": row["step_key"],
                "rework_round": row["rework_round"],
                "event_type": row["event_type"],
                "actor": row["actor"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in self.db.execute(
                "SELECT * FROM treatment_step_events WHERE ledger_id=? ORDER BY event_id",
                (ledger_id,),
            ).fetchall()
        ]
        return {
            "ledger_id": ledger["ledger_id"],
            "alert_id": ledger["alert_id"],
            "zone_record_id": ledger["zone_record_id"],
            "status": ledger["status"],
            "closure_kind": ledger["closure_kind"],
            "closed_at": ledger["closed_at"],
            "closed_by": ledger["closed_by"],
            "current_version": ledger["current_version"],
            "as_of": utc_text(now),
            "plan": next((v for v in versions if v["version"] == ledger["current_version"]), None),
            "versions": versions,
            "steps": steps,
            "missing_steps": [
                {"step_key": s["step_key"], "rework_round": s["rework_round"],
                 "label": s["label"], "status": s["status"],
                 "deadline_at": s["deadline_at"], "overdue": s["overdue"]}
                for s in outstanding
            ],
            "next_step": None if next_step is None else {
                "step_key": next_step["step_key"],
                "rework_round": next_step["rework_round"],
                "label": next_step["label"],
                "status": next_step["status"],
                "deadline_at": next_step["deadline_at"],
                "overdue": next_step["overdue"],
            },
            "next_deadline_at": _next_deadline(outstanding),
            "changes": changes,
        }

    def _step_view(self, row: sqlite3.Row, now) -> dict[str, Any]:
        deadline_at = row["deadline_at"]
        overdue = bool(
            row["status"] in ("pending", "in_progress") and deadline_at is not None
            and parse_utc(deadline_at) < now
        )
        round_no = row["rework_round"]
        label = STEP_LABELS.get(row["step_key"], row["step_key"])
        if row["step_key"] == REWORK_KEY:
            label = f"返工清除（第 {round_no} 轮）"
        elif round_no and row["step_key"] in {"reinspection", "disposal", "review"}:
            label = f"{label}（第 {round_no} 轮）"
        return {
            "step_instance_id": row["step_instance_id"],
            "plan_version": row["plan_version"],
            "step_key": row["step_key"],
            "rework_round": round_no,
            "step_seq": row["step_seq"],
            "label": label,
            "status": row["status"],
            "deadline_hours": row["deadline_hours"],
            "deadline_at": deadline_at,
            "overdue": overdue,
            "completed_at": row["completed_at"],
            "completed_by": row["completed_by"],
            "evidence": json.loads(row["evidence_json"] or "{}"),
        }

    def list_ledgers(self, token: str) -> list[dict[str, Any]]:
        self._require(token, "ledger.read")
        return [
            dict(row) for row in self.db.execute(
                "SELECT ledger_id,alert_id,zone_record_id,current_version,status,closure_kind,"
                "closed_at,updated_at FROM treatment_ledgers ORDER BY created_at"
            ).fetchall()
        ]


def _strip_captured(content: Mapping[str, Any]) -> dict[str, Any]:
    """语义比较：忽略生成时刻与修订说明（后者是版本元数据，非可执行内容）。"""
    clone = json.loads(canonical_json(content))
    clone.get("risk", {}).pop("captured_at", None)
    clone.pop("reason", None)
    return clone


def _next_deadline(pending: Sequence[Mapping[str, Any]]) -> str | None:
    dated = [s["deadline_at"] for s in pending if s["deadline_at"] is not None]
    return min(dated) if dated else None
