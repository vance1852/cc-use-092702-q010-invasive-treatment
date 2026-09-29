from __future__ import annotations

import unittest
from datetime import datetime, timezone

from biosafety_ops.clock import FrozenClock
from biosafety_ops.errors import Conflict, InvalidState, ValidationFailed
from biosafety_ops.models import MonitoringRecord, ZoneRecord
from biosafety_ops.service import BiosafetyService

START = datetime(2026, 9, 29, 8, 0, tzinfo=timezone.utc)


def area(area_m2=1200.0):
    return {"area_id": "A1", "location": "北坡林缘 3 号界桩", "area_m2": area_m2,
            "polygon": ["p1", "p2"], "observed_at": "2026-09-29T06:30:00Z"}


ZONES = [{"zone_code": "C1", "method": "人工刈割", "area_m2": 600},
         {"zone_code": "C2", "method": "药剂喷施", "area_m2": 600}]


class LedgerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FrozenClock(START)
        self.s = BiosafetyService(clock=self.clock)
        self.s.bootstrap()
        self.admin = self.s.auth.login("admin", "biosafety-admin")
        self.op = self.s.auth.login("operator", "biosafety-operator")
        self.qa = self.s.auth.login("quality", "biosafety-quality")
        self.s.register_zone_record(self.admin, ZoneRecord("Z1", "林缘-北坡", "quarantine", 500, 5))
        ingested = self.s.ingest_monitoring_record(
            self.admin,
            MonitoringRecord("M1", "Z1", "patrol-7", 120, 250, 90, "2026-09-29T06:00:00Z"),
        )
        self.alert_id = ingested["alert_id"]

    def open_ledger(self, **over):
        params = dict(
            alert_id=self.alert_id, zone_record_id="Z1", area=area(),
            clearing_zones=ZONES, notice_recipients=["crew-a", "crew-b"],
            chemical_batches=["LOT-2026-09", "LOT-2026-10"],
            reason="巡护队确认成片加拿大一枝黄花",
        )
        params.update(over)
        view = self.s.ledger.create_ledger(self.admin, **params)
        self.lid = view["ledger_id"]
        return view

    def report(self, report_id, payload, token=None, **extra):
        body = {"report_id": report_id, "payload": payload}
        body.update(extra)
        return self.s.ledger.report_progress(token or self.op, self.lid, body)

    def view(self, token=None):
        return self.s.ledger.ledger_view(token or self.qa, self.lid)

    def notice(self):
        return self.report("r-notice", {"signed": True, "signer": "crew-a",
                                        "signed_recipients": ["crew-a"]})

    def isolate(self):
        return self.report("r-isolate", {"sealed": True, "measures": "警戒带+警示牌"})

    def clear_all(self):
        self.report("r-c1", {"zones": [{"zone_code": "C1", "cleared_at": "2026-09-29T11:00:00Z"}]})
        return self.report("r-c2", {"zones": [{"zone_code": "C2", "cleared_at": "2026-09-29T12:00:00Z"}],
                                    "all_zones_cleared": True,
                                    "chemical_batches_used": ["LOT-2026-09"]})

    def reinspect(self, passed=True, residual=0, report_id="r-rein"):
        return self.report(report_id, {"quadrat_ids": ["Q1", "Q2"], "residual_count": residual,
                                       "inspector": "zhang", "passed": passed})

    def dispose(self):
        return self.report("r-dispose", {"manifest_id": "MF-1", "destination": "市危废焚烧中心",
                                         "carrier": "净原公司", "mass_kg": 480})

    def close_happy_path(self):
        self.open_ledger()
        self.notice()
        self.isolate()
        self.clear_all()
        self.reinspect()
        self.dispose()


class PlanVersionTests(LedgerTestBase):
    def test_plan_is_immutable_and_anchors_deadlines_to_clock(self) -> None:
        view = self.open_ledger()
        self.assertEqual(view["current_version"], 1)
        self.assertEqual(view["next_step"]["step_key"], "notice")
        self.assertEqual(view["next_step"]["deadline_at"], "2026-09-29T20:00:00Z")
        self.assertEqual(len(view["plan"]["content_sha256"]), 64)
        # 风险快照记录的是方案生成当时的风险状态。
        self.assertEqual(view["plan"]["content"]["risk"]["alert_id"], self.alert_id)
        self.assertEqual(view["plan"]["content"]["risk"]["severity"], "critical")

    def test_scope_change_creates_new_version_without_losing_history(self) -> None:
        self.open_ledger()
        self.notice()
        revised = self.s.ledger.revise_plan(
            self.admin, self.lid, change_kind="scope_changed", reason="向西侧扩散 300m2",
            area={**area(), "area_m2": 1500},
        )
        self.assertEqual(revised["current_version"], 2)
        # 旧版本下已完成的通知签收保留来历；旧版本未完成步骤已让位。
        v1_steps = {(s["step_key"], s["rework_round"]): s for s in revised["steps"]
                    if s["plan_version"] == 1}
        self.assertEqual(v1_steps[("notice", 0)]["status"], "completed")
        self.assertEqual(v1_steps[("clearing", 0)]["status"], "superseded")
        v2 = revised["plan"]["content"]
        self.assertEqual(v2["area"]["area_m2"], 1500)
        self.assertEqual(revised["next_step"]["step_key"], "notice")
        # 新版本门控从通知重新开始，且上报必须落在新版本上。
        self.report("r-notice-v2", {"signed": True, "signer": "crew-a",
                                    "signed_recipients": ["crew-a"]})
        view = self.view()
        self.assertEqual(view["changes"][0]["event_type"], "plan.generated")
        self.assertEqual({c["plan_version"] for c in view["changes"]}, {1, 2})

    def test_identical_revision_is_rejected(self) -> None:
        self.open_ledger()
        with self.assertRaises(Conflict):
            self.s.ledger.revise_plan(self.admin, self.lid, change_kind="scope_changed",
                                      reason="没有实际变化")

    def test_risk_withdrawal_closes_ledger_with_a_new_version(self) -> None:
        self.open_ledger()
        self.notice()
        closed = self.s.ledger.revise_plan(
            self.admin, self.lid, change_kind="risk_withdrawn", reason="上级核实为误报")
        self.assertEqual(closed["status"], "closed")
        self.assertEqual(closed["closure_kind"], "risk_withdrawn")
        self.assertEqual(closed["current_version"], 2)
        # 旧任务仍然完整可查。
        self.assertEqual(closed["versions"][0]["version"], 1)
        self.assertTrue(any(s["step_key"] == "notice" and s["status"] == "completed"
                            for s in closed["steps"] if s["plan_version"] == 1))
        with self.assertRaises(InvalidState):
            self.report("r-late", {"sealed": True, "measures": "x"})
        # 撤回不能重复生成版本。
        with self.assertRaises(Conflict):
            self.s.ledger.revise_plan(self.admin, self.lid, change_kind="risk_withdrawn",
                                      reason="再次撤回")

    def test_closed_ledger_cannot_be_revised(self) -> None:
        self.close_happy_path()
        self.report("r-review", {"finding": "齐全", "approved": True}, token=self.qa)
        with self.assertRaises(InvalidState):
            self.s.ledger.revise_plan(self.admin, self.lid, change_kind="plan_adjusted",
                                      reason="结案后调整")


class ExecutionGateTests(LedgerTestBase):
    def test_steps_must_follow_order(self) -> None:
        self.open_ledger()
        with self.assertRaises(InvalidState):
            self.report("r-jump", {"sealed": True, "measures": "x"}, step_key="isolation")
        self.notice()
        with self.assertRaises(ValidationFailed):
            self.report("r-bad-iso", {"sealed": False})

    def test_notice_signer_must_be_on_recipient_list(self) -> None:
        self.open_ledger()
        with self.assertRaises(ValidationFailed):
            self.report("r-x", {"signed": True, "signer": "outsider",
                                "signed_recipients": ["outsider"]})

    def test_partial_clearing_keeps_gate_and_blocks_duplicate_zone(self) -> None:
        self.open_ledger()
        self.notice()
        self.isolate()
        first = self.report("r-c1", {"zones": [{"zone_code": "C1"}]})
        self.assertFalse(first["gate_advanced"])
        self.assertEqual(first["remaining_zones"], ["C2"])
        # 外协队伍换 report_id 重复上报同一分区：不能推进两次。
        with self.assertRaises(Conflict):
            self.report("r-c1-dup", {"zones": [{"zone_code": "C1"}]})
        # 上报方案外分区被拒绝。
        with self.assertRaises(ValidationFailed):
            self.report("r-cx", {"zones": [{"zone_code": "C9"}]})
        # 完成清除时必须登记方案内药剂批次的实际使用。
        with self.assertRaises(ValidationFailed):
            self.report("r-c2-no-lot", {"zones": [{"zone_code": "C2"}],
                                        "all_zones_cleared": True})
        with self.assertRaises(ValidationFailed):
            self.report("r-c2-bad-lot", {"zones": [{"zone_code": "C2"}],
                                         "all_zones_cleared": True,
                                         "chemical_batches_used": ["LOT-FAKE"]})

    def test_reinspection_failure_opens_rework_then_closes(self) -> None:
        self.open_ledger()
        self.notice()
        self.isolate()
        self.clear_all()
        failed = self.reinspect(passed=False, residual=3, report_id="r-rein-fail")
        self.assertEqual(failed["step_key"], "rework")
        self.assertEqual(failed["rework_round"], 1)
        self.assertEqual(self.view()["next_step"]["step_key"], "rework")
        # 同轮废弃的 disposal/review 已被 supersede，门控是返工。
        self.report("r-rework", {"zones": ["Q2 周边 200m2"]})
        self.reinspect(passed=True, residual=0, report_id="r-rein-ok")
        self.dispose()
        result = self.report("r-review", {"finding": "资料齐全", "approved": True}, token=self.qa)
        self.assertEqual(result["status"], "closed")
        view = self.view()
        keys = [(s["step_key"], s["rework_round"], s["status"]) for s in view["steps"]]
        self.assertIn(("reinspection", 0, "failed"), keys)
        self.assertIn(("rework", 1, "completed"), keys)
        self.assertIn(("reinspection", 1, "completed"), keys)
        self.assertEqual(view["missing_steps"], [])

    def test_review_rejection_also_reopens_rework(self) -> None:
        self.close_happy_path()
        rejected = self.report("r-review-bad", {"finding": "台账照片缺失", "approved": False},
                               token=self.qa)
        self.assertEqual(rejected["because"], "review_rejected")
        self.report("r-rework", {"zones": ["全样方复喷"]})
        self.report("r-rein2", {"quadrat_ids": ["Q1"], "residual_count": 0,
                                "inspector": "li", "passed": True})
        self.report("r-dispose2", {"manifest_id": "MF-2", "destination": "焚烧中心",
                                   "carrier": "净原", "mass_kg": 30})
        self.report("r-review-ok", {"finding": "补齐", "approved": True}, token=self.qa)
        self.assertEqual(self.view()["status"], "closed")

    def test_reviewer_must_be_independent(self) -> None:
        # 全链条由 admin 亲自执行；admin 虽有复核权，也不能复核自己的执行。
        self.open_ledger()
        self.report("n", {"signed": True, "signer": "crew-a", "signed_recipients": ["crew-a"]},
                    token=self.admin)
        self.report("i", {"sealed": True, "measures": "警戒带"}, token=self.admin)
        self.report("c1", {"zones": [{"zone_code": "C1"}]}, token=self.admin)
        self.report("c2", {"zones": [{"zone_code": "C2"}], "all_zones_cleared": True,
                          "chemical_batches_used": ["LOT-2026-09"]}, token=self.admin)
        self.report("ri", {"quadrat_ids": ["Q1"], "residual_count": 0,
                           "inspector": "zhang", "passed": True}, token=self.admin)
        self.report("d", {"manifest_id": "MF-1", "destination": "焚烧中心",
                          "carrier": "净原", "mass_kg": 10}, token=self.admin)
        with self.assertRaises(InvalidState):
            self.report("rv", {"finding": "自批", "approved": True}, token=self.admin)
        # 未参与执行的 quality 可以独立复核结案。
        self.report("rv2", {"finding": "独立复核通过", "approved": True}, token=self.qa)
        self.assertEqual(self.view()["status"], "closed")


class IdempotencyTests(LedgerTestBase):
    def test_same_report_replays_without_double_advance(self) -> None:
        self.open_ledger()
        payload = {"signed": True, "signer": "crew-a", "signed_recipients": ["crew-a"]}
        first = self.report("r-1", payload)
        second = self.report("r-1", payload)
        self.assertEqual(first, second)
        progress_rows = self.s.db.execute(
            "SELECT count(*) FROM treatment_step_progress"
        ).fetchone()[0]
        self.assertEqual(progress_rows, 1)
        # 门控仍然只推进了一次，下一步是隔离。
        self.assertEqual(self.view()["next_step"]["step_key"], "isolation")

    def test_same_report_id_with_different_payload_conflicts(self) -> None:
        self.open_ledger()
        self.report("r-1", {"signed": True, "signer": "crew-a", "signed_recipients": ["crew-a"]})
        with self.assertRaises(Conflict):
            self.report("r-1", {"signed": True, "signer": "crew-b",
                                "signed_recipients": ["crew-b"]})

    def test_replay_after_revision_returns_original_result(self) -> None:
        self.open_ledger()
        self.notice()
        self.s.ledger.revise_plan(self.admin, self.lid, change_kind="scope_changed",
                                  reason="扩散", area={**area(), "area_m2": 2000})
        # 旧 report_id 在新版本下重放：返回首次结果，不会把旧签收当作新版本再推进。
        replayed = self.notice()
        self.assertEqual(replayed["plan_version"], 1)
        view = self.view()
        self.assertEqual(view["next_step"]["step_key"], "notice")


class ClockAndQueryTests(LedgerTestBase):
    def test_overdue_reflects_injected_clock(self) -> None:
        self.open_ledger(deadline_hours={"notice": 6})
        self.clock.advance(hours=7)
        view = self.view()
        self.assertEqual(view["next_step"]["step_key"], "notice")
        self.assertEqual(view["next_step"]["deadline_at"], "2026-09-29T14:00:00Z")
        self.assertTrue(view["next_step"]["overdue"])

    def test_chained_deadlines_anchor_to_previous_completion(self) -> None:
        self.open_ledger()
        self.notice()
        self.isolate()
        self.clock.advance(hours=10)  # 清除在 18:00 完成
        self.report("r-c1", {"zones": [{"zone_code": "C1", "cleared_at": "2026-09-29T17:00:00Z"}]})
        self.report("r-c2", {"zones": [{"zone_code": "C2", "cleared_at": "2026-09-29T18:00:00Z"}],
                            "all_zones_cleared": True,
                            "chemical_batches_used": ["LOT-2026-09"]})
        view = self.view()
        # 复查窗口 168h，自末次清除（18:00）起算，而非方案生效时刻。
        self.assertEqual(view["next_step"]["deadline_at"], "2026-10-06T18:00:00Z")

    def test_changes_reference_plan_version(self) -> None:
        self.open_ledger()
        self.notice()
        view = self.view()
        notice_event = next(c for c in view["changes"] if c["event_type"] == "notice.signed")
        self.assertEqual(notice_event["plan_version"], 1)
        self.assertIn("signer", notice_event["payload"])

    def test_permissions(self) -> None:
        # operator 不能生成方案；viewer 不能执行。
        with self.assertRaises(PermissionError):
            self.s.ledger.create_ledger(
                self.op, alert_id=self.alert_id, zone_record_id="Z1", area=area(),
                clearing_zones=ZONES, notice_recipients=["crew-a"],
                chemical_batches=["LOT-1"], reason="x",
            )


if __name__ == "__main__":
    unittest.main()
