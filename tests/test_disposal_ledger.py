"""处置方案版本化与执行台账的领域规则测试。"""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone

from biosafety_ops.clock import FrozenClock
from biosafety_ops.errors import Conflict, InvalidState, NotFound, ValidationFailed
from biosafety_ops.models import MonitoringRecord, ZoneRecord
from biosafety_ops.service import BiosafetyService

T0 = datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)
SCOPE = {"species": "加拿大一枝黄花", "zone_codes": ["A", "B"], "area_m2": 1200.0}


def build_service():
    clock = FrozenClock(T0)
    svc = BiosafetyService(":memory:", clock)
    svc.bootstrap()
    admin = svc.auth.login("admin", "biosafety-admin")
    operator = svc.auth.login("operator", "biosafety-operator")
    reviewer = svc.auth.login("reviewer", "biosafety-reviewer")
    svc.register_zone_record(admin, ZoneRecord("Z1", "north", "water", 680, 5))
    ingested = svc.ingest_monitoring_record(
        admin,
        MonitoringRecord("RD1", "Z1", "sensor-01", 160, 230, 88, "2026-09-25T06:00:00+00:00"),
    )
    ticket = svc.create_treatment_ticket(admin, "Z1", ingested["alert_id"], "crew-north", 1)
    return svc, clock, admin, operator, reviewer, ticket["treatment_ticket_id"], ingested["alert_id"]


def finish_attempt_1(svc, tokens, ledger_id, zones=("A", "B"), *, review_results=None):
    admin, operator, _ = tokens
    svc.ledgers.ack_notification(operator, ledger_id, "n", [{"recipient": "crew-north"}])
    svc.ledgers.report_isolation(operator, ledger_id, "i")
    for idx, zone in enumerate(zones):
        svc.ledgers.report_clearing(
            operator, ledger_id, f"c-{zone}", zone,
            [{"chemical_batch_no": f"CHEM-{zone}", "agent_name": "草甘膦", "dosage": "300倍液"}],
        )
        svc.ledgers.report_waste(
            operator, ledger_id, f"w-{zone}", zone, f"MANIFEST-{zone}", "固废中心", "合规填埋场", 5.0 + idx
        )
    if review_results is None:
        review_results = {z: "pass" for z in zones}
    quadrats = [{"quadrat_code": z, "result": result, "reviewer": "patrol-zhang"}
                for z, result in review_results.items()]
    return svc.ledgers.submit_review(operator, ledger_id, "r", quadrats)


class PlanVersionTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock, self.admin, self.operator, self.reviewer, self.ticket, self.alert = build_service()

    def test_initial_plan_snapshots_risk_and_scope(self):
        view = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        self.assertEqual(view["current_plan_version"], 1)
        self.assertEqual(view["plan"]["change_kind"], "initial")
        self.assertEqual(view["plan"]["impact_scope"]["zone_codes"], ["A", "B"])
        self.assertEqual(view["plan"]["risk_snapshot"]["alert_id"], self.alert)
        self.assertEqual(len(view["plan"]["content_sha256"]), 64)
        codes = [(s["step_code"], s["zone_code"]) for s in view["steps"]]
        self.assertEqual(codes[0], ("notification", None))
        self.assertIn(("clearing", "A"), codes)
        self.assertIn(("review", None), codes)

    def test_plan_rows_are_immutable(self):
        view = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        plan_id = view["plan"]["plan_id"]
        for statement in ("UPDATE disposal_plans SET reason='x' WHERE plan_id=?",
                          "DELETE FROM disposal_plans WHERE plan_id=?"):
            with self.assertRaises(sqlite3.IntegrityError):
                try:
                    self.svc.db.execute(statement, (plan_id,))
                finally:
                    self.svc.db.rollback()

    def test_scope_change_creates_version_and_carries_done_zones(self):
        v1 = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        self.svc.ledgers.ack_notification(self.operator, v1["ledger_id"], "n", [{"recipient": "c"}])
        self.svc.ledgers.report_isolation(self.operator, v1["ledger_id"], "i")
        self.svc.ledgers.report_clearing(
            self.operator, v1["ledger_id"], "ca", "A",
            [{"chemical_batch_no": "B1", "agent_name": "草甘膦", "dosage": "x"}],
        )
        v2 = self.svc.ledgers.create_plan(
            self.admin, self.ticket,
            {"species": "加拿大一枝黄花", "zone_codes": ["A", "C"], "area_m2": 400.0},
            change_kind="scope_changed", reason="林缘扩界",
        )
        self.assertEqual(v2["current_plan_version"], 2)
        self.assertEqual(v2["plan"]["supersedes_plan_id"], v1["plan"]["plan_id"])
        # 旧台账封存，旧任务仍可查且带版本来历
        self.assertEqual(self.svc.ledgers.ledger(self.admin, v1["ledger_id"])["state"], "superseded")
        carried = [s for s in v2["steps"] if s["carried_from_step_id"]]
        self.assertEqual([(s["step_code"], s["zone_code"], s["status"]) for s in carried],
                         [("clearing", "A", "done")])
        pending_zones = {s["zone_code"] for s in v2["missing_steps"] if s["step_code"] == "clearing"}
        self.assertEqual(pending_zones, {"C"})

    def test_identical_content_cannot_make_new_version(self):
        self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        with self.assertRaises(Conflict):
            self.svc.ledgers.create_plan(self.admin, self.ticket, dict(SCOPE))

    def test_withdrawal_requires_withdrawn_alert_and_short_circuits_steps(self):
        self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        with self.assertRaises(InvalidState):
            self.svc.ledgers.create_plan(
                self.admin, self.ticket, None, change_kind="risk_withdrawn", reason="误判")
        result = self.svc.ledgers.withdraw_risk(self.admin, self.alert, "复核为本地种")
        self.assertEqual(result["status"], "withdrawn")
        with self.assertRaises(Conflict):
            self.svc.ledgers.withdraw_risk(self.admin, self.alert, "再次撤回")
        v2 = self.svc.ledgers.create_plan(
            self.admin, self.ticket, None, change_kind="risk_withdrawn", reason="误判")
        self.assertEqual([s["step_code"] for s in v2["steps"]], ["notification", "closeout"])

    def test_operator_cannot_create_plan(self):
        with self.assertRaises(PermissionError):
            self.svc.ledgers.create_plan(self.operator, self.ticket, SCOPE)


class LedgerExecutionTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock, self.admin, self.operator, self.reviewer, self.ticket, self.alert = build_service()
        self.ledger = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        self.ledger_id = self.ledger["ledger_id"]

    def test_notification_deadline_uses_clock(self):
        notif = self.ledger["missing_steps"][0]
        self.assertEqual(notif["deadline_at"], "2026-09-26T08:00:00Z")
        self.assertTrue(notif["actionable"])

    def test_clearing_requires_isolation_and_valid_zone(self):
        self.svc.ledgers.ack_notification(self.operator, self.ledger_id, "n", [{"recipient": "c"}])
        with self.assertRaises(InvalidState):
            self.svc.ledgers.report_clearing(
                self.operator, self.ledger_id, "c", "A",
                [{"chemical_batch_no": "B", "agent_name": "a", "dosage": "d"}],
            )
        self.svc.ledgers.report_isolation(self.operator, self.ledger_id, "i")
        with self.assertRaises(ValidationFailed):
            self.svc.ledgers.report_clearing(
                self.operator, self.ledger_id, "c", "Z9",
                [{"chemical_batch_no": "B", "agent_name": "a", "dosage": "d"}],
            )
        with self.assertRaises(ValidationFailed):
            self.svc.ledgers.report_clearing(self.operator, self.ledger_id, "c", "A", [])

    def test_idempotent_replay_does_not_advance_twice(self):
        first = self.svc.ledgers.ack_notification(
            self.operator, self.ledger_id, "k1", [{"recipient": "crew-north"}])
        self.assertFalse(first["duplicate"])
        replay = self.svc.ledgers.ack_notification(
            self.operator, self.ledger_id, "k1", [{"recipient": "crew-north"}])
        self.assertTrue(replay["duplicate"])
        self.assertEqual(replay["step_id"], first["step_id"])
        events = self.svc.ledgers.ledger(self.operator, self.ledger_id)["timeline"]
        self.assertEqual([e["event_type"] for e in events].count("notification.acked"), 1)
        with self.assertRaises(Conflict):
            self.svc.ledgers.ack_notification(
                self.operator, self.ledger_id, "k1", [{"recipient": "another-crew"}])

    def test_waste_requires_matching_clearing_attempt(self):
        self.svc.ledgers.ack_notification(self.operator, self.ledger_id, "n", [{"recipient": "c"}])
        self.svc.ledgers.report_isolation(self.operator, self.ledger_id, "i")
        with self.assertRaises(InvalidState):
            self.svc.ledgers.report_waste(
                self.operator, self.ledger_id, "w", "A", "M1", "car", "dest", 3.0)
        self.svc.ledgers.report_clearing(
            self.operator, self.ledger_id, "c", "A",
            [{"chemical_batch_no": "B", "agent_name": "a", "dosage": "d"}])
        result = self.svc.ledgers.report_waste(
            self.operator, self.ledger_id, "w", "A", "M1", "car", "dest", 3.0)
        self.assertFalse(result["duplicate"])
        # 同一分区已移交，重复上报不会推进步骤
        with self.assertRaises(InvalidState):
            self.svc.ledgers.report_waste(
                self.operator, self.ledger_id, "w2", "A", "M2", "car", "dest", 3.0)
        # 联单号全局唯一：在仍开放的 B 分区复用 M1 被拒绝
        self.svc.ledgers.report_clearing(
            self.operator, self.ledger_id, "c2", "B",
            [{"chemical_batch_no": "B2", "agent_name": "a", "dosage": "d"}])
        with self.assertRaises(Conflict):
            self.svc.ledgers.report_waste(
                self.operator, self.ledger_id, "w3", "B", "M1", "car", "dest", 3.0)

    def test_review_must_cover_all_zones_of_attempt(self):
        finish_attempt_1(self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id,
                         review_results={"A": "pass", "B": "pass"})
        # 再建一张台账验证覆盖性校验
        ledger2 = self.svc.ledgers.create_plan(
            self.admin, self.ticket,
            {"species": "加拿大一枝黄花", "zone_codes": ["X"], "area_m2": 10.0},
            change_kind="scope_changed")
        self.svc.ledgers.ack_notification(self.operator, ledger2["ledger_id"], "n2", [{"recipient": "c"}])
        self.svc.ledgers.report_isolation(self.operator, ledger2["ledger_id"], "i2")
        self.svc.ledgers.report_clearing(
            self.operator, ledger2["ledger_id"], "cx", "X",
            [{"chemical_batch_no": "B", "agent_name": "a", "dosage": "d"}])
        self.svc.ledgers.report_waste(
            self.operator, ledger2["ledger_id"], "wx", "X", "M", "c", "d", 1.0)
        with self.assertRaises(ValidationFailed):
            self.svc.ledgers.submit_review(
                self.operator, ledger2["ledger_id"], "r",
                [{"quadrat_code": "X", "result": "pass", "reviewer": "p"},
                 {"quadrat_code": "Y", "result": "fail", "reviewer": "p"}])


class ReworkAndCloseoutTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock, self.admin, self.operator, self.reviewer, self.ticket, self.alert = build_service()
        self.ledger = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        self.ledger_id = self.ledger["ledger_id"]

    def test_failed_review_opens_rework_only_for_failed_zones(self):
        result = finish_attempt_1(
            self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id,
            review_results={"A": "fail", "B": "pass"})
        self.assertFalse(result["passed"])
        self.assertEqual(result["rework_zones"], ["A"])
        view = self.svc.ledgers.ledger(self.operator, self.ledger_id)
        self.assertEqual(view["state"], "rework")
        rework = [(s["step_code"], s["zone_code"], s["attempt"]) for s in view["missing_steps"]]
        self.assertIn(("clearing", "A", 2), rework)
        self.assertIn(("waste_manifest", "A", 2), rework)
        self.assertIn(("review", None, 2), rework)
        self.assertNotIn(("clearing", "B", 2), rework)
        # B 在第 2 拨次不能重复清除
        with self.assertRaises(InvalidState):
            self.svc.ledgers.report_clearing(
                self.operator, self.ledger_id, "cb", "B",
                [{"chemical_batch_no": "B", "agent_name": "a", "dosage": "d"}])

    def test_rework_then_pass_allows_closeout(self):
        finish_attempt_1(
            self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id,
            review_results={"A": "fail", "B": "pass"})
        self.svc.ledgers.report_clearing(
            self.operator, self.ledger_id, "c2", "A",
            [{"chemical_batch_no": "B2", "agent_name": "草甘膦", "dosage": "d"}])
        self.svc.ledgers.report_waste(
            self.operator, self.ledger_id, "w2", "A", "M2", "c", "d", 2.0)
        passed = self.svc.ledgers.submit_review(
            self.operator, self.ledger_id, "r2",
            [{"quadrat_code": "A", "result": "pass", "reviewer": "patrol-li"}])
        self.assertTrue(passed["passed"])
        view = self.svc.ledgers.ledger(self.reviewer, self.ledger_id)
        self.assertEqual(view["state"], "active")
        closed = self.svc.ledgers.independent_closeout(self.reviewer, self.ledger_id, "approved", note="结案")
        self.assertEqual(closed["state"], "closed")
        final = self.svc.ledgers.ledger(self.reviewer, self.ledger_id)
        self.assertEqual(final["missing_steps"], [])
        self.assertIsNone(final["next_deadline"])
        self.assertTrue(final["event_chain_ok"])
        self.assertEqual(len(final["evidence"]["chemical_batches"]), 3)
        self.assertEqual(len(final["evidence"]["waste_manifests"]), 3)

    def test_closeout_requires_approve_role_and_non_executor(self):
        finish_attempt_1(self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id)
        with self.assertRaises(PermissionError):
            self.svc.ledgers.independent_closeout(self.operator, self.ledger_id, "approved")
        # admin 具备 approve 权限但没有参与现场事件，可以结案；结案后再次结案被拒绝
        closed = self.svc.ledgers.independent_closeout(self.reviewer, self.ledger_id, "approved")
        self.assertEqual(closed["state"], "closed")
        with self.assertRaises(InvalidState):
            self.svc.ledgers.independent_closeout(
                self.reviewer, self.ledger_id, "approved", idempotency_key="closeout-again")

    def test_executor_cannot_close_out_even_with_approve_permission(self):
        finish_attempt_1(self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id)
        # admin 具备 approve 权限，但只要在执行事件中出现即被回避——这里 admin 没执行现场动作，
        # 用 operator 上报全部现场动作；另造一名参与过执行的审批人
        self.svc.auth.create_user("fieldboss", "biosafety-fieldboss", "quality")
        boss = self.svc.auth.login("fieldboss", "biosafety-fieldboss")
        self.svc.ledgers.independent_closeout(self.reviewer, self.ledger_id, "rejected", note="存疑")
        self.svc.ledgers.report_clearing(
            self.operator, self.ledger_id, "rc", "A",
            [{"chemical_batch_no": "R1", "agent_name": "a", "dosage": "d"}])
        self.svc.ledgers.report_clearing(
            self.operator, self.ledger_id, "rc2", "B",
            [{"chemical_batch_no": "R2", "agent_name": "a", "dosage": "d"}])
        self.svc.ledgers.report_waste(self.operator, self.ledger_id, "rw", "A", "RA", "c", "d", 1.0)
        self.svc.ledgers.report_waste(self.operator, self.ledger_id, "rw2", "B", "RB", "c", "d", 1.0)
        # boss 参与执行一次复查
        self.svc.ledgers.submit_review(
            boss, self.ledger_id, "rr",
            [{"quadrat_code": "A", "result": "pass", "reviewer": "fieldboss"},
             {"quadrat_code": "B", "result": "pass", "reviewer": "fieldboss"}])
        with self.assertRaises(InvalidState):
            self.svc.ledgers.independent_closeout(boss, self.ledger_id, "approved")

    def test_closeout_rejection_reworks_all_zones(self):
        finish_attempt_1(self.svc, (self.admin, self.operator, self.reviewer), self.ledger_id)
        rejected = self.svc.ledgers.independent_closeout(
            self.reviewer, self.ledger_id, "rejected", note="药剂批次异常")
        self.assertEqual(rejected["state"], "rework")
        view = self.svc.ledgers.ledger(self.reviewer, self.ledger_id)
        attempts = {(s["step_code"], s["zone_code"]): s["attempt"] for s in view["missing_steps"]}
        self.assertEqual(attempts[("clearing", "A")], 2)
        self.assertEqual(attempts[("clearing", "B")], 2)
        self.assertEqual(attempts[("closeout", None)], 2)


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock, self.admin, self.operator, self.reviewer, self.ticket, self.alert = build_service()
        self.ledger = self.svc.ledgers.create_plan(self.admin, self.ticket, SCOPE)
        self.ledger_id = self.ledger["ledger_id"]

    def test_missing_steps_show_waiting_and_next_deadline(self):
        view = self.svc.ledgers.ledger(self.operator, self.ledger_id)
        by_code = {(s["step_code"], s["zone_code"]): s for s in view["missing_steps"]}
        self.assertTrue(by_code[("notification", None)]["actionable"])
        self.assertFalse(by_code[("isolation", None)]["actionable"])
        self.assertEqual(by_code[("isolation", None)]["waiting_on"], ["notification"])
        self.assertFalse(by_code[("clearing", "A")]["actionable"])
        self.assertEqual(by_code[("review", None)]["waiting_on"],
                         ["clearing/A", "clearing/B", "waste_manifest/A", "waste_manifest/B"])
        self.assertEqual(view["next_deadline"]["step_code"], "notification")
        self.assertFalse(view["next_deadline"]["overdue"])
        self.clock.advance(hours=25)
        view = self.svc.ledgers.ledger(self.operator, self.ledger_id)
        self.assertTrue(view["next_deadline"]["overdue"])

    def test_timeline_carries_plan_version_and_chain_validates(self):
        self.svc.ledgers.ack_notification(self.operator, self.ledger_id, "n", [{"recipient": "c"}])
        v2 = self.svc.ledgers.create_plan(
            self.admin, self.ticket,
            {"species": "加拿大一枝黄花", "zone_codes": ["A", "B", "C"], "area_m2": 2000.0},
            change_kind="scope_changed", reason="扩大")
        timeline = self.svc.ledgers.ledger(self.admin, v2["ledger_id"])["timeline"]
        self.assertEqual(timeline[0]["event_type"], "plan.created")
        self.assertEqual(timeline[0]["plan_version"], 2)
        # 旧台账的事件仍保留且哈希链独立可验
        old = self.svc.ledgers.ledger(self.admin, self.ledger_id)
        self.assertTrue(old["event_chain_ok"])
        self.assertEqual(old["timeline"][-1]["event_type"], "plan.superseded")

    def test_ticket_overview_and_lineage(self):
        self.svc.ledgers.create_plan(
            self.admin, self.ticket,
            {"species": "加拿大一枝黄花", "zone_codes": ["A"], "area_m2": 10.0},
            change_kind="scope_changed")
        overview = self.svc.ledgers.ticket_overview(self.reviewer, self.ticket)
        self.assertEqual(overview["current_plan_version"], 2)
        self.assertEqual(overview["current_change_kind"], "scope_changed")
        self.assertIn("next_deadline", overview)
        self.assertEqual([v["plan_version"] for v in overview["versions"]], [1, 2])
        with self.assertRaises(NotFound):
            self.svc.ledgers.ticket_overview(self.reviewer, "wo-missing")


if __name__ == "__main__":
    unittest.main()
