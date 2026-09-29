"""处置台账 HTTP 接口的集成测试。"""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer

from biosafety_ops.api import Handler
from biosafety_ops.clock import FrozenClock
from biosafety_ops.models import MonitoringRecord, ZoneRecord
from biosafety_ops.service import BiosafetyService


class ApiLedgerTests(unittest.TestCase):
    def setUp(self):
        clock = FrozenClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc))
        Handler.service = BiosafetyService(":memory:", clock)
        Handler.service.bootstrap()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.admin = self.login("admin", "biosafety-admin")
        self.operator = self.login("operator", "biosafety-operator")
        self.reviewer = self.login("reviewer", "biosafety-reviewer")
        self._seed()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, token=None, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def login(self, user_id, password):
        _, body = self.request("POST", "/login", payload={"user_id": user_id, "password": password})
        return body["token"]

    def _seed(self):
        self.request("POST", "/zone_records", self.admin,
                     {"zone_record_id": "Z1", "collection_zone": "north", "biosafety_type": "water",
                      "length_m": 680, "criticality": 5})
        _, ingested = self.request("POST", "/zone_records/Z1/monitoring_records", self.admin,
                                   {"monitoring_record_id": "RD1", "sensor_source_id": "s1",
                                    "speed_kmh": 160, "traffic_flow_vph": 230, "impact_index": 88,
                                    "observed_at": "2026-09-25T06:00:00+00:00"})
        self.alert_id = ingested["alert_id"]
        _, ticket = self.request("POST", "/zone_records/Z1/work-orders", self.admin,
                                 {"alert_id": self.alert_id, "assignee": "crew", "priority": 1})
        self.ticket_id = ticket["treatment_ticket_id"]

    def _create_plan(self, token=None):
        status, body = self.request("POST", f"/work-orders/{self.ticket_id}/plans", token or self.admin,
                                    {"impact_scope": {"species": "加拿大一枝黄花", "zone_codes": ["A"],
                                                      "area_m2": 100}})
        self.assertEqual(status, 201)
        return body

    def _finish(self, ledger_id):
        self.request("POST", f"/ledgers/{ledger_id}/ack-notification", self.operator,
                     {"idempotency_key": "n", "recipients": [{"recipient": "crew"}]})
        self.request("POST", f"/ledgers/{ledger_id}/isolation", self.operator,
                     {"idempotency_key": "i"})
        self.request("POST", f"/ledgers/{ledger_id}/clearing", self.operator,
                     {"idempotency_key": "c", "zone_code": "A",
                      "chemical_batches": [{"chemical_batch_no": "B1", "agent_name": "草甘膦",
                                            "dosage": "d"}]})
        self.request("POST", f"/ledgers/{ledger_id}/waste", self.operator,
                     {"idempotency_key": "w", "zone_code": "A", "manifest_id": "M1",
                      "carrier": "固废中心", "destination": "填埋场", "weight_kg": 3.0})
        self.request("POST", f"/ledgers/{ledger_id}/review", self.operator,
                     {"idempotency_key": "r",
                      "quadrats": [{"quadrat_code": "A", "result": "pass", "reviewer": "patrol"}]})

    def test_full_flow_overview_and_idempotent_replay(self):
        plan = self._create_plan()
        ledger_id = plan["ledger_id"]
        status, overview = self.request("GET", f"/work-orders/{self.ticket_id}/overview", self.reviewer)
        self.assertEqual(status, 200)
        self.assertEqual(overview["current_plan_version"], 1)
        self.assertTrue(overview["missing_steps"])
        self.assertEqual(overview["next_deadline"]["step_code"], "notification")

        self._finish(ledger_id)
        # 重复进度上报：同键重放返回 duplicate=true 且不产生第二条事件
        status, replay = self.request("POST", f"/ledgers/{ledger_id}/isolation", self.operator,
                                      {"idempotency_key": "i"})
        self.assertEqual(status, 200)
        self.assertTrue(replay["duplicate"])
        status, view = self.request("GET", f"/ledgers/{ledger_id}", self.reviewer)
        self.assertEqual(sum(1 for e in view["timeline"] if e["event_type"] == "isolation.reported"), 1)

        status, closed = self.request("POST", f"/ledgers/{ledger_id}/closeout", self.reviewer,
                                      {"result": "approved", "note": "独立复核通过"})
        self.assertEqual(status, 200)
        self.assertEqual(closed["state"], "closed")
        status, view = self.request("GET", f"/ledgers/{ledger_id}", self.reviewer)
        self.assertEqual(view["missing_steps"], [])
        self.assertTrue(view["event_chain_ok"])

    def test_review_fail_then_rework_via_api(self):
        ledger_id = self._create_plan()["ledger_id"]
        self.request("POST", f"/ledgers/{ledger_id}/ack-notification", self.operator,
                     {"idempotency_key": "n", "recipients": [{"recipient": "crew"}]})
        self.request("POST", f"/ledgers/{ledger_id}/isolation", self.operator,
                     {"idempotency_key": "i"})
        self.request("POST", f"/ledgers/{ledger_id}/clearing", self.operator,
                     {"idempotency_key": "c", "zone_code": "A",
                      "chemical_batches": [{"chemical_batch_no": "B1", "agent_name": "a",
                                            "dosage": "d"}]})
        self.request("POST", f"/ledgers/{ledger_id}/waste", self.operator,
                     {"idempotency_key": "w", "zone_code": "A", "manifest_id": "M1",
                      "carrier": "c", "destination": "d", "weight_kg": 1})
        status, failed = self.request("POST", f"/ledgers/{ledger_id}/review", self.operator,
                                      {"idempotency_key": "r",
                                       "quadrats": [{"quadrat_code": "A", "result": "fail",
                                                     "residual_count": 2, "reviewer": "patrol"}]})
        self.assertEqual(status, 200)
        self.assertEqual(failed["rework_zones"], ["A"])
        status, view = self.request("GET", f"/ledgers/{ledger_id}", self.operator)
        self.assertEqual(view["state"], "rework")
        self.assertIn(("clearing", "A", 2),
                      [(s["step_code"], s["zone_code"], s["attempt"]) for s in view["missing_steps"]])

    def test_version_endpoint_and_permissions(self):
        self._create_plan()
        status, versions = self.request("GET", f"/work-orders/{self.ticket_id}/plans", self.reviewer)
        self.assertEqual(status, 200)
        self.assertEqual(len(versions["versions"]), 1)
        # operator 无权生成方案
        status, err = self.request("POST", f"/work-orders/{self.ticket_id}/plans", self.operator,
                                   {"impact_scope": {"species": "x", "zone_codes": ["A"], "area_m2": 1}})
        self.assertEqual(status, 403)
        # 未知路由
        status, _ = self.request("GET", "/nope", self.reviewer)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
