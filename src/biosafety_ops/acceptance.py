"""离线命令行验收入口。

演示加拿大一枝黄花处置执行台账：方案版本、通知签收、现场隔离、分区清除、
复查不通过返工、废弃物去向、范围变化新版本与独立复核结案。
"""
from __future__ import annotations
import argparse,json
from datetime import datetime, timezone
from .clock import FrozenClock
from .models import MonitoringRecord,ZoneRecord
from .service import BiosafetyService

def run():
    clock=FrozenClock(datetime(2026,9,29,8,0,tzinfo=timezone.utc))
    s=BiosafetyService(clock=clock); s.bootstrap()
    admin=s.auth.login("admin","biosafety-admin"); op=s.auth.login("operator","biosafety-operator"); qa=s.auth.login("quality","biosafety-quality")
    s.register_zone_record(admin,ZoneRecord("CASE-DEMO","north-lime-margin","water",680,5))
    r=s.ingest_monitoring_record(admin,MonitoringRecord("RD-DEMO","CASE-DEMO","sensor_source-01",160,230,88,"2026-09-24T10:00:00+00:00"))
    report=s.risk_report(admin,"CASE-DEMO")
    order=s.create_treatment_ticket(admin,"CASE-DEMO",r["alert_id"],"crew-north",1)
    s.add_preservation_resource(admin,"PUMP-01","mobile-cold-box","north",2)
    allocation=s.allocate(admin,"PUMP-01",order["treatment_ticket_id"],1)

    opened=s.ledger.create_ledger(admin,alert_id=r["alert_id"],zone_record_id="CASE-DEMO",
        area={"area_id":"AREA-N1","location":"北坡林缘 3 号界桩","area_m2":1200,"polygon":["p1","p2"],"observed_at":"2026-09-24T08:00:00Z"},
        clearing_zones=[{"zone_code":"Z-A","method":"人工刈割","area_m2":600},{"zone_code":"Z-B","method":"药剂喷施","area_m2":600}],
        notice_recipients=["crew-north","station-watch"],chemical_batches=["LOT-2026-0901","LOT-2026-0902"],
        reason="巡护队确认林缘成片加拿大一枝黄花")
    lid=opened["ledger_id"]
    def prog(rid,token=op,**payload): return s.ledger.report_progress(token,lid,{"report_id":rid,"payload":payload})
    prog("n-1",signer="crew-north",signed_recipients=["crew-north","station-watch"],signed=True)
    prog("i-1",sealed=True,measures="警戒带+警示牌+定点巡查")
    prog("c-1",zones=[{"zone_code":"Z-A","cleared_at":"2026-09-29T11:00:00Z"}])
    prog("c-2",zones=[{"zone_code":"Z-B","cleared_at":"2026-09-29T12:00:00Z"}],all_zones_cleared=True,chemical_batches_used=["LOT-2026-0901","LOT-2026-0902"])
    # 首次复查发现残株 -> 返工 -> 复查通过
    failed=prog("q-1",quadrat_ids=["Q1","Q2"],residual_count=3,inspector="zhang",passed=False)
    clock.advance(hours=6)
    prog("rw-1",zones=["Q2 周边 200m2 复喷"],chemical_batches_used=["LOT-2026-0902"])
    prog("q-2",quadrat_ids=["Q1","Q2","Q3"],residual_count=0,inspector="zhang",passed=True)
    prog("d-1",manifest_id="MF-2026-001",destination="市危废焚烧中心",carrier="净原环保",mass_kg=480)
    prog("rv-1",token=qa,finding="台账、样方照片与联单齐全，独立复核通过",approved=True)
    view=s.ledger.ledger_view(qa,lid)
    return {"status":"ok","zone_record":"CASE-DEMO","severity":r["risk"]["severity"],"probability":report["violation_probability"],"allocation":allocation["plan_id"],
            "ledger_id":lid,"ledger_status":view["status"],"current_version":view["current_version"],
            "rework_rounds":failed["rework_round"],"steps":len(view["steps"]),"changes":len(view["changes"]),
            "missing_steps":view["missing_steps"],"next_deadline_at":view["next_deadline_at"]}
def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--workspace",default="."); parser.parse_args(); print(json.dumps(run(),ensure_ascii=False))
if __name__=="__main__":main()
