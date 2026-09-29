"""离线命令行验收入口。"""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from .clock import FrozenClock
from .models import MonitoringRecord,ZoneRecord
from .service import BiosafetyService
def run():
    clock=FrozenClock(datetime(2026,9,25,8,0,tzinfo=timezone.utc))
    s=BiosafetyService(clock=clock); s.bootstrap()
    t=s.auth.login("admin","biosafety-admin"); o=s.auth.login("operator","biosafety-operator"); q=s.auth.login("reviewer","biosafety-reviewer")
    s.register_zone_record(t,ZoneRecord("CASE-DEMO","north","water",680,5))
    r=s.ingest_monitoring_record(t,MonitoringRecord("RD-DEMO","CASE-DEMO","sensor_source-01",160,230,88,"2026-09-24T10:00:00+00:00"))
    report=s.risk_report(t,"CASE-DEMO")
    order=s.create_treatment_ticket(t,"CASE-DEMO",r["alert_id"],"crew-north",1)
    ticket=order["treatment_ticket_id"]
    s.add_preservation_resource(t,"PUMP-01","mobile-cold-box","north",2)
    allocation=s.allocate(t,"PUMP-01",ticket,1)
    # 1) 以当时有效的风险记录与影响范围生成 v1 方案（不可覆盖）
    scope={"species":"加拿大一枝黄花","zone_codes":["edge-A","edge-B"],"area_m2":1200.0}
    v1=s.ledgers.create_plan(t,ticket,scope)
    lid=v1["ledger_id"]
    # 2) 通知签收、现场隔离、分区清除（药剂批次）、废弃物联单
    s.ledgers.ack_notification(o,lid,"notif-1",[{"recipient":"crew-north","signed_at":"2026-09-25T09:00:00Z"}])
    clock.advance(hours=1)
    s.ledgers.report_isolation(o,lid,"iso-1")
    s.ledgers.report_clearing(o,lid,"clear-A","edge-A",[{"chemical_batch_no":"CHEM-2026-001","agent_name":"草甘膦","dosage":"300倍液"}])
    s.ledgers.report_clearing(o,lid,"clear-B","edge-B",[{"chemical_batch_no":"CHEM-2026-002","agent_name":"草甘膦","dosage":"300倍液"}])
    s.ledgers.report_waste(o,lid,"waste-A","edge-A","MAN-001","市政固废中心","合规填埋场F-2",12.5)
    s.ledgers.report_waste(o,lid,"waste-B","edge-B","MAN-002","市政固废中心","合规填埋场F-2",8.0)
    # 3) 复查：edge-A 不通过 → 同方案内返工；外协重复键重放不推进
    s.ledgers.submit_review(o,lid,"review-1",[{"quadrat_code":"edge-A","result":"fail","residual_count":3,"reviewer":"patrol-zhang"},{"quadrat_code":"edge-B","result":"pass","residual_count":0,"reviewer":"patrol-zhang"}])
    replay=s.ledgers.report_isolation(o,lid,"iso-1")
    s.ledgers.report_clearing(o,lid,"clear-A-2","edge-A",[{"chemical_batch_no":"CHEM-2026-003","agent_name":"草甘膦","dosage":"400倍液"}])
    s.ledgers.report_waste(o,lid,"waste-A-2","edge-A","MAN-003","市政固废中心","焚烧厂B-1",5.0)
    s.ledgers.submit_review(o,lid,"review-2",[{"quadrat_code":"edge-A","result":"pass","residual_count":0,"reviewer":"patrol-li"}])
    # 4) 独立复核结案（复核人未参与现场执行）
    s.ledgers.independent_closeout(q,lid,"approved",note="独立复核通过，台账关闭")
    final=s.ledgers.ledger(q,lid)
    overview=s.ledgers.ticket_overview(q,ticket)
    return {"status":"ok","zone_record":"CASE-DEMO","severity":r["risk"]["severity"],"probability":report["violation_probability"],"allocation":allocation["plan_id"],"plan_version":final["current_plan_version"],"ledger_state":final["state"],"replay_duplicate":replay["duplicate"],"event_count":len(final["timeline"]),"event_chain_ok":final["event_chain_ok"],"missing_steps":final["missing_steps"],"next_deadline":final["next_deadline"],"chemical_batches":len(final["evidence"]["chemical_batches"]),"waste_manifests":len(final["evidence"]["waste_manifests"]),"versions":len(overview["versions"])}
def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--workspace",default="."); parser.parse_args(); print(json.dumps(run(),ensure_ascii=False))
if __name__=="__main__":main()
