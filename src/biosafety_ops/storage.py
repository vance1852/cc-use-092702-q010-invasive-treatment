"""SQLite 结构、事务和审计事件辅助函数。"""
from __future__ import annotations
import json, sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,role TEXT NOT NULL,salt TEXT NOT NULL,password_hash TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id TEXT NOT NULL,expires_at TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS zone_records(zone_record_id TEXT PRIMARY KEY,collection_zone TEXT NOT NULL,biosafety_type TEXT NOT NULL,length_m REAL NOT NULL,criticality INTEGER NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS monitoring_records(monitoring_record_id TEXT PRIMARY KEY,zone_record_id TEXT NOT NULL REFERENCES zone_records(zone_record_id),sensor_source_id TEXT NOT NULL,speed_kmh REAL NOT NULL,traffic_flow_vph REAL NOT NULL,impact_index REAL NOT NULL,observed_at TEXT NOT NULL,UNIQUE(zone_record_id,sensor_source_id,observed_at));
CREATE TABLE IF NOT EXISTS alerts(alert_id TEXT PRIMARY KEY,zone_record_id TEXT NOT NULL REFERENCES zone_records(zone_record_id),fingerprint TEXT NOT NULL UNIQUE,severity TEXT NOT NULL,score REAL NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,resolved_at TEXT);
CREATE TABLE IF NOT EXISTS treatment_tickets(treatment_ticket_id TEXT PRIMARY KEY,zone_record_id TEXT NOT NULL,alert_id TEXT NOT NULL,assignee TEXT NOT NULL,status TEXT NOT NULL,priority INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS preservation_resources(preservation_resource_id TEXT PRIMARY KEY,kind TEXT NOT NULL,collection_zone TEXT NOT NULL,capacity INTEGER NOT NULL,available INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS allocations(plan_id TEXT PRIMARY KEY,preservation_resource_id TEXT NOT NULL,treatment_ticket_id TEXT NOT NULL,quantity INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(preservation_resource_id,treatment_ticket_id));
CREATE TABLE IF NOT EXISTS audit_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,action TEXT NOT NULL,actor TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL);

-- 处置方案：纯追加版本链，任何 UPDATE/DELETE 由下方触发器拒绝
CREATE TABLE IF NOT EXISTS disposal_plans(plan_id TEXT PRIMARY KEY,treatment_ticket_id TEXT NOT NULL REFERENCES treatment_tickets(treatment_ticket_id),plan_version INTEGER NOT NULL CHECK(plan_version>=1),change_kind TEXT NOT NULL CHECK(change_kind IN ('initial','scope_changed','risk_withdrawn')),supersedes_plan_id TEXT REFERENCES disposal_plans(plan_id),risk_snapshot_json TEXT NOT NULL,impact_scope_json TEXT NOT NULL,windows_json TEXT NOT NULL,scope_fingerprint TEXT NOT NULL,content_sha256 TEXT NOT NULL,reason TEXT NOT NULL DEFAULT '',created_by TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(treatment_ticket_id,plan_version));
CREATE INDEX IF NOT EXISTS idx_disposal_plans_ticket ON disposal_plans(treatment_ticket_id,plan_version);
CREATE TRIGGER IF NOT EXISTS disposal_plans_no_update BEFORE UPDATE ON disposal_plans
BEGIN SELECT RAISE(ABORT,'disposal_plans 是不可覆盖的方案版本，禁止 UPDATE');END;
CREATE TRIGGER IF NOT EXISTS disposal_plans_no_delete BEFORE DELETE ON disposal_plans
BEGIN SELECT RAISE(ABORT,'disposal_plans 是不可覆盖的方案版本，禁止 DELETE');END;

-- 每个方案版本对应一份执行台账；旧版本台账只封存不删除
CREATE TABLE IF NOT EXISTS disposal_ledgers(ledger_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL UNIQUE REFERENCES disposal_plans(plan_id),treatment_ticket_id TEXT NOT NULL,plan_version INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','rework','superseded','closed')),created_by TEXT NOT NULL,created_at TEXT NOT NULL,closed_at TEXT);

-- 步骤实例：通知签收/现场隔离/分区清除/废弃物联单/复查样方/独立结案，返工产生 attempt+1 的新步骤
CREATE TABLE IF NOT EXISTS disposal_steps(step_id TEXT PRIMARY KEY,ledger_id TEXT NOT NULL REFERENCES disposal_ledgers(ledger_id),plan_id TEXT NOT NULL REFERENCES disposal_plans(plan_id),plan_version INTEGER NOT NULL,step_code TEXT NOT NULL CHECK(step_code IN ('notification','isolation','clearing','waste_manifest','review','closeout')),step_seq INTEGER NOT NULL,attempt INTEGER NOT NULL DEFAULT 1,zone_code TEXT,detail_json TEXT NOT NULL DEFAULT '{}',status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','in_progress','done','failed','cancelled')),blocked_by_step_id TEXT REFERENCES disposal_steps(step_id),deadline_at TEXT,completed_at TEXT,carried_from_step_id TEXT,created_at TEXT NOT NULL,UNIQUE(ledger_id,step_seq));
CREATE INDEX IF NOT EXISTS idx_disposal_steps_ledger ON disposal_steps(ledger_id,step_code,attempt,zone_code);

-- 仅追加的哈希链事件流；每个事件都带方案版本
CREATE TABLE IF NOT EXISTS disposal_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,ledger_id TEXT NOT NULL REFERENCES disposal_ledgers(ledger_id),plan_id TEXT NOT NULL,plan_version INTEGER NOT NULL,step_id TEXT,event_type TEXT NOT NULL,payload_json TEXT NOT NULL,idempotency_key TEXT,actor TEXT NOT NULL,previous_hash TEXT NOT NULL,event_hash TEXT NOT NULL UNIQUE,created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_disposal_events_ledger ON disposal_events(ledger_id,event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_disposal_events_idem ON disposal_events(ledger_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TRIGGER IF NOT EXISTS disposal_events_no_update BEFORE UPDATE ON disposal_events
BEGIN SELECT RAISE(ABORT,'处置事件流仅追加，禁止 UPDATE');END;
CREATE TRIGGER IF NOT EXISTS disposal_events_no_delete BEFORE DELETE ON disposal_events
BEGIN SELECT RAISE(ABORT,'处置事件流仅追加，禁止 DELETE');END;

-- 外协上报幂等回执：重复键同内容返回首次结果，不同内容拒绝
CREATE TABLE IF NOT EXISTS disposal_idempotency(ledger_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,request_sha256 TEXT NOT NULL,response_json TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(ledger_id,idempotency_key));

CREATE TABLE IF NOT EXISTS disposal_receipts(receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,step_id TEXT NOT NULL REFERENCES disposal_steps(step_id),recipient TEXT NOT NULL,signed_at TEXT NOT NULL,evidence_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disposal_chemical_batches(batch_use_id INTEGER PRIMARY KEY AUTOINCREMENT,step_id TEXT NOT NULL REFERENCES disposal_steps(step_id),chemical_batch_no TEXT NOT NULL,agent_name TEXT NOT NULL,dosage TEXT NOT NULL,applied_at TEXT NOT NULL,evidence_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disposal_waste_manifests(manifest_id TEXT PRIMARY KEY,step_id TEXT NOT NULL REFERENCES disposal_steps(step_id),carrier TEXT NOT NULL,destination TEXT NOT NULL,weight_kg REAL NOT NULL CHECK(weight_kg>0),handed_over_at TEXT NOT NULL,evidence_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disposal_review_quadrats(quadrat_id TEXT PRIMARY KEY,step_id TEXT NOT NULL REFERENCES disposal_steps(step_id),quadrat_code TEXT NOT NULL,result TEXT NOT NULL CHECK(result IN ('pass','fail')),residual_count INTEGER NOT NULL DEFAULT 0,reviewed_at TEXT NOT NULL,reviewer TEXT NOT NULL,note TEXT NOT NULL DEFAULT '',UNIQUE(step_id,quadrat_code));
CREATE TABLE IF NOT EXISTS disposal_closeouts(closeout_id INTEGER PRIMARY KEY AUTOINCREMENT,ledger_id TEXT NOT NULL REFERENCES disposal_ledgers(ledger_id),attempt INTEGER NOT NULL,reviewer TEXT NOT NULL,result TEXT NOT NULL CHECK(result IN ('approved','rejected')),note TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);
"""
def utcnow() -> str: return datetime.now(timezone.utc).isoformat()
def connect(path: str = ":memory:") -> sqlite3.Connection:
    db=sqlite3.connect(path,timeout=10,check_same_thread=False); db.row_factory=sqlite3.Row; db.execute("PRAGMA foreign_keys=ON"); db.execute("PRAGMA journal_mode=WAL"); db.executescript(SCHEMA); db.commit(); return db
@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try: db.execute("BEGIN IMMEDIATE"); yield db; db.commit()
    except Exception: db.rollback(); raise
def audit(db, entity_type, entity_id, action, actor, payload):
    db.execute("INSERT INTO audit_events(entity_type,entity_id,action,actor,payload,created_at) VALUES(?,?,?,?,?,?)",(entity_type,entity_id,action,actor,json.dumps(payload,ensure_ascii=False,sort_keys=True),utcnow()))
def rows(db, query, args=()): return [dict(r) for r in db.execute(query,args).fetchall()]
