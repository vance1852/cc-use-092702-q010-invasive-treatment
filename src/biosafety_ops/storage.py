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
CREATE TABLE IF NOT EXISTS treatment_ledgers(ledger_id TEXT PRIMARY KEY,alert_id TEXT NOT NULL,zone_record_id TEXT NOT NULL,current_version INTEGER NOT NULL,status TEXT NOT NULL,closure_kind TEXT,closed_at TEXT,closed_by TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS treatment_plan_versions(ledger_id TEXT NOT NULL,version INTEGER NOT NULL,alert_id TEXT NOT NULL,content_json TEXT NOT NULL,content_sha256 TEXT NOT NULL,change_kind TEXT NOT NULL,reason TEXT NOT NULL,effective_at TEXT NOT NULL,superseded_at TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(ledger_id,version));
CREATE TABLE IF NOT EXISTS treatment_step_instances(step_instance_id INTEGER PRIMARY KEY AUTOINCREMENT,ledger_id TEXT NOT NULL,plan_version INTEGER NOT NULL,step_key TEXT NOT NULL,step_seq INTEGER NOT NULL,rework_round INTEGER NOT NULL DEFAULT 0,deadline_hours REAL NOT NULL,deadline_at TEXT,status TEXT NOT NULL,evidence_json TEXT NOT NULL DEFAULT '{}',completed_at TEXT,completed_by TEXT,UNIQUE(ledger_id,plan_version,step_key,rework_round));
CREATE TABLE IF NOT EXISTS treatment_step_progress(step_instance_id INTEGER NOT NULL,progress_key TEXT NOT NULL,report_id TEXT NOT NULL,payload_json TEXT NOT NULL,created_by TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(step_instance_id,progress_key));
CREATE TABLE IF NOT EXISTS treatment_step_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,ledger_id TEXT NOT NULL,plan_version INTEGER NOT NULL,step_instance_id INTEGER,step_key TEXT,rework_round INTEGER NOT NULL DEFAULT 0,event_type TEXT NOT NULL,actor TEXT NOT NULL,payload_json TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS treatment_progress_reports(report_id TEXT PRIMARY KEY,ledger_id TEXT NOT NULL,plan_version INTEGER NOT NULL,reporter TEXT NOT NULL,step_key TEXT NOT NULL,rework_round INTEGER NOT NULL DEFAULT 0,payload_sha256 TEXT NOT NULL,response_json TEXT NOT NULL,created_at TEXT NOT NULL);
"""
def utcnow() -> str: return datetime.now(timezone.utc).isoformat()
def connect(path: str = ":memory:") -> sqlite3.Connection:
    db=sqlite3.connect(path,timeout=10); db.row_factory=sqlite3.Row; db.execute("PRAGMA foreign_keys=ON"); db.execute("PRAGMA journal_mode=WAL"); db.executescript(SCHEMA); db.commit(); return db
@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try: db.execute("BEGIN IMMEDIATE"); yield db; db.commit()
    except Exception: db.rollback(); raise
def audit(db, entity_type, entity_id, action, actor, payload):
    db.execute("INSERT INTO audit_events(entity_type,entity_id,action,actor,payload,created_at) VALUES(?,?,?,?,?,?)",(entity_type,entity_id,action,actor,json.dumps(payload,ensure_ascii=False,sort_keys=True),utcnow()))
def rows(db, query, args=()): return [dict(r) for r in db.execute(query,args).fetchall()]
