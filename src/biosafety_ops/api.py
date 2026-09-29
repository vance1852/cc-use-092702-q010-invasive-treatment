"""依赖标准库的 JSON HTTP API。"""
from __future__ import annotations
import argparse,json
from http.server import BaseHTTPRequestHandler,HTTPServer
from .models import MonitoringRecord,ZoneRecord
from .service import BiosafetyService
class Handler(BaseHTTPRequestHandler):
    service=BiosafetyService()
    def _send(self,status,payload):
        data=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
    def _token(self):return self.headers.get("Authorization","").removeprefix("Bearer ")
    def do_GET(self):
        try:
            if self.path=="/health":return self._send(200,{"status":"ok","service":"urban-biosafety"})
            if self.path=="/ledgers":return self._send(200,{"ledgers":self.service.ledger.list_ledgers(self._token())})
            if self.path.startswith("/ledgers/"):return self._send(200,self.service.ledger.ledger_view(self._token(),self.path.split("/")[2]))
            if self.path.startswith("/zone_records/") and self.path.endswith("/risk"):return self._send(200,self.service.risk_report(self._token(),self.path.split("/")[2]))
            if self.path.startswith("/zone_records/"):return self._send(200,self.service.zone_record(self._token(),self.path.split("/",2)[2]))
            return self._send(404,{"error":"not found"})
        except PermissionError as e:return self._send(403,{"error":str(e)})
        except KeyError as e:return self._send(404,{"error":f"不存在: {e.args[0]}"})
        except Exception as e:return self._send(400,{"error":str(e)})
    def do_POST(self):
        try:
            body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))) or b"{}")
            if self.path=="/login":return self._send(200,{"token":self.service.auth.login(body["user_id"],body["password"])})
            token=self._token()
            if self.path=="/zone_records":return self._send(201,self.service.register_zone_record(token,ZoneRecord(body["zone_record_id"],body["collection_zone"],body["biosafety_type"],body["length_m"],body["criticality"])))
            if self.path.startswith("/zone_records/") and self.path.endswith("/monitoring_records"):
                sid=self.path.split("/")[2]; r=MonitoringRecord(body["monitoring_record_id"],sid,body["sensor_source_id"],body["speed_kmh"],body["traffic_flow_vph"],body["impact_index"],body["observed_at"]); return self._send(201,self.service.ingest_monitoring_record(token,r))
            if self.path.startswith("/zone_records/") and self.path.endswith("/work-orders"):
                return self._send(201,self.service.create_treatment_ticket(token,self.path.split("/")[2],body["alert_id"],body["assignee"],body.get("priority",3)))
            if self.path=="/ledgers":return self._send(201,self.service.ledger.create_ledger(token,alert_id=body["alert_id"],zone_record_id=body["zone_record_id"],area=body["area"],clearing_zones=body.get("clearing_zones",[]),notice_recipients=body.get("notice_recipients",[]),chemical_batches=body.get("chemical_batches",[]),reason=body.get("reason",""),deadline_hours=body.get("deadline_hours")))
            if self.path.startswith("/ledgers/") and self.path.endswith("/progress"):
                return self._send(200,self.service.ledger.report_progress(token,self.path.split("/")[2],body))
            if self.path.startswith("/ledgers/") and self.path.endswith("/revisions"):
                return self._send(201,self.service.ledger.revise_plan(token,self.path.split("/")[2],change_kind=body["change_kind"],reason=body.get("reason",""),area=body.get("area"),clearing_zones=body.get("clearing_zones"),notice_recipients=body.get("notice_recipients"),chemical_batches=body.get("chemical_batches"),deadline_hours=body.get("deadline_hours")))
            return self._send(404,{"error":"not found"})
        except PermissionError as e:return self._send(403,{"error":str(e)})
        except KeyError as e:return self._send(404,{"error":f"不存在: {e.args[0]}"})
        except Exception as e:return self._send(400,{"error":str(e)})
def main():
    p=argparse.ArgumentParser(); p.add_argument("--database",default=":memory:"); p.add_argument("--host",default="127.0.0.1"); p.add_argument("--port",type=int,default=8080); a=p.parse_args(); Handler.service=BiosafetyService(a.database); Handler.service.bootstrap(); HTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=="__main__":main()
