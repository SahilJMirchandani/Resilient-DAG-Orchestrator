from __future__ import annotations
import json, os, sqlite3, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
DB = Path(os.getenv("DATABASE_PATH", str(ROOT / "orbit.db")))
WORKERS = max(1, min(int(os.getenv("WORKER_COUNT", "3")), 8))
TERMINAL = {"SUCCEEDED", "DEAD_LETTERED", "BLOCKED"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB, timeout=15, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=15000")
    return c


def init_db() -> None:
    with connect() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,name TEXT,status TEXT,created_at TEXT,updated_at TEXT,definition_json TEXT);
        CREATE TABLE IF NOT EXISTS tasks(run_id TEXT,task_id TEXT,name TEXT,dependencies_json TEXT,status TEXT,attempts INTEGER DEFAULT 0,max_attempts INTEGER DEFAULT 3,fail_mode TEXT DEFAULT 'none',duration_ms INTEGER DEFAULT 450,next_run_at REAL,output_json TEXT,error TEXT,idempotency_key TEXT,updated_at TEXT,PRIMARY KEY(run_id,task_id),FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT,task_id TEXT,event_type TEXT,message TEXT,created_at TEXT,FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS checkpoints(id INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT,task_id TEXT,output_json TEXT,created_at TEXT,UNIQUE(run_id,task_id),FOREIGN KEY(run_id,task_id) REFERENCES tasks(run_id,task_id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dead_letters(id INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT,task_id TEXT,attempts INTEGER,error TEXT,created_at TEXT,FOREIGN KEY(run_id,task_id) REFERENCES tasks(run_id,task_id) ON DELETE CASCADE);
        """)


def event(c: sqlite3.Connection, run_id: str, kind: str, message: str, task_id: str | None = None) -> None:
    c.execute("INSERT INTO events(run_id,task_id,event_type,message,created_at) VALUES(?,?,?,?,?)", (run_id, task_id, kind, message, now()))


def validate(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not tasks or len(tasks) > 30:
        raise HTTPException(400, "A workflow must contain between 1 and 30 tasks.")
    result, ids = [], set()
    for i, raw in enumerate(tasks, 1):
        tid = str(raw.get("id", "")).strip(); name = str(raw.get("name", tid)).strip()
        deps = raw.get("dependencies", []); mode = raw.get("fail_mode", "none")
        try: attempts, duration = int(raw.get("max_attempts", 3)), int(raw.get("duration_ms", 450))
        except (ValueError, TypeError): raise HTTPException(400, f"Task {tid or i}: attempts and duration must be integers.")
        if not tid or len(tid) > 32 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in tid): raise HTTPException(400, f"Task {i}: invalid task id.")
        if tid in ids: raise HTTPException(400, f"Duplicate task id: {tid}")
        if not name or len(name) > 80: raise HTTPException(400, f"Task {tid}: name must be 1–80 characters.")
        if not isinstance(deps, list) or any(not isinstance(x, str) for x in deps): raise HTTPException(400, f"Task {tid}: dependencies must be a list of ids.")
        if len(deps) != len(set(deps)) or tid in deps: raise HTTPException(400, f"Task {tid}: duplicate or self dependency.")
        if not 1 <= attempts <= 5: raise HTTPException(400, f"Task {tid}: max_attempts must be 1–5.")
        if not 100 <= duration <= 1500: raise HTTPException(400, f"Task {tid}: duration_ms must be 100–1500.")
        if mode not in {"none", "fail_once", "always_fail"}: raise HTTPException(400, f"Task {tid}: invalid fail_mode.")
        ids.add(tid); result.append({"id":tid,"name":name,"dependencies":deps,"max_attempts":attempts,"fail_mode":mode,"duration_ms":duration})
    for t in result:
        missing = set(t["dependencies"]) - ids
        if missing: raise HTTPException(400, f"Task {t['id']} depends on unknown task: {sorted(missing)[0]}")
    remaining = {t["id"]: set(t["dependencies"]) for t in result}
    ready = [tid for tid, deps in remaining.items() if not deps]; visited = 0
    while ready:
        tid = ready.pop(); visited += 1
        for other in remaining:
            if tid in remaining[other]:
                remaining[other].remove(tid)
                if not remaining[other]: ready.append(other)
    if visited != len(result): raise HTTPException(400, "Cycle detected. A DAG cannot contain circular task dependencies.")
    return result


_validate_dag = validate


def demo_tasks(scenario: str) -> list[dict[str, Any]]:
    if scenario not in {"success", "retry", "poison"}: raise HTTPException(400, "Choose success, retry, or poison.")
    ts = [
      {"id":"extract","name":"Extract source data","dependencies":[],"duration_ms":650},
      {"id":"validate","name":"Validate records","dependencies":["extract"],"duration_ms":550},
      {"id":"enrich","name":"Enrich customer data","dependencies":["validate"],"duration_ms":850},
      {"id":"score","name":"Score transactions","dependencies":["validate"],"duration_ms":720},
      {"id":"aggregate","name":"Aggregate results","dependencies":["enrich","score"],"duration_ms":500},
      {"id":"publish","name":"Publish report","dependencies":["aggregate"],"duration_ms":450},
    ]
    for t in ts: t.update(max_attempts=3, fail_mode="none")
    if scenario == "retry": next(t for t in ts if t["id"]=="enrich")["fail_mode"] = "fail_once"
    if scenario == "poison": next(t for t in ts if t["id"]=="score")["fail_mode"] = "always_fail"
    return ts


def create_run(name: str, raw_tasks: list[dict[str, Any]]) -> str:
    tasks = validate(raw_tasks); rid = uuid.uuid4().hex[:10]; stamp = now()
    with connect() as c:
        c.execute("INSERT INTO runs VALUES(?,?,?,?,?,?)", (rid,name[:100],"QUEUED",stamp,stamp,json.dumps({"tasks":tasks,"worker_count":WORKERS})))
        for t in tasks:
            c.execute("INSERT INTO tasks(run_id,task_id,name,dependencies_json,status,attempts,max_attempts,fail_mode,duration_ms,idempotency_key,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
             (rid,t["id"],t["name"],json.dumps(t["dependencies"]),"PENDING",0,t["max_attempts"],t["fail_mode"],t["duration_ms"],f"{rid}:{t['id']}",stamp))
        event(c,rid,"RUN_CREATED",f"Workflow accepted with {len(tasks)} tasks; DAG validation passed.")
        event(c,rid,"CONTROL_PLANE_READY",f"Local worker pool capacity is {WORKERS}.")
    return rid


def run_scheduler(rid: str) -> None:
    with connect() as c:
        c.execute("UPDATE runs SET status='RUNNING',updated_at=? WHERE id=?",(now(),rid)); event(c,rid,"RUN_STATUS_CHANGED","Workflow status changed to RUNNING.")
    def execute(t: dict[str, Any]) -> None:
        time.sleep(t["duration_ms"]/1000.0)
        failed = t["fail_mode"] == "always_fail" or (t["fail_mode"] == "fail_once" and t["attempt"] == 1)
        with connect() as c:
            row=c.execute("SELECT attempts,max_attempts FROM tasks WHERE run_id=? AND task_id=?",(rid,t["id"])).fetchone()
            if failed and row["attempts"] < row["max_attempts"]:
                # Exponential retry backoff: 0.3s, 0.6s, 1.2s, capped at 2.4s for this demo.
                delay_seconds = min(0.3 * (2 ** max(0, row["attempts"] - 1)), 2.4)
                c.execute("UPDATE tasks SET status='RETRYING',error=?,next_run_at=?,updated_at=? WHERE run_id=? AND task_id=?",("Injected transient failure",time.time()+delay_seconds,now(),rid,t["id"]))
                event(c,rid,"TASK_RETRYING",f"{t['name']} failed on attempt {t['attempt']}; retry scheduled with exponential backoff ({delay_seconds:.2f}s).",t["id"])
            elif failed:
                error="Injected poison-task failure; retry limit exhausted"
                c.execute("UPDATE tasks SET status='DEAD_LETTERED',error=?,next_run_at=NULL,updated_at=? WHERE run_id=? AND task_id=?",(error,now(),rid,t["id"]))
                c.execute("INSERT INTO dead_letters(run_id,task_id,attempts,error,created_at) VALUES(?,?,?,?,?)",(rid,t["id"],row["attempts"],error,now()))
                event(c,rid,"TASK_DEAD_LETTERED",f"{t['name']} exhausted retries and entered the dead-letter queue.",t["id"])
            else:
                output={"task_id":t["id"],"task_name":t["name"],"attempt":t["attempt"],"upstream_outputs_received":t["dependencies"],"artifact":{"records_processed":120+t["attempt"],"source":"demo://transactions"}}
                c.execute("INSERT INTO checkpoints(run_id,task_id,output_json,created_at) VALUES(?,?,?,?) ON CONFLICT(run_id,task_id) DO UPDATE SET output_json=excluded.output_json,created_at=excluded.created_at",(rid,t["id"],json.dumps(output),now()))
                c.execute("UPDATE tasks SET status='SUCCEEDED',output_json=?,error=NULL,next_run_at=NULL,updated_at=? WHERE run_id=? AND task_id=?",(json.dumps(output),now(),rid,t["id"]))
                event(c,rid,"CHECKPOINT_SAVED",f"Durable output checkpoint saved for {t['name']} before completion was committed.",t["id"])
                event(c,rid,"TASK_SUCCEEDED",f"{t['name']} succeeded on attempt {t['attempt']}.",t["id"])
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        while True:
            with connect() as c:
                rows=[dict(r) for r in c.execute("SELECT * FROM tasks WHERE run_id=?",(rid,)).fetchall()]
                byid={r["task_id"]:r for r in rows}
                for t in rows:
                    if t["status"]=="RETRYING" and (t["next_run_at"] or 0)<=time.time():
                        c.execute("UPDATE tasks SET status='READY',next_run_at=NULL,updated_at=? WHERE run_id=? AND task_id=?",(now(),rid,t["task_id"]))
                        event(c,rid,"TASK_READY",f"{t['name']} is ready for retry.",t["task_id"]); t["status"]="READY"
                for t in rows:
                    if t["status"] != "PENDING": continue
                    deps=[byid[d]["status"] for d in json.loads(t["dependencies_json"])]
                    if any(s in {"DEAD_LETTERED","BLOCKED"} for s in deps):
                        c.execute("UPDATE tasks SET status='BLOCKED',error='A dependency did not succeed',updated_at=? WHERE run_id=? AND task_id=?",(now(),rid,t["task_id"]))
                        event(c,rid,"TASK_BLOCKED",f"{t['name']} is blocked by an unsuccessful dependency.",t["task_id"]); t["status"]="BLOCKED"
                    elif all(s=="SUCCEEDED" for s in deps):
                        c.execute("UPDATE tasks SET status='READY',updated_at=? WHERE run_id=? AND task_id=?",(now(),rid,t["task_id"]))
                        event(c,rid,"TASK_READY",f"{t['name']} is ready; all dependencies succeeded.",t["task_id"]); t["status"]="READY"
                active=len(futures)
                for t in [x for x in rows if x["status"]=="READY"][:max(0,WORKERS-active)]:
                    attempt=int(t["attempts"])+1
                    c.execute("UPDATE tasks SET status='RUNNING',attempts=?,updated_at=? WHERE run_id=? AND task_id=?",(attempt,now(),rid,t["task_id"]))
                    event(c,rid,"TASK_STARTED",f"Worker claimed {t['name']} (attempt {attempt}).",t["task_id"])
                    job={"id":t["task_id"],"name":t["name"],"duration_ms":t["duration_ms"],"fail_mode":t["fail_mode"],"attempt":attempt,"dependencies":json.loads(t["dependencies_json"])}
                    futures[pool.submit(execute,job)] = t["task_id"]
                terminal=all(t["status"] in TERMINAL for t in rows)
                waiting=any(t["status"] in {"PENDING","READY","RUNNING","RETRYING"} for t in rows)
                if terminal and not futures and not waiting:
                    final="FAILED" if any(t["status"]=="DEAD_LETTERED" for t in rows) else "SUCCEEDED"
                    c.execute("UPDATE runs SET status=?,updated_at=? WHERE id=?",(final,now(),rid))
                    event(c,rid,"RUN_STATUS_CHANGED",f"Workflow status changed to {final}.")
                    break
            for f in [x for x in list(futures) if x.done()]:
                f.result(); futures.pop(f,None)
            time.sleep(0.06)


def snapshot(rid: str) -> dict[str, Any]:
    with connect() as c:
        run=c.execute("SELECT * FROM runs WHERE id=?",(rid,)).fetchone()
        if not run: raise HTTPException(404,"Workflow not found.")
        tasks=[dict(r) for r in c.execute("SELECT * FROM tasks WHERE run_id=? ORDER BY rowid",(rid,)).fetchall()]
        for t in tasks:
            t["dependencies"]=json.loads(t.pop("dependencies_json")); t["output"]=json.loads(t.pop("output_json")) if t.get("output_json") else None
        events=[dict(r) for r in c.execute("SELECT * FROM events WHERE run_id=? ORDER BY id DESC LIMIT 120",(rid,)).fetchall()]
        checkpoints=[dict(r) for r in c.execute("SELECT * FROM checkpoints WHERE run_id=? ORDER BY id",(rid,)).fetchall()]
        dead=[dict(r) for r in c.execute("SELECT * FROM dead_letters WHERE run_id=? ORDER BY id DESC",(rid,)).fetchall()]
    retries=sum(max(0,t["attempts"]-1) for t in tasks)
    return {"id":run["id"],"name":run["name"],"status":run["status"],"created_at":run["created_at"],"updated_at":run["updated_at"],"tasks":tasks,"events":events,"checkpoints":checkpoints,"dead_letters":dead,"metrics":{"total_tasks":len(tasks),"completed":sum(t["status"]=="SUCCEEDED" for t in tasks),"retries":retries,"attempts":sum(t["attempts"] for t in tasks),"checkpoint_count":len(checkpoints),"dead_lettered":sum(t["status"]=="DEAD_LETTERED" for t in tasks),"blocked":sum(t["status"]=="BLOCKED" for t in tasks),"worker_count":WORKERS}}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app=FastAPI(title="ORBIT — Resilient DAG Orchestrator",version="1.0.0",lifespan=lifespan)

@app.get("/")
def home():
    return FileResponse(STATIC / "index.html")

@app.get("/api/health")
def health():
    return {"status":"ok","worker_capacity":WORKERS,"mode":"educational-simulation"}

@app.get("/api/runs")
def list_runs():
    with connect() as c:
        rows=c.execute("SELECT id,name,status,created_at,updated_at FROM runs ORDER BY created_at DESC LIMIT 12").fetchall()
    return {"runs":[dict(r) for r in rows]}

@app.post("/api/runs/demo")
def demo(background: BackgroundTasks, scenario: str="success"):
    name={"success":"Happy path workflow","retry":"Transient failure + retry","poison":"Poison task + dead-letter"}.get(scenario,scenario)
    rid=create_run(name,demo_tasks(scenario)); background.add_task(run_scheduler,rid); return {"id":rid,"status":"QUEUED"}

@app.post("/api/runs")
def custom(background: BackgroundTasks, payload: dict[str,Any]):
    tasks=payload.get("tasks")
    if not isinstance(tasks,list): raise HTTPException(400,"Provide a tasks array.")
    rid=create_run(str(payload.get("name") or "Custom workflow"),tasks); background.add_task(run_scheduler,rid); return {"id":rid,"status":"QUEUED"}

@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    return snapshot(run_id)

@app.post("/api/runs/{run_id}/tasks/{task_id}/retry")
def recover(run_id: str, task_id: str, background: BackgroundTasks):
    with connect() as c:
        rows=[dict(r) for r in c.execute("SELECT task_id,dependencies_json,status FROM tasks WHERE run_id=?",(run_id,)).fetchall()]
        byid={r["task_id"]:r for r in rows}
        if task_id not in byid: raise HTTPException(404,"Task not found.")
        if byid[task_id]["status"]!="DEAD_LETTERED": raise HTTPException(400,"Only a dead-lettered task can be recovered.")
        reset={task_id}; changed=True
        while changed:
            changed=False
            for t in rows:
                if t["task_id"] not in reset and set(json.loads(t["dependencies_json"])) & reset:
                    reset.add(t["task_id"]); changed=True
        for tid in reset:
            c.execute("UPDATE tasks SET status='PENDING',attempts=0,fail_mode='none',next_run_at=NULL,error=NULL,output_json=NULL,updated_at=? WHERE run_id=? AND task_id=?",(now(),run_id,tid))
            c.execute("DELETE FROM checkpoints WHERE run_id=? AND task_id=?",(run_id,tid))
            event(c,run_id,"MANUAL_RECOVERY",f"Operator recovery reset {tid}; downstream tasks will be rescheduled.",tid)
        c.execute("UPDATE runs SET status='QUEUED',updated_at=? WHERE id=?",(now(),run_id))
    background.add_task(run_scheduler,run_id)
    return {"id":run_id,"recovered_tasks":sorted(reset),"status":"QUEUED"}
