# ORBIT — Resilient DAG Orchestrator

**ORBIT** is an educational SSD Virtual Lab microproject based on Group 07's *Resilient Distributed DAG Orchestrator* design. It provides an interactive task-dependency graph and lets students inspect normal execution, transient failures and retries, checkpoints, dead-lettering, and manual recovery.

## Features

- DAG validation, missing-dependency checks, duplicate-ID checks, and cycle detection
- Dependency-aware scheduling with a bounded concurrent worker pool
- Task states including `PENDING`, `READY`, `RUNNING`, `RETRYING`, `SUCCEEDED`, `DEAD_LETTERED`, and `BLOCKED`
- SQLite state store, event history, task-output checkpoints, and dead-letter records
- Interactive graph, task inspector, metrics, event timeline, and custom-DAG JSON editor
- FastAPI endpoints and interactive docs at `/docs`

## Run locally

Python 3.11 or 3.12 is recommended.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

Open <http://127.0.0.1:8000>. On Windows, you can also use `run_local.bat`.

## Experiments

1. **Happy path:** all six tasks succeed; `enrich` and `score` are independent branches and can execute concurrently.
2. **Transient failure:** `enrich` fails once, retries, and eventually succeeds.
3. **Poison task:** `score` exhausts its retry budget; the task is dead-lettered and downstream work is blocked. Select the failed task and trigger **Recover task & downstream**.
4. **Custom DAG:** submit your own graph as JSON; a cyclic graph or missing dependency is rejected.

## API quick reference

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Health and worker capacity |
| GET | `/api/runs` | Recent workflows |
| POST | `/api/runs/demo?scenario=success` | Start success experiment |
| POST | `/api/runs/demo?scenario=retry` | Start retry experiment |
| POST | `/api/runs/demo?scenario=poison` | Start dead-letter experiment |
| GET | `/api/runs/{run_id}` | Workflow state, tasks, events, and checkpoints |
| POST | `/api/runs` | Submit a custom DAG |
| POST | `/api/runs/{run_id}/tasks/{task_id}/retry` | Recover failed task and downstream work |

## Deploy on Render

The root-level `render.yaml` configures a single Python web service. In Render, choose **New → Blueprint**, connect this public repository, review the service configuration, and deploy it. See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for health checks and smoke tests.

## Architecture mapping

| CA1 component | This MVP |
|---|---|
| Orchestrator control plane | FastAPI routes and scheduler loop |
| Metadata/state store | SQLite `runs` and `tasks` tables |
| Write-ahead/transition history | Append-only `events` table and SQLite WAL mode |
| Task queue | In-process ready-task selection |
| Worker pool | Bounded `ThreadPoolExecutor` |
| Checkpoint store | SQLite `checkpoints` table |
| Completion event feedback | Worker futures processed by the scheduler |
| Dead-letter queue | SQLite `dead_letters` table |
| Health monitor / worker leases | Not implemented as an independent service |

## Scope and limitations

This is a **teaching MVP**, not a production distributed orchestrator. The scheduler and workers run in one Python process; queues and autoscaling are simulated. It does not implement real SQS/Pub/Sub, independent autoscaling workers, replicated control-plane nodes, or a distributed heartbeat/lease service. On free hosting, the local SQLite database may reset after restart, redeploy, or spin-down. Describe these limitations accurately in the lab journal.
