# ORBIT — Resilient DAG Orchestrator

**ORBIT** is an educational SSD Virtual Lab microproject based on Group 07's *Resilient Distributed DAG Orchestrator* design. It provides an interactive task-dependency graph and lets students inspect normal execution, transient failures and retries, checkpoints, dead-lettering, and manual recovery.

## Features

- **Guided Virtual Lab:** a structured, browser-based learning experience covering Aim, Theory, Procedure, Simulation, Observations, Self-test, Result, and References
- **Live lab route:** [Open the ORBIT Virtual Lab](https://resilient-dag-orchestrator.onrender.com/virtual-lab) (served by the same FastAPI application and backed by the ORBIT API)
- DAG validation, missing-dependency checks, duplicate-ID checks, and cycle detection
- Dependency-aware scheduling with a bounded concurrent worker pool
- Task states including `PENDING`, `READY`, `RUNNING`, `RETRYING`, `SUCCEEDED`, `DEAD_LETTERED`, and `BLOCKED`
- SQLite state store, event history, task-output checkpoints, and dead-letter records
- Interactive graph, task inspector, retry/checkpoint metrics, event timeline, and custom-DAG JSON editor
- Fault-injection rehearsals for worker-crash reclaim, duplicate completion/idempotency, and persisted-state replay audit
- Per-run elapsed time and completed-task throughput, with recovery-window measurement after manual recovery
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

Open <http://127.0.0.1:8000> for the main dashboard. On Windows, you can also use `run_local.bat`.

### Guided Virtual Lab

Open <http://127.0.0.1:8000/virtual-lab> to use the guided experiment page locally, or visit the [deployed ORBIT Virtual Lab](https://resilient-dag-orchestrator.onrender.com/virtual-lab).

The lab is organised into these learning sections:

1. **Aim & learning outcomes** — defines the experiment objective.
2. **Theory** — explains DAG dependencies, scheduling, retries, checkpoints, dead-lettering, and idempotency.
3. **Procedure** — provides a beginner-friendly sequence for running the experiments.
4. **Simulation** — starts supported ORBIT runs and displays the dependency graph, task state, event timeline, and recovery control.
5. **Observations** — reads the selected run's status, task states, retries, checkpoint counts, and app-reported metrics from the API.
6. **Self-test** — checks understanding with five multiple-choice questions and feedback.
7. **Result** — summarises the latest run and its observed values.
8. **References** — links to Virtual Labs learning examples and the ORBIT implementation.

**Recommended first run:** read Aim and Theory, follow the Procedure, run **Happy Path**, then compare it with **Transient Failure** and **Poison Task**. In the Poison Task scenario, select the dead-lettered task and choose **Recover task & downstream** to observe recovery.

The lab uses the same supported scenario IDs documented below. Worker-crash reclaim, duplicate delivery, and restart/replay are explicitly labelled **educational rehearsals**: they inject events into a single-process application and do not represent actual worker-process termination, a second broker delivery, or a real process restart.

## Experiments

1. **Happy path:** all six tasks succeed; `enrich` and `score` are independent branches and can execute concurrently.
2. **Transient failure:** `enrich` fails once, retries, and eventually succeeds.
3. **Poison task:** `score` exhausts its retry budget; the task is dead-lettered and downstream work is blocked. Select the failed task and trigger **Recover task & downstream**.
4. **Worker crash rehearsal:** inject a simulated worker crash, then observe lease-expiry-style reclaim and a successful retry.
5. **Duplicate-delivery rehearsal:** observe a simulated duplicate completion and the idempotency guard; the demo keeps one checkpoint for the task.
6. **Control-plane replay audit:** emit a simulated restart event and inspect how many successful tasks/checkpoints exist in the state store. This is an audit rehearsal, not an actual process restart.
7. **Custom DAG:** submit your own graph as JSON; a cyclic graph or missing dependency is rejected.

### Scenario reference

| Scenario | What to observe |
|---|---|
| `success` | Dependency order, concurrent independent branches, and saved output checkpoints |
| `retry` | The `enrich` task fails once, retries, then succeeds |
| `poison` | The `score` task exhausts retries; downstream tasks block until manual recovery |
| `worker_crash` | An injected worker-crash event followed by simulated lease-expiry reclaim |
| `duplicate` | An injected duplicate-completion event and the idempotency guard |
| `replay` | A simulated restart marker and audit of existing task/checkpoint state |

## API quick reference

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Health and worker capacity |
| GET | `/api/runs` | Recent workflows |
| POST | `/api/runs/demo?scenario=success` | Start success experiment |
| POST | `/api/runs/demo?scenario=retry` | Start retry experiment |
| POST | `/api/runs/demo?scenario=poison` | Start dead-letter experiment |
| POST | `/api/runs/demo?scenario=worker_crash` | Worker crash and lease-reclaim rehearsal |
| POST | `/api/runs/demo?scenario=duplicate` | Duplicate-delivery/idempotency rehearsal |
| POST | `/api/runs/demo?scenario=replay` | Simulated restart and checkpoint audit |
| GET | `/api/runs/{run_id}` | Workflow state, tasks, events, checkpoints, and run metrics |
| POST | `/api/runs` | Submit a custom DAG |
| POST | `/api/runs/{run_id}/tasks/{task_id}/retry` | Recover failed task and downstream work |

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/SahilJMirchandani/Resilient-DAG-Orchestrator)

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

## Fault-injection and metric interpretation

The worker-crash experiment uses an injected event and retry delay to model lease-expiry reclamation; it does not kill an operating-system worker. The duplicate-delivery experiment emits an injected duplicate-completion event and idempotency-guard event; it does not publish a second broker message. The control-plane replay experiment emits a simulated restart marker and audits existing persisted task/checkpoint counts; it does not restart the running process or reconstruct state from a separate WAL. Elapsed time and task throughput are measured within this single service. Recovery window is only populated after the manual recovery action.

## Scope and limitations

This is a **teaching MVP**, not a production distributed orchestrator. The scheduler and workers run in one Python process; queues and autoscaling are simulated. It does not implement real SQS/Pub/Sub, independent autoscaling workers, replicated control-plane nodes, or a distributed heartbeat/lease service. On free hosting, the local SQLite database may reset after restart, redeploy, or spin-down. Describe these limitations accurately in the lab journal.
