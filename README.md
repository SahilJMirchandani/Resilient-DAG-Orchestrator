# ORBIT — Resilient DAG Orchestrator

An educational SSD Virtual Lab microproject based on the Group 07 CA1 Phase 1 design. ORBIT visualizes task dependencies, task-state transitions, retries, checkpoints, dead-lettering, and manual recovery.

## Repository contents

- `ResilientDAGOrchestrator.zip` — complete project source, including the FastAPI backend, interactive dashboard, tests, and local startup script.
- `render.yaml` — Render Blueprint configuration.
- `requirements.txt` — Python runtime dependencies.
- `Procfile` — web process declaration.

The Render build extracts the project bundle and runs the application from the extracted source directory. To work with the normal project files locally, download and extract `ResilientDAGOrchestrator.zip`.

## Deployment

1. In Render, choose **New → Blueprint** and select this repository.
2. Review the service settings from `render.yaml`, then deploy.
3. Open the Render URL and test `/api/health`, `/docs`, and the three experiments in the dashboard.

See `DEPLOYMENT_GUIDE.md` inside the ZIP for the complete walkthrough and smoke-test checklist.

## Local run

Extract the ZIP, then from the extracted project directory run:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000.

## Scope and limitations

This is a teaching MVP, not a production distributed orchestrator. The scheduler and worker pool run in one Python process; queues and autoscaling are simulated. On free ephemeral hosting, SQLite workflow history may reset after restarts or redeploys. The project does not implement real SQS/Pub/Sub, independent autoscaling workers, or replicated control-plane recovery.

