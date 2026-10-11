# ORBIT Deployment Guide — GitHub + Render

## Repository

The source is available in `app.py`, `static/index.html`, `requirements.txt`, `render.yaml`, and `Procfile`. The dashboard is served by FastAPI. No API keys or external service credentials are required.

## Deploy with Render Blueprint

1. Sign in to [Render](https://render.com/).
2. Choose **New → Blueprint**.
3. Select `SahilJMirchandani/Resilient-DAG-Orchestrator`.
4. Review the service named `resilient-dag-orchestrator`, then apply the Blueprint.
5. Wait for the service state to become **Live** and open its assigned public URL.

The root `render.yaml` sets the Python runtime, installs `requirements.txt`, runs `uvicorn app:app --host 0.0.0.0 --port $PORT`, and uses `/api/health` as the health check.

## Smoke test the deployed service

Replace `<PUBLIC-URL>` with the URL shown by Render.

1. Open `<PUBLIC-URL>/api/health`; expect JSON with `"status":"ok"`.
2. Open `<PUBLIC-URL>/docs` to inspect the interactive API documentation.
3. From the dashboard, run **Happy path** and check that all six tasks succeed.
4. Run **Transient failure**; check that `enrich` retries and eventually succeeds.
5. Run **Poison task**; check that `score` is dead-lettered and downstream tasks are blocked. Select `score` and click **Recover task & downstream**.
6. Run **Worker crash**; confirm the event timeline marks the worker crash and subsequent reclaim. This is an injected simulation, not an actual process kill.
7. Run **Duplicate delivery**; confirm the idempotency-guard event and one checkpoint for the `score` task. This is an injected event rehearsal, not a real broker redelivery.
8. Run **Checkpoint replay**; confirm the simulated restart marker and state-store audit. It does not restart the actual process.
9. Check the performance panel for elapsed time and completed-tasks-per-second. Recovery time appears after you recover a dead-lettered task.
10. Submit a custom DAG, then try a cyclic dependency and confirm that the API rejects it.
11. Capture journal screenshots only after these checks are complete.

## Limitations

This is an educational single-process simulation. The scheduler and worker pool run inside the same Python service; cloud queues, independent autoscaling workers, a separate health monitor/lease service, and replicated control-plane recovery are not implemented. Render free services can spin down when idle, and files written to their local filesystem may be lost after restart or redeployment. Treat hosted SQLite history as disposable demonstration data.
