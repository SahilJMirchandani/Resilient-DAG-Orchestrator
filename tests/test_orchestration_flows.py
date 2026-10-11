"""Integration tests for the main ORBIT educational workflow scenarios."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import app


class OrchestrationFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls._original_db = app.DB
        app.DB = Path(cls._tmp.name) / "orbit_test.db"
        cls.client = TestClient(app.app)
        cls.client.__enter__()
        cls._demo_tasks = staticmethod(app.demo_tasks)

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)
        app.DB = cls._original_db
        cls._tmp.cleanup()

    def fast_demo(self, scenario):
        tasks = self._demo_tasks(scenario)
        for task in tasks:
            task["duration_ms"] = 100
        return tasks

    def start_demo(self, scenario):
        with patch.object(app, "demo_tasks", side_effect=self.fast_demo):
            response = self.client.post("/api/runs/demo", params={"scenario": scenario})
        self.assertEqual(response.status_code, 200, response.text)
        run_id = response.json()["id"]
        state = self.client.get(f"/api/runs/{run_id}")
        self.assertEqual(state.status_code, 200, state.text)
        return run_id, state.json()

    def test_health_dashboard_and_api_docs(self):
        self.assertEqual(self.client.get("/api/health").json()["status"], "ok")
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/docs").status_code, 200)

    def test_happy_path_completes_all_tasks_and_checkpoints(self):
        _, state = self.start_demo("success")
        self.assertEqual(state["status"], "SUCCEEDED")
        self.assertEqual(state["metrics"]["completed"], 6)
        self.assertEqual(state["metrics"]["checkpoint_count"], 6)
        self.assertEqual(state["metrics"]["retries"], 0)

    def test_transient_failure_retries_and_succeeds(self):
        _, state = self.start_demo("retry")
        self.assertEqual(state["status"], "SUCCEEDED")
        self.assertEqual(state["metrics"]["completed"], 6)
        self.assertEqual(state["metrics"]["retries"], 1)
        enrich = next(task for task in state["tasks"] if task["task_id"] == "enrich")
        self.assertEqual(enrich["attempts"], 2)
        self.assertTrue(any(event["event_type"] == "TASK_RETRYING" for event in state["events"]))
        retry_event = next(event for event in state["events"] if event["event_type"] == "TASK_RETRYING")
        self.assertIn("exponential backoff", retry_event["message"])

    def test_poison_task_blocks_dependants_and_manual_recovery_succeeds(self):
        run_id, state = self.start_demo("poison")
        self.assertEqual(state["status"], "FAILED")
        self.assertEqual(state["metrics"]["dead_lettered"], 1)
        self.assertEqual(state["metrics"]["blocked"], 2)
        response = self.client.post(f"/api/runs/{run_id}/tasks/score/retry")
        self.assertEqual(response.status_code, 200, response.text)
        recovered = self.client.get(f"/api/runs/{run_id}").json()
        self.assertEqual(recovered["status"], "SUCCEEDED")
        self.assertEqual(recovered["metrics"]["completed"], 6)

    def test_custom_dag_rejects_cycle_as_client_error(self):
        response = self.client.post("/api/runs", json={"name": "bad graph", "tasks": [
            {"id": "a", "dependencies": ["b"]},
            {"id": "b", "dependencies": ["a"]},
        ]})
        self.assertEqual(response.status_code, 400)
        self.assertIn("Cycle detected", response.json()["detail"])

    def test_custom_dag_rejects_non_object_task(self):
        response = self.client.post("/api/runs", json={"name": "bad task", "tasks": [None]})
        self.assertEqual(response.status_code, 400)
        self.assertIn("JSON object", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
