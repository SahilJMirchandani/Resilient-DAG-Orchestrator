"""Small validation tests that do not require an external database or web server."""
import unittest

from fastapi import HTTPException
import app


class DagValidationTests(unittest.TestCase):
    def test_accepts_linear_dag(self):
        tasks = [
            {"id": "fetch", "name": "Fetch", "dependencies": []},
            {"id": "clean", "name": "Clean", "dependencies": ["fetch"]},
        ]
        result = app._validate_dag(tasks)
        self.assertEqual([task["id"] for task in result], ["fetch", "clean"])

    def test_rejects_unknown_dependency(self):
        tasks = [{"id": "a", "name": "A", "dependencies": ["ghost"]}]
        with self.assertRaises(HTTPException) as context:
            app._validate_dag(tasks)
        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("unknown task", str(context.exception.detail))

    def test_rejects_cycle(self):
        tasks = [
            {"id": "a", "name": "A", "dependencies": ["b"]},
            {"id": "b", "name": "B", "dependencies": ["a"]},
        ]
        with self.assertRaises(HTTPException) as context:
            app._validate_dag(tasks)
        self.assertIn("Cycle detected", str(context.exception.detail))

    def test_rejects_duplicate_ids(self):
        tasks = [
            {"id": "same", "name": "A", "dependencies": []},
            {"id": "same", "name": "B", "dependencies": []},
        ]
        with self.assertRaises(HTTPException) as context:
            app._validate_dag(tasks)
        self.assertIn("Duplicate task id", str(context.exception.detail))


if __name__ == "__main__":
    unittest.main()
