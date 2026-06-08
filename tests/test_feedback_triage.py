import io
import json
import tarfile
import tempfile
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import importlib.util


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "feedback_triage.py"


def load_module():
    spec = importlib.util.spec_from_file_location("feedback_triage", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["feedback_triage"] = module
    spec.loader.exec_module(module)
    return module


def tar_bytes(files):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in files.items():
            data = content.encode("utf-8") if isinstance(content, str) else content
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class FeedbackTriageTest(unittest.TestCase):
    def test_fetch_downloads_archive_over_ssh_and_extracts_run_dir(self):
        feedback = {
            "items": [
                {
                    "id": 1,
                    "type": "analysis_issue",
                    "message": "Wrong generated inlet",
                    "attachments": [
                        {"kind": "generated_omentity", "filename": "generated_omentity.py"}
                    ],
                }
            ]
        }
        archive = tar_bytes({
            "feedback.json": json.dumps(feedback),
            "attachments/feedback-000001/generated_omentity.py": "# generated\n",
        })
        triage = load_module()

        with tempfile.TemporaryDirectory() as tmp:
            with patch("feedback_triage.subprocess.run") as run:
                run.return_value = SimpleNamespace(stdout=archive, stderr=b"", returncode=0)

                run_dir = triage.fetch_feedback_archive(
                    inbox_dir=Path(tmp),
                    host="ivm-1",
                    app_dir="/srv/app",
                    port=8000,
                    mode="new",
                    mark_exported=False,
                    ssh_command="ssh",
                )

                command = run.call_args.args[0]
                self.assertEqual(command[:3], ["ssh", "ivm-1", "bash"])
                self.assertIn(b"/api/feedback/dag-issues/archive", run.call_args.kwargs["input"])
                self.assertTrue((run_dir / "dag-issues-feedback.tar.gz").exists())
                self.assertTrue((run_dir / "feedback.json").exists())
                self.assertTrue((run_dir / "attachments" / "feedback-000001" / "generated_omentity.py").exists())
                fetch_info = json.loads((run_dir / "fetch.json").read_text(encoding="utf-8"))
                self.assertEqual(fetch_info["host"], "ivm-1")
                self.assertEqual(fetch_info["mode"], "new")
                self.assertFalse(fetch_info["mark_exported"])
                self.assertEqual(fetch_info["item_count"], 1)

    def test_analyze_writes_review_with_omentity_comparison_and_diagnosis(self):
        feedback = {
            "items": [
                {
                    "id": 1,
                    "created_at": "2026-06-09T12:00:00+00:00",
                    "type": "analysis_issue",
                    "message": "Server prefix is wrong",
                    "source_type": "repo",
                    "dag_id": "sample_dag",
                    "repo_name": "analytics",
                    "dag_path": "dags/sample.py",
                    "repo_commit": "abc123",
                    "attachments": [
                        {"kind": "dag_source", "filename": "dag_source__sample.py"},
                        {"kind": "generated_omentity", "filename": "generated_omentity.py"},
                        {"kind": "difference", "filename": "difference.md"},
                        {"kind": "metadata", "filename": "metadata.json"},
                    ],
                }
            ]
        }
        dag_source = """
task = PythonOperator(
    task_id="load_sales",
    inlets=[OMEntity(entity=Entity.TABLE, fqn="dm13.raw.sales")],
    outlets=[OMEntity(entity=Entity.TABLE, fqn="dm13.dm.sales")]
)
"""
        generated = """
# =================================================================
# Task: load_sales
# Function: load_sales
# Connection: do-ch13 (from decorator)
# =================================================================

inlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.raw.sales"),
],
outlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.dm.sales"),
]
"""

        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            feedback_dir = run_dir / "attachments" / "feedback-000001"
            feedback_dir.mkdir(parents=True)
            (run_dir / "feedback.json").write_text(json.dumps(feedback), encoding="utf-8")
            (feedback_dir / "dag_source__sample.py").write_text(dag_source, encoding="utf-8")
            (feedback_dir / "generated_omentity.py").write_text(generated, encoding="utf-8")
            (feedback_dir / "difference.md").write_text("# MISMATCH\n", encoding="utf-8")
            (feedback_dir / "metadata.json").write_text('{"analysis_summary": {"output_count": 1}}', encoding="utf-8")

            triage = load_module()
            review_path = triage.analyze_feedback_run(run_dir)
            review = review_path.read_text(encoding="utf-8")

        self.assertIn("Server prefix is wrong", review)
        self.assertIn("load_sales", review)
        self.assertIn("dm13.raw.sales", review)
        self.assertIn("do-ch13.raw.sales", review)
        self.assertIn("Вероятная причина: server mapping", review)
        self.assertIn("config/server_mapping.yaml", review)

    def test_analyze_diagnoses_cross_server_schema_when_last_two_fqn_parts_match(self):
        feedback = {
            "items": [
                {
                    "id": 1,
                    "created_at": "2026-06-09T12:00:00+00:00",
                    "type": "analysis_issue",
                    "message": "analytics_4 is from trino, not ch9",
                    "source_type": "repo",
                    "dag_id": "sample_dag",
                    "repo_name": "analytics",
                    "dag_path": "dags/sample.py",
                    "repo_commit": "abc123",
                    "attachments": [
                        {"kind": "dag_source", "filename": "dag_source__sample.py"},
                        {"kind": "generated_omentity", "filename": "generated_omentity.py"},
                    ],
                }
            ]
        }
        dag_source = """
task = PythonOperator(
    task_id="exchange",
    inlets=[OMEntity(entity=Entity.TABLE, fqn="{TRINO_CONN}.tracker_insights.analytics_4.entrypoints")],
    outlets=[]
)
"""
        generated = """
# =================================================================
# Task: exchange
# =================================================================

inlets=[
    OMEntity(entity=Entity.TABLE, fqn="click-do-ch9.analytics_4.entrypoints"),
],
outlets=[]
"""

        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            feedback_dir = run_dir / "attachments" / "feedback-000001"
            feedback_dir.mkdir(parents=True)
            (run_dir / "feedback.json").write_text(json.dumps(feedback), encoding="utf-8")
            (feedback_dir / "dag_source__sample.py").write_text(dag_source, encoding="utf-8")
            (feedback_dir / "generated_omentity.py").write_text(generated, encoding="utf-8")

            triage = load_module()
            review_path = triage.analyze_feedback_run(run_dir)
            review = review_path.read_text(encoding="utf-8")

        self.assertIn("cross-server source schema", review)
        self.assertIn("analytics_4.entrypoints", review)
        self.assertIn("click-do-ch9", review)


if __name__ == "__main__":
    unittest.main()
