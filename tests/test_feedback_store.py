import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from web.feedback_store import AnalysisFeedbackContext, FeedbackStore


class FeedbackStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def sample_context(self, warnings_text: str = "") -> AnalysisFeedbackContext:
        return AnalysisFeedbackContext(
            source_type="repo",
            dag_id="daily_sales",
            repo_name="analytics",
            dag_path="dags/daily_sales.py",
            repo_commit="abc123",
            original_filename=None,
            source_text="from airflow import DAG\n",
            analysis_options={"force_all_tasks": True, "compare_existing": True, "initial_view": "dag"},
            analysis_summary={"task_count": 2, "output_count": 1, "warnings_count": 1 if warnings_text else 0},
            generated_text="inlets=[]\noutlets=[]\n",
            difference_text="# MATCH\n",
            warnings_text=warnings_text,
        )

    def test_global_feedback_saves_without_attachments(self):
        store = FeedbackStore(self.root)

        record = store.create_global_feedback("Please add CSV export")

        self.assertEqual(record["type"], "global")
        self.assertEqual(record["status"], "new")
        self.assertEqual(record["message"], "Please add CSV export")
        self.assertEqual(record["attachments"], [])
        self.assertTrue((self.root / "feedback.sqlite3").exists())

    def test_empty_global_feedback_is_rejected(self):
        store = FeedbackStore(self.root)

        with self.assertRaises(ValueError):
            store.create_global_feedback("  ")

    def test_analysis_issue_feedback_saves_required_attachments(self):
        store = FeedbackStore(self.root)

        record = store.create_analysis_issue_feedback("Wrong outlet", self.sample_context())

        kinds = {attachment["kind"] for attachment in record["attachments"]}
        self.assertEqual(kinds, {"dag_source", "generated_omentity", "difference", "metadata"})
        self.assertEqual(record["type"], "analysis_issue")
        self.assertEqual(record["source_type"], "repo")
        self.assertEqual(record["repo_name"], "analytics")
        self.assertEqual(record["repo_commit"], "abc123")
        self.assertTrue(all(attachment["sha256"] for attachment in record["attachments"]))
        self.assertTrue(all(attachment["size_bytes"] > 0 for attachment in record["attachments"]))

        metadata_attachment = next(attachment for attachment in record["attachments"] if attachment["kind"] == "metadata")
        metadata_path = self.root / metadata_attachment["relative_path"]
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        self.assertEqual(metadata["dag_id"], "daily_sales")
        self.assertEqual(metadata["analysis_summary"]["output_count"], 1)

    def test_analysis_issue_feedback_adds_warnings_attachment_when_present(self):
        store = FeedbackStore(self.root)

        record = store.create_analysis_issue_feedback("Parser warning looks suspicious", self.sample_context("ambiguous SQL"))

        self.assertIn("warnings", {attachment["kind"] for attachment in record["attachments"]})

    def test_new_mode_filters_and_mark_exported_updates_only_returned_rows(self):
        store = FeedbackStore(self.root)
        global_record = store.create_global_feedback("General")
        issue_record = store.create_analysis_issue_feedback("Wrong DAG", self.sample_context())

        new_issues = store.list_feedback("analysis_issue", mode="new")
        store.mark_exported([record["id"] for record in new_issues])

        self.assertEqual([record["id"] for record in new_issues], [issue_record["id"]])
        self.assertEqual(store.get_feedback(issue_record["id"])["status"], "exported")
        self.assertEqual(store.get_feedback(global_record["id"])["status"], "new")

    def test_dag_issue_archive_contains_manifest_and_attachments(self):
        store = FeedbackStore(self.root)
        issue = store.create_analysis_issue_feedback("Wrong DAG", self.sample_context())

        archive_bytes = store.build_dag_issue_archive([issue])

        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
            names = archive.getnames()
            manifest = json.loads(archive.extractfile("feedback.json").read().decode("utf-8"))

        self.assertIn("feedback.json", names)
        self.assertIn("attachments/feedback-000001/generated_omentity.py", names)
        self.assertEqual(manifest["items"][0]["id"], issue["id"])

    def test_invalid_mode_is_rejected(self):
        store = FeedbackStore(self.root)

        with self.assertRaises(ValueError):
            store.list_feedback("analysis_issue", mode="recent")


if __name__ == "__main__":
    unittest.main()
