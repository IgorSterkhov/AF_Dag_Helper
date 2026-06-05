import tempfile
import unittest
from pathlib import Path

from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAPPING_FILE = PROJECT_ROOT / "config" / "server_mapping.yaml"


class DAGAnalysisServiceTest(unittest.TestCase):
    def test_analyzes_sample_dag_with_graph_data(self):
        service = DAGAnalysisService(PROJECT_ROOT, MAPPING_FILE)
        request = DAGAnalysisRequest(
            dag_path=PROJECT_ROOT / "Dags samples" / "api_ch3_dict_sc_suppliers.py",
            force_all_tasks=True,
            compare_existing=True,
            initial_view="dag",
        )

        result = service.analyze(request)

        self.assertEqual(result.dag_id, "api_ch3_dict_sc_suppliers")
        self.assertIn("task_update_supplier_office_links", result.generated_text)
        self.assertIn("MATCH", result.difference_text)
        self.assertIn("dag_view", result.graph_data)
        self.assertGreater(result.output_count, 0)

    def test_writes_source_to_runtime_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime_dir = Path(tmp)
            service = DAGAnalysisService(PROJECT_ROOT, MAPPING_FILE, runtime_dir=runtime_dir)

            dag_path = service.write_source_to_runtime_file(
                "sample_runtime",
                "from airflow.models import DAG\n",
            )

            self.assertTrue(dag_path.exists())
            self.assertEqual(dag_path.suffix, ".py")
            self.assertTrue(dag_path.read_text(encoding="utf-8").startswith("from airflow"))
            self.assertTrue(dag_path.resolve().is_relative_to(runtime_dir.resolve()))


if __name__ == "__main__":
    unittest.main()
