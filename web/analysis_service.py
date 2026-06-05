"""Shared DAG analysis service for web UI."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from analyzer.dag_parser import DAGParser
from analyzer.models import SQLAnalysisResult
from analyzer.sql_analyzer import SQLAnalyzer
from generator.fqn_builder import FQNBuilder
from generator.omentity_generator import OMEntityGenerator
from test_against_samples import extract_existing_omentity
from visualizer.cytoscape_diagram import CytoscapeGraphBuilder
from visualizer.diagram import DataFlowDiagram


@dataclass
class DAGAnalysisRequest:
    dag_path: Path
    force_all_tasks: bool = True
    compare_existing: bool = True
    initial_view: str = "dag"


@dataclass
class DAGAnalysisResult:
    dag_id: str
    generated_text: str
    difference_text: str
    text_diagram: str
    graph_data: Dict[str, Any]
    warnings: List[str] = field(default_factory=list)
    task_count: int = 0
    output_count: int = 0


class DAGAnalysisService:
    """Runs the existing parser/analyzer/generator pipeline for UI consumers."""

    def __init__(
        self,
        project_root: Path,
        mapping_file: Path,
        runtime_dir: Optional[Path] = None,
    ):
        self.project_root = Path(project_root).resolve()
        self.mapping_file = Path(mapping_file)
        self.runtime_dir = Path(runtime_dir or self.project_root / ".runtime" / "uploads")

    def write_source_to_runtime_file(self, name: str, source: str) -> Path:
        safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)
        if not safe_name:
            safe_name = "uploaded_dag"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        path = self.runtime_dir / f"{safe_name}.py"
        path.write_text(source, encoding="utf-8")
        return path

    def analyze(self, request: DAGAnalysisRequest) -> DAGAnalysisResult:
        dag_parser = DAGParser()
        sql_analyzer = SQLAnalyzer()
        fqn_builder = FQNBuilder(str(self.mapping_file))
        generator = OMEntityGenerator(fqn_builder)

        dag_result = dag_parser.parse_file(str(request.dag_path))
        existing_omentity = extract_existing_omentity(str(request.dag_path)) if request.compare_existing else {}

        if request.force_all_tasks:
            for task in dag_result.tasks:
                task.has_omentity = False

        sql_results = {
            sql_var: sql_analyzer.analyze(sql_code)
            for sql_var, sql_code in dag_result.sql_variables.items()
        }

        func_sql_results = {}
        for func_name, func_info in dag_result.functions.items():
            combined = SQLAnalysisResult()
            for sql_var in func_info.sql_variables:
                if sql_var in sql_results:
                    combined = combined.merge(sql_results[sql_var])
            if combined.inlets or combined.outlets or combined.dictionaries:
                func_sql_results[func_name] = combined

        outputs = generator.generate_for_dag(dag_result, func_sql_results)
        dag_id = dag_result.dag_id or "Unknown"
        text_diagram = DataFlowDiagram().render_all(outputs) if outputs else ""
        graph_data: Dict[str, Any] = {}
        if outputs:
            graph_data = CytoscapeGraphBuilder().build_full_data(outputs, dag_id)
            graph_data["initial_view"] = request.initial_view

        return DAGAnalysisResult(
            dag_id=dag_id,
            generated_text=self._format_generated_text(outputs, dag_result, request.force_all_tasks),
            difference_text=self._format_difference_text(outputs, existing_omentity),
            text_diagram=text_diagram,
            graph_data=graph_data,
            warnings=list(dag_result.warnings),
            task_count=len(dag_result.tasks),
            output_count=len(outputs),
        )

    def _format_generated_text(self, outputs, dag_result, force_all_tasks: bool) -> str:
        lines = [
            "# " + "=" * 65,
            f"# DAG: {dag_result.dag_id or 'Unknown'}",
            f"# Mode: {'force all tasks' if force_all_tasks else 'missing OMEntity only'}",
            f"# Tasks processed: {len(outputs)}",
            "# " + "=" * 65,
            "",
        ]
        if not outputs:
            lines.append("# No tasks with SQL/API lineage were found")
            return "\n".join(lines)

        for output in outputs:
            lines.append(output.generated_code)
            lines.append("")
        return "\n".join(lines)

    def _format_difference_text(self, outputs, existing_omentity) -> str:
        if not existing_omentity:
            return "(comparison disabled or no existing OMEntity found)"

        lines: List[str] = []
        for output in outputs:
            existing = existing_omentity.get(output.task_id)
            lines.append("# " + "-" * 65)
            lines.append(f"# Task: {output.task_id}")
            if not existing or (not existing.inlets and not existing.outlets):
                lines.append("# (no existing OMEntity)")
                continue

            gen_inlet_fqns = {item.fqn for item in output.inlets}
            gen_outlet_fqns = {item.fqn for item in output.outlets}
            exist_inlet_fqns = {item.fqn for item in existing.inlets}
            exist_outlet_fqns = {item.fqn for item in existing.outlets}

            extra_inlets = gen_inlet_fqns - exist_inlet_fqns
            missing_inlets = exist_inlet_fqns - gen_inlet_fqns
            extra_outlets = gen_outlet_fqns - exist_outlet_fqns
            missing_outlets = exist_outlet_fqns - gen_outlet_fqns

            if not extra_inlets and not missing_inlets and not extra_outlets and not missing_outlets:
                lines.append("# MATCH")
                continue

            lines.append("# MISMATCH")
            for label, values in (
                ("Extra inlets", extra_inlets),
                ("Missing inlets", missing_inlets),
                ("Extra outlets", extra_outlets),
                ("Missing outlets", missing_outlets),
            ):
                if values:
                    lines.append(f"# {label}:")
                    for fqn in sorted(values):
                        lines.append(f"#   - {fqn}")
        return "\n".join(lines)
