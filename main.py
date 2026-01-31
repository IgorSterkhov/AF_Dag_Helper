"""Главный модуль для анализа DAG файлов."""

from pathlib import Path
from typing import Optional, Dict

from analyzer.dag_parser import DAGParser
from analyzer.sql_analyzer import SQLAnalyzer
from analyzer.models import SQLAnalysisResult
from generator.fqn_builder import FQNBuilder
from generator.omentity_generator import OMEntityGenerator


class DAGAnalyzer:
    """Главный класс для анализа DAG файлов."""

    def __init__(self, mapping_file: Optional[str] = None):
        self.dag_parser = DAGParser()
        self.sql_analyzer = SQLAnalyzer()
        self.fqn_builder = FQNBuilder(mapping_file)
        self.generator = OMEntityGenerator(self.fqn_builder)

    def analyze_dag(self, dag_path: str) -> str:
        """
        Анализирует DAG файл и возвращает сгенерированный код OMEntity.

        Args:
            dag_path: Путь к DAG файлу

        Returns:
            Строка с кодом OMEntity для всех задач
        """
        # 1. Парсим DAG файл
        dag_result = self.dag_parser.parse_file(dag_path)

        # 2. Анализируем SQL для каждой SQL переменной
        sql_results: Dict[str, SQLAnalysisResult] = {}

        for sql_var, sql_code in dag_result.sql_variables.items():
            result = self.sql_analyzer.analyze(sql_code)
            sql_results[sql_var] = result

        # 3. Связываем функции с их SQL результатами
        func_sql_results: Dict[str, SQLAnalysisResult] = {}

        for func_name, func_info in dag_result.functions.items():
            combined = SQLAnalysisResult()
            for sql_var in func_info.sql_variables:
                if sql_var in sql_results:
                    combined = combined.merge(sql_results[sql_var])
            if combined.inlets or combined.outlets or combined.dictionaries:
                func_sql_results[func_name] = combined

        # 4. Генерируем OMEntity
        outputs = self.generator.generate_for_dag(dag_result, func_sql_results)

        # 5. Форматируем итоговый вывод
        if not outputs:
            return self._format_no_results(dag_result)

        return self._format_output(outputs, dag_result)

    def _format_output(self, outputs, dag_result) -> str:
        """Форматирует список OMEntityOutput в строку."""
        lines = []

        # Заголовок файла
        lines.append("# " + "=" * 65)
        lines.append(f"# DAG: {dag_result.dag_id or 'Unknown'}")
        lines.append(f"# Tasks with missing OMEntity: {len(outputs)}")
        lines.append("# " + "=" * 65)
        lines.append("")

        # Добавляем код для каждой задачи
        for output in outputs:
            lines.append(output.generated_code)
            lines.append("")
            lines.append("")

        return "\n".join(lines)

    def _format_no_results(self, dag_result) -> str:
        """Форматирует вывод когда нет результатов."""
        lines = []
        lines.append("# " + "=" * 65)
        lines.append(f"# DAG: {dag_result.dag_id or 'Unknown'}")
        lines.append("# " + "=" * 65)
        lines.append("")

        if dag_result.warnings:
            lines.append("# Warnings:")
            for w in dag_result.warnings:
                lines.append(f"#   - {w}")
            lines.append("")

        # Проверяем почему нет результатов
        tasks_with_omentity = [t for t in dag_result.tasks if t.has_omentity]
        tasks_without_callable = [t for t in dag_result.tasks if not t.python_callable]

        if tasks_with_omentity:
            lines.append(f"# Tasks already have OMEntity: {len(tasks_with_omentity)}")
            for t in tasks_with_omentity:
                lines.append(f"#   - {t.task_id}")
            lines.append("")

        if tasks_without_callable:
            lines.append(f"# Tasks without python_callable: {len(tasks_without_callable)}")
            for t in tasks_without_callable:
                lines.append(f"#   - {t.task_id}")
            lines.append("")

        if not dag_result.sql_variables:
            lines.append("# No SQL variables found in DAG")
            lines.append("")

        if not dag_result.tasks:
            lines.append("# No PythonOperator tasks found in DAG")

        return "\n".join(lines)


def main():
    """CLI точка входа."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_dag.py> [mapping_file.yaml]")
        sys.exit(1)

    dag_path = sys.argv[1]
    mapping_file = sys.argv[2] if len(sys.argv) > 2 else None

    analyzer = DAGAnalyzer(mapping_file)
    result = analyzer.analyze_dag(dag_path)
    print(result)


if __name__ == "__main__":
    main()
