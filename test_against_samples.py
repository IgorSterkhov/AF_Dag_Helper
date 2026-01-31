"""
Тестирование утилиты на DAG файлах с готовыми OMEntity.

Сравнивает сгенерированные OMEntity с существующими в DAG файлах.
Выводит различия и предлагает обновления маппингов.

Использование:
    python test_against_samples.py              # тест всех samples
    python test_against_samples.py dag.py       # тест одного файла
"""

import ast
import io
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple

# UTF-8 вывод для Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Добавляем корень проекта в path
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from analyzer.dag_parser import DAGParser
from analyzer.sql_analyzer import SQLAnalyzer
from analyzer.models import SQLAnalysisResult
from generator.fqn_builder import FQNBuilder
from generator.omentity_generator import OMEntityGenerator


@dataclass
class OMEntityInfo:
    """Информация об одном OMEntity."""
    entity_type: str  # TABLE, API
    fqn: str

    def __hash__(self):
        return hash((self.entity_type, self.fqn))

    def __eq__(self, other):
        if not isinstance(other, OMEntityInfo):
            return False
        return self.entity_type == other.entity_type and self.fqn == other.fqn


@dataclass
class TaskOMEntity:
    """OMEntity для одной задачи."""
    task_id: str
    inlets: List[OMEntityInfo] = field(default_factory=list)
    outlets: List[OMEntityInfo] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Результат сравнения для одной задачи."""
    task_id: str
    # Существующие в DAG
    existing_inlets: Set[OMEntityInfo] = field(default_factory=set)
    existing_outlets: Set[OMEntityInfo] = field(default_factory=set)
    # Сгенерированные утилитой
    generated_inlets: Set[OMEntityInfo] = field(default_factory=set)
    generated_outlets: Set[OMEntityInfo] = field(default_factory=set)

    @property
    def missing_inlets(self) -> Set[OMEntityInfo]:
        """Inlets которые есть в DAG, но не сгенерированы."""
        return self.existing_inlets - self.generated_inlets

    @property
    def extra_inlets(self) -> Set[OMEntityInfo]:
        """Inlets которые сгенерированы, но нет в DAG."""
        return self.generated_inlets - self.existing_inlets

    @property
    def missing_outlets(self) -> Set[OMEntityInfo]:
        """Outlets которые есть в DAG, но не сгенерированы."""
        return self.existing_outlets - self.generated_outlets

    @property
    def extra_outlets(self) -> Set[OMEntityInfo]:
        """Outlets которые сгенерированы, но нет в DAG."""
        return self.generated_outlets - self.existing_outlets

    @property
    def is_match(self) -> bool:
        """Полное совпадение."""
        return (not self.missing_inlets and not self.extra_inlets and
                not self.missing_outlets and not self.extra_outlets)


def extract_existing_omentity(dag_path: str) -> Dict[str, TaskOMEntity]:
    """
    Извлекает существующие OMEntity из DAG файла через AST.

    Returns:
        Dict[task_id, TaskOMEntity]
    """
    with open(dag_path, 'r', encoding='utf-8') as f:
        code = f.read()

    tree = ast.parse(code)
    tasks = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func_name = _get_call_name(node)
            if func_name in ('PythonOperator', 'ShortCircuitOperator'):
                task_info = _parse_operator_omentity(node)
                if task_info and task_info.task_id:
                    tasks[task_info.task_id] = task_info

    return tasks


def _get_call_name(call: ast.Call) -> str:
    """Получает имя вызываемой функции."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    elif isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ''


def _parse_operator_omentity(call: ast.Call) -> Optional[TaskOMEntity]:
    """Парсит OMEntity из вызова PythonOperator."""
    task = TaskOMEntity(task_id='')

    for keyword in call.keywords:
        if keyword.arg == 'task_id':
            if isinstance(keyword.value, ast.Constant):
                task.task_id = keyword.value.value

        elif keyword.arg == 'inlets':
            task.inlets = _extract_omentity_list(keyword.value)

        elif keyword.arg == 'outlets':
            task.outlets = _extract_omentity_list(keyword.value)

    return task


def _extract_omentity_list(node: ast.expr) -> List[OMEntityInfo]:
    """Извлекает список OMEntity из AST узла."""
    entities = []

    if isinstance(node, ast.List):
        for elem in node.elts:
            if isinstance(elem, ast.Call):
                entity = _parse_omentity_call(elem)
                if entity:
                    entities.append(entity)

    return entities


def _parse_omentity_call(call: ast.Call) -> Optional[OMEntityInfo]:
    """Парсит один вызов OMEntity(...)."""
    entity_type = None
    fqn = None

    for keyword in call.keywords:
        if keyword.arg == 'entity':
            # Entity.TABLE или Entity.API
            if isinstance(keyword.value, ast.Attribute):
                entity_type = keyword.value.attr

        elif keyword.arg == 'fqn':
            if isinstance(keyword.value, ast.Constant):
                fqn = keyword.value.value
            elif isinstance(keyword.value, ast.JoinedStr):
                # f-string
                fqn = _extract_fstring_value(keyword.value)

    if entity_type and fqn:
        return OMEntityInfo(entity_type=entity_type, fqn=fqn)
    return None


def _extract_fstring_value(node: ast.JoinedStr) -> str:
    """Извлекает значение из f-string."""
    parts = []
    for value in node.values:
        if isinstance(value, ast.Constant):
            parts.append(str(value.value))
        elif isinstance(value, ast.FormattedValue):
            # Пробуем получить значение переменной
            if isinstance(value.value, ast.Name):
                parts.append(f"{{{value.value.id}}}")
            else:
                parts.append('{...}')
    return ''.join(parts)


def parse_generated_omentity(generated_text: str) -> Dict[str, TaskOMEntity]:
    """
    Парсит сгенерированный текст OMEntity.

    Returns:
        Dict[task_id, TaskOMEntity]
    """
    tasks = {}
    current_task_id = None
    current_task = None
    in_inlets = False
    in_outlets = False

    for line in generated_text.split('\n'):
        line = line.strip()

        # Ищем Task: task_id
        if line.startswith('# Task:'):
            if current_task and current_task_id:
                tasks[current_task_id] = current_task
            current_task_id = line.replace('# Task:', '').strip()
            current_task = TaskOMEntity(task_id=current_task_id)
            in_inlets = False
            in_outlets = False

        # Начало inlets
        elif line.startswith('inlets=['):
            in_inlets = True
            in_outlets = False

        # Начало outlets
        elif line.startswith('outlets=['):
            in_outlets = True
            in_inlets = False

        # Конец списка
        elif line in ('],', ']'):
            if in_inlets:
                in_inlets = False
            elif in_outlets:
                in_outlets = False

        # OMEntity строка
        elif 'OMEntity(' in line and current_task:
            entity = _parse_omentity_line(line)
            if entity:
                if in_inlets:
                    current_task.inlets.append(entity)
                elif in_outlets:
                    current_task.outlets.append(entity)

    # Последняя задача
    if current_task and current_task_id:
        tasks[current_task_id] = current_task

    return tasks


def _parse_omentity_line(line: str) -> Optional[OMEntityInfo]:
    """Парсит строку вида OMEntity(entity=Entity.TABLE, fqn="...")."""
    # Entity type
    entity_match = re.search(r'Entity\.(\w+)', line)
    # FQN
    fqn_match = re.search(r'fqn="([^"]+)"', line)

    if entity_match and fqn_match:
        return OMEntityInfo(
            entity_type=entity_match.group(1),
            fqn=fqn_match.group(1)
        )
    return None


def compare_omentity(
    existing: Dict[str, TaskOMEntity],
    generated: Dict[str, TaskOMEntity]
) -> List[ComparisonResult]:
    """Сравнивает существующие и сгенерированные OMEntity."""
    results = []

    # Все task_id из обоих источников
    all_task_ids = set(existing.keys()) | set(generated.keys())

    for task_id in sorted(all_task_ids):
        result = ComparisonResult(task_id=task_id)

        if task_id in existing:
            result.existing_inlets = set(existing[task_id].inlets)
            result.existing_outlets = set(existing[task_id].outlets)

        if task_id in generated:
            result.generated_inlets = set(generated[task_id].inlets)
            result.generated_outlets = set(generated[task_id].outlets)

        results.append(result)

    return results


def suggest_mappings(results: List[ComparisonResult]) -> Dict[str, str]:
    """
    Анализирует различия и предлагает обновления маппингов.

    Ищет паттерны где FQN отличаются только сервером.
    """
    suggestions = {}

    for result in results:
        # Сравниваем extra (сгенерированные) с missing (ожидаемые)
        for extra in result.extra_inlets | result.extra_outlets:
            for missing in result.missing_inlets | result.missing_outlets:
                # Проверяем совпадение schema.table
                extra_parts = extra.fqn.split('.')
                missing_parts = missing.fqn.split('.')

                if len(extra_parts) >= 2 and len(missing_parts) >= 2:
                    # schema.table часть
                    extra_table = '.'.join(extra_parts[1:])
                    missing_table = '.'.join(missing_parts[1:])

                    if extra_table == missing_table:
                        # Серверы разные, таблицы одинаковые
                        extra_server = extra_parts[0]
                        missing_server = missing_parts[0]
                        if extra_server != missing_server:
                            suggestions[extra_server] = missing_server

    return suggestions


def format_report(dag_path: str, results: List[ComparisonResult], mappings: Dict[str, str]) -> str:
    """Форматирует отчёт о сравнении."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"DAG: {Path(dag_path).name}")
    lines.append("=" * 70)
    lines.append("")

    matches = sum(1 for r in results if r.is_match)
    total = len(results)

    lines.append(f"Задач проанализировано: {total}")
    lines.append(f"Полных совпадений: {matches}")
    lines.append(f"С расхождениями: {total - matches}")
    lines.append("")

    # Детали по каждой задаче
    for result in results:
        if result.is_match:
            lines.append(f"[OK] {result.task_id}")
        else:
            lines.append(f"[!!] {result.task_id} - РАСХОЖДЕНИЯ:")
            lines.append("")

            # СУЩЕСТВУЮЩИЙ (в DAG)
            lines.append("    СУЩЕСТВУЮЩИЙ (в DAG):")
            lines.append("      inlets:")
            if result.existing_inlets:
                for e in sorted(result.existing_inlets, key=lambda x: x.fqn):
                    marker = "  ← отсутствует в генерации" if e in result.missing_inlets else ""
                    lines.append(f"        {e.entity_type}: {e.fqn}{marker}")
            else:
                lines.append("        (пусто)")

            lines.append("      outlets:")
            if result.existing_outlets:
                for e in sorted(result.existing_outlets, key=lambda x: x.fqn):
                    marker = "  ← отсутствует в генерации" if e in result.missing_outlets else ""
                    lines.append(f"        {e.entity_type}: {e.fqn}{marker}")
            else:
                lines.append("        (пусто)")

            lines.append("")

            # СГЕНЕРИРОВАННЫЙ
            lines.append("    СГЕНЕРИРОВАННЫЙ:")
            lines.append("      inlets:")
            if result.generated_inlets:
                for e in sorted(result.generated_inlets, key=lambda x: x.fqn):
                    marker = "  ← лишний (нет в DAG)" if e in result.extra_inlets else ""
                    lines.append(f"        {e.entity_type}: {e.fqn}{marker}")
            else:
                lines.append("        (пусто)")

            lines.append("      outlets:")
            if result.generated_outlets:
                for e in sorted(result.generated_outlets, key=lambda x: x.fqn):
                    marker = "  ← лишний (нет в DAG)" if e in result.extra_outlets else ""
                    lines.append(f"        {e.entity_type}: {e.fqn}{marker}")
            else:
                lines.append("        (пусто)")

            # Специальный случай: задача не обработана
            if not result.generated_inlets and not result.generated_outlets:
                if result.existing_inlets or result.existing_outlets:
                    lines.append("")
                    lines.append("    [!] Задача не обработана утилитой (нет SQL или API)")

        lines.append("")

    # Предложения по маппингам
    if mappings:
        lines.append("-" * 70)
        lines.append("ПРЕДЛОЖЕНИЯ ПО МАППИНГАМ:")
        lines.append("-" * 70)
        for from_server, to_server in sorted(mappings.items()):
            lines.append(f"  {from_server} -> {to_server}")
        lines.append("")
        lines.append("Добавьте в config/server_mapping.yaml:")
        lines.append("```yaml")
        lines.append("server_mapping:")
        for from_server, to_server in sorted(mappings.items()):
            lines.append(f"  {from_server}: {to_server}")
        lines.append("```")

    return "\n".join(lines)


def analyze_dag_force(dag_path: str, mapping_file: Optional[str] = None) -> Dict[str, TaskOMEntity]:
    """
    Анализирует DAG файл ПРИНУДИТЕЛЬНО, игнорируя существующие OMEntity.

    Это нужно для тестирования - чтобы сравнить что утилита сгенерирует
    с тем что уже есть в DAG.
    """
    # Компоненты
    dag_parser = DAGParser()
    sql_analyzer = SQLAnalyzer()
    fqn_builder = FQNBuilder(mapping_file)
    generator = OMEntityGenerator(fqn_builder)

    # 1. Парсим DAG
    dag_result = dag_parser.parse_file(dag_path)

    # 2. Анализируем SQL
    sql_results: Dict[str, SQLAnalysisResult] = {}
    for sql_var, sql_code in dag_result.sql_variables.items():
        result = sql_analyzer.analyze(sql_code)
        sql_results[sql_var] = result

    # 3. Связываем функции с SQL
    func_sql_results: Dict[str, SQLAnalysisResult] = {}
    for func_name, func_info in dag_result.functions.items():
        combined = SQLAnalysisResult()
        for sql_var in func_info.sql_variables:
            if sql_var in sql_results:
                combined = combined.merge(sql_results[sql_var])
        if combined.inlets or combined.outlets or combined.dictionaries:
            func_sql_results[func_name] = combined

    # 4. Генерируем OMEntity для ВСЕХ задач (игнорируя has_omentity)
    generated_tasks = {}

    from analyzer.connection_resolver import ConnectionResolver
    resolver = ConnectionResolver(dag_result)

    for task in dag_result.tasks:
        func_name = task.python_callable
        if not func_name:
            continue

        conn_id, conn_source = resolver.resolve_for_function(func_name)
        if not conn_id:
            conn_id = "UNKNOWN"

        # Собираем SQL результаты
        sql_result = func_sql_results.get(func_name)
        if not sql_result:
            func_info = dag_result.functions.get(func_name)
            if func_info:
                combined = SQLAnalysisResult()
                for sql_var in func_info.sql_variables:
                    if sql_var in sql_results:
                        combined = combined.merge(sql_results[sql_var])
                if combined.inlets or combined.outlets or combined.dictionaries:
                    sql_result = combined

        # Проверяем наличие API или dst_table в op_kwargs
        has_api = bool(task.op_kwargs_api) or (func_info and func_info.api_connections)
        has_dst_table = bool(task.op_kwargs_dst_table)
        has_cross_server = func_info and func_info.cross_server_calls

        if not sql_result and not has_api and not has_dst_table and not has_cross_server:
            continue

        # Генерируем OMEntity
        task_omentity = TaskOMEntity(task_id=task.task_id)

        # API из op_kwargs
        if task.op_kwargs_api:
            for api_conn in task.op_kwargs_api:
                # Резолвим значение переменной
                if api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                task_omentity.inlets.append(OMEntityInfo(entity_type="API", fqn=api_fqn))

        # API из функции
        if func_info and func_info.api_connections:
            for api_conn in func_info.api_connections:
                if api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                inlet = OMEntityInfo(entity_type="API", fqn=api_fqn)
                if inlet not in task_omentity.inlets:
                    task_omentity.inlets.append(inlet)

        # dst_table из op_kwargs как outlet
        if task.op_kwargs_dst_table:
            dst_table = task.op_kwargs_dst_table
            # Резолвим переменную из string_variables
            if dst_table in dag_result.string_variables:
                dst_table = dag_result.string_variables[dst_table]
            # Парсим schema.table
            if '.' in dst_table:
                parts = dst_table.split('.')
                schema = parts[0]
                table = parts[1] if len(parts) > 1 else ''
                fqn = fqn_builder.build_fqn(conn_id, schema, table)
                task_omentity.outlets.append(OMEntityInfo(entity_type="TABLE", fqn=fqn))

        # Обрабатываем SQL результаты если есть
        if sql_result:
            # Inlets
            for table_ref in sql_result.inlets:
                if table_ref.is_remote:
                    fqn = fqn_builder.build_fqn_for_remote(
                        table_ref.remote_prefix, table_ref.schema, table_ref.table)
                else:
                    fqn = fqn_builder.build_fqn(conn_id, table_ref.schema, table_ref.table)
                task_omentity.inlets.append(OMEntityInfo(entity_type="TABLE", fqn=fqn))

            # Dictionaries -> inlets
            for dict_ref in sql_result.dictionaries:
                fqn = fqn_builder.build_fqn(conn_id, dict_ref.schema, dict_ref.table)
                task_omentity.inlets.append(OMEntityInfo(entity_type="TABLE", fqn=fqn))

            # Outlets
            for table_ref in sql_result.outlets:
                fqn = fqn_builder.build_fqn(conn_id, table_ref.schema, table_ref.table)
                task_omentity.outlets.append(OMEntityInfo(entity_type="TABLE", fqn=fqn))

        generated_tasks[task.task_id] = task_omentity

    return generated_tasks


def test_dag_file(dag_path: str, mapping_file: Optional[str] = None) -> Tuple[str, List[ComparisonResult], Dict[str, str]]:
    """
    Тестирует один DAG файл.

    Returns:
        (report, results, suggested_mappings)
    """
    # 1. Извлекаем существующие OMEntity из DAG
    existing = extract_existing_omentity(dag_path)

    # 2. Анализируем DAG принудительно (игнорируя существующие OMEntity)
    generated = analyze_dag_force(dag_path, mapping_file)

    # 3. Сравниваем
    results = compare_omentity(existing, generated)

    # 4. Предлагаем маппинги
    mappings = suggest_mappings(results)

    # 5. Формируем отчёт
    report = format_report(dag_path, results, mappings)

    return report, results, mappings


def test_all_samples(samples_dir: str = "Dags samples", mapping_file: Optional[str] = None) -> str:
    """Тестирует все DAG файлы в папке samples."""
    samples_path = ROOT_DIR / samples_dir

    if not samples_path.exists():
        return f"Папка {samples_dir} не найдена"

    dag_files = list(samples_path.glob("**/*.py"))
    if not dag_files:
        return f"В папке {samples_dir} нет .py файлов"

    all_reports = []
    all_mappings = {}
    total_tasks = 0
    total_matches = 0

    for dag_file in sorted(dag_files):
        report, results, mappings = test_dag_file(str(dag_file), mapping_file)
        all_reports.append(report)
        all_mappings.update(mappings)
        total_tasks += len(results)
        total_matches += sum(1 for r in results if r.is_match)

    # Итоговый отчёт
    summary = []
    summary.append("=" * 70)
    summary.append("ИТОГО")
    summary.append("=" * 70)
    summary.append(f"DAG файлов: {len(dag_files)}")
    summary.append(f"Задач всего: {total_tasks}")
    summary.append(f"Совпадений: {total_matches}")
    summary.append(f"Расхождений: {total_tasks - total_matches}")
    summary.append("")

    if all_mappings:
        summary.append("ВСЕ ПРЕДЛОЖЕННЫЕ МАППИНГИ:")
        summary.append("```yaml")
        summary.append("server_mapping:")
        for from_server, to_server in sorted(all_mappings.items()):
            summary.append(f"  {from_server}: {to_server}")
        summary.append("```")

    return "\n\n".join(all_reports) + "\n\n" + "\n".join(summary)


def main():
    """CLI точка входа."""
    mapping_file = str(ROOT_DIR / "config" / "server_mapping.yaml")

    if len(sys.argv) > 1:
        # Тест конкретного файла
        dag_path = sys.argv[1]
        report, _, _ = test_dag_file(dag_path, mapping_file)
        print(report)
    else:
        # Тест всех samples
        report = test_all_samples(mapping_file=mapping_file)
        print(report)


if __name__ == "__main__":
    main()
