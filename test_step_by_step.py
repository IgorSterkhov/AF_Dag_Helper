"""
Пошаговое тестирование DAG с детальным анализом расхождений.

Сравнивает сгенерированные OMEntity с существующими и показывает
полные фрагменты кода для анализа причин расхождений.

Использование:
    python test_step_by_step.py              # показать список файлов
    python test_step_by_step.py 0            # тест файла #0
    python test_step_by_step.py dag.py       # тест по имени
"""

import ast
import io
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple

# UTF-8 вывод для Windows - обрабатывается через PYTHONIOENCODING
# (закомментировано т.к. может вызывать проблемы в некоторых средах)

# Добавляем корень проекта в path
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from test_against_samples import (
    OMEntityInfo, TaskOMEntity, ComparisonResult,
    extract_existing_omentity, analyze_dag_force, compare_omentity,
    suggest_mappings, normalize_fqn, load_reference_corrections
)
from analyzer.dag_parser import DAGParser


SAMPLES_DIR = ROOT_DIR / "Dags samples"
REFERENCE_DIR = ROOT_DIR / "Base docs" / "reference dags"


@dataclass
class CodeSnippet:
    """Фрагмент кода с метаданными."""
    label: str
    code: str
    start_line: int
    end_line: int


@dataclass
class TaskCodeContext:
    """Контекст кода для одной задачи."""
    task_id: str
    function_name: Optional[str] = None
    function_snippet: Optional[CodeSnippet] = None
    task_definition: Optional[CodeSnippet] = None
    sql_variables: Dict[str, CodeSnippet] = field(default_factory=dict)
    connection_variables: Dict[str, CodeSnippet] = field(default_factory=dict)
    api_variables: Dict[str, CodeSnippet] = field(default_factory=dict)


def get_dag_files_list(use_reference: bool = False) -> List[Tuple[int, Path]]:
    """Возвращает нумерованный список DAG файлов."""
    target_dir = REFERENCE_DIR if use_reference else SAMPLES_DIR
    if not target_dir.exists():
        return []

    dag_files = sorted(target_dir.glob("**/*.py"))
    return list(enumerate(dag_files))


def print_files_list(use_reference: bool = False):
    """Выводит нумерованный список файлов."""
    files = get_dag_files_list(use_reference)
    target_dir = REFERENCE_DIR if use_reference else SAMPLES_DIR
    if not files:
        print(f"Папка {target_dir} не найдена или пуста")
        return

    label = "REFERENCE DAGS" if use_reference else "DAG ФАЙЛЫ ДЛЯ ТЕСТИРОВАНИЯ"
    print("=" * 70)
    print(label)
    print("=" * 70)
    print()

    for idx, path in files:
        print(f"  [{idx:2d}] {path.name}")

    print()
    print("-" * 70)
    prefix = "ref" if use_reference else ""
    print(f"Использование: python test_step_by_step.py {prefix} <номер>".strip())
    print(f"Пример: python test_step_by_step.py {prefix} 0".strip())
    print("-" * 70)


def find_dag_file(arg: str, use_reference: bool = False) -> Optional[Path]:
    """Находит DAG файл по номеру или имени."""
    files = get_dag_files_list(use_reference)

    # Попытка как число
    try:
        idx = int(arg)
        if 0 <= idx < len(files):
            return files[idx][1]
        else:
            print(f"Ошибка: номер {idx} вне диапазона [0-{len(files)-1}]")
            return None
    except ValueError:
        pass

    # Поиск по имени
    for _, path in files:
        if arg in path.name or path.name == arg:
            return path

    print(f"Файл не найден: {arg}")
    return None


def read_file_lines(path: Path) -> List[str]:
    """Читает файл и возвращает список строк."""
    with open(path, 'r', encoding='utf-8') as f:
        return f.readlines()


def extract_lines(lines: List[str], start: int, end: int) -> str:
    """Извлекает строки [start:end+1] (1-based)."""
    # Конвертируем в 0-based
    start_idx = max(0, start - 1)
    end_idx = min(len(lines), end)
    return ''.join(lines[start_idx:end_idx])


def find_node_lines(node: ast.AST) -> Tuple[int, int]:
    """Возвращает (start_line, end_line) для AST узла."""
    start = node.lineno
    end = node.end_lineno if hasattr(node, 'end_lineno') and node.end_lineno else start
    return start, end


def extract_code_contexts(dag_path: Path) -> Dict[str, TaskCodeContext]:
    """
    Извлекает контекст кода для всех задач в DAG.
    """
    lines = read_file_lines(dag_path)

    with open(dag_path, 'r', encoding='utf-8') as f:
        code = f.read()

    tree = ast.parse(code)
    contexts: Dict[str, TaskCodeContext] = {}

    # Собираем информацию о функциях
    functions: Dict[str, Tuple[int, int, ast.FunctionDef]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            start, end = find_node_lines(node)
            # Учитываем декораторы
            if node.decorator_list:
                start = min(d.lineno for d in node.decorator_list)
            functions[node.name] = (start, end, node)

    # Собираем переменные верхнего уровня
    variables: Dict[str, Tuple[int, int, str]] = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    start, end = find_node_lines(node)
                    var_code = extract_lines(lines, start, end)
                    variables[target.id] = (start, end, var_code)

    # Находим все PythonOperator
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func_name = _get_call_name(node)
            if func_name in ('PythonOperator', 'ShortCircuitOperator'):
                task_info = _parse_task_call(node, lines, functions, variables)
                if task_info:
                    contexts[task_info.task_id] = task_info

    return contexts


def _get_call_name(call: ast.Call) -> str:
    """Получает имя вызываемой функции."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    elif isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ''


def _parse_task_call(
    call: ast.Call,
    lines: List[str],
    functions: Dict[str, Tuple[int, int, ast.FunctionDef]],
    variables: Dict[str, Tuple[int, int, str]]
) -> Optional[TaskCodeContext]:
    """Парсит вызов PythonOperator и собирает контекст."""
    task_id = None
    python_callable = None
    op_kwargs_vars = []

    for keyword in call.keywords:
        if keyword.arg == 'task_id':
            if isinstance(keyword.value, ast.Constant):
                task_id = keyword.value.value

        elif keyword.arg == 'python_callable':
            if isinstance(keyword.value, ast.Name):
                python_callable = keyword.value.id

        elif keyword.arg == 'op_kwargs':
            # Собираем переменные из op_kwargs
            if isinstance(keyword.value, ast.Call):
                # dict(...)
                for kw in keyword.value.keywords:
                    if isinstance(kw.value, ast.Name):
                        op_kwargs_vars.append(kw.value.id)
            elif isinstance(keyword.value, ast.Dict):
                # {...}
                for val in keyword.value.values:
                    if isinstance(val, ast.Name):
                        op_kwargs_vars.append(val.id)

    if not task_id:
        return None

    ctx = TaskCodeContext(task_id=task_id, function_name=python_callable)

    # Код определения задачи
    start, end = find_node_lines(call)
    ctx.task_definition = CodeSnippet(
        label="Определение задачи",
        code=extract_lines(lines, start, end),
        start_line=start,
        end_line=end
    )

    # Код функции
    if python_callable and python_callable in functions:
        func_start, func_end, func_node = functions[python_callable]
        ctx.function_snippet = CodeSnippet(
            label=f"Функция {python_callable}",
            code=extract_lines(lines, func_start, func_end),
            start_line=func_start,
            end_line=func_end
        )

    # Переменные из op_kwargs
    for var_name in op_kwargs_vars:
        if var_name in variables:
            var_start, var_end, var_code = variables[var_name]

            # Классифицируем переменную
            if 'CONN' in var_name or 'conn' in var_name.lower():
                if 'API' in var_name or 'api' in var_name.lower():
                    ctx.api_variables[var_name] = CodeSnippet(
                        label=f"API connection: {var_name}",
                        code=var_code,
                        start_line=var_start,
                        end_line=var_end
                    )
                else:
                    ctx.connection_variables[var_name] = CodeSnippet(
                        label=f"Connection: {var_name}",
                        code=var_code,
                        start_line=var_start,
                        end_line=var_end
                    )
            elif 'SQL' in var_name or 'QUERY' in var_name:
                ctx.sql_variables[var_name] = CodeSnippet(
                    label=f"SQL: {var_name}",
                    code=var_code,
                    start_line=var_start,
                    end_line=var_end
                )

    # Ищем SQL переменные используемые в функции
    if python_callable and python_callable in functions:
        _, _, func_node = functions[python_callable]
        for node in ast.walk(func_node):
            if isinstance(node, ast.Name) and node.id in variables:
                var_name = node.id
                if var_name not in ctx.sql_variables:
                    var_start, var_end, var_code = variables[var_name]
                    # Проверяем что это SQL
                    if any(kw in var_code.upper() for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM']):
                        ctx.sql_variables[var_name] = CodeSnippet(
                            label=f"SQL: {var_name}",
                            code=var_code,
                            start_line=var_start,
                            end_line=var_end
                        )

    # Connection переменные из декоратора @with_db
    if python_callable and python_callable in functions:
        _, _, func_node = functions[python_callable]
        for dec in func_node.decorator_list:
            if isinstance(dec, ast.Call):
                if isinstance(dec.func, ast.Name) and dec.func.id == 'with_db':
                    for arg in dec.args:
                        if isinstance(arg, ast.Name) and arg.id in variables:
                            var_name = arg.id
                            if var_name not in ctx.connection_variables:
                                var_start, var_end, var_code = variables[var_name]
                                ctx.connection_variables[var_name] = CodeSnippet(
                                    label=f"Connection (from @with_db): {var_name}",
                                    code=var_code,
                                    start_line=var_start,
                                    end_line=var_end
                                )

    return ctx


def format_code_block(snippet: CodeSnippet, max_lines: int = 20) -> str:
    """Форматирует блок кода с номерами строк."""
    lines_out = []
    lines_out.append(f"  КОД: {snippet.label}")
    lines_out.append("  ┌" + "─" * 55)

    code_lines = snippet.code.rstrip('\n').split('\n')
    line_num = snippet.start_line

    shown = 0
    for line in code_lines:
        if shown >= max_lines:
            remaining = len(code_lines) - shown
            lines_out.append(f"  │     ... ещё {remaining} строк ...")
            break
        lines_out.append(f"  │ {line_num:3d}:  {line.rstrip()}")
        line_num += 1
        shown += 1

    lines_out.append("  └" + "─" * 55)
    return '\n'.join(lines_out)


def analyze_discrepancy(
    result: ComparisonResult,
    ctx: Optional[TaskCodeContext],
    existing: TaskOMEntity,
    generated: TaskOMEntity
) -> List[str]:
    """Анализирует расхождение и возвращает список строк анализа."""
    lines = []

    # Отсутствующие inlets (есть в DAG, нет в генерации)
    for entity in result.missing_inlets:
        lines.append("")
        lines.append(f"▸ АНАЛИЗ: Отсутствует inlet [{entity.entity_type}] {entity.fqn}")
        lines.append("")

        if entity.entity_type == "API":
            lines.append("  ПРИЧИНА: API connection не детектирован")
            lines.append("")

            # Показываем API переменные
            if ctx and ctx.api_variables:
                for var_name, snippet in ctx.api_variables.items():
                    lines.append(format_code_block(snippet))
                    lines.append("")

            # Показываем функцию если есть HttpHook
            if ctx and ctx.function_snippet:
                if 'HttpHook' in ctx.function_snippet.code:
                    lines.append(format_code_block(ctx.function_snippet))
                    lines.append("")

            # Показываем op_kwargs
            if ctx and ctx.task_definition:
                if 'op_kwargs' in ctx.task_definition.code:
                    lines.append(format_code_block(ctx.task_definition))
                    lines.append("")

            lines.append("  ВЫВОД: Нужно резолвить API connection через op_kwargs или параметры функции")

        elif entity.entity_type == "TABLE":
            lines.append("  ПРИЧИНА: Таблица не найдена в SQL или bulk_dump")
            lines.append("")

            # Показываем SQL переменные
            if ctx and ctx.sql_variables:
                for var_name, snippet in ctx.sql_variables.items():
                    lines.append(format_code_block(snippet))
                    lines.append("")

            # Показываем функцию
            if ctx and ctx.function_snippet:
                lines.append(format_code_block(ctx.function_snippet))
                lines.append("")

            lines.append("  ВЫВОД: Проверить SQL запросы и bulk_dump вызовы")

    # Лишние inlets (есть в генерации, нет в DAG)
    for entity in result.extra_inlets:
        lines.append("")
        lines.append(f"▸ АНАЛИЗ: Лишний inlet [{entity.entity_type}] {entity.fqn}")
        lines.append("")
        lines.append("  ПРИЧИНА: Сгенерировано но отсутствует в эталоне")
        lines.append("  ВАРИАНТЫ:")
        lines.append("    1. Эталон неполный - нужно добавить в DAG")
        lines.append("    2. Ложное срабатывание - утилита неправильно интерпретирует код")

        if ctx and ctx.sql_variables:
            lines.append("")
            for var_name, snippet in ctx.sql_variables.items():
                lines.append(format_code_block(snippet))

    # Отсутствующие outlets
    for entity in result.missing_outlets:
        lines.append("")
        lines.append(f"▸ АНАЛИЗ: Отсутствует outlet [{entity.entity_type}] {entity.fqn}")
        lines.append("")
        lines.append("  ПРИЧИНА: INSERT/bulk_dump не детектирован")

        if ctx and ctx.function_snippet:
            lines.append("")
            lines.append(format_code_block(ctx.function_snippet))

        if ctx and ctx.task_definition and 'dst_table' in ctx.task_definition.code:
            lines.append("")
            lines.append(format_code_block(ctx.task_definition))

    # Лишние outlets
    for entity in result.extra_outlets:
        lines.append("")
        lines.append(f"▸ АНАЛИЗ: Лишний outlet [{entity.entity_type}] {entity.fqn}")
        lines.append("")
        lines.append("  ПРИЧИНА: Сгенерировано но отсутствует в эталоне")

        if ctx and ctx.sql_variables:
            lines.append("")
            for var_name, snippet in ctx.sql_variables.items():
                if 'INSERT' in snippet.code.upper():
                    lines.append(format_code_block(snippet))

    # Key mismatches
    if result.key_mismatches:
        lines.append("")
        lines.append("▸ АНАЛИЗ: Расхождения по key-группам")
        lines.append("")
        for entity_type, fqn, ex_key, gen_key in result.key_mismatches:
            ex_str = f"'{ex_key}'" if ex_key else "None"
            gen_str = f"'{gen_key}'" if gen_key else "None"
            lines.append(f"  [{entity_type}] {fqn}")
            lines.append(f"    ПРИЧИНА: Ключ группы не совпадает (ожидается {ex_str}, сгенерировано {gen_str})")

    return lines


def format_entity_list(entities: Set[OMEntityInfo], missing: Set[OMEntityInfo], label: str) -> List[str]:
    """Форматирует список entity с маркерами."""
    lines = []
    if entities:
        for e in sorted(entities, key=lambda x: x.fqn):
            marker = "  <- НЕ СГЕНЕРИРОВАНО" if e in missing else ""
            key_str = f"  key='{e.key}'" if e.key else ""
            lines.append(f"║   [{e.entity_type}] {e.fqn}{key_str}{marker}")
    else:
        lines.append("║   (пусто)")
    return lines


def format_detailed_report(
    dag_path: Path,
    file_idx: int,
    results: List[ComparisonResult],
    contexts: Dict[str, TaskCodeContext],
    existing: Dict[str, TaskOMEntity],
    generated: Dict[str, TaskOMEntity],
    mappings: Dict[str, str]
) -> Tuple[str, bool]:
    """
    Форматирует детальный отчёт.

    Returns:
        (report_text, has_discrepancy)
    """
    lines = []
    lines.append("═" * 70)
    lines.append(f"DAG: {dag_path.name}  [файл #{file_idx}]")
    lines.append("═" * 70)
    lines.append("")

    matches = sum(1 for r in results if r.is_match)
    total = len(results)
    has_discrepancy = matches < total

    lines.append(f"Задач: {total}  |  Совпадений: {matches}  |  Расхождений: {total - matches}")
    lines.append("")

    for result in results:
        if result.is_match:
            key_info = f"  (key mismatches: {len(result.key_mismatches)})" if result.key_mismatches else ""
            lines.append(f"[OK] {result.task_id}{key_info}")

            # Показываем key-группы даже для OK задач
            if result.key_mismatches:
                lines.append("")
                lines.append("    KEY-ГРУППЫ (расхождения):")
                for entity_type, fqn, ex_key, gen_key in result.key_mismatches:
                    ex_str = f"'{ex_key}'" if ex_key else "None"
                    gen_str = f"'{gen_key}'" if gen_key else "None"
                    lines.append(f"      {entity_type}: {fqn}")
                    lines.append(f"        reference: key={ex_str}  generated: key={gen_str}")
                lines.append("")
        else:
            lines.append(f"[!!] {result.task_id} - РАСХОЖДЕНИЕ")
            lines.append("")

            # Существующий
            lines.append("╔══ СУЩЕСТВУЮЩИЙ В DAG " + "═" * 47)
            lines.append("║ inlets:")
            lines.extend(format_entity_list(result.existing_inlets, result.missing_inlets, ""))
            lines.append("║ outlets:")
            lines.extend(format_entity_list(result.existing_outlets, result.missing_outlets, ""))
            lines.append("╚" + "═" * 69)
            lines.append("")

            # Сгенерированный
            lines.append("╔══ СГЕНЕРИРОВАННЫЙ " + "═" * 50)
            lines.append("║ inlets:")
            lines.extend(format_entity_list(result.generated_inlets, result.extra_inlets, ""))
            lines.append("║ outlets:")
            lines.extend(format_entity_list(result.generated_outlets, result.extra_outlets, ""))
            lines.append("╚" + "═" * 69)
            lines.append("")

            # Детальный анализ с кодом
            ctx = contexts.get(result.task_id)
            exist_task = existing.get(result.task_id, TaskOMEntity(task_id=result.task_id))
            gen_task = generated.get(result.task_id, TaskOMEntity(task_id=result.task_id))

            analysis = analyze_discrepancy(result, ctx, exist_task, gen_task)
            lines.extend(analysis)

            # Визуализация key-групп (если есть key в reference или generated)
            key_groups = _build_key_group_display(exist_task, gen_task)
            if key_groups:
                lines.append("")
                lines.extend(key_groups)
            lines.append("")

    # Предложения по маппингам
    if mappings:
        lines.append("")
        lines.append("-" * 70)
        lines.append("ПРЕДЛОЖЕНИЯ ПО МАППИНГАМ:")
        for from_server, to_server in sorted(mappings.items()):
            lines.append(f"  {from_server} -> {to_server}")
        lines.append("")
        lines.append("Добавьте в config/server_mapping.yaml:")
        lines.append("```yaml")
        for from_server, to_server in sorted(mappings.items()):
            lines.append(f"  {from_server}: {to_server}")
        lines.append("```")

    if has_discrepancy:
        lines.append("")
        lines.append("═" * 70)
        lines.append("ОСТАНОВЛЕНО: обнаружено расхождение")
        next_idx = file_idx + 1
        files = get_dag_files_list()
        if next_idx < len(files):
            lines.append(f"Для продолжения: python test_step_by_step.py {next_idx}")
        else:
            lines.append("Это был последний файл.")
        lines.append("═" * 70)

    return '\n'.join(lines), has_discrepancy


def _build_key_group_display(
    existing: TaskOMEntity, generated: TaskOMEntity
) -> List[str]:
    """Строит визуализацию key-групп для задачи."""
    lines = []

    # Собираем key-группы из reference
    ref_groups = _collect_key_groups(existing.inlets, existing.outlets)
    gen_groups = _collect_key_groups(generated.inlets, generated.outlets)

    if not ref_groups and not gen_groups:
        return []

    if ref_groups:
        lines.append("╔══ KEY-ГРУППЫ В REFERENCE " + "═" * 42)
        for key in sorted(ref_groups.keys(), key=lambda k: (k is None, k or '')):
            inlets, outlets = ref_groups[key]
            key_str = f"key='{key}'" if key else "(no key)"
            inlet_names = [_short_fqn(i.fqn) for i in inlets]
            outlet_names = [_short_fqn(o.fqn) for o in outlets]
            flow = ", ".join(inlet_names)
            if outlet_names:
                flow += " -> " + ", ".join(outlet_names)
            lines.append(f"║ {key_str}: {flow}")
        lines.append("╚" + "═" * 69)

    if gen_groups:
        lines.append("╔══ KEY-ГРУППЫ В ГЕНЕРАЦИИ " + "═" * 42)
        for key in sorted(gen_groups.keys(), key=lambda k: (k is None, k or '')):
            inlets, outlets = gen_groups[key]
            key_str = f"key='{key}'" if key else "(no key)"
            inlet_names = [_short_fqn(i.fqn) for i in inlets]
            outlet_names = [_short_fqn(o.fqn) for o in outlets]
            flow = ", ".join(inlet_names)
            if outlet_names:
                flow += " -> " + ", ".join(outlet_names)
            lines.append(f"║ {key_str}: {flow}")
        lines.append("╚" + "═" * 69)

    return lines


def _collect_key_groups(
    inlets: List[OMEntityInfo], outlets: List[OMEntityInfo]
) -> Dict[Optional[str], Tuple[List[OMEntityInfo], List[OMEntityInfo]]]:
    """Группирует entity по key."""
    groups: Dict[Optional[str], Tuple[List[OMEntityInfo], List[OMEntityInfo]]] = {}
    for item in inlets:
        if item.key not in groups:
            groups[item.key] = ([], [])
        groups[item.key][0].append(item)
    for item in outlets:
        if item.key not in groups:
            groups[item.key] = ([], [])
        groups[item.key][1].append(item)
    return groups


def _short_fqn(fqn: str) -> str:
    """Сокращает FQN: убирает серверный префикс."""
    parts = fqn.split('.')
    if len(parts) >= 3:
        return ".".join(parts[1:])
    return fqn


def test_single_file(dag_path: Path, file_idx: int) -> Tuple[str, bool]:
    """
    Тестирует один DAG файл.

    Returns:
        (report, has_discrepancy)
    """
    mapping_file = str(ROOT_DIR / "config" / "server_mapping.yaml")
    corrections_file = str(ROOT_DIR / "config" / "known_reference_errors.yaml")

    # 0. Загружаем коррекции reference
    corrections = load_reference_corrections(corrections_file)

    # 1. Извлекаем существующие OMEntity
    existing = extract_existing_omentity(str(dag_path))

    # 2. Генерируем OMEntity
    generated = analyze_dag_force(str(dag_path), mapping_file)

    # 3. Сравниваем (с учётом коррекций)
    results = compare_omentity(existing, generated, corrections)

    # 4. Предлагаем маппинги
    mappings = suggest_mappings(results)

    # 5. Извлекаем контекст кода
    contexts = extract_code_contexts(dag_path)

    # 6. Формируем отчёт
    report, has_discrepancy = format_detailed_report(
        dag_path, file_idx, results, contexts, existing, generated, mappings
    )

    return report, has_discrepancy


def main():
    """CLI точка входа."""
    use_reference = False
    args = sys.argv[1:]

    # Проверяем флаг ref/--ref/--reference
    if args and args[0] in ('ref', '--ref', '--reference'):
        use_reference = True
        args = args[1:]

    if not args:
        print_files_list(use_reference)
        return

    arg = args[0]
    dag_path = find_dag_file(arg, use_reference)

    if not dag_path:
        return

    # Находим индекс файла
    files = get_dag_files_list(use_reference)
    file_idx = next((i for i, p in files if p == dag_path), 0)

    report, has_discrepancy = test_single_file(dag_path, file_idx)
    print(report)


if __name__ == "__main__":
    main()
