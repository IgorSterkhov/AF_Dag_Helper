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
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple, Pattern

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


def normalize_fqn(fqn: str) -> str:
    """
    Нормализует FQN для сравнения.
    Удаляет суффикс _d из имени таблицы.

    Пример: do-ch6.buffer.na_boxes_shk_d -> do-ch6.buffer.na_boxes_shk
    """
    parts = fqn.rsplit('.', 1)  # Разделяем на prefix и table
    if len(parts) == 2:
        prefix, table = parts
        if table.endswith('_d'):
            table = table[:-2]
        return f"{prefix}.{table}"
    return fqn


@dataclass
class OMEntityInfo:
    """Информация об одном OMEntity."""
    entity_type: str  # TABLE, API, TOPIC
    fqn: str
    key: Optional[str] = None

    def __hash__(self):
        return hash((self.entity_type, normalize_fqn(self.fqn)))

    def __eq__(self, other):
        if not isinstance(other, OMEntityInfo):
            return False
        return (self.entity_type == other.entity_type and
                normalize_fqn(self.fqn) == normalize_fqn(other.fqn))


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
    # Key-группы (информационное сравнение)
    key_mismatches: List[Tuple[str, str, Optional[str], Optional[str]]] = field(default_factory=list)
    # [(entity_type, fqn, existing_key, generated_key), ...]
    # Коррекции и подозрительные FQN
    applied_corrections: List['CorrectionInfo'] = field(default_factory=list)
    ignored_fqns: List[str] = field(default_factory=list)
    suspicious_fqns: List['SuspiciousFQN'] = field(default_factory=list)

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


@dataclass
class CorrectionInfo:
    """Информация о применённой коррекции."""
    original_fqn: str
    corrected_fqn: Optional[str]  # None = игнорировать
    correction_type: str  # 'exact', 'pattern', 'ignored'
    pattern_used: Optional[str] = None


@dataclass
class SuspiciousFQN:
    """Подозрительный FQN (возможная ошибка в reference)."""
    fqn: str
    reason: str
    suggested_correction: Optional[str] = None


@dataclass
class ReferenceCorrections:
    """Загруженные коррекции из конфига."""
    exact_corrections: Dict[str, Optional[str]] = field(default_factory=dict)
    pattern_corrections: List[Tuple[Pattern, str]] = field(default_factory=list)
    enabled: bool = True
    verbose: bool = True


def load_reference_corrections(config_path: Optional[str] = None) -> ReferenceCorrections:
    """
    Загружает коррекции ссылок из YAML конфига.

    Возвращает пустые коррекции если файл не существует (graceful degradation).
    """
    if config_path is None:
        config_path = str(ROOT_DIR / "config" / "known_reference_errors.yaml")

    corrections = ReferenceCorrections()

    path = Path(config_path)
    if not path.exists():
        return corrections  # Graceful: возвращаем пустые коррекции

    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}

        ref_corrections = config.get('reference_corrections', {})

        # Загружаем точные коррекции
        corrections.exact_corrections = ref_corrections.get('exact', {}) or {}

        # Компилируем паттерны
        for pattern_info in ref_corrections.get('patterns', []) or []:
            pattern_str = pattern_info.get('pattern', '')
            replacement = pattern_info.get('replacement', '')
            if pattern_str:
                try:
                    compiled = re.compile(pattern_str)
                    corrections.pattern_corrections.append((compiled, replacement))
                except re.error:
                    pass  # Пропускаем невалидные паттерны

        # Загружаем настройки
        settings = config.get('settings', {})
        corrections.enabled = settings.get('enabled', True)
        corrections.verbose = settings.get('verbose', True)

    except Exception:
        pass  # Возвращаем дефолтные коррекции при любой ошибке

    return corrections


def apply_correction(
    fqn: str,
    corrections: ReferenceCorrections
) -> Tuple[str, Optional[CorrectionInfo]]:
    """
    Применяет коррекцию к FQN.

    Returns:
        (corrected_fqn, correction_info) - correction_info = None если коррекция не применена
        Если FQN должен быть игнорирован, возвращает (None, correction_info)
    """
    if not corrections.enabled:
        return fqn, None

    # 1. Проверяем точные коррекции
    if fqn in corrections.exact_corrections:
        corrected = corrections.exact_corrections[fqn]
        info = CorrectionInfo(
            original_fqn=fqn,
            corrected_fqn=corrected,
            correction_type='ignored' if corrected is None else 'exact'
        )
        return corrected, info

    # 2. Пробуем паттерны
    for pattern, replacement in corrections.pattern_corrections:
        if pattern.match(fqn):
            corrected = pattern.sub(replacement, fqn)
            if corrected != fqn:
                info = CorrectionInfo(
                    original_fqn=fqn,
                    corrected_fqn=corrected,
                    correction_type='pattern',
                    pattern_used=pattern.pattern
                )
                return corrected, info

    return fqn, None


def detect_suspicious_fqn(fqn: str) -> Optional[SuspiciousFQN]:
    """
    Определяет подозрительные FQN (возможные ошибки в reference).

    Детектируемые паттерны:
    - 4+ части (server.extra.schema.table)
    """
    parts = fqn.split('.')

    # Проверяем 4+ части (стандарт - server.schema.table = 3 части)
    if len(parts) >= 4:
        # Предлагаем коррекцию - убираем вторую часть
        suggested = f"{parts[0]}.{'.'.join(parts[2:])}"
        return SuspiciousFQN(
            fqn=fqn,
            reason=f"FQN имеет {len(parts)} частей (ожидается 3: server.schema.table)",
            suggested_correction=suggested
        )

    return None


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
    key = None

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

        elif keyword.arg == 'key':
            if isinstance(keyword.value, ast.Constant):
                key = str(keyword.value.value)

    if entity_type and fqn:
        return OMEntityInfo(entity_type=entity_type, fqn=fqn, key=key)
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
    """Парсит строку вида OMEntity(entity=Entity.TABLE, fqn="...", key="...")."""
    # Entity type
    entity_match = re.search(r'Entity\.(\w+)', line)
    # FQN
    fqn_match = re.search(r'fqn="([^"]+)"', line)
    # Key (optional)
    key_match = re.search(r'key="([^"]+)"', line)

    if entity_match and fqn_match:
        return OMEntityInfo(
            entity_type=entity_match.group(1),
            fqn=fqn_match.group(1),
            key=key_match.group(1) if key_match else None
        )
    return None


def compare_omentity(
    existing: Dict[str, TaskOMEntity],
    generated: Dict[str, TaskOMEntity],
    corrections: Optional[ReferenceCorrections] = None
) -> List[ComparisonResult]:
    """Сравнивает существующие и сгенерированные OMEntity с учётом коррекций."""
    results = []

    if corrections is None:
        corrections = ReferenceCorrections()

    # Все task_id из обоих источников
    all_task_ids = set(existing.keys()) | set(generated.keys())

    for task_id in sorted(all_task_ids):
        result = ComparisonResult(task_id=task_id)

        if task_id in existing:
            # Применяем коррекции к reference FQN (inlets)
            for entity in existing[task_id].inlets:
                corrected_fqn, correction_info = apply_correction(entity.fqn, corrections)

                if correction_info:
                    result.applied_corrections.append(correction_info)
                    if corrected_fqn is None:
                        result.ignored_fqns.append(entity.fqn)
                        continue  # Пропускаем игнорируемые

                # Детектируем подозрительные (только если нет коррекции)
                if not correction_info:
                    suspicious = detect_suspicious_fqn(entity.fqn)
                    if suspicious:
                        result.suspicious_fqns.append(suspicious)

                # Создаём скорректированный entity
                corrected_entity = OMEntityInfo(
                    entity_type=entity.entity_type,
                    fqn=corrected_fqn
                )
                result.existing_inlets.add(corrected_entity)

            # Применяем коррекции к reference FQN (outlets)
            for entity in existing[task_id].outlets:
                corrected_fqn, correction_info = apply_correction(entity.fqn, corrections)

                if correction_info:
                    result.applied_corrections.append(correction_info)
                    if corrected_fqn is None:
                        result.ignored_fqns.append(entity.fqn)
                        continue

                if not correction_info:
                    suspicious = detect_suspicious_fqn(entity.fqn)
                    if suspicious:
                        result.suspicious_fqns.append(suspicious)

                corrected_entity = OMEntityInfo(
                    entity_type=entity.entity_type,
                    fqn=corrected_fqn
                )
                result.existing_outlets.add(corrected_entity)

        if task_id in generated:
            result.generated_inlets = set(generated[task_id].inlets)
            result.generated_outlets = set(generated[task_id].outlets)

        # Сравнение key-групп (информационное)
        if task_id in existing and task_id in generated:
            _collect_key_mismatches(
                existing[task_id], generated[task_id], result
            )

        results.append(result)

    return results


def _collect_key_mismatches(
    existing_task: TaskOMEntity,
    generated_task: TaskOMEntity,
    result: ComparisonResult
) -> None:
    """Собирает расхождения по key между existing и generated."""
    # Строим lookup по (entity_type, normalized_fqn) -> key
    existing_keys = {}
    for item in existing_task.inlets + existing_task.outlets:
        norm_fqn = normalize_fqn(item.fqn)
        existing_keys[(item.entity_type, norm_fqn)] = item.key

    generated_keys = {}
    for item in generated_task.inlets + generated_task.outlets:
        norm_fqn = normalize_fqn(item.fqn)
        generated_keys[(item.entity_type, norm_fqn)] = item.key

    # Сравниваем ключи для совпадающих entity
    for ident, ex_key in existing_keys.items():
        if ident in generated_keys:
            gen_key = generated_keys[ident]
            if ex_key != gen_key:
                result.key_mismatches.append(
                    (ident[0], ident[1], ex_key, gen_key)
                )


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


def suggest_reference_corrections(results: List[ComparisonResult]) -> Dict[str, str]:
    """
    Анализирует расхождения и предлагает коррекции reference FQN.

    Ищет паттерны где:
    - Reference имеет больше частей FQN чем generated
    - schema.table совпадают, но reference имеет лишние компоненты
    """
    suggestions = {}

    for result in results:
        # Сравниваем missing (reference) с extra (generated)
        for missing in result.missing_inlets | result.missing_outlets:
            for extra in result.extra_inlets | result.extra_outlets:
                # Пропускаем разные типы entity
                if missing.entity_type != extra.entity_type:
                    continue

                missing_parts = missing.fqn.split('.')
                extra_parts = extra.fqn.split('.')

                # Случай 1: Reference имеет 4 части, generated - 3
                # и последние 2 части совпадают (schema.table)
                if len(missing_parts) == 4 and len(extra_parts) == 3:
                    missing_table = '.'.join(missing_parts[-2:])
                    extra_table = '.'.join(extra_parts[-2:])

                    if missing_table == extra_table:
                        # Reference имеет лишний компонент
                        suggestions[missing.fqn] = extra.fqn

                # Случай 2: Одинаковый сервер, одинаковая таблица, разная середина
                elif len(missing_parts) >= 3 and len(extra_parts) >= 3:
                    if (missing_parts[0] == extra_parts[0] and
                        missing_parts[-1] == extra_parts[-1]):
                        # Сервер и таблица совпадают, середина отличается
                        if len(missing_parts) > len(extra_parts):
                            suggestions[missing.fqn] = extra.fqn

        # Также предлагаем коррекции на основе подозрительных паттернов
        for suspicious in result.suspicious_fqns:
            if suspicious.suggested_correction:
                # Проверяем совпадает ли предложенная коррекция с чем-то сгенерированным
                for gen in result.generated_inlets | result.generated_outlets:
                    if normalize_fqn(gen.fqn) == normalize_fqn(suspicious.suggested_correction):
                        suggestions[suspicious.fqn] = suspicious.suggested_correction
                        break

    return suggestions


def format_report(
    dag_path: str,
    results: List[ComparisonResult],
    mappings: Dict[str, str],
    ref_corrections: Optional[Dict[str, str]] = None
) -> str:
    """Форматирует отчёт о сравнении."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"DAG: {Path(dag_path).name}")
    lines.append("=" * 70)
    lines.append("")

    matches = sum(1 for r in results if r.is_match)
    total = len(results)

    # Статистика по коррекциям
    total_corrections = sum(len(r.applied_corrections) for r in results)
    total_suspicious = sum(len(r.suspicious_fqns) for r in results)
    total_ignored = sum(len(r.ignored_fqns) for r in results)
    total_key_mismatches = sum(len(r.key_mismatches) for r in results)

    lines.append(f"Задач проанализировано: {total}")
    lines.append(f"Полных совпадений: {matches}")
    lines.append(f"С расхождениями: {total - matches}")
    if total_key_mismatches:
        lines.append(f"Расхождения по key-группам: {total_key_mismatches}")

    if total_corrections or total_suspicious or total_ignored:
        lines.append("")
        lines.append("Коррекции ссылок:")
        if total_corrections:
            lines.append(f"  Применено коррекций: {total_corrections}")
        if total_ignored:
            lines.append(f"  Пропущено (ignored): {total_ignored}")
        if total_suspicious:
            lines.append(f"  Подозрительных FQN: {total_suspicious}")

    lines.append("")

    # Детали по каждой задаче
    for result in results:
        # Определяем статус и суффикс
        has_extras = result.applied_corrections or result.suspicious_fqns
        suffix_parts = []
        if result.applied_corrections:
            suffix_parts.append(f"corrections: {len(result.applied_corrections)}")
        if result.suspicious_fqns:
            suffix_parts.append(f"suspicious: {len(result.suspicious_fqns)}")
        suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""

        if result.is_match:
            lines.append(f"[OK] {result.task_id}{suffix}")
            # Показываем коррекции даже для OK задач
            if result.applied_corrections:
                lines.append("")
                lines.append("    ПРИМЕНЕННЫЕ КОРРЕКЦИИ:")
                for corr in result.applied_corrections:
                    if corr.correction_type == 'ignored':
                        lines.append(f"      [SKIP] {corr.original_fqn}")
                    elif corr.correction_type == 'exact':
                        lines.append(f"      [EXACT] {corr.original_fqn}")
                        lines.append(f"           -> {corr.corrected_fqn}")
                    elif corr.correction_type == 'pattern':
                        lines.append(f"      [PATTERN] {corr.original_fqn}")
                        lines.append(f"             -> {corr.corrected_fqn}")
        else:
            lines.append(f"[!!] {result.task_id} - РАСХОЖДЕНИЯ:{suffix}")
            lines.append("")

            # Показываем примененные коррекции
            if result.applied_corrections:
                lines.append("    ПРИМЕНЕННЫЕ КОРРЕКЦИИ:")
                for corr in result.applied_corrections:
                    if corr.correction_type == 'ignored':
                        lines.append(f"      [SKIP] {corr.original_fqn}")
                    elif corr.correction_type == 'exact':
                        lines.append(f"      [EXACT] {corr.original_fqn}")
                        lines.append(f"           -> {corr.corrected_fqn}")
                    elif corr.correction_type == 'pattern':
                        lines.append(f"      [PATTERN] {corr.original_fqn}")
                        lines.append(f"             -> {corr.corrected_fqn}")
                lines.append("")

            # Показываем подозрительные FQN
            if result.suspicious_fqns:
                lines.append("    ПОДОЗРИТЕЛЬНЫЕ FQN (возможные ошибки в reference):")
                for susp in result.suspicious_fqns:
                    lines.append(f"      [?] {susp.fqn}")
                    lines.append(f"          Причина: {susp.reason}")
                    if susp.suggested_correction:
                        lines.append(f"          Возможно: {susp.suggested_correction}")
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

        # Key mismatches (показываем и для OK и для проблемных задач)
        if result.key_mismatches:
            lines.append("")
            lines.append("    KEY-ГРУППЫ (расхождения):")
            for entity_type, fqn, ex_key, gen_key in result.key_mismatches:
                ex_str = f"'{ex_key}'" if ex_key else "None"
                gen_str = f"'{gen_key}'" if gen_key else "None"
                lines.append(f"      {entity_type}: {fqn}")
                lines.append(f"        reference: key={ex_str}  generated: key={gen_str}")

        lines.append("")

    # Предложения по коррекциям reference
    if ref_corrections:
        lines.append("-" * 70)
        lines.append("ПРЕДЛОЖЕНИЯ ДЛЯ known_reference_errors.yaml:")
        lines.append("-" * 70)
        lines.append("")
        lines.append("Добавьте в config/known_reference_errors.yaml:")
        lines.append("```yaml")
        lines.append("reference_corrections:")
        lines.append("  exact:")
        for incorrect, correct in sorted(ref_corrections.items()):
            lines.append(f'    "{incorrect}": "{correct}"')
        lines.append("```")
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


def analyze_dag_force(dag_path: str, mapping_file: Optional[str] = None, include_dictget_as_inlets: bool = True) -> Dict[str, TaskOMEntity]:
    """
    Анализирует DAG файл ПРИНУДИТЕЛЬНО, игнорируя существующие OMEntity.

    Использует OMEntityGenerator напрямую, временно убирая флаг has_omentity
    у всех задач, чтобы генератор обработал их.

    Args:
        include_dictget_as_inlets: Включать ли dictGet таблицы как inlets (по умолчанию False)
    """
    dag_parser = DAGParser()
    sql_analyzer = SQLAnalyzer()
    fqn_builder = FQNBuilder(mapping_file)
    generator = OMEntityGenerator(fqn_builder, include_dictget_as_inlets=include_dictget_as_inlets)

    # 1. Парсим DAG
    dag_result = dag_parser.parse_file(dag_path)

    # 2. Временно убираем has_omentity чтобы генератор обработал все задачи
    original_flags = {}
    for task in dag_result.tasks:
        original_flags[task.task_id] = task.has_omentity
        task.has_omentity = False

    # 3. Анализируем SQL
    sql_results: Dict[str, SQLAnalysisResult] = {}
    for sql_var, sql_code in dag_result.sql_variables.items():
        result = sql_analyzer.analyze(sql_code)
        sql_results[sql_var] = result

    # 4. Связываем функции с SQL
    func_sql_results: Dict[str, SQLAnalysisResult] = {}
    for func_name, func_info in dag_result.functions.items():
        combined = SQLAnalysisResult()
        for sql_var in func_info.sql_variables:
            if sql_var in sql_results:
                combined = combined.merge(sql_results[sql_var])
        if combined.inlets or combined.outlets or combined.dictionaries:
            func_sql_results[func_name] = combined

    # 5. Генерируем через OMEntityGenerator
    outputs = generator.generate_for_dag(dag_result, func_sql_results)

    # 6. Восстанавливаем флаги
    for task in dag_result.tasks:
        task.has_omentity = original_flags.get(task.task_id, False)

    # 7. Конвертируем OMEntityOutput -> TaskOMEntity
    generated_tasks = {}
    for output in outputs:
        task_omentity = TaskOMEntity(task_id=output.task_id)
        for item in output.inlets:
            task_omentity.inlets.append(OMEntityInfo(
                entity_type=item.entity_type.value,
                fqn=item.fqn,
                key=item.key
            ))
        for item in output.outlets:
            task_omentity.outlets.append(OMEntityInfo(
                entity_type=item.entity_type.value,
                fqn=item.fqn,
                key=item.key
            ))
        generated_tasks[output.task_id] = task_omentity

    return generated_tasks


def test_dag_file(
    dag_path: str,
    mapping_file: Optional[str] = None,
    corrections_file: Optional[str] = None
) -> Tuple[str, List[ComparisonResult], Dict[str, str], Dict[str, str]]:
    """
    Тестирует один DAG файл.

    Returns:
        (report, results, suggested_mappings, suggested_ref_corrections)
    """
    # Загружаем коррекции reference
    corrections = load_reference_corrections(corrections_file)

    # 1. Извлекаем существующие OMEntity из DAG
    existing = extract_existing_omentity(dag_path)

    # 2. Анализируем DAG принудительно (игнорируя существующие OMEntity)
    generated = analyze_dag_force(dag_path, mapping_file)

    # 3. Сравниваем (с учётом коррекций)
    results = compare_omentity(existing, generated, corrections)

    # 4. Предлагаем маппинги серверов
    mappings = suggest_mappings(results)

    # 5. Предлагаем коррекции reference
    ref_corrections = suggest_reference_corrections(results)

    # 6. Формируем отчёт
    report = format_report(dag_path, results, mappings, ref_corrections)

    return report, results, mappings, ref_corrections


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
    all_ref_corrections = {}
    total_tasks = 0
    total_matches = 0

    for dag_file in sorted(dag_files):
        report, results, mappings, ref_corrections = test_dag_file(str(dag_file), mapping_file)
        all_reports.append(report)
        all_mappings.update(mappings)
        all_ref_corrections.update(ref_corrections)
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

    # Все предложенные коррекции reference
    if all_ref_corrections:
        summary.append("ВСЕ ПРЕДЛОЖЕННЫЕ КОРРЕКЦИИ REFERENCE:")
        summary.append("```yaml")
        summary.append("reference_corrections:")
        summary.append("  exact:")
        for incorrect, correct in sorted(all_ref_corrections.items()):
            summary.append(f'    "{incorrect}": "{correct}"')
        summary.append("```")
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
        arg = sys.argv[1]

        if arg in ('--ref', '--reference'):
            # Тест reference dags
            report = test_all_samples(
                samples_dir="Base docs/reference dags",
                mapping_file=mapping_file
            )
            print(report)
        else:
            # Тест конкретного файла
            dag_path = arg
            report, _, _, _ = test_dag_file(dag_path, mapping_file)
            print(report)
    else:
        # Тест всех samples
        report = test_all_samples(mapping_file=mapping_file)
        print(report)


if __name__ == "__main__":
    main()
