"""SQL анализатор на базе sqlglot для ClickHouse."""

import re
from typing import Dict, List, Set, Optional
import sqlglot
from sqlglot import exp

from .models import SQLAnalysisResult, TableReference, TableSource


# Все варианты dictGet функций в ClickHouse
DICTGET_FUNCTIONS = {
    'dictget', 'dictgetordefault', 'dictgetornull',
    'dictgetint8', 'dictgetint16', 'dictgetint32', 'dictgetint64',
    'dictgetuint8', 'dictgetuint16', 'dictgetuint32', 'dictgetuint64',
    'dictgetfloat32', 'dictgetfloat64', 'dictgetstring',
    'dictgetdate', 'dictgetdatetime', 'dictgetuuid',
    'dictgetint8ordefault', 'dictgetint16ordefault', 'dictgetint32ordefault', 'dictgetint64ordefault',
    'dictgetuint8ordefault', 'dictgetuint16ordefault', 'dictgetuint32ordefault', 'dictgetuint64ordefault',
    'dictgetfloat32ordefault', 'dictgetfloat64ordefault', 'dictgetstringordefault',
    'dictgetdateordefault', 'dictgetdatetimeordefault',
    'dicthas', 'dictgethierarchy', 'dictisin',
}


# ClickHouse / psycopg2 параметры %(name)s ломают sqlglot (парсит % как modulo).
# Заменяем на безопасный строковый литерал перед парсингом.
_PY_PLACEHOLDER_RE = re.compile(r'%\(([a-zA-Z_][a-zA-Z0-9_]*)\)s')


def _preprocess_sql_for_parse(sql: str) -> str:
    """Готовит SQL к sqlglot: подменяет %(name)s на строковый литерал."""
    return _PY_PLACEHOLDER_RE.sub(r"'__p_\1__'", sql)


class SQLAnalyzer:
    """Анализатор SQL запросов для извлечения lineage."""

    def __init__(self, dialect: str = "clickhouse"):
        self.dialect = dialect
        self._temp_tables: Set[str] = set()

    def analyze(self, sql: str) -> SQLAnalysisResult:
        """
        Анализирует SQL и возвращает inlets/outlets.

        Args:
            sql: SQL запрос или несколько запросов (разделённых ;)

        Returns:
            SQLAnalysisResult с найденными таблицами
        """
        result = SQLAnalysisResult()
        self._temp_tables.clear()

        # Пробуем парсить через sqlglot
        try:
            statements = sqlglot.parse(_preprocess_sql_for_parse(sql), dialect=self.dialect)
        except Exception as e:
            # Fallback на regex если sqlglot не смог
            result.warnings.append(f"sqlglot parse error: {e}, using regex fallback")
            return self._analyze_with_regex(sql)

        for statement in statements:
            if statement is None:
                continue
            self._analyze_statement(statement, result)

        # Дополнительно парсим ALTER TABLE через regex (sqlglot не поддерживает ATTACH/REPLACE PARTITION)
        self._extract_alter_table_partitions(sql, result)

        # Дополнительно парсим OPTIMIZE TABLE через regex
        self._extract_optimize_table(sql, result)

        # Удаляем временные таблицы из результатов
        result.inlets = [t for t in result.inlets if t.full_name not in self._temp_tables]
        result.outlets = [t for t in result.outlets if t.full_name not in self._temp_tables]

        # Убираем дубликаты
        result.inlets = list(set(result.inlets))
        result.outlets = list(set(result.outlets))
        result.dictionaries = list(set(result.dictionaries))
        result.remote_tables = list(set(result.remote_tables))

        return result

    def _analyze_statement(self, statement: exp.Expression, result: SQLAnalysisResult):
        """Анализирует одно SQL выражение."""
        # Проверяем на CREATE TEMPORARY TABLE
        if isinstance(statement, exp.Create):
            self._handle_create(statement, result)
            return

        # Проверяем на DROP TABLE
        if isinstance(statement, exp.Drop):
            return  # Игнорируем DROP

        # Проверяем на TRUNCATE TABLE
        if isinstance(statement, exp.TruncateTable):
            return  # Игнорируем TRUNCATE

        # Проверяем на INSERT
        if isinstance(statement, exp.Insert):
            self._handle_insert(statement, result)
            return

        # Для SELECT и других - извлекаем FROM/JOIN
        self._extract_from_tables(statement, result)
        self._extract_dictget(statement, result)

    def _handle_create(self, statement: exp.Create, result: SQLAnalysisResult):
        """Обрабатывает CREATE TABLE."""
        # Получаем имя таблицы
        table_expr = statement.this
        if table_expr:
            table_name = self._get_table_name(table_expr)
            if table_name:
                # Проверяем на TEMPORARY
                if statement.args.get('properties'):
                    props = statement.args['properties']
                    if any('temporary' in str(p).lower() for p in props.expressions if hasattr(props, 'expressions')):
                        self._temp_tables.add(table_name)
                        return

                # Проверяем по тексту SQL (fallback)
                sql_text = statement.sql(dialect=self.dialect).lower()
                if 'temporary' in sql_text or 'temp ' in sql_text:
                    self._temp_tables.add(table_name)
                    return

        # Анализируем подзапрос в AS (...)
        if statement.expression:
            self._extract_from_tables(statement.expression, result)
            self._extract_dictget(statement.expression, result)

    def _handle_insert(self, statement: exp.Insert, result: SQLAnalysisResult):
        """Обрабатывает INSERT INTO."""
        # Получаем целевую таблицу
        table_expr = statement.this
        if table_expr:
            # Когда INSERT имеет список колонок, statement.this - это Schema, а не Table
            if isinstance(table_expr, exp.Schema):
                table_expr = table_expr.this  # Извлекаем Table из Schema

            table_ref = self._parse_table_expression(table_expr, TableSource.INSERT)
            if table_ref:
                result.outlets.append(table_ref)

        # Анализируем SELECT часть
        if statement.expression:
            self._extract_from_tables(statement.expression, result)
            self._extract_dictget(statement.expression, result)

    def _extract_from_tables(self, statement: exp.Expression, result: SQLAnalysisResult):
        """Извлекает таблицы из FROM и JOIN."""
        # Ищем все Table expressions
        for table in statement.find_all(exp.Table):
            # Пропускаем CTE ссылки
            if self._is_cte_reference(table, statement):
                continue

            table_ref = self._parse_table_expression(table, TableSource.FROM)
            if table_ref:
                # Определяем тип источника (FROM или JOIN)
                parent = table.parent
                if parent and isinstance(parent, exp.Join):
                    table_ref.source = TableSource.JOIN

                if table_ref.is_remote:
                    result.remote_tables.append(table_ref)
                result.inlets.append(table_ref)

    def _extract_dictget(self, statement: exp.Expression, result: SQLAnalysisResult):
        """Извлекает словари из dictGet вызовов."""
        # Ищем Anonymous функции (которые sqlglot не распознал)
        for func in statement.find_all(exp.Anonymous):
            func_name = func.this.lower() if isinstance(func.this, str) else str(func.this).lower()

            if func_name in DICTGET_FUNCTIONS:
                # Первый аргумент - имя словаря
                if func.expressions:
                    first_arg = func.expressions[0]
                    dict_name = self._extract_string_value(first_arg)
                    if dict_name:
                        table_ref = self._parse_dict_name(dict_name)
                        if table_ref:
                            result.dictionaries.append(table_ref)

        # Также ищем Func (на случай если sqlglot распознал функцию)
        for func in statement.find_all(exp.Func):
            func_name = func.sql_name().lower() if hasattr(func, 'sql_name') else ''
            if func_name in DICTGET_FUNCTIONS:
                args = list(func.args.values())
                if args:
                    first_arg = args[0]
                    if isinstance(first_arg, list) and first_arg:
                        first_arg = first_arg[0]
                    dict_name = self._extract_string_value(first_arg)
                    if dict_name:
                        table_ref = self._parse_dict_name(dict_name)
                        if table_ref:
                            result.dictionaries.append(table_ref)

    def _extract_alter_table_partitions(self, sql: str, result: SQLAnalysisResult):
        """Извлекает таблицы из ALTER TABLE ATTACH/REPLACE PARTITION через regex.

        sqlglot не поддерживает ATTACH/REPLACE PARTITION для ClickHouse,
        поэтому используем regex для извлечения таблиц.
        """
        # Паттерн: ALTER TABLE schema.table ATTACH|REPLACE PARTITION ... FROM schema.table
        alter_pattern = r'ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)\s+(?:ATTACH|REPLACE)\s+PARTITION\s+.*?FROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'

        for match in re.finditer(alter_pattern, sql, re.IGNORECASE | re.DOTALL):
            target_full = match.group(1)  # -> outlet
            source_full = match.group(2)  # -> inlet

            target_parts = target_full.split('.')
            source_parts = source_full.split('.')

            if len(target_parts) == 2:
                result.outlets.append(TableReference(
                    schema=target_parts[0],
                    table=target_parts[1],
                    source=TableSource.INSERT
                ))

            if len(source_parts) == 2:
                is_remote = source_parts[0].startswith('remote_') or source_parts[0] == 'remote'
                ref = TableReference(
                    schema=source_parts[0],
                    table=source_parts[1],
                    source=TableSource.FROM,
                    is_remote=is_remote,
                    remote_prefix=source_parts[0] if is_remote else None
                )
                if is_remote:
                    result.remote_tables.append(ref)
                result.inlets.append(ref)

    def _extract_optimize_table(self, sql: str, result: SQLAnalysisResult):
        """Извлекает таблицы из OPTIMIZE TABLE через regex."""
        # Паттерн: OPTIMIZE TABLE schema.table [PARTITION ...] [FINAL]
        optimize_pattern = r'OPTIMIZE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
        for match in re.finditer(optimize_pattern, sql, re.IGNORECASE):
            full_name = match.group(1)
            parts = full_name.split('.')
            if len(parts) == 2:
                result.outlets.append(TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.INSERT
                ))

    def _is_cte_reference(self, table: exp.Table, root: exp.Expression) -> bool:
        """Проверяет, является ли таблица ссылкой на CTE."""
        table_name = table.name
        if not table_name:
            return False

        # Ищем WITH clause
        for with_clause in root.find_all(exp.With):
            for cte in with_clause.expressions:
                if isinstance(cte, exp.CTE) and cte.alias == table_name:
                    return True

        return False

    def _parse_table_expression(self, table: exp.Table, source: TableSource) -> Optional[TableReference]:
        """Парсит Table expression в TableReference."""
        table_name = table.name
        schema_name = table.db if hasattr(table, 'db') and table.db else None

        if not table_name:
            return None

        # Проверяем на remote_ префикс или ровно 'remote' как схему
        is_remote = False
        remote_prefix = None

        if schema_name and (schema_name.startswith('remote_') or schema_name == 'remote'):
            is_remote = True
            remote_prefix = schema_name

        # Если схема не указана, пробуем извлечь из имени
        if not schema_name and '.' in table_name:
            parts = table_name.split('.')
            if len(parts) == 2:
                schema_name = parts[0]
                table_name = parts[1]
                if schema_name.startswith('remote_') or schema_name == 'remote':
                    is_remote = True
                    remote_prefix = schema_name

        if not schema_name:
            return None  # Пропускаем таблицы без схемы

        return TableReference(
            schema=schema_name,
            table=table_name,
            source=source,
            is_remote=is_remote,
            remote_prefix=remote_prefix
        )

    def _parse_dict_name(self, dict_name: str) -> Optional[TableReference]:
        """Парсит имя словаря в TableReference."""
        # Убираем кавычки если есть
        dict_name = dict_name.strip("'\"")

        if '.' in dict_name:
            parts = dict_name.split('.')
            if len(parts) == 2:
                return TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.DICTGET
                )
        return None

    def _extract_string_value(self, expr: exp.Expression) -> Optional[str]:
        """Извлекает строковое значение из выражения."""
        if isinstance(expr, exp.Literal):
            return expr.this
        if isinstance(expr, str):
            return expr
        return None

    def _get_table_name(self, table_expr: exp.Expression) -> Optional[str]:
        """Получает полное имя таблицы из выражения."""
        if isinstance(table_expr, exp.Table):
            schema = table_expr.db if hasattr(table_expr, 'db') and table_expr.db else ''
            name = table_expr.name
            if schema:
                return f"{schema}.{name}"
            return name
        return None

    def analyze_per_statement(self, sql: str) -> List[SQLAnalysisResult]:
        """
        Анализирует SQL и возвращает результаты по каждому statement отдельно.

        Используется для определения key-групп: каждый statement с inlets/outlets
        потенциально представляет отдельный поток данных.

        Returns:
            Список SQLAnalysisResult, по одному на каждый значимый statement.
            TRUNCATE, DROP, CREATE TEMPORARY TABLE пропускаются.
        """
        results = []
        self._temp_tables.clear()

        try:
            statements = sqlglot.parse(_preprocess_sql_for_parse(sql), dialect=self.dialect)
        except Exception:
            # Fallback: возвращаем один общий результат
            return [self.analyze(sql)]

        # Первый проход: собираем temporary tables и их источники
        _temp_table_sources: Dict[str, List[TableReference]] = {}
        for statement in statements:
            if statement is None:
                continue
            if isinstance(statement, exp.Create):
                table_expr = statement.this
                if table_expr:
                    table_name = self._get_table_name(table_expr)
                    if table_name:
                        is_temp = statement.find(exp.TemporaryProperty) is not None
                        if not is_temp:
                            sql_text = statement.sql(dialect=self.dialect).lower()
                            is_temp = 'temporary' in sql_text or 'temp ' in sql_text
                        if is_temp:
                            self._temp_tables.add(table_name)
                            # Извлекаем источники из AS SELECT
                            if statement.expression:
                                source_result = SQLAnalysisResult()
                                self._extract_from_tables(statement.expression, source_result)
                                self._extract_dictget(statement.expression, source_result)
                                _temp_table_sources[table_name] = (
                                    source_result.inlets + source_result.dictionaries
                                )

        # Второй проход: анализируем каждый statement
        for statement in statements:
            if statement is None:
                continue

            stmt_result = SQLAnalysisResult()
            self._analyze_statement(statement, stmt_result)

            # Ищем ссылки на temp tables без схемы (они отфильтрованы _parse_table_expression)
            for table in statement.find_all(exp.Table):
                table_name = table.name
                schema_name = table.db if hasattr(table, 'db') and table.db else None
                if not schema_name and table_name in self._temp_tables:
                    sources = _temp_table_sources.get(table_name, [])
                    for src in sources:
                        src_name = src.full_name
                        if src_name in self._temp_tables:
                            nested = _temp_table_sources.get(src_name, [])
                            for ns in nested:
                                if ns not in stmt_result.inlets:
                                    stmt_result.inlets.append(ns)
                        else:
                            if src not in stmt_result.inlets:
                                stmt_result.inlets.append(src)

            # Резолвим временные таблицы: подставляем их источники
            resolved_inlets = []
            for t in stmt_result.inlets:
                if t.full_name in self._temp_tables:
                    sources = _temp_table_sources.get(t.full_name, [])
                    for src in sources:
                        if src.full_name in self._temp_tables:
                            # Рекурсия: источник — тоже temp table
                            nested = _temp_table_sources.get(src.full_name, [])
                            for ns in nested:
                                if ns not in resolved_inlets:
                                    resolved_inlets.append(ns)
                        else:
                            if src not in resolved_inlets:
                                resolved_inlets.append(src)
                else:
                    if t not in resolved_inlets:
                        resolved_inlets.append(t)
            stmt_result.inlets = resolved_inlets
            stmt_result.outlets = [t for t in stmt_result.outlets if t.full_name not in self._temp_tables]

            # Убираем дубликаты
            stmt_result.inlets = list(set(stmt_result.inlets))
            stmt_result.outlets = list(set(stmt_result.outlets))
            stmt_result.dictionaries = list(set(stmt_result.dictionaries))
            stmt_result.remote_tables = list(set(stmt_result.remote_tables))

            # Добавляем только если есть значимые данные
            if stmt_result.inlets or stmt_result.outlets:
                results.append(stmt_result)

        # Также парсим ALTER TABLE и OPTIMIZE через regex
        alter_result = SQLAnalysisResult()
        self._extract_alter_table_partitions(sql, alter_result)
        self._extract_optimize_table(sql, alter_result)

        # ALTER/OPTIMIZE таблицы, не покрытые sqlglot — добавляем как отдельные statements
        # Группируем ALTER по парам (outlet, inlet)
        if alter_result.outlets or alter_result.inlets:
            # Каждый ALTER с FROM — отдельный flow (inlet→outlet)
            alter_outlets_seen = set()
            for i, outlet in enumerate(alter_result.outlets):
                ar = SQLAnalysisResult()
                ar.outlets.append(outlet)
                # Ищем соответствующий inlet (ALTER ... FROM source)
                if i < len(alter_result.inlets):
                    inlet = alter_result.inlets[i]
                    ar.inlets.append(inlet)
                    if inlet.is_remote:
                        ar.remote_tables.append(inlet)
                alter_outlets_seen.add(outlet.full_name)

                # Не дублируем если такой outlet уже есть в основных results
                already_covered = any(
                    any(o.full_name == outlet.full_name for o in r.outlets)
                    for r in results
                )
                if not already_covered:
                    results.append(ar)

        return results if results else [SQLAnalysisResult()]

    def _analyze_with_regex(self, sql: str) -> SQLAnalysisResult:
        """Fallback анализ через regex."""
        result = SQLAnalysisResult()

        # Паттерн для FROM/JOIN таблиц
        table_pattern = r'(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
        for match in re.finditer(table_pattern, sql, re.IGNORECASE):
            full_name = match.group(1)
            parts = full_name.split('.')
            if len(parts) == 2:
                is_remote = parts[0].startswith('remote_')
                ref = TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.FROM,
                    is_remote=is_remote,
                    remote_prefix=parts[0] if is_remote else None
                )
                if is_remote:
                    result.remote_tables.append(ref)
                result.inlets.append(ref)

        # Паттерн для INSERT INTO
        insert_pattern = r'insert\s+into\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
        for match in re.finditer(insert_pattern, sql, re.IGNORECASE):
            full_name = match.group(1)
            parts = full_name.split('.')
            if len(parts) == 2:
                ref = TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.INSERT
                )
                result.outlets.append(ref)

        # Паттерн для dictGet
        dictget_pattern = r"dict(?:Get|Has|GetOrDefault|GetOrNull)[a-zA-Z0-9]*\s*\(\s*['\"]([^'\"]+)['\"]"
        for match in re.finditer(dictget_pattern, sql, re.IGNORECASE):
            dict_name = match.group(1)
            parts = dict_name.split('.')
            if len(parts) == 2:
                ref = TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.DICTGET
                )
                result.dictionaries.append(ref)

        # Паттерн для ALTER TABLE ATTACH/REPLACE PARTITION ... FROM
        alter_pattern = r'ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)\s+(?:ATTACH|REPLACE)\s+PARTITION\s+.*?FROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
        for match in re.finditer(alter_pattern, sql, re.IGNORECASE | re.DOTALL):
            target_full = match.group(1)  # -> outlet
            source_full = match.group(2)  # -> inlet

            target_parts = target_full.split('.')
            source_parts = source_full.split('.')

            if len(target_parts) == 2:
                result.outlets.append(TableReference(
                    schema=target_parts[0],
                    table=target_parts[1],
                    source=TableSource.INSERT
                ))

            if len(source_parts) == 2:
                is_remote = source_parts[0].startswith('remote_') or source_parts[0] == 'remote'
                ref = TableReference(
                    schema=source_parts[0],
                    table=source_parts[1],
                    source=TableSource.FROM,
                    is_remote=is_remote,
                    remote_prefix=source_parts[0] if is_remote else None
                )
                if is_remote:
                    result.remote_tables.append(ref)
                result.inlets.append(ref)

        # Паттерн для OPTIMIZE TABLE
        optimize_pattern = r'OPTIMIZE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+)'
        for match in re.finditer(optimize_pattern, sql, re.IGNORECASE):
            full_name = match.group(1)
            parts = full_name.split('.')
            if len(parts) == 2:
                result.outlets.append(TableReference(
                    schema=parts[0],
                    table=parts[1],
                    source=TableSource.INSERT
                ))

        # Убираем дубликаты
        result.inlets = list(set(result.inlets))
        result.outlets = list(set(result.outlets))
        result.dictionaries = list(set(result.dictionaries))
        result.remote_tables = list(set(result.remote_tables))

        return result
