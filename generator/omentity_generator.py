"""Генератор OMEntity кода для DAG задач."""

from typing import List, Dict, Optional

from analyzer.models import (
    SQLAnalysisResult, DAGParseResult, TaskInfo,
    OMEntityOutput, EntityType, TableReference, FunctionInfo
)
from analyzer.sql_analyzer import SQLAnalyzer
from analyzer.connection_resolver import ConnectionResolver
from .fqn_builder import FQNBuilder


class OMEntityGenerator:
    """Генератор OMEntity кода для DAG задач."""

    def __init__(self, fqn_builder: FQNBuilder):
        self.fqn_builder = fqn_builder
        self.sql_analyzer = SQLAnalyzer()

    def generate_for_dag(
        self,
        dag_result: DAGParseResult,
        sql_results: Dict[str, SQLAnalysisResult]
    ) -> List[OMEntityOutput]:
        """
        Генерирует OMEntity для всех задач в DAG.

        Args:
            dag_result: Результат парсинга DAG
            sql_results: Результаты анализа SQL для каждой функции

        Returns:
            Список OMEntityOutput для каждой задачи
        """
        outputs = []
        resolver = ConnectionResolver(dag_result)

        for task in dag_result.tasks:
            # Пропускаем задачи с существующими OMEntity
            if task.has_omentity:
                continue

            func_name = task.python_callable
            if not func_name:
                continue

            conn_id, conn_source = resolver.resolve_for_function(func_name)
            if not conn_id:
                conn_id = "UNKNOWN"

            # Собираем SQL результаты для функции
            sql_result = sql_results.get(func_name)
            if not sql_result:
                # Пробуем найти по SQL переменным функции
                func_info = dag_result.functions.get(func_name)
                if func_info:
                    combined_result = SQLAnalysisResult()
                    for sql_var in func_info.sql_variables:
                        if sql_var in sql_results:
                            combined_result = combined_result.merge(sql_results[sql_var])
                    if combined_result.inlets or combined_result.outlets or combined_result.dictionaries:
                        sql_result = combined_result

            # Получаем информацию о функции
            func_info = dag_result.functions.get(func_name)

            # Если есть cross-server вызовы или API - обрабатываем даже без sql_result
            has_cross_server = func_info and func_info.cross_server_calls
            has_api = (func_info and func_info.api_connections) or task.op_kwargs_api
            has_dst_table = task.op_kwargs_dst_table

            if not sql_result and not has_cross_server and not has_api and not has_dst_table:
                continue

            output = self._generate_for_task(
                task, conn_id, conn_source, sql_result,
                func_info, dag_result, resolver
            )
            outputs.append(output)

        return outputs

    def _generate_for_task(
        self,
        task: TaskInfo,
        connection_id: str,
        connection_source: str,
        sql_result: SQLAnalysisResult,
        func_info: FunctionInfo = None,
        dag_result: DAGParseResult = None,
        resolver: ConnectionResolver = None
    ) -> OMEntityOutput:
        """Генерирует OMEntity для одной задачи."""
        inlets = []
        outlets = []
        warnings = []

        # Инициализируем sql_result если None
        if sql_result is None:
            sql_result = SQLAnalysisResult()

        # Обрабатываем API connections из функции
        if func_info and func_info.api_connections:
            for api_conn in func_info.api_connections:
                # Резолвим значение переменной если нужно
                if dag_result and api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                inlets.append((EntityType.API, api_fqn))

        # Обрабатываем API connections из op_kwargs задачи
        if task.op_kwargs_api:
            for api_conn in task.op_kwargs_api:
                # Резолвим значение переменной если нужно
                if dag_result and api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                inlet_tuple = (EntityType.API, api_fqn)
                if inlet_tuple not in inlets:
                    inlets.append(inlet_tuple)

        # Обрабатываем dst_table из op_kwargs как outlet
        if task.op_kwargs_dst_table and dag_result:
            dst_table = task.op_kwargs_dst_table
            # Резолвим переменную из string_variables
            if dst_table in dag_result.string_variables:
                dst_table = dag_result.string_variables[dst_table]
            # Парсим schema.table
            if '.' in dst_table:
                parts = dst_table.split('.')
                schema = parts[0]
                table = parts[1] if len(parts) > 1 else ''
                fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                outlets.append((EntityType.TABLE, fqn))

        # Обрабатываем bulk_dump tables из функции
        if func_info and func_info.bulk_dump_tables and dag_result:
            for table_type, table_value in func_info.bulk_dump_tables:
                resolved_table = None

                if table_type == 'param':
                    # Это параметр функции - резолвим из op_kwargs
                    if task.op_kwargs_all and table_value in task.op_kwargs_all:
                        kwarg_value = task.op_kwargs_all[table_value]
                        # Резолвим переменную из string_variables
                        if kwarg_value in dag_result.string_variables:
                            resolved_table = dag_result.string_variables[kwarg_value]
                        else:
                            resolved_table = kwarg_value
                else:
                    # Это literal или переменная в scope функции
                    if table_value in dag_result.string_variables:
                        resolved_table = dag_result.string_variables[table_value]
                    elif '.' in table_value:
                        # Уже schema.table
                        resolved_table = table_value

                if resolved_table and '.' in resolved_table:
                    parts = resolved_table.split('.', 1)
                    schema = parts[0]
                    table = parts[1] if len(parts) > 1 else ''
                    fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                    outlet_tuple = (EntityType.TABLE, fqn)
                    if outlet_tuple not in outlets:
                        outlets.append(outlet_tuple)

        # Обрабатываем cross-server вызовы
        if func_info and func_info.cross_server_calls and dag_result:
            for call in func_info.cross_server_calls:
                # Резолвим source и destination connections
                src_conn = call.src_connection
                if src_conn in dag_result.connection_variables:
                    src_conn = dag_result.connection_variables[src_conn]

                dst_conn = call.dst_connection
                if dst_conn in dag_result.connection_variables:
                    dst_conn = dag_result.connection_variables[dst_conn]

                # Анализируем take_data SQL для inlets
                take_sql = dag_result.sql_variables.get(call.take_data_var)
                if take_sql:
                    take_result = self.sql_analyzer.analyze(take_sql)
                    for table_ref in take_result.inlets:
                        if table_ref.is_remote:
                            fqn = self.fqn_builder.build_fqn_for_remote(
                                src_conn, table_ref.remote_prefix, table_ref.table
                            )
                        else:
                            fqn = self.fqn_builder.build_fqn(src_conn, table_ref.schema, table_ref.table)
                        inlet_tuple = (EntityType.TABLE, fqn)
                        if inlet_tuple not in inlets:
                            inlets.append(inlet_tuple)

                # Анализируем insert_data SQL для outlets
                insert_sql = dag_result.sql_variables.get(call.insert_data_var)
                if insert_sql:
                    insert_result = self.sql_analyzer.analyze(insert_sql)
                    for table_ref in insert_result.outlets:
                        fqn = self.fqn_builder.build_fqn(dst_conn, table_ref.schema, table_ref.table)
                        outlet_tuple = (EntityType.TABLE, fqn)
                        if outlet_tuple not in outlets:
                            outlets.append(outlet_tuple)

        # Обрабатываем inlets (FROM/JOIN таблицы)
        for table_ref in sql_result.inlets:
            if table_ref.is_remote:
                fqn = self.fqn_builder.build_fqn_for_remote(
                    connection_id, table_ref.remote_prefix, table_ref.table
                )
                warnings.append(
                    f'"{table_ref.full_name}" uses prefix "{table_ref.remote_prefix}" - different connection?'
                )
            else:
                fqn = self.fqn_builder.build_fqn(
                    connection_id,
                    table_ref.schema,
                    table_ref.table
                )
            inlets.append((EntityType.TABLE, fqn))

        # Обрабатываем dictionaries
        for dict_ref in sql_result.dictionaries:
            fqn = self.fqn_builder.build_fqn(
                connection_id,
                dict_ref.schema,
                dict_ref.table
            )
            inlets.append((EntityType.TABLE, fqn))

        # Обрабатываем remote таблицы (если ещё не добавлены через inlets)
        for table_ref in sql_result.remote_tables:
            fqn = self.fqn_builder.build_fqn_for_remote(
                connection_id, table_ref.remote_prefix, table_ref.table
            )
            inlet_tuple = (EntityType.TABLE, fqn)
            if inlet_tuple not in inlets:
                inlets.append(inlet_tuple)

        # Обрабатываем outlets (INSERT INTO)
        for table_ref in sql_result.outlets:
            fqn = self.fqn_builder.build_fqn(
                connection_id,
                table_ref.schema,
                table_ref.table
            )
            outlets.append((EntityType.TABLE, fqn))

        # Убираем дубликаты сохраняя порядок
        inlets = list(dict.fromkeys(inlets))
        outlets = list(dict.fromkeys(outlets))

        # Генерируем код
        code = self._format_omentity_code(
            task.task_id,
            task.python_callable,
            connection_id,
            connection_source,
            inlets,
            outlets,
            warnings,
            sql_result
        )

        return OMEntityOutput(
            task_id=task.task_id,
            function_name=task.python_callable or '',
            connection_id=connection_id,
            connection_source=connection_source,
            inlets=inlets,
            outlets=outlets,
            warnings=warnings,
            generated_code=code
        )

    def _format_omentity_code(
        self,
        task_id: str,
        func_name: Optional[str],
        conn_id: str,
        conn_source: str,
        inlets: List[tuple],
        outlets: List[tuple],
        warnings: List[str],
        sql_result: SQLAnalysisResult
    ) -> str:
        """Форматирует итоговый код OMEntity."""
        lines = []

        # Заголовок
        lines.append("# " + "=" * 65)
        lines.append(f"# Task: {task_id}")
        if func_name:
            lines.append(f"# Function: {func_name}")
        lines.append(f"# Connection: {conn_id} (from {conn_source})")
        lines.append("# " + "=" * 65)
        lines.append("")

        # Детализация источников
        lines.append("# Detected SOURCES (inlets):")

        # Обычные таблицы
        tables = [t for t in sql_result.inlets if not t.is_remote]
        if tables:
            lines.append("#   Tables:")
            for t in tables:
                lines.append(f"#     - {t.full_name} ({t.source.value})")

        # Remote таблицы
        remote = sql_result.remote_tables
        if remote:
            lines.append("#   Remote tables:")
            for t in remote:
                lines.append(f"#     - {t.remote_prefix}.{t.table}")

        # Словари
        if sql_result.dictionaries:
            lines.append("#   Dictionaries:")
            for d in sql_result.dictionaries:
                lines.append(f"#     - {d.full_name}")

        lines.append("")
        lines.append("# Detected TARGETS (outlets):")
        if sql_result.outlets:
            for t in sql_result.outlets:
                lines.append(f"#     - {t.full_name} (INSERT INTO)")
        else:
            lines.append("#     (none detected)")

        lines.append("")
        lines.append("# " + "-" * 65)
        lines.append("# Generated OMEntity:")
        lines.append("# " + "-" * 65)
        lines.append("")

        # Код inlets
        if inlets:
            lines.append("inlets=[")
            for entity_type, fqn in inlets:
                lines.append(f'    OMEntity(entity=Entity.{entity_type.value}, fqn="{fqn}"),')
            lines.append("],")
        else:
            lines.append("inlets=[],")

        # Код outlets
        if outlets:
            lines.append("outlets=[")
            for entity_type, fqn in outlets:
                lines.append(f'    OMEntity(entity=Entity.{entity_type.value}, fqn="{fqn}"),')
            lines.append("]")
        else:
            lines.append("outlets=[]")

        # Предупреждения
        if warnings:
            lines.append("")
            lines.append("# WARNINGS:")
            for w in warnings:
                lines.append(f"#   - {w}")

        return "\n".join(lines)
