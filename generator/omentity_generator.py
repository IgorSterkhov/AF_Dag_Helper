"""Генератор OMEntity кода для DAG задач."""

from typing import List, Dict, Optional

from analyzer.models import (
    SQLAnalysisResult, DAGParseResult, TaskInfo,
    OMEntityOutput, OMEntityItem, EntityType, TableReference,
    FunctionInfo, DataFlowGroup
)
from analyzer.sql_analyzer import SQLAnalyzer
from analyzer.connection_resolver import ConnectionResolver
from .fqn_builder import FQNBuilder
from .key_assigner import KeyAssigner


class OMEntityGenerator:
    """Генератор OMEntity кода для DAG задач."""

    def __init__(self, fqn_builder: FQNBuilder, include_dictget_as_inlets: bool = True):
        self.fqn_builder = fqn_builder
        self.sql_analyzer = SQLAnalyzer()
        self.include_dictget_as_inlets = include_dictget_as_inlets
        self.key_assigner = KeyAssigner()

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
            has_bulk_dump = func_info and func_info.bulk_dump_tables
            has_bulk_dump_transfer = func_info and func_info.bulk_dump_transfers

            if not sql_result and not has_cross_server and not has_api and not has_dst_table and not has_bulk_dump and not has_bulk_dump_transfer:
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
        """Генерирует OMEntity для одной задачи через flow-группы."""
        flows = []
        warnings = []

        if sql_result is None:
            sql_result = SQLAnalysisResult()

        # FLOW: API → bulk_dump/dst_table
        api_flow = self._build_api_flow(
            task, connection_id, func_info, dag_result
        )
        if api_flow and (api_flow.inlets or api_flow.outlets):
            flows.append(api_flow)

        # FLOW(s): Cross-server calls
        cs_flows = []
        if func_info and func_info.cross_server_calls and dag_result:
            cs_flows = self._build_cross_server_flows(
                func_info, connection_id, dag_result, warnings
            )

        # FLOW(s): cursor.execute(SQL) → hook.bulk_dump(table)
        bulk_transfer_flows = []
        if func_info and func_info.bulk_dump_transfers and dag_result:
            bulk_transfer_flows = self._build_bulk_dump_transfer_flows(
                func_info, dag_result, warnings
            )

        # FLOW(s): SQL (per-statement)
        sql_flows = self._build_sql_flows(
            connection_id, sql_result, func_info, dag_result, task, warnings
        )

        # Разделяем SQL flows: primary connection и other connections.
        # Cross-server loads are emitted first; source-only cutoff reads on the
        # same server are merged into that source-side flow.
        sql_primary = []
        sql_other = []

        if sql_flows:
            cs_inlet_fqns = set()
            cs_outlet_fqns = set()
            for f in cs_flows:
                cs_inlet_fqns.update(i.fqn for i in f.inlets)
                cs_outlet_fqns.update(o.fqn for o in f.outlets)

            # Определяем server prefix из connection_id (через fqn_builder mapping)
            primary_server = self.fqn_builder.get_server_name(connection_id)

            for sf in sql_flows:
                sf_inlet_fqns = {i.fqn for i in sf.inlets}
                sf_outlet_fqns = {o.fqn for o in sf.outlets}
                # Пропускаем SQL flow если cross-server уже покрыл те же таблицы
                if sf_inlet_fqns <= cs_inlet_fqns and sf_outlet_fqns <= cs_outlet_fqns:
                    continue
                # Классифицируем по outlet: если данные пишутся на primary — flow primary.
                # Если outlet-ов нет — смотрим на первый inlet.
                servers = [o.fqn.split('.', 1)[0] for o in sf.outlets if '.' in o.fqn]
                if not servers:
                    servers = [i.fqn.split('.', 1)[0] for i in sf.inlets if '.' in i.fqn]
                if servers and servers[0] == primary_server:
                    sql_primary.append(sf)
                elif servers:
                    sql_other.append(sf)
                else:
                    sql_primary.append(sf)

        if cs_flows:
            sql_primary = self._merge_source_only_flows_into_cross_server(sql_primary, cs_flows)
            sql_other = self._merge_source_only_flows_into_cross_server(sql_other, cs_flows)

        flows.extend(bulk_transfer_flows)
        flows.extend(cs_flows)
        flows.extend(sql_primary)
        flows.extend(sql_other)

        # Назначаем key через KeyAssigner
        inlets, outlets = self.key_assigner.assign_keys(flows)

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
            generated_code=code,
            flows=flows
        )

    def _build_api_flow(
        self,
        task: TaskInfo,
        connection_id: str,
        func_info: FunctionInfo,
        dag_result: DAGParseResult
    ) -> Optional[DataFlowGroup]:
        """Собирает flow для API → bulk_dump/dst_table."""
        inlets = []
        outlets = []

        # API inlets из функции
        if func_info and func_info.api_connections:
            for api_conn in func_info.api_connections:
                if dag_result and api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                inlets.append(OMEntityItem(EntityType.API, api_fqn))

        # API inlets из op_kwargs задачи
        if task.op_kwargs_api:
            for api_conn in task.op_kwargs_api:
                if dag_result and api_conn in dag_result.connection_variables:
                    api_fqn = dag_result.connection_variables[api_conn]
                else:
                    api_fqn = api_conn
                item = OMEntityItem(EntityType.API, api_fqn)
                if item not in inlets:
                    inlets.append(item)

        if not inlets:
            return None

        # Outlets: dst_table из op_kwargs
        if task.op_kwargs_dst_table and dag_result:
            dst_table = task.op_kwargs_dst_table
            if dst_table in dag_result.string_variables:
                dst_table = dag_result.string_variables[dst_table]
            if '.' in dst_table:
                parts = dst_table.split('.')
                schema = parts[0]
                table = parts[1] if len(parts) > 1 else ''
                fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                outlets.append(OMEntityItem(EntityType.TABLE, fqn))

        # Outlets: bulk_dump tables из функции
        if func_info and func_info.bulk_dump_tables and dag_result:
            for table_type, table_value in func_info.bulk_dump_tables:
                resolved_table = self._resolve_bulk_dump_table(
                    table_type, table_value, task, dag_result
                )
                if resolved_table and '.' in resolved_table:
                    parts = resolved_table.split('.', 1)
                    schema = parts[0]
                    table = parts[1] if len(parts) > 1 else ''
                    fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                    item = OMEntityItem(EntityType.TABLE, fqn)
                    if item not in outlets:
                        outlets.append(item)

        return DataFlowGroup(
            flow_type='api',
            inlets=inlets,
            outlets=outlets,
            source_description='API connection → bulk_dump/dst_table'
        )

    def _build_sql_flows(
        self,
        connection_id: str,
        sql_result: SQLAnalysisResult,
        func_info: FunctionInfo,
        dag_result: DAGParseResult,
        task: TaskInfo,
        warnings: List[str]
    ) -> List[DataFlowGroup]:
        """
        Собирает flow-группы из SQL анализа.

        Использует per-statement анализ для разделения на key-группы.
        """
        flows = []

        if not func_info or not dag_result:
            # Fallback: один flow из sql_result
            if sql_result and (sql_result.inlets or sql_result.outlets):
                flow = self._sql_result_to_flow(connection_id, sql_result, warnings)
                if flow:
                    flows.append(flow)
            return flows

        # Пробуем per-statement анализ с per-variable connection
        per_stmt_results = []  # list of (conn_id, stmt_result)
        transfer_sql_vars = {
            transfer.sql_var
            for transfer in getattr(func_info, 'bulk_dump_transfers', [])
        }
        for sql_var in func_info.sql_variables:
            if sql_var in transfer_sql_vars:
                continue
            # Resolve connection per SQL variable (may differ from function-level)
            var_conns = self._resolve_sql_var_connections(
                sql_var, connection_id, func_info, dag_result
            )
            sql_code = dag_result.sql_variables.get(sql_var)
            if sql_code:
                stmt_results = self.sql_analyzer.analyze_per_statement(sql_code)
                for var_conn in var_conns:
                    for stmt in stmt_results:
                        per_stmt_results.append((var_conn, stmt))

        if per_stmt_results and len(per_stmt_results) > 1:
            # Несколько statement'ов — каждый в отдельный flow с правильным connection
            for var_conn, stmt_result in per_stmt_results:
                flow = self._sql_result_to_flow(var_conn, stmt_result, warnings)
                if flow:
                    flows.append(flow)
        elif per_stmt_results and len(per_stmt_results) == 1:
            # Один statement — используем его connection
            var_conn, stmt_result = per_stmt_results[0]
            flow = self._sql_result_to_flow(var_conn, stmt_result, warnings)
            if flow:
                flows.append(flow)
        elif (
            sql_result
            and (sql_result.inlets or sql_result.outlets)
            and not transfer_sql_vars
        ):
            # Fallback — один flow с function-level connection
            flow = self._sql_result_to_flow(connection_id, sql_result, warnings)
            if flow:
                flows.append(flow)

        # Добавляем bulk_dump outlets (если нет API flow)
        if func_info and func_info.bulk_dump_tables and dag_result:
            has_api = (func_info.api_connections) or task.op_kwargs_api
            if not has_api:
                bulk_outlets = self._resolve_bulk_dump_outlets(
                    func_info, task, connection_id, dag_result
                )
                if bulk_outlets:
                    # Добавляем bulk_dump outlets к последнему SQL flow
                    if flows:
                        for item in bulk_outlets:
                            if item not in flows[-1].outlets:
                                flows[-1].outlets.append(item)
                    else:
                        flows.append(DataFlowGroup(
                            flow_type='bulk_dump',
                            outlets=bulk_outlets,
                            source_description='bulk_dump tables'
                        ))

        # Добавляем dst_table outlet (если нет API flow)
        if task.op_kwargs_dst_table and dag_result:
            has_api = (func_info and func_info.api_connections) or task.op_kwargs_api
            if not has_api:
                dst_table = task.op_kwargs_dst_table
                if dst_table in dag_result.string_variables:
                    dst_table = dag_result.string_variables[dst_table]
                if '.' in dst_table:
                    parts = dst_table.split('.')
                    schema = parts[0]
                    table = parts[1] if len(parts) > 1 else ''
                    fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                    item = OMEntityItem(EntityType.TABLE, fqn)
                    if flows:
                        if item not in flows[-1].outlets:
                            flows[-1].outlets.append(item)
                    else:
                        flows.append(DataFlowGroup(
                            flow_type='sql',
                            outlets=[item],
                            source_description='dst_table from op_kwargs'
                        ))

        # Merge flows с одинаковыми outlet-ами
        if len(flows) > 1:
            flows = self._merge_flows_by_outlet(flows)
            flows = self._merge_sequential_sql_flows(flows)

        return flows

    def _merge_source_only_flows_into_cross_server(
        self,
        source_flows: List[DataFlowGroup],
        cross_server_flows: List[DataFlowGroup],
    ) -> List[DataFlowGroup]:
        """Attach no-outlet source reads to cross-server source-side lineage."""
        remaining: List[DataFlowGroup] = []
        for flow in source_flows:
            if flow.outlets or not flow.inlets:
                remaining.append(flow)
                continue

            merged = False
            for cs_flow in cross_server_flows:
                if self._source_flow_related_to_cross_server(flow, cs_flow):
                    for item in flow.inlets:
                        if item not in cs_flow.inlets:
                            cs_flow.inlets.append(item)
                    merged = True
                    break
            if not merged:
                remaining.append(flow)
        return remaining

    def _source_flow_related_to_cross_server(
        self,
        source_flow: DataFlowGroup,
        cross_server_flow: DataFlowGroup,
    ) -> bool:
        source_families = {
            self._table_family(item.fqn)
            for item in source_flow.inlets
        }
        cross_server_families = {
            self._table_family(item.fqn)
            for item in cross_server_flow.inlets
        }
        source_families.discard(None)
        cross_server_families.discard(None)
        return bool(source_families & cross_server_families)

    def _table_family(self, fqn: str):
        parts = fqn.split(".")
        if len(parts) < 3:
            return None

        server = parts[0]
        schema = ".".join(parts[1:-1])
        table = parts[-1]
        for suffix in ("_rc_d", "_rc", "_d"):
            if table.endswith(suffix):
                table = table[: -len(suffix)]
                break
        return server, schema, table

    def _merge_flows_by_outlet(self, flows: List[DataFlowGroup]) -> List[DataFlowGroup]:
        """Merge SQL flows that target the same outlet table(s)."""
        merged = {}  # frozenset(outlet_fqns) -> DataFlowGroup
        order = []   # preserve first-seen order

        for index, flow in enumerate(flows):
            outlet_key = frozenset(item.fqn for item in flow.outlets)
            if not outlet_key:
                outlet_key = ("__source_only__", index)
            if outlet_key in merged:
                existing = merged[outlet_key]
                for item in flow.inlets:
                    if item not in existing.inlets:
                        existing.inlets.append(item)
            else:
                merged[outlet_key] = DataFlowGroup(
                    flow_type=flow.flow_type,
                    inlets=list(flow.inlets),
                    outlets=list(flow.outlets),
                    source_description=flow.source_description
                )
                order.append(outlet_key)

        return [merged[key] for key in order]

    def _merge_sequential_sql_flows(self, flows: List[DataFlowGroup]) -> List[DataFlowGroup]:
        """Merge SQL flows when one statement writes a table consumed by another."""
        result: List[DataFlowGroup] = []
        consumed_indexes = set()

        for index, flow in enumerate(flows):
            if index in consumed_indexes:
                continue
            if flow.flow_type != "sql":
                result.append(flow)
                continue

            merged = DataFlowGroup(
                flow_type=flow.flow_type,
                inlets=list(flow.inlets),
                outlets=list(flow.outlets),
                source_description=flow.source_description,
            )

            changed = True
            while changed:
                changed = False
                outlet_fqns = {item.fqn for item in merged.outlets}
                for other_index, other in enumerate(flows):
                    if (
                        other_index <= index
                        or other_index in consumed_indexes
                        or other.flow_type != "sql"
                    ):
                        continue

                    consumed_fqns = outlet_fqns & {item.fqn for item in other.inlets}
                    if not consumed_fqns:
                        continue

                    for item in other.inlets:
                        if item not in merged.inlets:
                            merged.inlets.append(item)
                    merged.outlets = [
                        item for item in merged.outlets
                        if item.fqn not in consumed_fqns
                    ]
                    for item in other.outlets:
                        if item not in merged.outlets:
                            merged.outlets.append(item)
                    consumed_indexes.add(other_index)
                    changed = True
                    break

            result.append(merged)

        return result

    def _resolve_sql_var_connections(
        self,
        sql_var: str,
        default_connection_id: str,
        func_info: FunctionInfo,
        dag_result: DAGParseResult
    ) -> List[str]:
        conn_vars = [
            conn_var
            for var_name, conn_var in getattr(func_info, 'sql_var_connection_bindings', [])
            if var_name == sql_var
        ]
        if not conn_vars and sql_var in func_info.sql_var_connections:
            conn_vars = [func_info.sql_var_connections[sql_var]]
        if not conn_vars:
            conn_vars = [default_connection_id]

        resolved: List[str] = []
        for conn_var in conn_vars:
            conn_id = dag_result.connection_variables.get(conn_var, conn_var)
            if conn_id not in resolved:
                resolved.append(conn_id)
        return resolved

    def _sql_result_to_flow(
        self,
        connection_id: str,
        sql_result: SQLAnalysisResult,
        warnings: List[str]
    ) -> Optional[DataFlowGroup]:
        """Конвертирует SQLAnalysisResult в DataFlowGroup."""
        inlets = []
        outlets = []

        # Inlets из FROM/JOIN
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
                    connection_id, table_ref.schema, table_ref.table
                )
            inlets.append(OMEntityItem(EntityType.TABLE, fqn))

        # Dictionaries (опционально)
        if self.include_dictget_as_inlets:
            for dict_ref in sql_result.dictionaries:
                fqn = self.fqn_builder.build_fqn(
                    connection_id, dict_ref.schema, dict_ref.table
                )
                inlets.append(OMEntityItem(EntityType.TABLE, fqn))

        # Remote tables (если не добавлены через inlets)
        for table_ref in sql_result.remote_tables:
            fqn = self.fqn_builder.build_fqn_for_remote(
                connection_id, table_ref.remote_prefix, table_ref.table
            )
            item = OMEntityItem(EntityType.TABLE, fqn)
            if item not in inlets:
                inlets.append(item)

        # Outlets из INSERT INTO
        for table_ref in sql_result.outlets:
            fqn = self.fqn_builder.build_fqn(
                connection_id, table_ref.schema, table_ref.table
            )
            outlets.append(OMEntityItem(EntityType.TABLE, fqn))

        if not inlets and not outlets:
            return None

        return DataFlowGroup(
            flow_type='sql',
            inlets=inlets,
            outlets=outlets,
            source_description='SQL FROM/JOIN → INSERT INTO'
        )

    def _build_bulk_dump_transfer_flows(
        self,
        func_info: FunctionInfo,
        dag_result: DAGParseResult,
        warnings: List[str]
    ) -> List[DataFlowGroup]:
        """Собирает flow для cursor.execute(SQL) → hook.bulk_dump(table)."""
        flows: List[DataFlowGroup] = []

        for transfer in func_info.bulk_dump_transfers:
            sql_code = dag_result.sql_variables.get(transfer.sql_var)
            if not sql_code:
                continue

            src_conn = self._resolve_connection_value(transfer.source_connection, dag_result)
            dst_conn = self._resolve_connection_value(transfer.dst_connection, dag_result)
            if not src_conn or not dst_conn:
                continue

            dst_item = self._table_name_to_item(dst_conn, transfer.dst_table)
            if not dst_item:
                continue

            stmt_results = self.sql_analyzer.analyze_per_statement(sql_code)
            for stmt_result in stmt_results:
                flow = self._sql_result_to_flow(src_conn, stmt_result, warnings)
                if not flow:
                    flow = DataFlowGroup(
                        flow_type='bulk_dump',
                        source_description=f'cursor.execute → bulk_dump {src_conn} → {dst_conn}'
                    )
                flow.flow_type = 'bulk_dump'
                flow.source_description = f'cursor.execute → bulk_dump {src_conn} → {dst_conn}'
                if dst_item not in flow.outlets:
                    flow.outlets.append(dst_item)
                flows.append(flow)

        if len(flows) > 1:
            flows = self._merge_flows_by_outlet(flows)
        return flows

    def _build_cross_server_flows(
        self,
        func_info: FunctionInfo,
        connection_id: str,
        dag_result: DAGParseResult,
        warnings: List[str]
    ) -> List[DataFlowGroup]:
        """Собирает flow-группы из cross-server вызовов (copy_ch_to_ch_pipe)."""
        flows = []

        for call in func_info.cross_server_calls:
            # Резолвим source и destination connections
            src_conn = call.src_connection
            if src_conn in dag_result.connection_variables:
                src_conn = dag_result.connection_variables[src_conn]

            dst_conn = call.dst_connection
            if dst_conn in dag_result.connection_variables:
                dst_conn = dag_result.connection_variables[dst_conn]

            # Resolve take_data SQL: sql_variables → string_variables → inline
            take_sql = None
            if call.take_data_var:
                take_sql = dag_result.sql_variables.get(call.take_data_var)
                if not take_sql:
                    take_sql = dag_result.string_variables.get(call.take_data_var)
            if not take_sql and call.take_data_inline:
                take_sql = call.take_data_inline

            # Resolve insert_data SQL: sql_variables → string_variables → inline
            insert_sql = None
            if call.insert_data_var:
                insert_sql = dag_result.sql_variables.get(call.insert_data_var)
                if not insert_sql:
                    insert_sql = dag_result.string_variables.get(call.insert_data_var)
            if not insert_sql and call.insert_data_inline:
                insert_sql = call.insert_data_inline

            if take_sql:
                stmt_results = self.sql_analyzer.analyze_per_statement(take_sql)

                for stmt_result in stmt_results:
                    inlets = []
                    outlets = []

                    # Inlets из take_data SQL
                    for table_ref in stmt_result.inlets:
                        if table_ref.is_remote:
                            fqn = self.fqn_builder.build_fqn_for_remote(
                                src_conn, table_ref.remote_prefix, table_ref.table
                            )
                        else:
                            fqn = self.fqn_builder.build_fqn(
                                src_conn, table_ref.schema, table_ref.table
                            )
                        item = OMEntityItem(EntityType.TABLE, fqn)
                        if item not in inlets:
                            inlets.append(item)

                    # Dictionaries из take_data SQL (dictGet)
                    if self.include_dictget_as_inlets:
                        for dict_ref in stmt_result.dictionaries:
                            fqn = self.fqn_builder.build_fqn(
                                src_conn, dict_ref.schema, dict_ref.table
                            )
                            item = OMEntityItem(EntityType.TABLE, fqn)
                            if item not in inlets:
                                inlets.append(item)

                    # Outlets из take_data (если есть INSERT — внутренний шаг)
                    if stmt_result.outlets:
                        for table_ref in stmt_result.outlets:
                            fqn = self.fqn_builder.build_fqn(
                                src_conn, table_ref.schema, table_ref.table
                            )
                            item = OMEntityItem(EntityType.TABLE, fqn)
                            if item not in outlets:
                                outlets.append(item)
                    else:
                        # Финальный SELECT → insert_data на dst_conn
                        if insert_sql:
                            insert_result = self.sql_analyzer.analyze(insert_sql)
                            for table_ref in insert_result.outlets:
                                fqn = self.fqn_builder.build_fqn(
                                    dst_conn, table_ref.schema, table_ref.table
                                )
                                item = OMEntityItem(EntityType.TABLE, fqn)
                                if item not in outlets:
                                    outlets.append(item)

                    if inlets or outlets:
                        flows.append(DataFlowGroup(
                            flow_type='cross_server',
                            inlets=inlets,
                            outlets=outlets,
                            source_description=f'copy_ch_to_ch_pipe {src_conn} → {dst_conn}'
                        ))
            else:
                # Нет take_data SQL — только outlets из insert_data
                if insert_sql:
                    insert_result = self.sql_analyzer.analyze(insert_sql)
                    outlets = []
                    for table_ref in insert_result.outlets:
                        fqn = self.fqn_builder.build_fqn(
                            dst_conn, table_ref.schema, table_ref.table
                        )
                        outlets.append(OMEntityItem(EntityType.TABLE, fqn))

                    if outlets:
                        flows.append(DataFlowGroup(
                            flow_type='cross_server',
                            outlets=outlets,
                            source_description=f'copy_ch_to_ch_pipe → {dst_conn} (no take_data)'
                        ))

        if len(flows) > 1:
            flows = self._merge_flows_by_outlet(flows)
        return flows

    def _resolve_connection_value(self, conn_var: str, dag_result: DAGParseResult) -> str:
        if conn_var in dag_result.connection_variables:
            return dag_result.connection_variables[conn_var]
        return conn_var

    def _table_name_to_item(self, connection_id: str, table_name: str) -> Optional[OMEntityItem]:
        if not table_name or '.' not in table_name:
            return None
        schema, table = table_name.split('.', 1)
        fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
        return OMEntityItem(EntityType.TABLE, fqn)

    def _resolve_bulk_dump_table(
        self,
        table_type: str,
        table_value: str,
        task: TaskInfo,
        dag_result: DAGParseResult
    ) -> Optional[str]:
        """Резолвит значение bulk_dump table."""
        if table_type == 'param':
            if task.op_kwargs_all and table_value in task.op_kwargs_all:
                kwarg_value = task.op_kwargs_all[table_value]
                if kwarg_value in dag_result.string_variables:
                    return dag_result.string_variables[kwarg_value]
                return kwarg_value
        else:
            if table_value in dag_result.string_variables:
                return dag_result.string_variables[table_value]
            elif '.' in table_value:
                return table_value
        return None

    def _resolve_bulk_dump_outlets(
        self,
        func_info: FunctionInfo,
        task: TaskInfo,
        connection_id: str,
        dag_result: DAGParseResult
    ) -> List[OMEntityItem]:
        """Резолвит все bulk_dump tables в OMEntityItem outlets."""
        outlets = []
        for table_type, table_value in func_info.bulk_dump_tables:
            resolved = self._resolve_bulk_dump_table(
                table_type, table_value, task, dag_result
            )
            if resolved and '.' in resolved:
                parts = resolved.split('.', 1)
                schema = parts[0]
                table = parts[1] if len(parts) > 1 else ''
                fqn = self.fqn_builder.build_fqn(connection_id, schema, table)
                item = OMEntityItem(EntityType.TABLE, fqn)
                if item not in outlets:
                    outlets.append(item)
        return outlets

    def _format_omentity_code(
        self,
        task_id: str,
        func_name: Optional[str],
        conn_id: str,
        conn_source: str,
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem],
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
            for item in inlets:
                if item.key:
                    lines.append(
                        f'    OMEntity(entity=Entity.{item.entity_type.value}, '
                        f'fqn="{item.fqn}", key="{item.key}"),'
                    )
                else:
                    lines.append(
                        f'    OMEntity(entity=Entity.{item.entity_type.value}, '
                        f'fqn="{item.fqn}"),'
                    )
            lines.append("],")
        else:
            lines.append("inlets=[],")

        # Код outlets
        if outlets:
            lines.append("outlets=[")
            for item in outlets:
                if item.key:
                    lines.append(
                        f'    OMEntity(entity=Entity.{item.entity_type.value}, '
                        f'fqn="{item.fqn}", key="{item.key}"),'
                    )
                else:
                    lines.append(
                        f'    OMEntity(entity=Entity.{item.entity_type.value}, '
                        f'fqn="{item.fqn}"),'
                    )
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
