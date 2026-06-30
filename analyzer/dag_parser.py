"""Парсер Python DAG файлов через AST."""

import ast
import re
from typing import Optional, List, Dict, Tuple
from pathlib import Path

from .models import DAGParseResult, TaskInfo, FunctionInfo, CrossServerCall, BulkDumpTransfer


class DAGParser:
    """Парсер Airflow DAG файлов."""

    # Ключевые слова SQL для эвристики
    SQL_KEYWORDS = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'JOIN',
                    'WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'WITH'}

    def __init__(self):
        self.result: Optional[DAGParseResult] = None

    def parse_file(self, filepath: str) -> DAGParseResult:
        """Парсит DAG файл и возвращает структурированный результат."""
        path = Path(filepath)
        with open(path, 'r', encoding='utf-8') as f:
            code = f.read()
        return self.parse_code(code)

    def parse_code(self, code: str) -> DAGParseResult:
        """Парсит Python код напрямую."""
        self.result = DAGParseResult()

        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            self.result.warnings.append(f"Syntax error: {e}")
            return self.result

        # Извлекаем данные
        self._extract_imports(tree)
        self._extract_connection_variables(tree)
        self._extract_string_variables(tree)
        self._extract_sql_variables(tree)
        self._extract_functions(tree)
        self._extract_tasks(tree)
        self._extract_dag_id(tree)

        # Извлекаем cross-server вызовы ПЕРЕД линковкой SQL
        self._extract_cross_server_calls(tree)

        # Связываем функции с SQL переменными через AST
        # (после cross-server, чтобы исключить SQL переменные из copy_ch_to_ch_pipe)
        self._link_functions_to_sql(tree)

        # Извлекаем API connections и bulk_dump tables
        self._extract_api_usage(tree)
        self._extract_bulk_dump_tables(tree)
        self._extract_bulk_dump_transfers(tree)
        self._propagate_lineage_through_calls(tree)

        return self.result

    def _extract_imports(self, tree: ast.Module):
        """Извлекает импорты."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.result.imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    self.result.imports.append(f"{module}.{alias.name}")

    def _extract_connection_variables(self, tree: ast.Module):
        """Извлекает переменные *_CONN_ID = "..."."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_name = target.id
                        # Проверяем паттерн *_CONN_ID, *_CONNECTION_ID или *_CONN
                        if (var_name.endswith('_CONN_ID') or
                            var_name.endswith('_CONNECTION_ID') or
                            var_name.endswith('_CONN')):
                            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                                self.result.connection_variables[var_name] = node.value.value

    def _extract_string_variables(self, tree: ast.Module):
        """Извлекает все строковые переменные (для резолвинга op_kwargs)."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_name = target.id
                        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                            self.result.string_variables[var_name] = node.value.value

    def _extract_sql_variables(self, tree: ast.Module):
        """Извлекает переменные содержащие SQL."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_name = target.id
                        value = self._get_string_value(node.value)
                        if value and self._is_sql_string(value):
                            self.result.sql_variables[var_name] = value

    def _extract_functions(self, tree: ast.Module):
        """Извлекает все функции с их декораторами."""
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_info = FunctionInfo(
                    name=node.name,
                    line_number=node.lineno
                )

                # Парсим декораторы
                for decorator in node.decorator_list:
                    dec_name, dec_args = self._parse_decorator(decorator)
                    func_info.decorators.append(dec_name)

                    # Проверяем на @with_db
                    if dec_name == 'with_db' and dec_args:
                        conn_id, conn_alias = self._parse_with_db_args(dec_args)
                        func_info.with_db_connections.append((conn_id or '', conn_alias or ''))
                        # First decorator = primary connection (backward compat)
                        if func_info.connection_id is None:
                            func_info.connection_id = conn_id
                            func_info.connection_alias = conn_alias

                # Map hook params → connections by alias matching
                if len(func_info.with_db_connections) > 1:
                    self._map_hooks_to_connections(node, func_info)

                self.result.functions[node.name] = func_info

    def _extract_tasks(self, tree: ast.Module):
        """Извлекает PythonOperator задачи."""
        for node in ast.walk(tree):
            call = None
            lineno = 0

            if isinstance(node, ast.Assign):
                # Assigned: task_var = PythonOperator(...)
                if isinstance(node.value, ast.Call):
                    call = node.value
                    lineno = node.lineno
            elif isinstance(node, ast.Expr):
                # Unassigned: PythonOperator(...)  (e.g. inside TaskGroup)
                if isinstance(node.value, ast.Call):
                    call = node.value
                    lineno = node.lineno

            if call:
                func_name = self._get_call_name(call)
                if func_name in ('PythonOperator', 'ShortCircuitOperator'):
                    task_info = self._parse_operator_call(call, lineno)
                    if task_info:
                        self.result.tasks.append(task_info)

    def _extract_dag_id(self, tree: ast.Module):
        """Извлекает dag_id из DAG(...)."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = self._get_call_name(node)
                if func_name == 'DAG':
                    # Ищем dag_id в аргументах
                    for keyword in node.keywords:
                        if keyword.arg == 'dag_id':
                            if isinstance(keyword.value, ast.Constant):
                                self.result.dag_id = keyword.value.value
                                return
                    # Или первый позиционный аргумент
                    if node.args and isinstance(node.args[0], ast.Constant):
                        self.result.dag_id = node.args[0].value

    def _parse_operator_call(self, call: ast.Call, lineno: int) -> Optional[TaskInfo]:
        """Парсит вызов PythonOperator."""
        task_info = TaskInfo(
            task_id='',
            line_number=lineno
        )

        for keyword in call.keywords:
            if keyword.arg == 'task_id':
                if isinstance(keyword.value, ast.Constant):
                    task_info.task_id = keyword.value.value

            elif keyword.arg == 'python_callable':
                if isinstance(keyword.value, ast.Name):
                    task_info.python_callable = keyword.value.id
                elif isinstance(keyword.value, ast.Attribute):
                    task_info.python_callable = keyword.value.attr

            elif keyword.arg == 'inlets':
                task_info.has_omentity = True
                task_info.existing_inlets = self._extract_omentity_fqns(keyword.value)

            elif keyword.arg == 'outlets':
                task_info.has_omentity = True
                task_info.existing_outlets = self._extract_omentity_fqns(keyword.value)

            elif keyword.arg == 'op_kwargs':
                # Парсим op_kwargs для API connections и dst_table
                self._parse_op_kwargs(keyword.value, task_info)

        if not task_info.task_id:
            return None

        return task_info

    def _parse_op_kwargs(self, node: ast.expr, task_info: TaskInfo):
        """Парсит op_kwargs=dict(...) для извлечения API connections и dst_table."""
        if not isinstance(node, ast.Call):
            return

        call_name = self._get_call_name(node)
        if call_name != 'dict':
            return

        for keyword in node.keywords:
            if keyword.arg:
                value = self._resolve_value(keyword.value)
                if value:
                    # Сохраняем все kwargs для резолвинга параметров функции
                    task_info.op_kwargs_all[keyword.arg] = value

                    # API connection (api_conn, api_connection, http_conn и т.д.)
                    if 'api' in keyword.arg.lower() or 'conn' in keyword.arg.lower():
                        task_info.op_kwargs_api.append(value)

                    # Целевая таблица (dst_table, table, target_table и т.д.)
                    if 'table' in keyword.arg.lower() or 'dst' in keyword.arg.lower():
                        task_info.op_kwargs_dst_table = value

    def _extract_omentity_fqns(self, node: ast.expr) -> List[str]:
        """Извлекает FQN из списка OMEntity."""
        fqns = []
        if isinstance(node, ast.List):
            for elem in node.elts:
                if isinstance(elem, ast.Call):
                    # Ищем fqn= в аргументах
                    for keyword in elem.keywords:
                        if keyword.arg == 'fqn':
                            if isinstance(keyword.value, ast.Constant):
                                fqns.append(keyword.value.value)
                            elif isinstance(keyword.value, ast.JoinedStr):
                                # f-string - пробуем извлечь
                                fqns.append(self._extract_fstring(keyword.value))
        return fqns

    def _extract_fstring(self, node: ast.JoinedStr) -> str:
        """Извлекает значение из f-string."""
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                parts.append('{...}')  # placeholder
        return ''.join(parts)

    def _parse_decorator(self, decorator: ast.expr) -> Tuple[str, List]:
        """Парсит декоратор и возвращает (имя, аргументы)."""
        if isinstance(decorator, ast.Name):
            return decorator.id, []
        elif isinstance(decorator, ast.Call):
            name = self._get_call_name(decorator)
            return name, decorator.args + [kw for kw in decorator.keywords]
        elif isinstance(decorator, ast.Attribute):
            return decorator.attr, []
        return '', []

    def _parse_with_db_args(self, args: List) -> Tuple[Optional[str], Optional[str]]:
        """Парсит аргументы @with_db(CONN_ID, "alias")."""
        conn_id = None
        alias = None

        for i, arg in enumerate(args):
            if isinstance(arg, ast.keyword):
                continue  # Пропускаем keyword аргументы

            if i == 0:
                # Первый аргумент - connection ID
                if isinstance(arg, ast.Name):
                    conn_id = arg.id
                elif isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    conn_id = arg.value

            elif i == 1:
                # Второй аргумент - alias
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    alias = arg.value

        return conn_id, alias

    def _map_hooks_to_connections(self, func_node: ast.FunctionDef, func_info: FunctionInfo):
        """Maps function hook parameters to @with_db connections by alias matching.

        Convention: @with_db(CONN, 'alias') injects a hook param named '<alias>_hook'.
        We match param names containing the alias to the corresponding connection.
        """
        # Get function parameter names (excluding **kwargs, *args)
        func_params = [arg.arg for arg in func_node.args.args]

        for conn_var, alias in func_info.with_db_connections:
            if not alias:
                continue
            # Find param(s) that contain the alias (e.g. 'ch6' matches 'ch6_hook', 'ch6_curs')
            for param in func_params:
                if alias in param:
                    func_info.hook_to_connection[param] = conn_var

    def _propagate_lineage_through_calls(self, tree: ast.Module):
        """Пробрасывает lineage из helper-функций в PythonOperator callable."""
        func_nodes = self._get_function_nodes(tree)
        changed = True
        while changed:
            changed = False
            for caller_name, caller_node in func_nodes.items():
                caller_info = self.result.functions.get(caller_name)
                if not caller_info:
                    continue

                for call, callee_name in self._iter_known_function_calls(caller_node):
                    callee_info = self.result.functions.get(callee_name)
                    callee_node = func_nodes.get(callee_name)
                    if not callee_info or not callee_node or callee_name == caller_name:
                        continue
                    hook_arg_mapping = self._build_hook_arg_mapping(
                        caller_info, callee_node, call
                    )
                    if self._merge_function_lineage(caller_info, callee_info, hook_arg_mapping):
                        changed = True

    def _build_hook_arg_mapping(
        self,
        caller_info: FunctionInfo,
        callee_node: ast.FunctionDef,
        call: ast.Call,
    ) -> Dict[str, Tuple[str, str]]:
        """Maps callee hook params to caller connections or caller hook params."""
        mapping: Dict[str, Tuple[str, str]] = {}
        callee_params = self._get_function_param_names(callee_node)

        for index, arg in enumerate(call.args):
            if index < len(callee_params):
                self._map_call_hook_arg(mapping, callee_params[index], arg, caller_info)

        for keyword in call.keywords:
            if keyword.arg:
                self._map_call_hook_arg(mapping, keyword.arg, keyword.value, caller_info)

        return mapping

    def _map_call_hook_arg(
        self,
        mapping: Dict[str, Tuple[str, str]],
        callee_param: str,
        arg: ast.expr,
        caller_info: FunctionInfo,
    ) -> None:
        if not isinstance(arg, ast.Name):
            return
        caller_name = arg.id
        if caller_name in caller_info.hook_to_connection:
            mapping[callee_param] = ('connection', caller_info.hook_to_connection[caller_name])
        else:
            mapping[callee_param] = ('hook', caller_name)

    def _merge_function_lineage(
        self,
        target: FunctionInfo,
        source: FunctionInfo,
        hook_arg_mapping: Dict[str, Tuple[str, str]],
    ) -> bool:
        changed = False

        for sql_var in source.sql_variables:
            if sql_var not in target.sql_variables:
                target.sql_variables.append(sql_var)
                changed = True

            changed = self._merge_sql_var_binding(
                target, source, sql_var, hook_arg_mapping
            ) or changed

        for api_conn in source.api_connections:
            if api_conn not in target.api_connections:
                target.api_connections.append(api_conn)
                changed = True

        for table_info in source.bulk_dump_tables:
            if table_info not in target.bulk_dump_tables:
                target.bulk_dump_tables.append(table_info)
                changed = True

        for call in source.cross_server_calls:
            if call not in target.cross_server_calls:
                target.cross_server_calls.append(call)
                changed = True

        for transfer in source.bulk_dump_transfers:
            remapped_transfer = self._remap_bulk_dump_transfer(transfer, hook_arg_mapping)
            if remapped_transfer not in target.bulk_dump_transfers:
                target.bulk_dump_transfers.append(remapped_transfer)
                changed = True

        return changed

    def _merge_sql_var_binding(
        self,
        target: FunctionInfo,
        source: FunctionInfo,
        sql_var: str,
        hook_arg_mapping: Dict[str, Tuple[str, str]],
    ) -> bool:
        changed = False

        connection_bindings = [
            conn_var
            for var_name, conn_var in source.sql_var_connection_bindings
            if var_name == sql_var
        ]
        if not connection_bindings and sql_var in source.sql_var_connections:
            connection_bindings = [source.sql_var_connections[sql_var]]

        hook_bindings = [
            hook_name
            for var_name, hook_name in source.sql_var_hook_bindings
            if var_name == sql_var
        ]
        if not hook_bindings and sql_var in source.sql_var_hooks:
            hook_bindings = [source.sql_var_hooks[sql_var]]

        remapped_hook = False
        for source_hook in hook_bindings:
            if source_hook in hook_arg_mapping:
                remapped_hook = True
                binding_type, binding_value = hook_arg_mapping[source_hook]
                if binding_type == 'connection':
                    changed = self._add_sql_var_connection(
                        target, sql_var, binding_value
                    ) or changed
                else:
                    changed = self._add_sql_var_hook(
                        target, sql_var, binding_value
                    ) or changed
            else:
                changed = self._add_sql_var_hook(target, sql_var, source_hook) or changed

        if not remapped_hook:
            for conn_var in connection_bindings:
                changed = self._add_sql_var_connection(target, sql_var, conn_var) or changed

        return changed

    def _add_sql_var_connection(
        self,
        func_info: FunctionInfo,
        sql_var: str,
        conn_var: str,
    ) -> bool:
        if not conn_var:
            return False
        binding = (sql_var, conn_var)
        changed = False
        if binding not in func_info.sql_var_connection_bindings:
            func_info.sql_var_connection_bindings.append(binding)
            changed = True
        if sql_var not in func_info.sql_var_connections:
            func_info.sql_var_connections[sql_var] = conn_var
        return changed

    def _add_sql_var_hook(
        self,
        func_info: FunctionInfo,
        sql_var: str,
        hook_name: str,
    ) -> bool:
        if not hook_name:
            return False
        binding = (sql_var, hook_name)
        changed = False
        if binding not in func_info.sql_var_hook_bindings:
            func_info.sql_var_hook_bindings.append(binding)
            changed = True
        if sql_var not in func_info.sql_var_hooks:
            func_info.sql_var_hooks[sql_var] = hook_name
        return changed

    def _remap_bulk_dump_transfer(
        self,
        transfer: BulkDumpTransfer,
        hook_arg_mapping: Dict[str, Tuple[str, str]],
    ) -> BulkDumpTransfer:
        source_connection, source_hook = self._remap_hook_binding(
            transfer.source_connection,
            transfer.source_hook,
            hook_arg_mapping,
        )
        dst_connection, dst_hook = self._remap_hook_binding(
            transfer.dst_connection,
            transfer.dst_hook,
            hook_arg_mapping,
        )
        return BulkDumpTransfer(
            sql_var=transfer.sql_var,
            source_connection=source_connection,
            source_hook=source_hook,
            dst_table=transfer.dst_table,
            dst_connection=dst_connection,
            dst_hook=dst_hook,
        )

    def _remap_hook_binding(
        self,
        connection: str,
        hook: str,
        hook_arg_mapping: Dict[str, Tuple[str, str]],
    ) -> Tuple[str, str]:
        if hook and hook in hook_arg_mapping:
            binding_type, binding_value = hook_arg_mapping[hook]
            if binding_type == 'connection':
                return binding_value, ''
            return '', binding_value
        return connection, hook

    def _get_function_nodes(self, tree: ast.Module) -> Dict[str, ast.FunctionDef]:
        return {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

    def _get_function_param_names(self, func_node: ast.FunctionDef) -> List[str]:
        return [arg.arg for arg in func_node.args.args]

    def _iter_known_function_calls(self, func_node: ast.FunctionDef):
        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                if call_name in self.result.functions:
                    yield node, call_name

    def _get_call_name(self, call: ast.Call) -> str:
        """Получает имя вызываемой функции."""
        if isinstance(call.func, ast.Name):
            return call.func.id
        elif isinstance(call.func, ast.Attribute):
            return call.func.attr
        return ''

    def _get_string_value(self, node: ast.expr) -> Optional[str]:
        """Извлекает строковое значение из AST узла."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        elif isinstance(node, ast.JoinedStr):
            # f-string - собираем части
            return self._extract_fstring(node)
        return None

    def _is_sql_string(self, value: str) -> bool:
        """Эвристика для определения SQL в строке."""
        upper_value = value.upper()
        # Должно содержать минимум 2 SQL ключевых слова
        matches = sum(1 for kw in self.SQL_KEYWORDS if kw in upper_value)
        return matches >= 2

    def _link_functions_to_sql(self, tree: ast.Module):
        """Связывает функции с SQL переменными через анализ AST тела функции."""
        # Собираем AST ноды функций
        func_nodes: Dict[str, ast.FunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_nodes[node.name] = node

        for func_name, func_info in self.result.functions.items():
            if func_name not in func_nodes:
                continue

            func_node = func_nodes[func_name]
            used_sql_vars = set()

            # Собираем SQL переменные, используемые в cross-server вызовах —
            # они обрабатываются отдельно с правильными connection_id
            cross_server_sql_vars = set()
            for cs_call in func_info.cross_server_calls:
                if cs_call.take_data_var:
                    cross_server_sql_vars.add(cs_call.take_data_var)
                if cs_call.insert_data_var:
                    cross_server_sql_vars.add(cs_call.insert_data_var)

            # Анализируем тело функции
            for node in ast.walk(func_node):
                # Прямое использование SQL переменной
                if isinstance(node, ast.Name) and node.id in self.result.sql_variables:
                    used_sql_vars.add(node.id)

                # Вызовы hook.exec_with_log(SQL_VAR, ...) и подобные
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    method_name = node.func.attr
                    if method_name in ('exec', 'exec_with_log', 'execute', 'get_records', 'on_cluster'):
                        # Get the hook object name (e.g. 'ch6_hook' from ch6_hook.exec_with_log(...))
                        hook_name = None
                        if isinstance(node.func.value, ast.Name):
                            hook_name = node.func.value.id

                        # Проверяем аргументы вызова
                        for arg in node.args:
                            sql_var = self._resolve_sql_arg(arg)
                            if sql_var and sql_var in self.result.sql_variables:
                                used_sql_vars.add(sql_var)
                                if hook_name:
                                    self._add_sql_var_hook(func_info, sql_var, hook_name)
                                # Track which hook executes this SQL var
                                if hook_name and hook_name in func_info.hook_to_connection:
                                    self._add_sql_var_connection(
                                        func_info,
                                        sql_var,
                                        func_info.hook_to_connection[hook_name],
                                    )

            # Исключаем SQL переменные из cross-server вызовов
            used_sql_vars -= cross_server_sql_vars

            func_info.sql_variables = list(used_sql_vars)

    def _extract_api_usage(self, tree: ast.Module):
        """Извлекает HttpHook вызовы из функций."""
        # Собираем AST ноды функций
        func_nodes: Dict[str, ast.FunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_nodes[node.name] = node

        for func_name, func_info in self.result.functions.items():
            if func_name not in func_nodes:
                continue

            func_node = func_nodes[func_name]
            api_connections = []

            # Получаем имена параметров функции чтобы исключить их
            func_params = set()
            for arg in func_node.args.args:
                func_params.add(arg.arg)
            for arg in func_node.args.kwonlyargs:
                func_params.add(arg.arg)

            for node in ast.walk(func_node):
                if isinstance(node, ast.Call):
                    call_name = self._get_call_name(node)
                    if call_name == 'HttpHook':
                        # Ищем http_conn_id в keyword аргументах
                        for kw in node.keywords:
                            if kw.arg == 'http_conn_id':
                                conn = self._resolve_value(kw.value)
                                # Исключаем параметры функции - они передаются через op_kwargs
                                if conn and conn not in func_params:
                                    api_connections.append(conn)

            func_info.api_connections = api_connections

    def _extract_bulk_dump_tables(self, tree: ast.Module):
        """Извлекает bulk_dump(table=...) вызовы из функций."""
        # Собираем AST ноды функций
        func_nodes: Dict[str, ast.FunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_nodes[node.name] = node

        for func_name, func_info in self.result.functions.items():
            if func_name not in func_nodes:
                continue

            func_node = func_nodes[func_name]
            bulk_dump_tables = []

            # Получаем имена параметров функции чтобы распознать их
            func_params = set()
            for arg in func_node.args.args:
                func_params.add(arg.arg)
            for arg in func_node.args.kwonlyargs:
                func_params.add(arg.arg)

            for node in ast.walk(func_node):
                if isinstance(node, ast.Call):
                    # Проверяем на *.bulk_dump(...)
                    if isinstance(node.func, ast.Attribute) and node.func.attr == 'bulk_dump':
                        # Ищем table= в keyword аргументах
                        for kw in node.keywords:
                            if kw.arg == 'table':
                                table_value = self._resolve_value(kw.value)
                                if table_value:
                                    # Помечаем если это параметр функции (резолвится из op_kwargs)
                                    if table_value in func_params:
                                        bulk_dump_tables.append(('param', table_value))
                                    else:
                                        bulk_dump_tables.append(('value', table_value))

            func_info.bulk_dump_tables = bulk_dump_tables

    def _extract_bulk_dump_transfers(self, tree: ast.Module):
        """Извлекает паттерн cursor.execute(SQL) -> hook.bulk_dump(table=...)."""
        func_nodes = self._get_function_nodes(tree)

        for func_name, func_info in self.result.functions.items():
            func_node = func_nodes.get(func_name)
            if not func_node:
                continue

            transfers: List[BulkDumpTransfer] = []

            for for_node in ast.walk(func_node):
                if not isinstance(for_node, ast.For):
                    continue

                loop_vars = self._extract_loop_target_names(for_node.target)
                rows = self._extract_loop_rows(for_node.iter)
                if not loop_vars or not rows:
                    continue

                for row in rows:
                    loop_values = {
                        name: self._resolve_loop_value(value)
                        for name, value in zip(loop_vars, row)
                    }

                    sql_sources: List[Tuple[str, str, str]] = []
                    bulk_targets: List[Tuple[str, str, str]] = []

                    for node in ast.walk(for_node):
                        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                            continue

                        method_name = node.func.attr
                        receiver = node.func.value.id if isinstance(node.func.value, ast.Name) else None
                        receiver_conn = func_info.hook_to_connection.get(receiver or "", "")

                        if method_name in ("execute", "get_records"):
                            for arg in node.args:
                                sql_var = self._resolve_sql_arg(arg)
                                if sql_var in loop_values:
                                    sql_var = loop_values[sql_var]
                                if sql_var and sql_var in self.result.sql_variables:
                                    sql_sources.append((sql_var, receiver_conn, receiver or ""))

                        elif method_name == "bulk_dump":
                            for kw in node.keywords:
                                if kw.arg != "table":
                                    continue
                                table_value = self._resolve_value(kw.value)
                                if table_value in loop_values:
                                    table_value = loop_values[table_value]
                                if table_value and table_value in self.result.string_variables:
                                    table_value = self.result.string_variables[table_value]
                                if table_value and "." in table_value:
                                    bulk_targets.append((table_value, receiver_conn, receiver or ""))

                    for sql_var, source_conn, source_hook in sql_sources:
                        for dst_table, dst_conn, dst_hook in bulk_targets:
                            transfer = BulkDumpTransfer(
                                sql_var=sql_var,
                                source_connection=source_conn,
                                source_hook=source_hook,
                                dst_table=dst_table,
                                dst_connection=dst_conn,
                                dst_hook=dst_hook,
                            )
                            if transfer not in transfers:
                                transfers.append(transfer)

                            if sql_var not in func_info.sql_variables:
                                func_info.sql_variables.append(sql_var)
                            if source_conn:
                                self._add_sql_var_connection(func_info, sql_var, source_conn)
                            if source_hook:
                                self._add_sql_var_hook(func_info, sql_var, source_hook)

            func_info.bulk_dump_transfers = transfers

    def _extract_loop_target_names(self, target: ast.expr) -> List[str]:
        if isinstance(target, ast.Name):
            return [target.id]
        if isinstance(target, (ast.Tuple, ast.List)):
            names = []
            for item in target.elts:
                if isinstance(item, ast.Name):
                    names.append(item.id)
            return names
        return []

    def _extract_loop_rows(self, node: ast.expr) -> List[List[ast.expr]]:
        if not isinstance(node, (ast.Tuple, ast.List)):
            return []
        rows: List[List[ast.expr]] = []
        for item in node.elts:
            if isinstance(item, (ast.Tuple, ast.List)):
                rows.append(list(item.elts))
        return rows

    def _resolve_loop_value(self, node: ast.expr) -> Optional[str]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return self._resolve_sql_arg(node)
        return None

    def _extract_cross_server_calls(self, tree: ast.Module):
        """Извлекает copy_ch_to_ch_pipe вызовы из функций."""
        # Собираем AST ноды функций
        func_nodes: Dict[str, ast.FunctionDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_nodes[node.name] = node

        for func_name, func_info in self.result.functions.items():
            if func_name not in func_nodes:
                continue

            func_node = func_nodes[func_name]
            cross_server_calls = []

            for node in ast.walk(func_node):
                if isinstance(node, ast.Call):
                    call_name = self._get_call_name(node)
                    if call_name == 'copy_ch_to_ch_pipe':
                        call_info = self._parse_copy_ch_to_ch_pipe(node)
                        if call_info:
                            cross_server_calls.append(call_info)

            func_info.cross_server_calls = cross_server_calls

    def _parse_copy_ch_to_ch_pipe(self, call: ast.Call) -> Optional[CrossServerCall]:
        """Парсит вызов copy_ch_to_ch_pipe и извлекает параметры."""
        take_data_var = None
        take_data_inline = None
        insert_data_var = None
        insert_data_inline = None
        src_ch = None
        dst_ch = None

        for kw in call.keywords:
            if kw.arg == 'take_data':
                sql_var = self._resolve_sql_arg(kw.value)
                if sql_var:
                    take_data_var = sql_var
                elif isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    take_data_inline = kw.value.value
            elif kw.arg == 'insert_data':
                sql_var = self._resolve_sql_arg(kw.value)
                if sql_var:
                    insert_data_var = sql_var
                elif isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    insert_data_inline = kw.value.value
            elif kw.arg == 'src_ch':
                src_ch = self._resolve_value(kw.value)
            elif kw.arg == 'dst_ch':
                dst_ch = self._resolve_value(kw.value)

        has_take = take_data_var or take_data_inline
        has_insert = insert_data_var or insert_data_inline

        if all([has_take, has_insert, src_ch, dst_ch]):
            return CrossServerCall(
                take_data_var=take_data_var,
                take_data_inline=take_data_inline,
                insert_data_var=insert_data_var,
                insert_data_inline=insert_data_inline,
                src_connection=src_ch,
                dst_connection=dst_ch
            )
        return None

    def _resolve_sql_arg(self, node: ast.expr) -> Optional[str]:
        """Резолвит SQL-переменную из прямого имени, SQL.format(...) или SQL % dict(...)."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return self._resolve_sql_arg(node.func.value)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return self._resolve_sql_arg(node.left)
        return None

    def _resolve_value(self, node: ast.expr) -> Optional[str]:
        """Резолвит значение AST узла в строку или имя переменной."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        elif isinstance(node, ast.Name):
            # Возвращаем имя переменной для последующего резолвинга
            return node.id
        elif isinstance(node, ast.Call):
            # Для вызовов типа SQL.format(...) берём первый аргумент
            if isinstance(node.func, ast.Attribute):
                if isinstance(node.func.value, ast.Name):
                    return node.func.value.id
        return None
