"""Парсер Python DAG файлов через AST."""

import ast
import re
from typing import Optional, List, Dict, Tuple
from pathlib import Path

from .models import DAGParseResult, TaskInfo, FunctionInfo, CrossServerCall


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
                        func_info.connection_id = conn_id
                        func_info.connection_alias = conn_alias

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
                    if method_name in ('exec', 'exec_with_log', 'on_cluster'):
                        # Проверяем аргументы вызова
                        for arg in node.args:
                            if isinstance(arg, ast.Name) and arg.id in self.result.sql_variables:
                                used_sql_vars.add(arg.id)
                            # Также проверяем ast.Call внутри (например lambda или другая функция)
                            elif isinstance(arg, ast.Call):
                                for inner_arg in arg.args:
                                    if isinstance(inner_arg, ast.Name) and inner_arg.id in self.result.sql_variables:
                                        used_sql_vars.add(inner_arg.id)

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
                if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    take_data_inline = kw.value.value
                elif isinstance(kw.value, ast.Name):
                    take_data_var = kw.value.id
            elif kw.arg == 'insert_data':
                if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    insert_data_inline = kw.value.value
                elif isinstance(kw.value, ast.Name):
                    insert_data_var = kw.value.id
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
