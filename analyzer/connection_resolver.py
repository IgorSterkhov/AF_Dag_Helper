"""Резолвер connection ID для функций."""

from typing import Tuple, Optional

from .models import DAGParseResult


class ConnectionResolver:
    """Определяет connection ID для функций."""

    def __init__(self, dag_result: DAGParseResult):
        self.dag_result = dag_result

    def resolve_for_function(self, func_name: str) -> Tuple[Optional[str], str]:
        """
        Определяет connection ID для функции.

        Args:
            func_name: Имя функции

        Returns:
            (connection_id, source) где source: "decorator", "variable", "unknown"
        """
        func_info = self.dag_result.functions.get(func_name)
        if not func_info:
            return None, "unknown"

        # 1. Из декоратора @with_db
        if func_info.connection_id:
            conn_var = func_info.connection_id

            # Если это переменная - резолвим в значение
            if conn_var in self.dag_result.connection_variables:
                return self.dag_result.connection_variables[conn_var], "decorator"

            # Может быть строковым литералом (уже значение)
            if conn_var.startswith('"') or conn_var.startswith("'"):
                return conn_var.strip('"\''), "decorator"

            # Возвращаем как есть (может быть значением)
            return conn_var, "decorator"

        # 2. Пробуем найти connection по контексту
        # Ищем первый найденный *_CONN_ID
        for var_name, conn_value in self.dag_result.connection_variables.items():
            # Используем первый найденный
            return conn_value, "inferred"

        return None, "unknown"

    def resolve_for_sql_variable(self, func_name: str, sql_var: str) -> Tuple[Optional[str], str]:
        """
        Determines connection ID for a specific SQL variable within a function.

        Uses hook-to-SQL-var mapping from AST analysis. Falls back to
        function-level connection if no per-variable mapping exists.

        Args:
            func_name: Function name
            sql_var: SQL variable name

        Returns:
            (connection_id, source) where source: "hook_analysis", "decorator", etc.
        """
        func_info = self.dag_result.functions.get(func_name)
        if not func_info:
            return None, "unknown"

        # Check sql_var_connections first (from hook analysis)
        if sql_var in func_info.sql_var_connections:
            conn_var = func_info.sql_var_connections[sql_var]
            if conn_var in self.dag_result.connection_variables:
                return self.dag_result.connection_variables[conn_var], "hook_analysis"
            return conn_var, "hook_analysis"

        # Fallback to function-level connection
        return self.resolve_for_function(func_name)

    def resolve_remote_connection(self, remote_prefix: str) -> str:
        """
        Определяет connection для remote_* таблиц.

        Args:
            remote_prefix: Префикс (например, "remote_ch")

        Returns:
            Connection ID (по умолчанию используем префикс)
        """
        # По умолчанию используем префикс как connection
        return remote_prefix
