"""Модели данных для анализатора DAG."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum


class EntityType(Enum):
    """Тип сущности для OMEntity."""
    TABLE = "TABLE"
    API = "API"
    TOPIC = "TOPIC"


class TableSource(Enum):
    """Источник ссылки на таблицу в SQL."""
    FROM = "FROM"
    JOIN = "JOIN"
    INSERT = "INSERT"
    DICTGET = "DICTGET"
    SUBQUERY = "SUBQUERY"


@dataclass
class TableReference:
    """Ссылка на таблицу, извлечённая из SQL."""
    schema: str
    table: str
    source: TableSource
    is_remote: bool = False
    remote_prefix: Optional[str] = None

    @property
    def full_name(self) -> str:
        """Возвращает полное имя таблицы: schema.table"""
        return f"{self.schema}.{self.table}"

    def __hash__(self):
        return hash((self.schema, self.table, self.is_remote, self.remote_prefix))

    def __eq__(self, other):
        if not isinstance(other, TableReference):
            return False
        return (self.schema == other.schema and
                self.table == other.table and
                self.is_remote == other.is_remote and
                self.remote_prefix == other.remote_prefix)


@dataclass
class SQLAnalysisResult:
    """Результат анализа SQL запроса."""
    inlets: List[TableReference] = field(default_factory=list)
    outlets: List[TableReference] = field(default_factory=list)
    dictionaries: List[TableReference] = field(default_factory=list)
    remote_tables: List[TableReference] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def merge(self, other: 'SQLAnalysisResult') -> 'SQLAnalysisResult':
        """Объединяет два результата анализа."""
        return SQLAnalysisResult(
            inlets=list(set(self.inlets + other.inlets)),
            outlets=list(set(self.outlets + other.outlets)),
            dictionaries=list(set(self.dictionaries + other.dictionaries)),
            remote_tables=list(set(self.remote_tables + other.remote_tables)),
            warnings=self.warnings + other.warnings
        )


@dataclass
class CrossServerCall:
    """Кросс-серверная операция copy_ch_to_ch_pipe."""
    take_data_var: Optional[str] = None       # SQL variable name for SELECT
    take_data_inline: Optional[str] = None    # Inline SQL string for SELECT
    insert_data_var: Optional[str] = None     # SQL variable name for INSERT
    insert_data_inline: Optional[str] = None  # Inline SQL string for INSERT
    src_connection: str = ""                   # Connection для source
    dst_connection: str = ""                   # Connection для destination


@dataclass
class FunctionInfo:
    """Информация о Python функции в DAG."""
    name: str
    decorators: List[str] = field(default_factory=list)
    connection_id: Optional[str] = None          # first/primary connection (backward compat)
    connection_alias: Optional[str] = None
    # All @with_db decorators in order: [(conn_var, alias), ...]
    with_db_connections: List[Tuple[str, str]] = field(default_factory=list)
    # Maps hook param name → conn variable name, e.g. {'ch6_hook': 'CH6_CONN'}
    hook_to_connection: Dict[str, str] = field(default_factory=dict)
    # Maps SQL var name → conn variable name (via hook analysis)
    sql_var_connections: Dict[str, str] = field(default_factory=dict)
    sql_variables: List[str] = field(default_factory=list)
    api_connections: List[str] = field(default_factory=list)
    bulk_dump_tables: List[tuple] = field(default_factory=list)  # [('value'|'param', table_name), ...]
    cross_server_calls: List[CrossServerCall] = field(default_factory=list)
    line_number: int = 0


@dataclass
class TaskInfo:
    """Информация о задаче (PythonOperator)."""
    task_id: str
    python_callable: Optional[str] = None
    existing_inlets: List[str] = field(default_factory=list)
    existing_outlets: List[str] = field(default_factory=list)
    op_kwargs_api: List[str] = field(default_factory=list)  # API connections из op_kwargs
    op_kwargs_dst_table: Optional[str] = None  # Целевая таблица из op_kwargs
    op_kwargs_all: Dict[str, str] = field(default_factory=dict)  # Все op_kwargs для резолвинга
    line_number: int = 0
    has_omentity: bool = False


@dataclass
class DAGParseResult:
    """Результат парсинга DAG файла."""
    dag_id: Optional[str] = None
    tasks: List[TaskInfo] = field(default_factory=list)
    functions: Dict[str, FunctionInfo] = field(default_factory=dict)
    sql_variables: Dict[str, str] = field(default_factory=dict)
    connection_variables: Dict[str, str] = field(default_factory=dict)
    string_variables: Dict[str, str] = field(default_factory=dict)  # Все строковые переменные
    imports: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class OMEntityItem:
    """Один элемент OMEntity (inlet или outlet) с опциональным key."""
    entity_type: EntityType
    fqn: str
    key: Optional[str] = None

    def __hash__(self):
        return hash((self.entity_type, self.fqn))

    def __eq__(self, other):
        if not isinstance(other, OMEntityItem):
            return False
        return self.entity_type == other.entity_type and self.fqn == other.fqn


@dataclass
class DataFlowGroup:
    """Группа потока данных (inlets -> outlets) для одного логического шага."""
    flow_type: str  # 'api', 'sql', 'cross_server', 'bulk_dump'
    inlets: List[OMEntityItem] = field(default_factory=list)
    outlets: List[OMEntityItem] = field(default_factory=list)
    source_description: str = ""


@dataclass
class OMEntityOutput:
    """Сгенерированный OMEntity для задачи."""
    task_id: str
    function_name: str
    connection_id: str
    connection_source: str
    inlets: List[OMEntityItem] = field(default_factory=list)
    outlets: List[OMEntityItem] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    generated_code: str = ""
