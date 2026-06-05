"""FQN Builder с маппингом серверов."""

import fnmatch
import re
import yaml
from pathlib import Path
from typing import Optional, Dict


# Префикс имени таблицы, кодирующий целевой сервер: `ch1_*`, `ch13_*`, `pg3_*`, `dm0_*`, ...
# Используется для таблиц из схемы `remote` (обёртки ClickHouse remote()),
# где реальный сервер не совпадает с подключением, в котором зарегистрирована таблица.
_REMOTE_TABLE_PREFIX_RE = re.compile(r'^([a-z]+\d+)_', re.IGNORECASE)


class FQNBuilder:
    """Строит FQN с учётом маппинга серверов."""

    def __init__(self, mapping_file: Optional[str] = None):
        self.mapping: Dict[str, str] = {}
        self.default_behavior = "passthrough"
        self.strip_d_suffix = True  # Удалять суффикс _d из таблиц

        if mapping_file:
            self.load_mapping(mapping_file)

    def load_mapping(self, filepath: str):
        """Загружает маппинг из YAML файла."""
        path = Path(filepath)
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                    self.mapping = config.get('server_mapping', {}) or {}
                    self.default_behavior = config.get('default_behavior', 'passthrough')
                    self.strip_d_suffix = config.get('strip_d_suffix', True)
            except Exception:
                pass

    def save_mapping(self, filepath: str):
        """Сохраняет маппинг в YAML файл."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        config = {
            'server_mapping': self.mapping,
            'default_behavior': self.default_behavior
        }

        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)

    def add_mapping(self, connection_id: str, server_name: str):
        """Добавляет маппинг connection -> server."""
        self.mapping[connection_id] = server_name

    def remove_mapping(self, connection_id: str):
        """Удаляет маппинг."""
        if connection_id in self.mapping:
            del self.mapping[connection_id]

    def normalize_table_name(self, table: str) -> str:
        """Удаляет суффикс _d из имени таблицы если включено."""
        if self.strip_d_suffix and table.endswith('_d'):
            return table[:-2]
        return table

    def get_server_name(self, connection_id: str) -> str:
        """Получает имя сервера для connection ID с поддержкой wildcard."""
        # 1. Сначала точное совпадение (быстрее и приоритетнее)
        if connection_id in self.mapping:
            return self.mapping[connection_id]

        # 2. Проверяем wildcard-паттерны (* и ?)
        for pattern, server_name in self.mapping.items():
            if '*' in pattern or '?' in pattern:
                if fnmatch.fnmatch(connection_id, pattern):
                    return server_name

        # 3. Passthrough если ничего не найдено
        return connection_id

    def build_fqn(self, connection_id: str, schema: str, table: str) -> str:
        """
        Строит FQN: server.schema.table

        Если есть маппинг connection_id -> server_name, использует его.
        Иначе passthrough: connection_id.schema.table
        """
        server = self.get_server_name(connection_id)
        normalized_table = self.normalize_table_name(table)
        return f"{server}.{schema}.{normalized_table}"

    def build_fqn_for_remote(self, connection_id: str, remote_prefix: str, table: str) -> str:
        """
        Строит FQN для remote_* таблиц.

        Формат: server.remote_prefix.table
        Пример: do-ch13.remote_ch.oof_position_status_v3_rc

        Для схемы 'remote' (обёртка ClickHouse remote()) пытаемся вывести реальный
        сервер из префикса имени таблицы (`ch1_*` → `do-ch1`, `pg3_*` → `do-pg3`).
        """
        conn = connection_id
        if remote_prefix == 'remote':
            inferred = self.infer_connection_from_table_name(table)
            if inferred:
                conn = inferred
        server = self.get_server_name(conn)
        normalized_table = self.normalize_table_name(table)
        return f"{server}.{remote_prefix}.{normalized_table}"

    def infer_connection_from_table_name(self, table: str) -> Optional[str]:
        """
        Выводит connection_id из префикса имени таблицы для схемы 'remote'.

        Пример: 'ch1_supply_gtd_raw_rc' → 'do-ch1'; 'pg3_orders' → 'do-pg3'.
        Возвращает None, если префикс не распознан.
        """
        match = _REMOTE_TABLE_PREFIX_RE.match(table)
        if not match:
            return None
        return f"do-{match.group(1).lower()}"
