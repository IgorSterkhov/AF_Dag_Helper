"""FQN Builder с маппингом серверов."""

import fnmatch
import yaml
from pathlib import Path
from typing import Optional, Dict


class FQNBuilder:
    """Строит FQN с учётом маппинга серверов."""

    def __init__(self, mapping_file: Optional[str] = None):
        self.mapping: Dict[str, str] = {}
        self.default_behavior = "passthrough"

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
        return f"{server}.{schema}.{table}"

    def build_fqn_for_remote(self, remote_prefix: str, schema: str, table: str) -> str:
        """
        Строит FQN для remote_* таблиц.

        Для remote таблиц схема обычно уже включена в prefix.
        Например: remote_ch.last_srid_position_v3 -> remote_ch.positions.last_srid_position_v3
        Или если schema=remote_ch, table=last_srid_position_v3 -> remote_ch.last_srid_position_v3
        """
        server = self.get_server_name(remote_prefix)
        # Для remote таблиц schema часто совпадает с remote_prefix
        if schema == remote_prefix:
            return f"{server}.{table}"
        return f"{server}.{schema}.{table}"
