"""Алгоритм автоматического назначения key-групп для OMEntity."""

from typing import List, Tuple

from analyzer.models import OMEntityItem, DataFlowGroup


class KeyAssigner:
    """
    Назначает key-группы для потоков данных.

    Правила:
    1. Если один поток данных → key не нужен (None)
    2. Если несколько потоков → ключи '1', '2', '3'...
    3. Если нет outlets → key не ставим
    """

    def assign_keys(
        self, flows: List[DataFlowGroup]
    ) -> Tuple[List[OMEntityItem], List[OMEntityItem]]:
        """
        Назначает key-группы для списка потоков данных.

        Args:
            flows: Список потоков данных (DataFlowGroup)

        Returns:
            (inlets, outlets) — списки OMEntityItem с назначенными key
        """
        if not flows:
            return [], []

        # Фильтруем пустые потоки
        non_empty = [f for f in flows if f.inlets or f.outlets]
        if not non_empty:
            return [], []

        # Один поток → без key
        has_outlets = any(f.outlets for f in non_empty)
        if len(non_empty) == 1 or not has_outlets:
            all_inlets = []
            all_outlets = []
            for flow in non_empty:
                all_inlets.extend(flow.inlets)
                all_outlets.extend(flow.outlets)
            return _dedupe(all_inlets), _dedupe(all_outlets)

        # Несколько потоков → назначаем key
        all_inlets = []
        all_outlets = []
        for i, flow in enumerate(non_empty, 1):
            key = str(i)
            for item in flow.inlets:
                all_inlets.append(OMEntityItem(
                    entity_type=item.entity_type,
                    fqn=item.fqn,
                    key=key
                ))
            for item in flow.outlets:
                all_outlets.append(OMEntityItem(
                    entity_type=item.entity_type,
                    fqn=item.fqn,
                    key=key
                ))

        return _dedupe_with_key(all_inlets), _dedupe_with_key(all_outlets)


def _dedupe(items: List[OMEntityItem]) -> List[OMEntityItem]:
    """Убирает дубликаты по (entity_type, fqn), сохраняя порядок."""
    seen = set()
    result = []
    for item in items:
        ident = (item.entity_type, item.fqn)
        if ident not in seen:
            seen.add(ident)
            result.append(item)
    return result


def _dedupe_with_key(items: List[OMEntityItem]) -> List[OMEntityItem]:
    """Убирает дубликаты по (entity_type, fqn, key), сохраняя порядок."""
    seen = set()
    result = []
    for item in items:
        ident = (item.entity_type, item.fqn, item.key)
        if ident not in seen:
            seen.add(ident)
            result.append(item)
    return result
