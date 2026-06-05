"""Конвертация OMEntityOutput в формат Cytoscape.js для интерактивных диаграмм."""

from typing import List, Dict, Any, Set, Tuple
from analyzer.models import OMEntityOutput, OMEntityItem, DataFlowGroup, EntityType


# Цветовая схема
COLORS = {
    'TABLE': {'bg': '#4FC3F7', 'border': '#0288D1', 'text': '#01579B'},
    'API': {'bg': '#FFB74D', 'border': '#F57C00', 'text': '#E65100'},
    'TOPIC': {'bg': '#CE93D8', 'border': '#8E24AA', 'text': '#4A148C'},
}


def _sanitize_id(s: str) -> str:
    """Очищает строку для использования как ID узла."""
    return s.replace('.', '_').replace('-', '_').replace(' ', '_')


def _entity_node_id(item: OMEntityItem, prefix: str = '') -> str:
    """Уникальный ID узла для сущности. prefix используется в DAG view для уникальности по task."""
    base = f"entity_{_sanitize_id(item.fqn)}"
    return f"{prefix}__{base}" if prefix else base


class CytoscapeGraphBuilder:
    """Конвертирует OMEntityOutput → Cytoscape.js элементы."""

    def _build_flow_elements(
        self,
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem],
        node_prefix: str = '',
        parent_id: str = '',
        flow_type: str = 'sql',
        seen_nodes: Set[str] = None,
    ) -> Tuple[list, list]:
        """
        Строит ноды и рёбра для одного потока inlets → outlets.
        Каждый inlet соединяется стрелкой с каждым outlet напрямую.
        """
        if seen_nodes is None:
            seen_nodes = set()

        nodes = []
        edges = []

        unique_inlets = list(dict.fromkeys(inlets))
        unique_outlets = list(dict.fromkeys(outlets))

        if not unique_inlets and not unique_outlets:
            return nodes, edges

        for item in unique_inlets + unique_outlets:
            nid = _entity_node_id(item, node_prefix)
            if nid not in seen_nodes:
                seen_nodes.add(nid)
                node_data = {
                    'id': nid,
                    'label': item.fqn,
                    'entity_type': item.entity_type.value,
                    'fqn': item.fqn,
                }
                if parent_id:
                    node_data['parent'] = parent_id
                nodes.append({'data': node_data})

        seen_edges = set()
        for inlet_item in unique_inlets:
            for outlet_item in unique_outlets:
                src = _entity_node_id(inlet_item, node_prefix)
                tgt = _entity_node_id(outlet_item, node_prefix)
                if src == tgt:
                    continue
                edge_key = (src, tgt)
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append({
                        'data': {
                            'id': f"{src}__{tgt}",
                            'source': src,
                            'target': tgt,
                            'edge_type': 'flow',
                            'flow_type': flow_type,
                        }
                    })

        return nodes, edges

    def build_task_view(self, output: OMEntityOutput) -> Dict[str, Any]:
        """
        Строит граф для одной задачи: inlets → outlets.
        Группирует таблицы по key (пунктирная рамка) если key назначен.
        Использует output.inlets/outlets (с key от KeyAssigner).
        """
        nodes = []
        edges = []
        seen_nodes: Set[str] = set()

        # Группируем по key
        keyed_inlets: Dict[str, List[OMEntityItem]] = {}
        keyed_outlets: Dict[str, List[OMEntityItem]] = {}

        for item in output.inlets:
            key = item.key or '__none__'
            keyed_inlets.setdefault(key, []).append(item)
        for item in output.outlets:
            key = item.key or '__none__'
            keyed_outlets.setdefault(key, []).append(item)

        all_keys = sorted(set(list(keyed_inlets.keys()) + list(keyed_outlets.keys())))

        for key in all_keys:
            inlets = keyed_inlets.get(key, [])
            outlets = keyed_outlets.get(key, [])

            # Compound parent для key-группы (только если key реально назначен)
            parent_id = ''
            if key != '__none__':
                parent_id = f"keygroup_{key}"
                nodes.append({
                    'data': {
                        'id': parent_id,
                        'label': f'key={key}',
                        'entity_type': 'key_group',
                    }
                })

            flow_nodes, flow_edges = self._build_flow_elements(
                inlets, outlets,
                node_prefix='',
                parent_id=parent_id,
                seen_nodes=seen_nodes,
            )
            nodes.extend(flow_nodes)
            edges.extend(flow_edges)

        return {
            'elements': {'nodes': nodes, 'edges': edges},
            'task_id': output.task_id,
        }

    def build_dag_view(self, outputs: List[OMEntityOutput], dag_id: str = '') -> Dict[str, Any]:
        """
        Строит граф для всего DAG:
        - Каждая задача = compound parent (рамка)
        - Таблицы внутри задач с полным FQN
        - Связи между задачами: outlet задачи A с тем же FQN что inlet задачи B
        """
        nodes = []
        edges = []
        seen_nodes: Set[str] = set()

        # Маппинг fqn → [(task_prefix, node_id), ...] для межтаскных связей
        fqn_outlets: Dict[str, List[tuple]] = {}  # fqn → [(task_prefix, node_id), ...]
        fqn_inlets: Dict[str, List[tuple]] = {}   # fqn → [(task_prefix, node_id), ...]

        for output in outputs:
            task_prefix = _sanitize_id(output.task_id)
            group_id = f"taskgroup_{task_prefix}"

            # Task group (compound parent)
            nodes.append({
                'data': {
                    'id': group_id,
                    'label': output.task_id,
                    'entity_type': 'task_group',
                }
            })

            # Собираем все inlets/outlets задачи
            all_inlets = []
            all_outlets = []
            if output.flows:
                for flow in output.flows:
                    all_inlets.extend(flow.inlets)
                    all_outlets.extend(flow.outlets)
            else:
                all_inlets = list(output.inlets)
                all_outlets = list(output.outlets)

            # Строим внутренние рёбра задачи
            flow_nodes, flow_edges = self._build_flow_elements(
                all_inlets, all_outlets,
                node_prefix=task_prefix,
                parent_id=group_id,
                seen_nodes=seen_nodes,
            )
            nodes.extend(flow_nodes)
            edges.extend(flow_edges)

            # Запоминаем для межтаскных связей
            for item in dict.fromkeys(all_inlets):
                nid = _entity_node_id(item, task_prefix)
                fqn_inlets.setdefault(item.fqn, []).append((task_prefix, nid))
            for item in dict.fromkeys(all_outlets):
                nid = _entity_node_id(item, task_prefix)
                fqn_outlets.setdefault(item.fqn, []).append((task_prefix, nid))

        # Межтаскные связи: outlet задачи A → inlet задачи B (по совпадению FQN)
        # Только outlet→inlet между РАЗНЫМИ задачами
        cross_edges_seen = set()
        for fqn, outlet_entries in fqn_outlets.items():
            if fqn in fqn_inlets:
                for out_task, out_nid in outlet_entries:
                    for in_task, in_nid in fqn_inlets[fqn]:
                        if out_task == in_task:
                            continue  # Пропускаем внутри одной задачи
                        edge_key = (out_nid, in_nid)
                        if edge_key not in cross_edges_seen:
                            cross_edges_seen.add(edge_key)
                            edges.append({
                                'data': {
                                    'id': f"cross_{out_nid}__{in_nid}",
                                    'source': out_nid,
                                    'target': in_nid,
                                    'edge_type': 'cross_task',
                                    'flow_type': 'cross_task',
                                }
                            })

        return {
            'elements': {'nodes': nodes, 'edges': edges},
            'dag_id': dag_id,
        }

    def build_full_data(self, outputs: List[OMEntityOutput], dag_id: str = '') -> Dict[str, Any]:
        """Строит полные данные для HTML-шаблона: task-views + dag-view."""
        return {
            'dag_id': dag_id,
            'initial_view': 'dag',
            'task_views': {
                output.task_id: self.build_task_view(output)
                for output in outputs
            },
            'dag_view': self.build_dag_view(outputs, dag_id),
        }
