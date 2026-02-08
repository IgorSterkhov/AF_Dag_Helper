"""Текстовые диаграммы потоков данных."""

from typing import List, Optional

from analyzer.models import OMEntityOutput, OMEntityItem, DataFlowGroup, EntityType


class DataFlowDiagram:
    """Генератор текстовых диаграмм потоков данных."""

    def render(self, output: OMEntityOutput, orientation: str = 'auto') -> str:
        """
        Рендерит диаграмму для одной задачи — последовательный пайплайн.

        Args:
            output: OMEntityOutput с inlets/outlets и flows
            orientation: не используется (оставлен для совместимости)

        Returns:
            Текстовая диаграмма
        """
        if not output.inlets and not output.outlets:
            return ""

        lines = []
        title = f"Task: {output.task_id}"
        sep = "=" * max(len(title) + 4, 50)
        lines.append(title)
        lines.append(sep)

        if output.flows:
            lines.extend(self._render_sequential_flows(
                output.flows, output.connection_id
            ))
        else:
            # Fallback: один шаг из всех inlets/outlets
            all_inlets = list(dict.fromkeys(output.inlets))
            all_outlets = list(dict.fromkeys(output.outlets))
            lines.append("")
            lines.extend(self._render_step(all_inlets, all_outlets, None))

        return "\n".join(lines)

    def render_all(self, outputs: List[OMEntityOutput], orientation: str = 'auto') -> str:
        """
        Рендерит диаграммы для всех задач DAG.

        Args:
            outputs: Список OMEntityOutput
            orientation: не используется (оставлен для совместимости)

        Returns:
            Текстовые диаграммы для всех задач
        """
        diagrams = []
        for output in outputs:
            diagram = self.render(output, orientation)
            if diagram:
                diagrams.append(diagram)
        return "\n\n".join(diagrams)

    # ─── Sequential flow rendering ──────────────────────────────────

    def _render_sequential_flows(
        self,
        flows: List[DataFlowGroup],
        default_connection: str
    ) -> List[str]:
        """Рендерит последовательные шаги потока данных."""
        lines = []
        # Фильтруем: пропускаем DDL-шаги (только inlets без outlets)
        non_empty = [f for f in flows if f.inlets and f.outlets]

        for i, flow in enumerate(non_empty):
            step_num = i + 1

            # Заголовок шага
            lines.append("")
            lines.append(f"  [{step_num}] {flow.source_description}")
            lines.append(f"  {'─' * 38}")

            # Рендер шага
            lines.extend(self._render_step(
                flow.inlets, flow.outlets, default_connection
            ))

            # Вертикальный коннектор к следующему шагу
            if i < len(non_empty) - 1:
                lines.extend(self._render_vertical_connector())

        return lines

    def _render_step(
        self,
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem],
        default_connection: Optional[str]
    ) -> List[str]:
        """
        Рендерит один шаг потока: inlets слева, стрелки, outlets справа.

        Формат:
          inlet_1  ──┐
          inlet_2  ──┼──▶ outlet_1
          inlet_3  ──┘

        Или с несколькими outlets:
          inlet_1  ──┐     ┌──▶ outlet_1
          inlet_2  ──┼─────┤
          inlet_3  ──┘     └──▶ outlet_2
        """
        indent = "  "

        inlet_names = [self._display_name(i, default_connection) for i in inlets]
        outlet_names = [self._display_name(o, default_connection) for o in outlets]

        # Только inlets (без outlets)
        if not outlet_names:
            return [f"{indent}{name}" for name in inlet_names]

        # Только outlets (без inlets)
        if not inlet_names:
            return [f"{indent}{'':>30}  ──▶ {name}" for name in outlet_names]

        n_in = len(inlet_names)
        n_out = len(outlet_names)
        max_inlet_len = max(len(n) for n in inlet_names)

        # Специальный случай: 1 inlet, 1 outlet — прямая стрелка
        if n_in == 1 and n_out == 1:
            padded = inlet_names[0].ljust(max_inlet_len)
            return [f"{indent}{padded}  ────▶ {outlet_names[0]}"]

        # Общий случай: строим скобки
        total_rows = max(n_in, n_out)
        in_offset = max(0, (total_rows - n_in) // 2)
        out_offset = max(0, (total_rows - n_out) // 2)

        # Строка, где горизонтальная труба соединяет скобки
        in_mid = in_offset + (n_in - 1) // 2
        out_mid = out_offset + (n_out - 1) // 2
        connect_row = (in_mid + out_mid) // 2

        # Если connect_row вне обеих скобок — подвинуть
        if connect_row < in_offset:
            connect_row = in_offset
        if connect_row > in_offset + n_in - 1:
            connect_row = in_offset + n_in - 1

        result = []
        for row in range(total_rows):
            in_idx = row - in_offset
            out_idx = row - out_offset

            # === Левая часть: inlet + скобка ===
            is_connect = (row == connect_row)
            if 0 <= in_idx < n_in:
                padded = inlet_names[in_idx].ljust(max_inlet_len)
                if n_in == 1:
                    left = f"{indent}{padded}  ──"
                elif in_idx == 0:
                    # ┐ = угол (лево+вниз), ┬ = тройник (лево+вниз+право)
                    ch = "┬" if is_connect else "┐"
                    left = f"{indent}{padded}  ──{ch}"
                elif in_idx == n_in - 1:
                    # ┘ = угол (лево+вверх), ┴ = тройник (лево+вверх+право)
                    ch = "┴" if is_connect else "┘"
                    left = f"{indent}{padded}  ──{ch}"
                else:
                    # ┤ = тройник (лево+вверх+вниз), ┼ = крест (все 4)
                    ch = "┼" if is_connect else "┤"
                    left = f"{indent}{padded}  ──{ch}"
            else:
                space = " " * max_inlet_len
                in_first = in_offset
                in_last = in_offset + n_in - 1
                if n_in > 1 and in_first < row < in_last:
                    left = f"{indent}{space}    │"
                else:
                    left = f"{indent}{space}     "

            # === Средняя часть: горизонтальная труба ===
            in_active = in_offset <= row <= in_offset + n_in - 1
            out_active = out_offset <= row <= out_offset + n_out - 1
            out_first = out_offset
            out_last = out_offset + n_out - 1

            if row == connect_row:
                mid = "─────"
            elif in_active and out_active:
                mid = "     "
            elif not in_active and out_active and out_first < row < out_last:
                mid = "     "
            else:
                mid = "     "

            # === Правая часть: скобка + outlet ===
            if 0 <= out_idx < n_out:
                if n_out == 1:
                    right = f"──▶ {outlet_names[out_idx]}"
                elif out_idx == 0:
                    ch = "┬" if is_connect else "┌"
                    right = f"{ch}──▶ {outlet_names[out_idx]}"
                elif out_idx == n_out - 1:
                    ch = "┴" if is_connect else "└"
                    right = f"{ch}──▶ {outlet_names[out_idx]}"
                else:
                    ch = "┼" if is_connect else "├"
                    right = f"{ch}──▶ {outlet_names[out_idx]}"
            else:
                # Вертикальная черта outlet-скобки если мы между первым и последним
                if n_out > 1 and out_first <= row <= out_last:
                    right = "│"
                else:
                    right = ""

            result.append(f"{left}{mid}{right}")

        return result

    def _render_vertical_connector(self) -> List[str]:
        """Вертикальная стрелка между шагами."""
        pad = " " * 40
        return [f"{pad}│", f"{pad}▼"]

    # ─── Display name helpers ───────────────────────────────────────

    def _display_name(
        self, item: OMEntityItem, default_connection: Optional[str]
    ) -> str:
        """Полный FQN для всех entity (как в OMEntity)."""
        if item.entity_type != EntityType.TABLE:
            return f"{item.fqn} ({item.entity_type.value})"
        return item.fqn
