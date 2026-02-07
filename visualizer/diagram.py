"""Текстовые диаграммы потоков данных."""

from typing import List, Optional

from analyzer.models import OMEntityOutput, OMEntityItem


class DataFlowDiagram:
    """Генератор текстовых диаграмм потоков данных."""

    MAX_HORIZONTAL_LINE = 120
    MAX_HORIZONTAL_ENTITIES = 4

    def render(self, output: OMEntityOutput, orientation: str = 'auto') -> str:
        """
        Рендерит диаграмму для одной задачи.

        Args:
            output: OMEntityOutput с inlets/outlets
            orientation: 'horizontal', 'vertical', 'auto'

        Returns:
            Текстовая диаграмма
        """
        if not output.inlets and not output.outlets:
            return ""

        # Группируем по key
        groups = self._group_by_key(output.inlets, output.outlets)

        # Определяем ориентацию
        if orientation == 'auto':
            orientation = self._detect_orientation(groups)

        lines = []
        title = f"Task: {output.task_id}"
        sep = "=" * max(len(title) + 4, 50)
        lines.append(title)
        lines.append(sep)

        if not groups:
            # Нет key-групп — простой вывод
            lines.extend(
                self._render_flat(output.inlets, output.outlets, orientation)
            )
        else:
            has_keyed = any(k is not None for k in groups)
            if not has_keyed:
                # Все без key — простой вывод
                all_inlets = []
                all_outlets = []
                for k, (ins, outs) in groups.items():
                    all_inlets.extend(ins)
                    all_outlets.extend(outs)
                lines.extend(
                    self._render_flat(all_inlets, all_outlets, orientation)
                )
            else:
                for key in sorted(groups.keys(), key=lambda k: (k is None, k or '')):
                    inlets, outlets = groups[key]
                    if key is not None:
                        lines.extend(
                            self._render_group(key, inlets, outlets, orientation)
                        )
                    else:
                        lines.extend(
                            self._render_flat(inlets, outlets, orientation)
                        )

        return "\n".join(lines)

    def render_all(self, outputs: List[OMEntityOutput], orientation: str = 'auto') -> str:
        """
        Рендерит диаграммы для всех задач DAG.

        Args:
            outputs: Список OMEntityOutput
            orientation: 'horizontal', 'vertical', 'auto'

        Returns:
            Текстовые диаграммы для всех задач
        """
        diagrams = []
        for output in outputs:
            diagram = self.render(output, orientation)
            if diagram:
                diagrams.append(diagram)
        return "\n\n".join(diagrams)

    def _group_by_key(
        self, inlets: List[OMEntityItem], outlets: List[OMEntityItem]
    ) -> dict:
        """Группирует inlets/outlets по key."""
        groups = {}
        for item in inlets:
            key = item.key
            if key not in groups:
                groups[key] = ([], [])
            groups[key][0].append(item)
        for item in outlets:
            key = item.key
            if key not in groups:
                groups[key] = ([], [])
            groups[key][1].append(item)
        return groups

    def _detect_orientation(self, groups: dict) -> str:
        """Определяет ориентацию: horizontal или vertical."""
        for key, (inlets, outlets) in groups.items():
            total_entities = len(inlets) + len(outlets)
            if total_entities > self.MAX_HORIZONTAL_ENTITIES:
                return 'vertical'

            # Проверяем длину горизонтальной строки
            line = self._format_horizontal_line(key, inlets, outlets)
            if len(line) > self.MAX_HORIZONTAL_LINE:
                return 'vertical'

        return 'horizontal'

    def _format_horizontal_line(
        self,
        key: Optional[str],
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem]
    ) -> str:
        """Формирует одну горизонтальную строку для оценки длины."""
        parts = []
        if key is not None:
            parts.append(f"[{key}] ")
        else:
            parts.append("  ")

        inlet_strs = [self._short_name(i) for i in inlets]
        outlet_strs = [self._short_name(o) for o in outlets]

        line = "".join(parts)
        line += ", ".join(inlet_strs)
        if inlets and outlets:
            line += " --> "
        line += ", ".join(outlet_strs)
        return line

    def _short_name(self, item: OMEntityItem) -> str:
        """Сокращённое имя entity для горизонтальной диаграммы."""
        fqn = item.fqn
        # Убираем серверный префикс для компактности
        parts = fqn.split('.')
        if len(parts) >= 3:
            name = ".".join(parts[1:])
        else:
            name = fqn

        if item.entity_type.value != "TABLE":
            return f"{name} ({item.entity_type.value})"
        return name

    def _full_name(self, item: OMEntityItem) -> str:
        """Имя entity без типа (тип уже в префиксе вертикальной диаграммы)."""
        parts = item.fqn.split('.')
        if len(parts) >= 3:
            return ".".join(parts[1:])
        return item.fqn

    def _render_flat(
        self,
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem],
        orientation: str
    ) -> List[str]:
        """Рендерит без key-группы."""
        if orientation == 'horizontal':
            return self._render_horizontal(None, inlets, outlets)
        else:
            return self._render_vertical(None, inlets, outlets)

    def _render_group(
        self,
        key: str,
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem],
        orientation: str
    ) -> List[str]:
        """Рендерит одну key-группу."""
        if orientation == 'horizontal':
            return self._render_horizontal(key, inlets, outlets)
        else:
            return self._render_vertical(key, inlets, outlets)

    def _make_box(self, title: str, items: List[str], connector: str = 'none') -> List[str]:
        """
        Строит текстовый бокс с рамкой.

        Args:
            title: заголовок бокса (INLETS / OUTLETS)
            items: строки содержимого
            connector: 'bottom' — ┬ в нижней границе, 'none' — обычная рамка
        """
        all_content = [title] + [f"  {item}" for item in items]
        width = max(len(s) for s in all_content) + 2  # padding

        lines = []
        lines.append(f"┌{'─' * width}┐")
        for s in all_content:
            lines.append(f"│ {s:<{width - 1}}│")

        if connector == 'bottom':
            half = width // 2
            lines.append(f"└{'─' * half}┬{'─' * (width - half - 1)}┘")
        else:
            lines.append(f"└{'─' * width}┘")

        return lines

    def _render_boxes_side_by_side(
        self,
        left: List[str],
        right: List[str],
        arrow: str = "───▶"
    ) -> List[str]:
        """Рендерит два бокса горизонтально рядом со стрелкой между ними."""
        left_width = max(len(line) for line in left) if left else 0
        max_h = max(len(left), len(right))

        # Выравниваем высоты
        while len(left) < max_h:
            left.append(" " * left_width)
        while len(right) < max_h:
            right.append("")

        # Стрелка на средней строке
        mid = max_h // 2
        spacer = "  "

        result = []
        for i in range(max_h):
            l = left[i].ljust(left_width)
            r = right[i]
            if i == mid:
                result.append(f"{l}{spacer}{arrow}{spacer}{r}")
            else:
                result.append(f"{l}{' ' * (len(spacer) + len(arrow) + len(spacer))}{r}")

        return result

    def _render_horizontal(
        self,
        key: Optional[str],
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem]
    ) -> List[str]:
        """Горизонтальный рендеринг: боксы слева направо со стрелками."""
        lines = []
        if key is not None:
            lines.append(f"  [{key}]")

        inlet_items = [self._short_name(i) for i in inlets] if inlets else ["(none)"]
        outlet_items = [self._short_name(o) for o in outlets] if outlets else ["(none)"]

        left_box = self._make_box("INLETS", inlet_items)
        right_box = self._make_box("OUTLETS", outlet_items)

        box_lines = self._render_boxes_side_by_side(left_box, right_box)
        for bl in box_lines:
            lines.append(f"  {bl}")

        return lines

    def _render_vertical(
        self,
        key: Optional[str],
        inlets: List[OMEntityItem],
        outlets: List[OMEntityItem]
    ) -> List[str]:
        """Вертикальный рендеринг: боксы сверху вниз со стрелками."""
        lines = []
        indent = "  "

        if key is not None:
            lines.append(f"{indent}[{key}]")

        inlet_items = [f"{item.entity_type.value}: {self._full_name(item)}" for item in inlets] if inlets else ["(none)"]
        outlet_items = [f"{item.entity_type.value}: {self._full_name(item)}" for item in outlets] if outlets else ["(none)"]

        # Бокс inlets с коннектором внизу
        inlet_box = self._make_box("INLETS", inlet_items, connector='bottom')
        box_width = max(len(line) for line in inlet_box)
        for bl in inlet_box:
            lines.append(f"{indent}{bl}")

        # Стрелка вниз (по центру бокса)
        center = box_width // 2 + 1
        lines.append(f"{indent}{' ' * center}│")
        lines.append(f"{indent}{' ' * center}▼")

        # Бокс outlets
        outlet_box = self._make_box("OUTLETS", outlet_items)
        for bl in outlet_box:
            lines.append(f"{indent}{bl}")

        return lines
