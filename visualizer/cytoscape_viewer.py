"""Просмотрщик Cytoscape.js диаграмм — pywebview или браузер."""

import json
import tempfile
import webbrowser
from pathlib import Path
from typing import Dict, Any

TEMPLATE_PATH = Path(__file__).parent / 'templates' / 'dag_lineage.html'


def _check_pywebview() -> bool:
    """Проверяет доступность pywebview."""
    try:
        import webview  # noqa: F401
        return True
    except ImportError:
        return False


class CytoscapeViewer:
    """Открывает интерактивные Cytoscape.js диаграммы."""

    def __init__(self):
        self._pywebview_available = _check_pywebview()

    @property
    def has_pywebview(self) -> bool:
        return self._pywebview_available

    def show(self, graph_data: Dict[str, Any], title: str = 'DAG Lineage'):
        """Показывает диаграмму лучшим доступным способом."""
        html_content = self._build_html(graph_data, title)

        if self._pywebview_available:
            self._show_pywebview(html_content, title)
        else:
            self._show_browser(html_content)

    def _build_html(self, graph_data: Dict[str, Any], title: str) -> str:
        """Читает HTML-шаблон и инжектирует данные графа."""
        template = TEMPLATE_PATH.read_text(encoding='utf-8')
        json_data = json.dumps(graph_data, ensure_ascii=False, indent=2)
        html = template.replace(
            'window.__GRAPH_DATA__ = null;',
            f'window.__GRAPH_DATA__ = {json_data};'
        )
        html = html.replace('{{TITLE}}', title)
        return html

    def _show_pywebview(self, html: str, title: str):
        """Открывает в pywebview окне (не блокирует Tkinter)."""
        import threading
        import webview

        def _run():
            webview.create_window(
                title,
                html=html,
                width=1200,
                height=800,
                min_size=(800, 500),
            )
            webview.start()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

    def _show_browser(self, html: str):
        """Fallback: сохраняет temp HTML и открывает в браузере."""
        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html', delete=False, encoding='utf-8'
        )
        tmp.write(html)
        tmp.close()
        webbrowser.open(f'file:///{tmp.name}')
