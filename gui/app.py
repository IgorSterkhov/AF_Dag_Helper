"""GUI приложение на Tkinter."""

import sys
from pathlib import Path

# Добавляем корневую директорию в path для импортов
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import yaml

from main import DAGAnalyzer
from generator.fqn_builder import FQNBuilder
from analyzer.dag_parser import DAGParser
from analyzer.sql_analyzer import SQLAnalyzer
from analyzer.models import SQLAnalysisResult
from analyzer.connection_resolver import ConnectionResolver
from generator.omentity_generator import OMEntityGenerator


class DAGHelperApp:
    """Главное окно приложения."""

    CONFIG_DIR = ROOT_DIR / "config"
    SETTINGS_FILE = CONFIG_DIR / "settings.yaml"
    MAPPING_FILE = CONFIG_DIR / "server_mapping.yaml"

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("AF DAGs Helper - OMEntity Generator")

        self.settings = self._load_settings()
        self.root.geometry(f"{self.settings.get('window_width', 900)}x{self.settings.get('window_height', 700)}")

        self.analyzer = DAGAnalyzer(str(self.MAPPING_FILE))
        self.current_file = None

        self._create_widgets()
        self._bind_events()

    def _load_settings(self) -> dict:
        """Загружает настройки из YAML."""
        if self.SETTINGS_FILE.exists():
            try:
                with open(self.SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                pass
        return {}

    def _save_settings(self):
        """Сохраняет настройки в YAML."""
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.SETTINGS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump(self.settings, f, default_flow_style=False, allow_unicode=True)
        except Exception:
            pass

    def _create_widgets(self):
        """Создает виджеты интерфейса."""
        # Стиль
        style = ttk.Style()
        style.configure('TButton', padding=5)
        style.configure('TLabel', padding=2)

        # Верхняя панель с кнопками
        top_frame = ttk.Frame(self.root, padding=10)
        top_frame.pack(fill=tk.X)

        self.btn_select = ttk.Button(
            top_frame,
            text="Выбрать DAG файл...",
            command=self._select_file
        )
        self.btn_select.pack(side=tk.LEFT, padx=5)

        self.btn_mapping = ttk.Button(
            top_frame,
            text="Настройки маппинга",
            command=self._open_mapping_dialog
        )
        self.btn_mapping.pack(side=tk.LEFT, padx=5)

        self.btn_analyze = ttk.Button(
            top_frame,
            text="Анализировать",
            command=self._analyze_dag
        )
        self.btn_analyze.pack(side=tk.RIGHT, padx=5)

        # Чекбокс принудительного режима
        self.force_mode = tk.BooleanVar(value=False)
        self.chk_force = ttk.Checkbutton(
            top_frame,
            text="Принудительно (все задачи)",
            variable=self.force_mode
        )
        self.chk_force.pack(side=tk.RIGHT, padx=10)

        # Метка с путём к файлу
        file_frame = ttk.Frame(self.root, padding=(10, 0, 10, 5))
        file_frame.pack(fill=tk.X)

        ttk.Label(file_frame, text="Файл:").pack(side=tk.LEFT)
        self.lbl_file = ttk.Label(file_frame, text="не выбран", foreground="gray")
        self.lbl_file.pack(side=tk.LEFT, padx=5)

        # Разделитель
        ttk.Separator(self.root, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10)

        # Текстовое поле для вывода
        text_frame = ttk.Frame(self.root, padding=10)
        text_frame.pack(fill=tk.BOTH, expand=True)

        self.text_output = scrolledtext.ScrolledText(
            text_frame,
            wrap=tk.NONE,
            font=("Consolas", 10),
            bg="#1e1e1e",
            fg="#d4d4d4",
            insertbackground="white"
        )
        self.text_output.pack(fill=tk.BOTH, expand=True)

        # Горизонтальный скроллбар
        h_scroll = ttk.Scrollbar(text_frame, orient=tk.HORIZONTAL, command=self.text_output.xview)
        h_scroll.pack(fill=tk.X)
        self.text_output.configure(xscrollcommand=h_scroll.set)

        # Нижняя панель с кнопками
        bottom_frame = ttk.Frame(self.root, padding=10)
        bottom_frame.pack(fill=tk.X)

        self.btn_copy = ttk.Button(
            bottom_frame,
            text="Копировать в буфер",
            command=self._copy_to_clipboard
        )
        self.btn_copy.pack(side=tk.LEFT, padx=5)

        self.btn_clear = ttk.Button(
            bottom_frame,
            text="Очистить",
            command=self._clear_output
        )
        self.btn_clear.pack(side=tk.LEFT, padx=5)

        # Статус
        self.lbl_status = ttk.Label(bottom_frame, text="", foreground="gray")
        self.lbl_status.pack(side=tk.RIGHT, padx=5)

    def _bind_events(self):
        """Привязывает обработчики событий."""
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        """Обработчик закрытия окна."""
        # Сохраняем размер окна
        self.settings['window_width'] = self.root.winfo_width()
        self.settings['window_height'] = self.root.winfo_height()
        self._save_settings()
        self.root.destroy()

    def _select_file(self):
        """Открывает диалог выбора файла."""
        initial_dir = self.settings.get('last_directory', str(ROOT_DIR))

        filepath = filedialog.askopenfilename(
            title="Выберите DAG файл",
            initialdir=initial_dir,
            filetypes=[("Python files", "*.py"), ("All files", "*.*")]
        )

        if filepath:
            self.current_file = filepath
            self.lbl_file.config(text=filepath, foreground="black")

            # Запоминаем директорию
            self.settings['last_directory'] = str(Path(filepath).parent)
            self._save_settings()

            # Автоматически анализируем
            self._analyze_dag()

    def _analyze_dag(self):
        """Анализирует выбранный DAG файл."""
        if not self.current_file:
            messagebox.showwarning("Предупреждение", "Сначала выберите DAG файл")
            return

        self.lbl_status.config(text="Анализирую...")
        self.root.update()

        try:
            # Перезагружаем маппинг на случай изменений
            self.analyzer.fqn_builder.load_mapping(str(self.MAPPING_FILE))

            if self.force_mode.get():
                result = self._analyze_dag_force(self.current_file)
            else:
                result = self.analyzer.analyze_dag(self.current_file)

            self.text_output.delete(1.0, tk.END)
            self.text_output.insert(tk.END, result)

            # Подсветка синтаксиса (простая)
            self._highlight_syntax()

            self.lbl_status.config(text="Готово")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка анализа:\n{str(e)}")
            self.lbl_status.config(text="Ошибка")

    def _analyze_dag_force(self, dag_path: str) -> str:
        """
        Анализирует DAG принудительно, игнорируя существующие OMEntity.

        Полезно для тестирования и сравнения с эталонными DAG файлами.
        """
        dag_parser = DAGParser()
        sql_analyzer = SQLAnalyzer()
        fqn_builder = self.analyzer.fqn_builder
        generator = OMEntityGenerator(fqn_builder)

        # 1. Парсим DAG
        dag_result = dag_parser.parse_file(dag_path)

        # 2. Анализируем SQL
        sql_results = {}
        for sql_var, sql_code in dag_result.sql_variables.items():
            result = sql_analyzer.analyze(sql_code)
            sql_results[sql_var] = result

        # 3. Связываем функции с SQL
        func_sql_results = {}
        for func_name, func_info in dag_result.functions.items():
            combined = SQLAnalysisResult()
            for sql_var in func_info.sql_variables:
                if sql_var in sql_results:
                    combined = combined.merge(sql_results[sql_var])
            if combined.inlets or combined.outlets or combined.dictionaries:
                func_sql_results[func_name] = combined

        # 4. Генерируем OMEntity для ВСЕХ задач (принудительно)
        resolver = ConnectionResolver(dag_result)
        outputs = []

        for task in dag_result.tasks:
            # Принудительно сбрасываем флаг has_omentity
            task.has_omentity = False

            func_name = task.python_callable
            if not func_name:
                continue

            conn_id, conn_source = resolver.resolve_for_function(func_name)
            if not conn_id:
                conn_id = "UNKNOWN"

            sql_result = func_sql_results.get(func_name)
            if not sql_result:
                func_info = dag_result.functions.get(func_name)
                if func_info:
                    combined = SQLAnalysisResult()
                    for sql_var in func_info.sql_variables:
                        if sql_var in sql_results:
                            combined = combined.merge(sql_results[sql_var])
                    if combined.inlets or combined.outlets or combined.dictionaries:
                        sql_result = combined

            if not sql_result:
                continue

            output = generator._generate_for_task(task, conn_id, conn_source, sql_result)
            outputs.append(output)

        # 5. Форматируем вывод
        if not outputs:
            return self._format_no_results_force(dag_result)

        return self._format_output_force(outputs, dag_result)

    def _format_output_force(self, outputs, dag_result) -> str:
        """Форматирует вывод для принудительного режима."""
        lines = []
        lines.append("# " + "=" * 65)
        lines.append(f"# DAG: {dag_result.dag_id or 'Unknown'}")
        lines.append("# РЕЖИМ: ПРИНУДИТЕЛЬНЫЙ (все задачи)")
        lines.append(f"# Задач обработано: {len(outputs)}")
        lines.append("# " + "=" * 65)
        lines.append("")

        for output in outputs:
            lines.append(output.generated_code)
            lines.append("")
            lines.append("")

        return "\n".join(lines)

    def _format_no_results_force(self, dag_result) -> str:
        """Форматирует вывод когда нет результатов в принудительном режиме."""
        lines = []
        lines.append("# " + "=" * 65)
        lines.append(f"# DAG: {dag_result.dag_id or 'Unknown'}")
        lines.append("# РЕЖИМ: ПРИНУДИТЕЛЬНЫЙ")
        lines.append("# " + "=" * 65)
        lines.append("")
        lines.append("# Нет задач с SQL для генерации OMEntity")
        lines.append("")

        if not dag_result.sql_variables:
            lines.append("# SQL переменные не найдены")

        if not dag_result.tasks:
            lines.append("# PythonOperator задачи не найдены")

        return "\n".join(lines)

    def _highlight_syntax(self):
        """Простая подсветка синтаксиса."""
        # Комментарии
        self.text_output.tag_configure("comment", foreground="#6a9955")
        # Строки
        self.text_output.tag_configure("string", foreground="#ce9178")
        # Ключевые слова
        self.text_output.tag_configure("keyword", foreground="#569cd6")

        content = self.text_output.get(1.0, tk.END)
        lines = content.split('\n')

        for i, line in enumerate(lines, 1):
            # Комментарии
            if line.strip().startswith('#'):
                self.text_output.tag_add("comment", f"{i}.0", f"{i}.end")
            else:
                # Строки в кавычках
                import re
                for match in re.finditer(r'"[^"]*"', line):
                    self.text_output.tag_add("string", f"{i}.{match.start()}", f"{i}.{match.end()}")

                # Ключевые слова
                for kw in ['OMEntity', 'Entity', 'TABLE', 'API', 'inlets', 'outlets', 'fqn', 'entity']:
                    start = 0
                    while True:
                        pos = line.find(kw, start)
                        if pos == -1:
                            break
                        self.text_output.tag_add("keyword", f"{i}.{pos}", f"{i}.{pos + len(kw)}")
                        start = pos + 1

    def _copy_to_clipboard(self):
        """Копирует содержимое в буфер обмена."""
        content = self.text_output.get(1.0, tk.END).strip()
        if not content:
            messagebox.showinfo("Информация", "Нечего копировать")
            return

        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        self.lbl_status.config(text="Скопировано в буфер")

    def _clear_output(self):
        """Очищает поле вывода."""
        self.text_output.delete(1.0, tk.END)
        self.lbl_status.config(text="")

    def _open_mapping_dialog(self):
        """Открывает диалог редактирования маппинга."""
        MappingDialog(self.root, self.analyzer.fqn_builder, self.MAPPING_FILE)


class MappingDialog:
    """Диалог редактирования маппинга серверов."""

    def __init__(self, parent, fqn_builder: FQNBuilder, mapping_file: Path):
        self.fqn_builder = fqn_builder
        self.mapping_file = mapping_file

        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Маппинг серверов")
        self.dialog.geometry("550x450")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Центрируем относительно родителя
        self.dialog.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - self.dialog.winfo_width()) // 2
        y = parent.winfo_y() + (parent.winfo_height() - self.dialog.winfo_height()) // 2
        self.dialog.geometry(f"+{x}+{y}")

        self._create_widgets()
        self._load_data()

    def _create_widgets(self):
        """Создает виджеты диалога."""
        # Описание
        desc_frame = ttk.Frame(self.dialog, padding=10)
        desc_frame.pack(fill=tk.X)

        ttk.Label(
            desc_frame,
            text="Маппинг Connection ID → Server Name в OpenMetadata",
            font=("", 9, "bold")
        ).pack(anchor=tk.W)

        ttk.Label(
            desc_frame,
            text="Если маппинг не указан, используется Connection ID как есть",
            foreground="gray"
        ).pack(anchor=tk.W)

        # Таблица маппингов
        table_frame = ttk.Frame(self.dialog, padding=(10, 0, 10, 10))
        table_frame.pack(fill=tk.BOTH, expand=True)

        columns = ("connection", "server")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=10)
        self.tree.heading("connection", text="Connection ID")
        self.tree.heading("server", text="Server Name")
        self.tree.column("connection", width=200)
        self.tree.column("server", width=200)

        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Поля ввода
        input_frame = ttk.Frame(self.dialog, padding=10)
        input_frame.pack(fill=tk.X)

        ttk.Label(input_frame, text="Connection ID:").grid(row=0, column=0, sticky=tk.W, padx=2)
        self.entry_conn = ttk.Entry(input_frame, width=25)
        self.entry_conn.grid(row=0, column=1, padx=5)

        ttk.Label(input_frame, text="Server Name:").grid(row=0, column=2, sticky=tk.W, padx=2)
        self.entry_server = ttk.Entry(input_frame, width=25)
        self.entry_server.grid(row=0, column=3, padx=5)

        # Кнопки добавления/удаления
        btn_frame = ttk.Frame(self.dialog, padding=(10, 0, 10, 10))
        btn_frame.pack(fill=tk.X)

        ttk.Button(btn_frame, text="Добавить", command=self._add_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Удалить выбранное", command=self._delete_mapping).pack(side=tk.LEFT, padx=5)

        # Разделитель
        ttk.Separator(self.dialog, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10)

        # Кнопки OK/Cancel
        bottom_frame = ttk.Frame(self.dialog, padding=10)
        bottom_frame.pack(fill=tk.X)

        ttk.Button(bottom_frame, text="Сохранить", command=self._save).pack(side=tk.RIGHT, padx=5)
        ttk.Button(bottom_frame, text="Отмена", command=self.dialog.destroy).pack(side=tk.RIGHT, padx=5)

    def _load_data(self):
        """Загружает данные в таблицу."""
        # Перезагружаем из файла
        self.fqn_builder.load_mapping(str(self.mapping_file))

        for conn, server in self.fqn_builder.mapping.items():
            self.tree.insert("", tk.END, values=(conn, server))

    def _add_mapping(self):
        """Добавляет новый маппинг."""
        conn = self.entry_conn.get().strip()
        server = self.entry_server.get().strip()

        if not conn or not server:
            messagebox.showwarning("Предупреждение", "Заполните оба поля")
            return

        # Проверяем на дубликат
        for item in self.tree.get_children():
            if self.tree.item(item)['values'][0] == conn:
                messagebox.showwarning("Предупреждение", f"Connection '{conn}' уже существует")
                return

        self.tree.insert("", tk.END, values=(conn, server))
        self.entry_conn.delete(0, tk.END)
        self.entry_server.delete(0, tk.END)

    def _delete_mapping(self):
        """Удаляет выбранный маппинг."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите строку для удаления")
            return

        for item in selected:
            self.tree.delete(item)

    def _save(self):
        """Сохраняет маппинги."""
        self.fqn_builder.mapping.clear()

        for item in self.tree.get_children():
            values = self.tree.item(item)['values']
            conn, server = values[0], values[1]
            self.fqn_builder.mapping[conn] = server

        self.fqn_builder.save_mapping(str(self.mapping_file))
        self.dialog.destroy()


def main():
    root = tk.Tk()
    app = DAGHelperApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
