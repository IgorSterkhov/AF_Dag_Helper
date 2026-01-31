"""
Утилита для поиска файлов по содержимому.

Сканирует папку рекурсивно, находит файлы с заданной строкой
и позволяет скопировать их в отдельную папку.
"""

import shutil
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from typing import List, Dict


class FileFinderApp:
    """Главное окно приложения поиска файлов."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Поиск файлов по содержимому")
        self.root.geometry("700x550")
        self.root.minsize(600, 450)

        # Переменные для чекбоксов файлов
        self.file_vars: Dict[Path, tk.BooleanVar] = {}
        self.found_files: List[Path] = []
        self.search_folder: Path = Path.cwd()

        self._create_widgets()

    def _create_widgets(self):
        """Создание элементов интерфейса."""
        # Фрейм параметров поиска
        search_frame = ttk.LabelFrame(self.root, text="Параметры поиска", padding=10)
        search_frame.pack(fill=tk.X, padx=10, pady=5)

        # Папка поиска
        ttk.Label(search_frame, text="Папка поиска:").grid(row=0, column=0, sticky=tk.W)
        self.folder_var = tk.StringVar(value=str(self.search_folder))
        folder_entry = ttk.Entry(search_frame, textvariable=self.folder_var, width=50)
        folder_entry.grid(row=0, column=1, padx=5, sticky=tk.EW)
        ttk.Button(search_frame, text="Обзор...", command=self._select_search_folder).grid(row=0, column=2)

        # Строка поиска
        ttk.Label(search_frame, text="Искать строку:").grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        self.pattern_var = tk.StringVar()
        pattern_entry = ttk.Entry(search_frame, textvariable=self.pattern_var, width=50)
        pattern_entry.grid(row=1, column=1, padx=5, pady=(5, 0), sticky=tk.EW)

        # Маска файлов
        ttk.Label(search_frame, text="Маска файлов:").grid(row=2, column=0, sticky=tk.W, pady=(5, 0))
        self.mask_var = tk.StringVar(value="*.py")
        mask_entry = ttk.Entry(search_frame, textvariable=self.mask_var, width=20)
        mask_entry.grid(row=2, column=1, padx=5, pady=(5, 0), sticky=tk.W)

        # Опции
        options_frame = ttk.Frame(search_frame)
        options_frame.grid(row=3, column=0, columnspan=3, sticky=tk.W, pady=(5, 0))

        self.case_sensitive_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Учитывать регистр",
                        variable=self.case_sensitive_var).pack(side=tk.LEFT)

        ttk.Button(options_frame, text="Найти", command=self._search_files).pack(side=tk.RIGHT, padx=(20, 0))

        search_frame.columnconfigure(1, weight=1)

        # Фрейм результатов
        results_frame = ttk.LabelFrame(self.root, text="Результаты", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.status_label = ttk.Label(results_frame, text="Введите параметры и нажмите 'Найти'")
        self.status_label.pack(anchor=tk.W)

        # Список файлов со скроллом
        list_frame = ttk.Frame(results_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.files_canvas = tk.Canvas(list_frame, yscrollcommand=scrollbar.set)
        self.files_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.files_canvas.yview)

        self.files_inner_frame = ttk.Frame(self.files_canvas)
        self.canvas_window = self.files_canvas.create_window((0, 0), window=self.files_inner_frame, anchor=tk.NW)

        self.files_inner_frame.bind("<Configure>", self._on_frame_configure)
        self.files_canvas.bind("<Configure>", self._on_canvas_configure)

        # Кнопки выбора
        select_frame = ttk.Frame(results_frame)
        select_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(select_frame, text="Выбрать все", command=self._select_all).pack(side=tk.LEFT)
        ttk.Button(select_frame, text="Снять выбор", command=self._deselect_all).pack(side=tk.LEFT, padx=5)

        # Фрейм копирования
        copy_frame = ttk.LabelFrame(self.root, text="Копирование", padding=10)
        copy_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(copy_frame, text="Папка назначения:").grid(row=0, column=0, sticky=tk.W)
        self.dest_var = tk.StringVar()
        dest_entry = ttk.Entry(copy_frame, textvariable=self.dest_var, width=50)
        dest_entry.grid(row=0, column=1, padx=5, sticky=tk.EW)
        ttk.Button(copy_frame, text="Обзор...", command=self._select_dest_folder).grid(row=0, column=2)

        self.keep_structure_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(copy_frame, text="Сохранять структуру папок",
                        variable=self.keep_structure_var).grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(5, 0))

        ttk.Button(copy_frame, text="Копировать выбранные",
                   command=self._copy_selected).grid(row=1, column=2, pady=(5, 0))

        copy_frame.columnconfigure(1, weight=1)

    def _on_frame_configure(self, event):
        """Обновление области прокрутки."""
        self.files_canvas.configure(scrollregion=self.files_canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        """Подстройка ширины внутреннего фрейма."""
        self.files_canvas.itemconfig(self.canvas_window, width=event.width)

    def _select_search_folder(self):
        """Диалог выбора папки поиска."""
        folder = filedialog.askdirectory(initialdir=self.folder_var.get())
        if folder:
            self.folder_var.set(folder)
            self.search_folder = Path(folder)

    def _select_dest_folder(self):
        """Диалог выбора папки назначения."""
        folder = filedialog.askdirectory()
        if folder:
            self.dest_var.set(folder)

    def _search_files(self):
        """Поиск файлов по заданным параметрам."""
        pattern = self.pattern_var.get()
        if not pattern:
            messagebox.showwarning("Внимание", "Введите строку для поиска")
            return

        folder = Path(self.folder_var.get())
        if not folder.exists():
            messagebox.showerror("Ошибка", "Папка не существует")
            return

        mask = self.mask_var.get() or "*"
        case_sensitive = self.case_sensitive_var.get()

        self.status_label.config(text="Поиск...")
        self.root.update()

        # Поиск
        self.found_files = []
        search_pattern = pattern if case_sensitive else pattern.lower()

        for file_path in folder.rglob(mask):
            if file_path.is_file():
                try:
                    content = file_path.read_text(encoding='utf-8')
                    check_content = content if case_sensitive else content.lower()
                    if search_pattern in check_content:
                        self.found_files.append(file_path)
                except (UnicodeDecodeError, PermissionError, OSError):
                    pass  # Пропускаем бинарные/недоступные файлы

        self._display_results()

    def _display_results(self):
        """Отображение найденных файлов."""
        # Очистка предыдущих результатов
        for widget in self.files_inner_frame.winfo_children():
            widget.destroy()
        self.file_vars.clear()

        if not self.found_files:
            self.status_label.config(text="Файлы не найдены")
            return

        self.status_label.config(text=f"Найдено: {len(self.found_files)} файлов")

        # Создание чекбоксов для каждого файла
        for file_path in sorted(self.found_files):
            var = tk.BooleanVar(value=False)
            self.file_vars[file_path] = var

            try:
                rel_path = file_path.relative_to(self.search_folder)
            except ValueError:
                rel_path = file_path

            cb = ttk.Checkbutton(self.files_inner_frame, text=str(rel_path), variable=var)
            cb.pack(anchor=tk.W)

    def _select_all(self):
        """Выбрать все файлы."""
        for var in self.file_vars.values():
            var.set(True)

    def _deselect_all(self):
        """Снять выбор со всех файлов."""
        for var in self.file_vars.values():
            var.set(False)

    def _copy_selected(self):
        """Копирование выбранных файлов."""
        dest_folder = self.dest_var.get()
        if not dest_folder:
            messagebox.showwarning("Внимание", "Выберите папку назначения")
            return

        dest_path = Path(dest_folder)
        dest_path.mkdir(parents=True, exist_ok=True)

        selected = [fp for fp, var in self.file_vars.items() if var.get()]
        if not selected:
            messagebox.showwarning("Внимание", "Не выбрано ни одного файла")
            return

        keep_structure = self.keep_structure_var.get()
        copied = 0
        errors = []

        for file_path in selected:
            try:
                if keep_structure:
                    try:
                        rel_path = file_path.relative_to(self.search_folder)
                    except ValueError:
                        rel_path = file_path.name
                    target = dest_path / rel_path
                    target.parent.mkdir(parents=True, exist_ok=True)
                else:
                    target = dest_path / file_path.name

                shutil.copy2(file_path, target)
                copied += 1
            except Exception as e:
                errors.append(f"{file_path.name}: {e}")

        if errors:
            messagebox.showwarning("Предупреждение",
                                   f"Скопировано: {copied}\nОшибки:\n" + "\n".join(errors[:5]))
        else:
            messagebox.showinfo("Готово", f"Скопировано файлов: {copied}")


def main():
    """Точка входа."""
    root = tk.Tk()
    FileFinderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
