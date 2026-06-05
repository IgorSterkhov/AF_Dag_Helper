# AF DAGs Helper

Утилита для анализа Airflow DAG файлов и автоматической генерации `OMEntity` (inlets/outlets) для трекинга lineage в OpenMetadata.

Парсит Python-код DAG через AST, извлекает SQL из переменных, анализирует таблицы (sqlglot), разрешает connection_id через `@with_db` декораторы и `op_kwargs`, и выдаёт готовый Python-код с `inlets`/`outlets`.

## Требования

- Python 3.9+
- Windows (пути к venv предполагают `venv\Scripts\`)
- Для интерактивных диаграмм (опционально): `pip install pywebview`

## Установка и запуск

### 1. Активация venv

**Windows (cmd):**
```cmd
cd "c:\DevWB\AF Dags Helper"
venv\Scripts\activate
```

**Windows (PowerShell):**
```powershell
cd "c:\DevWB\AF Dags Helper"
.\venv\Scripts\Activate.ps1
```

После активации в промпте появится `(venv)`.

### 2. Запуск GUI

```cmd
python gui\app.py
```

Или без активации venv — указав полный путь к интерпретатору:
```cmd
"c:\DevWB\AF Dags Helper\venv\Scripts\python.exe" "c:\DevWB\AF Dags Helper\gui\app.py"
```

### 3. Установка зависимостей (если venv пустой)

```cmd
pip install -r requirements.txt
```

Зависимости:
- `sqlglot>=23.0.0` — парсинг SQL (диалект ClickHouse)
- `pyyaml>=6.0` — конфиги
- `tkinter` — встроен в Python
- `pywebview` (опционально) — нативное окно для интерактивных диаграмм

## Использование GUI

Основное окно:

1. **«Выбрать DAG файл...»** — открывает диалог выбора `.py` файла DAG. Последняя директория запоминается в `config/settings.yaml`. После выбора анализ запускается автоматически.
2. **«Настройки маппинга»** — таблица `Connection ID → Server Name` (редактирует `config/server_mapping.yaml`). Кнопки «Добавить» / «Удалить» / «Сохранить».
3. **«Анализировать»** — перезапустить анализ текущего файла (после правки маппинга).
4. **«Принудительно (все задачи)»** — игнорировать существующие `OMEntity` в DAG; генерировать для всех `PythonOperator` задач. Включено по умолчанию — нужно для сравнения и для диаграмм.
5. **«Выводить и сравнить имеющиеся OMEntity»** — для каждой задачи показать блоки `Existed OMEntity` и `Difference` (MATCH / MISMATCH + список расхождений `+`/`-` по FQN).
6. **«Копировать в буфер»** — весь вывод в буфер обмена.
7. **«Очистить»** — очистить поле вывода.
8. **«Interactive Diagram»** — интерактивная диаграмма lineage на Cytoscape.js (`DAG view` — весь DAG, `Task view` — по задачам). Открывается во встроенном окне (pywebview) или в браузере. Работает только в принудительном режиме.

Поле вывода — тёмная тема с простой подсветкой синтаксиса (комментарии, строки, ключевые слова `OMEntity`/`Entity`/`inlets`/`outlets`). `Ctrl+C` / `Ctrl+A` работают в любой раскладке (Windows).

Размер окна и путь к последней директории сохраняются в `config/settings.yaml` при закрытии.

## Использование CLI

```cmd
python main.py path\to\dag.py
python main.py path\to\dag.py config\server_mapping.yaml
```

Вывод печатается в stdout.

## Тестирование против эталонных DAG

```cmd
python test_against_samples.py
python test_against_samples.py "Dags samples\api_ch3_hr_erp_updates.py"
```

Сравнивает сгенерированный `OMEntity` с существующим в DAG, отмечает расхождения и подсказывает, какие записи добавить в `server_mapping.yaml`.

## Конфигурация

### `config/server_mapping.yaml`

Маппинг Airflow `connection_id` → OpenMetadata `server_name`. Поддерживаются шаблоны с `*`:

```yaml
default_behavior: passthrough   # если нет маппинга — использовать connection_id как есть
server_mapping:
  do-ch13: dm13
  do-ch4*: do-ch4               # все connection_id, начинающиеся с do-ch4
  do-ch8*: do-ch8
```

### `config/settings.yaml`

Пользовательские настройки GUI (создаётся автоматически, не в git):
- `last_directory` — последняя открытая директория
- `window_width` / `window_height` — размер окна

## Что детектит утилита

**Connection ID через переменные:**
```python
CH_13_CONN_ID = "do-ch13"
```

**`@with_db` декораторы:**
```python
@with_db(CH_13_CONN_ID, "ch13")
def my_function(hook, ...):
    ...
```

**SQL в строковых переменных** (эвристика по ключевым словам SQL):
- `SELECT ... FROM ...` / `JOIN` → **inlets**
- `INSERT INTO ...` → **outlets**
- `dictGet*('dict.name', ...)` → **inlets** (словари)

**API вызовы `HttpHook`** → `OMEntity(entity=Entity.API, fqn="<http_conn_id>")` в inlets.

**`bulk_dump(table='schema.table', ...)`** → **outlets** (прямой вызов и через `op_kwargs`).

**Разрешение параметров через `op_kwargs`** — если таблица/conn передаётся в task как параметр, утилита находит значение в `PythonOperator(op_kwargs=...)`.

## Пример вывода

```python
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

inlets=[OMEntity(entity=Entity.TABLE, fqn="dm13.schema.source_table")]
outlets=[OMEntity(entity=Entity.TABLE, fqn="dm13.schema.target_table")]
```

## Структура проекта

```
analyzer/        AST парсер DAG, SQL анализ (sqlglot), резолвер connection
generator/       построение FQN и генерация кода OMEntity
visualizer/      текстовая и интерактивная (Cytoscape.js) диаграммы
gui/             Tkinter приложение
config/          server_mapping.yaml, settings.yaml
Dags for test/   DAG без OMEntity (вход)
Dags samples/    DAG с эталонным OMEntity (для сравнения)
main.py          CLI / класс DAGAnalyzer
test_against_samples.py   сравнение с эталонами
```

Детали по архитектуре пайплайна — см. [CLAUDE.md](CLAUDE.md).
