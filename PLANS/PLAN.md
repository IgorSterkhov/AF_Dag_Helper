# AF DAGs Helper - План разработки

## Цель проекта

Утилита для анализа Airflow DAG файлов и автоматической генерации OMEntity (inlets/outlets) для метаданных OpenMetadata.

---

## Архитектура

### Структура проекта

```
af_dags_helper/
├── analyzer/
│   ├── __init__.py
│   ├── dag_parser.py          # AST парсинг Python DAG
│   ├── sql_analyzer.py        # sqlglot анализ SQL (ClickHouse)
│   ├── connection_resolver.py # Определение connection из кода
│   └── api_detector.py        # Поиск API/HTTP вызовов
├── generator/
│   ├── __init__.py
│   ├── fqn_builder.py         # Построение FQN с маппингом серверов
│   └── omentity_generator.py  # Генерация кода inlets/outlets
├── config/
│   ├── server_mapping.yaml    # Маппинг серверов (редактируемый)
│   └── settings.yaml          # Общие настройки (последняя папка и т.д.)
├── handlers/                  # Существующие модули
│   ├── sql_formatter.py
│   └── sql_parser.py
├── gui/
│   └── app.py                 # Tkinter GUI
├── cli.py                     # CLI интерфейс
└── requirements.txt
```

---

## Ключевые компоненты

### 1. SQL Analyzer (sqlglot)

**Задача:** Парсинг ClickHouse SQL и извлечение lineage.

**Извлекаемые данные:**
- `FROM` / `JOIN` таблицы → inlets
- `INSERT INTO` таблицы → outlets
- `dictGet*()` вызовы → inlets (словари)
- Таблицы с префиксом `remote_*` → отдельный connection

**Библиотека:** `sqlglot` с диалектом `clickhouse`

```python
import sqlglot
ast = sqlglot.parse(sql, dialect="clickhouse")
```

### 2. DAG Parser (Python AST)

**Задача:** Парсинг Python DAG файла.

**Извлекаемые данные:**
- Список задач (PythonOperator, и др.)
- Связь task_id → python_callable
- SQL переменные (строки с SQL кодом)
- Декораторы `@with_db(CONN_ID)`
- Существующие inlets/outlets (если есть)

### 3. Connection Resolver

**Задача:** Определение connection ID для каждой функции/задачи.

**Источники:**
1. Декоратор `@with_db(CONN_ID)` или `@with_db(CONN_ID, "alias")`
2. Переменные вида `*_CONN_ID = "..."`
3. Явные вызовы hooks с `conn_id=...`
4. Fallback на конфиг

### 4. FQN Builder с маппингом серверов

**Задача:** Построение FQN с учётом маппинга серверов.

**Логика:**
```
SQL table: "core_wh.srid_tracker_ttl"
Connection: "do-ch13"

1. Проверяем server_mapping.yaml
2. Если есть маппинг "do-ch13" → "dm13":
   FQN = "dm13.core_wh.srid_tracker_ttl"
3. Если маппинга нет (passthrough):
   FQN = "do-ch13.core_wh.srid_tracker_ttl"
```

**Конфиг server_mapping.yaml:**
```yaml
server_mapping:
  # Пустой по умолчанию
  # Пользователь добавляет по мере необходимости:
  # do-ch3: clickhouse-prod-3
  # do-ch13: dm13

default_behavior: passthrough
```

### 5. OMEntity Generator

**Задача:** Генерация готового Python кода.

**Выход:**
```python
# Task: task_upd_reorders
# Connection: do-ch13

inlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"),
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.dict.subjects"),
],
outlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_reorders"),
]
```

### 6. GUI (Tkinter)

**Функционал:**
- Кнопка выбора DAG файла (FileDialog)
- Текстовое поле для вывода результатов
- Кнопка копирования в буфер обмена
- Запоминание последней папки в `config/settings.yaml`
- Кнопка редактирования маппинга серверов

**Макет:**
```
┌─────────────────────────────────────────────────────┐
│  AF DAGs Helper                              [—][×] │
├─────────────────────────────────────────────────────┤
│  [Выбрать DAG файл...]     [Настройки маппинга]    │
│                                                     │
│  Файл: C:/path/to/dag.py                           │
├─────────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────────┐ │
│  │ # Task: task_upd_reorders                     │ │
│  │ # Connection: do-ch13                         │ │
│  │                                               │ │
│  │ inlets=[                                      │ │
│  │     OMEntity(entity=Entity.TABLE,             │ │
│  │              fqn="do-ch13.core_wh..."),       │ │
│  │ ],                                            │ │
│  │ outlets=[...]                                 │ │
│  │                                               │ │
│  └───────────────────────────────────────────────┘ │
│                                                     │
│  [Копировать в буфер]              [Анализировать] │
└─────────────────────────────────────────────────────┘
```

---

## Формат выходных данных

Для каждой задачи без OMEntity:

```python
# ═══════════════════════════════════════════════════════════════
# Task: task_upd_reorders
# Function: upd_reorders
# Connection: do-ch13 (from @with_db decorator)
# ═══════════════════════════════════════════════════════════════

# Detected SOURCES (inlets):
#   Tables:
#     - core_wh.srid_tracker_ttl (FROM/JOIN)
#     - datamart.srid_tracker_reorders (self-join)
#     - remote_ch.last_srid_position_v3 (remote)
#   Dictionaries:
#     - dict.product_cards_nm
#     - dict.subjects
#     - dict.cbr_currency

# Detected TARGETS (outlets):
#     - datamart.srid_tracker_reorders (INSERT INTO)

# ─────────────────────────────────────────────────────────────────
# Generated OMEntity:
# ─────────────────────────────────────────────────────────────────

inlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"),
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_reorders"),
    OMEntity(entity=Entity.TABLE, fqn="remote_ch.last_srid_position_v3"),
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.dict.product_cards_nm"),
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.dict.subjects"),
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.dict.cbr_currency"),
],
outlets=[
    OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_reorders"),
]

# ⚠️ WARNINGS:
#   - "remote_ch.last_srid_position_v3" uses prefix "remote_ch" - different connection?
```

---

## Зависимости

```
sqlglot>=23.0.0    # SQL парсинг с поддержкой ClickHouse
pyyaml>=6.0        # Чтение/запись YAML конфигов
# tkinter          # Встроен в Python
```

---

## CLI интерфейс

```bash
# Анализ DAG файла
python cli.py analyze path/to/dag.py

# С указанием конфига маппинга
python cli.py analyze path/to/dag.py --mapping config/server_mapping.yaml

# Добавить маппинг сервера
python cli.py add-mapping "do-ch13" "dm13"

# Запуск GUI
python cli.py gui
# или просто
python gui/app.py
```

---

## Этапы реализации

1. **Структура проекта** - создать папки и requirements.txt
2. **SQL Analyzer** - парсинг SQL через sqlglot
3. **DAG Parser** - парсинг Python AST
4. **Connection Resolver** - определение connections
5. **FQN Builder** - построение FQN с маппингом
6. **OMEntity Generator** - генерация кода
7. **GUI** - интерфейс на tkinter
8. **Тестирование** - проверка на примерах DAG

---

## Тестовые данные

- `Dags samples/api_ch3_hr_erp_updates.py` - пример с готовыми OMEntity
- `Dags for test/dm13_reordered_srids.py` - пример без OMEntity (целевой для генерации)

---

## Доработки (январь 2026)

### Результаты тестирования

| Метрика | До | После |
|---------|-----|-------|
| Совпадений | 1 (10%) | 4 (40%) |
| Расхождений | 9 (90%) | 6 (60%) |

### Выполненные задачи

#### 1. Маппинг серверов
- Добавлен маппинг `do-ch-deliverytime: do-ch13` в `config/server_mapping.yaml`

#### 2. Связывание SQL с функциями
- Переписан метод `_link_functions_to_sql` в `dag_parser.py`
- Теперь анализирует AST тела функции вместо regex по всему файлу
- Детектирует вызовы `hook.exec()`, `hook.exec_with_log()`, `hook.on_cluster()`

#### 3. Детекция API (Entity.API)
- Добавлен метод `_extract_api_usage` для поиска HttpHook вызовов
- Добавлен парсинг `op_kwargs` для извлечения `api_conn` и `dst_table`
- Исключены параметры функции из API connections (они передаются через op_kwargs)
- Добавлен словарь `string_variables` для резолвинга строковых переменных

#### 4. Парсинг copy_ch_to_ch_pipe
- Добавлен метод `_extract_cross_server_calls`
- Извлекаются параметры `take_data`, `insert_data`, `src_ch`, `dst_ch`
- В `omentity_generator.py` добавлена обработка кросс-серверных вызовов

### Изменённые файлы

- `config/server_mapping.yaml` - добавлен маппинг
- `analyzer/models.py` - добавлены `CrossServerCall`, `api_connections`, `cross_server_calls`, `op_kwargs_api`, `string_variables`
- `analyzer/dag_parser.py` - переписан `_link_functions_to_sql`, добавлены методы для API и cross-server
- `generator/omentity_generator.py` - добавлена обработка API и cross-server
- `test_against_samples.py` - обновлён для тестирования новой логики

### Известные ограничения

- Сложные кросс-серверные DAG (`lake_nm_from_darkstore.py`) требуют ручной проверки
- Динамически формируемый SQL через `.format()` не всегда резолвится
- Множественные `@with_db` декораторы требуют доработки
