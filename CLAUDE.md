# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AF DAGs Helper is a utility for analyzing Airflow DAG files and auto-generating OMEntity (inlets/outlets) for OpenMetadata lineage tracking. It parses Python DAG files, extracts SQL queries, analyzes table references, and generates ready-to-use OMEntity code.

## Commands

**Запуск через venv (рекомендуется):**
```bash
"c:\DevWB\AF Dags Helper\venv\Scripts\python.exe" "c:\DevWB\AF Dags Helper\main.py" path/to/dag.py
```

**Или при активированном venv:**
```bash
# GUI application
python gui/app.py

# CLI - analyze a single DAG file
python main.py path/to/dag.py

# CLI - with custom server mapping
python main.py path/to/dag.py config/server_mapping.yaml
```

## Architecture

The data flow follows this pipeline:

```
DAG file (.py)
    ↓
DAGParser (Python AST)
    ↓ extracts: tasks, functions, SQL variables, decorators, connection IDs
SQLAnalyzer (sqlglot)
    ↓ extracts: FROM/JOIN tables → inlets, INSERT INTO → outlets, dictGet → dictionaries
ConnectionResolver
    ↓ resolves: @with_db decorator args, *_CONN_ID variables
FQNBuilder
    ↓ applies: server_mapping.yaml (connection_id → server_name)
OMEntityGenerator
    ↓ outputs: formatted Python code with inlets/outlets
```

**Key classes:**
- `DAGAnalyzer` ([main.py](main.py)) - orchestrates the pipeline
- `DAGParser` ([analyzer/dag_parser.py](analyzer/dag_parser.py)) - Python AST traversal
- `SQLAnalyzer` ([analyzer/sql_analyzer.py](analyzer/sql_analyzer.py)) - sqlglot-based SQL parsing
- `ConnectionResolver` ([analyzer/connection_resolver.py](analyzer/connection_resolver.py)) - connection ID resolution
- `FQNBuilder` ([generator/fqn_builder.py](generator/fqn_builder.py)) - FQN construction with mapping
- `OMEntityGenerator` ([generator/omentity_generator.py](generator/omentity_generator.py)) - code generation

## OMEntity Format

Generated code follows this pattern:
```python
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

inlets=[OMEntity(entity=Entity.TABLE, fqn="server.schema.table")]
outlets=[OMEntity(entity=Entity.TABLE, fqn="server.schema.table")]
```

## DAG File Patterns

The analyzer looks for these patterns in DAG files:

**Connection ID variables:**
```python
CH_13_CONN_ID = "do-ch13"
```

**@with_db decorator:**
```python
@with_db(CH_13_CONN_ID, "ch13")
def my_function(hook, ...):
    ...
```

**SQL in string variables (detected by SQL keywords heuristic):**
```python
SQL_QUERY = '''
SELECT * FROM schema.table
INSERT INTO target.table
'''
```

**dictGet functions in SQL (treated as inlets):**
```sql
dictGetOrNull('dict.product_cards_nm', 'subject_id', nm_id)
```

**Remote tables (different connection):**
- Tables with `remote_*` schema prefix are flagged as remote connections

## Server Mapping

`config/server_mapping.yaml` maps Airflow connection IDs to OpenMetadata server names:
```yaml
server_mapping:
  do-ch13: dm13
  do-ch3: clickhouse-prod-3
default_behavior: passthrough  # use connection_id as-is if no mapping
```

## Test Data

- `Dags for test/` - DAG files without OMEntity (input for testing generation)
- `Dags samples/` - DAG files with existing OMEntity (reference examples)

## Testing Against Samples

Скрипт [test_against_samples.py](test_against_samples.py) сравнивает генерацию с эталонными DAG файлами:

```bash
# Тест всех файлов в Dags samples/
python test_against_samples.py

# Тест конкретного файла
python test_against_samples.py "Dags samples/api_ch3_hr_erp_updates.py"
```

**Что делает:**
1. Извлекает существующие OMEntity из DAG через AST
2. Запускает утилиту генерации
3. Сравнивает результаты (✓ совпадение / ✗ расхождение)
4. Предлагает обновления для `server_mapping.yaml`

**Типичные причины расхождений:**
- API inlets (`Entity.API`) - утилита не детектит API, только SQL
- Таблицы вне SQL переменных (динамический SQL, op_kwargs)
- Несовпадение серверов - нужен маппинг в `server_mapping.yaml`
- Задача уже имеет OMEntity - утилита пропускает такие задачи

## Dependencies

- `sqlglot` - SQL parsing with ClickHouse dialect support
- `pyyaml` - YAML config handling
- `tkinter` - GUI (built into Python)
