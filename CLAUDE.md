# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project System Prompt

Treat `CLAUDE.md` as the persistent project system prompt and the source of repository-level instructions. On every new session, look for system/project instructions in this file first. When long-lived system prompt instructions change, update `CLAUDE.md` instead of keeping them only in chat history.

Work pragmatically: inspect the current structure and git status before edits, preserve user changes, keep commits scoped, and verify with the project commands before reporting completion.

When web UI/UX changes are made, update the in-app help text in the same change and include help relevance in the final self-review before reporting completion.

Before deploying to `ivm-1`, run local verification first. For web UI changes this means the relevant unit tests, Python syntax checks, and a local smoke check when the change affects runtime behavior. Deploy only after local checks pass.

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

# Web UI
export AF_DAGS_HELPER_AUTH_USER=admin
export AF_DAGS_HELPER_AUTH_PASSWORD=change-me
python -m web.app --host 127.0.0.1 --port 8000

# CLI - analyze a single DAG file
python main.py path/to/dag.py

# CLI - with custom server mapping
python main.py path/to/dag.py config/server_mapping.yaml

# Deploy web UI to ivm-1
scripts/deploy_ivm1.sh

# Terminal ops menu for ivm-1
scripts/ivm1_ops.sh
scripts/ivm1_ops.sh health
scripts/ivm1_ops.sh status
scripts/ivm1_ops.sh follow
scripts/ivm1_ops.sh version
scripts/ivm1_ops.sh deploy
scripts/ivm1_ops.sh feedback-fetch

# Local feedback triage
scripts/feedback_triage.py fetch --mode new
scripts/feedback_triage.py analyze .runtime/feedback_inbox/<run-dir>

# Show deployed web UI credentials
ssh ivm-1 'cat /home/igor.sterhov/dev/af_dags_helper/.runtime/auth.env'
```

## Architecture

The data flow follows this pipeline:

```
DAG file (.py)
    ↓
DAGParser (Python AST)
    ↓ extracts: tasks, functions, SQL variables, decorators, connection IDs,
    ↓           HttpHook calls (API inlets), bulk_dump calls (table outlets)
SQLAnalyzer (sqlglot)
    ↓ extracts: FROM/JOIN tables → inlets, INSERT INTO → outlets, dictGet → dictionaries
ConnectionResolver
    ↓ resolves: @with_db decorator args, *_CONN_ID variables
FQNBuilder
    ↓ applies: server_mapping.yaml (connection_id → server_name)
OMEntityGenerator
    ↓ outputs: formatted Python code with inlets/outlets (TABLE and API entities)
FastAPI + NiceGUI web UI
    ↓ exposes: upload/paste/server-file analysis, generated output, diff, diagrams
```

**Key classes:**
- `DAGAnalyzer` ([main.py](main.py)) - orchestrates the pipeline
- `DAGParser` ([analyzer/dag_parser.py](analyzer/dag_parser.py)) - Python AST traversal
- `SQLAnalyzer` ([analyzer/sql_analyzer.py](analyzer/sql_analyzer.py)) - sqlglot-based SQL parsing
- `ConnectionResolver` ([analyzer/connection_resolver.py](analyzer/connection_resolver.py)) - connection ID resolution
- `FQNBuilder` ([generator/fqn_builder.py](generator/fqn_builder.py)) - FQN construction with mapping
- `OMEntityGenerator` ([generator/omentity_generator.py](generator/omentity_generator.py)) - code generation
- `DAGAnalysisService` ([web/analysis_service.py](web/analysis_service.py)) - shared analysis workflow for web UI
- `web.app` ([web/app.py](web/app.py)) - FastAPI + NiceGUI application and `/health`
- `BasicAuthMiddleware` ([web/auth.py](web/auth.py)) - Basic Auth gate for web UI HTTP/WebSocket routes; `/health` is public
- `scripts/feedback_triage.py` ([scripts/feedback_triage.py](scripts/feedback_triage.py)) - local-only feedback fetch/analyze workflow for DAG issue reports

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

**API calls with HttpHook (treated as API inlets):**
```python
API_CONNECTION_ID = 'api-wh-px-partner-sc'

@with_db(CH3_CONN_ID, 'ch3')
def update_links(ch3_hook):
    hook = HttpHook(http_conn_id=API_CONNECTION_ID, method='GET')
    response = hook.run('api/ref_links/...')
    ch3_hook.bulk_dump(table='dict_office.supplier_office_links', ...)
```
Generated output:
```python
inlets=[OMEntity(entity=Entity.API, fqn="api-wh-px-partner-sc")]
outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch3.dict_office.supplier_office_links")]
```

**API via op_kwargs (parameter resolution):**
```python
@with_db(CH3_CONN_ID)
def get_hr_api_data(hook, api_conn, dst_table, ...):
    api_hook = HttpHook(http_conn_id=api_conn, ...)
    hook.bulk_dump(table=dst_table, ...)

task = PythonOperator(
    op_kwargs=dict(
        api_conn=API_WB_DEPARTMENTS_CONN,
        dst_table=API_WB_DEPARTMENTS_DST_TABLE),
    ...)
```

**bulk_dump calls (treated as table outlets):**
- Direct: `hook.bulk_dump(table='schema.table', ...)` → outlet
- Via parameter: resolved from op_kwargs → string_variables

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
- Таблицы вне SQL переменных (динамический SQL, сложные op_kwargs)
- Несовпадение серверов - нужен маппинг в `server_mapping.yaml`
- Задача уже имеет OMEntity - утилита пропускает такие задачи
- SQL синтаксис не поддерживается sqlglot (ALTER, OPTIMIZE и т.д.)

## Feedback Triage Workflow

When the user asks to analyze new feedback / замечания from the web UI, do the work directly:

1. Fetch new DAG issue feedback from the deployed service:
   ```bash
   scripts/feedback_triage.py fetch --mode new
   ```
2. If direct `ssh ivm-1` fails from WSL/Codex, retry with the tsh fallback:
   ```bash
   AF_DAGS_HELPER_SSH_COMMAND="/mnt/c/Windows/System32/tsh17.exe ssh" \
   AF_DAGS_HELPER_DEPLOY_HOST="igor.sterhov@ivm-1.ivms.vm.dm.v2.wb-cloud.ru" \
   scripts/feedback_triage.py fetch --mode new
   ```
3. Analyze the printed run directory:
   ```bash
   scripts/feedback_triage.py analyze .runtime/feedback_inbox/<run-dir>
   ```
4. Read `.runtime/feedback_inbox/<run-dir>/review.md`, inspect attachments when needed, and summarize the likely root cause in chat.
5. Propose analyzer/service code changes to the user before editing analyzer logic.

Default fetch behavior must stay conservative: do not pass `--mark-exported` unless the user explicitly asks or the archive was already saved/analyzed and marking is intentionally part of the task. Feedback inbox data lives under `.runtime/feedback_inbox/`, which is local runtime state and must not be committed or deployed.

## Dependencies

- `sqlglot` - SQL parsing with ClickHouse dialect support
- `pyyaml` - YAML config handling
- `tkinter` - GUI (built into Python)
