import tempfile
import threading
import unittest
from pathlib import Path
from typing import Optional
import re

from analyzer.lightweight_table_parser import extract_sql_references
from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAPPING_FILE = PROJECT_ROOT / "config" / "server_mapping.yaml"


def analyze_source(source: str, mapping_content: Optional[str] = None) -> str:
    with tempfile.TemporaryDirectory() as tmp:
        dag_path = Path(tmp) / "dag.py"
        dag_path.write_text(source, encoding="utf-8")
        mapping_file = MAPPING_FILE
        if mapping_content is not None:
            mapping_file = Path(tmp) / "server_mapping.yaml"
            mapping_file.write_text(mapping_content, encoding="utf-8")
        service = DAGAnalysisService(PROJECT_ROOT, mapping_file)
        result = service.analyze(
            DAGAnalysisRequest(
                dag_path=dag_path,
                force_all_tasks=True,
                compare_existing=False,
            )
        )
        return result.generated_text


def task_block(generated: str, task_id: str) -> str:
    return generated.split(f"# Task: {task_id}", 1)[1].split("# Task:", 1)[0]


def generated_table_refs(generated: str) -> set:
    refs = set()
    for fqn in re.findall(r'OMEntity\(entity=Entity\.TABLE, fqn="([^"]+)"', generated):
        if "." in fqn:
            refs.add(fqn.split(".", 1)[1])
    return refs


class LineageRegressionTest(unittest.TestCase):
    def test_cursor_execute_bulk_dump_uses_cursor_connection_for_inlets_and_hook_connection_for_outlets(self):
        source = '''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db

CH9_CONN = "click-do-ch9"
TRINO_CONN = "trino-wbcrop-tracker"

SELECT_HISTORY = """
SELECT id
FROM tracker_insights.analytics_4.history
"""

@with_db(CH9_CONN, "ch9")
@with_db(TRINO_CONN, "trino")
def exchange(ch9_hook, trino_curs):
    for clh_table_name, select_query in (
        ("buffer.trackerwb_history", SELECT_HISTORY),
    ):
        trino_curs.execute(select_query)
        while data := trino_curs.fetchmany(10000):
            ch9_hook.bulk_dump(table=clh_table_name, data=data)

with DAG(dag_id="cursor_transfer"):
    PythonOperator(task_id="exchange", python_callable=exchange)
'''

        generated = analyze_source(source)

        self.assertIn(
            'fqn="trino-wbcrop-tracker.tracker_insights.analytics_4.history"',
            generated,
        )
        self.assertNotIn('fqn="click-do-ch9.tracker_insights.analytics_4.history"', generated)
        self.assertIn('fqn="click-do-ch9.buffer.trackerwb_history"', generated)

    def test_python_operator_lineage_includes_sql_from_called_helper_functions(self):
        source = '''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db
from utils.data_exchange import copy_ch_to_ch_pipe

DM_CONN = "click-do-ch13_sterhov"
LAKE_R_CONN = "click-do-lake-r"

GET_CHANGED_DATES_RECENT = """
SELECT DISTINCT toDate(measure_ts) AS measure_date
FROM datamart.v3_by_srid_rc_d
"""

BY_SUBJECT_FO = """
SELECT *
FROM datamart.v3_by_srid_d
"""

DM3_INSERT_BUFFER_SUBJECT_FO = """
INSERT INTO buffer.v3_by_subject_fo FORMAT MsgPack
"""

DM3_CLEARING_SUBJECT_FO = """
INSERT INTO buffer.v3_by_subject_fo
SELECT *
FROM datamart.v3_by_subject_fo
"""

DM3_ATTACH_DATA_TO_DATAMART = """
ALTER TABLE datamart.v3_by_subject_fo
ATTACH PARTITION '202406'
FROM buffer.v3_by_subject_fo
"""

def process_week_batch(ch13_hook):
    copy_ch_to_ch_pipe(
        take_data=BY_SUBJECT_FO,
        insert_data=DM3_INSERT_BUFFER_SUBJECT_FO,
        src_ch=LAKE_R_CONN,
        dst_ch=DM_CONN,
    )
    ch13_hook.exec(DM3_CLEARING_SUBJECT_FO)
    ch13_hook.exec_with_log(DM3_ATTACH_DATA_TO_DATAMART)

def process_weeks(ch13_hook):
    process_week_batch(ch13_hook)

@with_db(LAKE_R_CONN, "r")
@with_db(DM_CONN, "ch13")
def update_v3_by_subject_fo_recent(r_hook, ch13_hook):
    r_hook.get_records(GET_CHANGED_DATES_RECENT)
    process_weeks(ch13_hook)

with DAG(dag_id="helper_transfer"):
    PythonOperator(
        task_id="update_v3_by_subject_fo_recent",
        python_callable=update_v3_by_subject_fo_recent,
    )
'''

        generated = analyze_source(source)

        self.assertIn('fqn="do-lake-r.datamart.v3_by_srid_rc_d"', generated)
        self.assertIn('fqn="do-lake-r.datamart.v3_by_srid_d"', generated)
        self.assertIn('fqn="do-ch13.buffer.v3_by_subject_fo"', generated)
        self.assertIn('fqn="do-ch13.datamart.v3_by_subject_fo"', generated)

    def test_shared_helper_lineage_uses_each_callers_hook_connection(self):
        source = '''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db

A_PRIMARY_CONN = "click-a-primary"
A_SECONDARY_CONN = "click-a-secondary"
B_PRIMARY_CONN = "click-b-primary"
B_SECONDARY_CONN = "click-b-secondary"

READ_SRC = """
SELECT *
FROM datamart.shared_source
"""

def load_shared(hook):
    hook.get_records(READ_SRC)

@with_db(A_PRIMARY_CONN, "a_primary")
@with_db(A_SECONDARY_CONN, "a_secondary")
def load_a(a_primary_hook, a_secondary_hook):
    load_shared(a_secondary_hook)

@with_db(B_PRIMARY_CONN, "b_primary")
@with_db(B_SECONDARY_CONN, "b_secondary")
def load_b(b_primary_hook, b_secondary_hook):
    load_shared(b_secondary_hook)

with DAG(dag_id="shared_helper"):
    PythonOperator(task_id="load_a", python_callable=load_a)
    PythonOperator(task_id="load_b", python_callable=load_b)
'''

        generated = analyze_source(source)

        load_a_block = generated.split("# Task: load_a", 1)[1].split("# Task:", 1)[0]
        load_b_block = generated.split("# Task: load_b", 1)[1].split("# Task:", 1)[0]
        self.assertIn('fqn="click-a-secondary.datamart.shared_source"', load_a_block)
        self.assertNotIn('fqn="click-a-primary.datamart.shared_source"', load_a_block)
        self.assertIn('fqn="click-b-secondary.datamart.shared_source"', load_b_block)
        self.assertNotIn('fqn="click-a-secondary.datamart.shared_source"', load_b_block)
        self.assertNotIn('fqn="click-b-primary.datamart.shared_source"', load_b_block)

    def test_same_helper_called_with_different_hooks_keeps_both_connections(self):
        source = '''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db

A_CONN = "click-a"
B_CONN = "click-b"

READ_SRC = """
SELECT *
FROM datamart.shared_source
"""

def load_shared(hook):
    hook.get_records(READ_SRC)

@with_db(A_CONN, "a")
@with_db(B_CONN, "b")
def load_both(a_hook, b_hook):
    load_shared(a_hook)
    load_shared(b_hook)

with DAG(dag_id="shared_helper_twice"):
    PythonOperator(task_id="load_both", python_callable=load_both)
'''

        generated_holder = []
        thread = threading.Thread(
            target=lambda: generated_holder.append(analyze_source(source)),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=2)

        self.assertFalse(thread.is_alive(), "analysis did not terminate")
        generated = generated_holder[0]
        self.assertIn('fqn="click-a.datamart.shared_source"', generated)
        self.assertIn('fqn="click-b.datamart.shared_source"', generated)

    def test_feedback_lake_ch13_subject_fo_generates_reference_lineage(self):
        mapping = """
server_mapping:
  click-do-ch13_sterhov: do-ch13
  click-do-lake-r: do-lake-r
default_behavior: passthrough
strip_d_suffix: false
"""
        source = '''
from collections import defaultdict
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db
from utils.data_exchange import copy_ch_to_ch_pipe

DM_CONN = "click-do-ch13_sterhov"
LAKE_R_CONN = "click-do-lake-r"

GET_CHANGED_DATES_RECENT = """
SELECT DISTINCT toDate(measure_ts) AS measure_date
FROM datamart.v3_by_srid_rc_d
"""

GET_CHANGED_DATES_FULL = """
SELECT DISTINCT toDate(measure_ts) AS measure_date
FROM datamart.v3_by_srid_d
"""

GEO_MAP_TMP = """
DROP TEMPORARY TABLE IF EXISTS v3_geo_map_tmp;
CREATE TEMPORARY TABLE v3_geo_map_tmp
ENGINE = Memory
AS
SELECT region_id AS branch_region_id,
       dictGetOrDefault('dict.region_oksm', 'id_fed', country_id, toInt32(10070)) AS moderated_fo_cnt_id
  FROM (
        SELECT region_id, country_id, country_name
          FROM dict.branch_office
         WHERE dictHas('dict.region_oksm', country_id)
       )
 GROUP BY branch_region_id, moderated_fo_cnt_id
"""

BY_SUBJECT_FO = GEO_MAP_TMP + """;
SELECT src.measure_date,
       dictGetInt32OrDefault('dict.product_cards_nm_short', 'seller_id', nm_id, 0) AS seller_id,
       dictGetUInt16OrDefault('dict.product_cards_nm_short', 'subject_id', nm_id, 0) AS subject_id,
       toInt32(dictGet('dict.branch_office', 'region_id', src.src_office_id)) AS src_region_id
  FROM datamart.v3_by_srid_d AS src FINAL
 WHERE toDate(src.measure_ts) IN %(days_changed)s
FORMAT MsgPack;
"""

DM3_INSERT_BUFFER_SUBJECT_FO = f"""
INSERT INTO buffer.v3_by_subject_fo FORMAT MsgPack
"""

DM3_CLEARING_SUBJECT_FO = """
INSERT INTO buffer.v3_by_subject_fo
SELECT *
  FROM datamart.v3_by_subject_fo AS src FINAL
 WHERE measure_date IN %(days_changed)s
   AND (measure_date, zcurve) NOT IN (
       SELECT measure_date, zcurve
         FROM buffer.v3_by_subject_fo
        WHERE measure_date IN %(days_changed)s)
"""

DM3_GET_PARTITIONS_EXPR = """
SELECT DISTINCT partition
  FROM system.parts
 WHERE database = 'buffer'
   AND table = 'v3_by_subject_fo'
"""

DM3_ATTACH_DATA_TO_DATAMART = """
ALTER TABLE datamart.v3_by_subject_fo
ATTACH PARTITION '{partition_name}'
FROM buffer.v3_by_subject_fo
"""

def process_week_batch(ch13_hook, week_start, days_changed):
    copy_ch_to_ch_pipe(
        take_data=BY_SUBJECT_FO % dict(days_changed=days_changed),
        insert_data=DM3_INSERT_BUFFER_SUBJECT_FO,
        src_ch=LAKE_R_CONN,
        dst_ch=DM_CONN,
        multiquery=True,
    )
    ch13_hook.exec(DM3_CLEARING_SUBJECT_FO, parameters=dict(days_changed=days_changed))
    for partition_name, in ch13_hook.get_records(DM3_GET_PARTITIONS_EXPR):
        ch13_hook.exec_with_log(
            DM3_ATTACH_DATA_TO_DATAMART.format(partition_name=partition_name))

def process_weeks(ch13_hook, weeks):
    for week_start, week_days in weeks.items():
        process_week_batch(ch13_hook, week_start, week_days)

@with_db(LAKE_R_CONN, "r")
@with_db(DM_CONN, "ch13")
def update_v3_by_subject_fo_recent(r_hook, ch13_hook):
    r_hook.get_records(GET_CHANGED_DATES_RECENT)
    process_weeks(ch13_hook, defaultdict(list))

@with_db(LAKE_R_CONN, "r")
@with_db(DM_CONN, "ch13")
def update_v3_by_subject_fo_full(r_hook, ch13_hook):
    r_hook.get_records(GET_CHANGED_DATES_FULL)
    process_weeks(ch13_hook, defaultdict(list))

@with_db(DM_CONN, "ch13")
def update_v3_by_subject_fo_manual(ch13_hook):
    process_weeks(ch13_hook, defaultdict(list))

with DAG(dag_id="lake_ch13_v3_by_subject_fo"):
    PythonOperator(task_id="update_v3_by_subject_fo_manual", python_callable=update_v3_by_subject_fo_manual)
    PythonOperator(task_id="update_v3_by_subject_fo_recent", python_callable=update_v3_by_subject_fo_recent)
    PythonOperator(task_id="update_v3_by_subject_fo_full", python_callable=update_v3_by_subject_fo_full)
'''

        generated = analyze_source(source, mapping)

        self.assertNotIn("system.parts", generated)
        for task_id in (
            "update_v3_by_subject_fo_manual",
            "update_v3_by_subject_fo_recent",
            "update_v3_by_subject_fo_full",
        ):
            block = task_block(generated, task_id)
            self.assertIn('fqn="do-lake-r.datamart.v3_by_srid_d", key="1"', block)
            self.assertIn('fqn="do-lake-r.dict.branch_office", key="1"', block)
            self.assertIn('fqn="do-lake-r.dict.product_cards_nm_short", key="1"', block)
            self.assertIn('fqn="do-lake-r.dict.region_oksm", key="1"', block)
            self.assertIn('fqn="do-ch13.buffer.v3_by_subject_fo", key="2"', block)
            self.assertIn('fqn="do-ch13.datamart.v3_by_subject_fo", key="2"', block)
            self.assertIn('fqn="do-ch13.buffer.v3_by_subject_fo", key="1"', block)
            self.assertIn('fqn="do-ch13.datamart.v3_by_subject_fo", key="2"', block)

        recent_block = task_block(generated, "update_v3_by_subject_fo_recent")
        self.assertIn('fqn="do-lake-r.datamart.v3_by_srid_rc_d", key="1"', recent_block)

    def test_generated_omentity_covers_lightweight_extracted_tables_and_dicts(self):
        mapping = """
server_mapping:
  click-do-ch13_sterhov: do-ch13
  click-do-lake-r: do-lake-r
default_behavior: passthrough
strip_d_suffix: false
"""
        source = '''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db
from utils.data_exchange import copy_ch_to_ch_pipe

DM_CONN = "click-do-ch13_sterhov"
LAKE_R_CONN = "click-do-lake-r"

GET_CHANGED_DATES_RECENT = """
SELECT DISTINCT toDate(measure_ts) AS measure_date
FROM datamart.v3_by_srid_rc_d
"""

BY_SUBJECT_FO = """
SELECT dictGetInt32OrDefault('dict.product_cards_nm_short', 'seller_id', nm_id, 0) AS seller_id,
       dictGet('dict.branch_office', 'region_id', src.src_office_id) AS src_region_id
  FROM datamart.v3_by_srid_d AS src FINAL
"""

DM3_INSERT_BUFFER_SUBJECT_FO = """
INSERT INTO buffer.v3_by_subject_fo FORMAT MsgPack
"""

DM3_ATTACH_DATA_TO_DATAMART = """
ALTER TABLE datamart.v3_by_subject_fo
ATTACH PARTITION '{partition_name}'
FROM buffer.v3_by_subject_fo
"""

def process_week_batch(ch13_hook):
    copy_ch_to_ch_pipe(
        take_data=BY_SUBJECT_FO,
        insert_data=DM3_INSERT_BUFFER_SUBJECT_FO,
        src_ch=LAKE_R_CONN,
        dst_ch=DM_CONN,
    )
    ch13_hook.exec_with_log(DM3_ATTACH_DATA_TO_DATAMART.format(partition_name="202406"))

@with_db(LAKE_R_CONN, "r")
@with_db(DM_CONN, "ch13")
def update_v3_by_subject_fo_recent(r_hook, ch13_hook):
    r_hook.get_records(GET_CHANGED_DATES_RECENT)
    process_week_batch(ch13_hook)

with DAG(dag_id="self_check"):
    PythonOperator(task_id="update_v3_by_subject_fo_recent", python_callable=update_v3_by_subject_fo_recent)
'''

        generated = analyze_source(source, mapping)
        refs = extract_sql_references(source)
        expected_refs = set(refs.tables + refs.dicts) - {"system.parts"}

        self.assertFalse(expected_refs - generated_table_refs(generated))


if __name__ == "__main__":
    unittest.main()
