from utils.db.clickhouse import get_ch_dwh_max_v2
import logging as log
from utils.data_exchange import copy_ch_to_ch_pipe
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from extra.get_pool import get_pool
from datetime import datetime, timedelta
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

CH22_CONN = 'do-ch2-02-recent'
LAKE_M_CONN = 'do-lake-m'

LAKE_LOAD_TO_CH22 = """
select shk_id, srid,
       argMaxIf(tare, ts, tare > 1 and s.waysheet_id > 1) as transfer_box_id,
       argMaxIf(s.waysheet_id, ts, tare > 1 and s.waysheet_id > 1) as waysheet_id,
       argMax(s.action_id, ts) as action_id,
       argMax(s.office_id, ts) as office_id,
       max(ts) + toIntervalHour(3) as dt
from core_wh.srid_tracker_rc_d s
where row_created >= parseDateTime64BestEffort('{min_time}')
  and row_created <= parseDateTime64BestEffort('{max_time}')
  and s.action_id >= 800 and s.action_id <= 1020 and s.action_id <> 1010 
  and shk_id > 0
group by srid, shk_id
SETTINGS distributed_group_by_no_merge = 1
FORMAT MsgPack
"""


@with_db(LAKE_M_CONN, 'lake')
@load_and_save_cutoff('lake_last_mile_box', 'lake', 'max_dt')
def load_new_states(lake_hook, cutoff):
    log.info('--- START ---')
    min_time, max_time, max_dwh_date, rec_count = \
        get_ch_dwh_max_v2(lake_hook, 'core_wh.srid_tracker_rc_d', 'row_created', cutoff, max_batch_records=100000000)

    if rec_count is not None:

        log.info('LAKE_LOAD_TO_CH22')
        copy_ch_to_ch_pipe(
            take_data=LAKE_LOAD_TO_CH22.format(min_time=min_time, max_time=max_time),
            insert_data='''INSERT INTO datamart.last_mile_box (shk_id, srid, transfer_box_id, waysheet_id,
                                                               action_id, office_id, dt) FORMAT MsgPack''',
            src_ch=LAKE_M_CONN,
            dst_ch=CH22_CONN,
            throw_if_empty=False)

    log.info('--- END ---')
    return max_time


default_args = {
    'owner': 'zhdanov.ivan2',
    'email': ['zhdanov.ivan2@wildberries.work'],
    'telegram': ['@Finder_84'],
    'band': ['zhdanov.ivan2'],
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}
with DAG(
        default_args=default_args,
        dag_id='lake_ch2_last_mile_box',
        schedule='*/30 * * * *',  # запускаем даг раз в 30 мин
        start_date=datetime(2025, 12, 19),
        catchup=False,
        max_active_runs=1,
        tags=[LAKE_M_CONN, CH22_CONN, 'datamart'],
        description="Даг обновляет витрину для отчета SS.68.",
) as dag:

    load_new_states_task = PythonOperator(
        task_id='load_new_states_task',
        doc="обновляет витрину для отчета SS.68.",
        python_callable=load_new_states,
        pool=get_pool(LAKE_M_CONN),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_rc")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch2-02-recent.datamart.last_mile_box")]
    )
