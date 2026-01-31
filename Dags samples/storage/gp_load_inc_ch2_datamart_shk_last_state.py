import time
import sys
from airflow.models import DAG
import logging as log
from extra.entities import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.data_exchange import copy_ch_to_ch
from hooks.clickhouse_hook import ClickhouseHook
from airflow.exceptions import AirflowException
from timeit import default_timer as timer
from utils.sensors.external_last_task_sensor import ExternalLastTaskSensor
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


DAG_DESCRIPTION = """
        - Забор данных по отсечке из CH-4 : shk_storage.shk_on_place - исторической таблицы по движениям ШК на МХ.
        - обработка на CH-2 и получение последнего состояние по статусу и по МХ
"""

CH2_CONN_ID = 'do-ch2-recent'
CH8_CONN_ID = 'do-ch8'

TELEGA = '@texnix'

LAST_STATE = """
insert into shk_storage.buffer_shk_on_place_last_state
    (shk_id, state_id, place_id, dt, employee_id, is_deleted, type_st)
select shk_id, state_id, place_id, dt, employee_id, isNull(place_id) as is_deleted, 1 as type_st
from shk_storage.buffer_shk_on_place
where  notEmpty(coalesce(state_id,''))
order by dt desc , place_id nulls first
limit 1 by shk_id;
"""

LAST_PLACE = """
insert into shk_storage.buffer_shk_on_place_last_state --3 m 18 s 1 день
    (shk_id, state_id, place_id, dt, employee_id, is_deleted, type_st)
select shk_id, state_id, place_id, dt, employee_id, isNull(place_id) as is_deleted, 2 as type_st
from shk_storage.buffer_shk_on_place
where  isNotNull(place_id)
;
"""

LAST_DELETED = """
insert into shk_storage.buffer_shk_on_place_last_state --2 m 9 s 1 день
    (shk_id, state_id, place_id, dt, employee_id, is_deleted, type_st)
select shk_id, state_id, place_id, dt, employee_id, isNull(place_id) as is_deleted, 3 as type_st
from shk_storage.buffer_shk_on_place
order by dt desc , place_id nulls first
limit 1 by shk_id;
;
"""

FROM_BUFFER_TO_LAST = """
insert into shk_storage.shk_on_place_last_state
(shk_id, state_id, place_id, dt, employee_id, is_deleted, _row_created, type_st) 
select shk_id, state_id, place_id, dt, employee_id, is_deleted, _row_created, type_st
from shk_storage.buffer_shk_on_place_last_state final;
"""


def fill_last():
    log.info(sys.version_info)

    """Заполняем таблицу последних мх на клике"""
    ch_hook = ClickhouseHook(CH2_CONN_ID)
    with ch_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('select count() from shk_storage.buffer_shk_on_place')
        rows = cursor.fetchone()[0]
        if rows == 0:
            rows = copy_ch_to_ch('shk_storage.buffer_shk_on_place',
                                 """select shk_id, state_id, place_id, dt, employee_id
                                    from shk_storage.shk_on_place
                                    where row_created>={rv} and row_created<{po}
                                    FORMAT CSVWithNames
                                    ;""",
                                 "select toInt32(max(row_created)) from shk_storage.shk_on_place ",
                                 """shk_id, state_id, place_id, dt, employee_id""",
                                 20000,
                                 CH8_CONN_ID,
                                 CH2_CONN_ID
                                 )
        if int(rows) == 0:
            return 'Нет данных'
        log.info(f' количество переброшенных строк: {rows}')
        time.sleep(100)
        cursor = conn.cursor()
        cnt = 0
        cnt_try = 0
        while cnt < int(rows):
            cursor.execute('select count() from shk_storage.buffer_shk_on_place;')
            cnt = cursor.fetchone()[0]
            if cnt == 0:
                return 'Нет данных'
            if cnt < int(rows):
                log.info('Ожидаем переброску данных')
                time.sleep(100)
                cnt_try += 1
                if cnt_try > 50:
                    raise AirflowException('Не дождались')
        cursor.execute(LAST_STATE)
        log.info('LAST_STATE done')
        cursor.execute(LAST_PLACE)
        log.info('LAST_PLACE done')
        cursor.execute(LAST_DELETED)
        log.info('LAST_DELETED done')
        cursor.execute(FROM_BUFFER_TO_LAST)
        log.info('FROM_BUFFER_TO_LAST done')
    return 'all done'


def truncate_buff():
    """Очистка буферных таблиц"""
    ch_hook = ClickhouseHook(CH2_CONN_ID)

    with ch_hook.get_conn() as conn:
        cursor = conn.cursor()
        log.info('Очистка буферных таблиц')
        cursor.execute('truncate table shk_storage.buffer_shk_on_place;')
        cursor.execute('truncate table shk_storage.buffer_shk_on_place_last_state;')


def_args = {
    'owner': 'izotov.dmitriy',
    'email': [],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'telegram': [TELEGA]
}

with DAG(
        default_args=def_args,
        description=f"Даг забирает исторические данные по ШК на МХ с do-ch8 и обновляет по последнему статусу и МХ на ch2 ",
        dag_id='gp_load_inc_ch2_datamart_shk_last_state',
        schedule='*/20 * * * *',
        start_date=datetime(2022, 6, 15, 9),
        tags=["storage", "last", CH2_CONN_ID, CH8_CONN_ID, TELEGA],
        max_active_runs=1,
        max_active_tasks=3
) as dag:
    task_fill_last = PythonOperator(
        task_id='task_fill_last',
        python_callable=fill_last,
        dag=dag,
        pool=get_pool(CH2_CONN_ID),
        doc='Забор исторических данных с do-ch8 по отсечке, и формирвоание таблицы последних состояний ШК ',
        inlets= [
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch8.shk_storage.shk_on_place",key="g1"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch2-recent.shk_storage.buffer_shk_on_place",key="g2"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch2-recent.shk_storage.buffer_shk_on_place_last_state",key="g3")
            ],
        outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch2-recent.shk_storage.buffer_shk_on_place",key="g1"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch2-recent.shk_storage.buffer_shk_on_place_last_state",key="g2"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch2-recent.shk_storage.shk_on_place_last_state",key="g3")
            ]
    )
    task_truncate_buff = PythonOperator(
        task_id='task_truncate_buff',
        python_callable=truncate_buff,
        pool=get_pool(CH2_CONN_ID),
        dag=dag
    )

    task_fill_last >> task_truncate_buff
