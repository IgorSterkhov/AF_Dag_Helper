from airflow.models import DAG
from extra.entities import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from hooks.clickhouse_hook import ClickhouseHook
from utils.decorators_with_conn import with_db
import pandas as pd
import logging as log
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


DAG_DESCRIPTION = """
    Сбор витрины с остатками на конкретный день, включая текущий. Пересчёт прошлого дня не меняет количество никак. Повторяемость идеальная. 
    29.12.2025 добавил и остатки sku_for_sale
"""

CH_CONN_ID = 'do-ch8'

TELEGA = '@texnix'

INS_DAY = """
insert into stage_wh.nm_for_sale_on_date --значение из предыдущего дня
(chrt_id, nm_id, update_dt, update_date, quantity, office_id)
select chrt_id, nm_id, update_dt,'{start}' as update_date_next, quantity, office_id
    from stage_wh.nm_for_sale_on_date   where update_date=date_add(day, -1, toDate('{start}')); 

insert into stage_wh.nm_for_sale_on_date --новые данные за текущие сутки
(chrt_id, nm_id, update_dt, quantity, office_id, update_date)
select chrt_id, nm_id, update_dt, quantity, office_id  ,toDate('{start}') as update_date
from stage_wh.nm_for_sale
where row_created>='{start}' and row_created<date_add(day, 1, toDate('{start}'))  and office_id!=0;

optimize table stage_wh.nm_for_sale_on_date partition {month} final; 

alter table stage_wh.nm_for_sale_on_date delete where quantity=0 and update_date='{start}';

optimize table stage_wh.nm_for_sale_on_date partition {month} final;
"""

INS_SKU="""
insert into stage_wh.sku_for_sale_on_date --значение из предыдущего дня
(sku_id, nm_id, office_id, update_date, quantity, delivery_mask)
select sku_id, nm_id, office_id, '{start}' as update_date_next, quantity, delivery_mask
    from stage_wh.sku_for_sale_on_date   where update_date=date_add(day, -1, toDate('{start}'));

insert into stage_wh.sku_for_sale_on_date --свежие данные
(sku_id, nm_id, office_id, update_date, quantity, delivery_mask)
select chrt_id as sku_id, nm_id,office_id,toDate('{start}') as update_date
      ,quantity, delivery_mask
from stage_wh.sku_for_sale
where update_dt>='{start}' and update_dt<date_add(day, 1, toDate('{start}'))  and office_id>0
order by sku_id, office_id,update_date desc
limit 1 by sku_id, office_id;

optimize table stage_wh.sku_for_sale_on_date partition {month} final;

alter table stage_wh.sku_for_sale_on_date delete where quantity=0 and update_date='{start}';

optimize table stage_wh.sku_for_sale_on_date partition {month} final;
"""

@with_db(CH_CONN_ID, 'ch', conn_kwargs={"send_receive_timeout": 19200})
def ins_nm_for_sale_on_date(ch_hook):
    #ch_hook = ClickhouseHook(clickhouse_conn_id=CH_CONN_ID)
    start_date = ch_hook.fetchone('select max(update_date) from stage_wh.nm_for_sale_on_date;')
    end_date = ch_hook.fetchone(" select toDate(max(row_created))-interval '1' DAY from stage_wh.nm_for_sale; ")
    res = (pd.date_range(
        min(start_date, end_date),
        max(start_date, end_date), freq='1D'
    )).strftime('%Y-%m-%d').tolist()
    for i in range(0, len(res)):
        start = str(res[i])
        month = start.replace('-', '')[:6]
        log.info(f'с {start} part {month}')
        ch_hook.exec_with_log(INS_DAY.format(start=start, month=month))

@with_db(CH_CONN_ID, 'ch', conn_kwargs={"send_receive_timeout": 21200})
def ins_sku_for_sale_on_date(ch_hook):
    #ch_hook = ClickhouseHook(clickhouse_conn_id=CH_CONN_ID)
    start_date = ch_hook.fetchone('select max(update_date) from stage_wh.sku_for_sale_on_date;')
    end_date = ch_hook.fetchone(" select toDate(max(update_dt))-interval '1' DAY from stage_wh.sku_for_sale; ")
    res = (pd.date_range(
        min(start_date, end_date),
        max(start_date, end_date), freq='1D'
    )).strftime('%Y-%m-%d').tolist()
    if len(res) > 7:
        k = 7 # для прокачки истории, с 2024-09-03 по 3 минуты идёт день, увеличиваясь арифметически.  2025-09-08 по 30 минут, даже на 9, 8 таймаутит
    else:
        k = len(res)
    for i in range(0, k):
        start = str(res[i])
        month = start.replace('-', '')[:6]
        log.info(f'с {start} part {month}')
        ch_hook.exec_with_log(INS_SKU.format(start=start, month=month))


def_args = {
    'owner': 'izotov.dmitriy',
    'email': [],
    'email_on_failure': True,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'telegram': [TELEGA]
}

with DAG(
        default_args=def_args,
        description=DAG_DESCRIPTION,
        dag_id='ch8_nm_for_sale_date',
        schedule='6 3 * * *',
        start_date=datetime(2024, 8, 10),
        catchup=False,
        tags=["nm_for_sale", CH_CONN_ID, TELEGA,'stage_wh'],
        max_active_runs=1,
        max_active_tasks=3
) as dag:
    task_nm_for_sale_on_date = PythonOperator(
        task_id='task_nm_for_sale_on_date',
        python_callable=ins_nm_for_sale_on_date,
        dag=dag,
        pool=get_pool(CH_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.nm_for_sale"),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch8.stage_wh.nm_for_sale_date"
            ),
        ],
    )

    task_sku_for_sale_on_date = PythonOperator(
        task_id='task_sku_for_sale_on_date',
        python_callable=ins_sku_for_sale_on_date,
        dag=dag,
        pool=get_pool(CH_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.sku_for_sale"),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch8.stage_wh.sku_for_sale_date"
            ),
        ],
    )
