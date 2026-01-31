import logging as log
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.data_exchange import copy_ch_to_ch
from datetime import datetime, timedelta
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH6_CONN_ID = 'do-ch6'

SRC_TABLE1 = 'srid_tracker.srid_tracker_prepared'
DST_TABLE = 'datamart.srid_tracker_tares'

LOAD_SRID_TRACKER = """
select distinct on (action_id,tare,office_id)
    action_id
    ,box_type
    ,dst_office_id
    ,employee_id
    ,office_id
    ,place_cod
    ,source
    ,tare
    ,tare_type
    ,ts
    ,waysheet_id
    ,null as driver_id
    ,row_created
from srid_tracker.srid_tracker_prepared
where 1=1 
    and tare is not null
    and tare_type = 'TBX'
    and ts>'2023-07-01'
    and row_created >= {rv}  
    and row_created < {po}
FORMAT CSVWithNames
"""


def ch6_to_ch6():
    copy_ch_to_ch(recipient_table=DST_TABLE,
                  take_data=LOAD_SRID_TRACKER,
                  take_max='SELECT toInt32(max(row_created)) FROM srid_tracker.srid_tracker_prepared',
                  columns='action_id,box_type,dst_office_id,' \
                        'employee_id,office_id,place_cod,source,' \
                        'tare,tare_type,ts,waysheet_id,driver_id,row_created',
                  shift_rv=3600,
                  src_ch=CH6_CONN_ID,
                  dst_ch=CH6_CONN_ID)


default_args = {
    'owner': 'borisov.grigoriy4',
    'email': ['Borisov.Grigoriy4@wildberries.work'],
    'telegram': ['@grisha_borisov'],
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}


with DAG(
        dag_id='ch6_to_ch6_srid_tracker_tares',
        default_args=default_args,
        schedule=timedelta(hours=3),
        start_date=datetime(2024, 7, 11),
        catchup=False,
        max_active_tasks=1,
        max_active_runs=1,
        description="Переливает данные по всем перемещениям коробок в витрину datamart.srid_tracker_tares",
        tags=['@grisha_borisov', CH6_CONN_ID, 'tracker'],
) as dag:

    task_ch6_to_ch6 = PythonOperator(
        task_id='ch6_to_ch6',
        python_callable=ch6_to_ch6,
        dag=dag,
        execution_timeout=timedelta(minutes=30),
        pool=get_pool(CH6_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch6.datamart.srid_tracker_prepared")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch6.datamart.srid_tracker_tares")]

    )
