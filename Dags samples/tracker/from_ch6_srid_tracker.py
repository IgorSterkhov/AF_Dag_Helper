from utils.data_exchange import copy_ch_to_ch
import logging as log
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extra.entities import Dataset

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

SRC_CONN_ID = 'do-ch6'
DST_DM3_CONN_ID = 'do-ch-deliverytime'
DST_CH_13_CONN_ID = 'do-ch13'
TELEGA = '@texnix'


# Перенос новых данных из CH6 на DM3
def ch6_to_dm(dst_connection_id):
    log.info(f'Загружаем строки из сридтрекера (пока препаред)')
    rows = copy_ch_to_ch(recipient_table='core_wh.srid_tracker_ttl',
                         take_data="""
                         select case when action_id=200 then 610 else action_id end as action_id
                         , box_type, dst_office_id, employee_id, next_office_id, office_id
                            , place_cod, shk_id, sm_id, srid, tare, tare_type, ts, waysheet_id, wb_sticker_id, payment_type
                            from srid_tracker.srid_tracker_prepared
                            where
                                action_id not in (0) 
                                --320 и 700 включил а 610 выключил 11.03.2024 / и включил, починили в 15:50 этого же дня.
                              -- and coalesce(box_type,0)!=2 --возвратные не нужны // теперь нужны, для datamart.srid_tracker_reorders
                              and row_created>={rv} and row_created<{po}
                              and ts<now() -- отсекаем статусы из будущего (ну и 3 часа буфера даём выходит, так как УТС)
                            limit 1 by srid,ts,action_id
                            FORMAT CSVWithNames; 
                         """,
                         take_max='select toInt32(max(row_created)) from srid_tracker.srid_tracker_prepared;',
    columns='action_id, box_type, dst_office_id, employee_id, next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, ts, waysheet_id, wb_sticker_id, payment_type',
                         shift_rv=4000,
                         src_ch=SRC_CONN_ID,
                         dst_ch=dst_connection_id
                         )
    return rows


def ch6_to_dm3():
    ch6_to_dm(DST_DM3_CONN_ID)


def ch6_to_ch_13():
    ch6_to_dm(DST_CH_13_CONN_ID)    


default_args = {
    'owner': 'izotov.dmitriy',
    'telegram': [TELEGA],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
        "from_ch6_srid_tracker",
        default_args=default_args,
        description=f"Переброска сридтрекера с 6 клика",
        schedule='*/2 * * * *',  # каждые 5 минут
        start_date=datetime(2023, 12, 25),
        tags=["tracker", SRC_CONN_ID, DST_DM3_CONN_ID, TELEGA],
        catchup=False,
        max_active_runs=1
) as dag:

    task_ch6_to_dm3 = PythonOperator(
        task_id="task_ch6_to_dm3",
        dag=dag,
        doc=f"История сридтрекера на {DST_DM3_CONN_ID}",
        python_callable=ch6_to_dm3,
        pool=get_pool(DST_DM3_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch6.srid_tracker.srid_tracker_prepared"), ],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.core_wh.srid_tracker_ttl"),]        
    )

    ch6_to_ch_13 = PythonOperator(
        task_id="ch6_to_ch_13",
        dag=dag,
        doc=f"История сридтрекера на {DST_CH_13_CONN_ID}",
        python_callable=ch6_to_ch_13,
        pool=get_pool(DST_CH_13_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch6.srid_tracker.srid_tracker_prepared"), ],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"), ]
    )
    
