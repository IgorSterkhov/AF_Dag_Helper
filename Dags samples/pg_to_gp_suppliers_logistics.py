import logging as log
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.data_exchange import copy_to_kh_csv
from utils.decorators_with_conn import with_db
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


SRC_CONN = 'pg-logistics'
CH3_CONN = 'do-ch3'

CH3_DST_TAB = 'dict_personal.suppliers_logistics'

COLUMNS = """supplier_id, supplier_name, supplier_type_id, supplier_freelancer_id,is_deleted, vat, inn, is_block_pay, vat_id, is_self, group_hierarchy_id,number_phone, supplier_sort_id, ts, rating_brak"""

SRC = """ SELECT supplier_id, supplier_name, supplier_type_id, supplier_freelancer_id
               , is_deleted, vat, TRIM(inn)::BIGINT, is_block_pay, vat_id, is_self, group_hierarchy_id
               , number_phone, supplier_sort_id, dt_upd::TIMESTAMP, rating_brak
            FROM suppliers.supplier
           WHERE dt_upd  > %s::TIMESTAMP
             AND TRIM(inn) ~ E'^\\\d+$'
           ORDER BY dt_upd; """


@with_db(CH3_CONN, 'ch3')
def get_supplier_kh(ch3_hook):
    max_dt = ch3_hook.fetchone(f'SELECT max(ts) FROM {CH3_DST_TAB}')
    log.info(max_dt)
    src_ch = """ SELECT supplier_id,TRIM(supplier_name,'''')as supplier_name,supplier_type_id,supplier_freelancer_id::bigint
                   , is_deleted, vat, TRIM(inn)::bigint as inn, is_block_pay, vat_id, is_self, group_hierarchy_id
                   , number_phone, supplier_sort_id, dt_upd::TIMESTAMP(0) as ts, rating_brak
                FROM suppliers.supplier
               WHERE 1=1
               AND TRIM(inn) ~ E'^\\\\\d+$'
               """

    copy_to_kh_csv(src_connection=SRC_CONN,
                   dst_connection=CH3_CONN,
                   src_table_name='(' + src_ch + ') as t' + f""" where ts>'{max_dt}'""",
                   dst_table_name=CH3_DST_TAB,
                   columns=COLUMNS,
                   need_trunc='no',
                   db_type='pg')


def_args = {
    'owner': 'borisov.grigoriy4',
    'email': ['borisov.grigoriy4@wb.ru'],
    'telegram': ['@grisha_borisov'],
    'email_on_failure': False,
    'max_active_tasks': 1,
}

with DAG(
        default_args=def_args,
        description="""Даг выполняет перенос новых либо измененных суплаеров от логистов""",
        dag_id='pg_to_gp_suppliers_logistics',
        schedule='43 5,8,15 * * *',  # At minute 43 past hour 5, 8, and 15 UTC
        start_date=datetime(2024, 2, 15),
        tags=["@grisha_borisov","@Novotrex", "suppliers", SRC_CONN, CH3_CONN],
        max_active_tasks=1,
        max_active_runs=1,

) as dag:
    task_get_supplier_kh = PythonOperator(
        task_id='get_supplier_kh',
        python_callable=get_supplier_kh,
        dag=dag,
        pool=get_pool(CH3_CONN),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"pg-logistics.suppliers.supplier")],
        outlets= [OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_personal.suppliers_logistics")]
    )
    task_get_supplier_kh
