import logging as log
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from utils.decorators_with_conn import with_db
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


API_CONNECTION_ID = 'api-wh-px-partner-sc'
CH3_CONN_ID = 'do-ch3'

TELEGA = '@artemy_kravtsov'
DESCRIPTION = 'Справочник партнёрских СЦ, api => [ch3]. DataOps-11332. dwh-unit#215'
FIELDS = [
    'supplier_id', 'office_id', 'organization_inn', 
    'organization_name', 'wh_id', 'wh_name', 'country_code']


@with_db(CH3_CONN_ID, 'ch3')
def update_links(ch3_hook):
    hook = HttpHook(http_conn_id=API_CONNECTION_ID, method='GET')
    response = hook.run('api/ref_links/suppliers/warehouses/links')
    response.raise_for_status()
    data = response.json().get('data')
    if data is None:
        log.info('No links found')
        return
    ch3_hook.bulk_dump(
        table='dict_office.supplier_office_links', 
        columns=FIELDS, 
        data=data)       


with DAG(
    dag_id='api_ch3_dict_sc_suppliers',
    description=DESCRIPTION,
    schedule=timedelta(days=1),
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["dict", 'api', 'offices', CH3_CONN_ID, TELEGA,"@grisha_borisov"],
    max_active_runs=1,
    default_args=dict(
        owner='kravcov.artemiy',
        telegram=[TELEGA],
        retries=3,
        retry_delay=timedelta(minutes=5))):

    task_update_offices = PythonOperator(
        task_id='task_update_supplier_office_links',
        python_callable=update_links,
        pool=get_pool(CH3_CONN_ID),
        inlets=[OMEntity(entity=Entity.API, fqn="api-wh-px-partner-sc")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_office.supplier_office_links")]
        )

    task_update_offices
