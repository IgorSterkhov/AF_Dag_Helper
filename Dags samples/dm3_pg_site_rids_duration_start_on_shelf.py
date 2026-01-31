from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.curs import CursIOClickHouse
from utils.decorators_with_conn import with_db
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


DESCRIPTION = "Назначение: Заполняет таблицу site.v_late_rids_duration_start_on_shelf на pg1 из витрины со сроками доставки в3 на ch13"

CH13_CONN_ID = 'do-ch13'
PG1_CONN_ID = "do-pg1"
TELEGA = '@i_sterkhov'

CH13_SELECT = """
SELECT  key_office_id as office_id,
        toDecimal32(sum(duration) / sum(cnt) / 3600, 2) as duration,
        toUInt32(sum(cnt)/7) as rids_cnt,
        toUInt16(1) as measure
FROM datamart.v3_by_offices new final
WHERE key_office_type = 1
  AND measure_code = 0
  AND measure_date >= today() - interval 7 day AND measure_date < today()
GROUP BY key_office_id
HAVING rids_cnt>99
ORDER BY key_office_id
"""

PG_TRUNCATE = "TRUNCATE TABLE site.v_late_rids_duration_start_on_shelf"

PG_INSERT = """
COPY site.v_late_rids_duration_start_on_shelf 
    (office_id, duration, rids_cnt, measure)
from stdin (format csv, delimiter '|', encoding 'utf8', null '\\N')
"""

@with_db(PG1_CONN_ID, 'pg')
def pg_load_data(pg_hook, pg_curs):
    ch13_curs_hook = CursIOClickHouse(
        connection_id=CH13_CONN_ID,
        query=CH13_SELECT,
        use_header=False)
    ch13_curs_hook.execute()
    pg_hook.exec_with_log(PG_TRUNCATE)
    pg_curs.copy_expert(PG_INSERT, ch13_curs_hook)
    ch13_curs_hook.close()

with DAG(
        dag_id="dm3_pg_site_rids_duration_start_on_shelf",
        description=DESCRIPTION,
        schedule_interval= '0 * * * *',  # 1 раз в час в 0 минут, тк источник заполняется раз в час в 45 минут (длит +-9 мин)
        start_date=datetime(2025, 8, 27),
        tags=[CH13_CONN_ID, PG1_CONN_ID, "site"],
        catchup=False,
        max_active_tasks=1,
        max_active_runs=1,
        default_args=dict(
                    owner='sterhov.igor',
                    telegram=[TELEGA],
                    catchup=False,
                    email_on_failure=True,
                    retries=2,
                    retry_delay=timedelta(minutes=10)),
) as dag:

    task_dm3_speed_agr_offices = PythonOperator(
        task_id="pg_load_data",
        dag=dag,
        doc="заливка данных на пг",
        pool=get_pool(CH13_CONN_ID),
        python_callable=pg_load_data,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.v3_by_offices")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-pg1.gp_addon.site.v_late_rids_duration_start_on_shelf")]
    )
