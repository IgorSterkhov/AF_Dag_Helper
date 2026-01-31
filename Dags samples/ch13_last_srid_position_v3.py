from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from utils.execes import exec_with_cutoff_ch

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH13_CONN_ID = "do-ch13"

CH13_LAST_V3 = """
truncate table buffer.last_srid_position_v3_new;

INSERT INTO buffer.last_srid_position_v3_new
(srid, status_oof, state, is_final, dt, suser_id, user_shard_id, create_ts, sm_id, dst_office_id, src_office_id,
 is_mp, delivery_time,  is_paid, order_type, pay_type, pay_mode, payment_type, chrt_id, nm_id, seller_id
 , price_rub, currency_id, price, row_created)
select
	srid, status_oof, state
	, if(dictGet('dict.positions_statuses', 'status_type', status_oof)<2,0,1) AS is_final
	, dt
    , suser_id, user_shard_id, create_ts, sm_id, dst_office_id, src_office_id,
 is_mp, delivery_time,  is_paid, order_type, pay_type, pay_mode, payment_type, chrt_id, nm_id, seller_id
     , round(price * dictGetFloat64OrDefault('dict.cbr_currency', 'rate',
                            (currency_id, coalesce(toDate(create_ts),today())),
                            dictGetFloat64OrDefault('dict.cbr_currency', 'rate',
                                                    (currency_id, today() - 3), 1))) as price_rub
     , currency_id, price, row_created
FROM (
    select *
    from remote_ch.oof_position_status_v3_rc
    where topic not in ('position-status')
        and row_created>=toDateTime(%(min_time)s)
        and row_created<=toDateTime(%(max_time)s)
) t
;

INSERT INTO buffer.last_srid_position_v3_new
(srid, status_oof, state, is_final, dt, suser_id, user_shard_id, create_ts, sm_id, dst_office_id, src_office_id,
 is_mp, delivery_time,delivery_time_delay,  is_paid, order_type, pay_type, pay_mode, payment_type, chrt_id, nm_id, seller_id
 , price_rub, currency_id, price, row_created)
select
    srid, status_oof, state, is_final, dt, suser_id, user_shard_id, create_ts, sm_id, dst_office_id, src_office_id,
 is_mp, delivery_time,delivery_time_delay,  is_paid, order_type, pay_type, pay_mode, payment_type, chrt_id, nm_id, seller_id
 , price_rub, currency_id, price, row_created
FROM positions.last_srid_position_v3
    where srid in (select srid FROM buffer.last_srid_position_v3_new)
    and create_ts>'2024-01-01'
settings max_insert_threads=5;

INSERT INTO positions.last_srid_position_v3
(srid, status_oof, state, is_final, dt, suser_id, user_shard_id, create_ts, sm_id, dst_office_id, src_office_id,
 is_mp, delivery_time,delivery_time_delay,  is_paid, order_type, pay_type, pay_mode, payment_type, chrt_id, nm_id, seller_id
 , price_rub, currency_id, price, row_created)
SELECT srid
       , argMax(status_oof, dt) as status_oof
       , argMax(state, dt) as state
       , argMax(is_final, dt) as is_final
       , max(dt) as dd
       , max(suser_id) as suser_id
       , max(user_shard_id) as user_shard_id 
       , min(create_ts) as create_ts
       , argMax( sm_id , dt) as sm_id
       , argMax( dst_office_id , dt) as dst_office_id
       , argMax( src_office_id , dt) as src_office_id
       , argMax( nullIf(is_mp,0) , dt) as is_mp
       , max(delivery_time) as delivery_time
       , max(delivery_time_delay) as delivery_time_delay
       , argMax( is_paid , dt) as is_paid
       , argMax( order_type , dt) as order_type
       , argMax( pay_type , dt) as pay_type
       , argMax( pay_mode , dt) as pay_mode
       , argMax( payment_type , dt) as payment_type
       , argMax( chrt_id , dt) as chrt_id
       , argMax( nm_id , dt) as nm_id
       , argMax( seller_id , dt) as seller_id
       , argMax( price_rub , dt) as price_rub
       , argMax( currency_id , dt) as currency_id
       , argMax( price , dt) as price
       , max(row_created) as _row_created
from buffer.last_srid_position_v3_new
GROUP BY srid
settings max_memory_usage = '190Gi',max_bytes_before_external_group_by = '170Gi', optimize_aggregation_in_order = 1;
"""


def fill_last_v3():
    exec_with_cutoff_ch(
        dst_table_name="positions.last_srid_position_v3",
        src_table_name="remote_ch.oof_position_status_v3_rc",
        sql=CH13_LAST_V3,
        conn_ch=CH13_CONN_ID,
        default_take_dt="""select toDateTime('2025-09-20 01:00:00')""",
        row_created="row_created",
        max_hour_offset_d=6,
        max_batch_records_d=20000000,
        back_seek_seconds_d=30,
    )


default_args = {
    "owner": "sokolov.k2",
    "email": ["sokolov.k2@wildberries.work"],
    "telegram": ["@ralkofinn"],
    "band": ["sokolov.k2"],
    "email_on_retry": False,
    "email_on_failure": True,
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="ch13_last_srid_position_v3",
    description="Новая версия дага ch_lake_last_srid_position, работает с v3 позишенами.",
    schedule="*/2 * * * *",
    start_date=datetime(2025, 12, 4),
    tags=["@sokolov.k2", CH13_CONN_ID, "positions", "last"],
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    fill_last_v3_task = PythonOperator(
        task_id="fill_last_v3_task",
        python_callable=fill_last_v3,
        execution_timeout=timedelta(hours=2),
        pool=get_pool(CH13_CONN_ID),
        inlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch13.remote_ch.oof_position_status_v3_rc"
            ),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch13.positions.last_srid_position_v3"
            ),
        ],
    )
