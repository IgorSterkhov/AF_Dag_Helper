from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from extra.entities import Dataset
from hooks.clickhouse_hook import ClickhouseHook
from utils.decorators_with_conn import with_db

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH13_CONN_ID = "do-ch13"
DESCRIPTION = "Подготавливаем данные для апи ННСЗ и ОРДО по новым датам доставки"

MAX_H="""
select min(m) from (
select max(row_created) as m from positions.last_srid_position_v3
         where create_ts>now()-interval '3' DAY
union  all
select max(row_created) as m from core_wh.srid_tracker_ttl );
"""

INS_DELAY = """
truncate  table buffer.delay_history;

insert into buffer.delay_history
(rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date, delivery_date_delay
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type,  currency_code, price, actual_hour)
select rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date, delivery_date_delay
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type,  currency_code, price,toDateTime('{actual_hour}') as actual_hour
    from (
select srid as rid, status_oof, dt,suser_id as user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id
     , ddate as delivery_date, delivery_time_delay as delivery_date_delay
, chrt_id as sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type, currency_id as  currency_code, price
       ,coalesce(delivery_time_delay,delivery_time) as ddate
       ,toHour(addHours('{actual_hour}',dictGet('dict.branch_office','hour_offset',dst_office_id)-3)) as now_h
from positions.last_srid_position_v3 final
               where now_h=18
                 and status_oof in (27,7)
               and ddate<=toDate('{actual_hour}')
        and srid not in (select rid from positions.delay_history where actual_hour>'{actual_hour}'-interval '1' DAY) -- отсекаем уже рассчитанные
        ) as v3;

insert into buffer.delay_history
(rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type, currency_code, price, actual_hour)
select rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date_delay as delivery_date
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type
, currency_code, price,'{actual_hour}' as actual_hour
    from positions.delay_position_v3
        where rid in (select rid from buffer.delay_history);

insert into positions.delay_history
(rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date, delivery_date_delay
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type,  currency_code, price, actual_hour)
select rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date, delivery_date_delay
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type,  currency_code, price,actual_hour
    from (
select rid, status_oof, dt, user_id, user_shard_id, is_mp, create_ts, sm_id, dst_office_id, src_office_id, delivery_date, delivery_date_delay
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type,  currency_code, price,actual_hour
    from buffer.delay_history
order by rid,delivery_date desc
limit 1 by rid )
as d where delivery_date<=toDate('{actual_hour}');

insert into core_wh.delay_srid_tracker
(action_id, dst_office_id, employee_id, next_office_id, office_id, goods_id, sm_id, rid, tare_id, tare_type, ts, user_id, user_shard_id, is_mp
, new_dt, waysheet_id, wbsticker_id, delivery_date, src_office_id
, sku_id, nm_id, seller_id, is_paid, order_type, pay_type, pay_mode, payment_type, currency_code, price, create_ts, actual_hour)
with (toDateTime('{actual_hour}')) as actual
select tt.action_id,v3.dst_office_id,tt.employee_id,nullIf(tt.next_office_id,0) as next_office_id,tt.office_id,tt.shk_id
     ,coalesce(v3.sm_id,tt.sm_id) as sm_id,rid,tt.tare,tt.tare_type,tt.ts
     ,v3.user_id, v3.user_shard_id, v3.is_mp
     ,new_dt,tt.waysheet_id,tt.wb_sticker_id,delivery_date
     ,coalesce(tt.wh_office_id,v3.src_office_id) as  src_office_id
     , v3.sku_id, v3.nm_id, v3.seller_id
     , is_paid, order_type, pay_type, pay_mode
     ,coalesce(tt.payment_type,v3.payment_type) as payment_type
     , v3.currency_code, v3.price,v3.create_ts,actual
from
    (select srid as rid,action_id,employee_id,next_office_id,office_id,shk_id,sm_id,tare,tare_type,ts,waysheet_id,wb_sticker_id,payment_type
     ,if(action_id=210,office_id,null) as  wh_office_id
    from core_wh.srid_tracker_ttl
        where srid in (select rid from  positions.delay_history
      prewhere actual_hour=actual)
        and action_id not in (400,500,800,205) -- логистические статусы могут быть абы где, берём физические пики на офисах
        and shk_id>0 -- только собранные шк
        and office_id>0
    order by ts desc limit 1 by srid
        )as tt
join (select rid,dst_office_id,sm_id,create_ts,delivery_date,is_paid,order_type,pay_type,pay_mode,payment_type
      ,row_created as new_dt,user_id, user_shard_id, is_mp, sku_id, nm_id, seller_id, currency_code, price, src_office_id
      from positions.delay_history
      prewhere actual_hour=actual
      and dst_office_id>0) as v3 using(rid)
where tt.action_id!=190 and tt.action_id <900 -- отменённые и доехавшие до пвз не пересчитываем, они уже считай доехали.
settings max_memory_usage = '200Gi',max_bytes_before_external_group_by = '160Gi',max_insert_threads=20,optimize_aggregation_in_order = 1;

insert into positions.delay_position_v3
(rid, dt, status_oof, status_oof_type, create_ts, entry, topic, user_id, user_shard_id, sm_id
, delivery_date, delivery_date_delay, dst_office_id, src_office_id, is_mp, goods_id, order_type, pay_type
, is_paid, pay_mode, payment_type, sku_id, nm_id, seller_id
, wbsticker_id, currency_code, price)
with (toDateTime('{actual_hour}')) as actual
select rid, dt, status_oof, status_oof_type, create_ts, entry, topic, user_id, user_shard_id, sm_id
, delivery_date, delivery_date_delay --начинаем считать новый срок доставки.
, dst_office_id, src_office_id, is_mp,  goods_id, order_type, pay_type, is_paid, pay_mode, payment_type
     , sku_id, nm_id, seller_id
     ,wbsticker_id, currency_code, price
from (
select rid,d.new_dt as  dt,6 as status_oof,3 as  status_oof_type, create_ts,'srid-tracker' entry,'none' as topic
     , user_id, user_shard_id, sm_id
, dst_office_id, src_office_id, is_mp, office_id, goods_id, wbsticker_id, currency_code, price
     , delivery_date, is_paid, order_type, pay_type, pay_mode, payment_type, sku_id, nm_id, seller_id
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(office_id,1)),0) as curr_osm_1
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(office_id,2)),0) as curr_osm_2
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(office_id,3)),0) as curr_osm_3
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(office_id,4)),0) as curr_osm_4
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(dst_office_id,1)),0) as dst_osm_1
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(dst_office_id,2)),0) as dst_osm_2
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(dst_office_id,3)),0) as dst_osm_3
, nullIf(dictGet('dict.office_osm_zones','osm_id',tuple(dst_office_id,4)),0) as dst_osm_4
, dictGet('dict.lm_osm_dist','lm_osm_id',coalesce(dst_osm_4,dst_osm_3,dst_osm_2,dst_osm_1,0)) as lm_osm_id
, dictGet('dict.lm_osm_dist','duration',coalesce(dst_osm_4,dst_osm_3,dst_osm_2,dst_osm_1,0)) as duration_pvz
, dictGet('dict.logistic_route2','duration',tuple(coalesce(curr_osm_4,curr_osm_3,curr_osm_2,curr_osm_1,0),lm_osm_id)) as duration_lm
,toDate(dateAdd(HOUR ,duration_pvz+duration_lm,d.new_dt)) as delivery_date_delay
from core_wh.delay_srid_tracker as d
    where rid in (select rid from  core_wh.delay_srid_tracker as ww
      prewhere ww.actual_hour=actual)
) as podg
where delivery_date_delay>delivery_date;
"""


@with_db(CH13_CONN_ID, "ch", conn_kwargs={"send_receive_timeout": 7200})
def calc_delay(ch_hook):
    max_time = ch_hook.fetchone(MAX_H)
    actual_hour = max_time.replace(minute=0, second=0, microsecond=0)
    now_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
    if actual_hour < now_hour:
        raise AirflowException("Данные запаздывают, подождём 5 минут")
    ch_hook.exec_with_log(INS_DELAY.format(actual_hour=actual_hour))
    return 'всё ок'


with DAG(
    default_args={
        "owner": "izotov.dmitriy",
        "telegram": ["@texnix"],
        "email_on_retry": False,
        "email_on_failure": True,
        "retries": 6,
        "retry_delay": timedelta(minutes=5),
    },
    dag_id="ch13_for_do_api_delay",
    description=DESCRIPTION,
    schedule="20 * * * *",
    start_date=datetime(2025, 1, 19),
    tags=["@texnix", "srid_tracker", 'api', CH13_CONN_ID],
    catchup=False,
    max_active_runs=1,
) as dag:
    task_calc_delay = PythonOperator(
        task_id="task_calc_delay",
        dag=dag,
        python_callable=calc_delay,
        pool=get_pool(CH13_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.positions.delay_history"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.positions.last_srid_position_v3"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.positions.buffer.delay_history"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.core_wh.delay_srid_tracker"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.positions.delay_position_v3"),
        ],
    )


