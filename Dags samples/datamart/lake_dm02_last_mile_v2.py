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

"""
Собираем витрину по ШК которые зависли на последней миле.
Логика формирования отчёта:
  - Отбираем ластовые записи по сридам из срид-трекера;
  - Если у срида последний статус (500,610,620,630,640,700), то считается что он на последней миле,
    отмечается статусом "Отсортирован на СЦ";
  - Меняем статус на "Прочее" если статус более 700
Если у коробки все ШК в статусе "Прочее" или старше 6 мес, то удаляем коробку из витрины.
"""

DM02_CONN = 'do-ch-dm02'
LAKE_M_CONN = "do-lake-m"

LAKE_CLEAR_BUFFER = """
truncate table buffer.last_mile_by_shk
"""

LAKE_GET_SRIDS_FROM_TRACKER = """
--1. Инсертим пачку новых статусов.
insert into datamart.last_mile_by_shk_d
(shk_id, dt, state_id, transfer_box_id, office_id, status, lm_status_dt, 
 pvz_status_dt, tare_load_dt, sale_dt, dst_office_id, nm_id, price_rub)
  select shk_id, dt, state_id, transfer_box_id, _office_id, status, lm_status_dt,
         pvz_status_dt, tare_load_dt, sale_dt, dst_office_id, nm_id, price_rub
    from cluster('lake_m', view(
             select srid, shk_id,
                    max(ts) + toIntervalHour(3) as dt,
                    argMax(action_id, ts) as state_id,
                    argMaxIf(tare, ts, tare > 1 and tare_type = 'TBX' and action_id in (500,610,620,630,640,700)) as transfer_box_id,
                    argMax(office_id, ts) as _office_id,
                    if(state_id in (500,610,620,630,640,700), 'Отсортирован на СЦ', 'Прочее') as status,
                    nullIf(maxIf(ts, action_id in (500,610,620,630,640,700)) + toIntervalHour(3), toDateTime('1970-01-01 06:00:00')) as lm_status_dt,
                    nullIf(maxIf(ts, action_id in (1030, 1033)) + toIntervalHour(3), toDateTime('1970-01-01 06:00:00')) as sale_dt,
                    nullIf(maxIf(ts, action_id in (900, 910, 1000, 1010, 1020)) + toIntervalHour(3), toDateTime('1970-01-01 06:00:00')) as pvz_status_dt,
                    nullIf(maxIf(ts, action_id in (800)) + toIntervalHour(3), toDateTime('1970-01-01 06:00:00')) as tare_load_dt
               from core_wh.srid_tracker_rc
              where row_created >= parseDateTime64BestEffort(%(min_time)s)
                and row_created <= parseDateTime64BestEffort(%(max_time)s)
                and action_id >= 500
              group by srid, shk_id
         )) as m
  left asof join (
             select srid, assumeNotNull(dst_office_id) as dst_office_id, assumeNotNull(nm_id) as nm_id, create_ts,
                    toInt64(round(price * dictGetOrDefault('dict.cbr_currency', 'rate',
                        tuple(assumeNotNull(currency_id), toDate(create_ts)), 1))) as price_rub
               from positions.last_srid_position_v3_d
           prewhere srid in (
                        select srid
                          from core_wh.srid_tracker_rc_d
                         where row_created >= parseDateTime64BestEffort(%(min_time)s)
                           and row_created <= parseDateTime64BestEffort(%(max_time)s)
                           and action_id >= 500)
                and create_ts > (
                        select min(ts) - toIntervalMonth(3) + toIntervalHour(3)
                          from core_wh.srid_tracker_rc_d
                         where row_created >= parseDateTime64BestEffort(%(min_time)s)
                           and row_created <= parseDateTime64BestEffort(%(max_time)s)
                           and action_id >= 500)
           order by create_ts desc
            limit 1 by srid
         ) as p
      on p.srid = m.srid
     and p.create_ts < m.dt
order by dt
settings distributed_product_mode='local', parallel_distributed_insert_select = 0;
"""

LAKE_GET_LAST_MILE_DATA_ONLY = """
--2. Оставляем только записи с тарами по которым есть хоть один ШК с last_mile статусом
insert into buffer.last_mile_by_shk_d
(shk_id, dt, state_id, transfer_box_id, office_id, dst_office_id, price_rub, nm_id, min_dt, max_dt,
 status, lm_status_dt, pvz_status_dt, tare_load_dt, sale_dt, lost_dt)
select shk_id, dt, state_id, transfer_box_id, office_id, dst_office_id, price_rub, nm_id, min_dt, max_dt,
       status, lm_status_dt, pvz_status_dt, tare_load_dt, sale_dt, lost_dt_
from (select shk_id, dt, state_id, transfer_box_id, office_id, dst_office_id, price_rub, nm_id,
             min(lm_status_dt) over w as min_lm_status_dt,
             min(dt) over w as min_dt,
             max(dt) over w as max_dt,
             status, lm_status_dt, pvz_status_dt, tare_load_dt, sale_dt, lost_dt
      from datamart.last_mile_by_shk_d final
      /*отбрасываем ластовые статусы без тары и тары без единого lm-статуса по ШК, а так же старше 6 мес*/
      where transfer_box_id is not null 
        and toDate(dt) > today() - toIntervalMonth(6)
      window w as (partition by transfer_box_id)
      qualify min_dt = min_lm_status_dt) shk
left any join (select shk_id, if(type_id = 8, operation_dt, null) as lost_dt_
               from shk_storage.inventory_lost_shk_all_d
               where shk_id in (select shk_id from datamart.last_mile_by_shk_d)
               order by operation_dt desc
               limit 1 by shk_id
               SETTINGS distributed_group_by_no_merge = 1) lost using (shk_id)
/*отсеиваем списанные ШК, оставляем только те тары у которых на last_mile остался хотя бы один ШК*/
where lost_dt_ is null
SETTINGS distributed_product_mode='local';
"""

LAKE_UPD_DATAMART = """
ALTER TABLE datamart.last_mile_by_shk
REPLACE PARTITION tuple()
FROM buffer.last_mile_by_shk
"""

DM02_CLEAR_BUFFER = """
truncate table buffer.last_mile_v2
"""

LAKE_LOAD_TO_DM02 = """
select shk_id, dt, state_id, transfer_box_id, office_id, dst_office_id, price_rub,
       nm_id, min_dt, max_dt, status, lm_status_dt, pvz_status_dt, tare_load_dt, sale_dt
from datamart.last_mile_by_shk_d
FORMAT MsgPack
"""

DM02_UPD_DATAMART = """
ALTER TABLE datamart.last_mile_v2
REPLACE PARTITION tuple()
FROM buffer.last_mile_v2
"""


@with_db(LAKE_M_CONN, 'lake')
@load_and_save_cutoff('lake_last_mile', 'lake', 'max_dt')
def lake_get_new_states(lake_hook, cutoff):
    log.info('--- START ---')
    min_time, max_time, max_dwh_date, rec_count = \
        get_ch_dwh_max_v2(lake_hook, 'core_wh.srid_tracker_rc_d', 'row_created', cutoff, max_batch_records=50000000)

    if rec_count is not None:

        log.info("LAKE_CLEAR_BUFFER")
        lake_hook.on_cluster(lake_hook.exec_with_log, LAKE_CLEAR_BUFFER)

        max_time = max_time.replace(microsecond=0)
        log.info("LAKE_GET_SRIDS_FROM_TRACKER")
        lake_hook.exec_with_log(LAKE_GET_SRIDS_FROM_TRACKER, parameters={"min_time": min_time, "max_time": max_time})

        log.info('LAKE_GET_LAST_MILE_DATA_ONLY')
        lake_hook.exec_with_log(LAKE_GET_LAST_MILE_DATA_ONLY)

        log.info("LAKE_UPD_DATAMART")
        lake_hook.on_cluster(lake_hook.exec_with_log, LAKE_UPD_DATAMART)

        return max_time


@with_db(DM02_CONN, 'ch')
def lake_dm02_load_new_states(ch_hook):
    log.info('DM02_CLEAR_BUFFER')
    ch_hook.exec_with_log(DM02_CLEAR_BUFFER)

    log.info('LAKE_LOAD_TO_DM02')
    copy_ch_to_ch_pipe(
        take_data=LAKE_LOAD_TO_DM02,
        insert_data='''INSERT INTO buffer.last_mile_v2 (shk_id, dt, state_id, transfer_box_id, office_id, dst_office_id, 
                                                        price_rub, nm_id, min_dt, max_dt, status, lm_status_dt, 
                                                        pvz_status_dt, tare_load_dt, sale_dt) FORMAT MsgPack''',
        src_ch=LAKE_M_CONN,
        dst_ch=DM02_CONN,
        throw_if_empty=False)

    log.info('DM02_UPD_DATAMART')
    ch_hook.exec_with_log(DM02_UPD_DATAMART)

    log.info('--- END ---')


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
        dag_id='lake_dm02_last_mile_v2',
        schedule='*/30 * * * *',  # запускаем даг раз в 30 мин
        start_date=datetime(2025, 12, 1),
        catchup=False,
        max_active_runs=1,
        tags=[LAKE_M_CONN, DM02_CONN, 'datamart'],
        description="Даг обновляет витрину для отчета 31.Последняя миля v2 (redash)",
) as dag:

    lake_get_new_states_task = PythonOperator(
        task_id='lake_get_new_states_task',
        doc="Обновление данных на lake для отчета 31.Последняя миля v2 (redash)",
        python_callable=lake_get_new_states,
        pool=get_pool(LAKE_M_CONN),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_rc"),
                OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.last_srid_position_v3"),
                OMEntity(entity=Entity.TABLE, fqn="do-lake-m.shk_storage.inventory_lost_shk_all")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.last_mile_by_shk")]
    )

    lake_dm02_load_new_states_task = PythonOperator(
        task_id='lake_dm02_load_new_states_task',
        doc="Переносит данные с lake на dm02 для отчета 31.Последняя миля v2 (redash)",
        python_callable=lake_dm02_load_new_states,
        pool=get_pool(DM02_CONN),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.last_mile_by_shk")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.datamart.last_mile_v2")]
    )

    lake_get_new_states_task >> lake_dm02_load_new_states_task
