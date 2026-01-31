import logging as log
from utils.data_exchange import copy_ch_to_ch_pipe
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from extra.get_pool import get_pool
from datetime import datetime, timedelta
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

CH41_CONN = 'do-ch4'
DM02_CONN = 'do-ch-dm02'
CHDT_CONN = 'do-ch13'
LAKE_M_CONN = 'do-lake-m'

LAKE_CLEAR_BUFFER = """
truncate table buffer.kgt_srids_list;
truncate table buffer.kgt_avg_hours_delivery;
"""

CH4_CLEAR_BUFFER = """
truncate table buffer.kgt_srids_list
"""

DM02_CLEAR_BUFFER = """
truncate table buffer.kgt_srid_delay
"""

CHDT_LOAD_TO_LAKE = """
select srid, 
       nullIf(toDate(delivery_time), toDate('1970-01-01')) as delivery_time,
       nullIf(toDate(seller_date), toDate('1970-01-01')) as plan_seller_date, 
       nullIf(toDate(create_dt), toDate('1970-01-01')) as create_dt, payment_type 
from datamart.srid_tracker_tangle ot
semi join (select srid, delivery_time from datamart.kbt_by_sm_id final) kgt using srid
left any join (select srid, seller_date from datamart.orders_seller_date final where create_ts > today() - toIntervalMonth(2) and seller_date is not null) sd using srid
where coalesce(ot.payment_type, 'CRD') not in ('FPY', 'WAI', 'NPY', 'WPG', 'MPM', 'QRC', 'QRS')
  and coalesce(ot.payment_type, 'CRD') not like 'S%%'
  and ot.create_dt > today() - toIntervalMonth(2)
  and kgt.delivery_time < today()
FORMAT MsgPack
"""

LAKE_LOAD_TO_CH41 = """
select srid from buffer.kgt_srids_list_d final
FORMAT MsgPack
"""

CH41_LOAD_TO_LAKE = """
select srid, max(toDate(dt)) as fact_seller_date
from stage_nats.supplies_scans ss
where srid in (select srid from buffer.kgt_srids_list)
  and toDate(dt) > today() - toIntervalMonth(2)
group by srid
FORMAT MsgPack
"""

LAKE_GET_AVG_HOURS_DELIVERY = """
insert into buffer.kgt_avg_hours_delivery
(src_office_id, dst_office_id, avg_hours_delivery)
select src_office_id,
       dst_office_id,
       round(quantile(0.8)(date_diff('minute', src_ts, dst_ts)/60), 2) as avg_hours_delivery
from cluster('lake_m', view(
    select action_id,
           any(action_id) over w as next_action_id,
           office_id as src_office_id,
           any(office_id) over w as dst_office_id,
           ts as src_ts,
           any(ts) over w as dst_ts
    from core_wh.srid_tracker
    where srid in (select srid from buffer.kgt_srids_list)
      and toYYYYMMDD(ts) >= toYYYYMMDD(today() - toIntervalMonth(2))
      and toYYYYMMDD(ts) <= toYYYYMMDD(today() - toIntervalMonth(1))
    window w as (partition by srid order by ts rows between 1 following and 1 following)
    qualify action_id = 400 and next_action_id = 500
    limit 1 by srid
    ))
group by src_office_id, dst_office_id
SETTINGS insert_quorum = 7;
"""

LAKE_LOAD_TO_DM02 = """
select srid, ts, action_id, office_id, wh_office_id, src_office_id, dst_office_id, time_400_to_500, time_700_to_400, time_700_to_1010,
       delivery_time, plan_seller_date, fact_seller_date, avg_hours_delivery, office_type, order_type, create_dt, payment_type

       /* Определяем виновного в задержке */
       ,if((fact_seller_date > plan_seller_date and plan_seller_date > toDate('1970-01-01'))
           or
           (fact_seller_date > delivery_time and delivery_time > toDate('1970-01-01'))
           or
           (or(action_id <= 130, action_id = 1010) and office_type = 'mp')
           or
           (date_diff('hour', create_dt, fact_seller_date) > date_diff('hour', fact_seller_date, delivery_time) 
            and create_dt > toDate('1970-01-01') and fact_seller_date > toDate('1970-01-01') and delivery_time > toDate('1970-01-01')),
          'Селлер', null) as is_sl_blame

       ,if((action_id <= 130 and office_type <> 'mp' and is_sl_blame is null)
           or
           (ts_210 > delivery_time and office_type <> 'mp' and is_sl_blame is null)
           or
           date_diff('hour', ts_110, ts_210) > 24 and ts_110 > toDate('1970-01-01') and office_type <> 'mp' and is_sl_blame is null,
          'Склад', null) as is_wh_blame

       ,if(time_400_to_500 > avg_hours_delivery * 2, 'Долго везли до СЦ', null) as is_lg_blame_1

       ,if(time_700_to_400 > 24 or
           time_700_to_1010 > 24 or
           (action_id = 700 and date_diff('hour', min_action_ts, now()) > 24),
          'Долго не забирали с СЦ', null) as is_lg_blame_2

       ,if(action_id >= 210 and action_id < 700 and is_sl_blame is null and is_wh_blame is null and is_lg_blame_1 is null,
          'Долгая сборка на СЦ', null) as is_sc_blame

from cluster('lake_m', view(
    select srtr.srid as srid,
           srtr.ts as ts,
           srtr.action_id as action_id,
           srtr.office_id as office_id,
           srtr.dst_office_id as dst_office_id,
           srtr.ts_110 as ts_110,
           srtr.ts_210 as ts_210,
           srtr.min_action_ts as min_action_ts,
           srtr.office_type as office_type,
           if(srtr.is_dbw = 1, 'DBW', srtr.order_type_raw) as order_type,
           srtr.wh_office_id as wh_office_id,
           srtr.src_office_id as src_office_id,
           srtr.time_400_to_500 as time_400_to_500,
           srtr.time_700_to_400 as time_700_to_400,
           srtr.time_700_to_1010 as time_700_to_1010,
           coalesce(pos.delivery_time, kgt.delivery_time) as delivery_time,
           coalesce(kgt.plan_seller_date, toDate('1970-01-01')) as plan_seller_date,
           /*если нет ф.селлер.дт из шипингов, берем дату первого статуса не на складе поставщика*/
           coalesce(kgt.fact_seller_date, srtr.fact_seller_date, toDate('1970-01-01')) as fact_seller_date,
           kgt.create_dt as create_dt,
           kgt.payment_type as payment_type,
           dd.avg_hours_delivery as avg_hours_delivery,
           pos.release_dt as release_dt
    from (
          select srid, (arrayJoin(data_3) as tup).1 as ts,
                 tup.2 as action_id,
                 tup.3 as office_id,
                 tup.4 as dst_office_id,
                 tup.5 as ts_110,
                 tup.6 as ts_210,
                 tup.7 as office_type,
                 tup.8 as src_office_id,
                 tup.9 as wh_office_id,
                 tup.11 as order_type_raw,
                 if(tup.11 = 'FBS',  toDate(tup.10), null) as fact_seller_date,
                 max(tup.15) over w as is_dbw,
                 /*Находим первую дату в непрерывной серии статусов в рамках одного офиса по сриду*/
                 min(tup.16) over w as min_action_ts,
                 max(tup.17) over w as time_400_to_500,
                 argMax(tup.18, tup.17) over w as office_id_400,
                 argMax(tup.19, tup.17) over w as office_id_500,
                 max(tup.20) over w as time_700_to_400,
                 max(tup.21) over w as time_700_to_1010
          from (
          /*Определяем временные интервалы движения срида*/
          select srid
                ,arrayShiftLeft(isl_ts, 1, toDate('1970-01-01')) as next_isl_ts
                ,arrayMap(x, y -> if(x.2 = 400 and x.12 = 500, date_diff('hour', x.16, y), null), data_2, next_isl_ts) as time_400_to_500 --17
                ,arrayMap(x -> if(x.2 = 400 and x.12 = 500, x.3, null), data_2) as office_id_400 --18
                ,arrayMap(x -> if(x.2 = 400 and x.12 = 500, x.13, null), data_2) as office_id_500 --19
                ,arrayMap(x, y -> if(x.2 = 700 and x.12 = 400, date_diff('hour', x.16, y), null), data_2, next_isl_ts) as time_700_to_400 --20
                ,arrayMap(x, y -> if(x.2 = 700 and x.12 = 1010, date_diff('hour', x.16, y), null), data_2, next_isl_ts) as time_700_to_1010 --21
                ,arrayMap((x, t45, o4, o5, t74, t71) ->
                           tupleConcat(x, tuple(t45), tuple(o4), tuple(o5), tuple(t74), tuple(t71)),
                           data_2, time_400_to_500, office_id_400, office_id_500, time_700_to_400, time_700_to_1010) as data_3
          from (
          /*Определяем первые даты островков*/
          select srid
                ,arrayMap(x -> x.14, data_1) as isl_id
                ,arrayShiftRight(isl_id, 1, 0) as prev_isl_id
                ,arrayMap((x, y) -> x.14 != y, data_1, prev_isl_id) as divider
                ,arraySplit((x, y) -> y, arrayMap(x -> x.1, data_1), divider) as ts_list
                ,arrayFlatten(arrayMap(x -> arrayWithConstant(length(x), x[1]), ts_list)) as isl_ts --16
                ,arrayMap((x, t) -> tupleConcat(x, tuple(t)), data_1, isl_ts) as data_2
          from (
          /*Определяем островки и тип перевозки*/
          select srid
                ,arrayMap(x -> x.1, data_main) as ts
                ,arrayMap(x -> x.2, data_main) as actions
                ,arrayMap(x -> x.3, data_main) as offices
                ,arrayShiftRight(offices, 1, 0) as prev_office
                ,arrayShiftRight(actions, 1, 0) as prev_action
                ,arrayShiftLeft(actions, 1, 0) as next_action  --12
                ,arrayShiftLeft(offices, 1, 0) as next_office  --13
                 /*Размечаем id островков в рамках окна (срид, офис, статус)*/
                ,arrayCumSum((x, prev_office_id, prev_action_id) -> not(x.3 = prev_office_id and x.2 = prev_action_id), data_main, prev_office, prev_action) as isl_id --14
                ,arrayMap((x, next_action_id) -> x.11 = 'FBS' and x.2 in (110, 111, 115, 112, 120, 121, 130, 140, 210) and next_action_id >= 1000, data_main, next_action) as is_dbw --15
                ,arrayMap((x, a, o, i, d) ->
                           tupleConcat(x, tuple(a), tuple(o), tuple(i), tuple(d)),
                           data_main, next_action, next_office, isl_id, is_dbw) as data_1
          from (
          select srid, arraySort(x -> x.1, groupUniqArray(tup)) as data_main
          from (
              select srid,
                     tuple(ts                                                   --1
                          ,action_id                                            --2
                          ,office_id                                            --3
                          ,any(dst_office_id) over w as dst_office_id           --4
                          ,maxIf(ts, action_id = 110) over w as ts_110          --5
                          ,maxIf(ts, action_id = 210) over w as ts_210          --6
                          ,dictGet('dict.branch_office', 'office_type', office_id) as office_type  --7
                          ,argMin(office_id, ts) over w as src_office_id                           --8
                          ,argMinIf(office_id, ts, action_id in (110, 111, 115, 112, 120, 121, 130, 140, 210) and office_type <> 'mp') over w as wh_office_id --9
                          ,minIf(ts, office_type <> 'mp') over w as fact_seller_date_raw                                                                      --10
                          ,if(argMinIf(office_type, ts, action_id in (110, 111, 115, 112, 120, 121, 130, 140, 210)) over w = 'mp', 'FBS', 'FBO') as order_type_raw) as tup  --11
              from core_wh.srid_tracker
              where srid in (select srid from buffer.kgt_srids_list where create_dt > today() - toIntervalMonth(1))
                and action_id not in (190)
                and ts > today() - toIntervalMonth(1)
              window w as (partition by srid))
          group by srid)))) s
          window w as (partition by srid)
          order by ts desc
          limit 1 by srid) as srtr
    left any join (select * from buffer.kgt_avg_hours_delivery final) dd on tuple(dd.src_office_id, dd.dst_office_id) = tuple(srtr.office_id_400, srtr.office_id_500)
    left any join (select * from buffer.kgt_srids_list final) kgt on kgt.srid = srtr.srid
    left any join (select srid, delivery_time, dt as release_dt
                     from positions.last_srid_position_v3
                    where srid in (select srid from buffer.kgt_srids_list)
                      and toDate(create_ts) > today() - toIntervalMonth(1)
                      and status_oof = 16) pos on pos.srid = srtr.srid
    ))
/*отсекаем сриды что были доставлены вовремя*/
where not(toDate(release_dt) < toDate(delivery_time) and toDate(release_dt) > toDate('1970-01-01'))
FORMAT MsgPack
"""

DM02_REPLACE_PARTITIONS = """
alter table datamart.kgt_srid_delay
replace partition tuple()
from buffer.kgt_srid_delay
"""


@with_db(LAKE_M_CONN, 'lake')
@with_db(CH41_CONN, 'ch4')
@with_db(DM02_CONN, 'dm02')
def upd_kgt_srids_delay(lake_hook, ch4_hook, dm02_hook):
    log.info('--- START ---')

    log.info("CLEAR_BUFFERS")
    lake_hook.on_cluster(lake_hook.exec_with_log, LAKE_CLEAR_BUFFER)
    ch4_hook.exec_with_log(CH4_CLEAR_BUFFER)
    dm02_hook.exec_with_log(DM02_CLEAR_BUFFER)

    log.info('CHDT_LOAD_TO_LAKE')
    copy_ch_to_ch_pipe(
        take_data=CHDT_LOAD_TO_LAKE,
        insert_data='''INSERT INTO buffer.kgt_srids_list_d (srid, delivery_time, plan_seller_date,
                                                            create_dt, payment_type) FORMAT MsgPack''',
        src_ch=CHDT_CONN,
        dst_ch=LAKE_M_CONN,
        throw_if_empty=False)

    log.info('LAKE_LOAD_TO_CH41')
    copy_ch_to_ch_pipe(
        take_data=LAKE_LOAD_TO_CH41,
        insert_data='INSERT INTO buffer.kgt_srids_list (srid) FORMAT MsgPack',
        src_ch=LAKE_M_CONN,
        dst_ch=CH41_CONN,
        throw_if_empty=False)

    log.info('CH41_LOAD_TO_LAKE')
    copy_ch_to_ch_pipe(
        take_data=CH41_LOAD_TO_LAKE,
        insert_data='INSERT INTO buffer.kgt_srids_list_d (srid, fact_seller_date) FORMAT MsgPack',
        src_ch=CH41_CONN,
        dst_ch=LAKE_M_CONN,
        throw_if_empty=False)

    log.info("LAKE_GET_AVG_HOURS_DELIVERY")
    lake_hook.exec_with_log(LAKE_GET_AVG_HOURS_DELIVERY)

    log.info('LAKE_LOAD_TO_DM02')
    copy_ch_to_ch_pipe(
        take_data=LAKE_LOAD_TO_DM02,
        insert_data='''INSERT INTO buffer.kgt_srid_delay (srid, ts, action_id, office_id, wh_office_id, src_office_id, 
                                                          dst_office_id, time_400_to_500, time_700_to_400, 
                                                          time_700_to_1010, delivery_time, plan_seller_date, 
                                                          fact_seller_date, avg_hours_delivery, office_type, 
                                                          order_type, create_dt, payment_type, is_sl_blame, is_wh_blame, 
                                                          is_lg_blame_1, is_lg_blame_2, is_sc_blame) FORMAT MsgPack''',
        src_ch=LAKE_M_CONN,
        dst_ch=DM02_CONN,
        throw_if_empty=False)

    log.info("DM02_REPLACE_PARTITIONS")
    dm02_hook.exec_with_log(DM02_REPLACE_PARTITIONS)

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
        dag_id='lake_dm02_kgt_srids_delay',
        schedule='*/30 * * * *',  # запускаем даг раз в 30 мин
        start_date=datetime(2026, 1, 20),
        catchup=False,
        max_active_runs=1,
        tags=[CH41_CONN, DM02_CONN, CHDT_CONN, LAKE_M_CONN, 'datamart'],
        description="Даг обновляет витрину для отчета SS.69.",
) as dag:

    upd_kgt_srids_delay = PythonOperator(
        task_id='upd_kgt_srids_delay',
        doc="обновляет витрину для отчета SS.69.",
        python_callable=upd_kgt_srids_delay,
        pool=get_pool(LAKE_M_CONN),
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.srid_tracker_tangle"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.kbt_by_sm_id"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.orders_seller_date"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_nats.supplies_scans"),
                OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker"),
                OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.last_srid_position_v3")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.datamart.kgt_srid_delay")]
    )
