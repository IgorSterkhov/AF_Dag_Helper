import logging as log
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from extra.entities import Dataset
from hooks.clickhouse_hook import ClickhouseHook
from utils.decorators_with_conn import with_db
import time

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH13_CONN_ID = "do-ch13"
DESCRIPTION = "Обновляем клубок движущихся товаров со склада до пвз, и собираем по ним отчёт 1 и 2"

AGG = """
truncate table datamart.srid_tracker_tangle_init_last_buffer;

insert into datamart.srid_tracker_tangle_init_last_buffer 
select
    max(dst_office_id) as m_dst, max(src_office_id) as m_src
    , coalesce(max(lm_office_id),max(n_sort_office_id)) as m_lm , --- офис лм это или текущий, или же тот на который едет из сорта (будущий) DataOps-6044
        srid, max(wb_sticker_id) as m_wb_sticker_id, min(create_dt) as m_create_dt
    , max(payment_type) as m_payment_type,max(ma) as action_type
    , untuple(argMax(pack, (_ts, _action_id))) AS pack
from (
        select case when action_id in (220,230,310,620,630,640) then  dst_office_id end as dst_office_id
                , case when action_id in (120,130,140,210,111,112) then  office_id end as  src_office_id
                , case when action_id in (620,630,640,700,800) then  office_id end as  lm_office_id
                , case when action_id in (230,310) and next_office_id!=office_id
                    then  next_office_id end as  n_sort_office_id -- следующий офис после сортировки, он же офис лм если что. DataOps-6044
                , srid
                , wb_sticker_id as wb_sticker_id
                , case when action_id in (110,120,130,140,210) then ts else toDateTime('2030-01-01') end as  create_dt
                , payment_type --max может посчитать ненулевое
                , case when action_id in (190,1000,1010,1030,1040,1041,1050,1070,1080,1033) then  2
                    when action_id in (110,120,130,140,210,111,112) then 1
                    else 0 end as  ma
                , tuple(action_id, box_type, employee_id, next_office_id, office_id, place_cod,
                        shk_id, sm_id, tare, tare_type, ts, waysheet_id, row_created) as pack
                , ts AS _ts
                , action_id AS _action_id
        from core_wh.srid_tracker_ttl
        where row_created>='{on_date_1} {start}:00:00' and row_created<'{on_date_2} {end}:00:00'
        )  as f
group by srid
;

insert into datamart.srid_tracker_tangle
    (action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id
    , next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, ts, waysheet_id
    , wb_sticker_id, row_created, payment_type,deleted)

select action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id
       , next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, ts, waysheet_id
       , wb_sticker_id, row_created, payment_type,deleted
from (
    --те заказы, которые собраны в прошлом, ещё не закончены, просто двигаются с прежними параметрами
    select action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id
        , next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, ts, waysheet_id
        , wb_sticker_id, row_created, payment_type, 0 as deleted
    from (
          select i.action_id, i.box_type, i.employee_id, i.next_office_id, i.office_id, i.place_cod, i.shk_id, i.sm_id, srid
                 , i.tare, i.tare_type, i.ts, i.waysheet_id, i.row_created
                 , coalesce (i.m_wb_sticker_id,t.wb_sticker_id) as  wb_sticker_id
                 , coalesce(i.m_src,t.src_office_id) as src_office_id
                 , coalesce(i.m_dst,t.dst_office_id) as dst_office_id
                 , coalesce(i.m_lm,t.lm_office_id) as lm_office_id
                 , t.create_dt
                 , coalesce(i.m_payment_type,t.payment_type) as payment_type
          from datamart.srid_tracker_tangle_init_last_buffer as i
          join datamart.srid_tracker_tangle as t using (srid)
          where i.action_type=0
          )
    union all
    --те заказы которые собрались в текущей сборке
    select action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id
        , next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, ts, waysheet_id
        , wb_sticker_id, row_created, payment_type, 0 as deleted
    from (
          select i.action_id, i.box_type, i.employee_id, i.next_office_id, i.office_id, i.place_cod, i.shk_id, i.sm_id, srid
                 , i.tare, i.tare_type, i.ts, i.waysheet_id, i.row_created
                 , i.m_wb_sticker_id as  wb_sticker_id
                 , coalesce(t.t_src, i.m_src) as src_office_id
                 , coalesce(t_dst, i.m_dst) as dst_office_id
                 , i.m_lm as lm_office_id
                 , if(t.create_dt < i.m_create_dt, t.create_dt, i.m_create_dt) as create_dt
                 , coalesce(i.m_payment_type,t.payment_type) as payment_type
          from datamart.srid_tracker_tangle_init_last_buffer as i
          left join
                (
                    select srid, create_ts as create_dt, payment_type, nullIf(src_office_id,0) as t_src, nullIf(dst_office_id,0) as t_dst
                    from positions.last_srid_position_v3 as tn final
                    where tn.srid in (select sr.srid from datamart.srid_tracker_tangle_init_last_buffer as sr where sr.action_type=1)
                    and tn.create_ts > now() - INTERVAL '3' MONTH
                    and tn.create_ts < now()
                ) as t
          on i.srid=t.srid
          where i.action_type=1
          )
    union all
    --те заказы, которые собраны в прошлом, и закончены в текущей сборке
    select action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id
        , next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, now() as ts, waysheet_id
        , wb_sticker_id, row_created, payment_type, 1 as deleted
    from (
          select i.action_id, i.box_type, i.employee_id, i.next_office_id, i.office_id, i.place_cod, i.shk_id, i.sm_id, srid
                 , i.tare, i.tare_type, i.ts, i.waysheet_id, i.row_created
                 , coalesce(i.m_wb_sticker_id,t.wb_sticker_id) as  wb_sticker_id
                 , coalesce(i.m_src,t.src_office_id) as src_office_id
                 , coalesce(i.m_dst,t.dst_office_id) as dst_office_id
                 , coalesce(i.m_lm,t.lm_office_id) as lm_office_id
                 , t.create_dt
                 , coalesce(i.m_payment_type,t.payment_type) as payment_type
          from datamart.srid_tracker_tangle_init_last_buffer as i
          join datamart.srid_tracker_tangle as t
          using (srid)
          where i.action_type=2
        )
)
;

optimize table datamart.srid_tracker_tangle final;    --cleanap не использовать

insert into datamart.orders_in_transit_st_history
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt, hr, date_report, next_office_id)
select dictGet('dict.action_list_srid', 'stage_num', toUInt64(action_id)) as stage_num
     , office_id
     , dst_office_id
     , lm_office_id, src_office_id, count() as srid_cnt
     , {end} as hr, toDate('{on_date_2}') as date_report
     , coalesce(case when action_id=500 then lm_office_id end, next_office_id) as next_office_id2
from  datamart.srid_tracker_tangle
where dictGet('dict.action_list_srid', 'is_reportable', toUInt64(action_id)) = 1 --только строки для отчёта
and coalesce(payment_type,'CRD') not in ('STT','FPY', 'WAI', 'NPY', 'WPG', 'MPM', 'QRC', 'QRS')
and ts>now()-interval '45' DAY -- убираем недвижимые
group by office_id, dst_office_id, lm_office_id, src_office_id,stage_num,next_office_id2;

alter table datamart.srid_tracker_tangle delete where deleted;
optimize table datamart.srid_tracker_tangle final; 

insert into datamart.orders_in_transit_st_poo_history
(office_id, region_poo_type, poo_type, region_name, region_type, country_name, stage_num, srid_cnt, hr, date_report)
with
      (select max(date_report) from datamart.orders_in_transit_st_history) as max_date_report
    , (select max(hr) from datamart.orders_in_transit_st_history where date_report=max_date_report) as max_hr
    , dictGet('dict.branch_office', 'type_point', toUInt64(office_id)) as type_point
    , multiIf(type_point in (1, 10),'собственный',
            type_point in (5, 6, 7),'франшизный',
            type_point in (8, 9), 'партнерский',
            'собственный') as poo_type
    , dictGet('dict.branch_office', 'region_id', toUInt64(office_id)) as region_id
    , dictGet('dict.branch_office', 'country_name', toUInt64(office_id)) as country_name
    , dictGet('dict.branch_office', 'region_name', toUInt64(office_id)) as region_name
    , ( if(region_id in (50, 77), 'МСК и МО', 'Регионы') ) as region_type
    , ( case
        when region_type = 'Регионы' then concat('Р: ', poo_type)
        when region_type = 'МСК и МО' then concat('М: ', poo_type)
        else poo_type
    end ) as region_poo_type
select
    office_id
    , region_poo_type
    , poo_type
    , region_name
    , region_type
    , country_name
    , stage_num
    , sum(srid_cnt) as srid_cnt
    , hr
    , date_report
from datamart.orders_in_transit_st_history h
where stage_num in ('9','9.1','10')
    and date_report = max_date_report
    and hr = max_hr
    and region_name not in ('', '-')
group by office_id, region_poo_type, poo_type, region_name, region_type, country_name, stage_num, hr, date_report;

insert into datamart.orders_in_transit_st_hour_history
(stage_num, srid_cnt, hr, date_report,stage_full)
with
      (select max(date_report) from datamart.orders_in_transit_st_history) as max_date_report
    , (select max(hr) from datamart.orders_in_transit_st_history where date_report=max_date_report) as max_hr
select
 stage_num, sum(srid_cnt), hr, date_report
 ,concat(stage_num, ' ', dictGet('dict.action_list_srid_stage', 'stage_name', stage_num)) as stage_full
from datamart.orders_in_transit_st_history
where date_report = max_date_report and  hr = max_hr
group by stage_num, hr, date_report ;
"""

INS_ST = """
truncate table buffer.orders_in_transit_st_last_full_1;

insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}') and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_1)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '1' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_2)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '2' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_3)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '3' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_4)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '4' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_5)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '5' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_6)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '6' DAY and hr={hr};
insert into buffer.orders_in_transit_st_last_full_1
(stage_num, office_id, dst_office_id, lm_office_id, src_office_id, srid_cnt_7)
select stage_num,office_id,dst_office_id,lm_office_id,src_office_id,srid_cnt    from datamart.orders_in_transit_st_history
         where date_report=toDate('{on_date}')-interval '7' DAY and hr={hr};

truncate table datamart.orders_in_transit_st_last_full;
insert into datamart.orders_in_transit_st_last_full
(stage_num, stage, stage_full, from_to, office_id, dst_office_id, lm_office_id, src_office_id
, srid_cnt, srid_cnt_1, srid_cnt_2, srid_cnt_3, srid_cnt_4, srid_cnt_5, srid_cnt_6, srid_cnt_7
, office_name, dst_office_name, lm_office_name, src_office_name, region_name, country_name)
select stage_num
     , dictGet('dict.action_list_srid_stage','stage_name',stage_num) as stage
     , stage_num||' '||stage as stage_full
     ,'' as from_to
     , office_id, dst_office_id, lm_office_id, src_office_id
     , sum(srid_cnt) as srid_cnt
     , sum(srid_cnt_1) as srid_cnt_1
     , sum(srid_cnt_2) as srid_cnt_2
     , sum(srid_cnt_3) as srid_cnt_3
     , sum(srid_cnt_4) as srid_cnt_4
     , sum(srid_cnt_5) as srid_cnt_5
     , sum(srid_cnt_6) as srid_cnt_6
     , sum(srid_cnt_7) as srid_cnt_7
     , if(office_id = 0, '', format('{{}} ({{}})', dictGet('dict.branch_office', 'office_name', office_id), toUInt64(office_id))) as office_name
     , if(dst_office_id = 0, '', format('{{}} ({{}})', dictGet('dict.branch_office', 'office_name', dst_office_id), toUInt64(dst_office_id))) as dst_office_name
     , if(lm_office_id = 0, '', format('{{}} ({{}})', dictGet('dict.branch_office', 'office_name', lm_office_id), toUInt64(lm_office_id))) as lm_office_name
     , if(src_office_id = 0, '', format('{{}} ({{}})', dictGet('dict.branch_office', 'office_name', toUInt64(src_office_id)), src_office_id)) as src_office_name
     , if(dst_office_id = 0, 'НЕТ ОФИСА НАЗНАЧЕНИЯ', dictGet('dict.branch_office', 'region_name', toUInt64(dst_office_id))) as region_name
     , if(dst_office_id = 0, 'НЕТ ОФИСА НАЗНАЧЕНИЯ', dictGet('dict.branch_office', 'country_name', toUInt64(dst_office_id))) as country_name
from  buffer.orders_in_transit_st_last_full_1
group by stage_num,office_id, dst_office_id, lm_office_id, src_office_id
         , office_name, dst_office_name, lm_office_name, src_office_name, region_name, country_name
;
"""

TAKE_LAST_HOUR = """
with m_date as (select max(date_report) as m_report from datamart.orders_in_transit_st_history)
select max(hr) as hr, (select m_report from m_date) as max_date
from datamart.orders_in_transit_st_history
where date_report=(select m_report from m_date);
"""

TAKE_LAST_ST = """
select min((f.m_date, f.m_hr)).2, min((f.m_date, f.m_hr)).1    from(
select toDate(max(row_created)) as m_date,toHour(max(row_created)) as m_hr from core_wh.srid_tracker_ttl
union all
select toDate(max(row_created)) as m_date,toHour(max(row_created)) as m_hr from positions.last_srid_position_v3
    where create_ts>now()-INTERVAL 7 DAY
) as f;
"""

MAKE_BACKUP_QUERY = """
insert into archive.srid_tracker_tangle_bak
    (action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id, next_office_id, 
    office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, create_dt, ts, waysheet_id, 
    wb_sticker_id, row_created, payment_type, date_bak)

select action_id, box_type, src_office_id, dst_office_id, lm_office_id, employee_id, 
       next_office_id, office_id, place_cod, shk_id, sm_id, srid, tare, tare_type, 
       create_dt, ts, waysheet_id, wb_sticker_id, row_created, payment_type, 
       now() as  date_bak
from datamart.srid_tracker_tangle
"""

TAKE_LAST_HOUR_POO_PICKS = """
with m_date as (select max(date_report) as m_report from datamart.orders_in_transit_st_poo_picks_history)
select max(hr) as hr, (select m_report from m_date) as max_date
from datamart.orders_in_transit_st_poo_picks_history
where date_report=(select m_report from m_date);
"""

AGG_POO_PICKS = """
create temporary table tmp9
(
    `srid` String ,
    action_type UInt8,
    `action_id` UInt16 ,
    `office_id` Int32 COMMENT 'айди офиса где произошло событие',
    `ts` DateTime COMMENT 'датавремя текущего статуса по трекеру UTC+0',
    `row_created` DateTime COMMENT 'из srid_tracker_ttl'
)
as
select
    srid
    , max(ma) as action_type
    , untuple(argMin(pack, (_ts, _action_id))) AS pack
from (
        with dictGet('dict.branch_office', 'type_point', office_id) as type_point
        select
            srid
            , case when action_id in (900, 910) then 1
              else 2 end as ma
            , tuple(action_id, office_id, ts, row_created) as pack
            , ts AS _ts
            , action_id AS _action_id
            , row_created
        from core_wh.srid_tracker_ttl
        where action_id>=900
            and row_created>='{start}' and row_created<'{end}'
            and  type_point in (1,2,3,5,6,7,8,9,10)
            and (box_type in (0,1) or box_type is null)
        )  as f
group by srid;

create temporary table tmp10
(
    `srid` String ,
    action_type UInt8,
    `action_id` UInt16 ,
    `office_id` Int32 COMMENT 'айди офиса где произошло событие',
    `ts` DateTime COMMENT 'датавремя текущего статуса по трекеру UTC+0',
    `row_created` DateTime COMMENT 'из srid_tracker_ttl'
)
as
select
    srid
    , max(ma) as action_type
    , untuple(argMin(pack, (_ts, _action_id))) AS pack
from (
        with dictGet('dict.branch_office', 'type_point', toUInt64(office_id)) as type_point
        select
            srid
            , case when action_id = 1000 then 1
              else 2 end as ma
            , tuple(action_id, office_id, ts, row_created) as pack
            , ts AS _ts
            , action_id AS _action_id
            , row_created
        from core_wh.srid_tracker_ttl
        where action_id>=1000 and action_id!=1010
            and row_created>='{start}' and row_created<'{end}'
            and  type_point in (1,2,3,5,6,7,8,9,10)
            and (box_type in (0,1) or box_type is null)
        )  as f
group by srid;

insert into datamart.srid_tracker_tangle_poo_picks
(srid, stage_num, office_id, action_id, ts, row_created, deleted)
select srid, stage_num, office_id, action_id, ts, row_created, deleted
from (
    --те заказы, у которых появился 900 или 910й статус - вставляем в 9ю группу
    select action_id,
         '9' as stage_num,
         office_id,
         ts,
         row_created,
         srid,
         0 as deleted
    from tmp9 as i
    left anti join datamart.srid_tracker_tangle_poo_picks pp on pp.srid = i.srid and pp.stage_num='9'
    where i.action_type=1
    union all
    --те заказы, у которых появился 1000й статус - вставляем в 10ю группу
    select action_id,
         '10' as stage_num,
         office_id,
         ts,
         row_created,
         srid,
         0 as deleted
    from tmp10 as i
    left anti join datamart.srid_tracker_tangle_poo_picks pp on pp.srid = i.srid and pp.stage_num='10'
    where i.action_type=1
    union all
    --те заказы, у которых появился 1000+ статус - удаляем из 9й группы
    select action_id,
         '9' as stage_num,
         office_id,
         ts,
         row_created,
         srid,
         1 as deleted
    from tmp9 as i
    inner join datamart.srid_tracker_tangle_poo_picks pp on pp.srid = i.srid and pp.stage_num='9'
    where i.action_type=2
    union all
    --те заказы, у которых появился 1010+ статус - удаляем из 10й группы
    select action_id,
         '10' as stage_num,
         office_id,
         ts,
         row_created,
         srid,
         1 as deleted
    from tmp10 as i
    inner join datamart.srid_tracker_tangle_poo_picks pp on pp.srid = i.srid and pp.stage_num='10'
    where i.action_type = 2
    ) q ;

optimize table datamart.srid_tracker_tangle_poo_picks final;

alter table datamart.srid_tracker_tangle_poo_picks delete where deleted;
optimize table datamart.srid_tracker_tangle_poo_picks final;

insert into datamart.orders_in_transit_st_poo_picks_history
(office_id, region_poo_type, poo_type, region_name, region_type, country_name, stage_num, srid_cnt, srid_agr_cnt, hr, date_report)
with
     dictGet('dict.branch_office', 'type_point', toUInt64(office_id)) as type_point
    , dictGet('dict.branch_office', 'city_id', toUInt64(office_id)) as city_id
    , multiIf(type_point in (1, 10),'собственный',
            type_point in (5, 6, 7),'франшизный',
            type_point in (8, 9), 'партнерский',
            'собственный') as poo_type
    , dictGet('dict.branch_office', 'region_name', toUInt64(office_id)) as region_name
    , dictGet('dict.branch_office', 'region_id', toUInt64(office_id)) as region_id
    , dictGet('dict.branch_office', 'country_name', toUInt64(office_id)) as country_name
    , ( if(region_id in (50, 77), 'МСК и МО', 'Регионы') ) as region_type
    , ( case
        when region_type = 'Регионы' then concat('Р: ', poo_type)
        when region_type = 'МСК и МО' then concat('М: ', poo_type)
        else poo_type
    end ) as region_poo_type
select
    office_id
    , region_poo_type
    , poo_type
    , region_name
    , region_type
    , country_name
    , stage_num
    , count() as srid_cnt
    , 0 as srid_agr_cnt
    , toHour(row_created + interval 1 hour) as hr
    , toDate(row_created + interval 1 hour) as date_report
from datamart.srid_tracker_tangle_poo_picks h
where row_created>='{start}' and row_created<'{end}'
group by office_id, stage_num, hr, date_report
settings max_memory_usage = '15Gi'
         , max_bytes_before_external_group_by = '7Gi'
;         
"""


@with_db(CH13_CONN_ID, "ch", conn_kwargs={"send_receive_timeout": 7200})
def tangle_to_report_1(ch_hook, ch_curs):
    try_cnt = 0
    while try_cnt < 40:
        ch_curs.execute(TAKE_LAST_HOUR)
        hr, on_date = ch_curs.fetchone()
        print("Время в таблицах отчёта:", hr, on_date)
        # 2. Берём текущее время
        now_hour = datetime.now().hour
        # 3. проверка обновлённости данных на это чат в витринах core_wh.srid_tracker_ttl , positions.last_srid_position_v3
        ch_curs.execute(TAKE_LAST_ST)
        current_hour, current_date = ch_curs.fetchone()
        if hr == now_hour and hr == current_hour:
            return "все данные валидны на текущее время"
        if current_date != on_date:
            if current_hour > -1:  # здесь было 7 часов, попросили круглосуточно
                on_date_2 = on_date + timedelta(days=1)
                for i in range(hr, 23):
                    start = ("0" + str(i))[-2:]
                    end = ("0" + str(i + 1))[-2:]
                    log.info(f"Обновляем данные с {start} по {end} на дату: {on_date}")
                    ch_hook.exec_with_log(
                        AGG.format(
                            start=start, end=end, on_date_1=on_date, on_date_2=on_date
                        )
                    )
                    ch_hook.exec_with_log(INS_ST.format(hr=i + 1, on_date=on_date))
                log.info(
                    f"Обновляем данные с 23  по 00 c даты {on_date} по дату {on_date_2}"
                )
                ch_hook.exec_with_log(
                    AGG.format(
                        start=23, end="00", on_date_1=on_date, on_date_2=on_date_2
                    )
                )
                ch_hook.exec_with_log(INS_ST.format(hr="00", on_date=on_date_2))
                try_cnt = 118
            else:
                try_cnt += 1
                log.info("Пока не наступили нужные данные, отдыхаем пару минут")
                time.sleep(120)
        elif hr < 23 and current_hour > hr:
            print("Время в исторических данных:", current_hour, current_date)
            for i in range(hr, current_hour):
                start = ("0" + str(i))[-2:]
                end = ("0" + str(i + 1))[-2:]
                log.info(f"Обновляем данные с {start} по {end} на дату: {on_date}")
                ch_hook.exec_with_log(
                    AGG.format(
                        start=start, end=end, on_date_1=on_date, on_date_2=on_date
                    )
                )
                ch_hook.exec_with_log(INS_ST.format(hr=i + 1, on_date=on_date))
                if int(end) >= now_hour:
                    return "Данные обновлены"
        elif current_hour < -1:
            return "Утро, пропускаем"
        else:
            try_cnt += 1
            log.info("Пока нет новых данных, ожидаем минуту")
            time.sleep(60)
    return "Больше нет новых данных"


@with_db(CH13_CONN_ID, "ch", conn_kwargs={"send_receive_timeout": 7200})
def tangle_to_report_2(ch_hook, ch_curs):
    ch_curs.execute(TAKE_LAST_HOUR_POO_PICKS)
    hr, on_date = ch_curs.fetchone()
    dttm = datetime(on_date.year, on_date.month, on_date.day, hr)

    ch_curs.execute(TAKE_LAST_ST)
    current_hour, current_date = ch_curs.fetchone()
    dttm_cur = datetime(
        current_date.year, current_date.month, current_date.day, current_hour
    )
    hours_diff = int((dttm_cur - dttm).total_seconds() // 3600)
    print(f"hours_diff: {hours_diff}")

    dttm_list = [dttm + timedelta(hours=x) for x in range(hours_diff)]

    for d in dttm_list:
        time_st, start, end = datetime.now(), d, d + timedelta(hours=1)
        print(f"starting script AGG_POO_PICKS, start: {start}, end: {end}")
        ch_hook.exec_with_log(AGG_POO_PICKS.format(start=start, end=end))
        print(f"finished scripts, duration: {datetime.now() - time_st}")


def make_backup():
    ch_hook = ClickhouseHook(CH13_CONN_ID)
    with ch_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(MAKE_BACKUP_QUERY)


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
    "ch13_report_1",
    default_args=default_args,
    description=DESCRIPTION,
    schedule="4 * * * *",
    start_date=datetime(2025, 12, 7),
    tags=["@sokolov.k2", "srid_tracker", "datamart", CH13_CONN_ID, "do-ch4"],  # do-ch4 - относится к ремоте
    catchup=True,
    max_active_runs=1,
) as dag:
    task_tangle_to_report_1 = PythonOperator(
        task_id="task_tangle_to_report_1",
        dag=dag,
        python_callable=tangle_to_report_1,
        pool=get_pool(CH13_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_tangle"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.positions.last_srid_position_v3"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.orders_in_transit_st_history"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.orders_in_transit_st_last_full"),
        ],
    )

    task_tangle_to_report_2 = PythonOperator(
        task_id="task_tangle_to_report_2",
        dag=dag,
        python_callable=tangle_to_report_2,
        pool=get_pool(CH13_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_tangle_poo_picks"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.core_wh.srid_tracker_ttl"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.positions.last_srid_position_v3"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.orders_in_transit_st_poo_picks_history"),
        ],
    )

    skip_make_backup_task = ShortCircuitOperator(
        task_id="skip_make_backup",
        dag=dag,
        python_callable=lambda x: x == "0",
        op_args=["{{ logical_date.hour }}"],
        pool=get_pool(CH13_CONN_ID),
    )

    make_backup_task = PythonOperator(
        task_id="make_backup",
        dag=dag,
        python_callable=make_backup,
        pool=get_pool(CH13_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.srid_tracker_tangle"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.archive.srid_tracker_tangle_bak"),
        ],
    )

    task_tangle_to_report_1 >> task_tangle_to_report_2 >> skip_make_backup_task >> make_backup_task
