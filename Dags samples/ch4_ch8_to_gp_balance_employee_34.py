import logging as log
from datetime import date
from datetime import datetime, timedelta, timezone
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client

from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.utils.session import provide_session
from pytz import timezone
from sqlalchemy import func

from hooks.clickhouse_hook import ClickhouseHook
from utils.cutoff import get_cutoff, save_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


"""
ДАГ для обновления витрин отчета 5.34. 
Задача - DO-6062.
Заказчик - Калашникова Н.Н. (@natalikalash)
Исполнитель - Жданов И.А. (@Finder_84)
"""

# Коннекты
CH4_PRIVATE_CONN_ID = 'do-ch4-pr'
CH4_CONN_ID = 'do-ch4'
CH8_CONN_ID = 'do-ch8'
CH8_02_CONN_ID = "do-ch8-xc"
CH4_DP_CONN_ID = "do-ch4-dp"
DM02_CONN_ID = 'do-ch-dm02'
CH7_02_CONN_ID = 'do-ch-fines-02'
LAKE_M_CONN = "do-lake-m"

# Таблицы
DST_TABLE_DM0_CO = 'datamart.employee_cnt_operations_34'
DST_TABLE_DMO_SS = 'buffer.balance_employee_base_34'
DST_TABLE_DMO_ES = 'datamart.balance_employee_shk_34'
DST_TABLE_DMO_WO = 'buffer.balance_employee_writeoff_34'
CUTOFF_ES_LOST = 'balance_employee_lost_34'
CUTOFF_ES_POST = 'balance_employee_post_34'

# Новые отсечки
MAX_DT_CH8 = """select max(toInt32(row_created)) 
                from shk_storage.shk_on_place
                where toYYYYMM(row_created) > toYYYYMM(now() - interval 6 month)"""
MAX_DT_CH4_LOST = "select max(_row_created) from buffer.balance_employee_list_shk_34 where type_id = 0"
MAX_DT_CH4_POST = "select max(_row_created) from buffer.balance_employee_list_shk_34 where type_id = 1"


# Список столбцов при переносе
COLUMNS_CH8_DM0 = """
employee_id, office_id, cnt, dt
"""

################## ЗАПРОСЫ ##################
CH8_CLEAR_OPERATIONS = """
truncate table buffer.cnt_operations_34;
"""

CH8_GET_OPERATIONS = """
insert into buffer.cnt_operations_34
(employee_id, office_id, cnt, dt, sign, _row_created)

with (select max(row_created) - interval 10 second
      from shk_storage.shk_on_place
      where 1=1
        and employee_id > 0
        and office_id > 0
        and row_created > toDateTime(%(max_dt_shk)s)) as max_rc

select
    employee_id,
    office_id,
    count(distinct shk_id) as cnt,
    toInt32(toDateTime32(toStartOfDay(dt))) as dt,
    1 as sign,
    max(row_created) as _row_created
from shk_storage.shk_on_place sop
where 1=1
    and employee_id > 0
    and office_id > 0
    and row_created > toDateTime(%(max_dt_shk)s)
    and row_created < max_rc
group by
    employee_id,
    office_id,
    dt
SETTINGS max_memory_usage = 30000000000;

insert into buffer.cnt_operations_34
(employee_id, office_id, cnt, dt, sign, _row_created)

with (select max(row_created) - interval 10 second
      from shk_storage.shk_on_place_tares
      where 1=1
        and employee_id > 0
        and office_id > 0
        and row_created > toDateTime(%(max_dt_tare)s)) as max_rc

select
    assumeNotNull(employee_id) as employee_id,
    office_id,
    count(distinct tare) as cnt,
    toInt32(toDateTime32(toStartOfDay(dt))) as dt,
    2 as sign,
    max(row_created) as _row_created
from shk_storage.shk_on_place_tares sop
where 1=1
    and employee_id > 0
    and office_id > 0
    and row_created > toDateTime(%(max_dt_tare)s)
    and row_created < max_rc
group by
    employee_id,
    office_id,
    dt
SETTINGS max_memory_usage = 30000000000;
"""

DM0_CLEAR_OPERATIONS = """
truncate table buffer.employee_cnt_operations_34;
"""

DM0_INSERT_OPERATIONS = """
insert into datamart.employee_cnt_operations_34
(employee_id, office_id, cnt, dt)
select employee_id, office_id, cnt, dt
from buffer.employee_cnt_operations_34;
"""

DM0_OPTIMIZE_OPERATIONS = """
optimize table datamart.employee_cnt_operations_34 partition {partition_key} final;
"""

CH4_QUERY_TRUNC_ES = """
truncate table buffer.balance_employee_list_shk_34;
truncate table buffer.balance_employee_shk_34;
truncate table buffer.balance_employee_shk_history_all;
"""

CH8_QUERY_TRUNC_ES = """
truncate table buffer.balance_employee_list_shk_34;
truncate table buffer.balance_employee_shk_history_all;
"""

LAKE_GET_SHK = """
select shk_id, type_id, row_created as _row_created
from (
    select shk_id, 0 as type_id, row_created
    from shk_storage.inventory_lost_shk_v2_rc_d
    where row_created > parseDateTimeBestEffort('{max_dt_lost}')

    union all

    select shk_id, 1 as type_id, row_created
    from shk_storage.inventory_lost_shk_found_v2_rc_d
    where row_created > parseDateTimeBestEffort('{max_dt_post}')
    )
FORMAT MsgPack
"""

CH8_GET_SHK_HISTORY = """
--2. Инсертим все новые связки ШК-Имплой из новой пачки списанных ШК.
insert into buffer.balance_employee_shk_history_all
(shk_id, employee_id, tare, tare_type, dt, row_created)
select
    shk_id,
    employee_id,
    if(sop.tare_type IN ('CON', 'TBX', 'BOX'), tare, 0) AS tare,
    if(sop.tare_type IN ('CON', 'TBX', 'BOX'), sop.tare_type, 'none') AS tare_type,
    dt,
    row_created
from shk_storage.shk_on_place sop
where 1=1
    and shk_id in (select shk_id from buffer.balance_employee_list_shk_34)
    and employee_id not in (0, 1)
    and employee_id is not null
    and state_id is not null
    and state_id not in ('WOG', '')
"""

CH4_QUERY_GET_ES = """
--3. Определяем дату следующего статуса (ШК), отсеиваем записи без тары.
drop table if exists shk_history;
create temporary table shk_history
(
    shk_id      Int64,
    employee_id Int32,
    tare        Int64,
    tare_type   String,
    dt          DateTime,
    dt_next     DateTime
)
engine = MergeTree
order by tare;

insert into shk_history
(shk_id, employee_id, tare, tare_type, dt, dt_next)
select
    shk_id, employee_id, tare, tare_type, dt, dt_next
from (
    select
        shk_id, employee_id, tare, tare_type, dt,
        if(dt_next_prc < '2000-01-01', parseDateTime32BestEffort('2100-01-01'), dt_next_prc) as dt_next,
        any(dt) over (partition by shk_id order by dt, row_created rows between 1 following and 1 following) AS dt_next_prc
    from buffer.balance_employee_shk_history_all
    )
where tare > 0
SETTINGS max_memory_usage = 30000000000;

--4. Инсертим ШК для которых НЕТ dt_next.
drop table if exists shk_history_wo_dt_next;
create temporary table shk_history_wo_dt_next
(
    shk_id      Int64,
    employee_id Int32,
    tare        Int64,
    tare_type   String,
    dt          DateTime
)
engine = MergeTree
order by tare;

insert into shk_history_wo_dt_next
(shk_id, employee_id, tare, tare_type, dt)
select shk_id, employee_id, tare, tare_type, dt
from shk_history
where dt_next = '2100-01-01'
    and tare_type in ('CON', 'TBX')
SETTINGS max_memory_usage = 30000000000;

--5. Инсертим ШК для которых ЕСТЬ dt_next.
drop table if exists shk_history_dt_next;
create temporary table shk_history_dt_next
(
    shk_id      Int64,
    employee_id Int32,
    tare        Int64,
    tare_type   String,
    dt          DateTime,
    dt_next     DateTime
)
engine = MergeTree
order by tare;

insert into shk_history_dt_next
(shk_id, employee_id, tare, tare_type, dt, dt_next)
select shk_id, employee_id, tare, tare_type, dt, dt_next
from shk_history
where dt_next <> '2100-01-01'
    and tare_type in ('CON', 'TBX')
SETTINGS max_memory_usage = 30000000000;

--6. Вытаскиваем уникальные тары
drop table if exists list_tares;
create temporary table list_tares
(
    tare Int64
)
engine = MergeTree
order by tare;

insert into list_tares
(tare)
select
    tare
from shk_history
where tare_type in ('CON', 'TBX')
group by tare
SETTINGS max_memory_usage = 30000000000,
         max_bytes_before_external_group_by = 10000000000,
         optimize_aggregation_in_order = 1;

--7. Вытаскиваем все мувинги тар
drop table if exists tares_history_all;
create temporary table tares_history_all
(
    tare        Int64,
    tare_type LowCardinality(String),
    dt          DateTime,
    employee_id Int32,
    lifecycle   String
)
engine = MergeTree
order by (tare, tare_type, lifecycle);

insert into tares_history_all
(tare, tare_type, dt, employee_id, lifecycle)
select
    box.tare,
    box.tare_type,
    box.dt,
    box.employee_id,
    coalesce(box.lifecycle, 'none') as lifecycle
from shk_storage.shk_on_place_tares box
where 1=1
    and tare in (select tare from list_tares)
SETTINGS max_memory_usage = 30000000000;

/* 8. Определяем следующие даты тар дял ШК у которых нет dt_next. вставляем только те у которых ближайший следующий
статус тары не позднее 7 дней, чем дата карайнего статуса ШК. */
insert into shk_history_dt_next
(shk_id, employee_id, tare, tare_type, dt, dt_next)
select shk_id, employee_id, tare, tare_type, dt, dt_next
from (
    select sop.shk_id, sop.employee_id, sop.tare, sop.tare_type, sop.dt, sopt.dt as dt_next,
    dateDiff(hour, toDateTime(sop.dt), toDateTime(sopt.dt)) as diff
    from shk_history_wo_dt_next sop
    left asof join
        (select tare, tare_type, dt
         from tares_history_all
         where tare in (select tare from shk_history_wo_dt_next)
         ) sopt on sopt.tare = sop.tare
               and sopt.tare_type = sop.tare_type
               and sopt.dt > sop.dt
    )
where diff between 0 and 169
SETTINGS max_memory_usage = 30000000000;

--9. Отбираем имплоев по мувингам тар
insert into buffer.balance_employee_shk_34
(shk_id, employee_id)
select
    shk.shk_id,
    box.employee_id
from tares_history_all box
inner join shk_history_dt_next shk using (tare, tare_type)
where 1=1
    and box.dt > shk.dt
    and box.dt < shk.dt_next
    and box.employee_id > 0
SETTINGS max_memory_usage = 30000000000,
         max_bytes_in_join = 10000000000,
         join_algorithm = 'grace_hash';

--10. Отбираем имплоев по ШК на МХ
insert into buffer.balance_employee_shk_34
(shk_id, employee_id)
select
    shk_id,
    employee_id
from buffer.balance_employee_shk_history_all
SETTINGS max_memory_usage = 30000000000;

--11. Дедуплицируем таблицу
optimize table buffer.balance_employee_shk_34 final;
"""

DM02_QUERY_LOAD_SS = """
insert into buffer.balance_employee_base_34
    (prev_operation_id, operation_dt, shk_id, amount, create_employee_id, lostreason_id, lostreason_name,
    reason_descr, contragent, type_id,  nm_desc, is_deleted, row_created)
select
    prev_operation_id, operation_dt, shk_id, amount, create_employee_id, lostreason_id, lostreason_name,
    reason_descr, contragent, type_id,  nm_desc, is_deleted, row_created
from (
    select
        leadInFrame(cast(operation_id as String)) over (PARTITION BY shk_id ORDER BY operation_dt desc, _row_created desc, type_id rows between unbounded preceding and unbounded following) as prev_operation_id,
        operation_dt, shk_id, nm_desc, amount,
        create_employee_id, lostreason_id,
        dictGet('dict.lostreason', 'lostreason_name', lostreason_id) as lostreason_name, reason_descr, type_id,
        contragent,
        if(type_id = 7, 1, 0) as is_deleted, row_created,
        row_number() over (partition by shk_id order by operation_dt desc, _row_created desc, type_id) as rn
    from datamart.inventory_lost_shk_all_short
    where row_created > toDateTime(%(cutoff)s)
    ) q
where q.rn = 1
"""

DM0_QUERY_RECALC_MAIN_SS = """
--0. Чистим буферку.
truncate table buffer.shortage_update;

-- 1. Грузим в буферку список ШК подлежащих обновлению.
insert into buffer.shortage_update
(shk_id, part_num)
select bs.shk_id, 'shk_for_upd' as part_num
from datamart.balance_employee_base_34 bs
inner join buffer.balance_employee_base_34 bf using shk_id
where bf.operation_dt > bs.operation_dt
    and bs.shk_id in (select shk_id from buffer.balance_employee_base_34);

-- 2.1. Грузим в буферку список ШК для удаления в dtm. (Отчет СС 5.34, 48).
insert into buffer.shortage_update
(shk_id, employee_id, operation_dt, is_deleted, part_num)
select shk_id, employee_id, operation_dt, 1 as is_deleted, 'shk_for_del' as part_num
from datamart.balance_employee_dtm_34
where shk_id in (select shk_id from buffer.shortage_update where part_num = 'shk_for_upd');

-- 2.2. Помечаем на удаление все записи в витрине, содержащие ШК из новой пачки данных. (Отчет СС 5.34).
insert into datamart.balance_employee_dtm_34
(shk_id, employee_id, operation_dt, is_deleted)
select shk_id, employee_id, operation_dt, is_deleted
from buffer.shortage_update where part_num = 'shk_for_del';

-- 2.3. Помечаем на удаление все записи в витрине, содержащие ШК из новой пачки данных. (Отчет СС 5.43).
insert into datamart.shortage_open_dtm_43
(shk_id, responsible_id, responsible_type, operation_dt, is_deleted)
select shk_id, responsible_id, responsible_type, operation_dt, 1 as is_deleted
from datamart.shortage_open_dtm_43
where shk_id in (select shk_id from buffer.shortage_update where part_num = 'shk_for_upd');

-- 3. Аттачим новую пачку данных в базовую таблицу.
alter table datamart.balance_employee_base_34 ATTACH partition tuple() from buffer.balance_employee_base_34;

-- 4. Пересчитываем базовую таблицу, схлапываем закрытые списания.
optimize table datamart.balance_employee_base_34 final;

-- 5. Грузим в буферку пачку данных для вставки в dtm 
insert into buffer.shortage_update
(prev_operation_id, operation_dt, shk_id, create_employee_id, lostreason_id, reason_descr, contragent,
 nm_desc, part_num)
select
    prev_operation_id, operation_dt, shk_id, create_employee_id, lostreason_id, reason_descr, contragent,
    nm_desc, 'shk_for_parsing' as part_num
from datamart.balance_employee_base_34
where shk_id in (select shk_id from buffer.balance_employee_base_34)
    and is_deleted = 0;

-- 6. Вставляем новую пачку списаний в буферку. Парсим поле contragent. (Отчет СС 5.34, 48).
insert into buffer.shortage_update
(operation_dt, shk_id, amount, create_employee_id, lostreason_id, reason_descr,
 employee_id, nm_desc, is_wrong_lost, is_wrong_transfer, is_collective_resp, is_deleted, part_num)
select
    operation_dt,
    shk_id,
    if(amount_v1 > 0, amount_v1, amount_v2) as amount,
    create_employee_id,
    lostreason_id,
    reason_descr,
    if(employee_id_v1 > 0, employee_id_v1, employee_id_v2) as employee_id,
    nm_desc,
    multiIf(is_collective_resp = 'Да', '-',
            es.shk_id = 0, 'Да', 'Нет') as is_wrong_lost,
    multiIf(is_collective_resp = 'Да', '-',
            es.shk_id = 0 and prev_operation_id = '00000000-0000-0000-0000-000000000000', 'Да', 'Нет') as is_wrong_transfer, 
    if(count() over (partition by shk_id) > 1, 'Да', 'Нет') as is_collective_resp,
    0 as is_deleted,
    'shk_for_insert' as part_num
from (
    select
        prev_operation_id, operation_dt, shk_id, create_employee_id, lostreason_id, reason_descr,
        JSONExtract(arrayJoin(JSONExtractArrayRaw(contragent)) AS unpivoted, 'employee_id', 'Int32') as employee_id_v1,
        JSONExtract(unpivoted, 'amount', 'decimal(10,2)') as amount_v1,
        if(JSONExtract(unpivoted, 'contragent_code', 'char(3)') = 'EMP',
           JSONExtract(unpivoted, 'contragent_id', 'Int32'), 0) as employee_id_v2,
           JSONExtract(unpivoted, 'retention_amount', 'decimal(10,2)') as amount_v2,
        nm_desc
    from (
        select
            prev_operation_id, operation_dt, shk_id, create_employee_id, lostreason_id, reason_descr, contragent,
            nm_desc
        from buffer.shortage_update
        where part_num = 'shk_for_parsing'
        )
    ) q
left join
    (select
        employee_id,
        shk_id
    from datamart.balance_employee_shk_34
    where shk_id in (select shk_id from buffer.balance_employee_base_34)
    ) es on (es.employee_id = q.employee_id_v1 and es.shk_id = q.shk_id) or (es.employee_id = q.employee_id_v2 and es.shk_id = q.shk_id)
where employee_id_v1 > 0 or employee_id_v2 > 0;

-- 7. Вставляем новую пачку списаний в буферку. Парсим поле contragent. (Отчет СС 5.34, 48).
insert into datamart.balance_employee_dtm_34
(operation_dt, shk_id, amount, create_employee_id, lostreason_id, reason_descr,
 employee_id, nm_desc, is_wrong_lost, is_wrong_transfer, is_collective_resp, is_deleted)
select operation_dt, shk_id, amount, create_employee_id, lostreason_id, reason_descr,
       employee_id, nm_desc, is_wrong_lost, is_wrong_transfer, is_collective_resp, is_deleted
from buffer.shortage_update
where part_num = 'shk_for_insert';

-- 8. Вставляем новую пачку списаний в витрину. Парсим поле contragent. (Отчет СС 5.43).
insert into datamart.shortage_open_dtm_43
(operation_dt, shk_id, amount, create_employee_id, lostreason_id, reason_descr,
 responsible_id, responsible_type, nm_desc, is_deleted)
select
    operation_dt,
    shk_id,
    if(amount_v2 > 0, amount_v2, amount_v1) as amount,
    create_employee_id,
    lostreason_id,
    reason_descr,
    multiIf(office_id_v2 > 0, office_id_v2,
            office_id_v1 > 0, office_id_v1,
            physical_person_id_v2 > 0, physical_person_id_v2, physical_person_id_v1) as responsible_id,
    multiIf(office_id_v2 > 0, 2,
            office_id_v1 > 0, 2,
            physical_person_id_v2 > 0, 1, 1) as responsible_type,
    nm_desc,
    0 as is_deleted
from (
    select
        operation_dt,
        shk_id,
        create_employee_id,
        lostreason_id,
        reason_descr,
        JSONExtract(arrayJoin(JSONExtractArrayRaw(contragent)) AS unpivoted, 'physical_person_id', 'Int32') as physical_person_id_v1,
        JSONExtract(unpivoted, 'office_id', 'Int32') as office_id_v1,
        JSONExtract(unpivoted, 'amount', 'decimal(10,2)') as amount_v1,
        if(JSONExtract(unpivoted, 'contragent_code', 'char(3)') = 'PER',
        JSONExtract(unpivoted, 'contragent_id', 'Int32'), 0) as physical_person_id_v2,
        if(JSONExtract(unpivoted, 'contragent_code', 'char(3)') = 'OFW',
        JSONExtract(unpivoted, 'contragent_id', 'Int32'), 0) as office_id_v2,
        JSONExtract(unpivoted, 'retention_amount', 'decimal(10,2)') as amount_v2,
        nm_desc
    from (
        select
            operation_dt, shk_id, create_employee_id, lostreason_id, reason_descr, contragent, nm_desc
        from buffer.shortage_update
        where part_num = 'shk_for_parsing'
        )
    ) q
where physical_person_id_v1 > 0
   or physical_person_id_v2 > 0
   or office_id_v1 > 0
   or office_id_v2 > 0;
"""

DM0_GET_SS = """
-- забираем списания DOAPP-44
select operation_dt, shk_id, amount, lostreason_id, employee_id, is_deleted, part_num
from buffer.shortage_update
where part_num in ('shk_for_del', 'shk_for_insert')
FORMAT MsgPack
"""

CH7_QUERY_RECALC_MAIN_SS = """
-- 1. Помечаем на удаление все записи в dtm, содержащие ШК из новой пачки данных. (Отчет СС 48).
insert into datamart.shortage_open_48
(shk_id, employee_id, operation_dt, is_deleted)
select shk_id, employee_id, operation_dt, is_deleted
from buffer.shortage_update_48 where part_num = 'shk_for_del';

-- 2. Вставляем новые списания в dtm. (Отчет СС 48).
insert into datamart.shortage_open_48
(operation_dt, shk_id, amount, lostreason_id,  employee_id, is_deleted)
select operation_dt, shk_id, amount, lostreason_id,  employee_id, is_deleted
from buffer.shortage_update_48 where part_num = 'shk_for_insert';
"""

DM0_QUERY_OPTIMIZE_SS_34 = """
    optimize table datamart.balance_employee_dtm_34 partition {partition_key} final;
"""

DM0_QUERY_OPTIMIZE_SS_43 = """
    optimize table datamart.shortage_open_dtm_43 partition {partition_key} final;
"""

CH7_QUERY_OPTIMIZE_SS_48 = """
    optimize table datamart.shortage_open_48 partition {partition_key} final;
"""

DM0_QUERY_RECALC_SECONDARY = """
--1. Вспомогательная витрина офис-сотрудник
insert into datamart.employee_office_relation_34
(employee_id, office_id, type_point_name, dt, cnt)
select
    employee_id,
    office_id,
    dictGet('dict.branch_office', 'type_point_name', office_id) as type_point_name,
    dt,
    cnt
from (
    select
        employee_id,
        office_id,
        dt,
        cnt,
        row_number() over (partition by (employee_id, dt) order by cnt desc) as rn
    from datamart.employee_cnt_operations_34
    where dt < 1735678800
    ) q
where rn = 1;

insert into datamart.employee_office_relation_34
(employee_id, office_id, type_point_name, dt, cnt)
select
    employee_id,
    office_id,
    dictGet('dict.branch_office', 'type_point_name', office_id) as type_point_name,
    dt,
    cnt
from (
    select
        employee_id,
        office_id,
        dt,
        cnt,
        row_number() over (partition by (employee_id, dt) order by cnt desc) as rn
    from datamart.employee_cnt_operations_34
    where dt >= 1735678800
    ) q
where rn = 1;


--2. Вспомогательная витрина с последней датой операции
insert into datamart.balance_employee_lt_34
(employee_id, last_dt)
select
    employee_id,
    toString(toDate32((max(dt)))) as last_dt
from datamart.employee_cnt_operations_34
group by employee_id;

--3. Вспомогательная витрина с историей операций по месяцам
insert into datamart.balance_employee_opr_34
(employee_id, office_id, cnt_0, cnt_1, cnt_2, cnt_3, cnt_4, cnt_5, cnt_all)
select
    employee_id,
    office_id,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now())) as cnt_0,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now() - toIntervalMonth(1))) as cnt_1,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now() - toIntervalMonth(2))) as cnt_2,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now() - toIntervalMonth(3))) as cnt_3,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now() - toIntervalMonth(4))) as cnt_4,
    sumIf(cnt, toStartOfMonth(toDateTime32(dt)) = toStartOfMonth(now() - toIntervalMonth(5))) as cnt_5,
    sum(cnt) as cnt_all
from datamart.employee_cnt_operations_34
group by employee_id, office_id;

--4. Вспомогательная витрина с итоговым балансом
drop table if exists tmp_balance_employee_all_34;
create temporary table tmp_balance_employee_all_34
engine = MergeTree
order by employee_id
partition by tuple()
as
select
   employee_id,
   is_collective_resp,
   sum(amount) as amount
from datamart.balance_employee_dtm_34
where is_deleted = 0
group by employee_id, is_collective_resp;

alter table datamart.balance_employee_all_34 replace partition tuple() from tmp_balance_employee_all_34;

--5. Вспомогательная витрина для департаменов
insert into datamart.balance_employee_dep_34
(department_name)
select distinct
    dictGet('dict.human_resource_department', 'department_name', dictGet('dict.human_resource_employee', 'department_id', employee_id)) as department_name
from datamart.balance_employee_dtm_34;

--6. Обновляем витрину с удержаниями
insert into datamart.balance_employee_writeoff_34
(operation_id, operation_dt, writeoff_amount, writeoff_type, employee_id)
select 
    operation_id, operation_dt, writeoff_amount, writeoff_type, employee_id
from buffer.balance_employee_writeoff_34
where employee_id in (select employee_id from datamart.balance_employee_all_34);

--7. Вспомогательная витрина с итоговым балансом в разрезе типов офисов
drop table if exists tmp_balance_employee_tpn_34;
create temporary table tmp_balance_employee_tpn_34
engine = MergeTree
order by type_point_name
partition by tuple()
as
select
    if(type_point_name = '', '-', type_point_name) as type_point_name,
    sum(amount) as amount
from (
    select
        tpn.type_point_name,
        amnt.amount
    from (
        select employee_id,
               sum(amount) as amount
        from datamart.balance_employee_all_34
        group by employee_id
        ) amnt
    left join (select
                    employee_id,
                    type_point_name,
                    sum(cnt) as sum_cnt
               from datamart.employee_office_relation_34
               group by employee_id,
                          type_point_name
               order by sum_cnt desc, type_point_name
               limit 1 by employee_id) tpn using (employee_id)
    )
group by type_point_name;

alter table datamart.balance_employee_tpn_34 replace partition tuple() from tmp_balance_employee_tpn_34;
"""

DM0_QUERY_DELETE_SS = """
alter table datamart.balance_employee_base_34 delete where is_deleted = 1;
alter table datamart.balance_employee_dtm_34 delete where is_deleted = 1;
alter table datamart.shortage_open_dtm_43 delete where is_deleted = 1;

optimize table datamart.employee_office_relation_34 final;
optimize table datamart.balance_employee_lt_34 final;
optimize table datamart.balance_employee_opr_34 final;
optimize table datamart.balance_employee_all_34 final;
optimize table datamart.balance_employee_dep_34 final;
"""

CH7_QUERY_DELETE_SS = """
alter table datamart.shortage_open_48 delete where is_deleted = 1;
"""

CH_QUERY_DELETE_ES = """
alter table datamart.balance_employee_shk_34 delete where shk_id not in (select shk_id from datamart.balance_employee_base_34);
optimize table datamart.balance_employee_shk_34 final;
"""


CH4_EMP_SAL_SCRIPT ="""
select
transaction_uid as operation_id,
toDateTime(operation_dt),
toFloat64(reward_amount_rub * (-1) as writeoff_amount), /*отрицательное если удержание, положительное если возмещение, поэтому множим на -1*/
'Удержание ПВЗ' as writeoff_type,
employee_id
from protected.employee_salary_payments poo
where 1=1
and reward_amount_rub <> 0
and _row_created > toDateTime('{date}')
order by operation_dt desc
limit 1 by operation_id
Format MsgPack;
"""

FILEDS_EMP_SAL_PAY = 'operation_id,operation_dt,writeoff_amount,writeoff_type,employee_id'


################## ФУНКЦИИ ##################
@provide_session
def check_run(session):
    # проверяем является ли запуск дага первым за сутки.
    run_counter = (session.query(func.count(DagRun.dag_id)).filter(DagRun.dag_id == 'ch4_ch8_to_gp_balance_employee_34',
                                                                   DagRun.start_date > datetime(
                                                                       *date.today().timetuple()[:3], 0, 0,
                                                                       tzinfo=timezone('Europe/Moscow'))).first())
    log.info(f"Запуск за день: {run_counter[0]}")

    if run_counter[0] == 1:
        log.info("Первый запуск за день.")
        return "task_first_run"
    else:
        log.info("Запуск не первый.")
        return "task_not_first_run"


@with_db(DM02_CONN_ID, 'dm0')
@with_db(CH8_02_CONN_ID, 'ch8')
def ch8_to_dm0_co(ch8_hook, dm0_hook, dm0_curs):
    """
    Функция определяет количество операций сотрудника по дням совершенных им в тех или иных офисах.
    Анализируются операции по ШК и Тарам.
    В дальнейшем эти данные используются для косвенного определения принадлежности сотрудника к офису,
    а так же для подсчета общего числа операций за период в отчете СС.5.34.
    Источник - shk_storage.shk_on_place (CH4).
    """
    log.info('----- ЭТАП I. ЗАГРУЗКА ОПЕРАЦИЙ (CH8 -> DM0) -----')

    log.info('Получаем отсечки для операций сотрудников с ШК')
    max_dt_shk = get_cutoff(db_cursor=dm0_curs, table_name='cutoff_cnt_shk_operations_34', field='max_dt',
                            def_cutoff_query="select toDateTime('2024-08-28 10:27:59')")
    log.info(f"Получена отсечка для ШК: {max_dt_shk}")

    log.info('Получаем отсечки для операций сотрудников с ТАРАМИ')
    max_dt_tare = get_cutoff(db_cursor=dm0_curs, table_name='cutoff_cnt_tare_operations_34', field='max_dt',
                             def_cutoff_query="select toDateTime('2024-08-27 00:00:00')")
    log.info(f"Получена отсечка для ТАР: {max_dt_tare}")

    log.info('CH8: Загружаем данные по операциям в буферку.')
    ch8_hook.exec_with_log(CH8_CLEAR_OPERATIONS)
    ch8_hook.exec_with_log(CH8_GET_OPERATIONS, parameters={'max_dt_shk': max_dt_shk, 'max_dt_tare': max_dt_tare})
    log.info('Done!')

    log.info('CH8->DM02: Переношу данные по операциям сотрудников.')
    dm0_hook.exec_with_log(DM0_CLEAR_OPERATIONS)
    copy_ch_to_ch_pipe(
        take_data='select employee_id, office_id, cnt, dt from buffer.cnt_operations_34 FORMAT MsgPack',
        insert_data='insert into buffer.employee_cnt_operations_34 (employee_id, office_id, cnt, dt) FORMAT MsgPack',
        src_ch=CH8_02_CONN_ID,
        dst_ch=DM02_CONN_ID,
        throw_if_empty=False)
    log.info('DM02: buffer.employee_cnt_operations_34 filled')

    log.info('Определяем список партиций для обновления витрин отчетов 5.34 и 5.43.')
    sql_get_partitions_co = """
        select array_agg(prt)
        from (select distinct toYYYYMM(toDateTime32(dt)) as prt
              from buffer.employee_cnt_operations_34)
        """
    partitions_list_co = dm0_hook.fetchone(sql_get_partitions_co)
    log.info(f'Получены партиции по операциям сотрудников.: {partitions_list_co}')

    log.info('Инсертим новую пачку и попартиционно делаем оптимайз витрины c операциями сотрудников')
    dm0_hook.exec_with_log(DM0_INSERT_OPERATIONS)

    for partition in partitions_list_co:
        log.info(f'partition_key: {partition}')
        dm0_hook.exec_with_log(DM0_OPTIMIZE_OPERATIONS.format(partition_key=partition))
        log.info('Done!')

    log.info('Витрина обновлена, оптимайз выполнен!')

    log.info('Сохраняем отсечки!')
    new_max_dt_shk = ch8_hook.fetchone('select max(_row_created) from buffer.cnt_operations_34 where sign = 1')
    if str(new_max_dt_shk) != '1970-01-01 03:00:00':
        save_cutoff(db_cursor=dm0_curs, table_name='cutoff_cnt_shk_operations_34', field='max_dt', value=new_max_dt_shk)

    new_max_dt_tare = ch8_hook.fetchone('select max(_row_created) from buffer.cnt_operations_34 where sign = 2')
    if str(new_max_dt_tare) != '1970-01-01 03:00:00':
        save_cutoff(db_cursor=dm0_curs, table_name='cutoff_cnt_tare_operations_34', field='max_dt', value=new_max_dt_tare)
    log.info('Done!')


def ch4_ch8_recalc_es():
    """
    Функция определяет к каким ШК и в какие дни прикасался сотрудник.
    Данная информация требуется для выявления необоснованных списаний и переносов на сотрудника.
    См соответствующие поля в отчете СС.5.34.
    """
    log.info('')
    log.info('----- ЭТАП II. РАСЧЕТ ИМПЛОЙ-ШК (CH4) -----')
    ch_hook_dm = ClickhouseHook(DM02_CONN_ID)
    ch_hook_ch4 = ClickhouseHook(CH4_DP_CONN_ID)
    ch_hook_ch8 = ClickhouseHook(CH8_CONN_ID)
    with ch_hook_dm.get_conn() as conn:
        dm0_cur = conn.cursor()

        log.info('Получаем отсечки для загрузки списанных и оприходованных ШК')
        max_dt_lost = get_cutoff(db_cursor=dm0_cur, table_name=CUTOFF_ES_LOST, field='max_dt',
                                 def_cutoff_query="select toDateTime('2024-04-16')")
        max_dt_post = get_cutoff(db_cursor=dm0_cur, table_name=CUTOFF_ES_POST, field='max_dt',
                                 def_cutoff_query="select toDateTime('2024-04-16')")
        log.info('Отсечки получены!')

        log.info('')
        log.info('Чистим буферные таблицы на CH4 и CH8')
        ch_hook_ch4.exec_with_log(CH4_QUERY_TRUNC_ES, autocommit=True)
        ch_hook_ch8.exec_with_log(CH8_QUERY_TRUNC_ES, autocommit=True)
        log.info('Done!')

        log.info('льем список ШК для анализа на ч8')
        copy_ch_to_ch_pipe(
            take_data=LAKE_GET_SHK.format(max_dt_lost=max_dt_lost, max_dt_post=max_dt_post),
            insert_data="""insert into buffer.balance_employee_list_shk_34 (shk_id, type_id, _row_created) FORMAT MsgPack""",
            src_ch=LAKE_M_CONN,
            dst_ch=CH8_CONN_ID)
        log.info('Done!')

        log.info('запускаем поиск истории на ч8 и переливаем на ч4')
        ch_hook_ch8.exec_with_log(CH8_GET_SHK_HISTORY)
        copy_src = "select shk_id, employee_id, tare, tare_type, dt, row_created from buffer.balance_employee_shk_history_all format MsgPack"
        insert_dst = "insert into buffer.balance_employee_shk_history_all (shk_id, employee_id, tare, tare_type, dt, row_created) FORMAT MsgPack"
        copy_ch_to_ch_pipe(take_data=copy_src,
                           insert_data=insert_dst,
                           src_ch=CH8_CONN_ID,
                           dst_ch=CH4_DP_CONN_ID)
        log.info('Done!')

        log.info('Запускаем подбор новых записей ИМПЛОЙ-ШК.')
        ch_hook_ch4.exec_with_log(CH4_QUERY_GET_ES)
        log.info('Done!')

        log.info('Сохраняем отсечки.')
        new_max_dt_lost = ch_hook_ch4.fetchone(MAX_DT_CH4_LOST)
        if str(new_max_dt_lost) != '1970-01-01 03:00:00':
            save_cutoff(db_cursor=dm0_cur, table_name=CUTOFF_ES_LOST, field='max_dt', value=new_max_dt_lost)

        new_max_dt_post = ch_hook_ch4.fetchone(MAX_DT_CH4_POST)
        if str(new_max_dt_post) != '1970-01-01 03:00:00':
            save_cutoff(db_cursor=dm0_cur, table_name=CUTOFF_ES_POST, field='max_dt', value=new_max_dt_post)
        log.info('Done!')

    log.info('')
    log.info('Переносим новые записи ИМПЛОЙ-ШК на DM02')

    copy_src = "select employee_id, shk_id from buffer.balance_employee_shk_34 format MsgPack"
    insert_dst = "insert into datamart.balance_employee_shk_34 (employee_id, shk_id) FORMAT MsgPack"

    copy_ch_to_ch_pipe(take_data=copy_src,
                       insert_data=insert_dst,
                       src_ch=CH4_DP_CONN_ID,
                       dst_ch=DM02_CONN_ID)

    log.info('Данные успешно загружены на!')

    log.info('Запускаем оптимайз таблицы с историей ИМПЛОЙ-ШК')
    ch_hook_es = ClickhouseHook(DM02_CONN_ID)
    ch_hook_es.exec_with_log('optimize table datamart.balance_employee_shk_34 final')
    log.info('Done!')


@with_db(DM02_CONN_ID, 'dm02')
@load_and_save_cutoff('balance_employee_base_34', 'dm02', 'max_dt')
def dm02_recalc_ss(dm02_curs, dm02_hook, cutoff):
    """
    Функция определяет новые документы списаний и оприходований по сотрудникам (другие типы ответственных не учитываются).
    Эти данные в дальнейшем льются на DM02, где реплейсингом остаются только открытые списания.
    """
    log.info('')
    log.info('----- ЭТАП III. РАСЧЕТ СПИСАНИЙ -----')

    log.info("Чистим буферку со СПИСАНИЯМИ на DMO2")
    dm02_hook.exec_with_log('truncate table buffer.balance_employee_base_34')
    log.info("Done!")

    log.info("Льем новые документы в буферку со СПИСАНИЯМИ на DMO2")
    dm02_hook.exec_with_log(DM02_QUERY_LOAD_SS, parameters={'cutoff': cutoff})
    log.info("Done!")

    dm02_curs.execute('select max(row_created) from buffer.balance_employee_base_34')
    new_cutoff = dm02_curs.fetchone()

    if new_cutoff[0] != datetime(1970, 1, 1, 3, 0):
        return new_cutoff


@with_db(CH4_PRIVATE_CONN_ID, 'ch4')
def ch4_recalc_wo(ch4_curs):
    """
    Функция грузит данные по сумме удержаний из ЗП сотрудников на DM02.
    Источники - Склад и ПВЗ (Чайковский).
    """
    log.info(' ')
    ch4_curs.execute("select max(_row_created) from protected.employee_salary_payments")
    next_cutoff = ch4_curs.fetchone()[0]
    #текущая максимальная дата
    max_dt = get_cutoff(ch4_curs,'protected.employee_salary_payments','max_dt')
    if next_cutoff!=max_dt:
        #Берем данные
        copy_ch_to_ch_pipe(
            take_data=CH4_EMP_SAL_SCRIPT.format(date=max_dt),
            insert_data=f"""INSERT INTO buffer.balance_employee_writeoff_34 ({FILEDS_EMP_SAL_PAY}) FORMAT MsgPack""",
            src_ch=CH4_PRIVATE_CONN_ID,
            dst_ch=DM02_CONN_ID,
        )
        #Сохраняем новую макс дату
        save_cutoff(ch4_curs, 'protected.employee_salary_payments', next_cutoff, field='max_dt')


@with_db(DM02_CONN_ID, 'dm0')
@with_db(CH7_02_CONN_ID, 'ch7')
def dm0_recalc_ss(dm0_hook, ch7_hook):
    """
    Функция пересчитывает основные и вспомогательные витрины для отчета СС.5.34 на DMO2.
    """

    log.info(' ')
    log.info('----- ЭТАП V. ПЕРЕСЧЕТ ВИТРИН (DM0) -----')

    log.info('Определяем список партиций для обновления витрин отчетов 5.34 и 5.43.')

    sql_get_partitions_34 = """
        select array_agg(prt)
        from (select distinct toYYYYMM(operation_dt) as prt
              from datamart.balance_employee_dtm_34
              where shk_id in (select bs.shk_id
                               from datamart.balance_employee_base_34 bs
                               inner join buffer.balance_employee_base_34 bf using shk_id
                               where bf.operation_dt > bs.operation_dt)
              order by prt)
        """
    partitions_list_34 = dm0_hook.fetchone(sql_get_partitions_34)
    log.info(f'Получены партиции. (Отчет СС 5.34).: {partitions_list_34}')

    sql_get_partitions_43 = """
        select array_agg(prt)
        from (select distinct toYYYYMM(operation_dt) as prt
              from datamart.shortage_open_dtm_43
              where shk_id in (select bs.shk_id
                               from datamart.balance_employee_base_34 bs
                               inner join buffer.balance_employee_base_34 bf using shk_id
                               where bf.operation_dt > bs.operation_dt)
              order by prt)
        """
    partitions_list_43 = dm0_hook.fetchone(sql_get_partitions_43)
    log.info(f'Получены партиции. (Отчет СС 5.43).: {partitions_list_43}')

    log.info('Запускаем пересчет ОСНОВНЫХ таблицы на клике.')
    dm0_hook.exec_with_log(DM0_QUERY_RECALC_MAIN_SS)
    log.info('Done!')

    log.info('Попартиционно делаем оптимайз для витрины. (Отчет СС 5.34).')
    for partition in partitions_list_34:
        log.info(f'partition_key: {partition}')
        dm0_hook.exec_with_log(DM0_QUERY_OPTIMIZE_SS_34.format(partition_key=partition))
        log.info('Done!')

    log.info('Попартиционно делаем оптимайз для витрины. (Отчет СС 5.43).')
    for partition in partitions_list_43:
        log.info(f'partition_key: {partition}')
        dm0_hook.exec_with_log(DM0_QUERY_OPTIMIZE_SS_43.format(partition_key=partition))
        log.info('Done!')

    log.info('Оптимайз выполнен!')

    log.info('Заполняем ВСПОМОГАТЕЛЬНЫЕ витрины на DM0')
    dm0_hook.exec_with_log(DM0_QUERY_RECALC_SECONDARY)
    log.info('Done!')

    log.info('Запускаем очистку помеченных к удалению строк')
    dm0_hook.exec_with_log(DM0_QUERY_DELETE_SS)
    log.info('Done!')

    # CH7: Чистим буферку списаний на CH7.
    log.info('CH7: Чистим буферку списаний на CH7.')
    ch7_hook.exec_with_log('truncate table buffer.shortage_update_48;')

    # DM0->CH7: Переношу списания на CH7
    log.info('DM0->CH7: Переношу списания на CH7')
    copy_ch_to_ch_pipe(
        take_data=DM0_GET_SS,
        insert_data='''INSERT INTO buffer.shortage_update_48 (operation_dt, shk_id, amount, lostreason_id, 
                                                              employee_id, is_deleted, part_num) FORMAT MsgPack''',
        src_ch=DM02_CONN_ID,
        dst_ch=CH7_02_CONN_ID,
        throw_if_empty=False)
    log.info('CH7: buffer.shortage_update_48 filled')

    log.info('Запускаем пересчет ОСНОВНЫХ таблицы на клике.')
    ch7_hook.exec_with_log(CH7_QUERY_RECALC_MAIN_SS)
    log.info('Done!')

    # протестировать partitions_list_34, активен ли после первого обращения
    log.info('Попартиционно делаем оптимайз для dtm. (Отчет СС 48).')
    for partition in partitions_list_34:
        log.info(f'partition_key: {partition}')
        ch7_hook.exec_with_log(CH7_QUERY_OPTIMIZE_SS_48.format(partition_key=partition))
        log.info('Done!')

    log.info('Запускаем очистку помеченных к удалению строк')
    ch7_hook.exec_with_log(CH7_QUERY_DELETE_SS)
    log.info('Done!')


def dm0_delete_es():
    """
    Функция раз в день удаляет записи из таблицы ИМПЛОЙ-ШК с ШК которые более не числятся списанными.
    """
    log.info(' ')
    log.info('-----УДАЛЕНИЕ ИЗБЫТОЧНЫХ ЗАПИСЕЙ ИМПЛОЙ-ШК (DM0) -----')
    ch_hook_es = ClickhouseHook(DM02_CONN_ID)

    log.info('Запускаем очистку избыточных записей ИМПЛОЙ-ШК')
    ch_hook_es.exec_with_log(CH_QUERY_DELETE_ES)
    log.info('Done!')


default_args = {
    'owner': 'zhdanov.ivan2',
    'email': ['zhdanov.ivan2@wildberries.work'],
    'telegram': ['@Finder_84'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
        dag_id="ch4_ch8_to_gp_balance_employee_34",
        description="Даг формирует витрины для отчета. СС.5.34. DO-6062.",
        default_args=default_args,
        schedule='20 */2 * * *',  # запускаем даг раз в 2 часа
        start_date=datetime(2024, 4, 9),
        tags=[CH4_CONN_ID, CH8_CONN_ID, DM02_CONN_ID, CH7_02_CONN_ID],
        max_active_runs=1
) as dag:
    check_run = BranchPythonOperator(
        task_id="task_check_run",
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        python_callable=check_run
    )

    first_run = EmptyOperator(
        task_id="task_first_run",
        dag=dag,
        pool=get_pool(CH4_CONN_ID)
    )

    not_first_run = EmptyOperator(
        task_id="task_not_first_run",
        dag=dag,
        pool=get_pool(CH4_CONN_ID)
    )

    dm0_delete_es = PythonOperator(
        task_id="task_dm0_delete_es",
        dag=dag,
        pool=get_pool(DM02_CONN_ID),
        inlets =[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.balance_employee_shk_34")],
        python_callable=dm0_delete_es
    )

    ch8_to_dm0_co = PythonOperator(
        task_id="task_ch8_to_dm0_co",
        dag=dag,
        pool=get_pool(CH8_CONN_ID),
        python_callable=ch8_to_dm0_co,
        trigger_rule="none_failed",
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch8.shk_storage.shk_on_place")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.employee_cnt_operations_34")]

    )

    ch4_ch8_recalc_es = PythonOperator(
        task_id="task_ch4_ch8_recalc_es",
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        python_callable=ch4_ch8_recalc_es,
        trigger_rule="none_failed",
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch8.shk_storage.shk_on_place"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch8.shk_storage.shk_on_place_tares"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.stage_external.inventory_lost_shk_v2"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.stage_external.inventory_lost_shk_found_v2")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.balance_employee_shk_34")]
    )

    dm02_recalc_ss = PythonOperator(
        task_id="task_dm02_recalc_ss",
        dag=dag,
        pool=get_pool(DM02_CONN_ID),
        python_callable=dm02_recalc_ss,
        trigger_rule="none_failed",
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.inventory_lost_shk_all_short")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.buffer.balance_employee_base_34")]
    )

    ch4_recalc_wo = PythonOperator(
        task_id="task_ch4_recalc_wo",
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        python_callable=ch4_recalc_wo,
        trigger_rule="none_failed",
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.protected.employee_salary_payments")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.buffer.balance_employee_writeoff_34")]
    )

    dm0_recalc_ss = PythonOperator(
        task_id="task_dm0_recalc_ss",
        dag=dag,
        pool=get_pool(DM02_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.balance_employee_base_34"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.buffer.balance_employee_base_34"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.buffer.shortage_update"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.balance_employee_dtm_34"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.shortage_open_dtm_43"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.balance_employee_shk_34")],
        python_callable=dm0_recalc_ss,
        trigger_rule="none_failed"
    )

    check_run >> [first_run, not_first_run]
    first_run >> dm0_delete_es >> ch8_to_dm0_co >> ch4_ch8_recalc_es >> dm02_recalc_ss >> ch4_recalc_wo >> dm0_recalc_ss
    not_first_run >> ch8_to_dm0_co >> ch4_ch8_recalc_es >> dm02_recalc_ss >> ch4_recalc_wo >> dm0_recalc_ss
