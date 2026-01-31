"""
Формируем витрину с кодами imei, S/N и т.д.

Описание процесса обновления витрины:
1. SC [ch8 + ch4 -> dm0]
1.1 Чистим целевой буфер.
1.2 Первый источник [ch8] shk_storage.shk_create (SC) уходит на накопительный буфер [ch8]
1.3 По списку ШК [dm0] получаем таблицу маппинга на СРИДы из [ch4] с помощью внешней таблицы, складываем в Join-буфер на [ch8]
1.4 Возвращаем маппинг сридов на ch8
1.5 Джоинис ШК и СРИД-ы и отправляем на целевой буффер на [dm0]
1.6 Накопительный буфер чистим от пустых сридов (значит не нашлось сопоставление в datamart.shk_srid
2. OM
2.1 Второй источник [ch4] stage_nats.orders_meta (OM)
2.2 Получаем в память очищенный срез данных по отсечке
2.3 Батчим память, т.к. на 1 дату может быть 39млн записей.
2.4 Складываем пачку из памяти в буфер 1 (накопление на обработку)
2.5 Перекладываем пачку в буфер 2 переджоинив с shk через готовую таблицу маппинга datamart.srid_shk
2.6 Перебрасываем пары srid-shk из буфера 1 в буфер на [ch2] почистив от пустых shk
2.7 Джоиним буфер на [ch2] c shk_rid_price_nm_v2 для получения nm по паре srid-shk, после чего отправляем результат в буфер на [ch4]
2.8 Джоиним буфер 1 с новым буфером, дополняя его shk + nm. Складываем результат в целевой буфер [dm0]
3. DM -> CH4
3.1 Получаем srid, nm, dt из целевого буфера
3.2 На CH4 батчуемся по этим данным и джоиним с таблицей oof_position_status_v3. Дедуплицируем по ближайшей dt по каждому srid, складываем в PAY буфер
3.3 Апдейтим PAY буфер по supplier_id
3.4 Перекладываем PAY буфер обратно на DM, делая его Join таблицей
3.5 Апдейт целевого буфера этим Pay буфером
4.
4.1 Подготовка буфера витрины к отправке
4.2 Полученные SC + OM обогощаем словарями на [dm0] по группам и категориям товара
4.3 Отправляем весь целевой буфер в целевую витрину [dm0]. Делаем optimize table final
4.4 Очистка накопителя для ОМ: Все что в целевом буффере по ОМ собираем и отправляем на ch4 батчами
4.5 Заполняем Join таблицу для обновления поля is_del на накопителе, далее его очистка по этому признаку
4.6 Считаем отсечки запросами по целевому буферу. Обновляем их

# источники основных данных
shk_storage.shk_create
stage_nats.orders_meta

# Здесь получаем данные на обработку, а так же оставляем необработанные ШК
buffer.shk_create_for_imei  # ch8
buffer.orders_meta_for_imei  # ch4

# Вспомогательные таблицы
buffer.om_srid_for_imei  # CH-4
buffer.om_srid_shk_for_imei  # CH-4
buffer.om_srid_shk_nm_for_imei  # CH-4
buffer.om_srid_shk_for_imei  # CH-2
buffer.shk_srid_for_imei  # DM был теперь CH8
buffer.dm_srid_pay_for_imei  # CH-4
buffer.dm_srid_pay_for_imei  # DM
buffer.del_srids_for_imei  # CH4
"""

import logging as log
import time
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from psycopg2.extensions import cursor
from python_utils import batcher

from hooks.clickhouse_hook import ClickhouseHook
from utils.cutoff import get_cutoff, save_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe
from utils.db.clickhouse import get_ch_dwh_max_v2
from utils.decorators_with_conn import with_db
from utils.openmeta_helper import Entity

CH8_CONN_ID = "do-ch8"
CH4_CONN_ID = "do-ch4"
CH2_CONN_ID = "do-ch2-recent"
CHDM_CONN_ID = "do-ch-dm02"

BATCH_SIZE_SC = 10_000_000
BATCH_SIZE_OM = 1_000_000
BACK_SECONDS = 12 * 3600

FIELD_DT = "max_dt"
START_DATE = "2024-01-01"

# Отбираем ШК по отсечке из источника
QUERY_COLLECT_SHK_CREATE = """
insert into buffer.shk_create_for_imei
SELECT
    shk_id,
    nm_id,
    null as srid,
    ext_id as identifier_number,
    ext_type_id as identifier_type,
    dt as identifier_dt,
    t.row_created
from shk_storage.shk_create t
prewhere
    row_created BETWEEN parseDateTime64BestEffort('%(min_time)s') AND parseDateTime64BestEffort('%(max_time)s')
    and identifier_type is not null and identifier_number is not null and identifier_type not in ('WBI', 'KIZ')
"""

QUERY_COLLECT_ORDERS_META = """
insert into buffer.orders_meta_for_imei
with cte as (
    select
        srid, dt, meta_key, meta_value, row_created
        ,row_number() OVER (PARTITION BY srid ORDER BY dt desc) r
    from stage_nats.orders_meta
    prewhere row_created BETWEEN parseDateTime64BestEffort('%(min_time)s') AND parseDateTime64BestEffort('%(max_time)s')
    AND meta_key IS NOT NULL and meta_value IS NOT null
)
select srid, dt, meta_key, meta_value, false as is_del, row_created from cte
where r = 1
"""

QUERY_SELECT_SHK_CREATE = """
SELECT
    t.shk_id,
    t.nm_id,
    d.srid as srid,
    null as srid_create_dt,
    null as srid_copied,
    null as srid_copied_create_dt,
    null as subject_id,
    null as subject_name,
    null as parent_id,
    null as parent_name,
    t.identifier_number,
    t.identifier_type,
    t.identifier_dt,
    'SC' as source,
    t.row_created
from buffer.shk_create_for_imei t
left join buffer.shk_srid_for_imei d on t.shk_id = d.shk_id
where d.srid is not null
FORMAT MsgPack
"""

""" Основная таблица - ранее сформированная без альтернативных сридов.
    Альты подтягиваются в буффер финальной сборки, т.к. от них нужен только номер и дата (дата позднее)
    Здесь размножаем каждый срид имеющий альтернативу на 2 срида - самый новый (текущий)
    и его самая старая по дате альтернатива (от которого все началось)
"""
QUERY_SELECT_ORDERS_META = """
SELECT
    t.shk_id,
    n.nm_id,
	alt.srid as srid,
    null as srid_create_dt,
    case when t.srid = alt.srid then null else t.srid end as srid_copied,
    null as srid_copied_create_dt,
    null as subject_id,
    null as subject_name,
    null as parent_id,
    null as parent_name,
	t.meta_value as identifier_number,
	CASE t.meta_key
    	WHEN 'imei' THEN 'IME'
	    ELSE upperUTF8(meta_key)
	end as identifier_type,
	t.dt as identifier_dt,
    'OM' as source,
    t.row_created
from buffer.om_srid_shk_for_imei t
join buffer.om_srid_shk_nm_for_imei n on n.shk_id = t.shk_id and n.srid = t.srid
left join (
    with cte as
    (select *
      , row_number() over (partition by shk_id order by min_ts) as old
      , row_number() over (partition by shk_id order by min_ts desc ) as new
     from datamart.shk_srid
     prewhere shk_id in (select shk_id from buffer.om_srid_shk_for_imei)
    )
    select * from cte where new = 1 or old = 1
    ) alt on alt.shk_id = t.shk_id and (alt.old == 1 or alt.srid != t.srid)
FORMAT MsgPack
"""

QUERY_INSERT_TO_TARGET_BUF = """
INSERT INTO buffer.srid_shk_imei
(shk_id, nm_id, srid, srid_create_dt, srid_copied, srid_copied_create_dt, subject_id, subject_name, parent_id, parent_name, identifier_number, identifier_type, identifier_dt, source, row_created)
FORMAT MsgPack"""

QUERY_UPDATE_SHK_CREATE = """
SET mutations_sync=1;

ALTER table buffer.srid_shk_imei
update
    subject_id = dictGet('dict.product_cards_nm', 'subject_id', nm_id),
    parent_id = dictGet('dict.product_cards_nm', 'parent_id', nm_id)
where subject_id is null or parent_id is null;

ALTER table buffer.srid_shk_imei
update
    subject_name = dictGet('dict.subjects', 'subject_name', subject_id),
    parent_name = dictGet('dict.subjects', 'parent_name', subject_id)
where subject_name is null or parent_name is null;
"""

QUERY_UPDATE_DM_PAY_CH4 = """
SET mutations_sync=1;

ALTER table buffer.dm_srid_pay_for_imei
update
    supplier_id = dictGet(dict.product_cards_nm, 'supplier_id_shk', nm_id)
where supplier_id is null;
"""

QUERY_UPDATE_DM_PAY = """
SET mutations_sync=1;

ALTER table buffer.srid_shk_imei
update
    status_id = joinGet(buffer.dm_srid_pay_for_imei, 'status_id', srid),
    supplier_id = joinGet(buffer.dm_srid_pay_for_imei, 'supplier_id', srid),
    payment_type = joinGet(buffer.dm_srid_pay_for_imei, 'payment_type', srid),
    price_rub = joinGet(buffer.dm_srid_pay_for_imei, 'price_rub', srid),
    srid_create_dt = joinGet(buffer.dm_srid_pay_for_imei, 'dt', srid)
where 1=1;
"""

QUERY_COLLECT_SHK = """
select
    shk_id,
    srid,
    argMax(min_ts , ver) create_dt
from datamart.shk_srid
where
    shk_id in (select shk_id from ext)
GROUP BY shk_id, srid
"""

QUERY_INSERT_SHK_SRID = """
TRUNCATE TABLE buffer.shk_srid_for_imei;

INSERT INTO buffer.shk_srid_for_imei (shk_id, srid, create_dt)
select shk_id, srid, create_dt from ext;
"""

QUERY_INSERT_OM_SRID = """
truncate table buffer.om_srid_for_imei;

insert into buffer.om_srid_for_imei
select * from ext
"""

QUERY_DELETE_OM = """
ALTER TABLE buffer.orders_meta_for_imei
update
    is_del = joinGet(buffer.del_srids_for_imei, 'is_del', srid)
where 1 = 1
SETTINGS mutations_sync=1;
"""

QUERY_INSERT_DM_PAY = """
insert into buffer.dm_srid_pay_for_imei (srid, nm_id, dt, status_id, supplier_id, payment_type, currency_id, price_rub)

with
    (select min(dt) - interval '7' day from ext) as min_t,
    (select max(dt) + interval '7' day from ext) as max_t
select srid,
    argMax(ext.nm_id, dt) as nm_id,
    argMax(create_ts, dt) as create_dt,
    argMax(status_oof, dt) as status_id,
    argMax(seller_id, dt) as supplier_id,
    argMax(payment_type, dt) as payment_type,
    argMaxIf(currency_id, dt, currency_id is not null) as currency_id,
    round(argMax(price, dt) * if(currency_id = 643, 1, dictGet('dict.cbr_currency', 'rate', (currency_id, max(dt)))) / 100) AS price_rub
from positions.oof_position_status_v3 as v3
join ext using(srid)
where
    v3.dt between min_t and max_t
    and v3.currency_id is not null and price is not null
    and v3.srid in (select srid from ext)
group by srid
having nm_id is not null and srid is not null and create_dt is not null and price_rub is not null;
"""

# здесь дубли, если будем брать min_ts (в принципе он дальше не нужен)
QUERY_INSERT_OM_SRID_SHK = """
truncate table buffer.om_srid_shk_for_imei;

insert into buffer.om_srid_shk_for_imei
select t.srid, t.dt, t.meta_key, t.meta_value, s.shk_id, null as min_ts, t.row_created
from (select distinct srid, shk_id from datamart.srid_shk
    where srid in (select srid from buffer.om_srid_for_imei)) as s
join buffer.om_srid_for_imei as t using (srid)
"""

"""
########################################################################################################
########################################################################################################
########################################################################################################
"""


def get_param(name: str, **context):
    return context.get("params", {}).get(name)


@with_db(CH8_CONN_ID, "ch8")
@with_db(CHDM_CONN_ID, "chdm")
def collect_data_shk_create(ch8_hook: ClickhouseHook, ch8_curs, chdm_hook: ClickhouseHook, ti, **context):
    stage_name = "Copy shk_create data [ch8-> dm0]"
    log.info(f"STAGE: {stage_name}")

    custom_from = get_param("from", **context)
    custom_to = get_param("to", **context)

    if custom_from:
        minmax = {"min_time": custom_from, "max_time": custom_to}
        rec_cnt = 1
        log.info(f"From date: {custom_from} to date: {custom_to}")
    else:
        cutoff = get_cutoff(
            db_cursor=ch8_curs,
            table_name="shk_storage.shk_create",
            field=FIELD_DT,
            def_cutoff_query=f"select parseDateTime64BestEffort('{START_DATE}')",
        )

        min_t, max_t, max_dwh_date, rec_cnt = get_ch_dwh_max_v2(
            ch8_hook,
            "shk_storage.shk_create",
            "row_created",
            cutoff,
            max_batch_records=BATCH_SIZE_SC,
            back_seek_seconds=BACK_SECONDS,
        )

        minmax = {"min_time": min_t, "max_time": max_t}
        log.info(f"From date: {min_t} to date: {max_t}")

    # Читсим целевой буффер. Буффер SHK не чистим, будем делать это отложенно после отправки
    chdm_hook.exec_with_log("TRUNCATE TABLE buffer.srid_shk_imei")

    if rec_cnt:
        ch8_hook.exec_with_log(QUERY_COLLECT_SHK_CREATE % minmax)

        if not custom_from:
            ti.xcom_push(key="sc_cutoff", value=max_t)

    log.info(f"STAGE DONE: {stage_name}")


@with_db(CH8_CONN_ID, "ch8")
@with_db(CH4_CONN_ID, "ch4")
def enrich_data_shk_create(ch8_hook: ClickhouseHook, ch4_hook: ClickhouseHook):
    stage_name = "Enrich data [ch4 -> chdm]"

    log.info(f"STAGE: {stage_name}")

    # Получаем ШК для сопоставления со сридами
    ids_list = ch8_hook.get_records("select distinct shk_id from buffer.shk_create_for_imei")

    external_tables = [
        {
            "name": "ext",
            "structure": [("shk_id", "Int64")],
            "data": ids_list,
        }
    ]

    # Получаем СРИД-ы на основе списка ШК из datamart.shk_srid. 1 к N
    data = ch4_hook.exec_with_external(QUERY_COLLECT_SHK, external_tables=external_tables)

    # Обогащение целевого буфера
    external_tables = [
        {
            "name": "ext",
            "structure": [("shk_id", "Int64"), ("srid", "String"), ("create_dt", "DateTime")],
            "data": data,
        }
    ]

    # чистим и заполняем Join таблицу
    ch8_hook.exec_script_external(QUERY_INSERT_SHK_SRID, external_tables=external_tables)

    # отправляем буффер в целевой на CHDM, размножив его доп. сридами из Join
    copy_ch_to_ch_pipe(
        take_data=QUERY_SELECT_SHK_CREATE,
        insert_data=QUERY_INSERT_TO_TARGET_BUF,
        src_ch=CH8_CONN_ID,
        dst_ch=CHDM_CONN_ID,
        throw_if_empty=False,
    )

    ch8_hook.exec_with_log("delete from buffer.shk_create_for_imei where srid is not null")
    log.info(f"STAGE DONE: {stage_name}.")


@with_db(CH4_CONN_ID, "ch4", conn_kwargs={"send_receive_timeout": 7200})
@with_db(CHDM_CONN_ID, "chdm", conn_kwargs={"send_receive_timeout": 7200})
def collect_data_orders_meta(ch4_hook: ClickhouseHook, ch4_curs, ti, **context):
    stage_name = "Copy orders_meta data [ch4-> dm0]"
    log.info(f"STAGE: {stage_name}")

    custom_from = get_param("from", **context)
    custom_to = get_param("to", **context)

    if custom_from:
        minmax = {"min_time": custom_from, "max_time": custom_to}
        log.info(f"From date: {custom_from} to date: {custom_to}")
    else:
        cutoff = get_cutoff(
            db_cursor=ch4_curs,
            table_name="stage_nats.orders_meta",
            field=FIELD_DT,
            def_cutoff_query=f"select parseDateTime64BestEffort('{START_DATE}')",
        )

        min_t, max_t, max_dwh_date, rec_cnt = get_ch_dwh_max_v2(
            ch4_hook,
            "stage_nats.orders_meta",
            "row_created",
            cutoff,
            max_batch_records=BATCH_SIZE_OM,
            back_seek_seconds=BACK_SECONDS,
        )

        minmax = {"min_time": min_t, "max_time": max_t}
        log.info(f"From date: {min_t} to date: {max_t}")

    # из ORDERS_META забираем по отсечке SRID и мету
    log.info("Step 1: take ORDERS_META with SRID into buffer")

    # Здесь копим новые и необработанные старые записи
    ch4_hook.exec_with_log(QUERY_COLLECT_ORDERS_META % minmax)
    data = ch4_hook.get_records("select srid, dt, meta_key, meta_value, row_created from buffer.orders_meta_for_imei")

    for rows in batcher(data, BATCH_SIZE_OM):
        external_tables = [
            {
                "name": "ext",
                "structure": [
                    ("srid", "String"),
                    ("dt", "DateTime"),
                    ("meta_key", "String"),
                    ("meta_value", "String"),
                    ("row_created", "DateTime"),
                ],
                "data": rows,
            }
        ]

        ch4_hook.exec_script_external(QUERY_INSERT_OM_SRID, external_tables=external_tables)
        # Перекладываем в буффер добавив SHK_ID. Здесь уже может быть потеря СРИД-а при джоине с datamart.srid_shk
        log.info("Step 2: Join and go to another buffer with SHK_ID")
        ch4_hook.exec_with_log(QUERY_INSERT_OM_SRID_SHK)

        log.info("Step 3: Go to CH2")
        copy_ch_to_ch_pipe(
            take_data="select distinct srid, shk_id from buffer.om_srid_shk_for_imei where shk_id is not null FORMAT MsgPack",
            insert_data="""TRUNCATE TABLE buffer.om_srid_shk_for_imei; INSERT INTO buffer.om_srid_shk_for_imei (srid, shk_id) FORMAT MsgPack""",
            src_ch=CH4_CONN_ID,
            dst_ch=CH2_CONN_ID,
            throw_if_empty=False,
        )

        take_data_sql = """select s.shk_id, s.srid, p.nm_id from buffer.om_srid_shk_for_imei s join
                        (select srid,nm_id,shk_id from datamart.shk_rid_price_nm_v2
                        where shk_id in (select shk_id from buffer.om_srid_shk_for_imei)) as p using (shk_id,srid) FORMAT MsgPack"""
        log.info("Step 4: Join NM and go back to CH4")
        copy_ch_to_ch_pipe(
            take_data=take_data_sql,
            insert_data="""TRUNCATE TABLE buffer.om_srid_shk_nm_for_imei; insert into buffer.om_srid_shk_nm_for_imei (shk_id, srid, nm_id) FORMAT MsgPack""",
            src_ch=CH2_CONN_ID,
            dst_ch=CH4_CONN_ID,
            throw_if_empty=False,
            client_parameters={"receive_timeout": 30 * 60 * 1000},
            dst_client_parameters={"receive_timeout": 30 * 60 * 1000},
        )

        # TMP_TABLE_OM_SRID_SHK_NM потенциально содержит меньше сридов, чем TMP_TABLE_OM_SRID_SHK
        log.info("Step 5: Join OM and NM and go to DM")
        copy_ch_to_ch_pipe(
            take_data=QUERY_SELECT_ORDERS_META,
            insert_data=QUERY_INSERT_TO_TARGET_BUF,
            src_ch=CH4_CONN_ID,
            dst_ch=CHDM_CONN_ID,
            throw_if_empty=False,
            client_parameters={"receive_timeout": 30 * 60 * 1000},
            dst_client_parameters={"receive_timeout": 30 * 60 * 1000},
        )

    if not custom_from:
        ti.xcom_push(key="om_cutoff", value=max_t)

    log.info(f"STAGE DONE: {stage_name}")


@with_db(CH4_CONN_ID, "ch4", conn_kwargs={"send_receive_timeout": 7200})
@with_db(CHDM_CONN_ID, "chdm", conn_kwargs={"send_receive_timeout": 7200})
def enrich_data_buffer_dm(ch4_hook: ClickhouseHook, chdm_hook: ClickhouseHook):
    stage_name = "Enrich buffer datamart data [chdm -> ch04 -> chdm]"
    log.info(f"STAGE: {stage_name}")

    log.info("Step 1: Get srid, nm, dt from CHDM")
    data = chdm_hook.get_records(
        """select srid, nm_id, identifier_dt as dt from buffer.srid_shk_imei where srid is not null and nm_id is not null order by dt desc limit 1 by srid"""
    )

    ch4_hook.exec_with_log("truncate table buffer.dm_srid_pay_for_imei")

    for i, rows in enumerate(batcher(data, 500_000), start=1):
        external_tables = [
            {
                "name": "ext",
                "structure": [("srid", "String"), ("nm_id", "Int32"), ("dt", "DateTime")],
                "data": rows,
            }
        ]
        log.info(f"Step 2.{i}: inserting pay info")
        ch4_hook.exec_script_external(QUERY_INSERT_DM_PAY, external_tables=external_tables)
        i += 1

    # Дополняем supplier_id из словаря по nm_id
    log.info("Step 3: Enrich supplier_id on CH4")
    ch4_hook.exec_with_log(QUERY_UPDATE_DM_PAY_CH4)

    log.info("Step 4: Go to CHDM")
    # Заполняем Join таблицу на DM
    copy_ch_to_ch_pipe(
        take_data="select srid, nm_id, dt, status_id, supplier_id, payment_type, price_rub from buffer.dm_srid_pay_for_imei FORMAT MsgPack",
        insert_data="TRUNCATE TABLE buffer.dm_srid_pay_for_imei; INSERT INTO buffer.dm_srid_pay_for_imei (srid, nm_id, dt, status_id, supplier_id, payment_type, price_rub) FORMAT MsgPack",
        src_ch=CH4_CONN_ID,
        dst_ch=CHDM_CONN_ID,
        throw_if_empty=False,
        client_parameters={"receive_timeout": 30 * 60 * 1000},
        dst_client_parameters={"receive_timeout": 30 * 60 * 1000},
    )

    # Обновляем буффер DM данными PAY
    log.info("Step 5: Enrich DM buffer (pay info)")
    chdm_hook.exec_with_log(QUERY_UPDATE_DM_PAY)

    log.info(f"STAGE DONE: {stage_name}")


def save_cutoff_xcom(db_cursor: cursor, table_name: str, value: datetime = None, field: str = "max_dt"):
    if value:
        value = value.replace(tzinfo=None)  # удаляем offset-aware полученный с БД airflow

        if value >= datetime(1999, 1, 1, 3, 0):  # datetime(1970, 1, 1, 3, 0):
            save_cutoff(db_cursor=db_cursor, table_name=table_name, field=field, value=value)
            log.info(f"CUTOFF updated {table_name} = {value}")
        else:
            log.error(f"CUTOFF invalid value: {value}, break saving")
    else:
        log.warning(f"CUTOFF is empty: {value}, break saving")


@with_db(CHDM_CONN_ID, "chdm", conn_kwargs={"send_receive_timeout": 7200})
def enrich_dict_buffer_dm(chdm_hook: ClickhouseHook):
    stage_name = "Enrich buffer DM data [dm0 -> dm0]"

    log.info(f"STAGE: {stage_name}")

    MAX_RETRY = 3
    for i in range(1, MAX_RETRY + 1):
        try:
            chdm_hook.exec_with_log(QUERY_UPDATE_SHK_CREATE)
            log.info(f"dict.product_cards_nm mutation success on attempt {i}")
            break
        except Exception as e:
            log.error(f"Attempt {i} failed with error: {str(e)}")
            if i == 3:
                log.error(f"Max {MAX_RETRY} retries reached. Raising exception.")
                raise
            else:
                time.sleep(60)

    log.info(f"STAGE DONE: {stage_name}")


@with_db(CHDM_CONN_ID, "chdm", conn_kwargs={"send_receive_timeout": 7200})
@with_db(CH4_CONN_ID, "ch4", conn_kwargs={"send_receive_timeout": 7200})
@with_db(CH8_CONN_ID, "ch8")
def collect_dm(chdm_hook: ClickhouseHook, ch4_curs, ch4_hook, ch8_curs, ch8_hook, ti, **context):
    stage_name = "Fill DM data [dm0 -> dm0]"
    log.info(f"STAGE: {stage_name}")

    chdm_hook.exec_with_log("""insert into datamart.srid_shk_imei select * from buffer.srid_shk_imei where srid is not null;
                                optimize table datamart.srid_shk_imei final;""")

    # зачищаем буфер ОМ от прогруженных данных
    data = chdm_hook.get_records("""select srid from buffer.srid_shk_imei where source = 'OM' order by srid""")

    for i, rows in enumerate(batcher(data, 100_000), start=1):
        external_tables = [
            {
                "name": "ext",
                "structure": [("srid", "String")],
                "data": rows,
            }
        ]
        log.info(f"Clear buffers.{i}")
        ch4_hook.exec_with_log("truncate table buffer.del_srids_for_imei")
        ch4_hook.exec_script_external(
            "insert into buffer.del_srids_for_imei (srid) select srid from ext;", external_tables=external_tables
        )

        ch4_hook.exec_with_log(QUERY_DELETE_OM)
        ch4_hook.exec_with_log(
            "ALTER TABLE buffer.orders_meta_for_imei DELETE where is_del = true SETTINGS mutations_sync=1;"
        )
        i += 1

    cnt_om = ch4_hook.fetchone("select count(*) from buffer.orders_meta_for_imei")
    cnt_sc = ch8_hook.fetchone("select count(*) from buffer.shk_create_for_imei")
    log.warning(f"Unprocessed SC: {cnt_sc}")
    log.warning(f"Unprocessed OM: {cnt_om}")

    custom_from = get_param("from", **context)
    if not custom_from:  # не обновляем отсечки, если была ручная прогрузка
        sc_cutoff = ti.xcom_pull(task_ids="task_collect_data_shk_create", key="sc_cutoff")
        save_cutoff_xcom(db_cursor=ch8_curs, table_name="shk_storage.shk_create", value=sc_cutoff)

        om_cutoff = ti.xcom_pull(task_ids="task_collect_data_orders_meta", key="om_cutoff")
        save_cutoff_xcom(db_cursor=ch4_curs, table_name="stage_nats.orders_meta", value=om_cutoff)

    log.info(f"STAGE DONE: {stage_name}.")


default_args = {
    "owner": "kamaltdinov.r6",
    "depends_on_past": False,
    "email": [
        "kamaltdinov.r6@wildberries.work",
    ],
    "telegram": ["@NPV42"],
    "band": [
        "kamaltdinov.r6",
    ],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    default_args=default_args,
    dag_id="chdm0_imei",
    schedule="*/50 * * * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["@NPV42", "imei", CH8_CONN_ID, CH4_CONN_ID, CHDM_CONN_ID],
    description="Витрина с кодами imei, S/N и т.д. {from:2025-10-01, to: 2025-10-03}",
) as dag:
    task_collect_data_shk_create = PythonOperator(
        task_id="task_collect_data_shk_create",
        doc="Сборка данных shk_create",
        python_callable=collect_data_shk_create,
        pool=get_pool(CH8_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.shk_storage.shk_create"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.buffer.shk_create_for_imei"),
        ],
    )

    task_enrich_data_shk_create = PythonOperator(
        task_id="task_enrich_data_shk_create",
        doc="Обогощение данных shk_create",
        python_callable=enrich_data_shk_create,
        pool=get_pool(CHDM_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.default.buffer.shk_create_for_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.datamart.shk_srid", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.default.buffer.shk_create_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.default.buffer.shk_srid_for_imei", key="g2"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.default.buffer.shk_srid_for_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g2"),
        ],
    )

    task_collect_data_orders_meta = PythonOperator(
        task_id="task_collect_data_orders_meta",
        doc="Сборка данных orders_meta",
        python_callable=collect_data_orders_meta,
        pool=get_pool(CH4_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.stage_nats.orders_meta", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.orders_meta_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.datamart.srid_shk", key="g3"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_for_imei", key="g3"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_shk_for_imei", key="g4"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch2-recent.default.buffer.om_srid_shk_for_imei", key="g5"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch2-recent.default.datamart.shk_rid_price_nm_v2", key="g5"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_shk_for_imei", key="g6"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_shk_nm_for_imei", key="g6"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.datamart.shk_srid", key="g6"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.orders_meta_for_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_shk_for_imei", key="g3"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch2-кусуте.default.buffer.om_srid_shk_for_imei", key="g4"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.om_srid_shk_nm_for_imei", key="g5"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g6"),
        ],
    )

    task_enrich_data_buffer_dm = PythonOperator(
        task_id="task_enrich_buffer_dm",
        doc="Дополняем информацией о платежах",
        python_callable=enrich_data_buffer_dm,
        pool=get_pool(CHDM_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.positions.oof_position_status_v3", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.dict.product_cards_nm", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.dm_srid_pay_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.dm_srid_pay_for_imei", key="g3"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.dm_srid_pay_for_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.dm_srid_pay_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g3"),
        ],
    )

    task_enrich_dict_buffer_dm = PythonOperator(
        task_id="task_enrich_dict_buffer_dm",
        doc="Дополняем справочниками из продуктовых карточек",
        python_callable=enrich_dict_buffer_dm,
        retries=4,
        retry_delay=timedelta(minutes=10),
        pool=get_pool(CHDM_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.dict.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.dict.subjects"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei"),
        ],
    )

    task_collect_dm = PythonOperator(
        task_id="task_collect_dm",
        doc="Перенос буфера на витрину",
        python_callable=collect_dm,
        pool=get_pool(CHDM_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.buffer.srid_shk_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.del_srids_for_imei", key="g3"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-dm02.default.datamart.srid_shk_imei", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.del_srids_for_imei", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.default.buffer.orders_meta_for_imei", key="g3"),
        ],
    )

    (
        task_collect_data_shk_create
        >> task_enrich_data_shk_create
        >> task_collect_data_orders_meta
        >> task_enrich_data_buffer_dm
        >> task_enrich_dict_buffer_dm
        >> task_collect_dm
    )
