import logging as log
from datetime import datetime, date
from os import uname
from kafka import KafkaProducer

from utils.decorators_with_conn import with_db
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.data_exchange import copy_ch_to_ch_pipe, get_producer_params_kafka
from utils.cutoff import get_cutoff, save_cutoff
from utils.db.clickhouse import get_ch_dwh_max_v2

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

"""Даг считает срок от скана поставки до пика товара определенных статусов по каждому сриду для отчета 019. Сроки сортировки МП"""

CH4 = 'do-ch4'
CHDM = 'do-ch-dm0'
KAFKA_CONN_ID = 'kafka-offices-sorting-periods'
KAFKA_BATCH_SIZE = 100

GET_PART = """ SELECT DISTINCT partition
                 FROM system.parts
                WHERE database = 'datamart'
                  AND table = 'sorting_periods'
                  AND toInt32(partition) < toYYYYMM(dateAdd(month, - 3, NOW())) """

INSERT_AGG = """ INSERT INTO datamart.sorting_periods_agg(scan_sort, cnt_srid, dt_diff)
                   SELECT toDate(scan_sort)                      AS scan_sort
                        , toInt32(count(srid))                   AS cnt_srid
                        , toDecimal64(round(avg(dt_diff), 1), 1) AS dt_diff
                     FROM datamart.sorting_periods
                    WHERE toYYYYMM(scan_sort) = toUInt32(%(part_no)s)
                    GROUP BY scan_sort; """

GET_NEW_PERIODS = """
-- Собираю новые сканы товаров
DROP TEMPORARY TABLE IF EXISTS orders_scans;
CREATE TEMPORARY TABLE orders_scans ENGINE = MergeTree ORDER BY srid AS
SELECT srid
     , argMin(office_id, dt) AS office_id2
     , argMin(shk_id, dt)    AS shk_id2
     , argMin(state_id, dt)  AS state_id2
     , min(dt)               AS dt2
  FROM stage_nats.orders_scans_rc
 WHERE state_id IN ('SOF', 'WLT', 'SSG', 'WPT', 'SPS', 'TWR', 'WAB', 'BRA', 'SHC', 'SMC',
                    'SMS', 'WSF', 'WMI', 'SAP', 'WPU', 'PEP', 'WAS', 'OSR')
   AND row_created >= '{min_cutoff_orders}'
   AND row_created <= '{max_cutoff_orders}'
 GROUP BY ALL;

-- Ищу по ним сканы поставок
DROP TEMPORARY TABLE IF EXISTS supplies_scans;
CREATE TEMPORARY TABLE supplies_scans ENGINE = Join(ALL, LEFT, srid) SETTINGS persistent = 0 AS
SELECT CAST(wb_sticker_id AS Nullable(Int64)) AS wb_sticker_id
     , CAST(srid AS Nullable(String))         AS srid
     , CAST(max(dt) AS Nullable(DateTime))    AS dt2
  FROM stage_nats.supplies_scans_rc
 WHERE  srid IN (SELECT srid FROM orders_scans)
 GROUP BY ALL;

-- Собираю декларации по сридам из сканов товаров
DROP TEMPORARY TABLE IF EXISTS declaration;
CREATE TEMPORARY TABLE declaration ENGINE = Join(ALL, LEFT, srid, main_office_id) SETTINGS persistent = 0 AS
SELECT CAST(srid AS Nullable(String))                                                          AS srid
     , CAST(argMin(shk_id, dt) AS Nullable(Int64))                                             AS shk_id2
     , CAST(argMin(office_id, dt) AS Nullable(Int64))                                          AS office_id2
     , CAST(dictGet('dict.branch_office', 'main_office_id', lifecycle_dst) AS Nullable(Int64)) AS main_office_id
     , CAST(argMin(lifecycle_dst, dt) AS Nullable(Int64))                                      AS lifecycle_dst2
     , CAST(min(dt) AS Nullable(DateTime))                                                     AS dt2
  FROM stage_wh.wh_tares_declaration
 WHERE dt >= toStartOfMonth(today()) - toIntervalMonth(1)
   AND wh_tare_entry IN ('PVZ', 'PPVZ')
   AND tare_type = 'MPB'
   AND shk_id IN (SELECT shk_id2 FROM orders_scans)
   AND dictGet('dict.branch_office', 'type_point', office_id) <= 10
  GROUP BY ALL;

-- Собираю результат, отсекая повторные сканы
DROP TEMPORARY TABLE IF EXISTS sorting_period;
CREATE TEMPORARY TABLE sorting_period ENGINE = MergeTree ORDER BY srid AS
SELECT
       COALESCE(c.office_id2, a.office_id2)   AS scan_office_id
     , COALESCE(dictGetOrNull('dict.branch_office', 'office_type', assumeNotNull(c.office_id2)),
                dictGetOrNull('dict.branch_office', 'office_type', assumeNotNull(a.office_id2))
               ) AS office_type
     , a.srid    AS srid
     , a.shk_id2 AS shk_id
     , a.dt2     AS scan_sort
     , CASE
         WHEN a.state_id2 NOT IN ('OSR') -- ПВЗ DO-8261
          AND a.dt2 >= COALESCE(b.dt2, c.dt2)
            THEN round(abs(date_diff('second', scan_sort, COALESCE(b.dt2, c.dt2))) /3600, 1) -- часов
         WHEN a.state_id2 IN ('OSR')
            THEN 0.1 -- 6 мин. для внескладской сортировки DO-8194
        ELSE 0
       END AS dt_diff
     , COALESCE(c.dt2, b.dt2) AS scan_mp
  FROM orders_scans        a
  LEFT JOIN supplies_scans b ON a.srid       = b.srid
  LEFT JOIN declaration    c ON a.srid       = c.srid
                            AND a.office_id2 = c.main_office_id
 WHERE a.srid NOT IN (SELECT srid FROM buffer.sorting_periods);

-- Сохраняю новые сканы в буферную таблицу с TTL 4 месяца
INSERT INTO buffer.sorting_periods (scan_office_id, office_type, srid, shk_id, scan_sort, dt_diff, scan_mp)
SELECT scan_office_id, office_type, srid, shk_id, scan_sort, dt_diff, scan_mp
  FROM sorting_period;

-- Отправляю на CHDM0
SELECT scan_office_id, office_type, srid, shk_id, scan_sort, dt_diff, scan_mp
  FROM sorting_period FORMAT Native; """

KAFKA_QUERY = """
         -- DataOps-8054
    WITH today() - toDate(scan_sort) = 1 AS is_yesterday,
         toString(today()) AS date,
         scan_office_id AS office_id,
         any(office_type) AS office_type,
         round(avg(dt_diff), 5) AS avg_3_days,
         round(avgIfOrNull(dt_diff, is_yesterday), 5) AS avg_yesterday
  SELECT office_id,
         formatRowNoNewline('JSONEachRow',
            date, office_id, office_type,
            avg_3_days, avg_yesterday
         ) AS json_data
    FROM datamart.sorting_periods AS src
   WHERE scan_office_id != 0
     AND scan_sort >= today() - toIntervalDay(3)
     AND scan_sort <  today()
GROUP BY scan_office_id
SETTINGS optimize_aggregation_in_order=1
"""

@with_db(CH4, 'ch4')
def get_sorting_periods_v2(ch4_hook, ch4_curs):
    """ Собираю инфо по срокам сортировки DO-3725. (время в часах между пиком МП и сортом) """
    prev_cutoff = get_cutoff(ch4_curs, 'datamart.sorting_periods_019', field='max_dt')

    (min_cutoff_orders, max_cutoff_orders, _, rec_count) = get_ch_dwh_max_v2(
        ch_hook=ch4_hook,
        changes_table_name='stage_nats.orders_scans_rc',
        date_field='row_created',
        max_dt=prev_cutoff,
        max_batch_records=10_000_000,
        back_seek_seconds=60)

    log.info(f'min_cutoff_orders is {min_cutoff_orders}, max_cutoff_orders is {max_cutoff_orders}, rec_count is {rec_count}')

    # CH4 --> CHDM. Копирую на витринный клик
    copy_ch_to_ch_pipe(take_data=GET_NEW_PERIODS.format(min_cutoff_orders=min_cutoff_orders, max_cutoff_orders=max_cutoff_orders),
                       insert_data=f""" INSERT INTO datamart.sorting_periods (scan_office_id, office_type, srid, shk_id, scan_sort, dt_diff, scan_mp) FORMAT Native """,
                       src_ch=CH4,
                       dst_ch=CHDM,
                       multiquery=True)

    # сохраняю отсечку
    save_cutoff(ch4_curs, 'datamart.sorting_periods_019', max_cutoff_orders, field='max_dt')


def check_drop_part_need(**context):
    """Проверяет необходимость удаления партиций"""
    now = date.today()
    # дата предыдущего успешного запуска дага
    prev_start_date_success = context.get('prev_start_date_success')
    prev_ds = prev_start_date_success.date() if prev_start_date_success else now

    log.info(f'now : {now}, prev_ds: {prev_ds}')

    cur_d = now.day
    dif = abs((now - prev_ds).days)
    log.info(f'dif = {dif}, число {cur_d}')
    # удаляю при первом запуске 1-го числа
    if dif >= 1 and cur_d == 1:
        return True
    else:
        return False


@with_db(CHDM, 'chdm')
def drop_partitions(chdm_curs):
    chdm_curs.execute(GET_PART)

    lst = chdm_curs.fetchall()
    ln = len(lst)

    log.info(f'буду удалять {ln} партицию(и) {lst}, держитесь там')
    if ln > 0:
        for part_no in lst:
            log.info(f'Сохраняю данные из партиции {part_no}')
            chdm_curs.execute(INSERT_AGG, parameters={'part_no': part_no})

            log.info(f'Удаляю {part_no}')
            chdm_curs.execute(f'ALTER TABLE datamart.sorting_periods DROP PARTITION {part_no};')

    chdm_curs.close()


@with_db(CHDM, 'dm0')
def kafka_push(dm0_curs):
    kafka_conn = BaseHook().get_connection(KAFKA_CONN_ID)
    producer = KafkaProducer(**get_producer_params_kafka(KAFKA_CONN_ID,batch_size=KAFKA_BATCH_SIZE, client_id=uname()[1]))
    if not producer.bootstrap_connected():
        return

    dm0_curs.execute(KAFKA_QUERY)
    ins_count = 0
    ok_count = 0
    while rows := dm0_curs.fetchmany(size=KAFKA_BATCH_SIZE):
        futures = []
        for row in rows:
            ins_count += 1
            futures.append(
                producer.send(
                    topic=kafka_conn.schema,
                    value=row[1].encode(),
                    key=str(row[0]).encode()))
        producer.flush()
        dones = [f.get() for f in futures]
        ok_count += len(dones)

    producer.close()
    if ins_count > ok_count:
        err_cnt = ins_count - ok_count
        raise AirflowFailException(f'messages not delivered - {err_cnt}')


def_args = {
    'owner': 'urazov.vasiliy',
    'email': ['urazov.vasiliy@wildberries.work'],
    'telegram': ['@Novotrex', '@artemy_kravtsov'],
    'email_on_failure': False,
    'max_active_tasks': 1,
    'max_active_runs': 1,
}

with DAG(
        default_args=def_args,
        description="""Даг расчитывает срок сорта (время в часах между пиком МП и сортом), затем отправляет на витринный клик. DataOps-3725""",
        dag_id='chdm0_gp_sorting_periods_v2',
        schedule='45 2,15 * * *',  # At minute 45 past hour 2 and 15 UTC
        start_date=datetime(2023, 8, 22),
        catchup=False,
        tags=["@Novotrex", '@artemy_kravtsov', CH4, CHDM, "sorting_periods"],
    ) as dag:

    task_get_sorting_periods_v2 = PythonOperator(
        task_id='task_get_sorting_periods_v2',
        python_callable=get_sorting_periods_v2,
        dag=dag,
        pool=get_pool(CH4),
        doc="""Собираю данные по шк и расчитываю срок сорта (время в часах между пиком МП и сортом)""",
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.stage_nats.orders_scans_rc",key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.stage_nats.supplies_scans_rc",key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.stage_wh.wh_tares_declaration",key="g1"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.buffer.sorting_periods",key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"{CHDM}.datamart.sorting_periods",key="g1"),
        ],
    )
    task_check_drop_part_need = ShortCircuitOperator(
        task_id="task_check_drop_part_need",
        dag=dag,
        python_callable=check_drop_part_need,
    )
    task_drop_partitions = PythonOperator(
        task_id="task_drop_partitions",
        dag=dag,
        pool=get_pool(CHDM),
        python_callable=drop_partitions,
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CHDM}.datamart.sorting_periods",key="g1"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CHDM}.datamart.sorting_periods_agg",key="g1"),
        ],
    )

    task_check_kafka_push_need = ShortCircuitOperator(
        task_id="check_kafka_push_need",
        python_callable=lambda a, b: a != b,  # последний успешный запуск - не сегодня
        op_args=["{{ prev_start_date_success.to_date_string() }}",
                 "{{ macros.datetime.today().strftime('%Y-%m-%d') }}"]
    )

    task_kafka_push = PythonOperator(
        pool=get_pool(CHDM),
        task_id="kafka_push",
        python_callable=kafka_push,
    )

    task_get_sorting_periods_v2 >> [task_check_drop_part_need, task_check_kafka_push_need]
    task_check_drop_part_need >> task_drop_partitions
    task_check_kafka_push_need >> task_kafka_push
