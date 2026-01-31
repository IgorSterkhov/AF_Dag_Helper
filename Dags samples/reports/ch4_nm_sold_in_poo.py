from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.cutoff import get_ch_dwh_max
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

CH4_CONN = 'do-ch4'
CH8_CONN = 'do-ch8'
CH8_XC_CONN = 'do-ch8-xc'
TELEGA = '@artemy_kravtsov'
DESCRIPTION = """
    DataOps-9184. 
    Две витрины по продажам НМ (в разрезе по офисам и без разреза). Запускается инкрементально, 
    выполняет расчёты на ч4 и переносит результаты на ч8-xc. Витрины хранятся 
    на ч8 из-за того,  что запросам из суперсета требуется подтягивать статистику 
    по остаткам НМ, прежде чем отдавать данные пользователям. 
    А остатки НМ есть только на ч8.
"""

AVOID_PARENTS = [
    1, 2, 4, 657, 1107, 1513, 1598, 1616, 2479, 3497, 4607, 4735, 5300, 6260, 8604]

TRUNCATE_BUFFER_CH4_CH8 = """
TRUNCATE TABLE buffer.nm_sold_in_poo;
"""

CH4_CALC_DATA = """
CREATE TEMPORARY TABLE recent_nm_ids
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT nm_id
  FROM stage_bo.transactions
 WHERE _kafka_timestamp >= %(min_cutoff)s
   AND _kafka_timestamp <= %(max_cutoff)s
   AND dictGet('dict.kladr_tree', 'district_name',
           dictGet('dict.branch_office', 'city_id', office_id)
       ) IN ('Московская область', 'Москва')
;

TRUNCATE TABLE buffer.cards_external_for_sold_in_poo
;

INSERT INTO buffer.cards_external_for_sold_in_poo
       (nm_id, is_deleted, parent_id, subject_id, 
        supplier_id_shk, brand_id, height, width, length, weight)

  SELECT coalesce(cr.nm_id, dim.nm_id, pr.nm_id) AS nm_id,
         is_deleted, parent_id, subject_id, 
         coalesce(
             nullIf(cr.supplier_id_shk, 0), 
             nullIf(dim.supplier_id_shk, 0)) AS supplier_id_shk, 
         brand_id, height, width, length, fin_weight
    FROM (SELECT nm_id, is_deleted, parent_id, subject_id, 
                   brand_id, supplier_id_shk
            FROM remote_ch3.product_cards_nm FINAL
           WHERE nm_id GLOBAL IN (SELECT nm_id FROM recent_nm_ids)) AS cr
    FULL 
    JOIN (SELECT nm_id, supplier_id_shk, height, width, length
            FROM remote_ch3.product_cards_nm_dimensions FINAL
           WHERE nm_id GLOBAL IN (SELECT nm_id FROM recent_nm_ids)) AS dim
      ON cr.nm_id = dim.nm_id
    FULL 
    JOIN (  WITH if(weight_brutto > 0 AND weight_brutto < 0.0001, 0.0001, weight_brutto) AS w
          SELECT nm_id, CAST(argMax(w, updated_at), 'Nullable(Decimal(12, 4))') AS fin_weight
            FROM remote_ch3.product_cards_tnved
           WHERE nm_id GLOBAL IN (SELECT nm_id FROM recent_nm_ids)
             AND isNotNull(weight_brutto)
        GROUP BY nm_id) AS pr
      ON coalesce(cr.nm_id, dim.nm_id) = pr.nm_id
;

  INSERT INTO  buffer.nm_sold_in_poo
         (date, office_id, nm_id, srid_cnt, sum_price)

  SELECT date,
         office_id,
         nm_id,
         count() AS srid_cnt,
         round(sum(price)) AS sum_price
    FROM (
           SELECT srid,
                  any(nm_id) AS nm_id,
                  any(office_id) AS office_id,
                  argMinIf(currency_id, src.transaction_dt, type='sale') AS currency,
                  toDate(minIf(src.transaction_dt, type='sale')) AS date,
                  argMinIf(price, src.transaction_dt, type='sale')
                      * if(currency = 643, 1,
                           dictGet('dict.cbr_currency',
                                   'rate', (currency, date))) AS price
             FROM stage_bo.transactions AS src
            WHERE src._kafka_timestamp >= %(min_cutoff)s - toIntervalMonth(5)  /* пересчёт всех продаж НМ с глубиной поиска в 5 месяцев */
              AND src._kafka_timestamp <= %(max_cutoff)s
              AND dictGet('dict.kladr_tree', 'district_name',
                      dictGet('dict.branch_office', 'city_id', src.office_id)
                  ) IN ('Московская область', 'Москва')                  /* только ПВЗ из МСК и МО */
              AND src.nm_id IN (
                                SELECT nm_id
                                  FROM recent_nm_ids
                                 WHERE nm_id NOT IN (
                                                     SELECT nm_id
                                                       FROM buffer.cards_external_for_sold_in_poo
                                                      WHERE is_deleted
                                                        AND parent_id IN %(avoid_parents)s
                                                            /* только по парентам не из списка */
                                                    ) 
                               )
         GROUP BY srid
           HAVING argMax(type, src.transaction_dt) = 'sale'  /* только продажи без возвратов */
              AND (currency = 643 OR dictHas('dict.cbr_currency', (currency, toDate(date))))
              AND date >= '2023-01-01'
         )
  GROUP BY date, office_id, nm_id
  SETTINGS max_bytes_before_external_group_by='90G'
"""

TAKE_DATA_FROM_CH4 = """
  SELECT date, office_id, nm_id,
         parent_id, subject_id,
         supplier_id_shk AS seller_id,
         dictGetOrDefault(
             'dict.sellers_portal', 
             'supplier_name', 
             supplier_id_shk, '') AS seller_name,
         dictGetOrDefault(
             'dict.brands', 
             'brand_name', 
             brand_id, '') AS brand_name,
         srid_cnt, sum_price,
         height, width, length, weight,
         now() AS row_created
    FROM buffer.nm_sold_in_poo AS src
LEFT ANY 
    JOIN buffer.cards_external_for_sold_in_poo AS cards
      ON src.nm_id = cards.nm_id
   WHERE parent_id NOT IN {avoid_parents}
  FORMAT MsgPack
"""

INSERT_DATA_CH8 = """
INSERT INTO buffer.nm_sold_in_poo
    (date, office_id, nm_id, parent_id, subject_id, 
     seller_id, seller_name, brand_name, srid_cnt, 
     sum_price, height, width, length, weight, row_created)
FORMAT MsgPack
"""

GET_PARTITIONS_LIST_CH4 = """
SELECT DISTINCT toYYYYMM(date) AS month
FROM buffer.nm_sold_in_poo
ORDER BY month
"""

MANIPULATE_PARTITIONS_CH8 = """
ALTER TABLE buffer.nm_sold_in_poo 
ATTACH PARTITION %(partition)s 
FROM datamart.nm_sold_in_poo
;

CREATE TEMPORARY TABLE nm_sold_in_poo_no_offices_tmp
ENGINE=MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date
AS datamart.nm_sold_in_poo_no_offices
;

 INSERT INTO nm_sold_in_poo_no_offices_tmp
        (date, nm_id, ttl_offices, parent_id, subject_id, seller_id, seller_name, 
         brand_name, synth_srid_cnt, srid_cnt, sum_price, height, width, length, weight)
 SELECT date,
        nm_id,
        any(ttl_offices) AS ttl_offices,
        any(parent_id) AS parent_id,
        any(subject_id) AS subject_id,
        any(seller_id) AS seller_id,
        any(seller_name) AS seller_name,
        any(brand_name) AS brand_name,
        sum(synth_srid_cnt) AS synth_srid_cnt,
        sum(srid_cnt) AS srid_cnt,
        sum(sum_price) AS sum_price,
        any(height) AS height,
        any(width) AS width,
        any(length) AS length,
        any(weight) AS weight
   FROM (
           WITH ((ttl_offices > 10) AND
                 ((rank / ttl_offices <= 0.05) OR (rank / ttl_offices >= 0.95))
                ) AS is_outlier
         SELECT *, if(is_outlier,  /* подмена выбросов средними значениями за день */
                      avgIf(srid_cnt, is_outlier=0) OVER (PARTITION BY nm_id, date),
                      srid_cnt) AS synth_srid_cnt
           FROM (SELECT *, count()   OVER (PARTITION BY nm_id, date) AS ttl_offices,
                        row_number() OVER (PARTITION BY nm_id, date ORDER BY srid_cnt) AS rank
                   FROM buffer.nm_sold_in_poo FINAL
                  WHERE toYYYYMM(date) = toUInt32(%(partition)s))
         )
GROUP BY nm_id, date
;

ALTER TABLE datamart.nm_sold_in_poo_no_offices
REPLACE PARTITION %(partition)s
FROM nm_sold_in_poo_no_offices_tmp
;

ALTER TABLE datamart.nm_sold_in_poo
REPLACE PARTITION %(partition)s
FROM buffer.nm_sold_in_poo
;

ALTER TABLE buffer.nm_sold_in_poo
DROP PARTITION %(partition)s
"""

REMAINING_NM_IDS_CH8 = """
CREATE TEMPORARY TABLE remainig_nm_ids_14d
ENGINE = MergeTree
ORDER BY tuple()
AS
    SELECT nm_id
      FROM stage_wh.nm_for_sale_on_date
     WHERE update_date BETWEEN today() -14 AND today() -1
  GROUP BY nm_id
    HAVING uniqExact(update_date) = 14
;

ALTER TABLE datamart.remainig_nm_ids_14d 
REPLACE PARTITION tuple() 
FROM remainig_nm_ids_14d
"""

SELLERS_WITH_MORE_THAN_5NM_CH8 = """
CREATE TEMPORARY TABLE sellers_with_more_than_5nm
ENGINE = MergeTree
ORDER BY tuple()
AS
    SELECT assumeNotNull(supplier_id) AS seller_id
      FROM shk_storage.shk_for_sale FINAL
     WHERE state_sale != 3
       AND create_dt > today() - toIntervalYear(2)
       AND isNotNull(supplier_id)
  GROUP BY supplier_id
    HAVING uniqUpTo(5)(nm_id) = 6
;

ALTER TABLE datamart.sellers_with_more_than_5nm 
REPLACE PARTITION tuple() 
FROM sellers_with_more_than_5nm
"""


@with_db(CH4_CONN, 'ch4')
@with_db(CH8_XC_CONN, 'ch8')
@load_and_save_cutoff('buffer.nm_sold_in_poo', 'ch4', 'max_dt')
def top_sales_in_poo(ch4_hook, ch8_hook, cutoff):
    (min_cutoff, max_cutoff, _, _) = get_ch_dwh_max(
        conn_id=CH4_CONN,
        changes_table_name='stage_bo.transactions',
        date_field='_kafka_timestamp',
        max_dt=cutoff,
        max_batch_records=500_000_000,
        max_hour_offset=60000,
        back_seek_seconds=1200)
    # макс. разрыв между _kafka_timestamp и row_created - 179 сек
    # (в таблице сортировка по _kafka_timestamp)
    ch4_hook.exec_with_log(TRUNCATE_BUFFER_CH4_CH8)
    ch8_hook.exec_with_log(TRUNCATE_BUFFER_CH4_CH8)
    ch4_hook.exec_with_log(
        CH4_CALC_DATA,
        parameters=dict(
            min_cutoff=min_cutoff,
            max_cutoff=max_cutoff,
            avoid_parents=AVOID_PARENTS))
    copy_ch_to_ch_pipe(
        take_data=TAKE_DATA_FROM_CH4.format(avoid_parents=AVOID_PARENTS),
        insert_data=INSERT_DATA_CH8,
        src_ch=CH4_CONN,
        dst_ch=CH8_XC_CONN,
        throw_if_empty=True)
    for partition in ch4_hook.get_records(GET_PARTITIONS_LIST_CH4):
        ch8_hook.exec_with_log(
            MANIPULATE_PARTITIONS_CH8,
            parameters=dict(partition=partition[0]))
    return max_cutoff


@with_db(CH8_XC_CONN, 'ch8')
def remainig_nm_ids_14d(ch8_hook):
    ch8_hook.exec_with_log(REMAINING_NM_IDS_CH8)


@with_db(CH8_CONN, 'ch8')
def sellers_with_more_than_5nm(ch8_hook):
    ch8_hook.exec_with_log(SELLERS_WITH_MORE_THAN_5NM_CH8)


with DAG(
        dag_id='ch4_nm_sold_in_poo',
        description=DESCRIPTION,
        start_date=datetime(2024, 6, 1),
        schedule='0 4 * * *',  # в 04:00 каждый день
        catchup=False,
        tags=[TELEGA, CH4_CONN, CH8_CONN, CH8_XC_CONN],
        max_active_runs=1,
        default_args=dict(
            owner='kravcov.artemiy',
            telegram=[TELEGA],
            email_on_failure=True,
            retries=2,
            retry_delay=timedelta(minutes=10)),
) as dag:
    nm_sold_in_poo_task = PythonOperator(
        pool=CH4_CONN,
        task_id='nm_sold_in_poo',
        python_callable=top_sales_in_poo,
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.remote_ch3.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.remote_ch3.product_cards_nm_dimensions"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.remote_ch3.product_cards_tnved"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.dict.kladr_tree"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_bo.transactions"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_external.suppliers")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch4.buffer.nm_sold_in_poo"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.buffer.nm_sold_in_poo"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.datamart.nm_sold_in_poo"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.datamart.nm_sold_in_poo_no_offices")])

    remainig_nm_ids_14d_task = PythonOperator(
        pool=CH8_XC_CONN,
        task_id='remainig_nm_ids_14d',
        python_callable=remainig_nm_ids_14d,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.nm_for_sale_on_date")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch8.datamart.remainig_nm_ids_14d")])

    sellers_with_more_than_5nm_task = PythonOperator(
        pool=CH8_CONN,
        task_id='sellers_with_more_than_5nm',
        python_callable=sellers_with_more_than_5nm,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch8.shk_storage.shk_for_sale")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch8.datamart.sellers_with_more_than_5nm")])

    nm_sold_in_poo_task
    remainig_nm_ids_14d_task
    sellers_with_more_than_5nm_task
