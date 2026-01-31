from datetime import datetime, timedelta
from extra.entities import Dataset
from airflow.models import DAG
from extra.get_pool import get_pool
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from utils.decorators_with_conn import with_db, with_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe 
from airflow.utils.session import provide_session
from airflow.models.taskinstance import TaskInstance
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity
from airflow.utils.state import State


CH8_CONN = 'do-ch8'
LAKE_CONN = 'do-lake-m'
DM_CONN = 'do-ch13'
TELEGA = '@artemy_kravtsov'
DESCRIPTION = """
    DataOps-9375. 
    Собирает витрины на dm3 для дарксторов: 
      — остатки и продажи на дату для НМ  (datamart.nm_from_darkstore) 
      — поступления в даркстор            (datamart.nm_income_darkstore) 
      — сроки доставки в дарксторах       (datamart.speeds_in_darkstore)
      — статистика по order_uid           (datamart.order_uids_from_darkstore)
      — статистика по order_uid + srid    (datamart.order_uids_detailed_from_darkstore)
    Расчёты инкрементальные (по отсечкам), преимущественно на лейках.
"""
DARKSTORES = [323608, 50129197]
MAX_RECORDS = 10_000_000_000
GET_RC = lambda x: '_rc_d' if (datetime.now() - x).days < 30 else '_d'

BLOCKED_PAYMENT_TYPES = [
    'FPY', 'NPY', 'QRC', 'SRD', 'SRF', 'SRG', 
    'SRN', 'SRR', 'SRT', 'STR', 'STT', 'WAI']
BLOCKED_WHEN_NOT_PAID_ORDO = ['WPG', 'QRS', 'MPM']

LAKE_CALC_DATA = """
 TRUNCATE TABLE buffer.recent_srids_darkstore
;

CREATE TEMPORARY TABLE unfinished_srids
ENGINE = Set
AS 
SELECT srid 
  FROM datamart.srids_from_darkstore
 WHERE last_status_is_final = False
   AND create_date > toDateTime(%(min_cutoff_oof)s) - toIntervalMonth(6)
;

/* 055: отбираю по отсечке надавние сриды */
   INSERT INTO buffer.recent_srids_darkstore
          (srid, nm_id, darkstore_id, create_date)

     WITH %(darkstores)s AS darkstores
   SELECT srid, 
          nm_id,
          darkstore_id,
          create_date
     FROM (
           SELECT srid, nm_id, src_office_id, create_ts,
                  payment_type, dt, ifNull(is_paid, False) AS is_paid
             FROM positions.oof_position_status_v3_rc
            WHERE row_created >= %(min_cutoff_oof)s
              AND row_created <= %(max_cutoff_oof)s
              AND %(min_cutoff_oof)s > now() - toIntervalMonth(1)
              AND (src_office_id IN darkstores OR srid IN unfinished_srids)

            UNION ALL
           SELECT srid, nm_id, src_office_id, create_ts,
                  payment_type, dt, ifNull(is_paid, False) AS is_paid
             FROM positions.oof_position_status_v3
            WHERE row_created >= %(min_cutoff_oof)s
              AND row_created <= %(max_cutoff_oof)s
              AND %(min_cutoff_oof)s <= now() - toIntervalMonth(1)
              AND (src_office_id IN darkstores OR srid IN unfinished_srids)
          )
 GROUP BY srid,
          nm_id,
          src_office_id AS darkstore_id,
          toDate(create_ts) AS create_date
;

 TRUNCATE TABLE buffer.srids_from_darkstore
;

/* 055: на основе полной истории недавних сридов рассчитываю метрики по сридам */
     INSERT INTO buffer.srids_from_darkstore
            (srid, order_uid, create_dt, create_date, last_status_id, last_status_is_final,
             poo_office_id, source, sm_id, nm_id, chrt_id, darkstore_id, price, is_deleted,
             row_created)
          
       WITH (toDateTime(%(min_cutoff_oof)s) - toIntervalYear(1)) AS minmax_partkey
     SELECT srid,
            any(order_uid) AS order_uid,
            any(create_dt) AS create_dt,
            toDate(create_dt) AS create_date,
            argMaxIf(status_id, dt, status_id != 0) AS last_status_id,
            dictGetOrDefault(
                'dict.positions_statuses', 
                'status_type', 
                last_status_id, 0) IN (2, 3) AS last_status_is_final,
            coalesce(
                argMax(dst_office_id, dt),
                any(dst_office_id)) AS poo_office_id,
            argMaxIf(source, dt, status_id != 0) AS source,
            argMax(sm_id, dt) AS sm_id,
            any(nm_id) AS nm_id,
            any(chrt_id) AS chrt_id,
            argMax(src_office_id, dt) AS darkstore_id,
            argMax(price, dt) * coalesce(
                dictGetOrNull(
                    'dict.cbr_currency',
                    'rate',
                    (argMax(currency_id, dt), create_date)),
                1) AS price,
            darkstore_id NOT IN %(darkstores)s AS is_deleted, 
            now() AS row_created
       FROM (
             SELECT srid, create_ts AS create_dt, nm_id, chrt_id, sm_id,
                    src_office_id, if(isNotNull(reject_reason),
                       coalesce(
                           toUInt16OrNull(dictGet('dict.positions_reject_reason', 'status_id_ordo', reject_reason)),
                           reject_reason),
                       status_oof) AS status_id,
                    dt, currency_id, price, order_uid, payment_type, 'oof_v3' AS source,
                    ifNull(is_paid, False) AS is_paid, dst_office_id
               FROM positions.oof_position_status_v3
              WHERE dt > minmax_partkey
                AND srid IN (SELECT srid FROM buffer.recent_srids_darkstore)
                AND isNotNull(dt)
            )
      WHERE status_id NOT IN (666, 122)
   GROUP BY srid
     HAVING argMax(payment_type, dt) NOT IN %(blocked_payment_types)s
        AND (argMax(is_paid, dt) = True
                 OR argMax(payment_type, dt) NOT IN %(blocked_when_not_paid_ordo)s)   
"""

GET_SRIDS_PARTITIONS_LAKE = """
   SELECT DISTINCT partition
     FROM cluster('lake_m', system.parts)
    WHERE active
      AND database = 'buffer'
      AND table = 'srids_from_darkstore'
 ORDER BY toUInt32(partition)
"""

MANIPULATE_SRIDS_PARTITION_LAKE = """
    ALTER TABLE datamart.srids_from_darkstore
   ATTACH PARTITION %(part_name)s
     FROM buffer.srids_from_darkstore
"""

LAKE_CALC_GET_GENERAL_DATA = """
/* 055: выбираю из недавних сридов все уникальные НМ + даты создания. 
   для этих НМ + дат пересчитываю метрики и переношу на ч8 */

   SELECT create_date, 
          darkstore_id, 
          a.nm_id AS nm_id,
          [price_ttl, price_poo, price_courier] AS price,
          [srid_ttl,  srid_poo,  srid_courier]  AS srid_cnt,
          [price_ttl_tkn, price_poo_tkn, price_courier_tkn] AS price_taken,
          [srid_ttl_tkn,  srid_poo_tkn,  srid_courier_tkn]  AS srid_taken_cnt,
          [price_ttl_rejected, price_poo_rejected, price_courier_rejected] AS price_rejected,
          [srid_ttl_rejected,  srid_poo_rejected,  srid_courier_rejected] AS srid_rejected_cnt,
          [price_ttl_cancelled, price_poo_cancelled, price_courier_cancelled] AS price_cancelled,
          [srid_ttl_cancelled,  srid_poo_cancelled,  srid_courier_cancelled] AS srid_cancelled_cnt,          
          [srid_not_taken_gr1_ttl, srid_not_taken_gr1_poo, srid_not_taken_gr1_courier] AS srid_not_taken_gr1_cnt,
          [srid_not_taken_gr2_ttl, srid_not_taken_gr2_poo, srid_not_taken_gr2_courier] AS srid_not_taken_gr2_cnt,
          [srid_waiting_ttl, srid_waiting_poo, srid_waiting_courier] AS srid_waiting_cnt
          
     FROM (     
                WITH last_status_id_fin IN (28) AS is_waiting,
                     last_status_id_fin IN (9, 16, 103, 106) AS is_taken,
                     last_status_id_fin IN (8, 48, 120, 124) AS is_rejected,
                     last_status_id_fin IN (1, 2, 13, 14, 101, 122, 123) AS is_cancelled,
                     last_status_id_fin IN (1, 2, 14, 101, 121, 122) AS is_not_taken_gr1,
                     last_status_id_fin IN (1, 2, 8, 13, 14, 38, 48, 101, 108, 109, 119, 120) AS is_not_taken_gr2
              SELECT nm_id, 
                     create_date, 
                     darkstore_id,
                     sum(price) AS price_ttl,
                     count() AS srid_ttl,
                     sumIf(price, sm_id=6) AS price_poo,
                     countIf(sm_id=6) AS srid_poo,
                     sumIf(price, sm_id=5) AS price_courier,
                     countIf(sm_id=5) AS srid_courier,
                     sumIf(price, is_taken) AS price_ttl_tkn,
                     countIf(is_taken) AS srid_ttl_tkn,
                     sumIf(price, sm_id=6 AND is_taken) AS price_poo_tkn,
                     countIf(sm_id=6 AND is_taken) AS srid_poo_tkn,
                     sumIf(price, sm_id=5 AND is_taken) AS price_courier_tkn,
                     countIf(sm_id=5 AND is_taken) AS srid_courier_tkn,
                     sumIf(price, is_rejected) AS price_ttl_rejected,
                     countIf(is_rejected) AS srid_ttl_rejected,
                     sumIf(price, sm_id=6 AND is_rejected) AS price_poo_rejected,
                     countIf(sm_id=6 AND is_rejected) AS srid_poo_rejected,
                     sumIf(price, sm_id=5 AND is_rejected) AS price_courier_rejected,
                     countIf(sm_id=5 AND is_rejected) AS srid_courier_rejected,
                     sumIf(price, is_cancelled) AS price_ttl_cancelled,
                     countIf(is_cancelled) AS srid_ttl_cancelled,
                     sumIf(price, sm_id=6 AND is_cancelled) AS price_poo_cancelled,
                     countIf(sm_id=6 AND is_cancelled) AS srid_poo_cancelled,
                     sumIf(price, sm_id=5 AND is_cancelled) AS price_courier_cancelled,
                     countIf(sm_id=5 AND is_cancelled) AS srid_courier_cancelled,                     
                     countIf(is_not_taken_gr1) AS srid_not_taken_gr1_ttl,
                     countIf(is_not_taken_gr1 AND sm_id=6) AS srid_not_taken_gr1_poo,
                     countIf(is_not_taken_gr1 AND sm_id=5) AS srid_not_taken_gr1_courier,
                     countIf(is_not_taken_gr2) AS srid_not_taken_gr2_ttl,
                     countIf(is_not_taken_gr2 AND sm_id=6) AS srid_not_taken_gr2_poo,
                     countIf(is_not_taken_gr2 AND sm_id=5) AS srid_not_taken_gr2_courier,
                     countIf(is_waiting) AS srid_waiting_ttl,
                     countIf(is_waiting AND sm_id=6) AS srid_waiting_poo,
                     countIf(is_waiting AND sm_id=5) AS srid_waiting_courier                                          
                FROM (
                        SELECT any(nm_id) AS nm_id, 
                               min(create_date) AS create_date, 
                               argMax(darkstore_id, row_created) AS darkstore_id,
                               argMax(sm_id, row_created) AS sm_id,
                               argMax(price, row_created) AS price,
                               argMax(
                                   if(last_status_id = 0, 16, last_status_id), 
                                   row_created) AS last_status_id_fin
                          FROM datamart.srids_from_darkstore_d AS src
                         WHERE (src.nm_id, 
                                src.create_date, 
                                src.darkstore_id) GLOBAL IN (
                                        SELECT nm_id, 
                                               create_date, 
                                               darkstore_id 
                                          FROM datamart.srids_from_darkstore_d
                                         WHERE row_created >= '{daily_min_cutoff}'
                                           AND row_created <= '{daily_max_cutoff}')
                      GROUP BY srid
                        HAVING argMax(is_deleted, row_created) = False
                     )
            GROUP BY nm_id, 
                     create_date, 
                     darkstore_id
          ) AS a

 SETTINGS distributed_group_by_no_merge=1
   FORMAT MsgPack  
"""

INSERT_DATA_CH8_BUFFER = """
 TRUNCATE TABLE buffer.nm_from_darkstore_v2
;

   INSERT INTO buffer.nm_from_darkstore_v2
          (create_date, darkstore_id, nm_id, price, srid_cnt, 
           price_taken, srid_taken_cnt, price_rejected, srid_rejected_cnt, 
           price_cancelled, srid_cancelled_cnt, srid_not_taken_gr1_cnt, 
           srid_not_taken_gr2_cnt, srid_waiting_cnt)
   FORMAT MsgPack
"""

ATTACH_DEAD_STOCK_CH8 = """
/* Для изменившихся НМ надо по датам присоединить остаток. Но этого мало - надо 
   и от всех остальных НМ за дату забрать остатки, т.к. всегда будут НМ, не 
   заказанные ни разу. Если этого не сделать, мы упустим эти НМ. Но среди 
   присоединённых НМ будут и такие, которые не были заказаны за дату в только недавних 
   сридах, хотя ранее по ним были продажи за дату. Их пока что не выделяю, т.к. не могу
   этого никак сделать, но перед вставкой в витрину (на датамарте) они отсеятся. */

   SELECT greatest(a.create_date, b.create_date) AS create_date, 
          toInt32(max2(a.darkstore_id, b.darkstore_id)) AS darkstore_id, 
          toInt32(max2(a.nm_id, b.nm_id)) AS nm_id, 
          price,
          arrayResize(srid_cnt, 3, 0) AS srid_cnt,
          arrayResize(price_taken, 3, 0) AS price_taken,
          arrayResize(srid_taken_cnt, 3, 0) AS srid_taken_cnt,
          arrayResize(price_rejected, 3, 0) AS price_rejected,
          arrayResize(srid_rejected_cnt, 3, 0) AS srid_rejected_cnt,
          arrayResize(price_cancelled, 3, 0) AS price_cancelled,
          arrayResize(srid_cancelled_cnt, 3, 0) AS srid_cancelled_cnt,          
          arrayResize(srid_not_taken_gr1_cnt, 3, 0) AS srid_not_taken_gr1_cnt,
          arrayResize(srid_not_taken_gr2_cnt, 3, 0) AS srid_not_taken_gr2_cnt,
          arrayResize(srid_waiting_cnt, 3, 0) AS srid_waiting_cnt,
          stock_cnt,
          ifNull(cards.title, '') AS title,
          ifNull(cards.parent_id, '') AS parent_id,
          ifNull(cards.subject_id, '') AS subject_id,
          dictGetOrDefault('dict.subjects', 'parent_name', subject_id, '') as parent_name,
          dictGetOrDefault('dict.subjects', 'subject_name', subject_id, '') as subject_name,
          ifNull(cards.supplier_id_shk, 0) AS seller_id,
          dictGetOrDefault('dict.suppliers_logistics', 'supplier_name', seller_id, '') AS seller_name,
          dictGetOrDefault('dict.brands', 'brand_name', cards.brand_id, '') as brand_name,
          now() - toIntervalYear((srid_cnt[1] = 0) * 10) AS _ver

     FROM (
           SELECT nm_id,
                  update_date AS create_date,
                  office_id AS darkstore_id,
                  toUInt32(sum(quantity)) AS stock_cnt
             FROM (
                      SELECT nm_id, update_date, office_id,
                             argMax(quantity, update_dt) AS quantity
                        FROM stage_wh.nm_for_sale_on_date
                       WHERE (office_id, update_date) IN (
                                 SELECT darkstore_id, create_date
                                   FROM buffer.nm_from_darkstore_v2)
                    GROUP BY nm_id, 
                             chrt_id, 
                             office_id, 
                             update_date
                  )
         GROUP BY nm_id, 
                  office_id, 
                  update_date
          ) AS a
FULL JOIN (
           SELECT create_date, darkstore_id, nm_id, price, srid_cnt, 
                  price_taken, srid_taken_cnt, price_rejected, srid_rejected_cnt, 
                  price_cancelled, srid_cancelled_cnt, srid_not_taken_gr1_cnt, 
                  srid_not_taken_gr2_cnt, srid_waiting_cnt
             FROM buffer.nm_from_darkstore_v2
          ) AS b
       ON a.nm_id = b.nm_id
      AND a.darkstore_id = b.darkstore_id
      AND a.create_date = b.create_date
 LEFT ANY 
     JOIN (
           SELECT nm_id, title, parent_id, subject_id, supplier_id_shk, brand_id
             FROM remote_ch3.product_cards_nm FINAL
            WHERE nm_id GLOBAL IN (
                      SELECT nm_id 
                        FROM buffer.nm_from_darkstore_v2
                       UNION ALL
                      SELECT nm_id
                        FROM stage_wh.nm_for_sale_on_date
                       WHERE (office_id, update_date) IN (
                                  SELECT darkstore_id, create_date
                                    FROM buffer.nm_from_darkstore_v2)
                                   )
          ) AS cards
       ON max2(a.nm_id, b.nm_id) = cards.nm_id
   FORMAT MsgPack
"""

INSERT_DATA_DM_BUFFER = """
 TRUNCATE TABLE buffer.nm_from_darkstore
;

   INSERT INTO buffer.nm_from_darkstore
          (create_date, darkstore_id, nm_id, price, srid_cnt, price_taken, srid_taken_cnt, 
           price_rejected, srid_rejected_cnt, price_cancelled, srid_cancelled_cnt, 
           srid_not_taken_gr1_cnt, srid_not_taken_gr2_cnt, srid_waiting_cnt, stock_cnt, 
           title, parent_id, subject_id, parent_name, subject_name, 
           seller_id, seller_name, brand_name, _ver)
   FORMAT MsgPack
"""

FILL_ALL_NM_EVER_DM = """
/* 055: обновляю источник словаря по всем НМ в дарксторе свежими данными из буферки */

   INSERT INTO buffer.all_nm_from_darkstore_ever
               (darkstore_id, nm_id, max_create_date, title, parent_id, 
                subject_id, parent_name, subject_name, seller_id, 
                seller_name, brand_name, appearance_date)

        SELECT tmp.darkstore_id,
               tmp.nm_id,
               tmp.max_create_date,
               tmp.title,
               tmp.parent_id,
               tmp.subject_id,
               tmp.parent_name,
               tmp.subject_name,
               tmp.seller_id,
               tmp.seller_name,
               tmp.brand_name,
               if(isNull(dm.appearance_date), 
                  tmp.appearance_date, 
                  if(dm.appearance_date > tmp.appearance_date,
                      tmp.appearance_date,
                      dm.appearance_date)) AS appearance_date
          FROM datamart.all_nm_from_darkstore_ever AS dm
RIGHT ANY JOIN (
                  SELECT darkstore_id, nm_id, 
                         max(create_date) AS max_create_date,
                         any(title) AS title,
                         any(parent_id) AS parent_id,
                         any(subject_id) AS subject_id,
                         any(parent_name) AS parent_name,
                         any(subject_name) AS subject_name,                         
                         any(seller_id) AS seller_id,
                         any(seller_name) AS seller_name,
                         any(brand_name) AS brand_name,
                         minIfOrNull(
                             create_date, 
                             stock_cnt > 0) AS appearance_date
                    FROM buffer.nm_from_darkstore
                GROUP BY darkstore_id, nm_id
               ) AS tmp
            ON dm.nm_id = tmp.nm_id
           AND dm.darkstore_id = tmp.darkstore_id
         WHERE (isNotNull(tmp.appearance_date) AND isNull(dm.appearance_date))
            OR (tmp.appearance_date < dm.appearance_date)
            OR tuple(tmp.max_create_date,
                     tmp.title,
                     tmp.parent_id,           /* если хоть одно поле поменялось */
                     tmp.subject_id,
                     tmp.seller_id,
                     tmp.seller_name,
                     tmp.brand_name,
                     tmp.parent_name,
                     tmp.subject_name) != tuple(dm.max_create_date,
                                                dm.title,
                                                dm.parent_id,
                                                dm.subject_id,
                                                dm.seller_id,
                                                dm.seller_name,
                                                dm.brand_name,
                                                dm.parent_name,
                                                dm.subject_name)
;

ALTER TABLE buffer.all_nm_from_darkstore_ever
ATTACH PARTITION tuple()
FROM datamart.all_nm_from_darkstore_ever
;

/* некрупная таблица, больше нескольких миллионов строк не наберётся */
OPTIMIZE TABLE buffer.all_nm_from_darkstore_ever FINAL
;

ALTER TABLE datamart.all_nm_from_darkstore_ever
REPLACE PARTITION tuple()
FROM buffer.all_nm_from_darkstore_ever
"""

GET_PARTITIONS_DM = """
   SELECT DISTINCT partition
     FROM system.parts
    WHERE active
      AND database = 'buffer'
      AND table = 'nm_from_darkstore'
 ORDER BY toUInt32(partition)
"""

MANIPULATE_PARTITION_DM = """
    ALTER TABLE buffer.nm_from_darkstore
   ATTACH PARTITION %(quarter)s
     FROM datamart.nm_from_darkstore
;

 OPTIMIZE TABLE buffer.nm_from_darkstore PARTITION %(quarter)s FINAL
;

    ALTER TABLE datamart.nm_from_darkstore
  REPLACE PARTITION %(quarter)s
     FROM buffer.nm_from_darkstore
"""

CH8_FIND_INCOMES = """
DROP TEMPORARY TABLE IF EXISTS incomes;
CREATE TEMPORARY TABLE incomes
ENGINE = MergeTree
ORDER BY tuple()
AS
  SELECT gi_id,
         darkstore_id,
         max(dt) AS max_dt,
         toDate(argMax(supply_dt, dt)) AS supply_date,
         argMin(actual_dt, dt) AS actual_dt,
         toDate(min(create_dt)) AS create_date,
         toDate(min(row_created)) AS min_rc,
         any(supplier_id_shk) AS supplier_id_shk,
         argMax(plan_qty, dt) AS plan_qty,
         argMax(income_qty, dt) AS income_qty,
         argMax(status_goods_id, dt) AS status_goods_id,
         argMax(is_deleted, dt) AS is_deleted
    FROM stage_wh.goods_incomes
   WHERE warehouse_id IN {darkstores}
     AND dt > today() - toIntervalYear(2)
     AND id IN (
                SELECT id
                  FROM stage_wh.goods_incomes
                 WHERE row_created >= '{income_min_cutoff}'
                   AND row_created <= '{income_max_cutoff}'
                   AND warehouse_id IN {darkstores}
                   
                 UNION ALL
                SELECT gi_id
                  FROM stage_wh.preorders
                 WHERE row_created >= '{preorder_min_cutoff}'
                   AND row_created <= '{preorder_max_cutoff}'

                 UNION ALL
                SELECT gi_id
                  FROM stage_wh.supplier_box_barcode
                 WHERE row_created >= '{barcode_min_cutoff}'
                   AND row_created <= '{barcode_max_cutoff}'
               )
GROUP BY warehouse_id AS darkstore_id, 
         id AS gi_id
;

DROP TEMPORARY TABLE IF EXISTS incomes_preorders;
CREATE TEMPORARY TABLE incomes_preorders
ENGINE = MergeTree
ORDER BY tuple()
AS
  SELECT gi_id, groupUniqArray(nm_id) AS nm_ids
    FROM (
           SELECT gi_id, nm_id, is_deleted, dt,
                  max(dt) OVER (PARTITION BY gi_id) AS max_dt_preorder
             FROM stage_wh.preorders
            WHERE gi_id IN (SELECT gi_id FROM incomes)
              AND create_dt >= (SELECT min(create_date) FROM incomes)
         )
   WHERE dt = max_dt_preorder
     AND is_deleted = FALSE
     AND nm_id > 0
GROUP BY gi_id
;

DROP TEMPORARY TABLE IF EXISTS incomes_boxes;
CREATE TEMPORARY TABLE incomes_boxes
ENGINE = MergeTree
ORDER BY tuple()
AS
  SELECT gi_id, box_id
    FROM stage_wh.supplier_box
   WHERE gi_id IN (SELECT gi_id FROM incomes)
     AND dt > (SELECT min(create_date) FROM incomes)
GROUP BY gi_id, box_id
  HAVING argMax(is_deleted, (dt, NOT is_deleted)) = FALSE
;

DROP TEMPORARY TABLE IF EXISTS incomes_barcodes;
CREATE TEMPORARY TABLE incomes_barcodes
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT gi_id, box_id, barcode, amount, max_dt
  FROM (
          SELECT gi_id, box_id, barcode, 
                 dt AS max_dt, amount, is_deleted
            FROM stage_wh.supplier_box_barcode
           WHERE box_id IN (SELECT box_id FROM incomes_boxes)
             AND dt > (SELECT min(create_date) FROM incomes)
        ORDER BY dt DESC, is_deleted
           LIMIT 1 BY gi_id, box_id, barcode
        )
 WHERE is_deleted = FALSE
;

DROP TEMPORARY TABLE IF EXISTS incomes_shk_on_place;
CREATE TEMPORARY TABLE incomes_shk_on_place
ENGINE = MergeTree
ORDER BY tuple()
AS
  SELECT DISTINCT barcode, nm_id
    FROM (
          SELECT barcode, nm_id
            FROM shk_storage.shk_repo
           WHERE barcode IN (SELECT barcode FROM incomes_barcodes)
             AND create_dt > (SELECT min(create_date) 
                                FROM incomes) - toIntervalMonth(6)
           UNION ALL
          SELECT barcode, nm_id
            FROM shk_storage.shk_on_place
           WHERE barcode IN (SELECT barcode FROM incomes_barcodes)
             AND row_created > (SELECT min(create_date) 
                                  FROM incomes) - toIntervalMonth(6)
          )
;

DROP TEMPORARY TABLE IF EXISTS incomes_cards;
CREATE TEMPORARY TABLE incomes_cards
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT nm_id, title, parent_id, subject_id, supplier_id_shk, brand_id, ts
  FROM remote_ch3.product_cards_nm FINAL
 WHERE nm_id GLOBAL IN (SELECT nm_id FROM incomes_shk_on_place
                         UNION ALL
                        SELECT arrayJoin(nm_ids) FROM incomes_preorders)
;

   SELECT gi_id AS gi_id,
          nm_id AS nm_id,
          darkstore_id AS darkstore_id,
          sum(qty) AS qty,
          any(create_date) AS create_date,
          any(supply_date) AS supply_date,
          min(actual_dt) AS actual_dt,
          any(seller_id) AS seller_id,
          any(status_goods_id) AS status_goods_id,
          any(is_deleted) AS is_deleted,
          any(title) AS title,
          any(parent_id) AS parent_id,
          any(subject_id) AS subject_id,
          any(parent_name) AS parent_name,
          any(subject_name) AS subject_name,
          any(brand_name) AS brand_name,
          any(seller_name) AS seller_name,
          now() AS row_created
          
     FROM (
           SELECT gi_id, nm_id, darkstore_id, qty, create_date, supply_date, 
                  actual_dt, seller_id, status_goods_id, is_deleted,
                  cards.title AS title,
                  cards.parent_id AS parent_id,
                  cards.subject_id AS subject_id,
                  cards.brand_id AS brand_id,
                  dictGetOrDefault('dict.subjects', 'parent_name', subject_id, '') as parent_name,
                  dictGetOrDefault('dict.subjects', 'subject_name', subject_id, '') as subject_name,
                  dictGetOrDefault('dict.brands', 'brand_name', cards.brand_id, '') as brand_name,
                  cards.supplier_id_shk AS seller_id_cards,
                  cards.brand_id AS brand_id,
                  cards.ts AS max_created_at,
                  dictGetOrDefault(
                      'dict.suppliers_logistics', 
                      'supplier_name', 
                      seller_id_cards, '') AS seller_name
           FROM (
                    SELECT inc.gi_id AS gi_id,
                           coalesce(sh.nm_id, pre.nm_id) AS nm_id,
                           has(any(pre_all_mn.nm_ids), nm_id) AS was_in_preorder,
                           bx.barcode AS barcode,
                           darkstore_id,
                           any(min_rc) AS create_date,
                           sum(bx.amount) AS qty,
                           any(supply_date) AS supply_date,
                           any(status_goods_id) AS status_goods_id,
                           any(actual_dt) AS actual_dt,
                           any(supplier_id_shk) AS seller_id,
                           any(is_deleted) AS is_deleted
                      FROM incomes AS inc
                 LEFT JOIN (
                               SELECT bx.gi_id AS gi_id, bx.box_id AS box_id,
                                      cd.barcode AS barcode, cd.amount AS amount
                                 FROM incomes_boxes AS bx
                           INNER JOIN incomes_barcodes AS cd
                                   ON bx.box_id = cd.box_id
                                WHERE bx.gi_id = cd.gi_id
                                   OR isNull(cd.gi_id)
                           ) AS bx
                        ON inc.gi_id = bx.gi_id    /* если коробка есть, но нет инфы про её содержимое,
                                                      то буду отображать содержимое из преордерсов. чтобы
                                                      это сделать, INNER JOIN-ом выбрасываю такие коробки */
                 LEFT JOIN incomes_shk_on_place AS sh
                        ON bx.barcode = sh.barcode /* присоединяю все НМ, которые нашлись по баркоду.
                                                      потом, одновременно с обогащением из карточек
                                                      оставлю из всех НМ только одну */
                 LEFT JOIN (
                            SELECT gi_id, toNullable(arrayJoin(nm_ids)) AS nm_id
                              FROM incomes_preorders
                           ) AS pre
                        ON inc.gi_id = pre.gi_id
                       AND bx.box_id = 0           /* присоединяю НМ из преордерсов к поставкам,
                                                      для которых не нашлось коробки с содержимым */
                 LEFT ANY JOIN incomes_preorders AS pre_all_mn
                        ON inc.gi_id = pre_all_mn.gi_id
                       AND bx.box_id != 0          /* присоединяю список НМ из преордерсов,
                                                      чтобы, присоединяя НМ по баркоду, отдать
                                                      предпочтение тем НМ, которые были в преоредере */
                     WHERE isNotNull(coalesce(sh.nm_id, pre.nm_id))
                  GROUP BY darkstore_id,
                           inc.gi_id AS gi_id,
                           barcode,
                           nm_id
                 ) AS a
        LEFT ANY JOIN incomes_cards AS cards
              ON a.nm_id = cards.nm_id
                  /* оставляю только те НМ по баркоду, которые определены за селлером,
                    осуществившем поставку. если у селлера за баркодом закреплено
                    несколько НМ, оставляю либо из преоредов, либо последнюю созданную */
           WHERE (seller_id_cards=0 OR seller_id_cards = a.seller_id)
           ORDER BY was_in_preorder DESC, 
                    max_created_at DESC
           LIMIT 1 BY darkstore_id, gi_id, barcode
           )
           
 GROUP BY darkstore_id, gi_id, nm_id
          /* У одной НМ могут быть разные баркоды. Так что после
             дедупликации по баркодам надо сгруппировать по НМ */
   FORMAT MsgPack
"""

INSERT_DATA_CH4_INCOME_BUFFER = """
TRUNCATE TABLE buffer.nm_income_darkstore
;

INSERT INTO buffer.nm_income_darkstore
    (gi_id, nm_id, was_in_preorder, barcode, darkstore_id, create_date, qty, 
     supply_date, status_goods_id, actual_dt, seller_id, is_deleted)
FORMAT MsgPack
"""

CH4_INCOME_JOIN_CARDS = """
   SELECT gi_id AS gi_id,
          nm_id AS nm_id,
          darkstore_id AS darkstore_id,
          sum(qty) AS qty,
          any(create_date) AS create_date,
          any(supply_date) AS supply_date,
          min(actual_dt) AS actual_dt,
          any(seller_id) AS seller_id,
          any(status_goods_id) AS status_goods_id,
          any(is_deleted) AS is_deleted,
          any(title) AS title,
          any(parent_id) AS parent_id,
          any(subject_id) AS subject_id,
          any(parent_name) AS parent_name,
          any(subject_name) AS subject_name,
          any(brand_name) AS brand_name,
          any(seller_name) AS seller_name,
          now() AS row_created
     FROM (
           SELECT gi_id, nm_id, darkstore_id, qty, create_date, supply_date, actual_dt, seller_id, status_goods_id,
                  is_deleted,
                  dictGetOrDefault('dict.product_cards_nm', 'title', nm_id, '') as title,
                  dictGet('dict.product_cards_nm', 'parent_id', nm_id) as parent_id,
                  dictGet('dict.product_cards_nm', 'subject_id', nm_id) as subject_id,
                  dictGetOrDefault('dict.subjects', 'parent_name', subject_id, '') as parent_name,
                  dictGetOrDefault('dict.subjects', 'subject_name', subject_id, '') as subject_name,
                  dictGet('dict.product_cards_nm', 'brand_id', nm_id) as brand_id,
                  dictGetOrDefault('dict.brands', 'brand_name', brand_id, '') as brand_name,
                  dictGet('dict.product_cards_nm', 'supplier_id_shk', nm_id) as seller_id_cards,
                  dictGet('dict.product_cards_nm', 'ts', nm_id) as max_created_at,
                  dictGetOrDefault('dict.sellers_portal', 'supplier_name', seller_id_cards, '') AS seller_name
           FROM buffer.nm_income_darkstore AS buf               
                   /* оставляю только те НМ по баркоду, которые определены за селлером,
                     осуществившем поставку. если у селлера за баркодом закреплено
                     несколько НМ, оставляю либо из преоредов, либо последнюю созданную */
            WHERE (isNull(seller_id_cards) OR seller_id_cards = buf.seller_id)
            ORDER BY was_in_preorder DESC, max_created_at DESC
            LIMIT 1 BY darkstore_id, gi_id, barcode
           )
 GROUP BY darkstore_id, gi_id, nm_id
          /* У одной НМ могут быть разные баркоды. Так что после
             дедупликации по баркодам надо сгруппировать по НМ */
   FORMAT MsgPack
"""

INSERT_DATA_DM_INCOME_BUFFER = """
TRUNCATE TABLE buffer.nm_income_darkstore
;

  INSERT INTO buffer.nm_income_darkstore
         (gi_id, nm_id, darkstore_id, qty, create_date, supply_date, actual_dt, 
          seller_id, status_goods_id, is_deleted, title, parent_id, subject_id, 
          parent_name, subject_name, brand_name, seller_name, row_created)
  FORMAT MsgPack
"""

GET_INCOME_PARTITIONS_DM = """
   SELECT DISTINCT partition
     FROM system.parts
    WHERE active
      AND database = 'buffer'
      AND table = 'nm_income_darkstore'
 ORDER BY toUInt32(partition)
"""

MANIPULATE_INCOME_PARTITIONS_DM = """
  INSERT INTO buffer.nm_income_darkstore
         (darkstore_id, create_date, nm_id, gi_id, 
          supply_date, actual_dt, is_deleted, row_created)
  SELECT darkstore_id, 
         create_date,
         nm_id, 
         gi_id, 
         supply_date,
         actual_dt,            
         TRUE AS is_deleted, 
         now() AS row_created
    FROM datamart.nm_income_darkstore
   WHERE (darkstore_id, gi_id) IN (
              SELECT darkstore_id, gi_id 
                FROM buffer.nm_income_darkstore)
     AND (darkstore_id, gi_id, nm_id) NOT IN (
              SELECT darkstore_id, gi_id, nm_id
                FROM buffer.nm_income_darkstore)
;

   ALTER TABLE buffer.nm_income_darkstore
  ATTACH PARTITION %(part_name)s
    FROM datamart.nm_income_darkstore
;

OPTIMIZE TABLE buffer.nm_income_darkstore PARTITION %(part_name)s FINAL
;

   ALTER TABLE datamart.nm_income_darkstore
 REPLACE PARTITION %(part_name)s
    FROM buffer.nm_income_darkstore
"""

CH8_GET_CURRENT_STOCK_TO_JOIN = """
SELECT nm_id,
       update_date AS current_date,
       office_id AS darkstore_id,
       quantity AS current_stock
  FROM stage_wh.nm_for_sale_on_date FINAL
 WHERE office_id IN {darkstores}
   AND update_date > today() -2
FORMAT MsgPack
"""

DM_INSERT_CURRENT_STOCK = """
TRUNCATE TABLE buffer.current_stock_darkstore_for_join
;

INSERT INTO buffer.current_stock_darkstore_for_join
       (nm_id, current_date, darkstore_id, current_stock)
FORMAT MsgPack
"""

DM_RENAME_CURRENT_STOCK_TO_JOIN = """
ALTER TABLE datamart.current_stock_darkstore_for_join  
REPLACE PARTITION tuple() 
FROM buffer.current_stock_darkstore_for_join
;

TRUNCATE TABLE buffer.current_stock_darkstore_for_join
"""

LAKE_GET_SPEED = """
TRUNCATE TABLE buffer.speeds_in_darkstore
;

/* 055: сроки для недавних сридов (по отсечке) */
INSERT INTO buffer.speeds_in_darkstore
       (srid, min_kafka_ts, poo_office_id, is_courier, is_delivered, 
        darkstore_id, is_deleted, row_created, measure_code,
        min_ts, max_ts, last_action_id)

  WITH %(darkstores)s AS darkstores,
       toDateTime(%(min_cutoff)s) AS min_cutoff,
       toDateTime(%(max_cutoff)s) AS max_cutoff
       
SELECT srid, 
       min_kafka_ts, 
       poo_office_id, 
       st11 != 0 AS is_courier,
       st0  != 0 AS is_delivered,
       src_office_id AS darkstore_id,
       src_office_id NOT IN darkstores AS is_deleted,
       now() AS row_created,
       measures.1 AS measure_code,
       measures.2 AS min_ts,
       measures.3 AS max_ts,
       measures.4 AS last_action_id
       
  FROM (
SELECT *, arrayJoin(arrayConcat(
                  /* 1 - Cборка заказа (вкл упаковку) */
                  /* 2 - Время с момента окончания сборки заказа до отгрузки из дарка */
                  /* 3 - Скорость доставки до ПВЗ */
                  /* 4 - Скорость доставки до клиента */
                  /* 5 - Время с момента доставки до ПВЗ до фактического получения */
                  /* 255 - Информация о сриде - последний статус и дата */
              if(st1 != 0 AND st3 != 0, 
                 [(1, tupleElement(msq[st1], 'min_ts'), 
                      tupleElement(msq[st3], 'max_ts'), 
                      tupleElement(msq[st3], 'last_action_id'))],
                 []),
              if(st3 != 0 AND st4 != 0, 
                 [(2, tupleElement(msq[st3], 'max_ts'), 
                      tupleElement(msq[st4], 'max_ts'), 
                      tupleElement(msq[st4], 'last_action_id'))],
                 []),
              if(st1 != 0 AND st9 != 0, 
                 [(3, tupleElement(msq[st1], 'min_ts'), 
                      tupleElement(msq[st9], 'max_ts'), 
                      tupleElement(msq[st9], 'last_action_id'))],
                 []),
              if(st1 != 0 AND st0 != 0, 
                 [(4, tupleElement(msq[st1], 'min_ts'), 
                      tupleElement(msq[st0], 'min_ts'), 
                      tupleElement(msq[st0], 'last_action_id'))],
                 []),
              if(st9 != 0 AND st0 != 0, 
                 [(5, tupleElement(msq[st9], 'max_ts'), 
                      tupleElement(msq[st0], 'min_ts'), 
                      tupleElement(msq[st0], 'last_action_id'))],
                 []),
              if(length(tuples) > 0, 
                 [(255, tuples[1].1, last_status_tuple.1, last_status_tuple.2)], 
                 [])
          )) AS measures
  FROM (
SELECT *, arrayFirstIndex(x -> x.step = 1 AND x.office_id = src_office_id, msq) AS st1,
          arrayFirstIndex(x -> x.step = 3 AND x.office_id = src_office_id, msq) AS st3,
          arrayFirstIndex(x -> x.step = 4 AND x.office_id = src_office_id, msq) AS st4,
          arrayLastIndex(x -> x.step = 9  AND x.office_id = poo_office_id, msq) AS st9,
          arrayLastIndex(x -> x.step = 11 AND x.office_id = poo_office_id, msq) AS st11,
          arrayLastIndex(x -> x.step = 0, msq) AS st0
  FROM (
SELECT *, arrayFirstIndex(
              x -> tupleElement(x[1], 'office_id') = poo_office_id AND tupleElement(x[1], 'step') = 0,
              steps_split) AS poo_idx,
          arrayFirstIndex(
              x -> tupleElement(x[1], 'action_id') = 190,  /* отмена заказа */
              steps_split) AS cancel_idx,
          arraySlice(
               arrayMap(
                    sts -> CAST(tuple(
                        tupleElement(sts[1], 'ts') AS min_ts,
                        tupleElement(sts[length(sts)], 'ts') AS max_ts,
                        tupleElement(sts[1], 'step') AS step,
                        tupleElement(sts[1], 'office_id') AS office_id,
                        tupleElement(sts[length(sts)], 'action_id') AS last_action_id
                    ), 'Tuple(min_ts DateTime, max_ts DateTime, step Int8, office_id Int32, last_action_id UInt16)'),
                    steps_split),
                /* всё до последнего шага перед ПВЗ, либо до предпоследнего перед отменой */
               1, nullIf(arrayMin(arrayFilter(x -> x > 0, [poo_idx, cancel_idx -1])), 0)) AS msq
  FROM (
SELECT *, arrayReverseSplit(
               /* сплитую, чтобы последовательности экшенов, отнесённых к одному шагу (step)
                  и произошедших в одном офисе, лежали в одном вложенном массиве */
              (x, y) -> (x.step, x.office_id) != (y.step, y.office_id),
              tuples,
              arrayShiftLeft(tuples, 1)) AS steps_split,
          arrayLast(x -> has(tracking_statuses, x.2), filtered_tuples) AS last_status_tuple
  FROM (
SELECT *, arrayMap(   /* замена некорректных action_id перед определением номера шага */
              (action_id, office, between_actions, between_office) -> multiIf(
                  /* отказной товар прибывает в СЦ (схоже с тем, что выше)
                     либо нет отметок о прибытии-принятии после транспортировки (во избежание 54 меры) */
                  action_id = 220 AND between_actions.1 IN (111, 800, 400), 200,
                  /* когда следующий офис не пвз, буфер отгрузки не до ПВЗ, а до склада (и т.п.) */
                  action_id IN (630, 640, 700, 800) AND between_office.1 != 0 AND between_office.2 = 0, /* не ПВЗ */
                  transform(action_id, [630, 640, 700, 800], [230, 310, 320, 400], action_id),
                  /* если предыдущий статус = 800 и следующий статус из ПВЗ, 400 заменяется на 800 */
                  action_id = 400 AND between_actions.1 = 800 AND between_office.2 = 1, 800,
                  /* прибыл в ПВЗ иногда ошибочно пикают, прибывая в СЦ */
                  action_id = 900 AND office != poo_office_id AND between_actions.2 < 900, 500,
                  action_id),
              actions,
              offices,
              between_distinct_actions,
              between_distinct_offices) AS correct_action_ids,
          arrayMap(   /* здесь каждому экшену соотносится номер шага */
              (tup, correct_action_id, next_distinct_office) -> CAST(
                   tuple(
                       tup.1 AS ts,
                       tup.3 AS office_id,
                       correct_action_id,
                       next_distinct_office.1,
                       transform(
                           correct_action_id,
                           [110, 111, 120,                 /* 1 - оформлен заказ, создан сборончый лист, отправлен на сборку */
                            210, 130,                      /* 3 - собран на складе или складе МП */
                            800,                           /* 4 - погружен в машину до ПВЗ */
                            900, 910, 1000,                /* 9 - ПВЗ: коробка прибыла на ПВЗ, приемка коробки, выложена на полку */
                            1030, 1040, 1050, 1035,        /* 0 - ПВЗ: финальные статусы, говорящие о том, что товар прибыл к клиенту */
                            1010                           /* 11 - Выдано курьеру */
                            ],
                           [1,1,1,
                            3,3,
                            4,
                            9,9,9,
                            0,0,0,0,
                            11
                            ],
                           127  /* max Int8 */
                       ) AS action_id),
                   'Tuple(ts DateTime, office_id Int32, action_id UInt16, next_distinct_office Int32, step Int8)'),
              filtered_tuples,
              correct_action_ids,
              between_distinct_offices) AS tuples
  FROM (
SELECT *, arrayMap(x -> x.2, filtered_tuples) AS actions,
          arrayMap(x -> x.3, filtered_tuples) AS offices,
          arrayFlatten(
              arrayMap(   /* ближайший и предыдущий экшен, которые отличаются от текущего */
                  (gr, prev_gr, next_gr) -> arrayWithConstant(length(gr), tuple(prev_gr[1], next_gr[1])),
                  arraySplit((x, next) -> next != 0, actions, arrayDifference(actions)) AS actions_split,
                  arrayShiftRight(actions_split, 1),
                  arrayShiftLeft(actions_split, 1))) AS between_distinct_actions,
          arrayFlatten(
              arrayMap(   /* ближайший офис, и является ли он ПВЗ, и предпоследний офис */
                  (gr, next_gr, prev_gr) -> arrayWithConstant(
                          length(gr), tuple(next_gr[1], next_gr[1] = poo_office_id, prev_gr[1])),
                  arraySplit((x, next) -> next != 0, offices, arrayDifference(offices)) AS offices_split,
                  arrayShiftLeft(offices_split, 1),
                  arrayShiftRight(offices_split, 1))) AS between_distinct_offices
  FROM (
SELECT *, /* некоторые статусы никак и нигде не нужно учитывать. их проще сразу исключить */
          arrayFilter(
              (x, next_action, prev_action) -> NOT ( FALSE
                  /* 220, 610 и 1080 статус из офиса, который отличается от предыдущего и от следующего.
                     эти статусы попадались не на своём месте
                     (220 - '37611816097264545.0.0', 1500 - '11267324098566732.0.0') */
                  OR (x.2 IN (220, 610, 1080, 1500)
                          AND x.3 != next_action.3
                          AND x.3 != prev_action.3
                          AND next_action.3 != 0)
                  /* 610, если предыдущий статус из того же офиса < 310 (Закрыли полибокс). во избежание 36, 16... мер */
                  OR (x.2 = 610
                          AND prev_action.2 < 310
                          AND prev_action.3 = x.3)
                  /* 400 и 800, если до и после него статусы на том же офисе и след. экшен не отметка о прибытии */
                  OR (x.2 IN (400, 800)
                          AND x.3 = next_action.3
                          AND x.3 = prev_action.3
                          AND next_action.2 NOT IN (500, 610, 900, 910))
                  /* исключаю повторные экшены шага 1*. У первых экшенов нет никакого предыдущего экшена */
                  OR (x.2 IN (110, 111, 120)
                          AND prev_action.2 != 0)
                  /* ошибочные 900-е. перед ними экшен <630 и следующий статус не из ПВЗ */
                  OR (x.2 = 900
                          AND prev_action.2 < 630
                          AND next_action.3 != 0
                          AND next_action.3 != x.3)
                  ),
              arraySlice(raw_tuples, f_step_1) AS _raw_tuples,
              arrayShiftLeft(_raw_tuples, 1),
              arrayShiftRight(_raw_tuples, 1)) AS filtered_tuples
  FROM (
         SELECT srid,   /* группировка по сриду (дальше не меняется) */
                toDate(min(_kafka_timestamp)) AS min_kafka_ts,  /* для партиционирования срида */
                /* ищу первый ПВЗ, в котором пикнули экшены нулевого шага, либо последний по ts */
                argMinIfOrNull(office_id, ts, action_id IN (1000, 1030, 1040, 1010)) AS first_reached_dst,
                groupArray(dst_office_id) AS all_dst,
                argMax(dst_office_id, ts) AS last_dst,
                ifNull(if(has(all_dst, first_reached_dst), first_reached_dst, last_dst), 0) AS poo_office_id,
                coalesce(
                    dictGetOrNull('dict.branch_office', 'main_office_id',
                        argMinIf(office_id, ts, action_id IN (110, 121, 130, 140, 210, 220)) AS src_off_raw),
                    src_off_raw) AS src_office_id,
                [1000, 900, 800, 700, 630, 640, 210, 190, 110, 1030, 1035, 1010] AS tracking_statuses,
                arrayCompact(
                    arraySort(
                        groupArray(
                            tuple(
                                ts + toIntervalHour(3),
                                action_id,
                                coalesce(
                                    dictGetOrNull(
                                        'dict.branch_office', 
                                        'main_office_id', 
                                        office_id),
                                    office_id
                                ))))) AS raw_tuples,
                arrayFirstIndex(
                    x -> x.2 IN (110, 111, 120),
                    raw_tuples) AS f_step_1
           FROM core_wh.srid_tracker
          WHERE ts > now() - toIntervalYear(2)
            AND srid IN (     /* если нижняя отсечка больше чем месячной давности,
                                 вместо рц-таблицы иду сканить основную */
                         SELECT srid
                           FROM core_wh.srid_tracker_rc
                          WHERE ((action_id IN (110, 121, 130, 140, 210, 220) 
                                     AND office_id IN darkstores)
                                 OR srid IN (SELECT srid 
                                               FROM datamart.speeds_in_darkstore
                                              WHERE NOT is_delivered))
                                    /* либо срид в дарксторе как в офисе сборки, либо срид 
                                       уже в витрине по дарксторам, но не дошёл до меры 0.
                                       иначе слишком много сридов подходит в группировке,
                                       и группировка становится тяжёлой. витрина по дарксторам 
                                       это реплейс, который не оптимайзится - поэтому будут
                                       попадаться несхлопнутые сроки для уже закрытых сридов */
                            AND row_created >= min_cutoff
                            AND row_created <= max_cutoff
                            AND min_cutoff > now() - toIntervalMonth(1)

                          UNION ALL
                         SELECT srid
                           FROM core_wh.srid_tracker
                          WHERE ((action_id IN (110, 121, 130, 140, 210, 220) 
                                     AND office_id IN darkstores)
                                 OR srid IN (SELECT srid 
                                               FROM datamart.speeds_in_darkstore
                                              WHERE NOT is_delivered))
                            AND row_created >= min_cutoff
                            AND row_created <= max_cutoff
                            AND min_cutoff <= now() - toIntervalMonth(1)
                        )
       GROUP BY srid
         HAVING max(payment_type LIKE 'S__') = 0    /* не берём возвратные */
  ))))))))
"""

LAKE_GET_SPEEDS_PARTITIONS = """
  SELECT DISTINCT partition
    FROM cluster('lake_m', system.parts)
   WHERE database = 'buffer'
     AND active = 1
     AND table = 'speeds_in_darkstore'
"""

LAKE_ATTACH_SPEEDS_PARTITIONS_FROM_BUFFER = """
   ALTER TABLE datamart.speeds_in_darkstore
  ATTACH PARTITION %(part_name)s
    FROM buffer.speeds_in_darkstore
"""

LAKE_CALC_ORDER_UIDS_GLOBAL = """
  INSERT INTO buffer.order_uids_from_darkstore_d
         (darkstore_id, order_uid, sm_id, create_dt, 
          nm_cnt, shk_cnt, row_created) 

  SELECT darkstore_id,
         order_uid,
         nullIf(topK(1)(sm_id)[1], 0) AS sm_id,
         toDate(min(create_dt)) AS create_dt,
         uniqExact(nm_id) AS nm_cnt,
         uniqExact(srid) AS shk_cnt,
         now() AS row_created
    FROM datamart.srids_from_darkstore_d AS src FINAL 
   WHERE is_deleted = False
     AND order_uid GLOBAL IN (
             SELECT order_uid
               FROM datamart.srids_from_darkstore_d
              WHERE row_created >= %(sr_min_cutoff)s
                AND row_created <= %(sr_max_cutoff)s)
     AND src.create_dt > (
             SELECT min(create_dt) - toIntervalDay(14)
               FROM datamart.srids_from_darkstore_d
              WHERE row_created >= %(sr_min_cutoff)s
                AND row_created <= %(sr_max_cutoff)s)
GROUP BY darkstore_id, order_uid
"""

LAKE_GET_ORDER_UIDS_PARTITIONS = """
  SELECT DISTINCT partition
    FROM cluster('lake_m', system.parts)
   WHERE database = 'buffer'
     AND active = 1
     AND table = 'order_uids_from_darkstore'
"""

LAKE_ATTACH_ORDER_UIDS_PARTITIONS_FROM_BUFFER = """
   ALTER TABLE datamart.order_uids_from_darkstore
  ATTACH PARTITION %(part_name)s
    FROM buffer.order_uids_from_darkstore
"""

LAKE_GET_RECENT_PARTITIONS_FROM_DATAMART_TABLES = """
/* к партициям из буферки, заполненной только что, добавляю партиции 
   из скоростей. скорости заполняются каждый час */

  SELECT toYYYYMM(create_dt)
    FROM buffer.order_uids_from_darkstore_d
   UNION DISTINCT
  SELECT toYYYYMM(max_ts)
    FROM datamart.speeds_in_darkstore_d
   WHERE row_created >= %(sp_min_cutoff)s
     AND row_created <= %(sp_max_cutoff)s    
"""

LAKE_OPERATIONAL_BY_YYYYMM = """
   SELECT create_dt, darkstore_id, srid_cnt_poo, srid_cnt_courier, 
          duration_poo, duration_courier, nm_cnt, shk_cnt, nm_cnt_poo, 
          shk_cnt_poo, nm_cnt_courier, shk_cnt_courier
     FROM (
             SELECT create_dt,
                    darkstore_id,
                    countResampleIf(1, 6, 1)(
                        measure_code, NOT is_courier) AS srid_cnt_poo,
                    sumResampleIf(1, 6, 1)(
                        max_ts - min_ts, measure_code, NOT is_courier) AS duration_poo,
                    countResampleIf(1, 6, 1)(
                        measure_code, is_courier) AS srid_cnt_courier,
                    sumResampleIf(1, 6, 1)(
                        max_ts - min_ts, measure_code, is_courier) AS duration_courier
               FROM datamart.speeds_in_darkstore_d FINAL
              WHERE toYYYYMM(max_ts) = {yyyymm}
                AND is_deleted = False
           GROUP BY darkstore_id, 
                    toDate(max_ts) AS create_dt
           ) AS speeds
GLOBAL FULL JOIN (
             SELECT create_dt,
                    darkstore_id,
                    toUInt32(round(avgOrNull(src.nm_cnt) * 1000)) AS nm_cnt,
                    toUInt32(round(avgOrNull(src.shk_cnt) * 1000)) AS shk_cnt,
                    toUInt32(round(avgIfOrNull(src.nm_cnt, sm_id=6) * 1000)) AS nm_cnt_poo,
                    toUInt32(round(avgIfOrNull(src.shk_cnt, sm_id=6) * 1000)) AS shk_cnt_poo,
                    toUInt32(round(avgIfOrNull(src.nm_cnt, sm_id=5) * 1000)) AS nm_cnt_courier,
                    toUInt32(round(avgIfOrNull(src.shk_cnt, sm_id=5) * 1000)) AS shk_cnt_courier
               FROM datamart.order_uids_from_darkstore_d AS src FINAL
              WHERE toYYYYMM(create_dt) = {yyyymm}
           GROUP BY darkstore_id, 
                    create_dt
          ) AS uids
    USING (darkstore_id, create_dt)
 ORDER BY darkstore_id, create_dt
WITH FILL FROM YYYYMMDDToDate({yyyymm} *100 +1)
            TO YYYYMMDDToDate({yyyymm} *100 +1) + toIntervalMonth(1)
   FORMAT MsgPack
"""

DM_OPERATIONAL_BY_YYYYMM_INSERT = """
TRUNCATE TABLE buffer.speeds_in_darkstore;

INSERT INTO buffer.speeds_in_darkstore
       (create_dt, darkstore_id, srid_cnt_poo, srid_cnt_courier, 
        duration_poo, duration_courier, nm_cnt, shk_cnt, nm_cnt_poo, 
        shk_cnt_poo, nm_cnt_courier, shk_cnt_courier)
FORMAT MsgPack
"""

DM_OPERATIONAL_REPLACE_PARTITION = """
ALTER TABLE datamart.speeds_in_darkstore 
REPLACE PARTITION %(yyyymm)s
FROM buffer.speeds_in_darkstore
"""

LAKE_TRUNCATE_SRIDS_LIST_45TH_TAB = """
TRUNCATE TABLE buffer.srids_for_recent_order_uids
"""

LAKE_GET_SRIDS_LIST_FOR_RECENT_ORDER_UIDS = """
/* hourly таски (nm и speeds) инкрементально заполняют датамарт-таблицы на лейках.
   with_cutoff может (особенно при перезапусках) запустить их несколько раз
   с разными отсечками, из-за чего буферки nm и speeds будут перезатираться - на их
   содержимое не следует полагаться. Поэтому забираю сриды по всем ордер уидам, 
   в которых есть хотя бы один срид, изменившийся с последнего запуска 
   этой hourly-таски. Их и будем актуализировать и переносить на dm3 */

  INSERT INTO buffer.srids_for_recent_order_uids_d 
         (srid, create_date)

    WITH (SELECT min(min_dt)
            FROM (SELECT min(min_kafka_ts) AS min_dt
                    FROM datamart.speeds_in_darkstore_d
                   WHERE row_created BETWEEN %(speeds_min_cutoff)s AND %(speeds_max_cutoff)s
                   UNION ALL
                  SELECT min(create_date) AS min_dt
                    FROM datamart.srids_from_darkstore_d
                   WHERE row_created BETWEEN %(srids_min_cutoff)s AND %(srids_max_cutoff)s                             
                 )) AS min_product_dt_in_recent_srids
  SELECT srid, create_date
    FROM datamart.srids_from_darkstore_d
   WHERE order_uid GLOBAL IN (
             SELECT order_uid 
               FROM datamart.srids_from_darkstore_d
              WHERE row_created BETWEEN %(srids_min_cutoff)s AND %(srids_max_cutoff)s
                 OR (create_date > min_product_dt_in_recent_srids - toIntervalMonth(1)
                     AND srid IN (SELECT srid 
                                    FROM datamart.speeds_in_darkstore_d
                                   WHERE row_created BETWEEN %(speeds_min_cutoff)s AND %(speeds_max_cutoff)s))
                             )
     AND create_date > min_product_dt_in_recent_srids - toIntervalMonth(6)
SETTINGS parallel_distributed_insert_select=2,
         distributed_product_mode='local'
"""

LAKE_GET_SRIDS_SPEEDS_FOR_RECENT_ORDER_UIDS = """
TRUNCATE TABLE buffer.srids_speeds_recent_batch
;

INSERT INTO buffer.srids_speeds_recent_batch 
       (srid, max_ts, min_ts, measure_code, 
        poo_office_id, last_action_id, is_deleted)

SELECT srid, max_ts, min_ts, measure_code, 
       poo_office_id, last_action_id, is_deleted
  FROM datamart.speeds_in_darkstore FINAL
 WHERE srid IN (SELECT srid FROM buffer.srids_for_recent_order_uids)
   AND min_kafka_ts > (SELECT min(create_date)
                         FROM buffer.srids_for_recent_order_uids
                       ) - toIntervalMonth(3)
"""

LAKE_GET_ORDER_UIDS_45TH_TAB = """
/* 055: джойню сриды со скоростями по сридам, чтобы получить данные для 4й и 5й вкладки */
   SELECT darkstore_id,
          order_uid,
          src.srid AS srid,
          any(src.nm_id) AS nm_id,
          ifNull(
            (dictGet(
                'dict.product_cards_nm',
                ('title', 'parent_id', 'subject_id'),
                nm_id
            ) AS cards).1, 
            '') AS title,
          cards.2 AS parent_id,
          (dictGet(
              'dict.subjects',
              ('parent_name', 'subject_name'),
              cards.3) AS subjects
          ).1 AS parent_name,
          cards.3 AS subject_id,
          subjects.2 AS subject_name,
          any(price) AS price,  /* sp приджойнен с произведением строк на кол-во мер в каждой. поэтому не sum */
          coalesce(
              /* предпочтительнее из срид-трекера, потому что
                 отсчёт сроков доставки именно по срид-трекеру */
              anyIf(sp.min_ts, sp.measure_code=255),
              any(create_dt)) AS create_dt,
          any(coalesce(src.poo_office_id, sp.poo_office_id)) AS poo_office_id,
          uniqExact(srid) OVER (PARTITION BY order_uid) AS sold_cnt,
          toInt64(round(anyIf(sp.max_ts - sp.min_ts, sp.measure_code=1))) AS avg_duration_m1,
          toInt64(round(anyIf(sp.max_ts - sp.min_ts, sp.measure_code=2))) AS avg_duration_m2,
          toInt64(round(any(
              coalesce(speeds_3.max_ts, speeds_4.max_ts)
              - coalesce(speeds_3.min_ts, speeds_4.min_ts)))) AS avg_duration_m3,
          coalesce(
              argMax(sm_id, coalesce(speeds_3.max_ts, speeds_4.max_ts, sp.max_ts)),
              any(sm_id)) AS sm_id,
          anyIf(sp.max_ts, sp.measure_code=255) AS last_action_id_dt,
          anyIf(sp.last_action_id, sp.measure_code=255) AS last_action_id,
          any(src.is_deleted) AS is_deleted,
          now() AS row_created
     FROM (SELECT *
             FROM datamart.srids_from_darkstore_d FINAL
            WHERE srid IN (SELECT srid FROM buffer.srids_for_recent_order_uids_d)
              AND create_date > (SELECT min(create_date)
                                   FROM buffer.srids_for_recent_order_uids_d
                                ) - toIntervalMonth(6)
          ) AS src
 LEFT ANY 
     JOIN (SELECT srid, max_ts, min_ts
             FROM buffer.srids_speeds_recent_batch_d
            WHERE measure_code = 3
              AND is_deleted = False) AS speeds_3        /* 3 - до ПВЗ */
       ON src.srid = speeds_3.srid
      AND src.sm_id != 5
 LEFT ANY 
     JOIN (SELECT srid, max_ts, min_ts
             FROM buffer.srids_speeds_recent_batch_d 
            WHERE measure_code = 4
              AND is_deleted = False) AS speeds_4       /* 4 - до клиента */
       ON src.srid = speeds_4.srid
      AND src.sm_id = 5
LEFT JOIN (SELECT srid, max_ts, min_ts, measure_code, poo_office_id, last_action_id
             FROM buffer.srids_speeds_recent_batch_d 
            WHERE measure_code NOT IN (3, 4)
              AND is_deleted = False) AS sp   /* остальные меры */
       ON src.srid = sp.srid        
 GROUP BY darkstore_id,
          order_uid,
          src.srid
 SETTINGS distributed_product_mode='local',
          join_use_nulls=1
   FORMAT MsgPack
"""

INSERT_ORDER_UIDS_45TH_TAB_DM3 = """
TRUNCATE TABLE buffer.order_uids_from_darkstore_detailed 
;
  INSERT INTO buffer.order_uids_from_darkstore_detailed 
         (darkstore_id, order_uid, srid, nm_id, title, parent_id, 
          parent_name, subject_id, subject_name, price, create_dt,
          poo_office_id, sold_cnt, avg_duration_m1, avg_duration_m2, 
          avg_duration_m3, sm_id, last_action_id_dt, last_action_id,
          is_deleted, row_created)
  FORMAT MsgPack
"""

GET_ORDER_UIDS_PARTITIONS_45TH_TAB_DM3 = """
  SELECT DISTINCT partition
    FROM system.parts
   WHERE active = 1
     AND database = 'buffer'
     AND table = 'order_uids_from_darkstore_detailed'
"""

MANIPULATE_PARTITIONS_45TH_TAB_DM3 = """
CREATE TEMPORARY TABLE order_uids_from_darkstore_4th_tab
AS datamart.order_uids_from_darkstore
ENGINE = ReplacingMergeTree(row_created)
PARTITION BY toYYYYMM(create_dt)
ORDER BY (darkstore_id, nm_id, order_uid)
PRIMARY KEY (darkstore_id, nm_id)
;

  INSERT INTO order_uids_from_darkstore_4th_tab
         (darkstore_id, order_uid, nm_id, sold_cnt, price, title, 
          parent_id, parent_name, subject_id, subject_name, create_dt, 
          sm_id, avg_duration, is_deleted, row_created)
  SELECT darkstore_id, 
         order_uid, 
         nm_id, 
         countIf(srid, NOT src.is_deleted) AS sold_cnt, 
         sumIf(price, NOT src.is_deleted) AS price, 
         any(title) AS title, 
         any(parent_id) AS parent_id, 
         any(parent_name) AS parent_name, 
         any(subject_id) AS subject_id, 
         any(subject_name) AS subject_name, 
         minIf(create_dt, NOT src.is_deleted) AS create_dt, 
         argMaxIf(
             sm_id, 
             coalesce(last_action_id_dt, toDateTime(0)), 
             NOT src.is_deleted) AS sm_id, 
         toInt64(round(avgIf(avg_duration_m3, NOT src.is_deleted))) AS avg_duration, 
         sum(src.is_deleted) = count() AS is_deleted, 
             /* если все сриды удалены, заказ тоже */
         any(row_created) AS row_created
    FROM buffer.order_uids_from_darkstore_detailed AS src
   WHERE toYYYYMM(src.create_dt) = %(part_name)s
GROUP BY darkstore_id,
         order_uid,
         nm_id   
;
   ALTER TABLE order_uids_from_darkstore_4th_tab
  ATTACH PARTITION %(part_name)s
    FROM datamart.order_uids_from_darkstore
;
OPTIMIZE TABLE order_uids_from_darkstore_4th_tab PARTITION %(part_name)s FINAL
;
   ALTER TABLE datamart.order_uids_from_darkstore
 REPLACE PARTITION %(part_name)s
    FROM order_uids_from_darkstore_4th_tab
;
   ALTER TABLE buffer.order_uids_from_darkstore_detailed
  ATTACH PARTITION %(part_name)s
    FROM datamart.order_uids_from_darkstore_detailed
;
OPTIMIZE TABLE buffer.order_uids_from_darkstore_detailed PARTITION %(part_name)s FINAL
;
   ALTER TABLE datamart.order_uids_from_darkstore_detailed
 REPLACE PARTITION %(part_name)s
    FROM buffer.order_uids_from_darkstore_detailed
"""


@with_db(LAKE_CONN, 'lake')
@with_cutoff(
    ('datamart.srids_from_darkstore_oof', lambda x: 'positions.oof_position_status_v3' + GET_RC(x), 'row_created', 'lake', 'oof', MAX_RECORDS))
def nm_from_darkstore_hourly(lake_hook, oof_min_cutoff, oof_max_cutoff):

    # поиск недавних сридов, заполняю реплейсинг (буферку) по сридам.
    # буферка нужна для таски, заполняющей 4-ую вкладку
    lake_hook.on_cluster(
        lake_hook.exec_with_log, 
        LAKE_CALC_DATA,
        parameters=dict(
            min_cutoff_oof=oof_min_cutoff, 
            max_cutoff_oof=oof_max_cutoff,
            blocked_payment_types=BLOCKED_PAYMENT_TYPES,
            blocked_when_not_paid_ordo=BLOCKED_WHEN_NOT_PAID_ORDO,
            darkstores=DARKSTORES))
    
    # из буферки по сридам переношу данные в датамарт
    partitions = lake_hook.get_records(GET_SRIDS_PARTITIONS_LAKE)
    for part_name in partitions:
        lake_hook.on_cluster(
            lake_hook.exec_with_log, 
            MANIPULATE_SRIDS_PARTITION_LAKE,
            parameters=dict(part_name=part_name[0]))


@with_db(DM_CONN, 'dm3')
@with_cutoff(('datamart.srids_from_darkstore_daily_for_nm', 'datamart.srids_from_darkstore_d', 'row_created', LAKE_CONN, 'daily', MAX_RECORDS, LAKE_CONN, '', -1))
def nm_from_darkstore_daily(dm3_hook, daily_min_cutoff, daily_max_cutoff):

    # считаю и переношу продажи с ч4 на ч8
    copy_ch_to_ch_pipe(
        take_data=LAKE_CALC_GET_GENERAL_DATA.format(
            daily_min_cutoff=daily_min_cutoff,
            daily_max_cutoff=daily_max_cutoff),
        insert_data=INSERT_DATA_CH8_BUFFER,
        src_ch=LAKE_CONN,
        dst_ch=CH8_CONN,
        multiquery=True)
    
    # добавляю НМ, которые есть на хранении, но не проданы. переношу в датамарт
    copy_ch_to_ch_pipe(
        take_data=ATTACH_DEAD_STOCK_CH8,
        insert_data=INSERT_DATA_DM_BUFFER,
        src_ch=CH8_CONN,
        dst_ch=DM_CONN,
        multiquery=True)
    
    # обновляю словарь по всем НМ в дарксторе
    dm3_hook.exec_with_log(FILL_ALL_NM_EVER_DM)

    # по одной партиции вставляю обновлённые данные в витрину
    partitions = dm3_hook.get_records(GET_PARTITIONS_DM)
    for quarter in partitions:
        dm3_hook.exec_with_log(
            MANIPULATE_PARTITION_DM, 
            parameters=dict(quarter=quarter[0]))
        

@with_db(DM_CONN, 'dm3')
@with_cutoff(
    ('datamart.nm_income_darkstore',          'stage_wh.goods_incomes',        'row_created', CH8_CONN, 'income',   MAX_RECORDS),
    ('datamart.nm_income_darkstore_preorder', 'stage_wh.preorders',            'row_created', CH8_CONN, 'preorder', MAX_RECORDS),
    ('datamart.nm_income_darkstore_barcode',  'stage_wh.supplier_box_barcode', 'row_created', CH8_CONN, 'barcode',  MAX_RECORDS))
def nm_income_darkstore_daily(dm3_hook,
        income_min_cutoff,   income_max_cutoff, 
        preorder_min_cutoff, preorder_max_cutoff, 
        barcode_min_cutoff,  barcode_max_cutoff):  

    # переношу информацию по недавним поставкам на ч4 для обогащения  
    QUERY = CH8_FIND_INCOMES.format(
        income_min_cutoff=income_min_cutoff,
        income_max_cutoff=income_max_cutoff,
        barcode_min_cutoff=barcode_min_cutoff,
        barcode_max_cutoff=barcode_max_cutoff,
        preorder_min_cutoff=preorder_min_cutoff,
        preorder_max_cutoff=preorder_max_cutoff,
        darkstores=DARKSTORES)
    copy_ch_to_ch_pipe(
        take_data=QUERY,
        insert_data=INSERT_DATA_DM_INCOME_BUFFER,
        src_ch=CH8_CONN,
        dst_ch=DM_CONN,
        multiquery=True)
    
    # по одной партиции вставляю обновлённые данные в витрину
    partitions = dm3_hook.get_records(GET_INCOME_PARTITIONS_DM)
    for part_name in partitions:
        dm3_hook.exec_with_log(
            MANIPULATE_INCOME_PARTITIONS_DM, 
            parameters=dict(part_name=part_name[0]))
        
    # переношу в датамарт остаток по НМ за последние два дня
    QUERY = CH8_GET_CURRENT_STOCK_TO_JOIN.format(darkstores=DARKSTORES)
    copy_ch_to_ch_pipe(
        take_data=QUERY,
        insert_data=DM_INSERT_CURRENT_STOCK,
        src_ch=CH8_CONN,
        dst_ch=DM_CONN,
        multiquery=True)
    dm3_hook.exec_with_log(DM_RENAME_CURRENT_STOCK_TO_JOIN)
    

@with_db(LAKE_CONN, 'lake')
@with_cutoff(
    ('datamart.speeds_in_darkstore_v2', lambda x: 'core_wh.srid_tracker' + GET_RC(x), 'row_created', 'lake', 'st', MAX_RECORDS))  
def speeds_in_darkstore_hourly(lake_hook, st_min_cutoff, st_max_cutoff):

    # рассчитываю меры для сридов, изменившихся с последнего запуска
    lake_hook.on_cluster(
        lake_hook.exec_with_log,
        LAKE_GET_SPEED,
        parameters=dict(
            min_cutoff=st_min_cutoff,
            max_cutoff=st_max_cutoff,
            darkstores=DARKSTORES))

    # перекладываю партиции из буферки в основную таблицу по мерам
    partitions = lake_hook.get_records(LAKE_GET_SPEEDS_PARTITIONS)
    for part_name in partitions:
        lake_hook.on_cluster(
            lake_hook.exec_with_log,
            LAKE_ATTACH_SPEEDS_PARTITIONS_FROM_BUFFER, 
            parameters=dict(part_name=part_name[0]))


@with_db(DM_CONN, 'dm')
@with_db(LAKE_CONN, 'lake')
@with_cutoff(
    ('datamart.srids_from_darkstore_daily_for_sp', 'datamart.srids_from_darkstore_d', 'row_created', 'lake', 'sr', MAX_RECORDS, 'lake', '', -1),
    ('datamart.speeds_in_darkstore_daily_for_sp',  'datamart.speeds_in_darkstore_d',  'row_created', 'lake', 'sp', MAX_RECORDS, 'lake', '', -1))
def speeds_in_darkstore_daily(dm_hook, lake_hook,
        sr_min_cutoff,   sr_max_cutoff,
        sp_min_cutoff,   sp_max_cutoff):  
    
    # заполняю буферку по недавним order_uid (для правой половины 3ей вкладки)
    lake_hook.exec_with_log(
        LAKE_CALC_ORDER_UIDS_GLOBAL, 
        parameters=dict(
            sr_min_cutoff=sr_min_cutoff,
            sr_max_cutoff=sr_max_cutoff))
    
    # перекладываю партиции из буферки в основную таблицу по order_uid
    partitions = lake_hook.get_records(LAKE_GET_ORDER_UIDS_PARTITIONS)
    for part_name in partitions:
        lake_hook.on_cluster(
            lake_hook.exec_with_log,
            LAKE_ATTACH_ORDER_UIDS_PARTITIONS_FROM_BUFFER, 
            parameters=dict(part_name=part_name[0]))

    # по одной партиции обрабатываю и переношу в витрину на dm3
    partitions = lake_hook.get_records(
        LAKE_GET_RECENT_PARTITIONS_FROM_DATAMART_TABLES, 
        parameters=dict(
            sp_min_cutoff=sp_min_cutoff,
            sp_max_cutoff=sp_max_cutoff))
    
    for yyyymm in partitions:
        copy_ch_to_ch_pipe(
            take_data=LAKE_OPERATIONAL_BY_YYYYMM.format(yyyymm=yyyymm[0]),
            insert_data=DM_OPERATIONAL_BY_YYYYMM_INSERT,
            src_ch=LAKE_CONN,
            dst_ch=DM_CONN,
            multiquery=True) 
        dm_hook.exec_with_log(
            DM_OPERATIONAL_REPLACE_PARTITION, 
            parameters=dict(yyyymm=yyyymm[0]))


@with_db(DM_CONN, 'dm3')
@with_db(LAKE_CONN, 'lake')
@with_cutoff(
    ('datamart.srids_from_darkstore_hourly', 'datamart.srids_from_darkstore_d', 'row_created', 'lake', 'srids',  MAX_RECORDS, 'lake', '', -1),
    ('datamart.speeds_in_darkstore_hourly',  'datamart.speeds_in_darkstore_d',  'row_created', 'lake', 'speeds', MAX_RECORDS, 'lake', '', -1))
def order_uids_from_darkstore_hourly(dm3_hook, lake_hook, 
        speeds_min_cutoff, speeds_max_cutoff,
        srids_min_cutoff,  srids_max_cutoff):

    # транкейт буферки, куда собираю все сриды, относящиеся к нужным order_uid
    lake_hook.on_cluster(
        lake_hook.exec_with_log,
        LAKE_TRUNCATE_SRIDS_LIST_45TH_TAB)
    
    # формирую список сридов (ордер_уидов), которые необходимо обновить на 4 и 5 вкладках
    lake_hook.exec_with_log(
        LAKE_GET_SRIDS_LIST_FOR_RECENT_ORDER_UIDS, 
        parameters=dict(
            speeds_min_cutoff=speeds_min_cutoff,
            speeds_max_cutoff=speeds_max_cutoff,
            srids_min_cutoff=srids_min_cutoff,  
            srids_max_cutoff=srids_max_cutoff))

    # забираю схлопнутую информацию из таблицы скоростей по сридам
    lake_hook.on_cluster(
        lake_hook.exec_with_log,
        LAKE_GET_SRIDS_SPEEDS_FOR_RECENT_ORDER_UIDS)

    # пересчитываю метрики по всем сридам, изменившимся с последнего раза, и переношу на дм3
    copy_ch_to_ch_pipe(
        take_data=LAKE_GET_ORDER_UIDS_45TH_TAB,
        insert_data=INSERT_ORDER_UIDS_45TH_TAB_DM3,
        src_ch=LAKE_CONN,
        dst_ch=DM_CONN,
        multiquery=True)

    # по одной партиции вставляю обновлённые данные в витрины
    partitions = dm3_hook.get_records(GET_ORDER_UIDS_PARTITIONS_45TH_TAB_DM3)
    for part_name in partitions:
        dm3_hook.exec_with_log(
            MANIPULATE_PARTITIONS_45TH_TAB_DM3, 
            parameters=dict(part_name=part_name[0]))


@provide_session
def skip_daily_tasks(logical_date, session):
    last_tin = session.query(TaskInstance)\
        .filter(TaskInstance.dag_id == 'lake_nm_from_darkstore',\
                TaskInstance.task_id == 'nm_from_darkstore_daily_task',\
                TaskInstance.state.in_((State.SUCCESS, State.RUNNING)))\
        .order_by(TaskInstance.start_date.desc())\
        .first()
    max_date = last_tin.execution_date if last_tin else logical_date
    return ((logical_date - max_date).days > 0) or (logical_date.hour == 2)


with DAG(
        dag_id='lake_nm_from_darkstore',
        description=DESCRIPTION,
        start_date=datetime(2024, 7, 1),
        schedule='50 * * * *',
        catchup=False,
        tags=[TELEGA, CH8_CONN, LAKE_CONN, DM_CONN],
        max_active_runs=1,
        default_args=dict(
            owner='kravcov.artemiy', 
            telegram=[TELEGA], 
            email_on_failure=True,
            retries=2,
            retry_delay=timedelta(minutes=10)),
        ) as dag:

    nm_from_darkstore_hourly_task = PythonOperator(
        pool=get_pool(LAKE_CONN),
        task_id='nm_from_darkstore_hourly',
        python_callable=nm_from_darkstore_hourly,
        doc='сриды в дарксторах',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.dict.cbr_currency"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions_rc"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2_rc")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.recent_srids_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.srids_from_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.srids_from_darkstore")])

    speeds_in_darkstore_hourly_task = PythonOperator(
        pool=get_pool(LAKE_CONN),
        task_id='speeds_in_darkstore_hourly',
        python_callable=speeds_in_darkstore_hourly,
        doc='сроки доставки в дарксторах',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_rc_d"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_d"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions_rc"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2_rc"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.speeds_in_darkstore"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.speeds_in_darkstore")])

    order_uids_from_darkstore_hourly_task = PythonOperator(
        pool=get_pool(LAKE_CONN),
        task_id='order_uids_from_darkstore_hourly',
        python_callable=order_uids_from_darkstore_hourly,
        doc='4 и 5 вкладки отчёта 055',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.dict.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.dict.subjects"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.srids_from_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.speeds_in_darkstore")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.srids_for_recent_order_uids"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.srids_speeds_recent_batch"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.buffer.order_uids_from_darkstore_detailed"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.order_uids_from_darkstore_detailed"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.order_uids_from_darkstore")])


    skip_daily_tasks_task = ShortCircuitOperator(
        task_id='skip_daily_tasks',
        doc='запуск раз в сутки зависимых тасок',
        python_callable=skip_daily_tasks)
    

    nm_from_darkstore_daily_task = PythonOperator(
        pool=get_pool(LAKE_CONN),
        task_id='nm_from_darkstore_daily',
        python_callable=nm_from_darkstore_daily,
        doc='1 вкладка отчёта 055',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.srids_from_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.remote_ch3.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.nm_for_sale_on_date")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.buffer.nm_from_darkstore_v2"), 
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.buffer.nm_from_darkstore"), 
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.buffer.all_nm_from_darkstore_ever"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.all_nm_from_darkstore_ever"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.nm_from_darkstore")])

    nm_income_darkstore_daily_task = PythonOperator(
        pool=get_pool(CH8_CONN),        
        task_id='nm_income_darkstore_daily',
        python_callable=nm_income_darkstore_daily,
        doc='2 вкладка отчёта 055',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.goods_incomes"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.preorders"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.supplier_box"), 
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.supplier_box_barcode"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.shk_storage.shk_repo"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.shk_storage.shk_on_place"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.remote_ch3.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch8.stage_wh.nm_for_sale_on_date")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.buffer.nm_income_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.nm_income_darkstore"), 
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.buffer.current_stock_darkstore_for_join"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch-deliverytime.datamart.current_stock_darkstore_for_join")])

    speeds_in_darkstore_daily_task = PythonOperator(
        pool=get_pool(LAKE_CONN),
        task_id='speeds_in_darkstore_daily',
        python_callable=speeds_in_darkstore_daily,
        doc='3 вкладка отчёта 055',
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_rc_d"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.core_wh.srid_tracker_d"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.oof_positions_rc"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2_rc"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.positions.position_changes_v2")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.recent_order_uids_darkstore"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.order_uids_from_darkstore"), 
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.datamart.order_uids_from_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.buffer.speeds_in_darkstore"),
            OMEntity(entity=Entity.TABLE, fqn="do-ch13.datamart.speeds_in_darkstore")])    

    nm_from_darkstore_hourly_task >> nm_from_darkstore_daily_task
    speeds_in_darkstore_hourly_task >> speeds_in_darkstore_daily_task
    [nm_from_darkstore_hourly_task, speeds_in_darkstore_hourly_task] >> order_uids_from_darkstore_hourly_task
    skip_daily_tasks_task >> [
        nm_from_darkstore_daily_task, 
        nm_income_darkstore_daily_task, 
        speeds_in_darkstore_daily_task]
