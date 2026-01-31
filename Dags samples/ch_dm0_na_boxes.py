import logging as log
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from utils.db.clickhouse import get_ch_dwh_max_v2
from utils.data_exchange import copy_ch_to_ch_pipe
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH3_CONN = 'do-ch3'
CH4_CONN = 'do-ch4'
CH6_CONN = 'do-ch6'
CH8_CONN = 'do-ch8'
DM0_CONN = 'do-ch-dm0'
LAKE_M_CONN = 'do-lake-m'
TELEGA = '@artemy_kravtsov'
DESCRIPTION = "Обновление данных по непринятым коробкам v3. Даг перенесён с Greenplum на ClickHouse по задаче DataOps-12816"


CH4_SHIPPING = """
/* ch_dm3_na_boxes, CH4_SHIPPING */
   SELECT transfer_box_id, supplier_id, waysheet_id, box_dst_office_id, employee_id, freelancer_id, dt,
          dictGet('dict.branch_office', 'main_office_id', shipping_src_office_id) AS shipping_src_office_id, 1
     FROM stage_nats.shipping_boxes
    WHERE _row_created >= {min_cutoff: DateTime}
      AND _row_created <= {max_cutoff: DateTime}
      AND transfer_box_id NOT IN (SELECT transfer_box_id FROM buffer.na_boxes_ship)
 ORDER BY dt DESC
  LIMIT 1 BY transfer_box_id
   FORMAT MsgPack
"""

LAKE_SHIPPING_INSERT = """
   INSERT INTO buffer.na_boxes_ship_d
          (transfer_box_id, supplier_id, waysheet_id, box_dst_office_id, 
           employee_id, freelancer_id, dt, shipping_src_office_id, source)
   FORMAT MsgPack
"""

DM0_NA_BOXES = """
/* ch_dm3_na_boxes, DM0_NA_BOXES */
   SELECT transfer_box_id, supplier_id, waysheet_id, box_dst_office_id, 
          employee_id, freelancer_id, dt, shipping_src_office_id, 2
     FROM datamart.na_boxes
 ORDER BY dt DESC  /* ORDER BY + LIMIT BY для перестраховки. Таблица на 1 млн. записей */
  LIMIT 1 BY transfer_box_id    
   FORMAT MsgPack
"""

LAKE_SHIPPING_DROP_PARTITION = """
ALTER TABLE buffer.na_boxes_ship DROP PARTITION %(partition)s
"""

LAKE_DECLARATIONS = """
 TRUNCATE TABLE buffer.na_boxes_decl
;
 
/* ch_dm3_na_boxes, LAKE_DECLARATIONS */
/* последние отгрузки коробок. сразу подтягиваем офис назначения и ШК, исключаем коробки для которых не найдены декларации */
   INSERT INTO buffer.na_boxes_decl 
          (tare, shk_id, nm_id, srid, dt_in_tare, dt, supplier_id, waysheet_id, 
           box_dst_office_id, shipping_src_office_id, employee_id, freelancer_id)
 
   SELECT decl.tare                   AS tare,
          decl._pack.1                AS shk_id,
          decl._pack.2                AS nm_id,
          decl._pack.3                AS srid,
          decl._pack.4                AS dt_in_tare,
          box.dt                      AS dt,
          box.supplier_id             AS supplier_id,
          box.waysheet_id             AS waysheet_id,
          box.box_dst_office_id       AS box_dst_office_id,
          box.shipping_src_office_id  AS shipping_src_office_id,
          box.employee_id             AS employee_id,
          box.freelancer_id           AS freelancer_id
     FROM buffer.na_boxes_ship AS box
     JOIN (            /* определяем, какие ШК содержатся в отгруженных коробках shipping */
            SELECT tare, 
                   max(dt) AS dt,
                   argMax(office_id, src.dt) AS office_id,
                   groupArrayArgMax(tuple(shk_id, nm_id, srid, tare_content_dt), src.dt) AS _pack
              FROM stage_wh.wh_tare_declaration_by_tare AS src
          PREWHERE tare IN (SELECT transfer_box_id FROM buffer.na_boxes_ship)
               AND tare_type = 'TBX'  /* контейнеры не учитываем */
                       /* достаем лишь те ШК и коробки, которые есть в последних отгрузках  */
             WHERE src.dt > (SELECT min(dt) - toIntervalDay(10) FROM buffer.na_boxes_ship)
          GROUP BY tare /* вытаскиваем список ШК тары из ее ПОСЛЕДНЕЙ декларации. 
                           даже TBX часто катаются по офисам: 2415539696 */
          ) AS decl
       ON box.transfer_box_id = decl.tare
      AND box.dt > decl.dt  /* дата отгрузки позже даты декларации  */
      AND box.shipping_src_office_id = dictGet('dict.branch_office', 'main_office_id', decl.office_id)
             /* офис отгрузки = офису формирования декларации. проверяем однокрышники, иначе часть коробок отсеивается */
    ARRAY JOIN decl._pack
"""

LAKE_SHK_ON_PLACE = """  /* ch_dm3_na_boxes, LAKE_SHK_ON_PLACE */
   INSERT INTO buffer.na_boxes_shk_d 
          (tare, shk_id, dt, shipping_src_office_id)
   SELECT tare, shk_id, dt, shipping_src_office_id
     FROM buffer.na_boxes_decl_d
 SETTINGS parallel_distributed_insert_select=0
;

/* ch_dm3_na_boxes, LAKE_SHK_ON_PLACE */
   SELECT DISTINCT tare, shk_id, 2 AS src
     FROM buffer.na_boxes_shk_d AS prc3
     JOIN (
           SELECT shk_id, dt, office_id,
                  and(rn = 1,
                      or(left(state_id, 1) = 'O',
                         state_id = 'UDG',
                         isNull(state_id))) AS is_accepted_in_poo
             FROM (           /* есть случаи когда дата отгрузки больше чем релиз на ПВЗ, 
                                 для исключения таких косяков проверям ШК на релиз */             
                   SELECT shk_id, dt, state_id,
                          dictGet('dict.branch_office', 'main_office_id', office_id) AS office_id,
                          row_number() OVER (PARTITION BY shk_id ORDER BY dt DESC) AS rn
                     FROM shk_storage.shk_on_place_d AS sop
                    WHERE sop.row_created >= now() - toIntervalMonth(6)
                      AND sop.dt >= now() - toIntervalMonth(6)
                      AND sop.shk_id IN (SELECT shk_id FROM buffer.na_boxes_shk_d)
                  )
          ) AS shk
       ON shk.shk_id = prc3.shk_id
      AND shk.dt >= prc3.dt
    WHERE or(shk.is_accepted_in_poo,   /* крайний статус получен на ПВЗ */
             and(shk.dt > prc3.dt,  /* статус получен после отгрузки коробки */
                 coalesce(prc3.shipping_src_office_id, 0) != coalesce(shk.office_id, 0)))
                     /* исключаем внутриофисные перемещения */
 SETTINGS distributed_product_mode='local', 
          distributed_group_by_no_merge=1
   FORMAT MsgPack
"""

CH6_SHK_ON_PLACE_INSERT = """
   INSERT INTO buffer.na_boxes_accepted_shk
          (tare, shk_id, source) 
   FORMAT MsgPack
"""

CH8_SHK_ON_PLACE_TARES = """  /* ch_dm3_na_boxes, CH8_SHK_ON_PLACE_TARES */
   SELECT DISTINCT tare, 3 as src
     FROM shk_storage.shk_on_place_tares AS sopt
SEMI JOIN buffer.na_boxes_tare AS prc3
       ON sopt.tare = prc3.tare
      AND sopt.dt > prc3.dt
    WHERE sopt.dt > (SELECT min(dt) FROM buffer.na_boxes_tare)
      AND sopt.tare IN (SELECT tare FROM buffer.na_boxes_tare)
   FORMAT MsgPack
"""

CH6_SHK_ON_PLACE_TARES_INSERT = """
 TRUNCATE TABLE buffer.na_boxes_accepted_box;
   INSERT INTO buffer.na_boxes_accepted_box 
          (tare, source) 
   FORMAT MsgPack
"""

CH6_SRID_TRACKER_INSERT = """  /* ch_dm3_na_boxes, CH6_SRID_TRACKER_INSERT */
   INSERT INTO buffer.na_boxes_accepted_shk
          (tare, shk_id, source) 
   SELECT DISTINCT tare, shk_id, 1 AS src
     FROM buffer.na_boxes_decl AS prc3
     JOIN (SELECT srid, ts + toIntervalHour(3) AS ts,
                  dictGet('dict.branch_office', 'main_office_id', office_id) AS office_id
             FROM core_wh.srid_tracker
         PREWHERE srid IN (SELECT srid FROM buffer.na_boxes_decl)
            WHERE ts >= now() - toIntervalMonth(3)
              AND hasAny(
                      dictGet('dict.action_list_srid', 'sources', action_id), 
                      ['logistics', 'delivery']) = False              
                  /* из ГП-версии по шк-трекеру: "исключаем статусы отгрузки и в пути, 
                     т,к. бывает, что они возникают сильно позже даты отгрузки" */
                  /* в кач-ве соответствия исключаю статусы от логистов - статус, на основании 
                     которого можно судить о том, что ШК принят, должен прийти не от них */
          ) AS st
       ON st.srid = prc3.srid
      AND prc3.dt < st.ts         /* статус получен после отгрузки коробки */
    WHERE or(coalesce(prc3.shipping_src_office_id, 0) != st.office_id,
             isNull(st.office_id))   
                 /* исключаем внутриофисные перемещения */
"""

CH4_POSITIONS = """  /* ch_dm3_na_boxes, CH4_POSITIONS */
   SELECT srid, argMax(dst_office_id, dt) AS dst_office_id
     FROM positions.oof_position_status_v3 AS src
 PREWHERE srid IN (SELECT srid FROM buffer.na_boxes_srid)
    WHERE dt >= (SELECT min(dt) - toIntervalMonth(6) FROM buffer.na_boxes_srid)
 GROUP BY srid
   HAVING isNotNull(dst_office_id)
   FORMAT MsgPack
"""

CH6_POSITIONS_INSERT = """
 TRUNCATE TABLE buffer.na_boxes_pos;
   INSERT INTO buffer.na_boxes_pos 
          (srid, dst_office_id) 
   FORMAT MsgPack
"""

CH3_LOSTPOST = """  /* ch_dm3_na_boxes, CH3_LOSTPOST */
   SELECT DISTINCT tare
     FROM buffer.na_boxes_shk AS box
     JOIN (SELECT shk_id, operation_dt
             FROM dict_final.inventory_lost_shk_all_short FINAL
            WHERE shk_id IN (SELECT shk_id FROM buffer.na_boxes_shk)
              AND operation_dt > {min_dt: String}
              AND type_id = 8
          ) AS lp
       ON box.shk_id = lp.shk_id
      AND operation_dt > dt
   FORMAT MsgPack
"""

CH6_LOSTPOST_INSERT = """
 TRUNCATE TABLE buffer.na_boxes_lp;
   INSERT INTO buffer.na_boxes_lp 
          (tare) 
   FORMAT MsgPack
"""

LAKE_SHIP_AND_DECL = """
   SELECT tare, shk_id, nm_id, srid, dt_in_tare, dt, supplier_id, waysheet_id, 
          box_dst_office_id, shipping_src_office_id, employee_id, freelancer_id
     FROM buffer.na_boxes_decl
   FORMAT MsgPack
"""

CH6_SHIP_AND_DECL_INSERT = """
 TRUNCATE TABLE buffer.na_boxes_decl;
   INSERT INTO buffer.na_boxes_decl 
          (tare, shk_id, nm_id, srid, dt_in_tare, dt, supplier_id, waysheet_id, 
           box_dst_office_id, shipping_src_office_id, employee_id, freelancer_id)
   FORMAT MsgPack
"""

CH6_CALCULATIONS = """
ALTER TABLE buffer.na_boxes_accepted_box DROP PARTITION 1;
ALTER TABLE buffer.na_boxes_accepted_box DROP PARTITION 2;
ALTER TABLE buffer.na_boxes_accepted_box DROP PARTITION 4
;

/* Если отгруженная тара в рамках одного ШК содержит отличающийся от последнего по дате srid, то она точно принята */
  INSERT INTO buffer.na_boxes_accepted_box 
         (tare, source)
  SELECT DISTINCT tare, 1 AS src
    FROM buffer.na_boxes_decl
 QUALIFY srid != argMax(srid, dt) OVER (PARTITION BY shk_id)
;

/* Джойн с позишенами - присоединяю dst_office_id. Откидываю сриды, по которым не подтянулся */
CREATE TEMPORARY TABLE shk_in_box_prc3
ENGINE=MergeTree
ORDER BY tare
AS
  SELECT tare AS tare,
         shk_id AS shk_id,
         nm_id AS nm_id,
         decl.srid AS srid,
         dt AS dt,
         toInt64(pos.dst_office_id) AS dst_office_id,
         nullIf(toInt64(bo.office_id), 0) AS new_dst_office_id,
         box_dst_office_id,
         shipping_src_office_id,
         employee_id,
         supplier_id,
         waysheet_id,
         freelancer_id AS freelancer_id,
         or(pos.dst_office_id = decl.box_dst_office_id,
            bo.office_id      = decl.box_dst_office_id
            ) AS shk_dst_equal_to_box_dst
    FROM buffer.na_boxes_decl AS decl
    JOIN buffer.na_boxes_pos AS pos
      ON pos.srid = decl.srid
    LEFT
ANY JOIN (SELECT office_id, old_office_id
            FROM dict.branch_office
           LIMIT 1 BY old_office_id) AS bo
      ON pos.dst_office_id = bo.old_office_id
   WHERE notEmpty(decl.srid)
     AND decl.tare NOT IN (SELECT tare FROM buffer.na_boxes_accepted_box WHERE source=1)
;

/* 1: если отгруженная тара в рамках одного ШК содержит отличающийся от последнего по дате srid (заполняется выше)
   2: если хоть один из ШК в коробке принят после отгрузки (есть движение по shk_on_place | srid_tracker)
   3: есть движение тары по shk_on_place_tares (заполняется отдельно)
   4: есть распаковка тары по wh_tare_unpacking */
   
   INSERT INTO buffer.na_boxes_accepted_box 
          (tare, source)

   SELECT DISTINCT tare, 2 AS source
     FROM buffer.na_boxes_accepted_shk

    UNION ALL 
   SELECT DISTINCT tare, 4 as source
     FROM stage_wh.wh_tare_unpacking AS unp
SEMI JOIN shk_in_box_prc3 AS prc3
       ON unp.tare_id = prc3.tare
      AND unp.dt > prc3.dt
    WHERE unp.dt > (SELECT min(dt) FROM shk_in_box_prc3)
      AND unp.tare_id IN (SELECT tare FROM shk_in_box_prc3)
      AND unp.tare_id NOT IN (
               SELECT tare FROM buffer.na_boxes_accepted_shk
                UNION ALL 
               SELECT tare FROM buffer.na_boxes_accepted_box)
;

/* Формируем табицу с коробками, где в одной коробке ШК имеют разные конечные офисы назнаения */
CREATE TEMPORARY TABLE tmp_tare_multi_dst
ENGINE=MergeTree
ORDER BY tare
AS
    WITH coalesce(nullIf(new_dst_office_id, 0), toInt64(dst_office_id)) AS _dst_office_id_fin
  SELECT tare
    FROM shk_in_box_prc3
GROUP BY tare
  HAVING uniqUpTo(1)(_dst_office_id_fin) = 2
;

/* Выгруженные коробки */
CREATE TEMPORARY TABLE unloaded_boxes
ENGINE=MergeTree
ORDER BY tare
AS
   SELECT tare_id AS tare, toInt64(office_id) AS office_id, unloaded_dt
     FROM stage_wh.wh_tares_unload
    WHERE tare_id IN (SELECT tare FROM shk_in_box_prc3)
      AND unloaded_dt > (SELECT min(dt) from shk_in_box_prc3)
;

TRUNCATE TABLE buffer.na_boxes
;

/* Заполнение итоговой витрины данными по непринятым коробкам */
  INSERT INTO buffer.na_boxes
         (transfer_box_id, dt, shipping_src_office_id, box_dst_office_id, dst_office_id, 
          supplier_id, waysheet_id, unloaded_dt, cnt_shk, direction_type, employee_id, freelancer_id)

  SELECT tare AS transfer_box_id,
         max(dt) AS dt,
         argMax(shipping_src_office_id, src.dt) AS shipping_src_office_id,
         argMax(box_dst_office_id, src.dt) AS box_dst_office_id,
         argMax(dst_office_id, src.dt) AS dst_office_id,
         argMax(supplier_id, src.dt) AS supplier_id,
         argMax(waysheet_id, src.dt) AS waysheet_id,
         argMax(unloaded_dt, src.dt) AS unloaded_dt,
         count() AS cnt_shk,
         argMax(direction_type, src.dt) AS direction_type,
         argMax(employee_id, src.dt) AS employee_id,
         argMax(freelancer_id, src.dt) AS freelancer_id

    FROM (   /* 1: Прямые отгруженные коробоки, доставленные, но не принятые на ПВЗ
                2: Продажные коробки. Были отгружены, затем выгружены в ПВЗ, и часть ШК (не все) из них приняты
                3: Продажные коробки. Были отгружены, но не выгружены, офис назначения ШК равен офису назначения коробки
                4: Продажные коробки. Были отгружены, но не выгружены, офис назначения ШК НЕ равен офису назначения коробки */

            WITH full_accepted_box AS (
                          /* отгруженные и полностью принятые коробки (все ШК в коробке приняты после отгрузки) */
                   SELECT tare 
                     FROM shk_in_box_prc3
                 GROUP BY tare
                   HAVING min((tare, shk_id) IN (
                              SELECT tare, shk_id 
                                FROM buffer.na_boxes_accepted_shk)) = True)
       
                 /* отгруженные коробки, которые выгружены в офисе назначения. дата выгрузки позже даты отгрузки */
          SELECT tare,
                 dt,
                 shipping_src_office_id,
                 box_dst_office_id,
                 dst_office_id,
                 supplier_id,
                 waysheet_id,
                 shk_id,
                 employee_id,
                 freelancer_id,
                 unl.unloaded_dt AS unloaded_dt,
                 multiIf(
                     tare NOT IN (SELECT tare FROM buffer.na_boxes_accepted_box),
                     1,  /* доставленные, но не принятые коробки */
                     tare IN (SELECT tare FROM buffer.na_boxes_accepted_box
                               WHERE tare NOT IN full_accepted_box),
                     2,  /* доставленные и частично принятые коробки (не все ШК в коробке приняты) */
                     NULL) AS direction_type
            FROM shk_in_box_prc3 AS decl
            JOIN unloaded_boxes AS unl
              ON unl.tare = decl.tare
             AND or(unl.office_id = decl.dst_office_id,
                    unl.office_id = decl.new_dst_office_id)
             AND unl.unloaded_dt > decl.dt
           WHERE decl.tare NOT IN (
                     SELECT tare FROM buffer.na_boxes_lp   /* отсеиваем коробки со списанными ШК */
                      UNION ALL
                     SELECT tare FROM tmp_tare_multi_dst)  /* исключаем коробки с мульти дестом по ШК */
             AND decl.shk_dst_equal_to_box_dst = True
             AND isNotNull(direction_type)
                 /* конечный офис назначения ШК = офису назначения коробки. shk_dst = box_dst */
       
           UNION ALL
       
                 /* коробки, которые были отгружены, не выгружены водителем в ПВЗ, не приняты в ПВЗ, на складе или СЦ. */
          SELECT decl.tare AS tare,
                 dt,
                 shipping_src_office_id,
                 box_dst_office_id,
                 dst_office_id,
                 supplier_id,
                 waysheet_id,
                 shk_id,
                 employee_id,
                 freelancer_id,
                 NULL AS unloaded_dt,
                 multiIf(
                     and(tare NOT IN (SELECT tare FROM tmp_tare_multi_dst),
                         decl.shk_dst_equal_to_box_dst = True),
                     3,  /* офис назначения шк равен офису назначения по отгрузке */
                     and(was_ever_unloaded=0,
                         decl.shk_dst_equal_to_box_dst = False),
                     4,  /* офис назначения шк НЕ равен офису назначения по отгрузке. тары нет в списке 
                            выгруженных куда угодно. коробка может содержать ШК с отличающимися офисами назначения */    
                     NULL) AS direction_type
            FROM shk_in_box_prc3 AS decl
       ANTI JOIN unloaded_boxes AS unl   /* тары нет в списке выгруженных в офисе назначения */
              ON unl.tare = decl.tare
             AND or(unl.office_id = decl.dst_office_id,
                    unl.office_id = decl.new_dst_office_id)
             AND unl.unloaded_dt > decl.dt
        LEFT ANY
            JOIN (SELECT tare, unloaded_dt, 1 AS was_ever_unloaded FROM unloaded_boxes) AS unl2
              ON unl2.tare = decl.tare
             AND unl2.unloaded_dt > decl.dt      
           WHERE decl.tare NOT IN (
                     SELECT tare FROM buffer.na_boxes_lp   /* отсеиваем коробки со списанными ШК */
                      UNION ALL
                     SELECT tare FROM buffer.na_boxes_accepted_box)  /* исключаем принятые коробки */
             AND isNotNull(direction_type)
         ) AS src
GROUP BY transfer_box_id
;

TRUNCATE TABLE buffer.na_boxes_by_shk
;

/* Заполнение итоговой витрины данными по непринятым коробкам с детализацией до ШК */
  INSERT INTO buffer.na_boxes_by_shk
         (tare, shk_id, nm_id, srid, dst_office_id, is_accepted)
  SELECT tare, shk_id, nm_id, srid, dst_office_id,
         (tare, shk_id) IN (
             SELECT tare, shk_id 
               FROM buffer.na_boxes_accepted_shk
              WHERE tare IN (SELECT transfer_box_id FROM buffer.na_boxes)) AS is_accepted
    FROM shk_in_box_prc3 AS prc3
   WHERE prc3.tare IN (SELECT transfer_box_id FROM buffer.na_boxes)
"""

CH6_NA_BOXES = """
  WITH dictGet('dict.waysheets_cache', ('dt_arrival', 'driver_id'), waysheet_id) AS _d
SELECT transfer_box_id, dt, shipping_src_office_id, box_dst_office_id, dst_office_id, 
       supplier_id, waysheet_id, unloaded_dt, cnt_shk, direction_type, employee_id, freelancer_id,
       dictGet('dict.branch_office', 'type_point_name', shipping_src_office_id) AS src_type_point_name,
       dictGet('dict.branch_office', 'type_point_name', dst_office_id) AS type_point_name,
       dictGet('dict.branch_office', 'region_name', shipping_src_office_id) AS src_district_name,
       dictGet('dict.branch_office', 'region_name', dst_office_id) AS district_name,
       _d.dt_arrival AS dt_arrival, _d.driver_id AS driver_id
  FROM buffer.na_boxes
FORMAT MsgPack
"""

DM0_NA_BOXES_INSERT = """
TRUNCATE TABLE buffer.na_boxes;

INSERT INTO buffer.na_boxes
       (transfer_box_id, dt, shipping_src_office_id, box_dst_office_id, dst_office_id, 
        supplier_id, waysheet_id, unloaded_dt, cnt_shk, direction_type, employee_id, 
        freelancer_id, src_type_point_name, type_point_name, src_district_name, 
        district_name, dt_arrival, driver_id)
FORMAT MsgPack
"""

CH6_NA_BOXES_BY_SHK = """
SELECT tare, shk_id, nm_id, title, srid, dst_office_id, is_accepted
  FROM buffer.na_boxes_by_shk
  LEFT ANY JOIN (
           SELECT nm_id, title 
             FROM remote_ch3.product_cards_nm 
            WHERE nm_id GLOBAL IN (SELECT nm_id FROM buffer.na_boxes_by_shk)
       ) AS cards
 USING (nm_id)
FORMAT MsgPack
"""

DM0_NA_BOXES_BY_SHK_INSERT = """
TRUNCATE TABLE buffer.na_boxes_by_shk;

INSERT INTO buffer.na_boxes_by_shk
       (tare, shk_id, nm_id, title, srid, dst_office_id, is_accepted)
FORMAT MsgPack
"""

DM0_REPLACE_PARTITION = """
CREATE TEMPORARY TABLE src_offices
ENGINE = MergeTree
PARTITION BY tuple()
PRIMARY KEY tuple()
ORDER BY shipping_src_office_id
AS SELECT DISTINCT shipping_src_office_id FROM buffer.na_boxes
;

ALTER TABLE datamart.na_boxes         REPLACE PARTITION tuple() FROM buffer.na_boxes;
ALTER TABLE datamart.na_boxes_by_shk  REPLACE PARTITION tuple() FROM buffer.na_boxes_by_shk;
ALTER TABLE datamart.na_boxes_offices REPLACE PARTITION tuple() FROM src_offices;
"""


@with_db(LAKE_M_CONN, 'lake')
@with_db(CH4_CONN, 'ch4')
@load_and_save_cutoff('ch_dm3_na_boxes', 'ch4', 'max_dt')
def shipping(ch4_hook, lake_hook, cutoff, **context):
    (min_cutoff, max_cutoff, _, _) = get_ch_dwh_max_v2(
        ch_hook=ch4_hook,
        changes_table_name='stage_nats.shipping_boxes',
        date_field='_row_created',
        max_batch_records=2e6,
        max_dt=cutoff)
    lake_hook.on_cluster(
        lake_hook.exec, 
        LAKE_SHIPPING_DROP_PARTITION,
        parameters=dict(partition=1))
    copy_ch_to_ch_pipe(
        take_data=CH4_SHIPPING,
        insert_data=LAKE_SHIPPING_INSERT,
        src_ch=CH4_CONN,
        dst_ch=LAKE_M_CONN,
        parameters=dict(
            min_cutoff=min_cutoff, 
            max_cutoff=max_cutoff))
    context["ti"].xcom_push(
        key="cutoff", 
        value=str(max_cutoff))


@with_db(LAKE_M_CONN, 'lake')
def na_boxes(lake_hook):
    lake_hook.on_cluster(
        lake_hook.exec, 
        LAKE_SHIPPING_DROP_PARTITION,
        parameters=dict(partition=2))    
    copy_ch_to_ch_pipe(
        take_data=DM0_NA_BOXES,
        insert_data=LAKE_SHIPPING_INSERT,
        src_ch=DM0_CONN,
        dst_ch=LAKE_M_CONN)


@with_db(CH6_CONN, 'ch6')
@with_db(LAKE_M_CONN, 'lake')
def declarations(lake_hook, ch6_hook):
    ch6_hook.exec('TRUNCATE TABLE buffer.na_boxes_accepted_shk')
    lake_hook.on_cluster(
        lake_hook.exec, 
        LAKE_DECLARATIONS)


@with_db(LAKE_M_CONN, 'lake')
def shk_on_place(lake_hook):
    lake_hook.on_cluster(lake_hook.exec, 'TRUNCATE TABLE buffer.na_boxes_shk')  
    copy_ch_to_ch_pipe(
        take_data=LAKE_SHK_ON_PLACE,
        insert_data=CH6_SHK_ON_PLACE_INSERT,
        src_ch=LAKE_M_CONN,
        dst_ch=CH6_CONN,
        multiquery=True)


def shk_on_place_tares():
    copy_ch_to_ch_pipe(
        take_data="SELECT tare, dt FROM buffer.na_boxes_decl_d FORMAT MsgPack",
        insert_data="TRUNCATE TABLE buffer.na_boxes_tare; INSERT INTO buffer.na_boxes_tare (tare, dt) FORMAT MsgPack",
        src_ch=LAKE_M_CONN,
        dst_ch=CH8_CONN,
        multiquery=True)
    copy_ch_to_ch_pipe(
        take_data=CH8_SHK_ON_PLACE_TARES,
        insert_data=CH6_SHK_ON_PLACE_TARES_INSERT,
        src_ch=CH8_CONN,
        dst_ch=CH6_CONN,
        multiquery=True)


@with_db(CH6_CONN, 'ch6')
def srid_tracker(ch6_hook):
    copy_ch_to_ch_pipe(
        take_data=LAKE_SHIP_AND_DECL,
        insert_data=CH6_SHIP_AND_DECL_INSERT,
        src_ch=LAKE_M_CONN,
        dst_ch=CH6_CONN,
        multiquery=True)
    ch6_hook.exec_with_log(CH6_SRID_TRACKER_INSERT)


def positions():
    copy_ch_to_ch_pipe(
        take_data="SELECT srid, dt FROM buffer.na_boxes_decl_d WHERE isNotNull(srid) FORMAT MsgPack",
        insert_data="TRUNCATE TABLE buffer.na_boxes_srid; INSERT INTO buffer.na_boxes_srid (srid, dt) FORMAT MsgPack",
        src_ch=LAKE_M_CONN,
        dst_ch=CH4_CONN,
        multiquery=True)
    copy_ch_to_ch_pipe(
        take_data=CH4_POSITIONS,
        insert_data=CH6_POSITIONS_INSERT,
        src_ch=CH4_CONN,
        dst_ch=CH6_CONN,
        multiquery=True)


@with_db(CH4_CONN, 'ch4')
def lostpost(ch4_hook):
    min_dt = ch4_hook.fetchone('SELECT min(dt) FROM buffer.na_boxes_decl')
    copy_ch_to_ch_pipe(
        take_data="SELECT shk_id, tare, dt FROM buffer.na_boxes_decl_d FORMAT MsgPack",
        insert_data="TRUNCATE TABLE buffer.na_boxes_shk; INSERT INTO buffer.na_boxes_shk (shk_id, tare, dt) FORMAT MsgPack",
        src_ch=LAKE_M_CONN,
        dst_ch=CH3_CONN,
        multiquery=True)
    copy_ch_to_ch_pipe(
        take_data=CH3_LOSTPOST,
        insert_data=CH6_LOSTPOST_INSERT,
        src_ch=CH3_CONN,
        dst_ch=CH6_CONN,
        multiquery=True,
        parameters=dict(min_dt=min_dt))


@with_db(CH6_CONN, 'ch6')
@with_db(DM0_CONN, 'dm1')
@load_and_save_cutoff('ch_dm3_na_boxes', CH4_CONN, 'max_dt')
def calculations(ch6_hook, dm1_hook, **context):
    ch6_hook.exec_with_log(CH6_CALCULATIONS)
    copy_ch_to_ch_pipe(
        take_data=CH6_NA_BOXES,
        insert_data=DM0_NA_BOXES_INSERT,
        src_ch=CH6_CONN,
        dst_ch=DM0_CONN,
        multiquery=True)
    copy_ch_to_ch_pipe(
        take_data=CH6_NA_BOXES_BY_SHK,
        insert_data=DM0_NA_BOXES_BY_SHK_INSERT,
        src_ch=CH6_CONN,
        dst_ch=DM0_CONN,
        multiquery=True)
    dm1_hook.exec_with_log(DM0_REPLACE_PARTITION)
    return context["ti"].xcom_pull(
        dag_id=context["dag"].dag_id,
        task_ids='shipping.shipping',
        key="cutoff")


with DAG(
    dag_id="ch_dm3_na_boxes",
    schedule='0 */3 * * *',  # каждые 3 часа
    start_date=datetime(2025, 11, 10),
    description=DESCRIPTION,
    max_active_runs=1,
    catchup=False,
    tags=[CH3_CONN, CH4_CONN, CH6_CONN, CH8_CONN, DM0_CONN, LAKE_M_CONN, TELEGA],
    render_template_as_native_obj=True,
    default_args=dict(
        owner="kravcov",
        telegram=[TELEGA],
        retries=2,
        retry_delay=timedelta(minutes=5),
    )):

    with TaskGroup(group_id='shipping') as shipping_group:
        shipping_task = PythonOperator(
            task_id='shipping',
            python_callable=shipping,
            inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_nats.shipping_boxes")],
            outlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.na_boxes_ship")],
            pool=CH4_CONN)

        na_boxes_task = PythonOperator(
            task_id='na_boxes',
            python_callable=na_boxes,
            inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-dm0.datamart.na_boxes")],
            outlets=[OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.na_boxes_ship")],
            pool=DM0_CONN)

    declarations_task = PythonOperator(
        task_id='declarations',
        python_callable=declarations,
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.na_boxes_ship"),
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.stage_wh.wh_tare_declaration_by_tare")],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-lake-m.buffer.na_boxes_decl")],
        pool=LAKE_M_CONN)

    with TaskGroup(group_id='gather_data') as gather_group:
        PythonOperator(
            task_id='shk_on_place',
            python_callable=shk_on_place,
            inlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-m.buffer.na_boxes_shk"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-m.shk_storage.shk_on_place")],
            outlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_shk"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_accepted_shk")],
            pool=LAKE_M_CONN)

        PythonOperator(
            task_id='shk_on_place_tares',
            python_callable=shk_on_place_tares,
            inlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch8.buffer.na_boxes_tare"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch8.shk_storage.shk_on_place_tares")],
            outlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch8.buffer.na_boxes_tare"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_accepted_box")],
            pool=CH8_CONN)

        PythonOperator(
            task_id='srid_tracker',
            python_callable=srid_tracker,
            inlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.dict.branch_office"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.dict.action_list_srid"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.core_wh.srid_tracker")],
            outlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch6.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_accepted_shk")],
            pool=CH6_CONN)

        PythonOperator(
            task_id='positions',
            python_callable=positions,
            inlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch4.buffer.na_boxes_srid"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch4.positions.oof_position_status_v3")],
            outlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch4.buffer.na_boxes_srid"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_pos")],               
            pool=CH4_CONN)

        PythonOperator(
            task_id='lostpost',
            python_callable=lostpost,
            inlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-m.buffer.na_boxes_decl"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch3.buffer.na_boxes_shk"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch3.dict_final.inventory_lost_shk_all_short")],
            outlets=[
                OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch3.buffer.na_boxes_shk"),
                OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_lp")],
            pool=CH3_CONN)

    calculations_task = PythonOperator(
        task_id='calculations',
        trigger_rule='all_success',
        python_callable=calculations,
        inlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch6.buffer.na_boxes_decl"),
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch6.buffer.na_boxes_accepted_shk"),
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch6.stage_wh.wh_tare_unpacking"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_decl"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_pos"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_lp"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_accepted_shk"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_accepted_box"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.stage_wh.wh_tares_unload"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-ch6.buffer.na_boxes"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-ch6.dict.waysheets_cache"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch6.buffer.na_boxes_by_shk"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch6.remote_ch3.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-ch-dm0.buffer.na_boxes"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-ch-dm0.buffer.na_boxes_by_shk")],
        outlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-ch6.buffer.na_boxes_accepted_box"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch6.buffer.na_boxes_by_shk"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-ch-dm0.buffer.na_boxes"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch-dm0.buffer.na_boxes_by_shk"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-ch-dm0.datamart.na_boxes"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-ch-dm0.datamart.na_boxes_by_shk")],
        pool=CH6_CONN)

    shipping_group >> declarations_task >> gather_group >> calculations_task
