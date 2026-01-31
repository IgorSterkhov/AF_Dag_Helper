import logging
from time import sleep
from itertools import combinations
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from collections import defaultdict
from datetime import datetime, timedelta
from utils.cutoff import get_cutoff
from concurrent.futures import ThreadPoolExecutor, wait
from utils.decorators_with_conn import with_db, with_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


DM_CONN = "do-ch13"
CH9_CONN = "do-ch9"
LAKE_M_CONN = "do-lake-m"
LAKE_R_CONN = "do-lake-r"
TELEGA = "@artemy_kravtsov"
DESCRIPTION = "Сроки доставки v3, DataOps-1099"
MEASURE_CODES = [
    0, 1, 2, 3, 4, 11, 13, 23, 34, 45, 56, 
    64, 65, 67, 70, 78, 89, 90, 93, 98, 99]
ACTIONS_WHITE_LIST = [
    110, 111, 112, 115, 116, 117, 120, 121, 122, 123, 124, 125, 130, 
    131, 140, 190, 200, 201, 210, 220, 230, 310, 320, 400, 500, 610, 
    620, 630, 640, 700, 800, 900, 910, 1000, 1010, 1020, 1030, 1033, 
    1035, 1040, 1041, 1050, 1070, 1075, 1080, 1090, 1400, 1500]
GET_RC = lambda x: "_rc_d" if (datetime.now() - x).days < 30 else "_d"
MAX_BATCH_RECORDS_ST = 80_000_000
MAX_BATCH_RECORDS_MANUAL = 40_000_000
V3_DATE_START = "2024-07-01"
CALC_HOURLY_FOR_WEEKS_NUM = 8


INSERT_SRIDS_FOR_MANUAL = """
   ALTER TABLE buffer.v3_srids_for_manual DROP PARTITION %(dag_id)s;

  INSERT INTO buffer.v3_srids_for_manual
         (dag_id, srid, synth_rn)
  SELECT %(dag_id)s, srid, 
         toDateTime(intDiv(row_number() OVER (ORDER BY srid), 10) +1) AS synth_rn
    FROM core_wh.srid_tracker
   WHERE row_created >= %(min_cutoff)s
     AND row_created <= %(max_cutoff)s
     AND srid != '145'
GROUP BY srid
SETTINGS optimize_aggregation_in_order=1
"""

CLEAR_SRIDS_FOR_MANUAL = """
ALTER TABLE buffer.v3_srids_for_manual DROP PARTITION %(dag_id)s
"""

INSERT_WEEK_TO_QUEUE_MANUALLY = """
  INSERT INTO public.v3_queue
         (week_start, queued_at, src_dag_id, days_changed)

  SELECT week_start, 
         now() AS queued_at, 
         any(src_dag_id),
         arraySort(
             groupUniqArrayArray(
                 arrayConcat(
                     new.days_changed, 
                     last_actual.days_changed))) AS days_changed
    FROM (SELECT toDate(week_start) AS week_start,
                 src_dag_id,
                 CAST(days_changed, 'Array(Date)') AS days_changed
            FROM system.one
      ARRAY JOIN %(week_start)s AS week_start,
                 %(src_dag_id)s AS src_dag_id,
                 %(days_changed)s AS days_changed
         ) AS new
LEFT ANY 
    JOIN (       /* присоединяю от предыдущих постановок в очередь 
                    days_changed. не важно, в работе неделя или нет */
          SELECT week_start, days_changed
            FROM public.v3_queue FINAL
         ) AS last_actual
   USING (week_start)
GROUP BY week_start
"""

DELETE_MANUAL_CUTOFF = """
ALTER TABLE public.max_val_cutoff DELETE WHERE table_name LIKE %(cutoff_name)s
"""

GET_NEXT_WEEK_FROM_QUEUE = """
CREATE TEMPORARY TABLE next_week_info
ENGINE = Memory
AS
    WITH argMax(
             tuple(src.week_start, src.queued_at, src.src_dag_id, src.days_changed),
             src.week_start) AS next
  SELECT next.1 AS week_start,
         next.2 AS queued_at,
         next.3 AS src_dag_id,
         next.4 AS days_changed,
         count() AS weeks_left
    FROM public.v3_queue AS src FINAL
   WHERE src.is_running = False
     AND if(%(is_manual)s, src.src_was_manual, NOT src.src_was_manual)
     AND if(%(is_hourly)s, src.week_start >= toStartOfWeek(now()) - toIntervalMonth(1), True)
     AND src.week_start NOT IN %(weeks_already_taken)s
  HAVING weeks_left > 0 
;

  INSERT INTO public.v3_queue
         (week_start, queued_at, src_dag_id, days_changed, is_running, run_dag_id)
  SELECT week_start, 
         queued_at, 
         src_dag_id, 
         days_changed,
         True AS is_running, 
         %(run_dag_id)s AS run_dag_id
    FROM next_week_info
;

  SELECT week_start, 
         week_start >= toStartOfWeek(today()) - toIntervalWeek(%(recent_backoff)s) AS is_recent,
         src_dag_id, days_changed, weeks_left
    FROM next_week_info
"""

REMOVE_WEEK_FROM_QUEUE = """
  INSERT INTO public.v3_queue
         (week_start, queued_at, src_dag_id, run_dag_id, days_changed, is_running, is_succeded)

  SELECT week_start, queued_at, src_dag_id, run_dag_id, days_changed,
         False AS is_running,
         True AS is_succeded
    FROM public.v3_queue AS src FINAL
   WHERE src.is_running = True
     AND src.run_dag_id = %(run_dag_id)s
     AND src.week_start = %(week_start)s
"""

LAKE_CLEAR_BUFFERS = """
ALTER TABLE buffer.v3_srid_set DROP PARTITION %(integer_dag_id)s;
ALTER TABLE buffer.v3_srid_recent DROP PARTITION %(integer_dag_id)s;
ALTER TABLE buffer.v3_srid_history DROP PARTITION %(integer_dag_id)s;
ALTER TABLE buffer.v3_queue DROP PARTITION %(dag_id)s
"""

LAKE_GET_CHANGED_SRIDS = """
INSERT INTO buffer.v3_srid_set (integer_dag_id, srid, ts)
  SELECT %(integer_dag_id)s, srid, toDateTime(0)
    FROM buffer.v3_srids_for_manual
   WHERE %(is_manual)s = True
     AND dag_id = %(dag_id)s
     AND synth_rn >= %(min_cutoff)s
     AND synth_rn <= %(max_cutoff)s
     
   UNION ALL
  SELECT %(integer_dag_id)s, srid, ts 
    FROM remote(
           'localhost',
           if(%(min_cutoff)s <= now() - toIntervalMonth(1),
              'core_wh.srid_tracker',
              'core_wh.srid_tracker_rc'))
   WHERE %(is_manual)s = False
     AND row_created >= %(min_cutoff)s
     AND row_created <= %(max_cutoff)s
     AND srid != '145'
     
   UNION ALL
  SELECT %(integer_dag_id)s, srid, dt
    FROM remote(
           'localhost',
           if(%(pos_min_cutoff)s <= now() - toIntervalMonth(1),
              'positions.oof_position_status_v3',
              'positions.oof_position_status_v3_rc'))
   WHERE %(is_manual)s = False
     AND row_created >= %(pos_min_cutoff)s
     AND row_created <= %(pos_max_cutoff)s
     AND toDateTime(dt) > toDateTime(0)
"""

LAKE_CALC_V3_SRIDS = """
/* lake_dm3_delivery_times_v3, CALC_V3_SRIDS */
INSERT INTO buffer.v3_srid_recent 
       (integer_dag_id, srid, is_mp, src_office_id, poo_office_id, is_deleted, nm_id,
        price, ddate, sm_id, measure_code, measure_office_id, measure_ts, measure_speed,
        daily_flag, iters_cnt, create_dt) 
SELECT %(integer_dag_id)s,
       srid,
       is_mp_fin,
       src_office_id,
       poo_office_id,
       is_deleted,
       nm_id,
       price,
       ddate, 
       sm_id,
       (arrayJoin(measures_agg) AS m).1 AS measure_code,
       m.2 AS measure_office_id,
       m.3 AS measure_ts,
       m.4 AS measure_speed,
       m.5 AS daily_flag,
       m.6 AS iters_cnt,
       create_dt
FROM (
SELECT srid, poo_office_id, src_office_id, is_deleted, is_mp_fin, nm_id, sm_id, price, ddate, create_dt,
       CAST(arrayConcat(
           arrayMap(
               x -> tuple(x.1, x.2, x.3, min2(2147483647, arraySum(z -> z.1 - z.2, x.4)), 1, 1),
               synth_measures_agg),
           arrayMap(
               (y, daily_rank) -> tuple(
                    y.1, y.2, y.3, min2(y.4, 2147483647), daily_rank=1, min2(y.5, 255)),
               sec_msq_agg_final, /* в первый раз когда мера встрачается у срида за день, независимо от колва офисов */
               arrayEnumerateUniq(arrayMap(m -> (m.1, toDate(m.3)), sec_msq_agg_final)))),
            'Array(Tuple(
                measure_code UInt8, 
                measure_office_id Int64, 
                measure_ts DateTime, 
                measure_speed Int32, 
                daily_flag UInt8,  
                iters_cnt UInt8))') AS measures_agg
FROM (
SELECT srid, poo_office_id, src_office_id, is_deleted, is_mp_fin, 
       tupleElement(data_7[1], 'min_ts_step_1') as create_dt,
       if(src_office_id = tupleElement(data_7[f_step_7], 'office_id')
              AND NOT hasAll(arrayMap(x -> x.1, data_9), [34, 67])
              AND hasAny(arrayMap(x -> x.1, data_9), [34, 67]),
          /* если офис сборки совпадает с офисом ПМ, 34 мера совпадает с 67,
             и отсутствующую меру надо задублировать значениями второй */
          arrayPushBack(
              data_9,
              tuple(
                    if(arrayElement(
                           data_9,
                           arrayFirstIndex(x -> x.1 IN (34, 67), data_9) AS src_sorting_idx
                       ).1 = 67, 34, 67) AS measure_code,
                    data_9[src_sorting_idx].2 AS office_id,
                    data_9[src_sorting_idx].3 AS measure_ts,
                    data_9[src_sorting_idx].4 AS duration,
                    data_9[src_sorting_idx].5 AS iters_cnt)),
          data_9) AS sec_msq_agg_final,
       CAST(arrayConcat(
           /* 0 - От создания до полки в ПВЗ. От первого статуса шага 1 до первого статуса шагов 10 или 11 */
           if(m0_idx != 0 AND f_step_1 != 0 AND f_step_1 < m0_idx,
              [(0, 
                poo_office_id, 
                tupleElement(data_7[m0_idx], 'min_ts_step_2'),
                [(tupleElement(data_7[m0_idx], 'min_ts_step_2'), 
                  tupleElement(data_7[1], 'min_ts_step_1'))])],
              []),
           /* 1 - В офисе сборки. От первого статуса меры 1 до последнего статуса в офисе сборки 
              (либо до первого статуса второго шага (кроссбордер)). 
              Должен быть следующий офис после офиса и в офисе сборки должно быть либо больше одной меры, 
              либо одна мера из нескольких экшенов в разное время. */
           if(and(f_step_1 != 0,
                  l_step_src != 0,
                  has_non_src,
                  started_with_111 = 0, /* DataOps-12905 */
                  or(f_step_1 < l_step_src, 
                     tupleElement(data_7[l_step_src], 
                         'min_ts_step_1') > tupleElement(data_7[f_step_1], 'max_ts_step_1'))),
              [(1, 
                src_office_id, 
                coalesce(
                    nullIf(src_leave_crossb, toDateTime(0)),
                    tupleElement(data_7[l_step_src], 'max_ts_step_1')) AS m1_ts,
                [(m1_ts, 
                  tupleElement(data_7[f_step_1], 'min_ts_step_1'))])],
              []),
           /* 2 - В дороге. Сумма всех 56 и 89 мер */
           if(length(on_the_road) != 0,
              [(2, 
                tupleElement(on_the_road[-1], 'office_id'), 
                arrayMax(x -> x.min_ts_step_2, on_the_road),
                arrayMap(x -> x.interval, on_the_road))],
              []),
           /* 3 - В сортировочных центрах (включая ПМ). Сумма всех 64 и 67 мер */
           if(length(in_sc) != 0,
              [(3, 
                tupleElement(in_sc[-1], 'office_id'), 
                arrayMax(x -> x.max_ts_step_2, in_sc),
                arrayMap(x -> x.interval, in_sc))],
              []),
           /* 4 - В ожидании машины. Сумма всех 45, 65 и 78 мер */
           if(length(waiting_for_the_car) != 0,
              [(4, 
                tupleElement(waiting_for_the_car[-1], 'office_id'), 
                arrayMax(x -> x.min_ts_step_2, waiting_for_the_car),
                arrayMap(x -> x.interval, waiting_for_the_car))],
              []),
           /* 70 - От последней сортировки до полки в ПВЗ. От последнего шага 7 до первого статуса шагов 10, 11 или 12
              исключая такие заказы, которые после последней сортировки были сразу переданы курьеру, без ПВЗ */
           if(poo_shelf_idx != 0 AND f_step_7 != 0 AND f_step_7 <= poo_shelf_idx AND from_lm_to_courier = 0,
              [(70, 
                tupleElement(data_7[f_step_7], 'office_id'), 
                tupleElement(data_7[poo_shelf_idx], 'min_ts_step_2'),
                [(tupleElement(data_7[poo_shelf_idx], 'min_ts_step_2'), 
                  tupleElement(data_7[f_step_7], 'max_ts_step_1'))])],
              []),
           /* 24 - Для МП заказов.
              От создания до 1й сортровки на офисе ВБ, или до приемки на офисе ПВЗ, если селлер принес товар на ПВЗ.
              - От первого статуса шага 1 до первого статуса шага 4 для не ПВЗ
              - От первого статуса шага 1 до первого статуса 140 для ПВЗ
              Учитывает пересборка ШК, мера считается для последней удачной сборки ШК и поставки МП на офис ВБ
               */
           if(is_mp_fin in (2,3)
              AND f_step_1 != 0
              AND tupleElement(data_7[f_step_1], 'min_ts_step_1') < l_step_4_after_1_tup.1,
              [(24,
                l_step_4_after_1_tup.2,
                l_step_4_after_1_tup.1,
                [(l_step_4_after_1_tup.1, tupleElement(data_7[f_step_1], 'min_ts_step_1'))])],
              [])
            ),
           'Array(Tuple(
               measure_code UInt8,
               office_id    Int32,
               measure_ts   DateTime,
               intervals    Array(Tuple(DateTime, DateTime))))') AS synth_measures_agg
FROM (
SELECT srid, poo_office_id, src_office_id, is_deleted, poo_shelf_idx, f_step_1, data_7,
       f_step_7, l_step_src, waiting_for_the_car, on_the_road, is_mp_fin, src_leave_crossb,
       l_step_4_after_1_tup, started_with_111, from_lm_to_courier,
       if(from_lm_to_courier, user_idx, poo_shelf_idx) AS m0_idx,
       arrayFirst(
           (x, i) -> x.next_office_id = poo_office_id AND x.step_1 IN (7, 8) AND i <= poo_shelf_idx,
           data_7, arrayEnumerate(data_7)).office_id AS lm_office_id,
       arrayMap( /* до сих пор меры выстроены хронологически, а тут схлопнуты по коду + офису окончания меры */
           (arr) -> tuple(tupleElement(arr[1], 'measure_code') AS measure_code,
                          tupleElement(arr[1], 'office_id') AS office_id, 
                          arrayMax(x -> x.end_ts, arr) AS measure_ts,
                          if(measure_code IN (45, 78), 
                              toInt64(round(arrayAvg(x -> x.duration, arr))), 
                              arraySum(x -> x.duration, arr)) AS duration,
                          length(arr) AS iters_cnt),
           arrayReverseSplit(
               (x, y) -> (x.measure_code, x.office_id) != (y.measure_code, y.office_id),
               data_8,
               arrayShiftLeft(data_8, 1))) AS data_9,
       arrayFirstIndex(
           x -> NOT has(all_src_offices, x.next_office_id), 
           arraySlice(data_7, l_step_src)) > 0 AS has_non_src,
       arrayFilter(x -> x.measure_code IN (64, 67, 65) /* эта 34ая - будущая 67я (двумя шагами вверх) */
                        OR (x.measure_code = 34 AND src_office_id = lm_office_id), data_7) AS in_sc
FROM (
SELECT srid, poo_office_id, src_office_id, all_src_offices, is_deleted, data_7, f_step_1, 
       src_leave_crossb, l_step_4_after_1_tup, started_with_111,
       multiIf(
           and(is_mp, 
               tupleElement(data_7[1], 'step_1') = 1, 
               tupleElement(data_7[1], 'step_2') = 11), 4,
           src_leave_crossb != toDateTime(0), 3, 
           src_office_id = 0, 0,
           started_with_111, 5,
           is_mp +1) AS is_mp_fin,
       arrayFirstIndex(x -> x.step_1 = 7, data_7) AS f_step_7,
       /* f_step_1_ts не обязательно будет первой датой в шаге. Это не так, например, если сначала 
          заказ был создан не с тем ШК, который впоследствии был собран. 18217879615435713.0.0 */
       arrayFirstIndex(
           x -> and(x.step_1 IN (1, 3, 14),
                    x.office_id = src_office_id,
                    f_step_1_ts >= x.min_ts_step_1,
                    f_step_1_ts <  x.min_ts_step_2), 
           data_7) AS f_step_1,
       arrayLastIndex(x -> x.office_id = src_office_id, data_7) AS l_step_src,
       arrayFilter(x -> x.measure_code IN (45, 78), data_7) AS waiting_for_the_car,
       arrayFilter(x -> x.measure_code IN (56, 89), data_7) AS on_the_road,
       arrayExists(x -> (x.step_1, x.step_2) = (7, 11) AND x.next_office_id = poo_office_id, data_7) AS from_lm_to_courier,
       arrayMap(  /* сокращённая и отсортированная версия. next_office_id -> office_id. 
                     Будет использоваться вместе с data_9 для разметки последовательных (не составных) мер */
           x -> CAST(tuple(x.measure_code, x.next_office_id, x.interval.1, x.duration),
                     'Tuple(measure_code UInt8, office_id Int32, end_ts DateTime, duration Int64)'),
           arraySort(x -> (x.measure_code, x.next_office_id), data_7)) AS data_8,
       arrayFirstIndex(
           x -> x.next_office_id = poo_office_id AND x.step_2 IN (10, 11),
           data_7) AS poo_shelf_idx,
       arrayFirstIndex(
           x -> x.office_id = poo_office_id AND x.step_2 = 12,
           data_7) AS user_idx
FROM (
SELECT srid, poo_office_id, src_office_id, all_src_offices, is_deleted, f_step_1_ts, is_mp, l_step_4_after_1_tup, started_with_111,
       /* 32 мера кроссбордера схлопнется с последующей 26 и станет 23-й.
          Потеряется время первого статуса кроссбордер-перевозки, а оно нужно для корректной 1й меры (В офисе сборки) */
       tupleElement(arrayFirst(
           x -> x.step_2 = 2 AND x.office_id = src_office_id, 
           data_6), 'min_ts_step_2') AS src_leave_crossb,
       CAST(
           arraySlice(   /* схлопываю 253 и 254 меры, рассчитываю длительности */
               arrayFold(    /* если 253 (объединить с предыдущей), тот popBack предыдущей и pushBack объединённой.
                                если 254 (объединить со следующей), то на следующем шаге popBack + pushBack */
                   (acc, m, prev_m) ->
                       arrayPushBack(
                           if((m.measure_code = 253 OR prev_m.measure_code = 254) AS treat_prev,
                               arrayPopBack(acc),
                               acc),
                           tuple(
                               if(m.measure_code = 253, acc[-1].1, m.step_1),
                               if(m.measure_code = 253, acc[-1].2, m.step_2),
                               if(m.measure_code = 253, acc[-1].3, m.measure_code),
                               if(m.measure_code = 253, acc[-1].4, m.office_id),
                               if(treat_prev, acc[-1].5, m.min_ts_step_1) AS curr_min_ts,
                               m.min_ts_step_2,
                               m.max_ts_step_1,
                               m.max_ts_step_2,
                               tuple(coalesce(nullIf(m.interval.1, 0), acc[-1].9.1),
                                     coalesce(nullIf(m.interval.2, 0), acc[-1].9.2)) AS curr_interval,
                               tuple(
                                   arrayElement(
                                       [curr_min_ts, m.min_ts_step_2, m.max_ts_step_1, m.max_ts_step_2],
                                       curr_interval.1),
                                   arrayElement(
                                       [curr_min_ts, m.min_ts_step_2, m.max_ts_step_1, m.max_ts_step_2],
                                       curr_interval.2)
                               ) AS curr_borders,
                               curr_borders.1 - curr_borders.2,
                               m.next_office_id
                           )),
                   data_6,
                   arrayShiftRight(data_6, 1),
                   CAST([], 'Array(Tuple(Int8, Int8, UInt8, Int32, DateTime, DateTime, DateTime, DateTime,
                                         Tuple(Int8, Int8), Tuple(Nullable(DateTime), Nullable(DateTime)),
                                         Nullable(Int32), Int32))')),
               /* если срок по последней мере считается вплоть до последнего экшена следующего шага, тогда после мало будет 
                  выкинуть предпоследний шаг (уже сделано). выкидываю ещё один последний, которому не хватает окончания срока.
                  пример - '5235628054892676950.0.0' */ 
            1, if(arrayLast(y -> y != 0, arrayMap(x -> tupleElement(x, 'interval').1, data_6)) = 4, 
                  -1, NULL)),
       'Array(Tuple(step_1         Int8,
                    step_2         Int8,
                    measure_code   UInt8,
                    office_id      Int32,
                    min_ts_step_1  DateTime,
                    min_ts_step_2  DateTime,
                    max_ts_step_1  DateTime,
                    max_ts_step_2  DateTime,
                    _interval      Tuple(Int8, Int8),
                    interval       Tuple(DateTime, DateTime),
                    duration       Int64,
                    next_office_id Int32))') AS data_7
FROM (
SELECT srid, poo_office_id, src_office_id, all_src_offices, is_deleted, f_step_1_ts, is_mp, started_with_111,
       arrayFirstIndex(
           x -> tupleElement(x[1], 'office_id') = poo_office_id AND tupleElement(x[1], 'step') = 12,
           data_5) AS delivered_idx,
       arrayFirstIndex(
           x -> tupleElement(x[1], 'action_id') = 190,  /* отмена заказа */
           data_5) AS cancel_idx,
       arrayLast(
           res, cur_first, prev_step_first, prev_2_step_first, cur_f_620, prev_f_620 -> or(
                /* либо предыдущий офис - это офис сборки, а текущий шаг - один из (4, 13) */
                and(prev_step_first.office_id = src_office_id, -- шаг назад был оформлен
                    or( cur_first.step IN (4,13), -- текущий шаг - сортировка нормальная (шаг 4) или 140м (шаг 13)
                        cur_first.step = 6 and cur_f_620 is not null )), -- или сортировка 620м
                /* либо из офиса сборки было прибытие в офис с action_id=(200, 500, 610) и без 620 в шаге, а затем уже в офис с шагом (4,7) */
                and(prev_2_step_first.office_id = src_office_id, --2 шага назад был оформлен
                    prev_step_first.step = 6, -- шаг назад был принят
                    prev_f_620 is null, --шаг назад не было 620, тк это сортировка
                    cur_first.step in (4,7), --текущий шаг - сортировка нормальная
                    prev_step_first.office_id = cur_first.office_id) -- принятие и сортировка на одном офисе
           ),
           arrayMap(x, i620 -> tuple(x[coalesce(i620,1)].ts, x[1].office_id), data_5, ind_620) as res, /* тупл для вывода, (ts, office_id)  */
           arrayMap(x -> x[1], data_5),   /* первый экшен в шаге */
           arrayMap(x -> x[1], arrayShiftRight(data_5, 1)),   /* первый экшен в предыдущем шаге */
           arrayMap(x -> x[1], arrayShiftRight(data_5, 2)),   /* первый экшен в позапрошлом шаге */
           arrayMap(x -> nullIf(arrayFirstIndex(xx -> xx.action_id = 620, x), 0), data_5) as ind_620, /* ts для первого 620 в текущем шаге */
           arrayMap(x -> nullIf(arrayFirstIndex(xx -> xx.action_id = 620, x), 0), arrayShiftRight(data_5, 1)) /* ts для первого 620 в прошлом шаге */
       ) as l_step_4_after_1_tup,
       CAST(arraySlice(    /* характеристика мер в хронологическом порядке */
           arrayMap(
               (step_first, step_last, next_step_first, next_step_last, prev_step_last, next_2_step_first) -> tuple(
                   step_first.step          AS st_1,
                   next_step_first.step     AS st_2,
                   /* Блок ниже записывает в measure_block:
                      1. measure_code UInt8 Номер меры.
                                    255 - мера с неизвестным action_id
                                    254 - нужно склеить со следующей мерой
                                    253 - нужно склеить с предыдущей мерой
                      2. interval Tuple(Int8, Int8) По каким датам определять длительность меры.
                                    Если обе равны нулю и номер меры 254 и 253, тогда границы интервала будут взяты
                                    у меры, с которой будет схлопнута 254 или 253, иначе - из этого интервала */
                   (multiIf(
                       /* избавляюсь от мер, если вдруг они есть, т.к. их номера заняты */
                       (st_1, st_2) IN ((1, 1), (2, 3), (7, 0), (10, 1), (2,4)),
                       (255, (2, 1)), /* min_ts_step_2 <- min_ts_step_1 */
                       /* если в офисе МП пикнут 130, а следующий офис отличается, тогда "23 - Доставка МП заказа в офис ВБ" */
                       st_1 = 3
                           AND is_mp
                           AND has(all_src_offices, step_last.office_id)
                           AND step_last.office_id != next_step_first.office_id,
                       (23, (2, 3)),  /* min_ts_step_2 <- max_ts_step_1 */

                       /* 23 мера для заказов, начавшихся со 111 статуса. прибытия (6го шага) может не быть
                          51759553615191738.0.0, 11560601614685807.0.0,
                          03ae21e855e6483a9d75f70b89950da4.2.0, 102394772614024330.0.0 */
                       started_with_111
                           AND step_first.office_id = src_office_id
                           AND next_step_first.office_id != src_office_id,
                       (23, (2, 1)),  /* min_ts_step_2 <- min_ts_step_1 */                       

                       /* от кроссбордер-сборки до пика в СЦ - "23 - Доставка МП заказа в офис ВБ". 
                          объединяю с предыдущей 12 (от кроссбордер-сборки до отправки), иначе 12 никуда не попадёт */
                       (st_1, st_2) IN ((2, 6), (2, 4)),
                       if(prev_step_last.step = 3, 
                          (253, (0, 0)), 
                          (23, (2, 1))),

                       /* от кроссбордер-сборки до отправки. сюда вольётся от отправки до прибытия в СЦ (и станет 23ей мерой) */     
                       (st_1, st_2, next_2_step_first.step) IN ((3, 2, 6), (3, 2, 4)),
                       (23, (2, 1)), /* min_ts_step_2 <- min_ts_step_1 */

                       /* в 127 сваливаются все экшены, не отнесённые ни к какому шагу. 255 мера - которая состоит хотя бы из одного 127.
                          а когда следующий шаг - 13 - либо это 23 мера (уже размечена выше), либо непонятно что */                       
                       st_1 = 127 OR st_2 IN (127, 13),
                       (255, (2, 1)), /* min_ts_step_2 <- min_ts_step_1 */
                       /* Было бы хорошо уметь разбить движение из ПВЗ до СЦ на составляющие: ожидание машины и время
                          непосредственно в пути. Но под это не хватает статусов, поэтому всё это время приравниваю к 56 мере.
                          А если коробка как-то оказалась на ПВЗ и едет обратно, засчитываю в транспортировку до СЦ */
                       
                       /* DBW заказы: после МП-сборки (120) сразу выдача курьеру */
                       is_mp AND (st_1, st_2) = (1, 11),
                       (93, (4, 1)), /* max_ts_step_2 <- min_ts_step_1 */

                       /* КГТ, дарксторы: из офиса ПМ сразу выдача курьеру без ПВЗ, приравниваю к 93 */
                       (st_1, st_2) = (7, 11)
                          AND (next_step_first.office_id = poo_office_id OR next_step_first.office_id = 0),
                       (93, (2, 3)),  /* min_ts_step_2 <- max_ts_step_1 */

                       /* 14 шаг - это 115 экшен (задание на сборку). Если есть, появляется 11 мера. 
                          Если нет, то 13 мера включает в себя 11 */
                       (st_1, st_2) = (1, 14) 
                            AND step_first.office_id = next_step_first.office_id,
                       (11, (4, 1)), /* max_ts_step_2 <- min_ts_step_1 */

                       (st_1, st_2) = (14, 3),
                       (13, (4, 3)), /* max_ts_step_2 <- max_ts_step_1 */

                       st_1 = 13,
                       if((is_mp AND has(all_src_offices, prev_step_last.office_id))
                              OR (st_2 = 6 AND next_step_first.action_id = 1500),
                          (56, (2, 3)), /* min_ts_step_2 <- max_ts_step_1 */
                          (255, (2, 1))), /* min_ts_step_2 <- min_ts_step_1 */
                       /* если офис сборки = офис ПМ, и между 3* и 6|7* - разрыв. если далее 6*
                          статусы, ты объединяю с ними. если 7*, то делаю из 3* -> 6*.
                          а если не ПМ, то делаю 34, и след. меру объединяю с этой */
                       st_1 = 3
                           AND st_2 IN (6, 7)
                           AND step_last.office_id = next_step_first.office_id,
                       if(step_last.next_distinct_office = poo_office_id,
                           if(st_2 = 6,
                              (254, (0, 0)),
                              (67, (4, 1))),  /* max_ts_step_2 <- min_ts_step_1 */
                           (34, (4, 1))), /* max_ts_step_2 <- min_ts_step_1 */
                       /* см. предыдущую корректировку */
                       (prev_step_last.step, st_1) = (3, 6)
                           AND step_first.office_id = prev_step_last.office_id
                           AND step_first.next_distinct_office != poo_office_id,
                       (253, (0, 0)),
                       /* схлопываю 67 и 76, 46 и 64, если они между шагами из того же офиса (кружения по офису) */
                       (prev_step_last.step, st_1, st_2) IN ((7, 6, 7), (6, 7, 6), (4, 6, 4), (6, 4, 6))
                           AND next_step_first.office_id = step_first.office_id
                           AND step_first.office_id = prev_step_last.office_id,
                       (253, (0, 0)),
                       /* нет этапа с транспортировкой, когда перемещение между однокрышниками */
                       st_1 = 4 AND st_2 IN (6, 7)
                           AND step_first.office_id = next_step_first.office_id,
                       if( /* если пред. шаг в том же офисе (исключая погрузки в машину), склеиваем
                              с предыдущей мерой. если не в том же, то со следующей */
                           step_first.office_id = prev_step_last.office_id
                           AND prev_step_last.action_id NOT IN (400, 800),
                               /* если след.шаг 5 или 8 (т.е. следующая мера - ожидание машины,
                                  то длительность этой склеенной меры - до последнего экшена ожидания) */
                           tuple(253, if(next_2_step_first.step IN (8, 5), (4, 1), (0, 1))),
                               /* склеивается со следующей мерой в том же офисе. напр., 51239829097372437.0.0 */
                           tuple(254, (0, 0))), /* min_ts_step_2 <- min_ts_step_1 */
                       /* 65 (транзит) может не склеиться из-за того, что между 6 и 5 вклинится 320-й экшен 4* шага. */
                       (st_1, st_2, next_2_step_first.step) = (6, 4, 5)
                           AND next_step_first.action_id = next_step_last.action_id
                           AND next_step_first.action_id = 320,
                                      /* до максимального, т.к. бывают серии одинаковых экшенов: 101109698613763737.0.0. 
                                         след. статус (транспортировка) начинается с max_ts_step_1 */
                       (65, (4, 1)),  /* max_ts_step_2 <- min_ts_step_1 */
                       /* если последний экшен шага 4 - коробка или буфер, нет погрузки и следующий шаг
                          6* в другом офисе, то засчитываю время между шагами в транспортировку.
                          то же самое, если нет статуса погрузки в машину до ПВЗ */
                       (st_1, st_2) IN ((4, 6), (7, 9))
                           AND step_first.office_id != next_step_first.office_id
                           AND step_last.action_id IN (310, 320, 640, 700),
                       ((st_1 + 1) * 10 + st_2, (2, 3)), /* min_ts_step_2 <- max_ts_step_1 */
                       /* если два подряд прибытия в офис без промежуточных статусов,
                          склеиваю с предыдущей мерой (как правило, транспортировка) */
                       (st_1, st_2) = (6, 6),
                       (253, (0, 0)),
                       /* если в офисе ПМ не вскрывалась коробка до ПВЗ, нет сортировки (67 меры) - получается 68я.
                          приравниваю 68-ую меру 78-ой (ожидание машины из ПМ в ПВЗ). */
                       (st_1, st_2) = (6, 8),
                       (78, (2, 3)),  /* min_ts_step_2 <- max_ts_step_1 */
                       /* 80 меру (которой не хватило 900 или 910 экшенов) превращаю в 89 */
                       st_1 = 8 AND st_2 IN (10, 11, 12),
                       (89, (2, 1)), /* min_ts_step_2 <- min_ts_step_1 */
                       /* если после прибытия на СЦ из сортировочных экшенов 7* есть только помещение в буфер,
                          тогда схлопываю 67 c 7* мерой, потому что сортировки не было - был транзит.
                          кроме тех случаев, когда через один шаг - выдача курьеру (59267122113004408.0.0) */
                       (st_1, st_2) IN (6, 7)
                           AND next_step_first.action_id = 700
                           AND next_step_first = next_step_last
                           AND next_2_step_first.step != 11,
                       (254, (0, 0)),
                       /* если водитель не пикнул прибытие в офис транзита, то 55 заменяю на 56 (В пути) */
                       (st_1, st_2, next_2_step_first.step) = (5, 5, 6),
                       (56, (2, 1)), /* min_ts_step_2 <- min_ts_step_1 */
                       /* в остальных случаях просто склеиваем инты для получения номера меры */

                       /* меры, оба шага которых должны быть из одного офиса. 
                          03ae21e855e6483a9d75f70b89950da4.2.0 DAS.i4213dd13080c900c979a7ed42c32592d.0.0 */ 
                       (st_1, st_2) IN ((1, 3), (3, 4), (6, 4), (6, 7))
                           AND step_first.office_id != next_step_first.office_id,
                       (255, (2, 1)),

                       tuple(
                           transform(
                               st_1 * 100 + st_2,
                               [910, 911, 1011, 1112, 1012],
                               [90, 93, 93, 98, 99],
                               if(st_1 >= 10 OR st_2 >= 10, 255, st_1 * 10 + st_2)) AS _m_code,
                           multiIf(
                               _m_code IN (45, 78, 90), (2, 3), /* min_ts_step_2 <- max_ts_step_1 */
                               _m_code IN (13, 65, 67, 89, 64, 93), (4, 1), /* max_ts_step_2 <- min_ts_step_1 */
                               _m_code IN (56, 99, 98), (2, 1), /* min_ts_step_2 <- min_ts_step_1 */
                               _m_code IN (34),
                                   if(step_last.office_id = next_step_first.office_id,
                                       (4, 3),   /* max_ts_step_2 <- max_ts_step_1 */
                                       (2, 3)),  /* min_ts_step_2 <- max_ts_step_1 */
                               (2, 1)        /* min_ts_step_2 <- min_ts_step_1 */
                           )
                       )
                   ) AS measure_block).1     AS measure_code,
                   step_first.office_id      AS office_id,
                   step_first.ts             AS min_ts_step_1,
                   next_step_first.ts        AS min_ts_step_2,
                   step_last.ts              AS max_ts_step_1,
                   next_step_last.ts         AS max_ts_step_2,
                   measure_block.2           AS _interval_,
                   next_step_first.office_id AS next_office_id),
               arrayMap(x -> x[1], data_5),   /* первый экшен в шаге */
               arrayMap(x -> x[length(x)], data_5),   /* последний экшен в шаге */
               arrayMap(x -> x[1], arrayShiftLeft(data_5, 1)),   /* первый экшен в следующем шаге */
               arrayMap(x -> x[length(x)], arrayShiftLeft(data_5, 1)),   /* последний экшен в следующем шаге */
               arrayMap(x -> x[length(x)], arrayShiftRight(data_5, 1)),   /* последний экшен в предыдущем шаге */
               arrayMap(x -> x[1], arrayShiftLeft(data_5, 2))   /* первый экшен в шаге через один вперёд */
           ),
           /* всё до 12 шага (выдачи, возврата или отмены по сроку) включительно,
              либо до предпоследнего шага включительно (если нет 12 шага, значит, последний не окончен),
              либо до предпоследнего перед отменой */
           1, arrayMin(arrayFilter(x -> x > 0, [delivered_idx, cancel_idx -1])) -1),
       'Array(Tuple(step_1         Int8,
                    step_2         Int8,
                    measure_code   UInt8,
                    office_id      Int32,
                    min_ts_step_1  DateTime,
                    min_ts_step_2  DateTime,
                    max_ts_step_1  DateTime,
                    max_ts_step_2  DateTime,
                    interval       Tuple(Int8, Int8),
                    next_office_id Int32))') AS data_6
FROM (
SELECT srid, poo_office_id, is_mp, src_office_id, all_src_offices, is_deleted, f_step_1_ts, started_with_111,
       arrayReverseSplit(
            /* сплитую, чтобы последовательности экшенов, отнесённых к одному шагу (step)
               и произошедших в одном офисе, лежали в одном вложенном массиве */
           (x, y) -> (x.office_id, x.step) != (y.office_id, y.step),
           data_4,
           arrayShiftLeft(data_4, 1)) AS data_5
FROM (
SELECT srid, poo_office_id, is_mp, src_office_id, all_src_offices, is_deleted, f_step_1_ts, started_with_111,
       arrayMap(   /* замена некорректных action_id перед определением номера шага */
           (action_id, office, between_actions, between_office) -> multiIf(
               /* 1500 статусом ошибочно обозначают прибытие товара со склада МП через ПВЗ на склад ВБ */
               action_id = 1500 AND is_mp, 200,
               /* Если первым статусом МП-заказа в СЦ является не прибытие (шаг 6*), а 220, то 220 => 620.
                  Причина в том, что 220 экшен отнесён к шагу 4* (экшены сортировки). А чтобы возникла
                  мера сортировки, нужен один 3* или 6* шаг, т.е. отправная точка для сортировки */
               action_id IN 220 AND is_mp AND between_actions.1 IN (1070, 140, 130), 620,
               /* кроссбордер или отказной товар прибывает в СЦ (схоже с тем, что выше)
                  либо нет отметок о прибытии-принятии после транспортировки (во избежание 54 меры)
                  например, 30245645614675641.0.0. подменяю на экшен 620, чтобы начать шаг 6 */
               action_id = 220 
                   AND between_actions.1 IN (111, 112, 800, 400, 116, 117, 121, 122, 123, 124, 125, 131) 
                   AND office != between_office.3, 620,
               /* когда следующий офис не пвз, а статусы характерны для ПМ */
               action_id IN (630, 640, 700, 800)  /* 59267122113004408.0.0, когда из ПМ - сразу клиенту */ 
                   AND office != poo_office_id  /* следующий - не-poo либо офис, из которого доставка курьером */
                   AND between_office.2 = 0,
               transform(action_id, [630, 640, 700, 800], [230, 310, 320, 400], action_id),
               /* если предыдущий статус >= 630 и следующий статус из ПВЗ и следующый статус выше 900, 
                  400 заменяется на 800 и 320 на 700. напр., 007f346b3de04bde9db441a3e89c7892 (400) и 51759553615191738.0.0 (320) */
               action_id IN (320, 400) 
                   AND between_actions.1 >= 630 
                   AND between_actions.2 >= 900 
                   AND between_office.2 = 1, 
               if(action_id = 320, 700, 800),
               /* прибыл в ПВЗ иногда ошибочно пикают, прибывая в СЦ */
               action_id = 900 AND office != poo_office_id AND between_actions.2 < 900, 500,
               action_id),
           actions,
           offices,
           between_distinct_actions,
           between_distinct_offices) AS correct_action_ids,
       arrayMap(   /* замена некорректных office_id */
           (action_id, office, ts, between_actions, between_office) -> multiIf(
               /* кроссбордер. почему-то статусы летят от 507 Коледино */
               action_id IN (116, 117, 121, 122, 123, 124, 125, 131) AND ts > f_step_1_ts, src_office_id,
               /* прибывая в неверный ПВЗ, отправляют тот ПВЗ, куда товар должен был попасть, но не попал.
                  dAa.241e19d1b59e4c27816a29177f2d59b7.0.1, но надо аккуратно, т.к. не всегда: 6342614075356745847.0.0 */
               and(action_id = 900,
                   office = poo_office_id,
                   office != between_actions.3,
                   between_actions.2 IN (1020, 1075)), between_actions.3,
               /* В DBW заказах курьерские статусы часто без офиса. меняю на офис сборки. напр., 5947594748605523257.1.0 */
               and(action_id IN (1010, 1030, 1040, 1050, 1035, 1090, 1041),
                   office = 0,
                   has(all_src_offices, between_office.3)), between_office.3,
               /* прибывают в верный ПВЗ, но отправляют неверный. напр., 5526378239213564993.0.0.
                  или ПВЗ-шные статусы, смежные со статусами из настоящего ПВЗ, отправляют из неправильного. 
                  напр., dy.rfd948a19ce7947ee909d19eb45024286.0.0 */
               and(action_id >= 900,
                   action_id != 1500,  /* 51424048112180360.0.2 */
                   poo_office_id != 0,
                   or(poo_office_id = between_office.1, 
                      poo_office_id = between_office.3)), poo_office_id, 
               /* если все статусы ПВЗ пришли из офиса 0 и ПВЗ определился как 0, значит, это курьерка */
               and(action_id IN (1010, 1030, 1040),
                   office = 0,
                   poo_office_id = 0), between_office.3, 
               office),
           actions,
           offices,
           arrayMap(x -> x.1, data_3),
           between_distinct_actions,
           between_distinct_offices) AS correct_office_ids,           
       arrayMap(   /* здесь каждому экшену соотносится номер шага */
           (tup, correct_action_id, correct_office_id,  next_distinct_office) -> CAST(
                tuple(
                    tup.1 AS ts,
                    correct_office_id,
                    correct_action_id,
                    next_distinct_office.1,
                    transform(
                        correct_action_id,
                        [110, 111, 120,                 /* 1 - оформлен заказ, создан сборончый лист, отправлен на сборку */
                         116, 117, 121, 122, 123, 124, 125, 131,  /* 2 - доставка из заграницы, пересечение границы, таможенное оформление */
                         112, 210, 130,                 /* 3 - собран на складе или складе МП */
                         220, 230, 310, 320,            /* 4 - предсорт, сорт и доставка до буфера отгрузки гофры или коробки */
                         400, 170,                      /* 5 - межскладская транспортировка или из ПВЗ до СЦ: коробка прибыла на ПВЗ, прибыла на СЦ */
                         200, 500, 610, 620, 1500,      /* 6 - прибыл на СЦ, пикнут на СЦ, предсорт на СЦ, прибыл возврат с ПВЗ */
                         630, 640, 700,                 /* 7 - статусы СЦ: предсорт, сорт до ПВЗ, закрытие коробки до ПВЗ и доставка до буфера до ПВЗ */
                         800,                           /* 8 - транспортировка до ПВЗ: коробка погружена из ЛМ до ПВЗ */
                         900, 910,                      /* 9 - ПВЗ: коробка прибыла на ПВЗ, приемка коробки */
                         1000,                          /* 10 - ПВЗ: принят, разложен на полку */
                         1010,                          /* 11 - ПВЗ: выдано курьеру */
                         1030, 1040, 1050, 1035, 1041,  /* 12 - Пользователь принял решение, забрать или вернуть товар */
                         140, 1070, 1080, 1075,         /* 13 - ПВЗ для МП или возвратов: собрано в ПВЗ, запикано в возвратную коробку, отправка отгрузки */
                         115                            /* 14 - Задание на сборку. Есть только у у не-МП офисов сборки */  
                         ],
                        [1,1,1,
                         2,2,2,2,2,2,2,2,
                         3,3,3,
                         4,4,4,4,
                         5,5,
                         6,6,6,6,6,
                         7,7,7,
                         8,
                         9,9,
                         10,
                         11,
                         12,12,12,12,12,
                         13,13,13,13,
                         14
                         ],
                        127  /* max Int8 */
                    ) AS action_id),
                'Tuple(ts DateTime, office_id Int32, action_id UInt16, next_distinct_office Int32, step Int8)'),
           data_3,
           correct_action_ids,
           correct_office_ids,
           between_distinct_offices) AS data_4
FROM (
SELECT srid, poo_office_id, is_mp, src_office_id, all_src_offices, is_deleted, actions, offices, data_3, 
       f_step_1_tuple.1 AS f_step_1_ts,
       f_step_1_tuple.2 = 111 AS started_with_111,
       arrayFlatten(
           arrayMap(   /* ближайший и предыдущий экшен, которые отличаются от текущего */
               (gr, prev_gr, next_gr) -> arrayWithConstant(length(gr), tuple(prev_gr[1].1, next_gr[1].1, next_gr[1].2)),
               actions_split,
               arrayShiftRight(actions_split, 1),
               arrayShiftLeft(actions_split, 1))) AS between_distinct_actions,
       arrayFlatten(
           arrayMap(   /* ближайший офис, и является ли он ПВЗ, и предпоследний офис */
               (gr, next_gr, prev_gr) -> arrayWithConstant(
                       length(gr), tuple(next_gr[1], next_gr[1] = poo_office_id, prev_gr[1])),
               offices_split,
               arrayShiftLeft(offices_split, 1),
               arrayShiftRight(offices_split, 1))) AS between_distinct_offices
FROM (
SELECT srid, is_mp, src_office_id, is_deleted, data_3, all_src_offices,
       /* последний офис нулевого шага перед одним из клиентских статусов (если такие есть) 
          при условии, что этот офис хотя бы раз есть в поле dst_office_id срид-трекера */
       nullIf(arrayLast(
            y -> y.2 IN (1000, 1010, 1030, 1040, 1050, 1035, 1041), 
            arraySlice(data_3, 1, nullIf(arrayFirstIndex(z -> z.2 IN (1030, 1040, 1050), data_3), 0))
       ).3, 0) AS last_reached_dst,
       /* последний офис ПМ, возникший перед курьерским статусом из нулевого офиса. 59267122113004408.0.0 */
       nullIf(arrayLast(
            y -> y.2 IN (640, 700), 
            arraySlice(data_3, 1, arrayFirstIndex(z -> z.2 = 1010 AND z.3 = 0, data_3))
       ).3, 0) AS last_reached_before_courier,
       coalesce(last_reached_dst, last_reached_before_courier, last_dst_from_st, 0) AS poo_office_id,
       arrayMap(x -> x.2, data_3) AS actions,
       arrayMap(x -> x.3, data_3) AS offices,
       arraySplit((x, next) -> next != 0, 
           arrayMap(x -> tuple(x.2, x.3), data_3), 
           arrayDifference(actions)) AS actions_split,
       arraySplit((x, next) -> next != 0, offices, arrayDifference(offices)) AS offices_split,
       arrayFirst(
           x -> and(x.2 IN (110, 111, 115, 120, 121, 112, 130, 140, 210), 
                    x.3 = src_office_id,
                    x.4 = coalesce(nullIf(last_shk, 0), x.4)),
           data_3) AS f_step_1_tuple
FROM (
SELECT srid, last_dst_from_st, is_mp, src_office_id, all_src_offices, last_shk, 
       length(data_2) > 1000 OR is_deleted AS is_deleted,
       /* некоторые статусы никак и нигде не нужно учитывать. их проще сразу исключить */
       if(length(data_2) > 1000, [],
        arrayFilter(
            (x, next_action, prev_action) -> NOT or(
                   (x.2 IN (220, 610, 1080, 1500)    /* 220, 610 и 1080 статус из офиса, который отличается от предыдущего и от следующего. */
                        AND x.3 != next_action.3     /* эти статусы попадались не на своём месте */
                        AND x.3 != prev_action.3     /* (220 - '37611816097264545.0.0', 1500 - '11267324098566732.0.0') */
                        AND next_action.3 != 0),
                   (x.2 = 610                        /* 610, если предыдущий статус из того же офиса < 310 (Закрыли полибокс). во избежание 36, 16... мер */
                        AND prev_action.2 < 310
                        AND prev_action.3 = x.3),
                   (x.2 = 140                        /* 140 (Собран на ПВЗ), если предыдущий статуса больше 900 и из того же офиса. */
                        AND prev_action.2 >= 900     /* Пример ошибки - '6998730968976302997.0.0' */
                        AND prev_action.3 = x.3),
                   (x.2 = 1000                       /* 1000 статус из офиса, который отличается от предыдущего и от следующего, */
                        AND prev_action.2 < 640      /* и предыдущий экшен меньше 640. напр., '20186294104353457.2.0' */
                        AND x.3 != next_action.3
                        AND x.3 != prev_action.3
                        AND next_action.3 != 0),
                   (x.2 IN (1010, 1030, 1040)        /* исключаю не-МП заказы (т.к. у МП заказов так работает DBW доставка), у которых */
                        AND x.3 = 0                  /* нулевой офис (т.к. баг найден с ошибочными статусами курьерки из нулевых офисов, dk.rf9fd55ddb2b2456b8e92b4dec32f4751.0.0), */
                        AND is_mp = 0                /* следующий статус ниже 1010 (т.е. реальной доставки не было) и статус не окружён 1м шагом (т.к. это тоже похоже на DBW) */  
                        AND prev_action.2 NOT IN (0, 110, 111, 120)
                        AND next_action.2 NOT IN (0, 110, 111, 120)
                        AND next_action.2 < 1010),
                   (x.2 IN (400, 800)                /* 400 и 800, если до и после него статусы на том же офисе и след. экшен не отметка о прибытии */
                        AND x.3 = next_action.3
                        AND x.3 = prev_action.3
                        AND next_action.2 NOT IN (500, 610, 900, 910)),
                   (x.2 IN (110, 111, 120)          /* исключаю случайно засланные экшены шага 1* (задания на сборку), из офисов, в которых */ 
                        AND prev_action.2 != 0      /* не было сборки. исключение - первый статус в истории срида (для точки отсчёте по 0-й мере) */
                        AND NOT has(all_src_offices, x.3)),
                   (x.2 = 900                       /* ошибочные 900-е. перед ними экшен <630 и следующий статус не из ПВЗ */
                        AND prev_action.2 < 630
                        AND next_action.3 < 900
                        AND next_action.3 != 0
                        AND next_action.3 != x.3),
                   (x.2 = 900                       /* ошибочные 900-е. их окружают статусы большие чем 900 из того же офиса */
                        AND prev_action.2 > 900
                        AND next_action.2 > 900
                        AND x.3 = next_action.3
                        AND x.3 = prev_action.3)
                ),
            arraySlice(data_2, f_step_1_slice) AS _raw_tuples,
            arrayShiftLeft(_raw_tuples, 1),
            arrayShiftRight(_raw_tuples, 1))) AS data_3

FROM (
SELECT srid, last_dst_from_st, is_mp, src_office_id, is_deleted, all_src_offices, data_2, last_shk,
       greatest(1,
           arrayFirstIndex(
               x -> x.2 IN (110, 111, 120),
               data_2)) AS f_step_1_slice
FROM (
SELECT srid, last_dst_from_st, is_deleted, data_2, last_shk,
       /* гарантирую, что src_office_id будет в массиве all_src_offices */
       arrayPushBack(arrayMap(x -> x.1, all_src_offices_raw), src_office_id) AS all_src_offices,
       coalesce(nullIf(last_shk_src_office, 0), src_off_raw) AS src_off_raw_fin,
       dictGet('dict.branch_office', ('main_office_id', 'office_type'), src_off_raw_fin) AS src_roof_raw,
       src_roof_raw.2 = 'mp' AS is_mp,
       if(or(src_roof_raw.1 = 0, 
             xor(is_mp, dictGet('dict.branch_office', 'office_type', src_roof_raw.1) = 'mp')),
          src_off_raw_fin,
          src_roof_raw.1) AS src_office_id
FROM (
SELECT srid, last_dst_from_st, is_deleted, src_off_raw,
       arrayMap(
           p -> tuple(p[1].3, p[1].4),
           arrayFilter(     /* нахожу в data_1 статус, с которого начиналось движение последнего ШК. DataOps-12476 */ 
               z -> hasAll(arrayMap(o -> if(o.2 IN (110, 111, 115, 120), 1, 2), z), [1, 2]),
               arraySplit(    /* запрос проверяет, чтобы после сплита внутри массива было создание / задание на сборку... */
                   (x, y) -> (x.4 != y.4) OR (x.3 != y.3),   /* ...вместе со сборкой для одного ШК внутри одного офиса */
                   arrayFilter(w -> w.2 IN (110, 111, 115, 112, 120, 121, 130, 140, 210) AND w.4 != 0, data_1) AS src_st, 
                   arrayShiftRight(src_st, 1)))) AS all_src_offices_raw,
       all_src_offices_raw[-1].1 AS last_shk_src_office,
       all_src_offices_raw[-1].2 AS last_shk,
       arrayFilter(   /* убираю дубликаты по экшену + офису, кроме первого и последнего */
           (_, current, next, prev) -> NOT (current = next AND current = prev),
           data_1,
           arrayMap(x -> (x.2, x.3), data_1) AS no_dt_tuples,
           arrayShiftLeft(no_dt_tuples, 1),
           arrayShiftRight(no_dt_tuples, 1)) AS data_2

FROM (
       SELECT srid,   /* группировка по сриду (дальше не меняется) */
              argMaxOrNull(dst_office_id, ts) AS last_dst_from_st,
              coalesce(
                  argMinIfOrNull(office_id, ts, action_id IN (112, 121, 130, 140, 210)),
                  argMinIf(office_id, ts, action_id IN (110, 111, 115, 120))) AS src_off_raw,
              arrayCompact(
                  arraySort(
                      groupArray(
                          tuple(
                              ts + toIntervalHour(3),
                              action_id,
                              if((dictGet('dict.branch_office', 'office_type', office_id) = 'mp'
                                  ) = (dictGet(
                                          'dict.branch_office', 
                                          'office_type',
                                          dictGetOrNull('dict.branch_office', 'main_office_id', office_id)) = 'mp'),
                                  dictGet('dict.branch_office', 'main_office_id', office_id),
                                  office_id),
                              shk_id
                              )))) AS data_1,
              or(
                  ifNull(argMax(payment_type, ts) LIKE 'S__', 0),   /* срид возвратный */
                  max(ts) - min(ts) >= 31536000          /* время жизни срида больше года */
                      /* или если больше тысячи записей в истории срида (посчитается после дедупликации) */
              ) AS is_deleted
         FROM core_wh.srid_tracker     /* если больше 12, то is_deleted. ещё +2 месяца, чтобы мог найтись */ 
        WHERE ts > (SELECT min(ts) - toIntervalMonth(14)     /* такой экшен, с которым станет 12+ */
                      FROM buffer.v3_srid_set 
                     WHERE integer_dag_id=%(integer_dag_id)s)
          AND srid IN (
                    SELECT srid 
                      FROM buffer.v3_srid_set
                     WHERE integer_dag_id = %(integer_dag_id)s)
          AND action_id IN %(actions_white_list)s
     GROUP BY srid     
)))))))))))))) AS measures

  LEFT ANY 
      JOIN (SELECT srid,
                   ifNull(sm_id, 0) AS sm_id,
                   toDateOrDefault(delivery_time) AS ddate,
                   ifNull(nm_id, 0) AS nm_id,
                   if(currency_id = 643, ifNull(price, 0),
                       toInt64(round(ifNull(price, 0) * dictGetFloat64OrDefault(
                           'dict.cbr_currency',
                           'rate',
                           (toUInt64OrDefault(currency_id), toDate(create_ts)),
                           dictGetFloat64OrDefault(
                               'dict.cbr_currency',
                               'rate',
                               (toUInt64OrDefault(currency_id), today() - 3),
                               1))))) AS price
              FROM positions.last_srid_position_v3 FINAL
          PREWHERE srid IN (
                       SELECT srid 
                         FROM buffer.v3_srid_set
                        WHERE integer_dag_id = %(integer_dag_id)s)
               AND (toYear(coalesce(toDateTime(%(pos_min_cutoff)s), toDateTime(0))) = 1970 
                    OR create_ts > %(pos_min_cutoff)s - toIntervalMonth(6))
        ) AS positions
  USING (srid)
)
WHERE measure_ts >= %(v3_date_start)s
SETTINGS max_memory_usage='140G',
         max_insert_threads=5,
         optimize_on_insert=0,
         optimize_aggregation_in_order=%(group_by_in_order)s,
         max_bytes_before_external_group_by='80G',
         do_not_merge_across_partitions_select_final=1
"""


LAKE_LOAD_HISTORY = """
         /* через GROUP BY такие большие объёмы в IN-е быстрее проходят, чем через FINAL */ 
  INSERT INTO buffer.v3_srid_history_d 
         (integer_dag_id, srid, measure_code, measure_office_id, 
          src_office_id, poo_office_id, measure_ts, measure_speed, create_dt,
          daily_flag, is_mp, nm_id, price, is_deleted, ddate, sm_id)
  SELECT %(integer_dag_id)s, srid, measure_code, measure_office_id,
         untuple(argMax(
             tuple(src_office_id, poo_office_id, measure_ts, measure_speed, create_dt,
                   daily_flag, is_mp, nm_id, price, is_deleted, ddate, sm_id),
             (row_created, is_deleted * -1)))
    FROM datamart.v3_by_srid
PREWHERE srid IN (SELECT srid 
                    FROM buffer.v3_srid_set_d
                   WHERE integer_dag_id=%(integer_dag_id)s)
   WHERE measure_ts > (SELECT min(ts) - toIntervalMonth(14)
                         FROM buffer.v3_srid_set_d
                        WHERE integer_dag_id=%(integer_dag_id)s) 
         /* toMonday нужно чтобы схлопнуть так, как схлопнул бы FINAL. 
            и выставить is_deleted сридам из других партиций */
GROUP BY srid, measure_code, measure_office_id, toMonday(measure_ts)
         /* если вставились в одно время, предпочтение строчке без is_deleted */
  HAVING argMax(is_deleted, (row_created, is_deleted * -1)) != 1
SETTINGS max_bytes_before_external_group_by='80G',
         aggregation_memory_efficient_merge_threads=50,  /* 96 по умолчанию */
         optimize_on_insert=0, 
         max_insert_threads=3
"""

LAKE_PROCESS_CHANGED_DATA = """
SET allow_suspicious_low_cardinality_types=1;

CREATE TEMPORARY TABLE v3_changed_data
ENGINE = MergeTree
PARTITION BY is_deleted
ORDER BY tuple()
AS
   SELECT coalesce(rec.srid, his.srid) AS srid, 
          coalesce(rec.measure_code, his.measure_code) AS measure_code,
          coalesce(rec.measure_office_id, his.measure_office_id) AS measure_office_id,
          isNull(rec.srid) AS new_row_is_null,
          isNull(his.srid) AS old_row_is_null,
          rec.src_office_id AS src_office_id, 
          rec.poo_office_id AS poo_office_id,
          rec.measure_speed AS measure_speed,
          rec.create_dt, 
          rec.daily_flag AS daily_flag, 
          rec.is_mp AS is_mp,
          rec.nm_id AS nm_id, 
          rec.price AS price, 
          rec.ddate AS ddate, 
          rec.sm_id AS sm_id, 
          rec.iters_cnt AS iters_cnt,
          his.measure_ts AS hist_measure_ts,
          his.src_office_id AS hist_src_office_id,
          his.poo_office_id AS hist_poo_office_id,
          his.measure_office_id AS hist_measure_office_id,
          his.is_mp AS hist_is_mp,
          his.sm_id AS hist_sm_id,
          assumeNotNull((arrayJoin(multiIf(
              isNull(rec.measure_ts), 
              [(True, his.measure_ts)],
                 /* хоть партиции и по неделям, но шардирование по дням недели. так что меры, 
                    сменившие даты, надо отменять отдельной строчкой с is_deleted */
              toDate(rec.measure_ts) != toDate(his.measure_ts),
              [(True, his.measure_ts), (False, rec.measure_ts)],
              [(rec.is_deleted, rec.measure_ts)])) AS flag_and_ts).1) AS is_deleted,
          flag_and_ts.2 AS measure_ts
              
     FROM (SELECT * FROM buffer.v3_srid_recent  WHERE integer_dag_id = %(integer_dag_id)s) AS rec
FULL JOIN (SELECT * FROM buffer.v3_srid_history WHERE integer_dag_id = %(integer_dag_id)s) AS his
       ON rec.srid = his.srid
      AND rec.measure_code = his.measure_code
      AND rec.measure_office_id = his.measure_office_id
    WHERE coalesce(
             or(rec.measure_ts    != his.measure_ts, 
                rec.measure_speed != his.measure_speed,
                rec.poo_office_id != his.poo_office_id, 
                rec.src_office_id != his.src_office_id, 
                rec.daily_flag    != his.daily_flag, 
                rec.is_mp         != his.is_mp, 
                rec.nm_id         != his.nm_id, 
                rec.ddate         != his.ddate, 
                rec.sm_id         != his.sm_id, 
                rec.price         != his.price, 
                rec.create_dt     != his.create_dt,
                rec.is_deleted    != his.is_deleted), 
             True)
 SETTINGS join_use_nulls=1
;

  INSERT INTO datamart.v3_by_srid_d
         (srid, measure_code, measure_office_id, measure_ts, is_deleted)
  SELECT srid, measure_code, measure_office_id, measure_ts, True AS is_deleted
    FROM v3_changed_data
   WHERE is_deleted = 1
SETTINGS optimize_on_insert=0, max_insert_threads=2
;

  INSERT INTO datamart.v3_by_srid_d
         (srid, is_mp, src_office_id, poo_office_id, is_deleted, nm_id,
          price, ddate, sm_id, measure_code, measure_office_id, measure_ts, create_dt, 
          measure_speed, daily_flag, iters_cnt) 
  SELECT srid, is_mp, src_office_id, poo_office_id, is_deleted, nm_id,
         price, ddate, sm_id, measure_code, measure_office_id, measure_ts, create_dt,
         measure_speed, daily_flag, iters_cnt
    FROM v3_changed_data
   WHERE is_deleted = 0
SETTINGS optimize_on_insert=0, max_insert_threads=2
;

CREATE TEMPORARY TABLE v3_changed_offices_tmp
ENGINE=Memory
AS
   SELECT DISTINCT 
          changed.1 AS measure_date,
          changed.2 AS measure_code,
          changed.3 AS src_office_id,
          changed.4 AS poo_office_id,
          changed.5 AS measure_office_id,
          changed.6 AS is_mp,
          changed.7 AS sm_id
     FROM (
              SELECT arrayJoin([
                         (toDate(measure_ts), measure_code, src_office_id, 
                              poo_office_id, measure_office_id, 
                              is_mp, sm_id, new_row_is_null), 
                         (toDate(hist_measure_ts), measure_code, hist_src_office_id, 
                              hist_poo_office_id, hist_measure_office_id, 
                              hist_is_mp, hist_sm_id, old_row_is_null)
                     ]) AS changed
                FROM v3_changed_data
               WHERE coalesce(measure_code) IN %(all_codes)s
          )
    WHERE isNotNull(measure_date)
      AND measure_date >= %(v3_date_start)s
      AND changed.8 != 1
;

   INSERT INTO buffer.v3_changed_offices_d
          (dag_id, measure_date, measure_code, src_office_id, 
           poo_office_id, measure_office_id, is_mp, sm_id)
   SELECT %(dag_id)s, measure_date, measure_code, src_office_id, 
          poo_office_id, measure_office_id, is_mp, sm_id
     FROM v3_changed_offices_tmp
    WHERE %(is_manual)s = False
 SETTINGS max_partitions_per_insert_block=0
;

   INSERT INTO buffer.v3_queue 
          (dag_id, week_start, days_changed)
   SELECT %(dag_id)s, week_start, 
          groupUniqArray(measure_date) AS days_changed
     FROM v3_changed_offices_tmp
 GROUP BY toMonday(measure_date) AS week_start
"""


INSERT_CALCED_TO_QUEUE = """
  INSERT INTO public.v3_queue
         (week_start, queued_at, src_dag_id, days_changed)

  SELECT week_start, 
         now() AS queued_at, 
         %(dag_id)s AS src_dag_id,
         arraySort(
              groupUniqArrayArray(
                  arrayConcat(
                      new.days_changed, 
                      last_actual.days_changed))) AS days_changed
                      
    FROM (
          SELECT week_start, groupArrayArray(days_changed) AS days_changed
            FROM buffer.v3_queue_d
           WHERE dag_id = %(dag_id)s
        GROUP BY week_start
         ) AS new
LEFT ANY 
    JOIN (       /* присоединяю от предыдущих постановок в очередь 
                    days_changed. не важно, в работе неделя или нет */
          SELECT week_start, days_changed
            FROM public.v3_queue FINAL
         ) AS last_actual
   USING (week_start)
GROUP BY week_start
"""


REASSIGN_CHANGED_OFFICES = """
SET allow_suspicious_low_cardinality_types=1
;

CREATE TEMPORARY TABLE v3_changed_offices_tmp
AS buffer.v3_changed_offices
ENGINE=MergeTree
PARTITION BY toMonday(measure_date)
ORDER BY dag_id
SETTINGS index_granularity=16258
;

/* изымаю партицию из боевой таблицы */
ALTER TABLE v3_changed_offices_tmp
ATTACH PARTITION %(week_start)s
FROM buffer.v3_changed_offices
;

ALTER TABLE buffer.v3_changed_offices
DROP PARTITION %(week_start)s
;

/* возвращаю забранные данные, но с другим dag_id */
INSERT INTO buffer.v3_changed_offices
SELECT * REPLACE (%(dag_id)s AS dag_id)
FROM v3_changed_offices_tmp
;

/* lake_dm3_delivery_times_v3, REASSIGN_CHANGED_OFFICES */
/* если по признакам ABC не осталось заказов, в строчке с ABC надо N сридов
   заменить на 0. но т.к. заказов больше нет, GROUP BY не сформирует строчку с ABC, 
   которая перезатёрла бы старую. раз в день запускаю для этого отдельные скрипты */

  INSERT INTO FUNCTION remoteSecure(
                remote_writer_ch13, 
                database='buffer', 
                table='v3_offices_clearing_' || toString(%(integer_dag_id)s))
         (measure_code, measure_date, src_office_id, poo_office_id, measure_office_id, 
          src_region_id, poo_region_id, is_mp, delivery_type, type_point) 

    WITH (SELECT geo_map_v2
            FROM buffer.v3_geo_map_vw(integer_dag_id=%(integer_dag_id)s)) AS geo_map,
         dictGet('dict.branch_office', 
             ('region_id', 'type_point'), 
             poo_office_id) AS poo_off
                     
  SELECT DISTINCT
         measure_code,
         measure_date,
         src.src_office_id AS src_office_id,
         src.poo_office_id AS poo_office_id,
         src.measure_office_id AS measure_office_id,
         transform(
             dictGet('dict.branch_office', 'region_id', src_office_id),
             geo_map.branch_region_id,
             geo_map.moderated_region_id,
             0) AS src_region_id,
         transform(
             poo_off.region_id,
             geo_map.branch_region_id,
             geo_map.moderated_region_id,
             0) AS poo_region_id,
         src.is_mp AS is_mp,
         CAST(transform(src.sm_id,
             [333, 444, 555, 17], 
             [2, 2, 2, 2], 
             1), 'UInt8') AS delivery_type,         
         if(poo_office_id = 0, 0,
             transform(
                 poo_off.type_point,
                 [1, 10, 34, 5, 6, 7, 8, 9, 14],
                 [1,  1,  1, 2, 2, 2, 3, 3,  4],
                 5)) AS poo_type_point
         
    FROM v3_changed_offices_tmp AS src
SETTINGS min_insert_block_size_bytes='2Gi',
         min_insert_block_size_rows=0
  FORMAT MsgPack
"""


DELETE_FROM_CHANGED_OFFICES = """
# запуск раз в сутки, таблица небольшая. DELETE, а не DROP PARTITION, потому 
# что я предпочёл извлечь выгоду из другого партиционирования в запросе 
# REASSIGN_CHANGED_OFFICES - в нём атомарность операций с партициями важнее, чем здесь
DELETE FROM buffer.v3_changed_offices WHERE dag_id=%(dag_id)s
"""

FILL_DISTRICS_MAP_LAKE = """
/* На время выполнения расчётов фиксирую имена и идентификаторы регионов из бранчей и
   в даге заменяю идентификаторы Int32 на UInt8, чтобы группировки по регионам проходили легче.
   Немного навожу порядок. Те регионы, которых нет в oksm, привожу к нулю. Добавляю фед. округа.
   Регионов 157 (макс 255). если добавятся, надо поменять на UInt16 в v3_geo_map */

ALTER TABLE buffer.v3_geo_map DROP PARTITION %(integer_dag_id)s
;

  INSERT INTO buffer.v3_geo_map 
         (integer_dag_id, is_region, branch_geo_id, moderated_geo_id, 
          moderated_country_id, smallint_geo_id, branch_geo_name)
  SELECT %(integer_dag_id)s, 
         is_region,
         branch_geo_id,
         moderated_geo_id,
         if(moderated_geo_id between 1 and 999,
            coalesce(dictGetOrNull('dict.region_oksm', 'id_fed', moderated_geo_id), 10070),
            moderated_geo_id) AS moderated_country_id,
         if(moderated_geo_id = 0, 0,   /* если регион из бранчей не удалось сопоставить с ОКСМ */
            dense_rank() OVER (        /* smallint делаю равным нулю, т.к. именно по нему */
               PARTITION BY is_region  /* будет фильтрация HAVING в запросах для витрин (by_lc и т.д.) */  
               ORDER BY moderated_geo_id)) AS smallint_geo_id,
         topK(1)(geo.4)[1] AS branch_geo_name

    FROM (          /* если имя есть в oksm, буду брать идентификатор из oksm. 
                       если в oksm нет ни имени, ни идентификатора, привожу к 0 */
               WITH (SELECT mapFromArrays(
                                    groupArray(region_name),
                                    groupArray(region_id))
                       FROM cluster('lake_r', dict.region_oksm)) AS map_oksm,
                            
                    /* у некоторых регионов стоит неверная страна. но если страна
                       отличается от России, я должен сделать id региона равным id-у 
                       страны, так что мне важно такие ситуации исправлять */
                    (SELECT CAST(tuple((groupArray((region_id, country_id, country_name)) as u).1, u.2, u.3),
                                'Tuple(region_id         Array(Int32),
                                       real_country_id   Array(Int32),
                                       real_country_name Array(String))'
                                ) AS regions_countries_map_ext
                      FROM (
                              SELECT region_id, country_id, topK(1)(country_name)[1] AS country_name
                                FROM cluster('lake_r', dict.branch_office)
                            GROUP BY region_id, country_id
                             QUALIFY uniq(country_id) OVER (PARTITION BY region_id) > 1
                                 AND dictHas('dict.region_oksm', country_id) AS in_oksm
                            ORDER BY count() * -1
                               LIMIT 1 BY region_id
                           )) AS rd_map,

                    transform(   /* заменяю ошибочно выставленные страны */
                        region_id,
                        rd_map.region_id,
                        rd_map.real_country_id,
                        country_id) AS real_country_id,
   
                    transform(   /* заменяю ошибочно выставленные страны */
                        region_id,
                        rd_map.region_id,
                        rd_map.real_country_name,
                        country_name) AS real_country_name,
   
                    coalesce(   /* если имя страны есть в oksm, буду брать идентификатор из oksm. 
                                /* если в oksm нет ни имени, ни идентификатора, привожу к 0 */
                        CAST(nullIf(map_oksm[real_country_name], 0), 'Nullable(Int32)'),
                        if(dictHas('dict.region_oksm', real_country_id), real_country_id, 0)
                    ) AS moderated_country_id,
   
                    coalesce(
                        /* если страна - не РФ, оставляю идентификатор страны в кач-ве идентификатора региона */
                        if(moderated_country_id != 1643, moderated_country_id, NULL),

                        /* если имя региона есть в oksm, буду брать идентификатор из oksm */
                        CAST(nullIf(map_oksm[region_name], 0), 'Nullable(Int32)'),

                        /* несколько отсмотренных вручную кодов (у которых имя в бранчах не совпадает с oksm) */
                        transform(
                            region_id, /* Кузбасс, Чувашия, Адыгея, Татарстан, Якутия, Алания, Чукотка */
                            [203786, 202230, 202298, 202274, 202267, 193327, 193254, 82],
                            [42, 42, 21, 1, 16, 14, 15, 87],
                            NULL),

                        /* если в oksm нет ни имени, ни идентификатора, привожу к 0 */
                        if(dictHas('dict.region_oksm', region_id), region_id, 0)
                    ) AS moderated_region_id
  
             SELECT arrayJoin(
                        [(TRUE, region_id, moderated_region_id, region_name),
                         (FALSE, real_country_id, moderated_country_id, real_country_name)]) AS geo
               FROM cluster('lake_r', dict.branch_office)
              GROUP BY region_id, country_id, country_name, region_name
             )
   WHERE geo.2 != 0
GROUP BY geo.1 AS is_region,
         geo.2 AS branch_geo_id,
         geo.3 AS moderated_geo_id
ORDER BY is_region, smallint_geo_id, branch_geo_id;
"""

BY_LOW_CARDINALITY = """
/* lake_dm3_delivery_times_v3, BY_LOW_CARDINALITY */
  INSERT INTO FUNCTION remoteSecure(
                remote_writer_ch13, 
                database='buffer', 
                table='v3_by_low_cardinality_' || toString(%(integer_dag_id)s))
         (measure_date, measure_code, src_geo_id, poo_geo_id, is_mp, type_point, 
          delivery_type, zcurve, srid_uniq, median, duration, cnt, boxplot)

    WITH (SELECT geo_map_v2 
            FROM buffer.v3_geo_map_vw(integer_dag_id=%(integer_dag_id)s)) AS geo_map
  SELECT measure_date,
         measure_code,
         transform(
             src_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS src_geo_id,
         transform(
             poo_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS poo_geo_id,
         toUInt8(coalesce(is_mp, 0)) AS is_mp_fin,
         toUInt8(coalesce(poo_type_point, 0)) AS type_point_fin,
         toUInt8(coalesce(delivery_type, 0)) AS delivery_type_fin,
         mortonEncode(
             modulo(src_geo_id, 4095),
             modulo(poo_geo_id, 4095),
             delivery_type_fin,
             type_point_fin,
             is_mp_fin) AS zcurve,
        sum(daily_flag) AS srid_uniq,
        (quantiles(0, 0.25, 0.5, 0.75, 1)(measure_speed) AS box)[3] AS median,
        sum(measure_speed) AS duration,
        count() AS cnt,
        tuple(box[1], box[2], box[4], box[5]) AS boxplot
   FROM (
            WITH dictGet('dict.branch_office', 
                     ('region_id', 'type_point'), 
                     poo_office_id) AS poo_off
          SELECT toDate(src.measure_ts) AS measure_date,
                 src.src_office_id AS src_office_id,
                 src.poo_office_id AS poo_office_id,
                 src.is_mp AS is_mp,
                 transform(  /* регионы РФ с geo_id < 1000. страны, кроме РФ, с geo_id >= 1000 */
                     dictGet('dict.branch_office', 'region_id', src_office_id),
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS src_smallint_geo_id,
                 transform(
                     poo_off.region_id,
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS poo_smallint_geo_id,                      
                 /* 1 собственный, 2 франшизный, 3 партнерский, 4 почта, 5 другое */
                 if(poo_office_id = 0, 0,
                     transform(
                         poo_off.type_point,
                         [1, 10, 34, 5, 6, 7, 8, 9, 14],
                         [1,  1,  1, 2, 2, 2, 3, 3,  4],
                         5)) AS poo_type_point,
                 CAST(transform(sm_id,       /* без CAST будет LowCardinality-типа, а с ним из-за бага */
                     [333, 444, 555, 17],    /* не работает сеттинг group_by_use_nulls */
                     [2, 2, 2, 2], 
                     1), 'UInt8') AS delivery_type,
                 measure_code,
                 nm_id,
                 measure_office_id,
                 measure_speed, 
                 daily_flag
            FROM datamart.v3_by_srid AS src FINAL
           WHERE toMonday(src.measure_ts) = %(week_start)s
             AND toDate(src.measure_ts) IN %(days_changed)s
             AND src.measure_code IN %(all_codes)s
         ) AS src
GROUP BY GROUPING SETS ({grouping_sets})
             /* я бы предпочёл GROUP BY CUBE, но у него нельзя в Клике указать дату и код меры
                постоянной частью группировки, а без этого CUBE кратно дольше выполняется */
  HAVING coalesce(poo_type_point, 1)      != 0
     AND coalesce(delivery_type, 1)       != 0
     AND coalesce(is_mp, 1)               != 0
     AND coalesce(src_smallint_geo_id, 1) != 0
     AND coalesce(poo_smallint_geo_id, 1) != 0
SETTINGS do_not_merge_across_partitions_select_final=1,
         max_bytes_before_external_group_by='90G',
         group_by_use_nulls=1,
         optimize_on_insert=0
"""

BY_OFFICES = """
/* lake_dm3_delivery_times_v3, BY_OFFICES */

  INSERT INTO FUNCTION remoteSecure(
                remote_writer_ch13, 
                database='buffer', 
                table='v3_by_offices_' || toString(%(integer_dag_id)s))
         (measure_date, measure_code, key_office_type, key_office_id, src_country_id, src_region_id, 
          poo_country_id, poo_region_id, is_mp, type_point, delivery_type, srid_uniq, median, duration, cnt) 

    WITH (SELECT geo_map_v2
            FROM buffer.v3_geo_map_vw(integer_dag_id=%(integer_dag_id)s)) AS geo_map,
         
         changed_offices AS (
             SELECT *
               FROM buffer.v3_changed_offices 
              WHERE dag_id=%(dag_id)s 
                AND toMonday(measure_date)=%(week_start)s
         )

  SELECT measure_date,
         measure_code,
         key_office.1 AS key_office_type,
         key_office.2 AS key_office_id,
         transform(
             src_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_fo_cnt_id,
             0) AS src_country_id,
         transform(
             src_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS src_region_id,
         transform(
             poo_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_fo_cnt_id,
             0) AS poo_country_id,
         transform(
             poo_smallint_geo_id,
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS poo_region_id,
         is_mp,
         poo_type_point AS type_point,
         delivery_type,
         sum(if(key_office_type=3, 1, daily_flag)) AS srid_uniq,
         median(measure_speed) AS median,
         sum(measure_speed) AS duration,
         count() AS cnt
    FROM (
            WITH dictGet('dict.branch_office', 
                     ('region_id', 'country_id', 'type_point'),
                     poo_office_id) AS poo_off
                     
          SELECT toDate(src.measure_ts) AS measure_date,
                 src.src_office_id AS src_office_id,
                 src.poo_office_id AS poo_office_id,
                 src.is_mp AS is_mp,
                 transform(
                     dictGet('dict.branch_office', 'region_id', src_office_id),
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS src_smallint_geo_id,
                 transform(
                     poo_off.region_id,
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS poo_smallint_geo_id,
                 if(poo_office_id = 0, 0,
                     transform(
                         poo_off.type_point,
                         [1, 10, 34, 5, 6, 7, 8, 9, 14],
                         [1,  1,  1, 2, 2, 2, 3, 3,  4],
                         5)) AS poo_type_point,
                 measure_code,
                 nm_id,
                 measure_office_id,
                 measure_speed, 
                 daily_flag, 
                 CAST(transform(sm_id, 
                     [333, 444, 555, 17], 
                     [2, 2, 2, 2], 
                     1), 'UInt8') AS delivery_type,
                 (measure_date, measure_code, src_office_id) IN (
                     SELECT measure_date, measure_code, src_office_id FROM changed_offices) AS is_src,
                 (measure_date, measure_code, poo_office_id) IN (
                     SELECT measure_date, measure_code, poo_office_id FROM changed_offices) AS is_poo,
                 (measure_date, measure_code, measure_office_id) IN (
                     SELECT measure_date, measure_code, measure_office_id FROM changed_offices) AS is_measure,
                 arrayJoin(
                     [if(%(is_manual)s OR is_src,     (1, src_office_id),     (0, 0)),
                      if(%(is_manual)s OR is_poo,     (2, poo_office_id),     (0, 0)),
                      if(%(is_manual)s OR is_measure, (3, measure_office_id), (0, 0))]) AS key_office
            FROM datamart.v3_by_srid AS src FINAL
        PREWHERE (%(is_manual)s OR or(is_src, is_poo, is_measure))
           WHERE toMonday(src.measure_ts) = %(week_start)s
             AND toDate(src.measure_ts) IN %(days_changed)s
             AND src.measure_code IN %(all_codes)s
         )
   WHERE key_office_id != 0
GROUP BY measure_date, measure_code, src_smallint_geo_id, poo_smallint_geo_id, 
         is_mp, poo_type_point, delivery_type,
         key_office_type, key_office_id
SETTINGS optimize_move_to_prewhere_if_final=1,
         do_not_merge_across_partitions_select_final=1,
         max_bytes_before_external_group_by='80G',
         min_insert_block_size_bytes='2Gi',
         min_insert_block_size_rows=0,
         group_by_use_nulls=1,
         optimize_on_insert=0
"""

BY_MANY_OFFICES = """
/* lake_dm3_delivery_times_v3, BY_MANY_OFFICES */

  INSERT INTO FUNCTION remoteSecure(
                remote_writer_ch13, 
                database='buffer', 
                table='v3_by_many_offices_' || toString(%(integer_dag_id)s))
         (measure_date, measure_code, src_office_id, poo_office_id, measure_office_id, 
          src_country_id, src_region_id, poo_country_id, poo_region_id, is_mp, type_point,
          delivery_type, zcurve, srid_uniq, sum_price, median, duration) 

    WITH (SELECT geo_map_v2
            FROM buffer.v3_geo_map_vw(integer_dag_id=%(integer_dag_id)s)) AS geo_map
  SELECT measure_date, 
         measure_code, 
         src_office_id, 
         poo_office_id, 
         measure_office_id,
         transform(
             any(src_smallint_geo_id),
             geo_map.smallint_region_id,
             geo_map.moderated_fo_cnt_id,
             0) AS src_country_id,
         transform(
             any(src_smallint_geo_id),
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS src_region_id,
         transform(
             any(poo_smallint_geo_id),
             geo_map.smallint_region_id,
             geo_map.moderated_fo_cnt_id,
             0) AS poo_country_id, 
         transform(
             any(poo_smallint_geo_id),
             geo_map.smallint_region_id,
             geo_map.moderated_region_id,
             0) AS poo_region_id,
         is_mp AS is_mp,
         any(poo_type_point) AS type_point,
         delivery_type AS delivery_type,
         mortonEncode(
             modulo(toUInt64(measure_office_id), 2097151),
             modulo(toUInt64(src_office_id),     2097151),
             modulo(toUInt64(poo_office_id),     2097151)) AS zcurve,
         count()               AS srid_uniq,
         round(sum(price) / 100) AS sum_price,
         median(measure_speed) AS median,
         sum(measure_speed)    AS duration

    FROM (
            WITH dictGet('dict.branch_office', 
                     ('region_id', 'type_point'), 
                     poo_office_id) AS poo_off

          SELECT toDate(src.measure_ts) AS measure_date,
                 src.src_office_id AS src_office_id,
                 src.poo_office_id AS poo_office_id,
                 src.is_mp AS is_mp,
                 transform(
                     dictGet('dict.branch_office', 'region_id', src_office_id),
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS src_smallint_geo_id,
                 transform(
                     poo_off.region_id,
                     geo_map.branch_region_id,
                     geo_map.smallint_region_id,
                     0) AS poo_smallint_geo_id,
                 if(poo_office_id = 0, 0,
                     transform(
                         poo_off.type_point,
                         [1, 10, 34, 5, 6, 7, 8, 9, 14],
                         [1,  1,  1, 2, 2, 2, 3, 3,  4],
                         5)) AS poo_type_point,
                 CAST(transform(sm_id, 
                     [333, 444, 555, 17], 
                     [2, 2, 2, 2], 
                     1), 'UInt8') AS delivery_type,
                 price,
                 measure_code,
                 measure_office_id,
                 measure_speed, 
                 daily_flag
            FROM datamart.v3_by_srid AS src FINAL
        PREWHERE (%(is_manual)s 
                  OR (measure_date, measure_code, src_office_id, poo_office_id, measure_office_id) IN (
                          SELECT measure_date, measure_code, src_office_id, poo_office_id, measure_office_id
                            FROM buffer.v3_changed_offices 
                           WHERE dag_id=%(dag_id)s 
                             AND toMonday(measure_date)=%(week_start)s))
           WHERE toMonday(src.measure_ts) = %(week_start)s
             AND toDate(src.measure_ts) IN %(days_changed)s
             AND src.measure_code IN %(all_codes)s
         )
GROUP BY measure_date, measure_code, src_office_id, poo_office_id, measure_office_id, is_mp, delivery_type
SETTINGS optimize_move_to_prewhere_if_final=1,
         max_bytes_before_external_group_by='80G',
         do_not_merge_across_partitions_select_final=1,
         min_insert_block_size_bytes='2Gi',
         min_insert_block_size_rows=0,
         optimize_on_insert=0
"""

BY_UNITED = """  /* ~30 тыс строк за неделю */
  SELECT measure_date,
         measure_code,
         coalesce(
             dictGet(
                 'dict.country_short_name',
                 'country_name',
                 dictGet(
                     'dict.sellers_portal',
                     'country_code',
                     dictGet(
                         'dict.product_cards_nm_short',
                         'seller_id',
                         nm_id)) AS seller_country_code
                 ),
             '') AS seller_country_name,
         if(src_country_id > 0, 
            dictGet('dict.region_oksm', 'region_name', src_country_id),
            '') AS src_country_name,
         if(poo_country_id > 0, 
            dictGet('dict.region_oksm', 'region_name', poo_country_id),
            '') AS poo_country_name,
         sum(daily_flag) AS srid_uniq,
         sum(measure_speed) AS duration,
         count() AS cnt
    FROM cluster(
            'lake_r',
            view(
                 WITH (SELECT geo_map_v2 
                         FROM buffer.v3_geo_map_vw(integer_dag_id={{integer_dag_id: UInt16}})) AS geo_map

               SELECT toDate(src.measure_ts) AS measure_date,
                      transform(
                          dictGet('dict.branch_office', 'region_id', src_office_id),
                          geo_map.branch_region_id,
                          geo_map.moderated_country_id,
                          0) AS src_country_id,
                      transform(
                          dictGet('dict.branch_office', 'region_id', poo_office_id),
                          geo_map.branch_region_id,
                          geo_map.moderated_country_id,
                          0) AS poo_country_id,
                      nm_id,
                      measure_code,
                      measure_speed, 
                      daily_flag
                 FROM datamart.v3_by_srid AS src FINAL
                WHERE toMonday(src.measure_ts) = {{week_start: Date}}
                  AND toDate(src.measure_ts) IN {{days_changed: Array(Date)}}
                  AND src.measure_code IN {{all_codes: Array(UInt8)}}
           ))
GROUP BY GROUPING SETS ({grouping_sets})
  HAVING or(isNull(seller_country_code), dictHas('dict.country_short_name', seller_country_code))
     AND or(isNull(src_country_id), src_country_id > 0)
     AND or(isNull(poo_country_id), poo_country_id > 0)
SETTINGS distributed_group_by_no_merge=1,
         do_not_merge_across_partitions_select_final=1,
         max_bytes_before_external_group_by='90G',
         group_by_use_nulls=1
  FORMAT MsgPack
"""

CH9_INSERT_UNITED = """
INSERT INTO datamart.v3_for_united 
       (measure_date, measure_code, seller_country_name, 
        src_country_name, poo_country_name, srid_uniq, duration, cnt)
FORMAT MsgPack
"""

DM3_DROP_BUFFERS = """
SET max_table_size_to_drop=0;
DROP TABLE IF EXISTS buffer.v3_by_low_cardinality_{integer_dag_id};
DROP TABLE IF EXISTS buffer.v3_by_offices_{integer_dag_id};
DROP TABLE IF EXISTS buffer.v3_by_many_offices_{integer_dag_id};
DROP TABLE IF EXISTS buffer.v3_offices_clearing_{integer_dag_id};
"""

DM3_CREATE_BUFFER = """
SET allow_suspicious_low_cardinality_types=1, max_partition_size_to_drop=0;

DROP TABLE IF EXISTS buffer.v3_by_low_cardinality_{integer_dag_id};
CREATE TABLE buffer.v3_by_low_cardinality_{integer_dag_id}
AS datamart.v3_by_low_cardinality
SETTINGS max_bytes_to_merge_at_max_space_in_pool=0
;

DROP TABLE IF EXISTS buffer.v3_by_offices_{integer_dag_id};
CREATE TABLE buffer.v3_by_offices_{integer_dag_id}
AS datamart.v3_by_offices
SETTINGS max_bytes_to_merge_at_max_space_in_pool=0
;

DROP TABLE IF EXISTS buffer.v3_by_many_offices_{integer_dag_id};
CREATE TABLE buffer.v3_by_many_offices_{integer_dag_id}
AS datamart.v3_by_many_offices
SETTINGS max_bytes_to_merge_at_max_space_in_pool=0
;
"""

DM3_CREATE_OFFICES_CLEARING = """
DROP TABLE IF EXISTS buffer.v3_offices_clearing_{integer_dag_id};
CREATE TABLE buffer.v3_offices_clearing_{integer_dag_id}
(
    measure_date       Date,
    measure_code       UInt8,
    src_office_id      Int64,
    poo_office_id      Int64,
    measure_office_id  Int64,
    src_region_id      LowCardinality(Int16),
    poo_region_id      LowCardinality(Int16),
    delivery_type      UInt8,
    is_mp              UInt8,
    type_point         UInt8
)
ENGINE = MergeTree
PARTITION BY toStartOfQuarter(measure_date)
ORDER BY tuple()
SETTINGS max_bytes_to_merge_at_max_space_in_pool=0
COMMENT 'Набор признаков, менявшихся за день (добавлялись новые сроки или были удалены прежние)'
SETTINGS allow_suspicious_low_cardinality_types=1;
"""

DM3_CLEARING_LOW_CARDINALITY = """
  INSERT INTO buffer.v3_by_low_cardinality_{integer_dag_id} 
         (measure_date, measure_code, src_geo_id, poo_geo_id, is_mp, 
          delivery_type, type_point, zcurve, srid_uniq, median, duration, cnt, boxplot)
  
  SELECT measure_date, measure_code, src_geo_id, poo_geo_id, 
         is_mp, delivery_type, type_point, zcurve, 
         0 AS srid_uniq, 
         0 AS median, 
         0 AS duration, 
         0 AS cnt, 
         tuple(0, 0, 0, 0) AS boxplot
    FROM datamart.v3_by_low_cardinality FINAL /* неделя в этой витрине всегда пересчитывается целиком  */
   WHERE measure_date IN %(days_changed)s     /* поэтому находить незатёртые строки можно по датамарту */
     AND cnt > 0
     AND zcurve NOT IN (
             SELECT zcurve   /* NB: у zcurve допустимы коллизии */
               FROM buffer.v3_by_low_cardinality_{integer_dag_id} 
              WHERE measure_date IN %(days_changed)s)
"""

DM3_CLEARING_OFFICES = """
  INSERT INTO buffer.v3_by_offices_{integer_dag_id} 
         (measure_date, measure_code, key_office_type, key_office_id, 
          src_region_id, poo_region_id, delivery_type, is_mp, type_point, 
          srid_uniq, median, duration, cnt)
  
  SELECT measure_date, measure_code, key_office_type, key_office_id,
         src_region_id, poo_region_id, delivery_type, is_mp, type_point,
         0 AS srid_uniq, 
         0 AS median, 
         0 AS duration, 
         0 AS cnt

    FROM (SELECT measure_date, measure_code, src_region_id,
                 poo_region_id, is_mp, type_point, delivery_type,
                 arrayJoin(
                     [(1, src_office_id),
                      (2, poo_office_id),
                      (3, measure_office_id)]) AS key_office
            FROM buffer.v3_offices_clearing_{integer_dag_id}
           WHERE measure_date IN %(days_changed)s
             AND key_office.2 != 0
         ) AS expected_groups

GROUP BY measure_date, measure_code, src_region_id,
         poo_region_id, is_mp, type_point, delivery_type,
         key_office.1 AS key_office_type, 
         key_office.2 AS key_office_id
  HAVING (measure_date, measure_code, src_region_id,
          poo_region_id, is_mp, type_point, 
          delivery_type, key_office_type, key_office_id) NOT IN (

              SELECT measure_date, measure_code, src_region_id,
                     poo_region_id, is_mp, type_point, 
                     delivery_type, key_office_type, key_office_id 
                FROM buffer.v3_by_offices_{integer_dag_id} 
               WHERE measure_date IN %(days_changed)s)
"""

DM3_CLEARING_MANY_OFFICES = """
  INSERT INTO buffer.v3_by_many_offices_{integer_dag_id} 
         (measure_date, measure_code, src_office_id, poo_office_id, 
          measure_office_id, src_region_id, poo_region_id, is_mp, 
          delivery_type, type_point, zcurve, srid_uniq, sum_price, median, duration)
  
  SELECT measure_date, measure_code, src_office_id, poo_office_id, 
         measure_office_id, src_region_id, poo_region_id, is_mp, 
         delivery_type, type_point, zcurve,
         0 AS srid_uniq, 
         0 AS sum_price,
         0 AS median, 
         0 AS duration

    FROM (SELECT measure_date, measure_code,
                 measure_office_id, src_office_id, poo_office_id,
                 any(src_region_id) AS src_region_id,
                 any(poo_region_id) AS poo_region_id,
                 is_mp,
                 delivery_type,
                 any(type_point) AS type_point,
                 mortonEncode(
                     modulo(toUInt64(measure_office_id), 2097151),
                     modulo(toUInt64(src_office_id),     2097151),
                     modulo(toUInt64(poo_office_id),     2097151)) AS zcurve
            FROM buffer.v3_offices_clearing_{integer_dag_id} 
           WHERE measure_date IN %(days_changed)s
        GROUP BY measure_date, measure_code, measure_office_id, 
                 src_office_id, poo_office_id, is_mp, delivery_type
         ) AS expected_groups

   WHERE (zcurve, is_mp, delivery_type) NOT IN (
             SELECT zcurve, is_mp, delivery_type 
                        /* is_mp и delivery_type не определяются через офисы */
               FROM buffer.v3_by_many_offices_{integer_dag_id} 
              WHERE measure_date IN %(days_changed)s)
"""

DM3_TRUNCATE_BUFFERS = """
TRUNCATE TABLE buffer.v3_by_low_cardinality_{integer_dag_id};
TRUNCATE TABLE buffer.v3_by_offices_{integer_dag_id};
TRUNCATE TABLE buffer.v3_by_many_offices_{integer_dag_id};
"""

DM3_DELETE_ON_MANUAL_LAUNCHES = """
SET lightweight_deletes_sync=0;
DELETE FROM datamart.v3_by_low_cardinality 
WHERE measure_date IN %(days_changed)s
AND row_created < %(started_at)s
;

DELETE FROM datamart.v3_by_offices 
WHERE measure_date IN %(days_changed)s
AND row_created < %(started_at)s
;

DELETE FROM datamart.v3_by_many_offices 
WHERE measure_date IN %(days_changed)s
AND row_created < %(started_at)s
"""

CH9_DELETE_ON_MANUAL_LAUNCHES = """
SET lightweight_deletes_sync=0;
DELETE FROM datamart.v3_for_united 
WHERE measure_date IN %(days_changed)s
AND row_created < %(started_at)s
"""

DM3_GET_PARTITIONS_EXPR = """
/* иногда одна половина недели оказывается в одной партиции, другая - в другой.
   надо переносить обе партиции. отсюда необходимость селектить партиции списком */

SELECT DISTINCT table, partition
  FROM system.parts
 WHERE database = 'buffer'
   AND multiSearchFirstPosition(table, [
        'v3_by_low_cardinality', 
        'v3_by_offices', 
        'v3_by_many_offices']) = 1
   AND endsWith(table, '_' || toString(%(integer_dag_id)s))
"""

DM3_MOVE_DATA_TO_DATAMART = """
ALTER TABLE datamart.{dm_table_name}
ATTACH PARTITION '{partition_name}'
FROM buffer.{table_name}
;

ALTER TABLE buffer.{table_name}
DROP PARTITION '{partition_name}'
SETTINGS max_partition_size_to_drop=0
"""

DM3_RELOAD_OFFICES_LIST = """
CREATE TEMPORARY TABLE tmp_v3_offices_catalog
AS datamart.v3_offices_catalog
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY tuple()
;

  INSERT INTO tmp_v3_offices_catalog 
         (office_id, office_name, office_type, full_address, close_date, used_as)
  SELECT office_id, office_name, office_type, full_address, close_date, used_as
    FROM dict.branch_office AS br
LEFT ANY JOIN (
                 SELECT toUInt64(key_office_id) AS office_id,
                        groupUniqArray(key_office_type) AS used_as
                   FROM datamart.v3_by_offices
               GROUP BY key_office_id
                 HAVING uniqUpToIf(3)(measure_date, cnt > 0) = 4
                    AND sum(cnt) > 10
         ) AS v3
   USING (office_id)
   WHERE length(used_as) > 0
;

ALTER TABLE datamart.v3_offices_catalog
REPLACE PARTITION tuple()
FROM tmp_v3_offices_catalog
"""


@with_db(LAKE_R_CONN)
@with_db(LAKE_R_CONN, "r7", hook_kwargs=dict(fixed_shard=7))
def srids_manual(hook, r7_hook, dag_id, logical_date, params):
    # на ручной запуск создаётся отдельная отсечка, по которой даг двигается
    # через те же вызовы calc_v3_srids (из декоратора), как и при регулярных запусках.
    # {"min_rc": "2024-07-01 00:00:00", "max_rc": "2025-03-30 00:00:00"}
    if (min_rc := params.get("min_rc")) is None:
        raise AirflowException('"min_rc" parameter is required')
    cutoff_name = f"datamart.v3_srids_{dag_id}"

    if params.get("just_dm3"):
        # just_dm3 если передан, то не надо пересчитывать сриды. пересчитать только витрины на дм3
        max_rc = params.get("max_rc")
        min_rc = datetime.strptime(min_rc, "%Y-%m-%d %H:%M:%S")
        max_rc = datetime.strptime(max_rc, "%Y-%m-%d %H:%M:%S") if max_rc else datetime.now()
        weeks = defaultdict(set)
        iter_day = datetime(*min_rc.timetuple()[:3])
        while iter_day < max_rc:
            week_start = (iter_day - timedelta(days=iter_day.weekday())).strftime("%Y-%m-%d")
            weeks[week_start].add(iter_day.strftime("%Y-%m-%d"))
            iter_day = iter_day + timedelta(days=1)

        data = list(map(list, zip(*[[k, dag_id, list(v)] for k, v in weeks.items()])))
        r7_hook.exec(
            INSERT_WEEK_TO_QUEUE_MANUALLY,
            parameters=dict(
                week_start=data[0], src_dag_id=data[1], days_changed=data[2]
            ))
        return

    # за промежутки времени по 100 млрд строк (чуть больше полугода)
    # в декорированной функции буду упорядочивать сриды и обходить их упорядоченными
    func = with_cutoff(
        dict(
            conn_str=LAKE_R_CONN,
            cutoff_name=cutoff_name,
            def_cutoff_query=f"SELECT toDateTime('{min_rc}')",
            table_name=lambda x: "core_wh.srid_tracker" + GET_RC(x),
            table_dt_column_name="row_created",
            next_cutoff_max_dt=params.get("max_rc"),
            back_seek_seconds=0 if params.get("max_rc") else 60,
            max_batch_records=100e9)
    )(srids_manual_iter)
    func(hook=hook, dag_id=dag_id, cutoff_name=cutoff_name, logical_date=logical_date)

    # чищу за собой после успешного завершения ручного запуска
    hook.on_cluster(hook.exec, CLEAR_SRIDS_FOR_MANUAL, parameters=dict(dag_id=dag_id))
    hook.exec(
        DELETE_MANUAL_CUTOFF,
        parameters=dict(cutoff_name=cutoff_name.replace("_", "\_") + "%"))


def srids_manual_iter(hook, min_cutoff, max_cutoff, cutoff_name, dag_id, logical_date):
    seq_cutoff_name = cutoff_name + "_synth_rn_" + str(max_cutoff)
    # при открытой max_rc после фейла (и, соотв, и перезапуска) ручного дага - сриды добавятся.
    # если max_cutoff не изменилась, значит, можно пользоваться старыми отсечками и продолжить
    # перебирать сриды с момента, где остановились. в противном случае сработает def_cutoff_query
    hook.on_cluster(
        hook.exec,
        INSERT_SRIDS_FOR_MANUAL,
        parameters=dict(min_cutoff=min_cutoff, max_cutoff=max_cutoff, dag_id=dag_id))
    func = with_cutoff(
        dict(
            conn_str=LAKE_R_CONN,
            back_seek_seconds=0,
            cutoff_name=seq_cutoff_name,
            def_cutoff_query="SELECT toDateTime(0)",
            table_name=f"cluster('lake_r', view(SELECT * FROM buffer.v3_srids_for_manual WHERE dag_id='{dag_id}'))",
            table_dt_column_name="synth_rn",
            max_batch_records=MAX_BATCH_RECORDS_MANUAL)
    )(calc_v3_srids)
    func(dag_id=dag_id, is_manual=True, logical_date=logical_date)


@with_db(LAKE_M_CONN, "m")
@with_cutoff(
    dict(
        conn_str=LAKE_R_CONN,
        cutoff_name="datamart.v3_srids_incremental",
        table_name=lambda x: "core_wh.srid_tracker" + GET_RC(x),
        table_dt_column_name="row_created",
        max_batch_records=MAX_BATCH_RECORDS_ST),
    dict(
        conn_str=LAKE_R_CONN,
        back_seek_seconds=800,
        cutoff_kwarg_prefix="pos",
        cutoff_name="datamart.v3_positions_incremental",
        table_name=lambda x: "positions.oof_position_status_v3" + GET_RC(x),
        table_dt_column_name="row_created",
        max_batch_records=MAX_BATCH_RECORDS_ST))
def srids_incremental(
        m_curs, min_cutoff, max_cutoff, rows_cnt, pos_min_cutoff, 
        pos_max_cutoff, dag_id, logical_date):
    
    for _ in range(40):
        # не более 20 минут ждёт появления новой отсечки для ластов по позишенам,
        # которая бы опережала взятую отсечку по самим позишенам
        lasts_cutoff = get_cutoff(
            db_cursor=m_curs,
            field="max_dt",
            table_name="positions.last_srid_position_v3_incremental")
        if pos_max_cutoff < lasts_cutoff:
            break
        logging.info("Waiting for new cutoff in positions.last_srid_position_v3")
        sleep(30)
    else:
        raise Exception("No recent cutoffs for positions_last_state. Is the table being updated?")
    calc_v3_srids(
        min_cutoff=min_cutoff,
        max_cutoff=max_cutoff,
        logical_date=logical_date,
        pos_min_cutoff=pos_min_cutoff,
        pos_max_cutoff=pos_max_cutoff,
        rows_cnt=rows_cnt,
        is_manual=False,
        dag_id=dag_id)


@with_db(LAKE_R_CONN, "r1", conn_kwargs=dict(send_receive_timeout=3600))
@with_db(LAKE_R_CONN, "r2", conn_kwargs=dict(send_receive_timeout=3600))
def calc_v3_srids(
        r1_hook, r2_hook, min_cutoff, max_cutoff, logical_date, is_manual, 
        dag_id, pos_min_cutoff=None, pos_max_cutoff=None, rows_cnt=None):
    
    delta = logical_date.replace(tzinfo=None) - datetime(logical_date.year, logical_date.month, 1)
    hours_from_month_start = (delta.days * 24 * 3600 + delta.seconds) // 3600
    integer_dag_id = hours_from_month_start + (logical_date.month * 1000 if is_manual else 0)
    group_by_in_order = int(is_manual or (rows_cnt is not None and rows_cnt > 10_000_000))
    logging.info(f"UInt16 dag_id will be {integer_dag_id}")

    # Чищу буферки и затем вставляю сриды по отсечкам
    all_parameters = dict(
        is_manual=int(is_manual),
        min_cutoff=min_cutoff,
        max_cutoff=max_cutoff,
        integer_dag_id=integer_dag_id,
        pos_min_cutoff=pos_min_cutoff,
        pos_max_cutoff=pos_max_cutoff,
        all_codes=MEASURE_CODES,
        v3_date_start=V3_DATE_START,
        actions_white_list=ACTIONS_WHITE_LIST,
        group_by_in_order=group_by_in_order,
        dag_id=dag_id)
    
    r1_hook.on_cluster(
        r1_hook.exec, 
        LAKE_CLEAR_BUFFERS, 
        parameters=all_parameters)
    r1_hook.on_cluster(
        r1_hook.exec_with_log, 
        LAKE_GET_CHANGED_SRIDS, 
        parameters=all_parameters)

    with ThreadPoolExecutor() as executor:
        # параллельно гружу историю сроков по сридам и считаю сроки для сридов заново
        for future in wait([
                executor.submit(
                    r1_hook.on_cluster, 
                    r1_hook.exec, 
                    LAKE_CALC_V3_SRIDS, 
                    parameters=all_parameters),
                executor.submit(
                    r2_hook.on_cluster, 
                    r2_hook.exec, 
                    LAKE_LOAD_HISTORY, 
                    parameters=all_parameters)
                ])[0]:
            future.result()

    r1_hook.on_cluster(r1_hook.exec, LAKE_PROCESS_CHANGED_DATA, parameters=all_parameters)
    # добавляет в очередь те недели, которые после обновления datamart.v3_by_srid требуют пересчёта
    r1_hook.exec(INSERT_CALCED_TO_QUEUE, parameters=dict(dag_id=dag_id))
    r1_hook.on_cluster(r1_hook.exec, LAKE_CLEAR_BUFFERS, parameters=all_parameters)


def get_grouping_sets(persistent_groups, grouping_combinations, skip_comb_func=None):
    grouping_sets = []
    for i in range(0, len(grouping_combinations) + 1):
        for comb in combinations(grouping_combinations, i):
            if skip_comb_func and skip_comb_func(comb) is True:
                continue
            grouping_sets.append("(" + ", ".join([*persistent_groups, *comb]) + ")")
    return grouping_sets


def timeit(func_name):
    def timeit_wrapper(func):
        def _timeit_wrapper(*args, **kwargs):
            q1 = datetime.now()
            func(*args, **kwargs)
            q2 = datetime.now()
            return f"{func_name:<18} took {(q2 - q1).seconds} seconds"
        return _timeit_wrapper
    return timeit_wrapper


def get_next_week(r7_hook, dag_id, is_manual, is_daily, weeks_already_taken):
    week_info = r7_hook.exec(
        GET_NEXT_WEEK_FROM_QUEUE,
        handler=lambda c: c.fetchone(),
        parameters=dict(
            run_dag_id=dag_id,
            is_daily=is_daily,
            is_manual=is_manual,
            is_hourly=not is_daily and not is_manual,
            recent_backoff=CALC_HOURLY_FOR_WEEKS_NUM,
            weeks_already_taken=weeks_already_taken,
        ))
    return week_info or [None] * 5


@with_db(DM_CONN, "ch13")
@with_db(CH9_CONN, "ch9")
@with_db(LAKE_R_CONN, "r")
@with_db(LAKE_R_CONN, "r7", hook_kwargs=dict(fixed_shard=7))
def calc_delivery_times(
        ch9_hook, ch13_hook, r_hook, r7_hook, dag_id, 
        logical_date, is_manual, is_daily):
    
    # вычисляю числовой идентификатор дага - порядковый номер часа logical_date от начала месяца
    delta = logical_date.replace(tzinfo=None) - datetime(logical_date.year, logical_date.month, 1)
    hours_from_month_start = (delta.days * 24 * 3600 + delta.seconds) // 3600
    integer_dag_id = hours_from_month_start + (logical_date.month * 1000 if is_manual else 0)
    logging.info(f"UInt16 dag_id will be {integer_dag_id}")

    # снимаю снапшот с кладра (один для всех лейков), подготавливаю числовые идентификаторы регионов
    r_hook.on_cluster(
        r_hook.exec,
        FILL_DISTRICS_MAP_LAKE,
        parameters=dict(integer_dag_id=integer_dag_id))

    # для каждого запуска дага создаю именные буферки на dm3, чтобы не использовать общие
    # буферки, и тем самым не создавать колонку dag_id -> не терять возможность сразу после
    # загрузки в буферку на dm3 делать replace partition
    ch13_hook.exec(DM3_CREATE_BUFFER.format(integer_dag_id=integer_dag_id))
    if is_daily:
        ch13_hook.exec(DM3_CREATE_OFFICES_CLEARING.format(integer_dag_id=integer_dag_id))

    # получаю из очереди первую неделю
    week_info = get_next_week(r7_hook, dag_id, is_manual, is_daily, [])
    week_start, is_recent, src_dag_id, days_changed, weeks_left = week_info
    weeks_already_taken, all_dates = [week_start], []
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    while week_start:
        logging.info(f'{week_start.strftime("%Y-%m-%d")} week start.')
        logging.info(f"{weeks_left} weeks left.")
        all_dates.extend(days_changed)
        try:

            # ежедневный даг должен подменить dag_id предыдущих дагов на свой,
            # если хочет обработать офисы, добавленные к пересчёту предыдущими дагами
            if is_daily:
                r_hook.on_cluster(
                    r_hook.exec,
                    REASSIGN_CHANGED_OFFICES,
                    parameters=dict(
                        dag_id=dag_id, 
                        week_start=week_start,
                        days_changed=days_changed,
                        all_codes=MEASURE_CODES,
                        integer_dag_id=integer_dag_id))

            ch13_hook.exec(DM3_TRUNCATE_BUFFERS.format(integer_dag_id=integer_dag_id))
            kwargs = dict(
                week_start=week_start,
                is_recent=is_recent,
                is_manual=is_manual,
                is_daily=is_daily,
                all_codes=MEASURE_CODES,
                dag_id=dag_id,
                ch13_hook=ch13_hook,
                integer_dag_id=integer_dag_id,
                days_changed=days_changed)

            # группировка и перенос сроков в буферки на dm3
            with ThreadPoolExecutor() as executor:
                for future in wait([
                        executor.submit(by_low_cardinality, **kwargs),
                        executor.submit(by_many_offices, **kwargs),
                        executor.submit(by_offices, **kwargs),
                        *((executor.submit(by_united, **kwargs),) if (is_daily or is_manual) else ()),
                        ])[0]:
                    logging.info(future.result())

            # заливка из буферок в витрины на dm3
            for table_name, partition_name in ch13_hook.get_records(
                DM3_GET_PARTITIONS_EXPR, parameters=dict(integer_dag_id=integer_dag_id)):

                dm_table_name = "_".join(table_name.split("_")[:-1])
                ch13_hook.exec_with_log(
                    DM3_MOVE_DATA_TO_DATAMART.format(
                        table_name=table_name,
                        dm_table_name=dm_table_name,
                        partition_name=partition_name))

        except Exception as exc:
            # если ошибка, нужно дропнуть именные буферки на дм3 и вернуть взятые недели в очередь
            ch13_hook.exec(DM3_DROP_BUFFERS.format(integer_dag_id=integer_dag_id))
            r7_hook.exec(
                INSERT_WEEK_TO_QUEUE_MANUALLY,
                parameters=dict(
                    week_start=[week_start],
                    src_dag_id=[src_dag_id],
                    days_changed=[days_changed],
                ))
            logging.info(f"{week_start} returned to queue")
            raise exc

        # если успех, пометить неделю пересчитанной
        r7_hook.exec(
            REMOVE_WEEK_FROM_QUEUE,
            parameters=dict(run_dag_id=dag_id, week_start=week_start))

        # следующая неделя становится текущей, дальше - очередная итерация
        # получаю след неделю, чтобы одновременно с обработкой текущей грузить следующую в буферку
        next_week_info = get_next_week(r7_hook, dag_id, is_manual, is_daily, weeks_already_taken)
        week_start, is_recent, src_dag_id, days_changed, weeks_left = next_week_info
        weeks_already_taken.append(week_start)

    # после выхода из цикла дропаю именные буферки на датамарте
    ch13_hook.exec(DM3_DROP_BUFFERS.format(integer_dag_id=integer_dag_id))

    # если успех ручного запуска, прежние данные надо удалить из витрин
    if is_manual:
        ch13_hook.exec_with_log(
            DM3_DELETE_ON_MANUAL_LAUNCHES,
            parameters=dict(started_at=started_at, days_changed=all_dates))
        ch9_hook.exec_with_log(
            CH9_DELETE_ON_MANUAL_LAUNCHES,
            parameters=dict(started_at=started_at, days_changed=all_dates))

    # если успех ежедневного запуска, надо удалить из буферки по офисам те офисы,
    # которые пересчитаны. иначе они подхватятся на следующий ежедневный запуск
    if is_daily:
        r_hook.on_cluster(
            r_hook.exec, 
            DELETE_FROM_CHANGED_OFFICES, 
            parameters=dict(dag_id=dag_id))

    # перезагружаю перечень офисов, datamart.v3_offices,
    # по которому происходит поиск офисов из отчёта
    if is_daily or is_manual:
        ch13_hook.exec(DM3_RELOAD_OFFICES_LIST)


@timeit("by_low_cardinality")
@with_db(LAKE_R_CONN, "r")
def by_low_cardinality(
        r_hook, ch13_hook, days_changed, all_codes, week_start, 
        is_recent, is_daily, integer_dag_id, **kwargs):
    
    persistent_groups = ["measure_date", "measure_code"]
    grouping_combinations = [
        "src_smallint_geo_id", "poo_smallint_geo_id", "delivery_type", "poo_type_point", "is_mp"]
    grouping_sets = ", ".join(
        get_grouping_sets(persistent_groups, grouping_combinations))
    r_hook.on_cluster(
        r_hook.exec,
        BY_LOW_CARDINALITY.format(grouping_sets=grouping_sets),
        parameters=dict(
            integer_dag_id=integer_dag_id,
            is_recent=is_recent,
            days_changed=days_changed,
            all_codes=all_codes,
            week_start=week_start,
        ))
       
    if is_daily:
        ch13_hook.exec(
            DM3_CLEARING_LOW_CARDINALITY.format(integer_dag_id=integer_dag_id),
            parameters=dict(days_changed=days_changed))    


@timeit("by_many_offices")
@with_db(LAKE_R_CONN, "r")
def by_many_offices(
        r_hook, ch13_hook, week_start, days_changed, all_codes, is_recent, 
        dag_id, integer_dag_id, is_manual, is_daily, **kwargs):
    
    r_hook.on_cluster(
        r_hook.exec,
        BY_MANY_OFFICES,
        parameters=dict(
            dag_id=dag_id,
            days_changed=days_changed,
            all_codes=all_codes,
            integer_dag_id=integer_dag_id,
            is_recent=is_recent,
            is_manual=is_manual,
            week_start=week_start,
        ))
       
    if is_daily:
        ch13_hook.exec(
            DM3_CLEARING_MANY_OFFICES.format(integer_dag_id=integer_dag_id),
            parameters=dict(days_changed=days_changed))


@timeit("by_offices")
@with_db(LAKE_R_CONN, "r")
def by_offices(
        r_hook, ch13_hook, week_start, days_changed, all_codes, is_recent, 
        dag_id, integer_dag_id, is_manual, is_daily, **kwargs):
    
    r_hook.on_cluster(
        r_hook.exec,
        BY_OFFICES,
        parameters=dict(
            dag_id=dag_id,
            days_changed=days_changed,
            all_codes=all_codes,
            integer_dag_id=integer_dag_id,
            is_recent=is_recent,
            is_manual=is_manual,
            week_start=week_start,
        ))

    if is_daily:
        ch13_hook.exec(
            DM3_CLEARING_OFFICES.format(integer_dag_id=integer_dag_id),
            parameters=dict(days_changed=days_changed))    


@timeit("by_united")
def by_united(week_start, days_changed, all_codes, integer_dag_id, **kwargs):
    persistent_groups = ["measure_date", "measure_code"]
    grouping_combinations = ["seller_country_code", "src_country_id", "poo_country_id"]
    grouping_sets = ", ".join(get_grouping_sets(persistent_groups, grouping_combinations))
    days_changed_fmt = [day.strftime("%Y-%m-%d") for day in days_changed]
    today = datetime.now().strftime("%Y-%m-%d")
    if today in days_changed_fmt:
        days_changed_fmt.remove(today)
    copy_ch_to_ch_pipe(
        take_data=BY_UNITED.format(grouping_sets=grouping_sets),
        insert_data=CH9_INSERT_UNITED,
        src_ch=LAKE_R_CONN,
        dst_ch=CH9_CONN,
        log_query=False,
        parameters=dict(
            week_start=week_start,
            days_changed=days_changed_fmt,
            all_codes=all_codes,
            integer_dag_id=integer_dag_id,
        ))


with DAG(
    dag_id="lake_dm3_do_delivery_times",
    schedule="45 * * * *",  # каждый час в 45 минут
    start_date=datetime(2024, 7, 1),
    description=DESCRIPTION,
    max_active_runs=3,
    catchup=False,
    tags=["delivery_times", LAKE_R_CONN, DM_CONN, TELEGA],
    render_template_as_native_obj=True,
    default_args=dict(
        owner="kravcov",
        telegram=[TELEGA],
        retries=2,
        trigger_rule="none_failed",
        retry_delay=timedelta(minutes=3),
    )):

    branching_srids_task = BranchPythonOperator(
        task_id="branching_srids",
        pool=LAKE_R_CONN,
        python_callable=lambda x: "srids_" + ("manual" if x else "incremental"),
        op_args=["{{ dag_run.external_trigger }}"],
    )

    srids_manual_task = PythonOperator(
        task_id="srids_manual",
        python_callable=srids_manual,
        retries=5,
        pool=LAKE_R_CONN,
        op_kwargs=dict(
            dag_id="{{ dag_run.run_id }}", 
            logical_date="{{ dag_run.logical_date }}"),
        inlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-r.core_wh.srid_tracker"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.core_wh.srid_tracker"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.core_wh.srid_tracker_rc"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.positions.oof_position_status_v3"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_srids_for_manual"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.buffer.v3_srid_set"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.positions.last_srid_position_v3"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.dict.suppliers_warehouse"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.core_wh.srid_tracker"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-lake-r.buffer.v3_srid_set"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-lake-r.datamart.v3_by_srid"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-lake-r.buffer.v3_srid_recent"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-lake-r.buffer.v3_srid_history"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-lake-r.buffer.v3_queue")],
        outlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-r.buffer.v3_srids_for_manual"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_srid_set"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.buffer.v3_srid_recent"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-lake-r.buffer.v3_srid_history"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-lake-r.datamart.v3_by_srid"),
            OMEntity(entity=Entity.TABLE, key='5', fqn="do-lake-r.buffer.v3_changed_offices"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-lake-r.public.v3_queue")
        ],
    )

    srids_incremental_task = PythonOperator(
        task_id="srids_incremental",
        python_callable=srids_incremental,
        max_active_tis_per_dag=1,  # если больше одной - работали бы по одним отсечкам параллельно
        pool=LAKE_R_CONN,
        op_kwargs=dict(
            dag_id="{{ dag_run.run_id }}", logical_date="{{ dag_run.logical_date }}"
        ),
    )

    branching_queue_task = BranchPythonOperator(
        task_id="branching_queue",
        pool=LAKE_R_CONN,
        python_callable=lambda x, y: "calc_"
        + (
            "just_manual_entries"
            if x
            else ("whole_queue" if y else "just_recent_weeks")
        ),
        op_args=["{{ dag_run.external_trigger }}", "{{ logical_date.hour == 2 }}"],
    )

    calc_just_recent_weeks_task = PythonOperator(
        task_id="calc_just_recent_weeks",
        pool=LAKE_R_CONN,
        python_callable=calc_delivery_times,
        op_kwargs=dict(
            dag_id="{{ dag_run.run_id }}",
            logical_date="{{ dag_run.logical_date }}",
            is_manual=False,
            is_daily=False),
        inlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-r.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-r.dict.region_oksm"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.datamart.v3_by_srid"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_geo_map"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.public.v3_queue"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_geo_map_vw"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_major_countries_vw"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.buffer.v3_changed_offices"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-lake-r.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-lake-r.buffer.v3_changed_offices"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch13.buffer.v3_offices_clearing"),
            OMEntity(entity=Entity.TABLE, key='5.1', fqn="do-ch13.buffer.v3_by_low_cardinality"),
            OMEntity(entity=Entity.TABLE, key='5.2', fqn="do-ch13.buffer.v3_by_offices"),
            OMEntity(entity=Entity.TABLE, key='5.3', fqn="do-ch13.buffer.v3_by_many_offices"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-ch13.dict.branch_office"),
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-ch13.datamart.v3_by_offices"),
            OMEntity(entity=Entity.TABLE, key='7', fqn="do-lake-r.dict.country_short_name"),
            OMEntity(entity=Entity.TABLE, key='7', fqn="do-lake-r.dict.sellers_portal"),
            OMEntity(entity=Entity.TABLE, key='7', fqn="do-lake-r.dict.product_cards_nm_short")],
        outlets=[
            OMEntity(entity=Entity.TABLE, key='1', fqn="do-lake-r.buffer.v3_geo_map"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch13.buffer.v3_by_low_cardinality"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch13.buffer.v3_by_offices"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch13.buffer.v3_by_many_offices"),
            OMEntity(entity=Entity.TABLE, key='2', fqn="do-ch9.datamart.v3_for_united"),
            OMEntity(entity=Entity.TABLE, key='3', fqn="do-ch13.buffer.v3_offices_clearing"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch13.buffer.v3_by_offices"),
            OMEntity(entity=Entity.TABLE, key='4', fqn="do-ch13.buffer.v3_by_many_offices"),
            OMEntity(entity=Entity.TABLE, key='5.1', fqn="do-ch13.datamart.v3_by_low_cardinality"),
            OMEntity(entity=Entity.TABLE, key='5.2', fqn="do-ch13.datamart.v3_by_offices"),
            OMEntity(entity=Entity.TABLE, key='5.3', fqn="do-ch13.datamart.v3_by_many_offices"), 
            OMEntity(entity=Entity.TABLE, key='6', fqn="do-ch13.datamart.v3_offices_catalog"),
            OMEntity(entity=Entity.TABLE, key='7', fqn="do-ch9.datamart.v3_for_united")
        ],
    )

    calc_just_manual_entries_task = PythonOperator(
        task_id="calc_just_manual_entries",
        pool=LAKE_R_CONN,
        python_callable=calc_delivery_times,
        op_kwargs=dict(
            dag_id="{{ dag_run.run_id }}",
            logical_date="{{ dag_run.logical_date }}",
            is_manual=True,
            is_daily=False,
        ),
    )

    calc_whole_queue_task = PythonOperator(
        task_id="calc_whole_queue",
        pool=LAKE_R_CONN,
        python_callable=calc_delivery_times,
        op_kwargs=dict(
            dag_id="{{ dag_run.run_id }}",
            logical_date="{{ dag_run.logical_date }}",
            is_manual=False,
            is_daily=True,
        ),
    )

    branching_srids_task >> [
        srids_manual_task, 
        srids_incremental_task
    ] >> branching_queue_task >> [
        calc_whole_queue_task, 
        calc_just_recent_weeks_task, 
        calc_just_manual_entries_task
    ]
