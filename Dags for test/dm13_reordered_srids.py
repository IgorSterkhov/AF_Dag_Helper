import logging as log
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db, with_cutoff


"""
Назначение. 
DataOps-7516 
https://youtrack.wildberries.ru/issue/DataOps-7516/Novyj-otchet-po-novomu-proektu-zakaza-otkaznikov

Даг заполняет витрину datamart.srid_tracker_reorders на dm0-3
Таблица с перезаказами. Определяются по статусу 111. Содержит последние статусы + доп. инфу
"""

# Задаем подключения
CH_13_CONN_ID = "do-ch13"
TELEGA = '@i_sterkhov'

CH13_REORDER = '''
--1. Вставка новых 111х
drop table if exists list_srid_111;
create temporary table list_srid_111
engine = MergeTree
order by srid
as (    select srid,
               max(ts) as ts,
               argMax(office_id, t.ts) as office_id
        from core_wh.srid_tracker_ttl t
        left anti join datamart.srid_tracker_reorders using (srid)
        where action_id = 111
          and row_created >= '{min_cutoff}'
          and row_created <= '{max_cutoff}'
        group by srid )
;

--2. Получаем последние состояния по новым 111м и тем которые уже были в прошлых итерациях
drop table if exists srid_last_row;
create temporary table srid_last_row
engine = MergeTree
order by srid
    as (select *,
             nullIf(arrayMin(x -> x > ts_320 ? x : null, ts_500_arr ), 0) as ts_500
        from (
            with argMax(tuple(t.shk_id, t.action_id, t.office_id, t.dst_office_id), t.ts) as tup
            select srid,
                   nullIf(tup.1,0) as shk_id,
                   max(t.ts) as ts,
                   tup.2 as action_id,
                   tup.3 as office_id,
                   nullIf(tup.4,0) as dst_office_id,
                   minIfOrNull(t.ts, t.action_id = 1000) as ts_1000,
                   argMinIfOrNull(t.office_id, t.ts, t.action_id = 1000) as office_id_1000,
                   coalesce(
                      minIfOrNull(t.ts, t.action_id = 310),
                      minIfOrNull(t.ts, t.action_id = 320) ) as ts_320,
                   countIf(t.action_id = 111) as count_111,
                   groupArrayIf(t.ts, t.action_id in (500, 610, 620, 630, 640)) as ts_500_arr,
                   max(row_created) as dwh_date
            from core_wh.srid_tracker_ttl t
            where true
              and row_created >= '{min_cutoff}'
              and row_created <= '{max_cutoff}'
              and t.srid in (select srid from datamart.srid_tracker_reorders r
                             where ts_111 >= today() - interval 3 month
                             union all
                             select srid from list_srid_111)
            group by srid ) )
;

--3. Обогащаем ласты данными из новых 111 и исторических данных
drop table if exists srid_actual;
create temporary table srid_actual
engine = MergeTree
order by srid
as (select l.srid                                  as srid,
           coalesce(l.shk_id, r.shk_id)            as shk_id,
           l.ts                                    as ts,
           l.action_id                             as action_id,
           l.office_id                             as office_id,
           coalesce(l.dst_office_id, r.dst_office_id)        as dst_office_id,
           r.price_rub                                       as price_rub,
           coalesce(r.ts_111, l_111.ts)                      as ts_111,
           coalesce(r.office_id_111, l_111.office_id)        as office_id_111,
           coalesce(r.ts_1000, l.ts_1000)                    as ts_1000,
           coalesce(r.ts_320, l.ts_320)                      as ts_320,
           coalesce(r.ts_500, l.ts_500)                      as ts_500,
           coalesce(r.office_id_1000, l.office_id_1000)      as office_id_1000,
           r.ts_prev_srid_1070                               as ts_prev_srid_1070,
           r.office_id_prev_srid_1070                        as office_id_prev_srid_1070,
           r.action_id_prev_srid_last                        as action_id_prev_srid_last,
           nullIf(r.ts_prev_srid_last, 0)                    as ts_prev_srid_last,
           r.office_id_prev_srid_last                        as office_id_prev_srid_last,
           ifNull(r.count_111,0) + ifNull(l.count_111,0)     as count_111,
           r.subject_id                                      as subject_id,
           r.parent_id                  as parent_id,
           r.subject_name               as subject_name,
           r.parent_name                as parent_name,
           l.dwh_date as dwh_date
    from srid_last_row l
    left join list_srid_111 l_111 using (srid)
    left join datamart.srid_tracker_reorders r using (srid)
) ;

--4. Обогащаем обновленные ласты - данными прошлого срида
drop table if exists prev_srid;
create temporary table prev_srid
engine = MergeTree
order by srid
    as ( with argMax(tuple(t.srid, t.action_id, t.office_id, t.ts), t.ts) as tup
         select ls.srid as srid,
                tup.1 as srid_prev,
                tup.2 as action_id_last,
                tup.3 as office_id_last,
                tup.4 as ts,
                maxIfOrNull(t.ts, action_id=1070) as ts_1070,
                argMaxIfOrNull(t.office_id, t.ts, t.action_id = 1070) as office_id_1070
        from core_wh.srid_tracker_ttl t
        inner join srid_actual ls
            on ls.shk_id = t.shk_id
            and ls.srid != t.srid
            and t.ts < ls.ts_111
            and ls.shk_id != 0
            and ls.ts_prev_srid_last is null
        where t.shk_id in (select shk_id from srid_actual
                           where shk_id != 0
                             and ts_prev_srid_last is null )
        group by ls.srid )
;

--5. Обогащаем обновленные ласты - позишенами и сабжектами
drop table if exists positions;
create temporary table positions
    engine = MergeTree
        order by srid
as (    select  srid,
                shk_id,
                price_rub,
                nm_id,
                dictGetOrNull('dict.product_cards_nm', 'subject_id', nm_id)             as subject_id,
                dictGetOrNull('dict.subjects', 'subject_name', subject_id)              as subject_name,
                dictGetOrNull('dict.subjects', 'parent_id', subject_id)                 as parent_id,
                dictGetOrNull('dict.subjects', 'parent_name', subject_id)               as parent_name
        from (  select srid, shk_id, nm_id,
                       cast(round(price *
                                  dictGetFloat64OrDefault('dict.cbr_currency', 'rate',
                                                          (currency_id, coalesce(toDate(create_ts),today())),
                                                          dictGetFloat64OrDefault('dict.cbr_currency', 'rate',
                                                                                  (currency_id, today() - 3), 1))),
                            'Nullable(Int64)') as price_rub
                  from ( with (select min(ts_111) - interval '30 day' from srid_actual) as min_ts_111
                         select srid,
                              shk_id,
                              price,
                              create_ts,
                              currency_id,
                              nm_id
                        from remote_ch.last_srid_position_v3 p
                       where srid global in (select srid from srid_actual r
                                             where nullIf(price_rub, 0) is null or nullIf(subject_id, 0) is null)
                         and create_ts >= min_ts_111 ) q1 ) q2

)
;

--6. Соединяем все в датамарт
insert into datamart.srid_tracker_reorders
(srid, shk_id, ts, action_id, office_id, dst_office_id, price_rub,
 ts_111, office_id_111, ts_1000, office_id_1000, ts_prev_srid_1070,
 office_id_prev_srid_1070, action_id_prev_srid_last, ts_prev_srid_last,
 office_id_prev_srid_last, count_111, subject_id, parent_id, subject_name,
 parent_name, dwh_date, ts_320, ts_500)
select r.srid,
       r.shk_id,
       r.ts,
       r.action_id,
       r.office_id,
       r.dst_office_id,
       coalesce(nullIf(r.price_rub,0), nullIf(pd.price_rub,0)) as price_rub,
       r.ts_111,
       r.office_id_111,
       r.ts_1000,
       r.office_id_1000,
       coalesce(r.ts_prev_srid_1070, prev.ts_1070) as ts_prev_srid_1070,
       coalesce(r.office_id_prev_srid_1070, prev.office_id_1070) as office_id_prev_srid_1070,
       coalesce(r.action_id_prev_srid_last, prev.action_id_last) as action_id_prev_srid_last,
       coalesce(r.ts_prev_srid_last, prev.ts) as ts_prev_srid_last,
       coalesce(r.office_id_prev_srid_last, prev.office_id_last) as office_id_prev_srid_last,
       count_111,
       coalesce(r.subject_id, pd.subject_id) as subject_id,
       coalesce(r.parent_id, pd.parent_id) as parent_id,
       coalesce(r.subject_name, pd.subject_name) as subject_name,
       coalesce(r.parent_name, pd.subject_name) as parent_name,
       r.dwh_date,
       r.ts_320,
       r.ts_500
    from srid_actual r
    left join prev_srid prev using (srid)
    left join positions pd using (srid)
;
'''

@with_db(CH_13_CONN_ID, "ch13")
@with_cutoff(
    dict(
        cutoff_name="datamart.srid_tracker_reorders",
        table_name="core_wh.srid_tracker_ttl",
        table_dt_column_name="row_created",
        conn_str=CH_13_CONN_ID,
         max_batch_records=500_000_000,
    )
)
def upd_reorders(ch13_hook, min_cutoff, max_cutoff):
    SQL = CH13_REORDER.format(
        min_cutoff=min_cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        max_cutoff=max_cutoff.strftime("%Y-%m-%d %H:%M:%S"),
    )
    # log.info("SQL " + SQL)

    log.info("inserting new data...")
    ch13_hook.exec_with_log(SQL)

@with_db(CH_13_CONN_ID, "ch13")
def optimize_reorders(ch13_hook):
    log.info(f'OPTIMIZE datamart.srid_tracker_reorders final ...')
    ch13_hook.exec_with_log(sql='OPTIMIZE TABLE datamart.srid_tracker_reorders final;', autocommit=True)
    log.info('Done!')


with DAG(
        dag_id="dm13_reordered_srids",
        description="Даг заполняет витрину datamart.srid_tracker_reorders на dm13",
        schedule= '0 * * * *',
        start_date=datetime(2025, 12, 29),
        catchup=False,
        max_active_runs=1,
        tags=["datamart", "srid_tracker_reorders", TELEGA, CH_13_CONN_ID],
        default_args=dict(
            owner='sterhov.igor',
            band='sterhov.igor',
            telegram=[TELEGA],
            catchup=False,
            email_on_failure=True,
            retries=2,
            retry_delay=timedelta(minutes=10)),
) as dag:

    task_upd_reorders = PythonOperator(
        task_id="task_upd_reorders",
        dag=dag,
        pool=CH_13_CONN_ID,
        python_callable=upd_reorders,
        trigger_rule="none_failed",
    )

    task_optimize_reorders = PythonOperator(
        task_id="task_optimize_reorders",
        dag=dag,
        pool=CH_13_CONN_ID,
        python_callable=optimize_reorders,
        trigger_rule="none_failed",
    )

    task_upd_reorders >> task_optimize_reorders
