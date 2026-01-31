import logging as log
from datetime import datetime, date

from utils.decorators_with_conn import with_db
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utils.data_exchange import copy_ch_to_ch_pipe
from utils.cutoff import get_cutoff, save_cutoff
from utils.db.clickhouse import get_ch_dwh_max_v2
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity

"""
Даг собирает отмененные кизованные (обязательная маркировка) заказы для отчета 056.1
Статусы отмен 4, 8, 16, 27, 48, 124, 190, раз в неделю удаляю немаркированные товары путем подмены партиции,
оставляя кизованный 16 статус на 1 месяц, на случай возврата (иначе с 8 статусом теряем payment_type),
а также 4 статус некизованный, т.к в дальнейшем его могут маркировать.
"""

CH4 = 'do-ch4'
CH3 = 'do-ch3'

FLDS = 'srid, sgtin, uin, imei, gtin, create_dt, supplier_id, payment_type, shk_id, status_id, reject_reason, dst_office_id, dt, is_kiz'
GET_OOF = """
SELECT srid
     , CAST(NULL, 'Nullable(String)') AS sgtin
     , CAST(NULL, 'Nullable(String)') AS uin
     , CAST(NULL, 'Nullable(String)') AS imei
     , CAST(NULL, 'Nullable(String)') AS gtin
     , argMax(create_ts, ver)         AS create_dt
     , argMax(seller_id, ver)         AS supplier_id
     , argMax(payment_type, ver)      AS payment_type
     , argMax(shk_id, ver)            AS shk_id
     , argMax(status_oof, ver)        AS status_id
     , argMax(reject_reason, ver)     AS reject_reason
     , argMax(dst_office_id, ver)     AS dst_office_id
     , argMax(dt, ver)                AS dt
     , CAST(NULL, 'Nullable(UInt8)')  AS is_kiz
  FROM positions.oof_position_status_v3_rc a
 WHERE a.row_created >= '{min_oof_v3_cutoff}'
   AND a.row_created <= '{max_oof_v3_cutoff}'
   AND status_oof IN (4, 8, 16, 27, 48, 124, 190)
 GROUP BY srid
FORMAT Native; """

GET_META = """
SELECT srid
     , if(meta_key = 'sgtin', meta_value, NULL) AS sgtin
     , if(meta_key = 'uin', meta_value, NULL)   AS uin
     , if(meta_key = 'imei', meta_value, NULL)  AS imei
     , if(meta_key = 'gtin', meta_value, NULL)  AS gtin
     , CAST(NULL, 'Nullable(DATETIME)')         AS create_dt
     , CAST(NULL, 'Nullable(UInt32)')           AS supplier_id
     , CAST(NULL, 'Nullable(String)')           AS payment_type
     , CAST(NULL, 'Nullable(Int64)')            AS shk_id
     , CAST(NULL, 'Nullable(UInt8)')            AS status_id
     , CAST(NULL, 'Nullable(UInt8)')            AS reject_reason
     , CAST(NULL, 'Nullable(Int64)')            AS dst_office_id
     , CAST(NULL, 'Nullable(DATETIME)')         AS dt
     , 1                                        AS is_kiz
  FROM stage_nats.orders_meta_rc a
 WHERE a.row_created >= '{min_orders_meta_cutoff}' 
   AND a.row_created <= '{max_orders_meta_cutoff}'
   AND meta_key IN ('sgtin', 'uin', 'imei', 'gtin')
   AND meta_value IS NOT NULL
 GROUP BY ALL
FORMAT Native; """

REPLACE_PART = f"""
DROP TEMPORARY TABLE IF EXISTS oof_v3_meta_replace;
CREATE TEMPORARY TABLE oof_v3_meta_replace
(
    srid          String,
    sgtin         Nullable(String),
    uin           Nullable(String),
    imei          Nullable(String),
    gtin          Nullable(String),
    create_dt     Nullable(DATETIME),
    supplier_id   Nullable(UInt32),
    payment_type  LowCardinality(Nullable(String)),
    shk_id        Nullable(Int64),
    status_id     Nullable(UInt8),
    reject_reason Nullable(UInt8),
    dst_office_id Nullable(Int64),
    dt            Nullable(DATETIME),
    is_kiz        Nullable(UInt8)
) ENGINE = MergeTree
  PARTITION BY tuple()
  ORDER BY srid;

INSERT INTO oof_v3_meta_replace ({FLDS})
SELECT srid, sgtin, uin, imei, gtin, create_dt, supplier_id, payment_type, shk_id, status_id, reject_reason, dst_office_id, dt, is_kiz
  FROM datamart.oof_v3_meta_cancel FINAL
 WHERE (status_id != 16                                    -- невыданные заказы с маркировкой (в пути и все отмены)
        AND is_kiz = 1
        OR 
        (status_id = 16                                    -- выданные за последние 30 дней с маркировкой (могут вернуть с 8, потеряем payment_type)
         AND dt >= today() - toIntervalDay(30)
         AND is_kiz = 1
        )
        OR (status_id = 4
            AND is_kiz = 0                                 -- оформленные, но еще без маркировки
           )
       );

ALTER TABLE datamart.oof_v3_meta_cancel
REPLACE PARTITION tuple()
   FROM oof_v3_meta_replace; """


@with_db(CH4, 'ch4')
def get_oof_v3_cancel(ch4_hook, ch4_curs):
    """ Собираю новые отмены из oof_v3 """
    prev_oof_v3_cutoff = get_cutoff(ch4_curs, 'oof_v3_meta_cancel_056_1', field='max_dt')

    (min_oof_v3_cutoff, max_oof_v3_cutoff, _, rec_count_oof) = get_ch_dwh_max_v2(
        ch_hook=ch4_hook,
        changes_table_name='positions.oof_position_status_v3_rc',
        date_field='row_created',
        max_dt=prev_oof_v3_cutoff,
        max_batch_records=50_000_000,
        back_seek_seconds=60)

    # CH4 --> CH3. Копирую отмены oof_v3 на CH3
    copy_ch_to_ch_pipe(take_data=GET_OOF.format(min_oof_v3_cutoff=min_oof_v3_cutoff, max_oof_v3_cutoff=max_oof_v3_cutoff),
                       insert_data=f""" INSERT INTO datamart.oof_v3_meta_cancel ({FLDS}) FORMAT Native """,
                       src_ch=CH4,
                       dst_ch=CH3)

    # сохраняю отсечку
    save_cutoff(ch4_curs, 'oof_v3_meta_cancel_056_1', max_oof_v3_cutoff, field='max_dt')


@with_db(CH4, 'ch4')
@with_db(CH3, 'ch3', conn_kwargs=dict(send_receive_timeout=3600))
def get_meta_info(ch4_hook, ch4_curs, ch3_hook):
    prev_orders_meta_cutoff = get_cutoff(ch4_curs, 'orders_meta_056_1', field='max_dt')

    (min_orders_meta_cutoff, max_orders_meta_cutoff, _, rec_count_meta) = get_ch_dwh_max_v2(
        ch_hook=ch4_hook,
        changes_table_name='stage_nats.orders_meta_rc',
        date_field='row_created',
        max_dt=prev_orders_meta_cutoff,
        max_batch_records=10_000_000,
        back_seek_seconds=60)

    # CH4 --> CH3. Копирую метаинформацию только sgtin,uin,imei,gtin на CH3
    copy_ch_to_ch_pipe(take_data=GET_META.format(min_orders_meta_cutoff=min_orders_meta_cutoff, max_orders_meta_cutoff=max_orders_meta_cutoff),
                       insert_data=f""" INSERT INTO datamart.oof_v3_meta_cancel ({FLDS}) FORMAT Native """,
                       src_ch=CH4,
                       dst_ch=CH3)

    # сохраняю отсечку
    save_cutoff(ch4_curs, 'orders_meta_056_1', max_orders_meta_cutoff, field='max_dt')

    ch3_hook.exec_with_log('OPTIMIZE TABLE datamart.oof_v3_meta_cancel FINAL')

def check_replace_part_need(**context):
    now = date.today()
    # дата предыдущего успешного запуска дага
    prev_start_date_success = context.get('prev_start_date_success')
    prev_ds = prev_start_date_success.date() if prev_start_date_success else now

    wkd = now.isoweekday()          # номер дня недели
    h_num = datetime.now().hour     # номер часа
    dif = abs((now - prev_ds).days) # дней от последнего успешного запуска

    log.info(f'prev_ds: {prev_ds}, wkd: {wkd}, dif: {dif}')

    if wkd == 6 and dif >= 1 and 0 <= h_num <= 8:
        return True
    else:
        return False


@with_db(CH3, 'ch3')
def replace_unkiz_cancel(ch3_hook):
    tdy = date.today().year
    ch3_hook.exec_with_log(REPLACE_PART, parameters={'part_no': tdy})


def_args = {
    'owner': 'urazov.vasiliy',
    'email': ['urazov.vasiliy@wildberries.work'],
    'telegram': ['@Novotrex'],
    'email_on_failure': False,
    'max_active_tasks': 1,
    'max_active_runs': 1,
}

with DAG(
        default_args=def_args,
        description="""Даг собирает отмененные кизованные (обязательная маркировка) заказы для отчета 056.1""",
        dag_id='ch4_to_ch3_kiz_cancel',
        schedule='17 */6 * * *',  # At minute 17 past every 6th hour UTC
        start_date=datetime(2025, 12, 4),
        catchup=False,
        tags=["@Novotrex", CH4, CH3, "kiz_cancel", "datamart"],
    ) as dag:

    task_get_oof_v3_cancel = PythonOperator(
        task_id='get_oof_v3_cancel',
        python_callable=get_oof_v3_cancel,
        dag=dag,
        pool=get_pool(CH4),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.positions.oof_position_status_v3_rc",key="g1"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH3}.datamart.oof_v3_meta_cancel",key="g1"),
        ],
    )

    task_get_meta_info = PythonOperator(
        task_id='get_meta_info',
        python_callable=get_meta_info,
        dag=dag,
        pool=get_pool(CH4),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH4}.stage_nats.orders_meta_rc",key="g1"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH3}.datamart.oof_v3_meta_cancel",key="g1"),
        ],
    )

    task_check_replace_part_need = ShortCircuitOperator(
        task_id="check_replace_part_need",
        dag=dag,
        python_callable=check_replace_part_need,
    )

    task_replace_unkiz_cancel = PythonOperator(
        task_id='replace_unkiz_cancel',
        python_callable=replace_unkiz_cancel,
        dag=dag,
        pool=get_pool(CH3),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH3}.datamart.oof_v3_meta_cancel",key="g1"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"{CH3}.datamart.oof_v3_meta_cancel",key="g1"),
        ],
    )

    task_get_oof_v3_cancel >> task_get_meta_info >> task_check_replace_part_need >> task_replace_unkiz_cancel