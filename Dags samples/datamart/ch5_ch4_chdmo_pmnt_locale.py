import logging as log
from datetime import datetime, timedelta,date
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.data_exchange import copy_ch_to_ch_pipe
from utils.decorators_with_conn import with_db
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH5_CONN = 'do-ch5'
CH4_CONN = 'do-ch4'
CHDM0_CONN = 'do-ch-dm0'

TAKE_DATA_CH5 = """
select coalesce(toString(transaction_uuid),transaction_bad_uuid) as transaction_uuid,srid,transaction_sum,created_at as created_dt,currency_id 
from stage_external.mpm_transaction_completed
where created_at>='{start}'
and created_at<'{end}'
and status = 'succeeded'
FORMAT MsgPack
"""

TAKE_DATA_CH4 = """
select created_dt as create_dt,src_country,dst_country,toDecimal64(sum(transaction_sum/100),2) as transaction_sum,card_issuer_country
from 
(select transaction_uuid,src_country,dst_country,card_issuer_country,created_dt,max(transaction_sum) as transaction_sum
from datamart.pmnt_locale
where created_dt = '{date}'
group by transaction_uuid,src_country,dst_country,card_issuer_country,created_dt)
group by created_dt,src_country,dst_country,card_issuer_country
FORMAT MsgPack;
"""

FIELDS_CH4_MPM = 'transaction_uuid,srid,transaction_sum,created_dt,currency_id'
FIELDS_CH4_PMNT = 'transaction_uuid,created_dt,src_country,dst_country,transaction_sum,card_issuer_country'
FIELDS_DM0 = 'create_dt,src_country,dst_country,transaction_sum,card_issuer_country'

UPDATE_PMNT_LOCALE="""
insert into datamart.pmnt_locale(transaction_uuid,created_dt,src_country,dst_country,transaction_sum,card_issuer_country)
select
b.transaction_uuid,toStartOfMonth(b.created_dt)
,dictGet('dict.branch_office','country_name',src_office_id)
,dictGet('dict.branch_office','country_name',dst_office_id)
,transaction_sum
,dictGet('dict.currency_codes','country',b.currency_id) AS card_issuer_country
from
(select distinct on (srid) srid ,src_office_id,dst_office_id
from positions.oof_position_status_v3
where 1=1
and src_office_id>0
and dst_office_id>0
and status_oof in (27,4)
and dt >='{start}'
and dt<'{end}'
and cityHash64(srid)%4={number}
and srid in (select srid from buffer.mpm_transaction_completed where cityHash64(srid)%4={number})
) a
inner join
(select transaction_uuid,created_dt,srid,transaction_sum,currency_id from buffer.mpm_transaction_completed where cityHash64(srid)%4={number}) b
on a.srid = b.srid
settings join_algorithm = 'full_sorting_merge';
"""

INS_CH = """ INSERT INTO datamart.pmnt_locale (create_dt, src_country, dst_country, transaction_sum, card_issuer_country)
             SELECT create_dt, src_country, dst_country, transaction_sum, card_issuer_country
               FROM buffer.pmnt_locale
              WHERE create_dt NOT IN (SELECT DISTINCT create_dt 
                                        FROM datamart.pmnt_locale
                                     ) """

TRUNCATE_BUFFER ='truncate table buffer.mpm_transaction_completed'
TRUNCATE_BUFFER_CHDM0 = "TRUNCATE TABLE buffer.pmnt_locale"


def get_dt():
    current_day = date.today()-timedelta(days=1)
    next_day = date.today()
    if current_day.month==1:
        month_back =current_day.replace(year=current_day.year-1,month=12,day=1)
    elif current_day.day>28:
        month_back = current_day.replace(month=current_day.month-1,day=28)
    else:
        month_back = current_day.replace(month=current_day.month-1)
    start_month_back = month_back.replace(day=1)
    return [current_day,next_day,month_back,start_month_back]


@with_db(CH4_CONN,'ch4')
def my_ch_to_ch_pipe(ch4_curs):
    ch4_curs.execute(TRUNCATE_BUFFER)
    dates = [f'{s:%Y-%m-%d}' for s in get_dt()]
    rows = copy_ch_to_ch_pipe(
        take_data=TAKE_DATA_CH5.format(start=dates[0],end = dates[1]),
        insert_data=f"insert into buffer.mpm_transaction_completed ({FIELDS_CH4_MPM}) FORMAT MsgPack",
        src_ch=CH5_CONN,
        dst_ch=CH4_CONN
    )
    log.info(f'Загружено {rows} строк! ')


@with_db(CH4_CONN,'ch4')
def update_data(ch4_curs):
    dates = [f'{s:%Y-%m-%d}' for s in get_dt()]
    for i in range(4):
        log.info("начали {date1} {date2} hash={num}".format(date1=dates[2], date2=dates[1],num=i))
        ch4_curs.execute(UPDATE_PMNT_LOCALE.format(FIELDS_CH4_PMNT = FIELDS_CH4_PMNT,start=dates[2],end=dates[1],number=i))
        log.info("сделали {date1} {date2} hash={num}".format(date1=dates[2], date2=dates[1],num=i))

    if date.today().day==8:
        ch4_chdm0()


@with_db(CHDM0_CONN,'chdm0')
def ch4_chdm0(chdm0_curs):
    dates = [f'{s:%Y-%m-%d}' for s in get_dt()]
    chdm0_curs.execute(TRUNCATE_BUFFER_CHDM0)
    rows = copy_ch_to_ch_pipe(
        take_data=TAKE_DATA_CH4.format(date=dates[3]),
        insert_data=f"insert into buffer.pmnt_locale ({FIELDS_DM0}) FORMAT MsgPack",
        src_ch=CH4_CONN,
        dst_ch=CHDM0_CONN
    )
    log.info('Вставляю только новый период')
    chdm0_curs.execute(INS_CH)


default_args = {
    'owner': 'borisov.grigoriy4',
    'telegram': ['@grisha_borisov'],
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
        "ch5_ch4_chdm0_pmnt_locale",
        default_args=default_args,
        description=f"Даг для отчета 042 в суперсете dwh-unit#184",
        schedule='0 6 * * *',
        start_date=datetime(2025, 12, 5),
        tags=["datamart","@grisha_borisov",CH4_CONN, CH5_CONN,"borisov.grigoriy4"],
        max_active_runs=1
) as dag:
    task_transfer_data = PythonOperator(
        task_id='task_transfer_data',
        python_callable=my_ch_to_ch_pipe,
        dag=dag,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch5.stage_external.mpm_transaction_completed")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.buffer.mpm_transaction_completed")],
        pool=get_pool('do-ch4')
    )
    task_insert_data = PythonOperator(
        task_id='task_insert_data',
        python_callable=update_data,
        dag=dag,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.datamart.pmnt_locale",key="g2"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch4.buffer.mpm_transaction_completed",key="g1"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch-dm0.buffer.pmnt_locale",key="g3")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch-dm0.buffer.pmnt_locale",key="g2"),
                 OMEntity(entity=Entity.TABLE, fqn="do-ch-dm0.datamart.pmnt_locale",key="g3"),
                 OMEntity(entity=Entity.TABLE, fqn="do-ch4.datamart.pmnt_locale",key="g1")],
        pool=get_pool('do-ch4')
    )
    task_transfer_data >> task_insert_data
