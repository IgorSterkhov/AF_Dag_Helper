from datetime import datetime, timedelta
import logging as log
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utils.data_exchange import copy_ch_to_ch
from utils.decorators_with_conn import with_db
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


# все использованные подключения
CH4_CONN_ID = 'do-ch4-dl'
CH6_CONN_ID = 'do-ch6'
CHDM0_CONN_ID = 'do-ch-dm0'

# Буфферные и основная таблицы
CH4_BUFFER_OLD_SHIPPINGS = 'buffer.datamart_old_shippings'
SRC_TABLE = 'datamart.shipping_history'
CHDM0_BUFFER_DATAMART = 'buffer.datamart_shipping_history'
CH4_BUFFER_SHIPPINGS_HISTORY = 'buffer.shippings_history'

# Скрипты удаления
TRUNCATE_BUFFER_OLD_SHIPPINGS_CH4 = """truncate table buffer.datamart_old_shippings;"""
TRUNCATE_BUFFER_DM0 = """truncate table buffer.datamart_shipping_history;"""
TRUNCATE_CH4_BUFFER_SHIPING_HISTORY = """truncate table buffer.shippings_history"""

# Виды используемых в переносах данных
COLUMNS_RESULT = 'transfer_box_id, shipping_id, ttn_id, waysheet_id, supplier_id, shipping_src_office_id, box_src_office_id, box_dst_office_id, box_type, log_type, loaded_dt, unloaded_dt, dt, row_created'

COLUMNS_TMP = 'transfer_box_id,shipping_id,ttn_id,waysheet_id,supplier_id,shipping_src_office_id,box_src_office_id,box_dst_office_id,dt,box_type,log_type'

COLUMNS_OLD_SHIPPING = 'transfer_box_id,shipping_id,waysheet_id,dt'

COLUMNS_SRID_TRACKER = 'transfer_box_id,waysheet_id,dt'

CHDMO_COLLECT_OLD_SHIPPINGS = """select transfer_box_id,shipping_id,waysheet_id,toDateTime(dt) as dt 
from datamart.shipping_history
where dt>now()-interval '10 day'
and coalesce(unloaded_dt,toDateTime('2000-01-01')) < '2001-01-01'
and row_created>={rv} and row_created<{po}
FORMAT CSVWithNames;"""

CH4_DMO_BUFFER_TO_BUFFER = """select transfer_box_id,
    shipping_id ,
    ttn_id,
    waysheet_id ,
    supplier_id ,
    shipping_src_office_id,
    box_src_office_id,
    box_dst_office_id,
    box_type ,
    log_type ,
    loaded_dt ,
    unloaded_dt ,
    dt  ,
    row_created
from buffer.shippings_history
where 1=1                   
and row_created>={rv} and row_created<{po}
FORMAT CSVWithNames;"""

INSERT_BUFFER_CH4_SHIPPINGS_OLD = """
insert into buffer.shippings_history
(transfer_box_id,shipping_id,ttn_id,waysheet_id,supplier_id, shipping_src_office_id
, box_src_office_id, box_dst_office_id,dt, box_type,log_type,loaded_dt,unloaded_dt,row_created)
with shippings as (
select distinct transfer_box_id,shipping_id,ttn_id
    ,waysheet_id,supplier_id,shipping_src_office_id
    ,box_src_office_id,box_dst_office_id,dt,'Продажная' as box_type
    ,'WBDRIVE' as log_type
from stage_nats.shipping_boxes sb
inner join buffer.datamart_old_shippings dos
    on sb.transfer_box_id = dos.transfer_box_id
    and sb.waysheet_id = dos.waysheet_id
    and sb.shipping_id = dos.shipping_id
    and sb.dt = dos.dt
where 1=1
    and dt>%(max_dt)s-interval '10 day'
union all
select distinct transfer_box_id,null as shipping_id,null as ttn_id,waysheet_id
    ,null as supplier_id,office_id as shipping_src_office_id, office_id as box_src_office_id
    ,null as box_dst_office_id,dt, 'Возвратная' as box_type, 'WBDRIVE' as log_type
from stage_nats.export_returned_boxes erb
inner join buffer.datamart_old_shippings dos
    on erb.transfer_box_id = dos.transfer_box_id
    and erb.waysheet_id = dos.waysheet_id
    and erb.dt = dos.dt
where dt>%(max_dt)s-interval '10 day'
),
shippings_load_unloads as (
select distinct transfer_box_id,shipping_id,ttn_id
    ,waysheet_id,sh.supplier_id as supplier_id,shipping_src_office_id
    ,box_src_office_id,coalesce(box_dst_office_id,wtu.office_id) as box_dst_office_id
    ,sh.dt as dt,box_type,log_type
    ,wtl.loaded_dt as loaded_dt,wtu.unloaded_dt as unloaded_dt
from shippings sh
left join
    (select distinct tare_id,office_id,loaded_dt,tare_type,tare_sticker
     from stage_wh.wh_tares_load
     where dt>%(max_dt)s-interval '10 day') wtl
    on ((wtl.tare_id = sh.transfer_box_id and wtl.office_id = sh.shipping_src_office_id)
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = box_src_office_id)
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = toInt32(dictGet('dict.branch_office','main_office_id',sh.shipping_src_office_id)))
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = toInt32(dictGet('dict.branch_office','main_office_id',sh.box_src_office_id))))
left join
    (select distinct tare_id,office_id,unloaded_dt,tare_type,tare_sticker
     from stage_wh.wh_tares_unload
     where dt>%(max_dt)s-interval '10 day') wtu
on sh.transfer_box_id = wtu.tare_id
    and wtu.tare_sticker = wtl.tare_sticker
where 1=1
    and (wtl.office_id = sh.shipping_src_office_id or wtl.office_id = sh.box_src_office_id)
    and (loaded_dt is null or greatest(sh.dt - wtl.loaded_dt,wtl.loaded_dt -sh.dt )<7200)
    and (unloaded_dt is null or greatest(sh.dt - wtu.unloaded_dt,wtu.unloaded_dt -sh.dt )<259200)
    and wtl.tare_type!='CON'
)
select distinct transfer_box_id,shipping_id
    ,ttn_id,waysheet_id,supplier_id,shipping_src_office_id
    ,box_src_office_id, box_dst_office_id,dt as dt
    ,box_type,log_type,loaded_dt,unloaded_dt
    ,coalesce(unloaded_dt,loaded_dt,dt) as row_created
from shippings_load_unloads;
"""

INSERT_BUFFER_CH4_SHIPPINGS_NEW = """
insert into buffer.shippings_history
    (transfer_box_id,shipping_id,ttn_id,waysheet_id
    ,supplier_id, shipping_src_office_id
    ,box_src_office_id,box_dst_office_id
    ,dt, box_type,log_type
    ,loaded_dt,unloaded_dt,row_created)
with shippings as (
    select distinct transfer_box_id,shipping_id
    ,ttn_id,waysheet_id,supplier_id,shipping_src_office_id
    ,box_src_office_id,box_dst_office_id,dt,'Продажная' as box_type
    ,'WBDRIVE' as log_type
from stage_nats.shipping_boxes sb
where 1=1
    and dt>%(max_dt)s
union all
select distinct transfer_box_id,null as shipping_id,null as ttn_id,waysheet_id
    ,null as supplier_id,office_id as shipping_src_office_id, office_id as box_src_office_id
    ,null as box_dst_office_id,dt, 'Возвратная' as box_type, 'WBDRIVE' as log_type
from stage_nats.export_returned_boxes erb
where dt>%(max_dt)s
),
shippings_load_unloads as (
select transfer_box_id,shipping_id,ttn_id
    ,waysheet_id,sh.supplier_id as supplier_id
    ,shipping_src_office_id,box_src_office_id
    ,coalesce(box_dst_office_id,wtu.office_id) as box_dst_office_id
    ,sh.dt as dt,box_type,log_type
    ,wtl.loaded_dt as loaded_dt,wtu.unloaded_dt as unloaded_dt
from shippings sh
left join 
(select distinct tare_id,office_id,loaded_dt,tare_type,tare_sticker from stage_wh.wh_tares_load where dt>%(max_dt)s) wtl
    on ((wtl.tare_id = sh.transfer_box_id and wtl.office_id = sh.shipping_src_office_id)
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = box_src_office_id)
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = toInt32(dictGet('dict.branch_office','main_office_id',sh.shipping_src_office_id)))
    or (wtl.tare_id = sh.transfer_box_id and wtl.office_id = toInt32(dictGet('dict.branch_office','main_office_id',sh.box_src_office_id))))
left join (select distinct tare_id,office_id,unloaded_dt,tare_type,tare_sticker from stage_wh.wh_tares_unload where dt>%(max_dt)s) wtu
    on sh.transfer_box_id = wtu.tare_id
    and wtu.tare_sticker = wtl.tare_sticker
where 1=1
    and (wtl.office_id = sh.shipping_src_office_id or wtl.office_id = sh.box_src_office_id)
    and (loaded_dt is null or greatest(sh.dt - wtl.loaded_dt,wtl.loaded_dt -sh.dt )<7200)
    and (unloaded_dt is null or greatest(sh.dt - wtu.unloaded_dt,wtu.unloaded_dt -sh.dt )<259200)
    and wtl.tare_type!='CON'
)
select distinct transfer_box_id,shipping_id
    ,ttn_id,waysheet_id,supplier_id
    ,shipping_src_office_id
    ,box_src_office_id, box_dst_office_id,dt as dt
    , box_type,log_type,loaded_dt,unloaded_dt
    ,coalesce(unloaded_dt,loaded_dt,dt) as row_created
from shippings_load_unloads;
"""

UPDATE_SHIPPINGS_HISTORY = """insert into datamart.shipping_history
    (transfer_box_id, shipping_id,ttn_id 
    , waysheet_id, supplier_id, shipping_src_office_id
    , box_src_office_id, box_dst_office_id
    , box_type, log_type, loaded_dt
    , unloaded_dt, dt, row_created)
select transfer_box_id, shipping_id
    , ttn_id, waysheet_id, supplier_id, shipping_src_office_id
    , box_src_office_id, box_dst_office_id 
    , box_type, log_type, loaded_dt
    , unloaded_dt, dt, row_created
from buffer.datamart_shipping_history"""


# Удаление буфферок
@with_db(CH6_CONN_ID,'ch6')
@with_db(CH4_CONN_ID,'ch4')
@with_db(CHDM0_CONN_ID,'chdm0')
def truncate_buffer_tables(ch6_hook,ch4_hook,chdm0_hook):
    ch4_hook.exec_with_log(TRUNCATE_BUFFER_OLD_SHIPPINGS_CH4)
    ch4_hook.exec_with_log(TRUNCATE_CH4_BUFFER_SHIPING_HISTORY)
    chdm0_hook.exec_with_log(TRUNCATE_BUFFER_DM0)


# Перенос данных по которым могли появиться даннеы в load unload
def collect_old_shippings():
    copy_ch_to_ch(recipient_table=CH4_BUFFER_OLD_SHIPPINGS,
                  take_data=CHDMO_COLLECT_OLD_SHIPPINGS,
                  take_max=f'select max(toInt32(row_created)) from {SRC_TABLE}',
                  columns=COLUMNS_OLD_SHIPPING,
                  shift_rv=20000,
                  src_ch=CHDM0_CONN_ID,
                  dst_ch=CH4_CONN_ID
                  )


# Собираем обновления и новые данные на ch4
@with_db(CH4_CONN_ID,'ch4')
@with_db(CHDM0_CONN_ID,'chdm0')
def insert_buffer_ch4_shippings(ch4_hook,chdm0_hook):
    mdt = chdm0_hook.fetchone(f'SELECT max(dt) FROM {SRC_TABLE}')
    ch4_hook.exec_with_log(INSERT_BUFFER_CH4_SHIPPINGS_OLD,parameters={"max_dt":mdt})
    ch4_hook.exec_with_log(INSERT_BUFFER_CH4_SHIPPINGS_NEW,parameters={"max_dt":mdt})


# Переносим данные в буфферку на dm0
def ch4_to_dmo_buffer_to_buffer():
    copy_ch_to_ch(recipient_table=CHDM0_BUFFER_DATAMART,
                  take_data=CH4_DMO_BUFFER_TO_BUFFER,
                  take_max=f'select max(toInt32(row_created)) from buffer.shippings_history',
                  columns=COLUMNS_RESULT,
                  shift_rv=20000,
                  src_ch=CH4_CONN_ID,
                  dst_ch=CHDM0_CONN_ID
                  )

@with_db(CHDM0_CONN_ID,'chdm0')
def chdmo_update_shipping_history(chdm0_hook):
    chdm0_hook.exec_with_log(UPDATE_SHIPPINGS_HISTORY)


default_args = {
    'owner': 'borisov.grigoriy4',
    'email': ['Borisov.Grigoriy4@wildberries.work'],
    'telegram': ['@grisha_borisov'],
    'max_active_tasks' : 1,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    "ch4_to_dm0_shippings_history",
    default_args=default_args,
    description=f"Даг обновляет данные по всем отгрузкам",
    schedule='5 4 * * *',
    start_date=datetime(2024, 10, 28),
    tags=["@grisha_borisov", 'datamart', CH4_CONN_ID,CH6_CONN_ID,CHDM0_CONN_ID],
    max_active_runs=1
) as dag:
    truncate_buffer_tables = PythonOperator(
        task_id='truncate_buffer_tables',
        python_callable=truncate_buffer_tables,
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm0.datamart.shipping_history")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.datamart_old_shippings")]
    )
    collect_old_shippings = PythonOperator(
        task_id='collect_old_shippings',
        python_callable=collect_old_shippings,
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm0.datamart.shipping_history")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.datamart_old_shippings")]
    )
    insert_buffer_ch4_shippings = PythonOperator(
        task_id='insert_buffer_ch4_shippings',
        python_callable=insert_buffer_ch4_shippings,
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm0.datamart.shipping_history")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.datamart_old_shippings")]
    )
    ch4_to_dmo_buffer_to_buffer = PythonOperator(
        task_id='ch4_to_dmo_buffer_to_buffer',
        python_callable=ch4_to_dmo_buffer_to_buffer,
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.shippings_history")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm0.buffer.datamart_shipping_history")]
    )
    chdmo_update_shipping_history = PythonOperator(
        task_id='chdmo_update_shipping_history',
        python_callable=chdmo_update_shipping_history,
        dag=dag,
        pool=get_pool(CH4_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.shippings_history")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm0.buffer.datamart_shipping_history")]
    )

    truncate_buffer_tables >> collect_old_shippings >> insert_buffer_ch4_shippings >> ch4_to_dmo_buffer_to_buffer >> chdmo_update_shipping_history
