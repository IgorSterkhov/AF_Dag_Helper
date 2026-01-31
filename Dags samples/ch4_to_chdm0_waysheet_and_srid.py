from datetime import datetime, timedelta
from airflow import DAG
import logging as log
from airflow.operators.python import PythonOperator
from utils.decorators_with_conn import with_db,with_cutoff
from utils.data_exchange import copy_ch_to_ch_pipe
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH4_CONN = 'do-ch4'
DM02_CONN_ID = 'do-ch-dm02'

CH4_LOAD_NEW_DECLARATIONS="""
insert into stage_wh.wh_tares_declaration_last (srid, shk_id, tare, dt, office_id, row_created)
select srid,untuple(argMax(tuple(shk_id, tare, dt, office_id, row_created), dt))
from stage_wh.wh_tares_declaration
where 1=1
and row_created>= %(min_cut)s
and row_created< %(max_cut)s
and srid is not null
and srid!=''
group by srid
SETTINGS optimize_on_insert=0
"""

CH4_WAYSHEET_AND_SRID_SCRIPT ="""
DROP TABLE IF EXISTS waysheet_and_box;
CREATE TEMPORARY TABLE waysheet_and_box
(
    `waysheet_id` Int32,
    `transfer_box_id` Int64,
    `shipping_src_office_id` Int64,
    `dt` DateTime
)
ENGINE = MergeTree
ORDER BY transfer_box_id;

insert into waysheet_and_box
(waysheet_id, transfer_box_id, shipping_src_office_id, dt)
select
    waysheet_id,
    transfer_box_id,
    src_office_id,
    dt
from(
    select
        waysheet_id,
        transfer_box_id,
        shipping_src_office_id as src_office_id,
        dt
    from stage_nats.shipping_boxes
    where 1=1
    and _row_created>= %(min_cut_ship)s
    and _row_created< %(max_cut_ship)s

    union distinct

    select
        waysheet_id,
        transfer_box_id,
        office_id as src_office_id,
        dt
    from stage_nats.export_returned_boxes
    where 1=1
    and row_created>= %(min_cut_exp)s
    and row_created< %(max_cut_exp)s
    ) q;
DROP TABLE IF EXISTS waysheet_and_srid_prc;
CREATE TEMPORARY TABLE waysheet_and_srid_prc
(
    `shk_id` Int64,
    `office_id` Int64,
    `srid` String,
    `tare` Int64
)
ENGINE = MergeTree
ORDER BY srid;

insert into waysheet_and_srid_prc(srid,office_id,shk_id,tare)
select srid,
       argMax(office_id,dt) as office_id,
       argMax(shk_id,dt) as shk_id,
       argMax(tare,dt)as tare
from stage_wh.wh_tares_declaration_last
where 1=1
and srid in (select srid
               from stage_wh.wh_tares_declaration_last
               where tare in (select transfer_box_id from waysheet_and_box box))

group by srid
having tare in (select transfer_box_id from waysheet_and_box box);

insert into buffer.waysheet_and_srid(waysheet_id, shk_id, nm_id, srid, tare, price_rub, customer_id)
select wab.waysheet_id
,a.shk_id
,b.nm_id
,srid
,a.tare
,b.price
,b.suser_id
from waysheet_and_srid_prc a
inner join waysheet_and_box wab
on wab.transfer_box_id = a.tare
and wab.shipping_src_office_id = a.office_id
left any join (
    select srid,price,suser_id,nm_id
    from positions.oof_position_status_v3
    where 1=1
    and dt>now()-interval '1 month'
    and srid in (select srid from waysheet_and_srid_prc)
) b
using srid
settings join_algorithm = 'full_sorting_merge';"""

CH4_TRUNCATE_BUFFER="""truncate table buffer.waysheet_and_srid"""


@with_db(CH4_CONN,'ch4')
@with_cutoff(
    dict(cutoff_name = 'stage_wh.wh_tares_declaration_last'
         ,table_name = 'stage_wh.wh_tares_declaration'
         ,table_dt_column_name = 'row_created'
         ,conn_str = 'ch4'
         ,cutoff_kwarg_prefix='decl'
         , max_batch_records=2_000_000)
)
def load_new_declarations(ch4_curs,decl_min_cutoff,decl_max_cutoff):
    ch4_curs.execute(CH4_LOAD_NEW_DECLARATIONS,parameters={"min_cut":decl_min_cutoff.strftime('%Y-%m-%d %H:%M:%S'),"max_cut":decl_max_cutoff.strftime('%Y-%m-%d %H:%M:%S')})


@with_db(CH4_CONN,'ch4')
@with_cutoff(
    dict(cutoff_name = 'shipping_boxes_030'
         ,table_name = 'stage_nats.shipping_boxes'
         ,table_dt_column_name = '_row_created'
         ,conn_str = 'ch4'
         ,cutoff_kwarg_prefix='ship'
         ,max_batch_records=2_000_000),
    dict(cutoff_name = 'export_returned_box_030'
         ,table_name = 'stage_nats.export_returned_boxes'
         ,table_dt_column_name = 'row_created'
         ,conn_str = 'ch4'
         ,cutoff_kwarg_prefix='exp'
         ,max_batch_records=2_000_000)
)
def update_data(ch4_hook,ship_min_cutoff,ship_max_cutoff,exp_min_cutoff,exp_max_cutoff):
    ch4_hook.exec_with_log(CH4_TRUNCATE_BUFFER)
    ch4_hook.exec_with_log(CH4_WAYSHEET_AND_SRID_SCRIPT,parameters={"min_cut_ship": ship_min_cutoff,
                                                                    "max_cut_ship": ship_max_cutoff,
                                                                    "min_cut_exp": exp_min_cutoff,
                                                                    "max_cut_exp": exp_max_cutoff})


def ch4_to_chdm0():
    rows = copy_ch_to_ch_pipe(
        take_data="""select waysheet_id
                    ,shk_id
                    ,nm_id
                    ,srid
                    ,tare
                    ,price_rub
                    ,customer_id
                    from buffer.waysheet_and_srid format MsgPack;""",
        insert_data=f"insert into datamart.waysheet_and_srid (waysheet_id,shk_id,nm_id,srid,tare,price_rub,customer_id) FORMAT MsgPack",
        src_ch=CH4_CONN,
        dst_ch=DM02_CONN_ID
    )
    log.info(f'Загружено {rows} строк! ')


default_args = {
    'owner': 'borisov.grigpriy4',
    'telegram': ['@grisha_borisov'],
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
        dag_id="ch4_to_dmo_waysheet_and_srid",
        description="Даг собирает витрину для отчета 030",
        default_args=default_args,
        schedule=timedelta(hours=1),
        start_date=datetime(2026, 1, 15),
        tags=[CH4_CONN,DM02_CONN_ID, "@grisha_borisov","borisov.grigoriy4","stage_wh"],
        max_active_tasks=1,
        max_active_runs=1
) as dag:
    task_upd_decl = PythonOperator(
        task_id="task_upd_decl",
        dag=dag,
        pool=get_pool(CH4_CONN),
        python_callable=load_new_declarations,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_wh.wh_tares_declaration")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.stage_wh.wh_tares_declaration_last")]

    )
    task_upd_waysheets = PythonOperator(
        task_id="task_upd_waysheets",
        dag=dag,
        pool=get_pool(CH4_CONN),
        python_callable=update_data,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_nats.shipping_boxes"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_nats.export_returned_boxes"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch4.stage_wh.wh_tares_declaration"),
                OMEntity(entity=Entity.TABLE, fqn="do-ch4.positions.oof_position_status_v3")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch4.buffer.waysheet_and_srid")]

    )
    task_ch4_to_chdm0 = PythonOperator(
        task_id="ch4_to_chdm0",
        dag=dag,
        pool=get_pool(CH4_CONN),
        python_callable=ch4_to_chdm0,
        inlets=[OMEntity(entity=Entity.TABLE, fqn="do-ch4.buffer.waysheet_and_srid")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch-dm02.datamart.waysheet_and_srid")]

    )

    task_upd_decl >> task_upd_waysheets >> task_ch4_to_chdm0
