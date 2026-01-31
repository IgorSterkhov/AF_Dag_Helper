import logging as log
from datetime import datetime, timedelta

import requests
from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from utils.curs import CursIOClickHouse
from utils.decorators_with_conn import load_and_save_cutoff, with_db
from utils.openmeta_helper import Entity

API_CONN_ID = "api-sam-frontend-v3"
CH3_CONN_ID = "do-ch3"
PG_CONN_ID = "do-pg1-bo"
GP_CONN_ID = "do-greenplum"

TELEGA = "@NPV42"
DESCRIPTION = "Заполнение dict_office.suppliers_warehouse_v3 на ch-3 данными из API sam-frontend (v3)"

BUF_SOURCE = "buffer.suppliers_warehouse_sources_v3"
BUF_CONNECTIONS = "buffer.suppliers_warehouse_connections_v3"

DICT_SOURCE = "dict_office.suppliers_warehouse_sources_v3"
DICT_CONNECTIONS = "dict_office.suppliers_warehouse_connections_v3"

DST_TAB = "dict_office.suppliers_warehouse_v3"

TRUNCATE_BUFFER_SOURCES = f"""
TRUNCATE TABLE {BUF_SOURCE};
"""

TRUNCATE_BUFFER_CONNECTIONS = f"""
TRUNCATE TABLE {BUF_CONNECTIONS};
"""

UPDATE_SUPPLIERS_WAREHOUSE = f"""
ALTER TABLE {DICT_SOURCE}
ATTACH PARTITION tuple()
FROM {BUF_SOURCE}
;

ALTER TABLE {DICT_CONNECTIONS}
ATTACH PARTITION tuple()
FROM {BUF_CONNECTIONS}
;

CREATE TEMPORARY TABLE updated_office_ids
ENGINE = Set
AS
SELECT id
  FROM {BUF_SOURCE}
 UNION ALL
SELECT source_id
  FROM {BUF_CONNECTIONS}

;
   INSERT INTO {DST_TAB}
          (office_id, office_name, type, supplier_id, supplier_name, dimensions_type, open_time, close_time, is_24_hours, address, region_id, country_code,
           currency_id, is_deleted, latitude, longitude, long_lat, res_type)

   SELECT untuple(src.datapack),
          con.res_type as res_type
    FROM (
            SELECT id,
                   argMax(tuple(
                           /* не менять порядок в тупле */
                           id, name, type, supplier_id, supplier_name, dimensions_type, open_time_tm, close_time_tm, is_24_hours, address_txt, region_id, country_code,
                           currency_id, is_deleted, latitude, longitude, coords),
                       row_ver) AS datapack
              FROM {DICT_SOURCE}
             WHERE id IN updated_office_ids
          GROUP BY id
--             HAVING NOT argMax(is_sc_flg, row_ver)
--                AND NOT argMax(is_wb_flg, row_ver)
         ) AS src
LEFT ANY
    JOIN (
            SELECT source_id,
                   argMax(res_type, row_ver) AS res_type
              FROM {DICT_CONNECTIONS}
             WHERE source_id IN updated_office_ids
          GROUP BY source_id
         ) AS con
      ON src.id = con.source_id
;

OPTIMIZE TABLE {DST_TAB} FINAL
"""


def get_api_content(api_url, auth_token, row_ver, limit=50, **kwargs) -> list:
    log.info(f"get data from url: {api_url}")
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(connect=10, backoff_factor=1))
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    params = {"row_ver": row_ver, "limit": limit}
    params.update(kwargs)

    resp = session.get(api_url, headers={"Authorization": auth_token}, params=params, verify=False)
    resp.raise_for_status()
    return resp.json()


def reorder_fields(content, table_fields: list):
    max_value = 0
    for idx, item in enumerate(content):
        content[idx] = [item.get(k) for k in table_fields]
        max_value = max(max_value, item["row_ver"])
    return max_value


def load_sources(hook, cutoff):
    # опрос апи
    api_conf = BaseHook().get_connection(API_CONN_ID)
    items = get_api_content(
        api_conf.host + "/sources", api_conf.password, row_ver=cutoff, limit=5000, type="all", details=False
    )["data"]
    if not items:
        log.info(f"Empty for {DICT_SOURCE}")
        return
    for item in items:
        item["longitude"] = item["coords"][0]
        item["latitude"] = item["coords"][1]
        item["supplier_id"] = min(999_999_999, item["supplier_id"])

    # инсерт
    table_fields = [
        "id",
        "name",
        "type",
        "supplier_id",
        "supplier_name",
        "dimensions_type",
        "open_time_tm",
        "close_time_tm",
        "is_24_hours",
        "address_txt",
        "region_id",
        "country_code",
        "currency_id",
        "row_ver",
        "is_deleted",
        "latitude",
        "longitude",
        "coords",
    ]
    max_row_ver = reorder_fields(items, table_fields)
    hook.exec_with_log(TRUNCATE_BUFFER_SOURCES)
    hook.bulk_dump(table=BUF_SOURCE, columns=table_fields, data=items)
    return max_row_ver


def load_connections(hook, cutoff):
    # опрос апи
    api_conf = BaseHook().get_connection(API_CONN_ID)
    items = get_api_content(api_conf.host + "/connections", api_conf.password, row_ver=cutoff, limit=5000)["data"]
    if not items:
        log.info(f"Empty for {DICT_CONNECTIONS}")
        return

    array_fields = ["regions", "logistic_restrictions", "special_delivery", "intervals", "availability_info"]
    for item in items:
        for f in array_fields:
            if item[f] is not None and (isinstance(item[f], list) or isinstance(item[f], dict)):
                item[f] = str(item[f])

    # инсерт
    table_fields = [
        "id",
        "name",
        "source_id",
        "supplier_id",
        "supplier_name",
        "res_type",
        "dimensions_type",
        "src_country_code",
        "dst_country_code",
        "logistic_restrictions",
        "row_ver",
        "is_deleted",
        "regions",
        "special_delivery",
        "delivery_time_minutes",
        "delivery_cost",
        "delivery_cost_whe",
        "order_min_cost",
        "order_min_cost_for_free_delivery",
        "order_min_cost_for_free_delivery_whe",
        "is_dbs_delivery_pvz",
        "delivery_interval_start",
        "delivery_interval_end",
        "intervals",
        "availability_info",
        "is_licensed_pvz_available",
        "delivery_time_minutes_whe",
        "order_min_cost_whe",
    ]
    max_row_ver = reorder_fields(items, table_fields)
    hook.exec_with_log(TRUNCATE_BUFFER_CONNECTIONS)
    hook.bulk_dump(table=BUF_CONNECTIONS, columns=table_fields, data=items)
    return max_row_ver


@with_db(CH3_CONN_ID)
@load_and_save_cutoff(DICT_SOURCE, None, "max_int", "src", None, "select 1")
@load_and_save_cutoff(DICT_CONNECTIONS, None, "max_int", "con", None, "select 1")
def update_suppliers_warehouse(hook, src_cutoff, con_cutoff):
    new_src_cutoff = load_sources(hook, src_cutoff)
    new_con_cutoff = load_connections(hook, con_cutoff)
    hook.exec_with_log(UPDATE_SUPPLIERS_WAREHOUSE)
    return new_con_cutoff, new_src_cutoff


@with_db(PG_CONN_ID, "pg")
def copy_data_ch_to_pg(pg_curs):
    COLUMNS = """office_id, office_name, type, supplier_id, supplier_name, dimensions_type, open_time, close_time,
        is_24_hours, address, region_id, country_code, currency_id, is_deleted, latitude, longitude, res_type, ts"""
    pg_curs.execute("truncate table dict.suppliers_warehouse_v3")

    clh_cur = CursIOClickHouse(
        connection_id=CH3_CONN_ID, query=f"select {COLUMNS} from dict_office.suppliers_warehouse_v3", use_header=False
    )
    clh_cur.execute()
    pg_curs.copy_expert(
        f"""COPY dict.suppliers_warehouse_v3 ({COLUMNS}) FROM STDIN
              (format csv, delimiter '|', header FALSE, encoding 'utf-8', force_null({COLUMNS}), NULL '\\N')""",
        clh_cur,
        100_000,
    )

@with_db(GP_CONN_ID, "gp")
def copy_data_ch_to_gp(gp_curs):
    COLUMNS_SRC = """office_id, office_name, supplier_name, supplier_id, res_type as delivery_type"""
    COLUMNS_TGT = """office_id, office_name, supplier_name, supplier_id, delivery_type"""
    gp_curs.execute("truncate table stage_external.suppliers_warehouse")

    clh_cur = CursIOClickHouse(
        connection_id=CH3_CONN_ID, query=f"select {COLUMNS_SRC} from dict_office.suppliers_warehouse_v3", use_header=False
    )
    clh_cur.execute()
    gp_curs.copy_expert(
        f"""COPY stage_external.suppliers_warehouse ({COLUMNS_TGT}) FROM STDIN
              (format csv, delimiter '|', header FALSE, encoding 'utf-8', force_null({COLUMNS_TGT}), NULL '\\N')""",
        clh_cur,
        100_000,
    )


with DAG(
    dag_id="ch3_external_suppliers_warehouse_v3",
    description=DESCRIPTION,
    tags=["api", API_CONN_ID, CH3_CONN_ID],
    schedule="50 5 * * *",
    start_date=datetime(2025, 1, 11),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    default_args=dict(owner="kamaltdinov.r6", telegram=[TELEGA], retry_delay=timedelta(minutes=10), retries=2),
) as dag:
    task_update_suppliers_warehouse = PythonOperator(
        task_id="task_update_suppliers_warehouse",
        python_callable=update_suppliers_warehouse,
        pool=get_pool(CH3_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.API, fqn="sam-frontend", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{BUF_SOURCE}", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{BUF_CONNECTIONS}", key="g3"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DICT_SOURCE}", key="g4"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DICT_CONNECTIONS}", key="g4"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{BUF_SOURCE}", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{BUF_CONNECTIONS}", key="g1"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DICT_SOURCE}", key="g2"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DICT_CONNECTIONS}", key="g3"),
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DST_TAB}", key="g4"),
        ],
    )

    task_send_suppliers_warehouse_pg = PythonOperator(
        task_id="task_send_suppliers_warehouse_pg",
        python_callable=copy_data_ch_to_pg,
        dag=dag,
        pool=get_pool(PG_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DST_TAB}"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.suppliers_warehouse_v3"),
        ],
    )

    task_send_suppliers_warehouse_gp = PythonOperator(
        task_id="task_send_suppliers_warehouse_gp",
        python_callable=copy_data_ch_to_gp,
        dag=dag,
        pool=get_pool(GP_CONN_ID),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.default.{DST_TAB}"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-gp.dwh.stage_external.suppliers_warehouse"),
        ],
    )

    task_update_suppliers_warehouse >> [task_send_suppliers_warehouse_pg, task_send_suppliers_warehouse_gp]
