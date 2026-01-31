import logging as log
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from json import JSONDecodeError
from urllib.parse import urlencode

import requests
import urllib3
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from python_utils import batcher
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError

urllib3.disable_warnings()

from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from urllib3.exceptions import ProtocolError

from utils.curs import CursDBIO, CursIOClickHouse, CursIODict
from utils.data_exchange import copy_to_kh_csv
from utils.decorators_with_conn import with_db
from utils.openmeta_helper import Entity

AUTH_CONN_ID = "api-auth-branch_office"

OFFICE_CREATOR_URL = "https://office-creator-api.wildberries.ru/api/v2/"

WORKERS_COUNT = 20

GEO_API_CONN_ID = "api-geo-ext-delivery"
GIS_API_CONN_ID = "api-gis-info"

PG_CONN_ID = "do-pg1-bo"
GP_CONN_ID = "do-greenplum"
DST_CONNECTION_VACUUM = "do-greenplum-vacuum"
CH3_CONN = "do-ch3"

BUF_TABLE_OFFICE_API = "buffer.office_creator_api_all"
BUF_TABLE_BRANCH_OFFICE = "buffer_bo.branch_offices"
BUF_TABLE_BRANCH_OFFICE_V = "buffer_bo.branch_office"
BUF_TABLE_KLADR_TREE = "buffer_bo.kladr_tree"


class AuthOrrError(Exception):
    pass


thread_local = threading.local()
vars(thread_local)


class OrrBearerAuth(requests.auth.AuthBase):
    def __init__(self):
        get_auth_token()

    def __call__(self, req):
        token = get_auth_token()
        req.headers["authorization"] = token["token_type"] + " " + token["access_token"]
        return req


def get_auth_token() -> str:
    attr_id = "orr_auth_token"

    if not hasattr(thread_local, attr_id):
        http_hook = BaseHook().get_hook(AUTH_CONN_ID)
        resp = http_hook.run(data=b"grant_type=client_credentials")
        setattr(thread_local, attr_id, resp.json())

    return getattr(thread_local, attr_id)


def get_or_create_http_session(session_id="session", auth_class=None):
    if not hasattr(thread_local, session_id):
        session = requests.Session()

        if auth_class:
            session.auth = auth_class()
        session.verify = False

        adapter = HTTPAdapter(max_retries=3)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        setattr(thread_local, session_id, session)

    return getattr(thread_local, session_id)


"""
########################################################################################################################
### OFFICE API TASKS
########################################################################################################################
"""


def get_office_api_content(api_url, map_record_hook, item_id=None) -> list:
    session = get_or_create_http_session("get_office_api_content", OrrBearerAuth)

    resp = session.get(api_url)
    resp.raise_for_status()

    try:
        item_data = resp.json()
        return map_record_hook(item_data, item_id) if map_record_hook else item_data
    except JSONDecodeError:
        log.critical(
            "JSONDecodeError: api unexpected response content-type (%s): %s, %s",
            resp.status_code,
            resp.headers.get("content-type"),
            api_url,
        )
    except ConnectionError:
        log.critical(
            "ConnectionError: api unexpected response content-type (%s): %s, %s",
            resp.status_code,
            resp.headers.get("content-type"),
            api_url,
        )
        raise


def save_api_data(cur, host, url, table_name, table_fields, map_record_hook=None, ids_list=None):
    log.info(f"load data from api to table {table_name}")

    content = []

    if ids_list:
        with ThreadPoolExecutor(max_workers=WORKERS_COUNT) as executor:
            futures = [
                executor.submit(
                    get_office_api_content, host + url + str(item_id), item_id=item_id, map_record_hook=map_record_hook
                )
                for item_id in ids_list
            ]

        for f in as_completed(futures):
            if f.result():
                content.append(f.result())
    else:
        content = get_office_api_content(host + url, map_record_hook=map_record_hook)

    if not len(content):
        log.info("got 0 records from api - skipping")
        return

    fld_order = table_fields.replace(" ", "").split(",")

    for idx, item in enumerate(content):
        content[idx] = {k: item.get(k) for k in fld_order}

    log.info(f"truncate table {table_name}")
    cur.execute(f"truncate table {table_name}")

    io_data = CursIODict(content)

    log.info(f"Upload data to {table_name}")
    query = f"""copy {table_name} ({table_fields}) from STDIN with (delimiter '|', FORMAT csv, header FALSE, FORCE_NULL({table_fields}))"""

    cur.copy_expert(query, io_data)


### creator api
@with_db(PG_CONN_ID, "pg")
def get_creator_api_branch_offices(pg_curs):
    log.info("get_creator_api_branch_offices")

    save_api_data(
        pg_curs,
        OFFICE_CREATOR_URL,
        "/offices/list",
        BUF_TABLE_OFFICE_API,
        "office_id, change_dt, change_employee_id, office_name, open_date, close_date, is_site_active, utc_timezone, office_shk, type_point, full_address, lat, lon, pvz_limit, limit_multiplier, area, cnt_fitting, way_info, work_time, company_change_date, country_code, region, city, street, house, level, props",
    )

    log.info("get_creator_api_branch_offices - done")


@with_db(PG_CONN_ID, "pg")
def fix_office_api(pg_curs):
    log.info("make local fixes")

    pg_curs.execute(f"""update {BUF_TABLE_OFFICE_API} get_by_id
    set lat = override.lat, lon = override.long
    from dict.geo_branch_office_override override
    where get_by_id.office_id = override.office_id""")

    log.info("make local fixes - done")


"""
########################################################################################################################
### GEO AND GIS TASK
########################################################################################################################
"""


def get_hour_offset(item, gis_conf):
    office_id, longitude, latitude = item

    session = get_or_create_http_session("http_geo_api")

    headers = {"Content-Type": "application/json", "Authorization": gis_conf.login + " " + gis_conf.password}

    params = urlencode({"longitude": longitude, "latitude": latitude})

    try:
        resp = session.get(gis_conf.host, params=params, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        return False
    except requests.exceptions.HTTPError:
        return False
    except ProtocolError:
        return False
    except ConnectionResetError:
        return False

    try:
        data = resp.json()
        return data["data"]

    except JSONDecodeError:
        log.critical(
            f"get_hour_offset (office_id: {office_id}, longitude: {longitude}, latitude: {latitude}): unexpected response content-type (%s): %s",
            resp.status_code,
            resp.headers.get("content-type"),
        )


def get_geo_content(item, geo_conf, gis_conf):
    office_id, longitude, latitude = item

    log.info(f"get office: {office_id}, {longitude}, {latitude}")

    headers = {"Content-Type": "application/json", "Accept": "application/json", "X-Token": geo_conf.password}

    data = {"longitude": str(longitude), "latitude": str(latitude)}

    session = get_or_create_http_session("http_geo_content")

    try:
        resp = session.post(geo_conf.host, timeout=3, headers=headers, json=data)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        return False
    except requests.exceptions.HTTPError:
        return False
    except ProtocolError:
        return False
    except ConnectionResetError:
        return False

    try:
        data = resp.json()

        gis_info = get_hour_offset(item, gis_conf)

        if gis_info and gis_info["utc_offset"]:
            utc_offset = int(gis_info["utc_offset"])
        else:
            utc_offset = 0
            log.error(f"cant decode utc_data: {gis_info}")

        data.update({"office_id": office_id, "long": longitude, "lat": latitude, "hour_offset": utc_offset})
    except JSONDecodeError:
        log.critical(
            "get_geo_content: unexpected response content-type (%s): %s",
            resp.status_code,
            resp.headers.get("content-type"),
        )
        raise

    return data


@with_db(PG_CONN_ID, "pg")
def update_geo_branch_office(pg_curs):
    log.info("update_geo_branch_office")

    query = f"""
        select
            office_id,
            lon long,
            lat
        from
            {BUF_TABLE_OFFICE_API} get_by_id
        where
            lon is not null and lat is not null and
            exists(select from {BUF_TABLE_OFFICE_API} ol where get_by_id.office_id = ol.office_id)
            and not exists(select from dict.geo_branch_office gbo where gbo.office_id = get_by_id.office_id and gbo.long = get_by_id.lon and gbo.lat = get_by_id.lat)
    """

    hook = BaseHook(None)
    geo_conf = hook.get_connection(GEO_API_CONN_ID)
    gis_conf = hook.get_connection(GIS_API_CONN_ID)

    pg_curs.execute(query)
    items = pg_curs.fetchall()

    if not len(items):
        return False

    log.info(f"Upload data to geo_branch_office. Items count: {len(items)}")
    i = 0
    for part in batcher(items, 10):
        log.info(f"Iteration: {(i := i + 1)}")

        content = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(get_geo_content, item=item, geo_conf=geo_conf, gis_conf=gis_conf) for item in part
            ]

        for f in as_completed(futures):
            item = f.result()
            if item:
                content.append(item)

        io_data = CursIODict(content)

        query = """copy dict.geo_branch_office(text_formatted, pos, country_code, country, province, district, locality, street, house, raw, office_id, long, lat, hour_offset) from STDIN with (delimiter '|', FORMAT csv, header FALSE,
        FORCE_NULL(text_formatted, pos, country_code, country, district, province, locality, street, house, raw, office_id, long, lat))"""

        pg_curs.copy_expert(query, io_data)

    return True


@with_db(PG_CONN_ID, "pg")
def update_geo_branch_office_mp(pg_curs):
    log.info("update_geo_branch_office_mp")

    # Проверяем каких еще офисов, пришедших из супплаер складов (имя, координаты) не хватает в dict.geo_branch_office
    hook = BaseHook(None)
    geo_conf = hook.get_connection(GEO_API_CONN_ID)
    gis_conf = hook.get_connection(GIS_API_CONN_ID)

    query = """
        select
            office_id,
            longitude long,
            latitude lat
        from
            dict.suppliers_warehouse_v3 sw
        where
            longitude is not null and latitude is not null
            and not exists(select from dict.geo_branch_office gbo where gbo.office_id = sw.office_id and gbo.long = sw.longitude and gbo.lat = sw.latitude)
    """

    pg_curs.execute(query)
    items = pg_curs.fetchall()

    if not len(items):
        return False

    log.info(f"Upload data to geo_branch_office. Items count {len(items)}")
    i = 0
    for part in batcher(items, 10):
        log.info(f"Iteration: {(i := i + 1)}")
        content = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(get_geo_content, item=item, geo_conf=geo_conf, gis_conf=gis_conf) for item in part
            ]

        for f in as_completed(futures):
            item = f.result()
            if item:
                content.append(item)

        log.info(f"len content = {len(content)}")

        io_data = CursIODict(content)

        query = """copy dict.geo_branch_office(text_formatted, pos, country_code, country, province, district, locality, street, house, raw, office_id, long, lat, hour_offset)
        from STDIN with (delimiter '|', FORMAT csv, header FALSE,
        FORCE_NULL(text_formatted, pos, country_code, country, district, province, locality, street, house, raw, office_id, long, lat))"""

        pg_curs.copy_expert(query, io_data)

    log.info("update_geo_branch_office_mp - done")


@with_db(PG_CONN_ID, "pg")
def update_geo_branch_office_locality(pg_curs):
    log.info("update_geo_branch_office_locality")

    query = """update dict.geo_branch_office
        set country = 'Россия', text_formatted = replace(text_formatted, 'Российская Федерация', 'Россия')
        where country = 'Российская Федерация'
    """
    pg_curs.execute(query)

    query = """
        with raw as (
            select
                office_id,
                long,
                lat,
                latest_update_time,
                raw::json -> 'response' -> 'GeoObjectCollection' -> 'featureMember' -> 0 -> 'GeoObject' -> 'metaDataProperty' -> 'GeocoderMetaData' -> 'Address' -> 'Components' as aparts,
                row_number() over (partition by office_id, long, lat order by latest_update_time desc) as rn
            from
                dict.geo_branch_office
        ), locations as (
            select
                office_id,
                long,
                lat,
                latest_update_time,
                replace(replace(case when aparts ->0->>'kind' = 'country' then aparts ->0->>'name' end, 'Кыргызстан', 'Киргизия'), 'Российская Федерация', 'Россия') as country,
                case when aparts ->2->>'kind' = 'province' then aparts ->1->>'name' end as district,
                case when aparts ->2->>'kind' = 'province' then aparts ->2->>'name' else aparts ->1->>'name' end province
            from
                raw
            where
                rn = 1
        )
        update
            dict.geo_branch_office as bo
        set
            country = loc.country,
            province = loc.province,
            district = loc.district,
            id_country = null,
            id_fo = null,
            id_district = null,
            id_city = null,
            kladr_id = null
        from
            locations loc
        where
            bo.office_id = loc.office_id
            and bo.long = loc.long
            and bo.lat = loc.lat
            and bo.latest_update_time = loc.latest_update_time
            and (
                bo.country <> loc.country or
                bo.province <> loc.province or
                bo.district <> loc.district
            )
    """
    pg_curs.execute(query)

    query = """update dict.geo_branch_office
        set locality = split_part(replace(replace(replace(replace(replace(text_formatted, concat(', ', coalesce(house, '#')), ''), concat(', ', coalesce(street, '#')), ''), concat(', ', coalesce(province, '#')), ''), concat(coalesce(country, '#'), ', '), ''), coalesce(country, '#'), ''),', ',1)
        where locality is null;
    """
    pg_curs.execute(query)

    log.info("update_geo_branch_office_locality - done")


@with_db(PG_CONN_ID, "pg")
def update_geo_kladr(pg_curs):
    log.info("update_geo_kladr")

    query = """
        drop table if exists new_gis_data;
        create temporary table new_gis_data (
            id serial,
            id_country int,
            country varchar(500) null,
            id_district int,
            district varchar(500) null,
            id_province int,
            province varchar(500) null,
            id_locality int,
            locality varchar(500) null
        );

        insert into new_gis_data (country, district, province, locality)
        select distinct
            trim(case when coalesce(country, '') = '' then '-' else country end) country,
            trim(case when coalesce(district, '') = '' then '-' else district end) district,
            trim(case when coalesce(province, '') = '' then '-' else province end) province,
            trim(case when coalesce(locality, '') = '' then '-' else locality end) locality
        from
            dict.geo_branch_office
        where
            country is not null;

        -- country
        insert into dict.geo_kladr(pid, level, name)
        select distinct
            null::integer,
            0,
            country
        from
            new_gis_data og
        where
            not exists(select from dict.geo_kladr gc where gc.level = 0 and og.country = gc.name);

        update
            new_gis_data og
        set
            id_country = gc.id
        from
            (select distinct id, name from dict.geo_kladr where level = 0) gc
        where
            gc.name = og.country;

        -- district
        insert into dict.geo_kladr(pid, level, name)
        select distinct
            id_country pid,
            1,
            district
        from
            new_gis_data og
        where
            not exists(select from dict.geo_kladr gr where gr.level = 1 and og.district = gr.name and gr.pid = id_country);

        update
            new_gis_data og
        set
            id_district = gc.id
        from
            (select distinct pid, id, name from dict.geo_kladr where level = 1) gc
        where
            gc.pid = og.id_country and gc.name = og.district;

        -- province
        insert into dict.geo_kladr(pid, level, name)
        select distinct
            id_district pid,
            2,
            province
        from
            new_gis_data og
        where
            not exists(select from dict.geo_kladr gr where gr.level = 2 and og.province = gr.name and gr.pid = id_district);

        update
            new_gis_data og
        set
            id_province = gc.id
        from
            (select distinct pid, id, name from dict.geo_kladr where level = 2) gc
        where
            gc.pid = og.id_district and gc.name = og.province;

        -- locality
        insert into dict.geo_kladr(pid, level, name)
        select distinct
            id_province pid,
            3,
            locality
        from
            new_gis_data og
        where
            not exists(select from dict.geo_kladr gr where gr.level = 3 and og.locality = gr.name and gr.pid = id_province);

        -- проверка на задвоение
        update
            new_gis_data og
        set
            id_locality = gc.id
        from
            (select distinct pid, id, name from dict.geo_kladr where level = 3) gc
        where
            gc.pid = og.id_province and gc.name = og.locality;
    """

    pg_curs.execute(query)
    log.info("update_geo_kladr - done")


@with_db(PG_CONN_ID, "pg")
def update_geo_branch_office_ids(pg_curs):
    log.info("update_geo_branch_office_ids")

    query = """
        -- country
        update
            dict.geo_branch_office gbo
        set
            id_country = gk.id,
            kladr_id = gk.id
        from
            dict.geo_kladr gk
        where
            country_code is not null and
            coalesce(gbo.country, '-') = gk.name and level = 0;

        -- district
        update
            dict.geo_branch_office gbo
        set
            id_fo = gk.id,
            kladr_id = gk.id
        from
            dict.geo_kladr gk
        where
            country_code is not null and
            gk.pid = gbo.id_country and coalesce(gbo.district, '-') = gk.name and level = 1;

        -- province
        update
            dict.geo_branch_office gbo
        set
            id_district = gk.id,
            kladr_id = gk.id
        from
            dict.geo_kladr gk
        where
            gk.pid = gbo.id_fo and coalesce(gbo.province, '-') = gk.name and level = 2;

        -- locality
        update
            dict.geo_branch_office gbo
        set
            id_city = gk.id,
            kladr_id = gk.id
        from
            dict.geo_kladr gk
        where
            gk.pid = gbo.id_district and coalesce(gbo.locality, '-') = gk.name and level = 3;
        """
    pg_curs.execute(query)
    log.info("update_geo_branch_office_ids - done")


@with_db(PG_CONN_ID, "pg")
def update_geo_kladr_tree(pg_curs):
    log.info("update_geo_kladr_tree")

    query = """
        with ids as (
            select
                l0.id,
                l0.name,
                case when (l4.level = 0) then l4.id when (l3.level = 0) then l3.id when (l2.level = 0) then l2.id when (l1.level = 0) then l1.id when (l0.level = 0) then l0.id else null::int end as id_country,
                case when (l4.level = 1) then l4.id when (l3.level = 1) then l3.id when (l2.level = 1) then l2.id when (l1.level = 1) then l1.id when (l0.level = 1) then l0.id else null::int end as id_region,
                case when (l4.level = 2) then l4.id when (l3.level = 2) then l3.id when (l2.level = 2) then l2.id when (l1.level = 2) then l1.id when (l0.level = 2) then l0.id else null::int end as id_district,
                case when (l4.level = 3) then l4.id when (l3.level = 3) then l3.id when (l2.level = 3) then l2.id when (l1.level = 3) then l1.id when (l0.level = 3) then l0.id else null::int end as id_city
            from
                dict.geo_kladr l0
                left join dict.geo_kladr l1 on l0.pid = l1.id
                left join dict.geo_kladr l2 on l1.pid = l2.id
                left join dict.geo_kladr l3 on l2.pid = l3.id
                left join dict.geo_kladr l4 on l3.pid = l4.id
        ), kladr as (
            select
                country.id id_country,
                country.name country_name,
                row_number() over (order by country.name) country_order_by,

                fo.id id_fo,
                fo.name fo_name,
                row_number() over (order by fo.name) fo_order_by,

                district.id id_district,
                district.name district_name,
                row_number() over (order by district.name) district_order_by,

                city.id id_city,
                city.name city_name,
                row_number() over (order by city.name) city_order_by,

                ids.id kladr_id,
                ids.name kladr_name
            from
                ids
                left join dict.geo_kladr country on country.id = ids.id_country
                left join dict.geo_kladr fo on fo.id = ids.id_region
                left join dict.geo_kladr district on district.id = ids.id_district
                left join dict.geo_kladr city on city.id = ids.id_city
        ), region as (
            select
                coalesce(country, '-') country,
                coalesce(district, '-') district,
                coalesce(province, '-') province,
                region_id,
                region_name
            from
                dict.geo_kladr_mapping_region_id
        ), country_code as (
            select distinct
                coalesce(id_country, 0) id_country,
                coalesce(country_code, '-') country_code
            from
                dict.geo_branch_office
            order by
                id_country
        )
        insert into dict.kladr_tree (id_country, country_name, country_code, country_order_by, id_fo, fo_name, fo_order_by, id_region, region_name, region_order_by, id_district, district_name, district_order_by, id_city, city_name, city_order_by, kladr_id, kladr_full_name)
        select
            kladr.id_country,
            country_name,
            country_code,
            country_order_by,
            id_fo,
            fo_name,
            fo_order_by,
            coalesce(region.region_id, id_district, kladr.id_country) as id_region,
            coalesce(region_name, district_name, country_name) as region_name,
            row_number() over (order by coalesce(region_name, district_name, country_name)) region_order_by,
            id_district,
            district_name,
            district_order_by,
            id_city,
            city_name,
            city_order_by,
            kladr_id,
            concat(country_name, ', ', district_name, case when id_city is not null then concat(', ', city_name) else '' end) kladr_full_name
        from
            kladr
            left join region on
                kladr.country_name = region.country
                and kladr.fo_name = region.district
                and kladr.district_name = region.province
            left join country_code on
                kladr.id_country = country_code.id_country
        where
            not exists(select from dict.kladr_tree kt where kt.kladr_id = kladr.kladr_id)
        ;
    """
    pg_curs.execute(query)

    log.info("update_geo_kladr_tree - done")


"""
########################################################################################################################
### GETTER-ы
########################################################################################################################
"""


@with_db(PG_CONN_ID, "pg")
def get_type_point(pg_curs):
    COLUMNS = "type_point, type_point_name"
    pg_curs.execute("truncate table dict.type_point")

    clh_cur = CursIOClickHouse(connection_id=CH3_CONN, query=f"select {COLUMNS} from dict.type_point", use_header=False)
    clh_cur.execute()
    pg_curs.copy_expert(
        f"""COPY dict.type_point ({COLUMNS}) FROM STDIN
              (format csv, delimiter '|', header FALSE, encoding 'utf-8', force_null({COLUMNS}), NULL '\\N')""",
        clh_cur,
        100_000,
    )


@with_db(PG_CONN_ID, "pg")
def get_region_oksm(pg_curs):
    COLUMNS = "region_id, code_okato, id_fed, status_name, region_name, region_second_name, country_2, country_3"
    pg_curs.execute("truncate table dict.region_oksm")

    clh_cur = CursIOClickHouse(
        connection_id=CH3_CONN, query=f"select {COLUMNS} from dict_office.region_oksm", use_header=False
    )
    clh_cur.execute()
    pg_curs.copy_expert(
        f"""COPY dict.region_oksm ({COLUMNS}) FROM STDIN
              (format csv, delimiter '|', header FALSE, encoding 'utf-8', force_null({COLUMNS}), NULL '\\N')""",
        clh_cur,
        100_000,
    )


@with_db(PG_CONN_ID, "pg")
def get_human_resource_employee(pg_curs):
    COLUMNS = "employee_id,employee_name,department_id"
    pg_curs.execute("truncate table dict.human_resource_employee")

    clh_cur = CursIOClickHouse(
        connection_id=CH3_CONN, query=f"select {COLUMNS} from dict.human_resource_employee", use_header=False
    )
    clh_cur.execute()
    pg_curs.copy_expert(
        f"""COPY dict.human_resource_employee ({COLUMNS}) FROM STDIN
              (format csv, delimiter '|', header FALSE, encoding 'utf-8', force_null({COLUMNS}), NULL '\\N')""",
        clh_cur,
        100_000,
    )


@with_db(PG_CONN_ID, "pg")
def update_offices(pg_curs):
    log.info("update_offices")

    query = f"""
        begin transaction;

        truncate table dict.branch_offices;

        with office_kladr as (
            select
                oid.office_id,

                coalesce(ok_country.region_id, kt.id_country) as id_country,
                coalesce(ok_country.region_name, kt.country_name) as country_name,
                coalesce(ok_country.country_2, kt.country_code, gbo.country_code) as country_code,

                coalesce(ok_region.region_id, kt.id_region) as id_region,
                coalesce(ok_region.region_name, kt.region_name) as region_name,

                coalesce(kt.id_city, kt.id_district, kt.id_region, kt.id_fo, kt.id_country) as id_city,
                coalesce(kt.city_name, kt.district_name, kt.region_name, kt.fo_name, kt.country_name) as city_name,
                gbo.hour_offset
            from
                {BUF_TABLE_OFFICE_API} oid
                inner join dict.geo_branch_office gbo on
                    oid.office_id = gbo.office_id
                    and oid.lon = gbo.long
                    and oid.lat = gbo.lat
                left join dict.kladr_tree kt on
                    gbo.kladr_id = kt.kladr_id

                left join dict.region_oksm ok_country on
                    kt.id_country = ok_country.region_id

                left join dict.region_oksm ok_region on
                    kt.id_region = ok_region.region_id
        ), poo as (
            select
                office_id,
                poo_id,
                poo_name,
                null as sm_id,
                null as sm_name
            from
                dict.logistics_poo
        ), employee_names as (
            select
                employee_id,
                employee_name
            from
                dict.human_resource_employee
            where employee_id in (
                select distinct rf_employee_id from dict.heritage_api_office_get_all_chiefs
                union select distinct rr_employee_id from dict.heritage_api_office_get_all_chiefs
                union select distinct zrr_employee_id from dict.heritage_api_office_get_all_chiefs
            )
        ), main_offices as (
            select
                main_office_id,
                max(case when c_api_all.type_point = 12 then 1 else 0 end)::bool as has_dc,
                max(case when c_api_all.type_point = 11 then 1 else 0 end)::bool as has_sc, -- СЦ (сортировочный центр)
                max(case when coalesce(mp.office_id, 0) > 0 then 1 else 0 end)::bool as has_mp,
                max(case when coalesce(wh.office_id, 0) > 0 then 1 else 0 end)::bool as has_wh
            from
                dict.under_one_roof_offices un

                left join {BUF_TABLE_OFFICE_API} c_api_all on
                    un.office_id = c_api_all.office_id

                left join dict.branch_offices_mp mp on
                    un.office_id = mp.office_id
                    and lower(mp.supplier_name) != 'wildberries'

                left join dict.branch_offices_wh wh on
                    un.office_id = wh.office_id
            group by
                main_office_id
        )
        insert into dict.branch_offices (
            office_id,
            office_name,
            old_office_id,
            type_point,
            type_point_name,
            office_shk,
            open_date,
            close_date,
            is_orr_open,
            estimated_activation_date,
            company_change_date,
            rent_contract_date,
            rental_holidays_end_date,
            full_address,
            country_id,
            country_name,
            region_id,
            region_name,
            city_id,
            city_name,
            street_name,
            house_name,
            office,
            ext_phone,
            phone,
            work_time,
            company_id,
            latitude,
            longitude,
            hour_offset,
            is_site_active,
            is_orr_branded,
            is_orr_franchise,
            is_franchise,
            is_dc,
            is_sc,
            is_wh,
            is_poo,
            is_mp,
            is_for_employee,
            is_wave_breaker,
            is_manufacturing,
            is_courier,
            franchise_way_info,
            region_chief_id,
            region_chief_name,
            region_deputy_chief_id,
            region_deputy_chief_name,
            office_chief_id,
            office_chief_name,
            poo_country,
            poo_id,
            poo_name,
            group_id,
            group_name,
            area,
            area_warehouse,
            warehouse_id,
            organization_id,
            home_id,
            dc_office_id,
            place_cod,
            cfo_id,
            poo_call_delay,
            dz_call_delay,
            delivery_days_period,
            og_id,
            supplier_id,
            supplier_to_wh_id,
            supplier_to_bo_id,
            cmrds_id,
            warehouse_office_id,
            is_need_invoice,
            is_seal_shipping,
            ctb_id,
            complication_delivery,
            is_outfit_assembly_available,
            has_courier_wh,
            currency_id,
            has_own_packages,
            wb_region_id,
            wb_region_name,
            props
        )
        select DISTINCT
            c_api_all.office_id,
            c_api_all.office_name::varchar(300) as office_name,
            lbo.old_office_id[1],
            c_api_all.type_point as type_point,
            type_point.type_point_name::varchar(100) as type_point_name,
            coalesce(pickpoints.office_shk, c_api_all.office_shk)::varchar(50) as office_shk,
            coalesce(pickpoints.open_date, c_api_all.open_date) as open_date,
            coalesce(pickpoints.close_date, c_api_all.close_date) as close_date,
            null::bool as is_orr_open, -- deprecated
            null::date as estimated_activation_date,
            null::date company_change_date, -- deprecated
            null::date as rent_contract_date,
            null::date as rental_holidays_end_date,
            c_api_all.full_address::varchar(500) as full_address,
            office_kladr.id_country country_id,
            office_kladr.country_name country_name,
            office_kladr.id_region as region_id,
            office_kladr.region_name as region_name,
            office_kladr.id_city city_id,
            office_kladr.city_name as city_name,
            lbo.street_name street_name,
            lbo.house_name house_name,
            null as office,
            null as  ext_phone,
            null phone,  -- deprecated
            c_api_all.work_time::varchar(100) work_time,
            lbo.company_id company_id,
            c_api_all.lat latitude,
            c_api_all.lon longitude,
            coalesce(office_kladr.hour_offset, c_api_all.utc_timezone) as hour_offset,
            coalesce(pickpoints.is_site_active, c_api_all.is_site_active) as is_site_active,

            null::bool as is_orr_branded,  -- deprecated!
            null::bool as is_orr_franchise,  -- deprecated!
            null::bool as is_franchise,  -- deprecated!

            c_api_all.type_point = 12 or coalesce(mo.has_dc, false) as is_dc, -- РЦ (распределительный центр)
            c_api_all.type_point = 11 or coalesce(mo.has_sc, false) as is_sc, -- СЦ (сортировочный центр)
            (case when coalesce(wh.office_id, 0) > 0 then 1 else 0 end)::bool or coalesce(mo.has_wh, false) as is_wh, -- Склад
            c_api_all.type_point = any (array [1, 2, 3, 5, 6, 7, 8, 9, 10]) as is_poo, -- ПВЗ / ППВЗ
            (case when coalesce(mp.office_id, 0) > 0 then 1 else 0 end)::bool as is_mp,

            false is_for_employee, -- deprecated!
            lbo.is_wave_breaker,
            lbo.is_manufacturing,
            false is_courier, -- deprecated!
            null franchise_way_info, -- deprecated!

            coalesce(all_chiefs.rr_employee_id, r.region_chief_id) region_chief_id,
            coalesce(rr_employee_name.employee_name, r.region_chief_name, '-') as region_chief_name,

            all_chiefs.zrr_employee_id region_deputy_chief_id,
            coalesce(zrr_employee_name.employee_name, '-') region_deputy_chief_name,

            all_chiefs.rf_employee_id office_chief_id,
            coalesce(rf_employee_name.employee_name, '-') office_chief_name,

            office_kladr.country_code as poo_country,

            poo.poo_id,
            poo.poo_name,

            null::int group_id,
            null::int group_name,

            lbo.area area,
            lbo.area_warehouse area_warehouse,

            lbo.warehouse_id,
            null::int as organization_id,
            null::int as home_id,
            lbo.dc_office_id,
            lbo.place_cod,
            lbo.cfo_id,
            null::int as poo_call_delay,
            null::int as dz_call_delay,
            null::int as delivery_days_period,
            null::int as og_id,
            lbo.supplier_id,
            lbo.supplier_to_wh_id,
            lbo.supplier_to_bo_id,
            null::int as cmrds_id,
            lbo.warehouse_office_id,
            lbo.is_need_invoice,
            lbo.is_seal_shipping,
            null::int as ctb_id,
            lbo.complication_delivery,
            null::bool as is_outfit_assembly_available,
            lbo.has_courier_wh,
            coalesce(lbo.currency_id, 0) currency_id,
            lbo.has_own_packages,
            region_get_structure.region_id as wb_region_id,
            region_get_structure.region_name as wb_region_name,
            c_api_all.props as props
        from
            {BUF_TABLE_OFFICE_API} c_api_all

            left join dict.branch_offices_wh wh on
                c_api_all.office_id = wh.office_id
            left join dict.branch_offices_mp as mp  on
                c_api_all.office_id = mp.office_id

            left join dict.logistics_branchoffice lbo on
                lbo.office_id = c_api_all.office_id

            left join dict.branch_offices_pickpoints pickpoints on
                pickpoints.office_id = c_api_all.office_id

            left join main_offices mo on
                c_api_all.office_id = mo.main_office_id

            left join poo on
                poo.office_id = c_api_all.office_id

            left join dict.type_point type_point on
                type_point.type_point = c_api_all.type_point

            left join dict.heritage_api_office_get_all_chiefs all_chiefs on
                all_chiefs.office_id = c_api_all.office_id

            left join employee_names as zrr_employee_name on
                zrr_employee_name.employee_id = all_chiefs.zrr_employee_id
            left join employee_names as rr_employee_name on
                rr_employee_name.employee_id = all_chiefs.rr_employee_id
            left join employee_names as rf_employee_name on
                rf_employee_name.employee_id = all_chiefs.rf_employee_id

            left join dict.heritage_api_region_get_structure region_get_structure on
                region_get_structure.office_id = c_api_all.office_id

            left join dict.region r on
                region_get_structure.region_id = r.region_id

            left join office_kladr on
                office_kladr.office_id = c_api_all.office_id
        ;

        create temporary table office_kladr as
        select
            oid.office_id,
            coalesce(ok_country.region_id, kt.id_country) as id_country,
            coalesce(ok_country.region_name, kt.country_name) as country_name,
            coalesce(ok_country.country_2, kt.country_code, gbo.country_code) as country_code,
            coalesce(ok_region.region_id, kt.id_region) as id_region,
            coalesce(ok_region.region_name, kt.region_name) as region_name,
            coalesce(kt.id_city, kt.id_district, kt.id_region, kt.id_fo, kt.id_country) as id_city,
            coalesce(kt.city_name, kt.district_name, kt.region_name, kt.fo_name, kt.country_name) as city_name,
            gbo.hour_offset
        from
            dict.branch_offices_mp oid
            inner join dict.geo_branch_office gbo on
                oid.office_id = gbo.office_id
                and oid.longitude = gbo.long
                and oid.latitude = gbo.lat
            left join dict.kladr_tree kt on
                gbo.kladr_id = kt.kladr_id

            left join dict.region_oksm ok_country on
                kt.id_country = ok_country.region_id

            left join dict.region_oksm ok_region on
                kt.id_region = ok_region.region_id;


        with poo as (
            select
                office_id,
                poo_id,
                poo_name,
                null as sm_id,
                null as sm_name
            from
                dict.logistics_poo
        )
        insert into dict.branch_offices (
            office_id,
            office_name,
            old_office_id,
            type_point,
            type_point_name,
            office_shk,
            open_date,
            close_date,
            is_orr_open,
            estimated_activation_date,
            company_change_date,
            rent_contract_date,
            rental_holidays_end_date,
            full_address,
            country_id,
            country_name,
            region_id,
            region_name,
            city_id,
            city_name,
            street_name,
            house_name,
            office,
            ext_phone,
            phone,
            work_time,
            company_id,
            latitude,
            longitude,
            hour_offset,
            is_site_active,
            is_orr_branded,
            is_orr_franchise,
            is_franchise,
            is_dc,
            is_sc,
            is_wh,
            is_poo,
            is_for_employee,
            is_wave_breaker,
            is_manufacturing,
            is_courier,
            franchise_way_info,
            region_chief_id,
            region_chief_name,
            region_deputy_chief_id,
            region_deputy_chief_name,
            office_chief_id,
            office_chief_name,
            poo_country,
            poo_id,
            poo_name,
            group_id,
            group_name,
            area,
            area_warehouse,
            warehouse_id,
            organization_id,
            home_id,
            dc_office_id,
            place_cod,
            cfo_id,
            poo_call_delay,
            dz_call_delay,
            delivery_days_period,
            og_id,
            supplier_id,
            supplier_to_wh_id,
            supplier_to_bo_id,
            cmrds_id,
            warehouse_office_id,
            is_need_invoice,
            is_seal_shipping,
            ctb_id,
            complication_delivery,
            is_outfit_assembly_available,
            has_courier_wh,
            currency_id,
            has_own_packages,
            is_mp
        )
        select DISTINCT
            mp.office_id,
            mp.office_name,
            null::bigint old_office_id,
            17 as type_point,
            'Склад поставщика' as type_point_name,
            null office_shk,
            created_at open_date,
            case when is_deleted then '2000-01-01'::date end close_date,
            not is_deleted is_orr_open,
            null::date estimated_activation_date,
            null::date company_change_date,
            null::date rent_contract_date,
            null::date rental_holidays_end_date,
            address full_address,
            office_kladr.id_country country_id,
            office_kladr.country_name country_name,
            office_kladr.id_region as region_id,
            office_kladr.region_name as region_name,
            office_kladr.id_city city_id,
            office_kladr.city_name as city_name,
            null street_name,
            null house_name,
            null office,
            null ext_phone,
            null phone,
            concat(open_time, ' ', close_time) work_time,
            supplier_id company_id,
            latitude,
            longitude,
            office_kladr.hour_offset,
            not is_deleted is_site_active,

            null::bool is_orr_branded,
            null::bool is_orr_franchise,
            null::bool is_franchise,

            null::bool is_dc,
            false as is_sc,
            null::bool is_wh,
            null::bool is_poo,
            null::bool is_for_employee,
            null::bool is_wave_breaker,
            null::bool is_manufacturing,
            con_is_wb_courier is_courier,
            null franchise_way_info,
            null::int region_chief_id,
            null region_chief_name,
            null::int region_deputy_chief_id,
            null region_deputy_chief_name,
            null::int office_chief_id,
            null office_chief_name,
            office_kladr.country_code as poo_country,
            poo.poo_id,
            poo.poo_name,
            null::int group_id,
            null group_name,
            null::numeric area,
            null::numeric area_warehouse,
            null::int warehouse_id,
            supplier_id organization_id,
            null::int home_id,
            null::int dc_office_id,
            null::int place_cod,
            null::int cfo_id,
            0 poo_call_delay,
            0 dz_call_delay,
            0 delivery_days_period,
            null::int og_id,
            null::int supplier_id,
            null::int supplier_to_wh_id,
            null::int supplier_to_bo_id,
            null::int cmrds_id,
            null::int warehouse_office_id,
            false is_need_invoice,
            false is_seal_shipping,
            null::int ctb_id,
            0 complication_delivery,
            null::bool is_outfit_assembly_available,
            null::bool has_courier_wh,
            0 currency_id,
            true has_own_packages,
            mp.type not in ('WH','FBS') as is_mp
        from
            dict.branch_offices_mp mp
            left join dict.kladr_tree kt on
                kt.kladr_id = mp.kladr_id
            left join poo on
                poo.office_id = mp.office_id
            left join office_kladr on
                office_kladr.office_id = mp.office_id
        where
            not exists(select from dict.branch_offices bo where mp.office_id = bo.office_id)
        ;

        update
            dict.branch_offices bo
        set
            region_name = kt.region_name
        from
            dict.kladr_tree kt
        where
            bo.city_id = kt.kladr_id
            and bo.region_name is null;

        commit transaction;
    """

    pg_curs.execute(query)

    log.info("update_offices - done")


@with_db(PG_CONN_ID, "pg")
def update_branch_offices_mp(pg_curs):
    log.info("update_branch_offices_mp")

    query = """
        begin transaction;
        truncate table dict.branch_offices_mp;

        with gbo as (
            select
                office_id, long longitude, lat latitude, kladr_id,
                row_number() over (partition by office_id order by latest_update_time desc) rn
            from dict.geo_branch_office gbo
            where exists(select from dict.suppliers_warehouse_v3 sw where gbo.office_id = sw.office_id and gbo.long = sw.longitude and gbo.lat = sw.latitude)
        )
        insert into dict.branch_offices_mp (office_id, office_name, latitude, longitude, is_kgt, is_disabled, is_24_hours_open, is_pickup_available, is_deleted,
            address, open_time, close_time, supplier_name, supplier_id, delivery_type,
            id_country, id_fo, id_region, id_district, id_city, kladr_id, dwh_date, type)
        select
            sw.office_id, sw.office_name, sw.latitude, sw.longitude,
            sw.dimensions_type > 0 as is_kgt,
            false as is_disabled,
            sw.is_24_hours as is_24_hours_open,
            false as is_pickup_available,
            sw.is_deleted,
            sw.address,
            sw.open_time::time as open_time,
            sw.close_time::time as close_time,
            sw.supplier_name,
            sw.supplier_id,
            sw.res_type as delivery_type,
            id_country, id_fo, id_region, id_district, id_city, gbo.kladr_id,
            sw.ts as dwh_date,
            sw.type
        from
            dict.suppliers_warehouse_v3 sw
            left join gbo on
                gbo.rn = 1
                and sw.office_id = gbo.office_id
                and sw.latitude = gbo.latitude
                and sw.longitude = gbo.longitude
            left join dict.kladr_tree kt on
                gbo.kladr_id = kt.kladr_id
        ;

        -- костыль, удаляем из мп офисов точно не мпшные
        delete from dict.branch_offices_mp
            where office_id in (
            select office_id from dict.branch_offices bo
            join dict.branch_offices_mp mp using (office_id)
            where (bo.type_point not in (0,17)
            and bo.office_name not like '%продавц%'
            and bo.office_name not like '% пост%'
            and bo.office_name not like '%силами%'
            and not mp.is_kgt) );

        commit transaction;
    """

    pg_curs.execute(query)

    log.info("update_branch_offices_mp - done")


@with_db(PG_CONN_ID, "pg")
def vacuum_tables(pg_hook):
    opt = "FULL ANALYZE"
    query = f"""
        VACUUM {opt} buffer.office_creator_api_all;
        VACUUM {opt} dict.geo_branch_office;
        VACUUM {opt} dict.branch_offices;
        VACUUM {opt} dict.branch_offices_mp;
        VACUUM {opt} dict.kladr_tree;
        VACUUM {opt} dict.geo_kladr;
        VACUUM {opt} dict.branch_offices_wh;
        VACUUM {opt} dict.office_region;
        VACUUM {opt} dict.branch_offices_pickpoints;
        VACUUM {opt} dict.logistics_branchoffice;
        VACUUM {opt} dict.logistics_poo;
        VACUUM {opt} dict.under_one_roof_offices;
        VACUUM {opt} dict.heritage_api_office_get_all_chiefs;
        VACUUM {opt} dict.heritage_api_region_get_structure;
        VACUUM {opt} dict.region;
        VACUUM {opt} dict.geo_kladr_mapping_region_id;
        VACUUM {opt} dict.type_point;
        VACUUM {opt} dict.suppliers_warehouse_v3;
        VACUUM {opt} dict.region_oksm;
        VACUUM {opt} dict.human_resource_employee;
    """

    log.info("VACUUM")
    pg_hook.exec_with_log(query, autocommit=True)
    log.info("VACUUM - done")


"""
########################################################################################################################
### SENDER-s
########################################################################################################################
"""

""" GP SENDERS """


def send_table_to_gp(src_curs, dst_curs, src_tab, dst_tab, src_col, dst_col):
    # Отправляет таблицу c PG на GP
    log.info(f"Send table to gp. from: {src_tab} to: {dst_tab}")

    SRC = f"""SELECT {src_col} FROM {src_tab};"""

    COPY_SRC = f"""
        copy {dst_tab} ({dst_col})
        from stdin (format csv, delimiter '|', encoding 'utf8', force_null ({dst_col}));"""

    """Копирую на GP"""
    dst_curs.execute(f"truncate table {dst_tab};")

    log.info("Подключаюсь..")
    io_curs = CursDBIO(src_conn=src_curs.connection, query=SRC)
    io_curs.execute()
    log.info("Подключился!")
    log.info("Вставляю..")

    dst_curs.copy_expert(COPY_SRC, io_curs, 50_000)
    log.info("Вставил..")

    if not io_curs.total_lines:
        log.info("got 0 lines, nothing to save")
        return


@with_db(PG_CONN_ID, "pg")
@with_db(GP_CONN_ID, "gp")
def send_office_to_gp(pg_curs, gp_curs):
    SRC_DST_FIELDS = """office_id, office_name, old_office_id, type_point, type_point_name, office_shk, open_date, close_date, is_orr_open, estimated_activation_date, company_change_date,
                         rent_contract_date, rental_holidays_end_date, full_address, country_id, country_name, region_id, region_name, city_id, city_name, street_name, house_name, office,
                         ext_phone, phone, work_time, company_id, latitude, longitude, hour_offset, is_site_active, is_orr_branded, is_orr_franchise, is_franchise, is_dc, is_sc, is_wh, is_poo,
                         is_for_employee, is_wave_breaker, is_manufacturing, is_courier, franchise_way_info, region_chief_id, region_chief_name, region_deputy_chief_id,
                         region_deputy_chief_name, office_chief_id, office_chief_name, poo_country, poo_id, poo_name, group_id, group_name, area, area_warehouse, warehouse_id, organization_id,
                         home_id, dc_office_id, place_cod, cfo_id, poo_call_delay, dz_call_delay, delivery_days_period, og_id, supplier_id, supplier_to_wh_id, supplier_to_bo_id, cmrds_id,
                         warehouse_office_id, is_need_invoice, is_seal_shipping, ctb_id, complication_delivery, is_outfit_assembly_available, has_courier_wh, currency_id, has_own_packages,
                         is_mp, wb_region_id, wb_region_name, props"""

    send_table_to_gp(
        src_curs=pg_curs,
        dst_curs=gp_curs,
        src_tab="dict.branch_offices",
        dst_tab=BUF_TABLE_BRANCH_OFFICE,
        src_col=SRC_DST_FIELDS,
        dst_col=SRC_DST_FIELDS,
    )


@with_db(PG_CONN_ID, "pg")
@with_db(GP_CONN_ID, "gp")
def send_kladr_tree_gp(pg_curs, gp_curs):
    SRC_DST_FIELDS = """id_country,country_name,country_code,country_order_by,id_fo,fo_name,fo_order_by,id_region,region_name,region_order_by,id_district,district_name,
                        district_order_by,id_city,city_name,city_order_by,kladr_id,kladr_full_name"""

    send_table_to_gp(
        src_curs=pg_curs,
        dst_curs=gp_curs,
        src_tab="dict.kladr_tree",
        dst_tab=BUF_TABLE_KLADR_TREE,
        src_col=SRC_DST_FIELDS,
        dst_col=SRC_DST_FIELDS,
    )


@with_db(PG_CONN_ID, "pg")
@with_db(GP_CONN_ID, "gp")
def send_office_v_gp(pg_curs, gp_curs):
    SRC_DST_FIELDS = """office_id, office_name, phone, ext_phone, start_date, close_date, estimated_activation_date,
             region_chief_id, region_chief_name, office_chief_id, office_chief_name, region_deputy_chief_id,
             region_deputy_chief_name, poo_id, poo_name, group_id, group_name, region_id, region_name, country_id,
             country_name, hour_offset, latitude, longitude, area, area_warehouse, work_time, is_for_employee,
             is_site_active, company_id, office_type, type_point, type_point_name, pvz_type, city_id, main_office_id,
             old_office_id, full_address, place_cod, supplier_id, props"""

    send_table_to_gp(
        src_curs=pg_curs,
        dst_curs=gp_curs,
        src_tab="dict.branch_office",
        dst_tab=BUF_TABLE_BRANCH_OFFICE_V,
        src_col=SRC_DST_FIELDS,
        dst_col=SRC_DST_FIELDS,
    )


@with_db(GP_CONN_ID, "gp")
def update_targets(gp_curs):
    log.info("update_office_targets")

    # Обновляем целевые оффисы на ГП
    gp_curs.execute(f"""
        begin transaction;

        delete from dict.branch_offices;

        insert into dict.branch_offices
        select * from {BUF_TABLE_BRANCH_OFFICE};

        end transaction;
    """)

    log.info("update_office_v_targets")
    # Обновляем бывшую вьюху офисов на ГП
    gp_curs.execute(f"""
        begin transaction;

        delete from dict.branch_office;

        insert into dict.branch_office
        select * from {BUF_TABLE_BRANCH_OFFICE_V};

        end transaction;
    """)

    log.info("update_kladr_tree_targets")
    # Обновляем целевые kladr_tree на ГП
    gp_curs.execute(f"""
        begin transaction;

        delete from dict.kladr_tree;

        insert into dict.kladr_tree
        select * from {BUF_TABLE_KLADR_TREE};

        end transaction;
    """)

    log.info("update_targets - done")


""" CH SENDERS """


def update_dict_office_branch_offices():
    ### Перенос данных dict.branch_offices на ch3 в таблицу dict_office.branch_office и в ch3.
    copy_to_kh_csv(
        src_connection=PG_CONN_ID,
        dst_connection=CH3_CONN,
        src_table_name="""(select office_id,office_name,phone,ext_phone,start_date,close_date
                        ,estimated_activation_date,region_chief_id,office_chief_id,region_deputy_chief_id,poo_id,poo_name
                        ,group_id,group_name,region_id,region_name,country_id,country_name,hour_offset,latitude
                        ,longitude,area,area_warehouse,work_time,is_for_employee,is_site_active,company_id
                        ,office_type,type_point,type_point_name,pvz_type,city_id,main_office_id,old_office_id,full_address,place_cod,supplier_id, props
                        from dict.branch_office) as t""",
        dst_table_name="dict_office.branch_office",
        columns="""office_id,office_name,phone,ext_phone,start_date,close_date,estimated_activation_date,
                        region_chief_id,office_chief_id,region_deputy_chief_id,poo_id,poo_name,group_id,group_name,region_id,
                        region_name,country_id,country_name,hour_offset,latitude,longitude,area,area_warehouse,work_time,
                        is_for_employee,is_site_active,company_id,office_type,type_point,type_point_name,pvz_type,city_id,
                        main_office_id,old_office_id,full_address,place_cod,supplier_id, props""",
        need_trunc="no",
    )


@with_db(CH3_CONN)
def update_dict_office_kladr_tree(hook):
    ### Перенос данных dict.kladr_tree на ch3 в таблицу dict_office.kladr_tree и в ch3.
    copy_to_kh_csv(
        src_connection=PG_CONN_ID,
        dst_connection=CH3_CONN,
        src_table_name="dict.kladr_tree",
        dst_table_name="buffer.kladr_tree",
        columns="""kladr_id,kladr_full_name,id_country,country_name,country_code,country_order_by,id_fo,fo_name,
                            fo_order_by,id_region,region_name,region_order_by,id_district,district_name,district_order_by,
                            id_city,city_name,city_order_by""",
        need_trunc="yes",
    )
    hook.exec_with_log("""
    ALTER TABLE dict_office.kladr_tree
    REPLACE PARTITION tuple()
    FROM buffer.kladr_tree
    """)


"""
########################################################################################################################
### THE END
########################################################################################################################
"""


default_args = {
    "owner": "stepanov.dmitriy22",
    "email": ["stepanov.dmitriy22@wildberries.work"],
    "telegram": ["@ionflux", "@NPV42"],
    "email_on_retry": False,
    "email_on_failure": True,
    "catchup": False,
    "max_active_tasks": 1,
    "retries": 4,
}

with DAG(
    default_args=default_args,
    dag_id="pg_api_region_and_offices",
    description="Даг обновления сводной таблицы офисов dict.branch_offices",
    schedule=timedelta(minutes=60),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=["dict", "api", "offices", PG_CONN_ID, CH3_CONN, GP_CONN_ID],
    max_active_tasks=4,
    max_active_runs=1,
) as dag:
    with TaskGroup(group_id="creator_api") as tg_creator_api:
        task_get_creator_api_branch_offices = PythonOperator(
            task_id="task_get_creator_api_branch_offices",
            python_callable=get_creator_api_branch_offices,
            retry_delay=timedelta(seconds=10),
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.API, fqn="office-creator-api"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.{BUF_TABLE_OFFICE_API}"),
            ],
        )

        task_fix_office_api = PythonOperator(
            task_id="task_fix_office_api",
            python_callable=fix_office_api,
            retry_delay=timedelta(seconds=10),
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office_override"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office_override", key="manual"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.{BUF_TABLE_OFFICE_API}"),
            ],
        )

        task_get_creator_api_branch_offices >> task_fix_office_api

    with TaskGroup(group_id="geo_api") as tg_geo_api:
        task_update_geo_branch_office = PythonOperator(
            task_id="task_update_geo_branch_office",
            python_callable=update_geo_branch_office,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.{BUF_TABLE_OFFICE_API}"),
                OMEntity(entity=Entity.API, fqn="geo-ext-delivery"),
                OMEntity(entity=Entity.API, fqn="gis-info"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
            ],
        )

        task_update_geo_branch_office_mp = PythonOperator(
            task_id="task_update_geo_branch_office_mp",
            python_callable=update_geo_branch_office_mp,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.suppliers_warehouse_v3"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
            ],
        )

        task_update_geo_branch_office_locality = PythonOperator(
            task_id="task_update_geo_branch_office_locality",
            python_callable=update_geo_branch_office_locality,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
        )

        task_update_geo_kladr = PythonOperator(
            task_id="task_update_geo_kladr",
            python_callable=update_geo_kladr,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_kladr"),
            ],
        )

        task_update_geo_branch_office_ids = PythonOperator(
            task_id="task_update_geo_branch_office_ids",
            python_callable=update_geo_branch_office_ids,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_kladr"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
            ],
        )

        task_update_geo_kladr_tree = PythonOperator(
            task_id="task_update_geo_kladr_tree",
            python_callable=update_geo_kladr_tree,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_kladr"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_kladr_mapping_region_id"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_kladr_mapping_region_id", key="manual"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.kladr_tree"),
            ],
        )

        task_update_dict_office_kladr_tree = PythonOperator(
            task_id="update_dict_office_kladr_tree",
            python_callable=update_dict_office_kladr_tree,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            doc="Перенос кладров на ch3",
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.kladr_tree"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.dict_office.kladr_tree"),
            ],
        )

        (
            task_update_geo_branch_office
            >> task_update_geo_branch_office_mp
            >> task_update_geo_branch_office_locality
            >> task_update_geo_kladr
            >> [task_update_geo_branch_office_ids, task_update_geo_kladr_tree]
            >> task_update_dict_office_kladr_tree
        )

    with TaskGroup(group_id="update") as tg_update:
        task_get_region_oksm = PythonOperator(
            task_id="task_get_region_oksm",
            python_callable=get_region_oksm,
            dag=dag,
            pool=get_pool(CH3_CONN),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.dict_office.region_oksm"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.region_oksm"),
            ],
        )

        task_get_human_resource_employee = PythonOperator(
            task_id="task_get_human_resource_employee",
            python_callable=get_human_resource_employee,
            dag=dag,
            pool=get_pool(CH3_CONN),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.dict.human_resource_employee"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.human_resource_employee"),
            ],
        )

        task_get_type_point = PythonOperator(
            task_id="task_get_type_point",
            python_callable=get_type_point,
            dag=dag,
            pool=get_pool(CH3_CONN),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.dict.type_point"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.type_point"),
            ],
        )

        task_update_branch_offices_mp = PythonOperator(
            task_id="task_update_branch_offices_mp",
            python_callable=update_branch_offices_mp,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.suppliers_warehouse_v3"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.kladr_tree"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.branch_offices_mp"),
            ],
        )

        task_vacuum_tables = PythonOperator(
            task_id="task_vacuum_tables",
            python_callable=vacuum_tables,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
        )

        task_update_offices = PythonOperator(
            task_id="task_update_offices",
            python_callable=update_offices,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.{BUF_TABLE_OFFICE_API}"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.geo_branch_office"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.logistics_branchoffice"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.region"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.region", key="manual"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.type_point"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.heritage_api_office_get_all_chiefs"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.heritage_api_office_get_all_chiefs", key="manual"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.heritage_api_region_get_structure"),
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.heritage_api_region_get_structure", key="manual"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.branch_offices"),
            ],
        )

        task_send_office_to_gp = PythonOperator(
            task_id="task_send_office_to_gp",
            python_callable=send_office_to_gp,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.branch_offices"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_BRANCH_OFFICE}"),
            ],
        )

        task_send_office_v_gp = PythonOperator(
            task_id="task_send_office_v_gp",
            python_callable=send_office_v_gp,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.branch_office"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_BRANCH_OFFICE_V}"),
            ],
        )

        task_send_kladr_tree_gp = PythonOperator(
            task_id="task_send_kladr_tree_gp",
            python_callable=send_kladr_tree_gp,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.kladr_tree"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_KLADR_TREE}"),
            ],
        )

        task_update_targets = PythonOperator(
            task_id="task_update_targets",
            python_callable=update_targets,
            dag=dag,
            pool=GP_CONN_ID,
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_BRANCH_OFFICE}", key="g1"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_BRANCH_OFFICE_V}", key="g2"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-gp.dwh.{BUF_TABLE_KLADR_TREE}", key="g3"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-gp.dwh.dict.branch_offices", key="g1"),
                OMEntity(entity=Entity.TABLE, fqn="do-gp.dwh.dict.branch_office", key="g2"),
                OMEntity(entity=Entity.TABLE, fqn="do-gp.dwh.dict.kladr_tree", key="g3"),
            ],
        )

        task_update_dict_office_branch_offices = PythonOperator(
            task_id="update_dict_office_branch_offices",
            python_callable=update_dict_office_branch_offices,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
            doc="""Таска выполняет перенос данных по офисам на ch3""",
            inlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-pg1.bo.dict.branch_office"),
            ],
            outlets=[
                OMEntity(entity=Entity.TABLE, fqn="do-ch3.default.dict_office.branch_office"),
            ],
        )

        (
            [task_get_region_oksm >> task_get_human_resource_employee >> task_get_type_point]
            >> task_update_branch_offices_mp
            >> task_vacuum_tables
            >> task_update_offices
            >> task_send_office_to_gp
            >> task_send_office_v_gp
            >> task_send_kladr_tree_gp
            >> task_update_targets
            >> task_update_dict_office_branch_offices
        )

    with TaskGroup(group_id="vacuum") as tg_vacuum:
        task_vacuum_branch_offices = PostgresOperator(
            task_id="task_vacuum_branch_offices",
            sql="vacuum analyze dict.branch_offices",
            postgres_conn_id=DST_CONNECTION_VACUUM,
            autocommit=True,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
        )

        task_vacuum_branch_office = PostgresOperator(
            task_id="task_vacuum_branch_office",
            sql="vacuum analyze dict.branch_office",
            postgres_conn_id=DST_CONNECTION_VACUUM,
            autocommit=True,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
        )

        task_vacuum_kladr_tree = PostgresOperator(
            task_id="task_vacuum_kladr_tree",
            sql="vacuum analyze dict.kladr_tree",
            postgres_conn_id=DST_CONNECTION_VACUUM,
            autocommit=True,
            dag=dag,
            pool=get_pool(PG_CONN_ID),
        )

        [task_vacuum_branch_offices, task_vacuum_branch_office, task_vacuum_kladr_tree]

    tg_creator_api >> tg_geo_api >> tg_update >> tg_vacuum
