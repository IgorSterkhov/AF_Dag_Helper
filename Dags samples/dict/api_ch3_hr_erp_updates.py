import logging as log
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.http.hooks.http import HttpHook
from utils.decorators_with_conn import with_db, load_and_save_cutoff
from utils.requests.auth import BearerAuth
from utils.db.clickhouse import get_ch_dwh_max_v2
from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH3_CONN_ID = "do-ch3"
TELEGA = '@artemy_kravtsov'
DESCRIPTION = "Обновление информации в источнике справочника по сотрудникам"

# dict_personal.human_resource_department
API_WB_DEPARTMENTS_CONN = "api-hr-employee-departments"
API_WB_DEPARTMENTS_DST_TABLE = 'dict_personal.human_resource_department'
API_WB_DEPARTMENTS_FIELDS_MAP = {
    'id': 'department_id', 
    'parentID': 'parent_department_id', 
    'name': 'department_name'}
API_WB_DEPARTMENTS_DEFAULT_VALUES = {
    'id': 0, 
    'parentID': 0, 
    'name': ''}

# dict_personal.wb_users_links
API_WB_USERS_CONN = "api-hr-wbusers"
API_WB_USERS_DST_TABLE = 'dict_personal.wb_users_links'
API_WB_USERS_FIELDS_MAP = {
    'employeeID': 'employee_id', 
    'isActiveEmployee': 'is_active_employee', 
    'isWork': 'is_work',
    'wbuserID': 'wbuser_id'}
API_WB_USERS_DEFAULT_VALUES = {
    'employeeID': 0, 
    'isActiveEmployee': False, 
    'isWork': False,
    'wbuserID': 0}


FILL_DICT_EMPLOYEES = """
  INSERT INTO dict_personal.employees
         (
          employee_id, wbuser_id, row_created,  
          /* ниже колонки в том же порядке, что в селекте */
          
          employee_name, last_name, first_name, middle_name, department_id, 
          cfo_id, country_id, create_dt, dt, birth_date, work_date, 
          creator_employee_id, ch_employee_id, vacation_days, passport, 
          snils, inn, is_deleted, is_quit_notice, ts
         )
         
  SELECT employee_id,
         links.wbuser_id,
         now() AS row_created,
         untuple(argmax_datapack)
    FROM (
            SELECT employee_id,
                   argMax(
                       tuple(
                          employee_name, last_name, first_name, middle_name, department_id, 
                          cfo_id, country_id, create_dt, dt, birth_date, work_date, 
                          creator_employee_id, ch_employee_id, vacation_days, passport, 
                          snils, inn, is_deleted, is_quit_notice, ts
                       ), ts) AS argmax_datapack
              FROM personal.erp_employee_update
             WHERE row_created BETWEEN %(min_cutoff)s AND %(max_cutoff)s
          GROUP BY employee_id
         ) AS upd
LEFT ANY 
    JOIN (
           SELECT employee_id, wbuser_id
             FROM dict_personal.wb_users_links FINAL
            WHERE employee_id IN (
                                  SELECT employee_id
                                    FROM personal.erp_employee_update
                                   WHERE row_created BETWEEN %(min_cutoff)s AND %(max_cutoff)s
                                 )
         ) AS links
   USING (employee_id)
"""


@with_db(CH3_CONN_ID)
def get_hr_api_data(hook, api_conn, default_values, fields_map, dst_table, payload):

    # получаю данные из API
    api_hook = HttpHook(
        http_conn_id=api_conn, 
        auth_type=BearerAuth, 
        method='GET')
    response = api_hook.run(data=payload)
    response.raise_for_status()
    content = response.json().get('data')
    if content is None:
        log.info('No data found')
        return
    
    # переименовываю ключи в словаре, чтобы совпадали с именами колонок в клике
    for idx, item in enumerate(content):
        content[idx] = {tgt_name: item.get(api_name) or default_values.get(api_name) 
                        for api_name, tgt_name in fields_map.items()}

    # загружаю данные в клик
    hook.bulk_dump(
        table=dst_table, 
        columns=fields_map.values(), 
        data=content)


@with_db(CH3_CONN_ID)
@load_and_save_cutoff(table_name='dict_personal.employees', field='max_dt')
def update_dict_employees(hook, cutoff):
    
    # INSERT INTO public.max_val_cutoff (table_name, max_dt, created) 
    # VALUES ('dict_personal.employees', toDateTime(0), now());
    
    # забираю отсечку
    (min_cutoff, max_cutoff, _, rec_cnt) = get_ch_dwh_max_v2(
        ch_hook=hook, 
        changes_table_name='personal.erp_employee_update', 
        date_field='row_created', 
        max_dt=cutoff,
        max_batch_records=100_000_000,
        back_seek_seconds=30)
    if rec_cnt is None:
        raise AirflowSkipException('got 0 lines, nothing to save')
    
    # вставляю обогащённые данные в dict_personal.employees
    hook.exec_with_log(
        FILL_DICT_EMPLOYEES, 
        parameters=dict(
            min_cutoff=min_cutoff, 
            max_cutoff=max_cutoff))
    
    return max_cutoff


with DAG(
    dag_id='api_ch3_hr_erp_updates',
    description=DESCRIPTION,
    tags=[CH3_CONN_ID, TELEGA, 'hr','dict',"@grisha_borisov"],
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 6, 15),
    max_active_runs=1,
    default_args=dict(
        owner='kravcov.artemiy',
        telegram=[TELEGA],
        email_on_failure=True,
        retries=4,
        retry_delay=timedelta(minutes=5))
    ) as dag:
    
    task_get_department_data = PythonOperator(
        task_id='task_get_department_data',
        python_callable=get_hr_api_data,
        pool=get_pool(CH3_CONN_ID),
        op_kwargs=dict(
            api_conn=API_WB_DEPARTMENTS_CONN, 
            default_values=API_WB_DEPARTMENTS_DEFAULT_VALUES, 
            fields_map=API_WB_DEPARTMENTS_FIELDS_MAP, 
            dst_table=API_WB_DEPARTMENTS_DST_TABLE,
            payload=None),
        inlets=[OMEntity(entity=Entity.API, fqn="api-hr-employee-departments")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_personal.human_resource_department")]

    )

    task_get_wbuser_connections = PythonOperator(
        task_id='task_get_wbuser_connections',
        python_callable=get_hr_api_data,
        pool=get_pool(CH3_CONN_ID),
        op_kwargs=dict(
            api_conn=API_WB_USERS_CONN, 
            default_values=API_WB_USERS_DEFAULT_VALUES, 
            fields_map=API_WB_USERS_FIELDS_MAP, 
            dst_table=API_WB_USERS_DST_TABLE,
            payload={'include-fired': "false"}),
        inlets=[OMEntity(entity=Entity.API, fqn="api-hr-wbusers")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_personal.wb_users_links")]
    )

    task_update_dict_employees = PythonOperator(
        task_id='update_dict_employees',
        python_callable=update_dict_employees,
        pool=get_pool(CH3_CONN_ID),
        inlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.personal.erp_employee_update"),
                OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_personal.wb_users_links")],
        outlets=[OMEntity(entity=Entity.TABLE, fqn=f"do-ch3.dict_personal.employees")]
    )

    [task_get_department_data, task_get_wbuser_connections] >> task_update_dict_employees
