import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

from utils.db.kafka.clickhouse_to_kafka import ClickHouseToKafka
from utils.decorators_with_conn import with_db, with_cutoff
from utils.data_exchange import get_producer_params_kafka

from extra.get_pool import get_pool
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
from utils.openmeta_helper import Entity


CH_09_CONN = "do-ch9"
KAFKA_CONN_ID = "kafka-oof-positions-test"

DEFAULTS_DECORATOR_ARGS = {
    "table_dt_column_name": "row_created",
    "conn_str": CH_09_CONN,
    "max_batch_records": 50_000_000,
    "back_seek_seconds": 0,
}


def push_to_kafka(ch_hook, topic, min_cutoff, max_cutoff):
    params_kafka = get_producer_params_kafka(
        KAFKA_CONN_ID,
        client_id=os.uname()[1],
        conn_for_hosts=KAFKA_CONN_ID,
    )

    kafka_producer = KafkaProducer(**params_kafka)

    sql = """
    select 
        message 
    from positions.oof_addon
    where True
        and row_created >= toDateTime('{min_cutoff}')
        and row_created < toDateTime('{max_cutoff}')
        and topic = '{topic}'
    ;
    """

    c = ClickHouseToKafka(ch_hook, kafka_producer)
    c.run(
        sql.format(topic=topic, min_cutoff=min_cutoff, max_cutoff=max_cutoff),
        topic,
        key_field="srid",
    )


@with_db(CH_09_CONN, "ch9")
@with_cutoff(
    dict(
        cutoff_name="kafka.oof-positions-order-test",
        table_name="positions.oof_addon",
        **DEFAULTS_DECORATOR_ARGS,
    )
)
def push_oof_order(ch9_hook, min_cutoff, max_cutoff):
    push_to_kafka(ch9_hook, "oof-positions-order-test", min_cutoff, max_cutoff)


@with_db(CH_09_CONN, "ch9")
@with_cutoff(
    dict(
        cutoff_name="kafka.oof-positions-final-v2-test",
        table_name="positions.oof_addon",
        **DEFAULTS_DECORATOR_ARGS,
    )
)
def push_oof_final(ch9_hook, min_cutoff, max_cutoff):
    push_to_kafka(ch9_hook, "oof-positions-final-v2-test", min_cutoff, max_cutoff)


@with_db(CH_09_CONN, "ch9")
@with_cutoff(
    dict(
        cutoff_name="kafka.oof-positions-other-v2-test",
        table_name="positions.oof_addon",
        **DEFAULTS_DECORATOR_ARGS,
    )
)
def push_oof_other(ch9_hook, min_cutoff, max_cutoff):
    push_to_kafka(ch9_hook, "oof-positions-other-v2-test", min_cutoff, max_cutoff)


def_args = {
    "owner": "sokolov.k2",
    "email": [],
    "email_on_failure": False,
    "catchup": False,
    "email_on_retry": False,
    "max_active_tasks": 3,
    "telegram": [
        "@ralkofinn",
    ],
}

with DAG(
    default_args=def_args,
    dag_id="kafka_ch9_to_oof_v2_test",
    description="Пушер из кликхауза в кафку",
    start_date=datetime(2025, 12, 22),
    schedule=None,
    catchup=False,
    tags=["@sokolov.k2", "kafka", CH_09_CONN, KAFKA_CONN_ID],
    max_active_runs=1,
) as dag:
    push_oof_order = PythonOperator(
        task_id="push_oof_order",
        dag=dag,
        python_callable=push_oof_order,
        retries=1,
        pool=get_pool(CH_09_CONN),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch9.datamart.oof_addon"),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch9.raw.oof_positions_order_test_raw"
            ),
        ],
    )

    push_oof_final = PythonOperator(
        task_id="push_oof_final",
        dag=dag,
        python_callable=push_oof_final,
        retries=1,
        pool=get_pool(CH_09_CONN),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch9.datamart.oof_addon"),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch9.raw.oof_positions_final_v2_test_raw"
            ),
        ],
    )

    push_oof_other = PythonOperator(
        task_id="push_oof_other",
        dag=dag,
        python_callable=push_oof_other,
        retries=1,
        pool=get_pool(CH_09_CONN),
        inlets=[
            OMEntity(entity=Entity.TABLE, fqn="do-ch9.datamart.oof_addon"),
        ],
        outlets=[
            OMEntity(
                entity=Entity.TABLE, fqn="do-ch9.raw.oof_positions_other_v2_test_raw"
            ),
        ],
    )
