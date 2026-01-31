import asyncio
import logging as log
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from clickhouse_driver.dbapi import OperationalError
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from pydantic import BaseModel

from hooks.clickhouse_cluster_hook import ClickhouseClusterHook

API_CONN_ID = "api-openmeta-bot"
THREADS = True

KAFKA_CONN_ID = {
    "kafka-openmeta-gold": "do-kafka-gold",
    "kafka-openmeta-vm": "do-kafka-vm",
}

IGNORE_TOPICS = ("card-balance",)  # Не поставляем образцы следующих топиков

DUMP_SERVERS = Variable.get("ch_schema_dump", deserialize_json=True)

# Маппинг коннектов БД (для экспорта стримов) к именам сервисов в OpenMeta
DATABASE_MAP = Variable.get("openmeta_db_map", deserialize_json=True)

# маппинг конфиогов clickhouse для Kafka движка к реальным именам кафки в топиках OpenMeta
ENGINES_MAP = {
    "do_kafka_vm": "do-kafka-vm",
    "do_kafka": "do-kafka-gold",
    "do_kafka_gold": "do-kafka-gold",
    "dataops_kafka_gold": "do-kafka-gold",
}

GET_MATVIEW_INSERTS = """
     with 7 as days_before_inactive,
          (
              /* готовлюсь подменить имена прокси-таблиц
                 на имена тех таблиц, в которые они пишут */
              select mapFromArrays(
                         groupArray(database || '.' || name),
                         groupArray(
                             arrayStringConcat(
                                 extractAllGroupsVertical(
                                     engine_full,
                                     '\\w+\\(.+?,\\s+\\W(.+?)\\W,\\s+\\W(.+?)\\W(?:,\\s.+?)?\\)$')[1],
                                 '.') AS source))
                from system.tables
               where engine = 'Distributed'
                 and length(source) > 0
          ) AS distsources

   select if(empty(a.from_table), f.from_table, a.from_table) AS from_table,
          a.to_table AS to_table,
          mv_name,
          topics AS topic,
          is_active,
          if(c.comment = '-', d.comment, c.comment) AS comment,
          looks_like_raw,
          kafka_engine

     from (  with extract(create_table_query, '\\sFROM\\s+(\\w+\\.\\w+)') AS _from,
                  extract(create_table_query, '^.+TO\\s+(\\w+\\.\\w+)') AS _to
           select if(empty(distsources[_from]), _from, distsources[_from]) AS from_table,
                  database || '.' || name AS mv_name,
                  if(empty(distsources[_to]), _to, distsources[_to]) AS to_table
             from system.tables
            where engine = 'MaterializedView') as a

left join (select database || '.' || table AS to_table,
                  max(modification_time) > today() - days_before_inactive AS is_active
             from system.parts
         group by to_table) as b
       on a.to_table = b.to_table

left join (select database || '.' || table AS to_table,
                  if(empty(comment), '-',
                     replaceRegexpAll(comment, '\\s+', ' ')) AS comment
             from system.tables) as c
       on a.to_table = c.to_table

left join (select database || '.' || table AS from_table,
                  if(empty(comment), '-',
                     '(комментарий к равке): ' || replaceRegexpAll(comment, '\\s+', ' ')) AS comment
             from system.tables) as d
       on a.from_table = d.from_table

left join (select database || '.' || table AS to_table,
                  (countIf(name='message') = 1
                       and hostName() not like 'do-raw-ch-01-%') AS looks_like_raw
                           /* в do-raw-ch-01 все таблицы целевые */
             from system.columns
         group by to_table) as e
       on a.to_table = e.to_table

full join (select database || '.' || table AS from_table,
                  extract(create_table_query, 'kafka_topic_list\\s*=\\s*''(.+?)''') AS topics,
                  extract(create_table_query, 'ENGINE\\s*=\\s*Kafka\\(([^)]+)\\)') AS kafka_engine
             from system.tables
            where notEmpty(topics)) as f
       on a.from_table = f.from_table
"""


class Service(BaseModel):
    id: str
    type: str
    fullyQualifiedName: str
    name: Optional[str] = None
    displayName: Optional[str] = None
    deleted: Optional[bool] = None
    href: Optional[str] = None
    description: Optional[str] = None


class MessageSchema(BaseModel):
    schemaText: Optional[str] = None
    schemaType: Optional[str] = None
    schemaFields: Optional[list[Any]] = None


class ChangeDescription(BaseModel):
    fieldsAdded: Optional[list[Any]] = None
    fieldsUpdated: Optional[list[Dict[str, Any]]] = None
    fieldsDeleted: Optional[list[Any]] = None
    previousVersion: Optional[float] = None


class TopicConfig(BaseModel):
    config: Optional[Dict[str, str]] = None


class Topic(BaseModel):
    id: str
    name: str
    service: Service
    serviceType: str
    fullyQualifiedName: Optional[str] = None
    version: Optional[float] = None
    updatedAt: Optional[int] = None
    updatedBy: Optional[str] = None
    messageSchema: Optional[MessageSchema] = None
    partitions: Optional[int] = None
    cleanupPolicies: Optional[list[str]] = None
    retentionTime: Optional[float] = None
    replicationFactor: Optional[int] = None
    maximumMessageSize: Optional[int] = None
    minimumInSyncReplicas: Optional[int] = None
    retentionSize: Optional[float] = None
    topicConfig: Optional[Dict[str, str]] = None
    href: Optional[str] = None
    changeDescription: Optional[ChangeDescription] = None
    deleted: Optional[bool] = None
    sourceHash: Optional[str] = None


class Column(BaseModel):
    name: Optional[str] = None
    dataType: Optional[str] = None
    dataLength: Optional[int] = None
    dataTypeDisplay: Optional[str] = None
    fullyQualifiedName: Optional[str] = None
    constraint: Optional[str] = None


class DatabaseSchema(BaseModel):
    id: Optional[str] = None
    type: Optional[str] = None
    name: Optional[str] = None
    fullyQualifiedName: Optional[str] = None
    displayName: Optional[str] = None
    deleted: Optional[bool] = None
    href: Optional[str] = None


class Database(BaseModel):
    id: Optional[str] = None
    type: Optional[str] = None
    serviceType: Optional[str] = None
    name: Optional[str] = None
    fullyQualifiedName: Optional[str] = None
    displayName: Optional[str] = None
    deleted: Optional[bool] = None
    default: Optional[bool] = None
    href: Optional[str] = None
    service: Optional[Service] = None


class LifeCycleAccessed(BaseModel):
    timestamp: Optional[int] = None
    accessedByAProcess: Optional[str] = None


class LifeCycle(BaseModel):
    accessed: Optional[LifeCycleAccessed] = None


class Table(BaseModel):
    id: str
    fullyQualifiedName: str
    name: str
    version: Optional[float] = None
    updatedAt: Optional[int] = None
    updatedBy: Optional[str] = None
    href: Optional[str] = None
    tableType: Optional[str] = None
    columns: Optional[list[Column]] = None
    databaseSchema: Optional[DatabaseSchema] = None
    database: Optional[Database] = None
    service: Optional[Service] = None
    serviceType: Optional[str] = None
    changeDescription: Optional[ChangeDescription] = None
    deleted: Optional[bool] = None
    lifeCycle: Optional[LifeCycle] = None
    sourceHash: Optional[str] = None


class EdgeSchema(BaseModel):
    id_from: str
    id_to: str
    type_from: str
    type_to: str


class OpenMetaApiHelper:
    def __init__(self, host: str, token: str):
        self.host = host
        self.token = token

    async def _get_openmeta_entity(self, entity: str, params: dict = None):
        url = f"{self.host}/v1/{entity}"
        params = params or {}
        params = {**params, "limit": 1000000}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, headers={"Authorization": f"Bearer {self.token}"}, params=params, timeout=60
                )
                response.raise_for_status()

                response_data = response.json()
                return response_data["data"]

            except httpx.HTTPError as e:
                log.error(f"Request for {url} failed with error: {e}")
                return []

    async def get_openmeta_topics(self, **params):
        return await self._get_openmeta_entity("topics", params)

    async def get_openmeta_tables(self, **params):
        return await self._get_openmeta_entity("tables", params)

    async def get_openmeta_databases(self, **params):
        return await self._get_openmeta_entity("databases", params)

    # Устанавливаем образец данных для конкретного топика
    async def put_openmeta_sample(self, id: str, data: list):
        url = f"{self.host}/v1/topics/{id}/sampleData"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    url,
                    headers={"Authorization": f"Bearer {self.token}"},
                    params={"limit": 1000},
                    json={"messages": data},
                )
                response.raise_for_status()

            except httpx.HTTPError as e:
                log.error(f"Request for {url} failed with error: {e}")

    # Устанавливаем цепочку данных
    async def put_openmeta_lineage(self, id_from: str, type_from: str, id_to: str, type_to: str):
        url = f"{self.host}/v1/lineage"
        data = {"edge": {"fromEntity": {"id": id_from, "type": type_from}, "toEntity": {"id": id_to, "type": type_to}}}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    url,
                    headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
                    json=data,
                )
                response.raise_for_status()
            except httpx.HTTPError as e:
                log.error(f"Request for {url} failed with error: {e}")


class KafkaHelper:
    def __init__(self, host, login, password):
        self.host = host
        self.login = login
        self.password = password

        self.config = {
            "bootstrap_servers": host,
            "sasl_plain_username": login,
            "sasl_plain_password": password,
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-512",
            "ssl_cafile": "/usr/local/share/ca-certificates/dataops-ca-chain.crt",
        }

    def consume_topic(self, topic_name, message_cnt, config, retry_cnt):
        consumer = KafkaConsumer(**config)
        try:
            partitions = consumer.partitions_for_topic(topic_name)
            topic_parts = [TopicPartition(topic_name, p) for p in partitions]
            consumer.assign(topic_parts)

            beginning_offsets = consumer.beginning_offsets(topic_parts)
            end_offsets = consumer.end_offsets(topic_parts)

            for tp in topic_parts:
                beginning = beginning_offsets[tp]
                end = end_offsets[tp]
                consumer.seek(tp, max(beginning, end - message_cnt))

            consumer.assign(topic_parts)
            messages = []
            retry = 0
            while len(messages) < message_cnt:
                try:
                    records = consumer.poll(timeout_ms=2000, max_records=1)
                    # records = next(consumer)
                    if not records:
                        retry += 1
                        if retry >= retry_cnt:
                            break
                        continue

                    for tps, msgs in records.items():
                        for msg in msgs:
                            try:
                                if topic_name not in IGNORE_TOPICS:
                                    messages.append(msg.value.decode("utf-8"))
                            except UnicodeDecodeError:
                                messages.append(msg.value.decode("utf-8", errors="ignore"))

                except KafkaError as e:
                    print(f"ERROR: {e}")
                    retry += 1
                    if retry >= retry_cnt:
                        break

            return messages
        finally:
            consumer.close()

    def consume_all_topics_concurrently(self, message_cnt=10, retry_cnt=4):
        """
        Читает все доступные топики из кафки в потоках.
        :param message_cnt: Сколько сообщений с конца (максимум) получить
        :param retry_cnt: сколько делать попыток повторного чтения
        :return:
        """
        conf = {"enable_auto_commit": False, "group_id": "test-consumer-group", "auto_offset_reset": "latest"}

        # Получаем метадату - список топиков
        admin_client = KafkaAdminClient(**self.config)
        topics = admin_client.list_topics()

        self.config.update(conf)
        results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.consume_topic, topic, message_cnt, self.config, retry_cnt): topic
                for topic in topics
            }
            for future in as_completed(futures):
                topic_name = futures[future]
                try:
                    results[topic_name] = future.result()
                except Exception as e:
                    print(f"Failed to consume {topic_name}: {e}")

        return results

    # Функция для чтения сообщений из Kafka
    def consume_all_topics(self, message_cnt: int = 10, retry_cnt=4):
        conf = {"enable_auto_commit": False, "group_id": "test-consumer-group", "auto_offset_reset": "latest"}

        admin_client = KafkaAdminClient(**self.config)
        topics = admin_client.list_topics()

        self.config.update(conf)
        consumer = KafkaConsumer(**self.config)
        res = {}
        decode_error = False
        print(f"{'TOPIC':<40}, {'St':<2}, {'Ln':<2}, {'OFFSET_LOW':<10}, {'OFFSET_HI':<10}, {'DECOD':<5}")
        for current_topic in topics:
            # log.info(f"Start reading topic: {current_topic}...")
            partitions = consumer.partitions_for_topic(current_topic)
            topic_parts = [TopicPartition(current_topic, p) for p in partitions]
            consumer.assign(topic_parts)

            beginning_offsets = consumer.beginning_offsets(topic_parts)
            end_offsets = consumer.end_offsets(topic_parts)
            llow, hhigh = -1, -1
            for tp in topic_parts:
                beginning = beginning_offsets[tp]
                end = end_offsets[tp]
                llow = max(llow, beginning)
                hhigh = max(hhigh, end)
                try:
                    consumer.seek(tp, max(beginning, end - message_cnt))
                except Exception as e:
                    print(f"Ошибка при seek на {tp}: {e}")
                    continue

            messages = []
            retry = 0
            while len(messages) < message_cnt:
                try:
                    records = consumer.poll(timeout_ms=2000, max_records=1)
                    if not records:
                        retry += 1
                        if retry >= retry_cnt:
                            break
                        continue

                    for tps, msgs in records.items():
                        for msg in msgs:
                            try:
                                if current_topic not in IGNORE_TOPICS:
                                    messages.append(msg.value.decode("utf-8"))
                            except UnicodeDecodeError:
                                decode_error = True
                                messages.append(msg.value.decode("utf-8", errors="ignore"))

                except KafkaError as e:
                    print(f"ERROR: {e}")
                    retry += 1
                    if retry >= retry_cnt:
                        break

            res[current_topic] = messages
            log.info(
                f"{current_topic:<40}, {'OK' if len(messages) else 'ER':<2}, {len(messages):<2}, {llow:<10}, {hhigh:<10}, {decode_error:<5}"
            )

        return res


def build_lineage_edges(clh_id, topics: dict, tables: dict, db_name: str) -> list[EdgeSchema]:
    """
    Извлекаем DDL матвьюх из system.tables, оттуда таблицы "from", "to", из DDL таблицы "from" дергаем топики и движок (что за кафка).
    результат получаем в namedTuple со схемой нижке
    :param clh_id: ид клика из переменных Airflow (смотри ключи DATABASE_MAP)
    :param topics: словарь топиков из openmeta
    :param tables: словарь таблиц из openmeta
    :param db_name: имя сервиса (базы данных) и схема (все default). Смотри значения DATABASE_MAP
    :return: Возвращаем пары источник-приемник для контракта создания узла
    """
    log.info(f"Build lineage edges for {clh_id}: {db_name} STARTING...")
    nt = namedtuple(
        "row", ["from_table", "to_table", "mv_name", "topic", "is_active", "comment", "looks_like_raw", "kafka_engine"]
    )
    res: list[EdgeSchema] = []

    con_name = clh_id + "-export"
    try:
        with ClickhouseClusterHook(con_name).get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(GET_MATVIEW_INSERTS)
            matviews = [nt(*x) for x in cursor.fetchall()]
            matviews = list(filter(lambda i: i.topic and i.kafka_engine in ENGINES_MAP, matviews))
            log.info(f"Extracted matviews with topics and engines: {len(matviews)}")

            for mv in matviews:
                open_service = ENGINES_MAP[mv.kafka_engine]
                full_table_name = f"{db_name}.{mv.from_table}"
                id_table = tables.get(full_table_name, {}).get("id")

                if not id_table:
                    log.warning(f"Matview {mv.mv_name}: Cant't find table in OpenMeta: {full_table_name}")
                    continue

                for t in mv.topic.split(","):
                    id_topic = topics.get(open_service, {}).get(t, {}).get("id")

                    if not id_topic:
                        log.warning(f"Matview {mv.mv_name}: Cant't find topic in OpenMeta: {t}")
                        continue

                    print(f"{open_service}.{t}, {full_table_name}")

                    if id_table and id_topic:
                        res.append(EdgeSchema(id_from=id_topic, id_to=id_table, type_from="topic", type_to="table"))

    except OperationalError as e:
        log.error(f"Operational error for connection {con_name}: {e}")
    except AirflowNotFoundException as e:
        log.error(f"Not found error  for connection {con_name}: {e}")

    return res


async def get_om_topics(openmeta: OpenMetaApiHelper) -> dict:
    # Получаем словарь топиков из метадаты разделенных по сервисам кафки
    data = await openmeta.get_openmeta_topics()
    topics: list[Topic] = [Topic(**i) for i in data]
    topics = list(filter(lambda x: x.serviceType == "Kafka", topics))  # Фильтруем только кафка сервисы

    open_topics = defaultdict(dict)
    for t in topics:
        open_topics[t.service.fullyQualifiedName][t.name] = {"id": t.id, "topic": t}

    return open_topics


async def get_om_tables(openmeta: OpenMetaApiHelper, db_name) -> dict:
    # Получаем словарь таблиц
    data = await openmeta.get_openmeta_tables(database=db_name)
    tables: list[Table] = [Table(**i) for i in data]

    open_tables = defaultdict(dict)
    for t in tables:
        open_tables[t.fullyQualifiedName] = {"id": t.id, "table": t}

    return open_tables


async def feed_openmeta_lineage():
    """
    Достает топики и таблицы из Метадаты асинхронно, строит lineage из пары источник-приемник
    :return:
    """
    log.info("Feed openmeta lineage. STARTING...")

    api_hook = BaseHook()
    api_conn = api_hook.get_connection(API_CONN_ID)
    openmeta = OpenMetaApiHelper(api_conn.host, api_conn.password)

    servers = [i for i in DUMP_SERVERS if i in DATABASE_MAP.keys()]
    not_map_servers = [i for i in DUMP_SERVERS if i not in DATABASE_MAP.keys()]

    open_topics_task = get_om_topics(openmeta)
    open_tables_task = [get_om_tables(openmeta, DATABASE_MAP[ch_id]) for ch_id in servers]

    open_topics, *open_tables = await asyncio.gather(open_topics_task, *open_tables_task)

    edges: list[EdgeSchema] = []
    for ch_id, tables in zip(servers, open_tables):
        edge = build_lineage_edges(ch_id, open_topics, tables, DATABASE_MAP[ch_id])
        edges.extend(edge)

    await asyncio.gather(*(openmeta.put_openmeta_lineage(**e.dict()) for e in edges))

    if not_map_servers:
        raise AirflowException(f'All DONE, but some connections were not found in mapping: {",".join(not_map_servers)}')
    else:
        log.info("Feed openmeta lineage. All DONE!")


async def feed_openmeta_from_kafka(threads=THREADS):
    """
    Достаем топики из Метадаты, достаем семплы из кафки, наполяем Метадату ими
    :param threads: запускать ли в многопоточке или синхронно
    :return:
    """
    log.info("Feed openmeta from kafka. STARTING...")

    api_hook = BaseHook()
    api_conn = api_hook.get_connection(API_CONN_ID)
    openmeta = OpenMetaApiHelper(api_conn.host, api_conn.password)

    open_topics = await get_om_topics(openmeta)  # топики OpenMeta

    for kafka_conn_id, service in KAFKA_CONN_ID.items():
        log.info(f"Feed service: {service}")
        kafka_hook = BaseHook()
        kafka_conn = kafka_hook.get_connection(kafka_conn_id)
        kafka = KafkaHelper(kafka_conn.host, kafka_conn.login, kafka_conn.password)

        if threads:
            kafka_topics = kafka.consume_all_topics_concurrently(message_cnt=10, retry_cnt=4)  # топики Kafka
        else:
            kafka_topics = kafka.consume_all_topics(message_cnt=10, retry_cnt=1)

        # Отправляем в OpenMeta
        for topic, data in kafka_topics.items():
            if topic in IGNORE_TOPICS:
                log.info(f"Topic: {topic}: IGNORING")
                # id = open_topics[service][topic]["id"]
                # await openmeta.put_openmeta_sample(id, []) # TODO вернуть если нужно зачистить игнор топики
            elif not data:
                log.info(f"Topic: {topic}: NO DATA")
            elif topic in open_topics[service].keys():
                log.info(f"Topic: {topic}: PROCESSING...")
                id = open_topics[service][topic]["id"]
                await openmeta.put_openmeta_sample(id, data)
            else:
                log.info(f"Topic: {topic}: NO TOPIC IN OpenMeta")

    log.info("Feed openmeta from kafka. All DONE!")


def feed_openmeta_from_kafka_wrap():
    asyncio.run(feed_openmeta_from_kafka())


def feed_openmeta_lineage_wrap():
    asyncio.run(feed_openmeta_lineage())


with DAG(
    default_args={
        "owner": "kamaltdinov.r6",
        "email": ["kamaltdinov.r6@wildberries.work"],
        "email_on_failure": False,
        "telegram": ["@NPV42"],
        "band": [
            "kamaltdinov.r6",
        ],
    },
    dag_id="openmeta_feeder",
    schedule=timedelta(hours=24),
    start_date=datetime(2023, 1, 31),
    tags=["@NPV42", "feeder", API_CONN_ID],
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    description="Вычитывает образцы топиков кафки и поставляет в OpenMataData как сэмплыю Строит lineage по DDL матвьюх",
) as dag:
    openmeta_kafka_sample_task = PythonOperator(
        task_id="openmeta_kafka_sample_task",
        python_callable=feed_openmeta_from_kafka_wrap,
        execution_timeout=timedelta(hours=24),
        pool="do-services",
    )

    openmeta_lineage_task = PythonOperator(
        task_id="openmeta_lineage_task",
        python_callable=feed_openmeta_lineage_wrap,
        execution_timeout=timedelta(hours=24),
        pool="default_pool",
    )

    [openmeta_kafka_sample_task, openmeta_lineage_task]
