import logging as log
import time
from functools import wraps
from typing import List, Type, TypeVar, Union

from airflow import DAG
from airflow_provider_openmetadata.hooks.openmetadata import OpenMetadataHook
from airflow_provider_openmetadata.lineage.backend import OpenMetadataLineageBackend
from airflow_provider_openmetadata.lineage.config.loader import get_lineage_config
from airflow_provider_openmetadata.lineage.runner import AirflowLineageRunner
from metadata.generated.schema.api.data.createAPIEndpoint import CreateAPIEndpointRequest
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.entity.services.apiService import ApiService
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.type.entityLineage import Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity, XLets, get_xlets_from_dag
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def retry_on_error(max_attempts=3, delay=0):
    """
    Декоратор для ретрая
    max_attempts (int): сколько попыток
    delay (float): пауза между ними
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            err = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:  # Not the last attempt
                        log.warning(f"Attempt {attempt + 1} of {max_attempts} failed: {e}")
                        err = e
                        if delay > 0:
                            time.sleep(delay)
                    else:
                        log.error(f"All {max_attempts} attempts failed")

            raise Exception(f"Function {func.__name__} failed after {max_attempts} attempts: {err}") from last_exception

        return wrapper

    return decorator


class Entity:
    # constants.py
    DB = DatabaseService
    TABLE = Table
    API = ApiService
    CONTAINER = Container
    TOPIC = Topic
    # USER = User
    # SERVICE = StorageService


class OpenMetaHelper:
    MANUAL_CONTAINER = "MANUL"

    def __init__(self, conn_id: str):
        self.omh = OpenMetadataHook(openmetadata_conn_id=conn_id)
        self.om = OpenMetadata(self.omh.get_conn())
        self.config = get_lineage_config()

    def create_container_for_service(self, name: str, service_name: str):
        from metadata.generated.schema.api.data.createContainer import CreateContainerRequest

        create_cont = CreateContainerRequest(name=name, service=service_name)
        create_db_entity = self.om.create_or_update(data=create_cont)

        return create_db_entity

    def create_api_endpoint(self, name: str):
        """Создать endpoint API"""
        req = CreateAPIEndpointRequest(
            name=name,
        )
        raise NotImplementedError

    def create_api_service(self, name: str, url: str = ""):
        """Создать API"""
        # https://github.com/open-metadata/OpenMetadata/blob/2fc4938484727332698a4637137fce2dc770875b/ingestion/examples/sample_data/ometa_api_service/service.json
        data = {
            "type": "rest",
            "serviceName": name,
            "serviceConnection": {"config": {"type": "Rest", "openAPISchemaURL": url, "token": "token"}},
            "sourceConfig": {"config": {}},
        }
        from metadata.generated.schema.metadataIngestion.workflow import (
            Source as WorkflowSource,
        )

        workflow_source = WorkflowSource(**data)
        obj = self.om.get_service_or_create(entity=Entity.API, config=workflow_source)

        log.info(f"Created: {obj.model_dump_json()}")
        return obj

    def get_dag_xlets(self, dag):
        xlet_list: List[XLets] = get_xlets_from_dag(dag)  # достает OMEntitie из старых дагов без преобразованимя имен
        return xlet_list

    def list_entities(self, type: Union[Type[T], str], fqn: str):
        """
        Список подсущностей по типу и имени родителя
        :param type: Entity.DB
        :param fqn: "do-pg1"
        :return: вернет ВСе что отностися к БД постгри
        """
        ent = self.om.list_entities(type, params={"fullyQualifiedName": fqn})
        return ent

    def entitie_by_name(self, type: Union[Type[T], str], fqn: str):
        """Возвращает сузность по типу и имени"""
        ent = self.om.get_by_name(type, fqn=fqn)
        return ent

    def entitie_lineage_by_name(self, type: Union[Type[T], str], fqn: str):
        """Достает родословную для сущности"""
        ent = self.om.get_lineage_by_name(type, fqn)
        return ent

    @retry_on_error(max_attempts=5, delay=5)
    def delete_lineage_entitie(self, type: Union[Type[T], str], fqn: str, clear_view: bool = False):
        """
        Зачищает связи для сузности
        :param type: тип исходной
        :param fqn: имя исходной
        :param clear_view: чистить ли связи на основе ViewLineage
        :return:
        """
        lineage = self.om.get_lineage_by_name(
            entity=type, fqn=fqn
        )  # lineage['upstreamEdges'] lineage['downstreamEdges'] - list

        if not lineage:
            log.warning(f"Can't delete entitie {fqn} not found! skip")
            return False

        # Удаляем ручнве связи, например наш Airflow
        self.om.delete_lineage_by_source(
            entity_type=lineage["entity"]["type"], entity_id=lineage["entity"]["id"], source=Source.Manual.value
        )

        # Удаляем связи на основе вьюх
        if clear_view:
            self.om.delete_lineage_by_source(
                entity_type=lineage["entity"]["type"],
                entity_id=lineage["entity"]["id"],
                source=Source.ViewLineage.value,
            )
        return True

    def add_service(self, dag):
        raise NotImplementedError

    def delete_lineage_dag(self, dag, clear_inlets=False):
        """
        Удаляет род со всех xlet-ов дага
        :param dag: наш даг
        :param clear_inlets: будем ли удалять так же инлеты (по дефолту только аутлеты)
        """
        # runner = AirflowLineageRunner(metadata=self.om, service_name=self.config.airflow_service_name, dag=dag)
        # pipeline_service = runner.get_or_create_pipeline_service()
        # pipeline = runner.create_or_update_pipeline_entity(pipeline_service)

        log.info("Deleting lineage for dag...")

        for task in dag.tasks:
            inlet_list, outlet_list = self._get_xlet_from_task(task)

            log.info(f"Deleting lineage from task: {task.task_id}...")

            del_list = outlet_list or []
            if clear_inlets:
                del_list += inlet_list

            done = set()  # список уже удаленных
            for ent in del_list:
                if ent.fqn not in done:
                    log.info(f"Deleting entitie lineage: {ent.fqn}...")

                    if self.delete_lineage_entitie(ent.entity, ent.fqn):
                        done.add(ent.fqn)

    def delete_entitie(self, type: Union[Type[T], str], fqn: str) -> None:
        service_id = str(self.om.get_by_name(entity=type, fqn=fqn).id.root)

        self.om.delete(
            entity=type,
            entity_id=service_id,
            recursive=True,
            hard_delete=True,
        )

    def _get_xlet_from_task(self, task):
        """
        Сканирует таску, определяет какой тип xlet-ов в ней и приводит к новому формату
        :param task:
        :return:
        """
        inlet_list: List[OMEntity] = []
        outlet_list: List[OMEntity] = []

        if task.inlets and isinstance(task.inlets[0], dict):  # Старая модель с datasets
            for i in task.inlets:
                for j in i["datasets"]:
                    inlet_list.append(OMEntity(entity=Entity.TABLE, fqn=j))
        else:
            inlet_list = task.inlets.copy()

        if task.outlets and isinstance(task.outlets[0], dict):
            for i in task.outlets:
                for j in i["datasets"]:
                    outlet_list.append(OMEntity(entity=Entity.TABLE, fqn=j))
        else:
            outlet_list = task.outlets.copy()

        return inlet_list, outlet_list

    @retry_on_error(max_attempts=5, delay=5)
    def add_lineage_dag(self, dag: DAG):
        """Добавляет родословные для всех тасок дага"""
        log.info(f"Adding lineage for DAG: {dag}...")

        runner = AirflowLineageRunner(metadata=self.om, service_name=self.config.airflow_service_name, dag=dag)

        pipeline_service = runner.get_or_create_pipeline_service()
        pipeline = runner.create_or_update_pipeline_entity(pipeline_service)

        _backend = OpenMetadataLineageBackend()

        for task in dag.tasks:
            inlet_list, outlet_list = self._get_xlet_from_task(task)

            if not inlet_list and not outlet_list:
                log.warning(f"Task XLETS from task {task.task_id} is empty, skip")
                continue

            log.info(f"Adding lineage from task: {task.task_id}...")

            # Проверяем на наличие признака ручной таблицы (только инлеты)
            for inlet in inlet_list:
                if inlet.key == "manual":
                    manual = self.create_container_for_service(f"Ручной ввод: {inlet.fqn}", self.MANUAL_CONTAINER)

                    xl = XLets(
                        inlets=[
                            OMEntity(entity=Entity.CONTAINER, fqn=manual.fullyQualifiedName),
                        ],
                        outlets=[inlet],
                    )

                    if pipeline:
                        runner.add_lineage(pipeline, xl)
                    else:
                        log.error("Fail to create or update pipeline. Skip")

            groups = set()
            for xlet in inlet_list + outlet_list:
                if xlet.key != "manual":
                    groups.add(xlet.key)

            for gr in groups:
                xl = XLets(
                    inlets=[x for x in inlet_list if x.key == gr], outlets=[x for x in outlet_list if x.key == gr]
                )

                if pipeline:
                    runner.add_lineage(pipeline, xl)
                else:
                    log.error("Fail to create or update pipeline. Skip")
