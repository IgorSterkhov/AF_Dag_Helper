# do-metadata
OpenMetaData - Платформа управления метданными

## Lineage Builder
Автоматическое построение Lineage для сущностей на основе данных из Airflow DAGs
- DAG генератор, сканирующий Airflow и создающий Lineages: http://do-airflow-01.el.wb.ru/dags/openmeta_lineage/grid
- Для ручного запуска с 1 дагом передать в параметры: {'dag': 'pg_api_region_and_offices'}
- Пример описания inlet, outlet в task-e для сценария: из АПИ данне данные текут в буфер, затем из буффера + ручной таблицы собираются в целевую 
```
  inlets=[ 
             OMEntity(entity=Entity.API_SERVICE, fqn="do-office-creator-api", key="g1"), 
             OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.buffer.some_buffer_table", key="g2"),
             OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.buffer.some_manual_table", key="g2"),
             OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.buffer.some_manual_table", key="manual"), # метка ручной
         ],
  outlets=[ 
             OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.buffer.some_buffer_table", key="g1"),
             OMEntity(entity=Entity.TABLE, fqn=f"do-pg1.bo.dict.target_table", key="g2),
          ],      
```
- Здесь key = "some_goup_name" - группировка потоков, key = "manual" - метка того, что таблица заполняется вручную, для такой таблицы генерируется соответствующий узле в дереве родословной.
- entity - тип сущности к которой строится связь. Возможные значения [CONTAINER, TABLE, API_SERVICE, API_ENDPOINT, TOPIC, DASHBOARD, DASHBOARD_DATA_MODEL]. Их описание и примеры в коде класса Entity.
- fqn - полное имя сущности из OpenMetaData. При неккоретном указание связь построена не будет. Имена можно найти во фронте OM:
  - API_SERVICE: Настройки -> Сервисы -> API (Поле "Наименование" в таблице)
  - DASHBOARD: Настройки -> Сервисы -> Дашборды -> do-superset -> Вкладка "Дашборды" -> Столбец "Наименование". Если наименование 107, то fqn = do-superset.107
  - TOPIC: Настройки -> Сервисы -> Обмен сообщениями -> do-kafka-vm -> Вкладка "Топики" -> Столбец "Наименования". Если наименование carb_balance, то fqn = do-kafka-vm.card-balance
  - TABLE: Настройки -> Сервисы -> Базы данных -> Выбрать нужную базу -> Выбрать схему -> Выбрать таблицу. fqn = БД_сервис.БД.Схема.Таблица.
  - и т.д. Все примеры fqn по типам сущности лежат в коде класса Entity
## Пример
- Пример можно найти в даге [pg_api_region_and_offices ](http://do-airflow-01.el.wb.ru/dags/pg_api_region_and_offices/grid?search=pg_api_region_and_offices)
## Паспорт проекта
- Номер инструмента:
- Куратор:
- ПМ: @popova.valeriya21
- Ответственный разраб: @stepanov.dmitriy22
- Напарник разраб: @kamaltdinov.r6
- Планируемые потребители: Датаопс
- Результат в каком виде: web-сервис
- Алерты:
- Схема бекапа:
- Источники:
- Инструменты:
- [Внутренний банд]()
- [epic start]()
- [epic project]()
- [epic aid]()