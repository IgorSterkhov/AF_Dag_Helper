"""
Мониторинг перепродаж возвратных КБ и ОВХ товаров.
Терминология:
- КБ - кросбордер - первый заказ товара с заграничного склада
- ОВХ - первый заказ со склада ВБ в РФ
- ФБО - fulfitment by operator (продажа со склада вб)
В идеале, КБ товары после возврата должны возвращаться поставщику, тем неменее иногда они перепродаются.
Также, для сравнительной аналитики в витрине хранятся ОВХ возвраты.
Мониторим только возвраты иностранных поставщиков:
    'cn' - китай
    'ae' - арабские эмираты
    'mo' - макао
    'hk' - гонконг
"""
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

from utils.decorators_with_conn import with_db, load_and_save_cutoff
from utils.db.clickhouse import get_ch_dwh_max_v2
from utils.data_exchange import copy_ch_to_ch_pipe
from utils.openmeta_helper import Entity


CH4_CONN_ID = "do-ch4"
CH7_CONN_ID = "do-ch-fines-01"
CH_LAKE_CONN_ID = "do-lake-m"


@with_db(CH4_CONN_ID, "ch4")
@with_db(CH7_CONN_ID, "ch7")
@load_and_save_cutoff(
    table_name="datamart.foreign_returns_pos",
    conn_str=CH7_CONN_ID,
    field="max_dt",
)
def ch4_positions_load_returns(ch4_hook, ch7_hook, cutoff) -> None:
    """Собирает отказы заграничных товаров из позишенов"""
    # читаем данные из positions.oof_position_status_v3_rc батчами
    min_t, max_t, _, _ = get_ch_dwh_max_v2(
        ch_hook=ch4_hook,
        changes_table_name='''
        view(
            select
                *
            from positions.oof_position_status_v3_rc
            where (
                -- возвратные сриды
                (startsWith(srid, 'mp.') = 1)
                -- только клиентские продажи и возвраты
                OR (
                    status_oof IN (8, 48, 120, 190, 124, 16, 103, 106)
                    AND NOT startsWith(coalesce(payment_type, ''), 'S')
                )
            )
        )''',
        date_field="row_created",
        max_dt=cutoff,
        max_batch_records=500_000_000,
        back_seek_seconds=10,
    )
    ch4_hook.exec_with_log('''
    CREATE TEMPORARY TABLE pos ENGINE=MergeTree() ORDER BY tuple() AS
    SELECT
        shk_id,
        srid,
        nm_id,
        dt,
        status_oof,
        create_ts + interval 3 hour AS created_dt,
        src_office_id,
        payment_type,
        row_created AS source_row_created
    FROM positions.oof_position_status_v3_rc
    WHERE shk_id IS NOT NULL
    AND row_created >= toDateTime(%(min_t)s)
    AND row_created <= toDateTime(%(max_t)s)
    AND (
        -- возвратные сриды
        (startsWith(srid, 'mp.') = 1)
        -- только клиентские продажи и возвраты
        OR (
            status_oof IN (8, 48, 120, 190, 124, 16, 103, 106)
            AND NOT startsWith(coalesce(payment_type, ''), 'S')
        )
    );

    TRUNCATE buffer.foreign_returns_pos;

    INSERT INTO buffer.foreign_returns_pos(
        shk_id,
        srid,
        return_dt,
        return_status,
        sale_dt,
        nm_id,
        created_dt,
        src_office_id,
        payment_type,
        seller_id,
        seller_country,
        source_row_created
    )
    SELECT
        shk_id,
        srid,
        minIf(
            toNullable(dt),
            status_oof IN (8, 48, 120, 190, 124)
        ) AS return_dt,
        argMinIf(
            toNullable(status_oof),
            dt,
            status_oof IN (8, 48, 120, 190, 124)
        ) AS return_status,
        minIf(toNullable(dt), status_oof IN (16, 103, 106)) AS sale_dt,
        any(pos.nm_id) AS nm_id,
        min(created_dt) AS created_dt,
        argMin(src_office_id, dt) as src_office_id,
        argMax(payment_type, dt) AS payment_type,
        any(nm_props.seller_id) AS seller_id,
        any(nm_props.seller_country) AS seller_country,
        max(pos.source_row_created) AS source_row_created
    FROM pos
    JOIN (
        -- товары только из списка стран
        SELECT
            nm_id,
            supplier_id_shk AS seller_id,
            dictGet('dict.sellers_portal', 'country_code', supplier_id_shk) seller_country
        FROM remote_ch3.product_cards_nm FINAL
        WHERE nm_id GLOBAL IN (SELECT nm_id FROM pos)
        AND seller_country IN ('cn', 'ae', 'mo', 'hk')
    ) nm_props
    ON pos.nm_id = nm_props.nm_id
    GROUP BY shk_id, srid;
    ''',
        parameters={"min_t": min_t, "max_t": max_t},
    )
    # переносим результаты на ch7
    ch7_hook.exec_with_log('''
    INSERT INTO buffer.foreign_returns_pos(
        shk_id,
        srid,
        return_dt,
        return_status,
        sale_dt,
        nm_id,
        created_dt,
        src_office_id,
        payment_type,
        seller_id,
        seller_country,
        source_row_created
    )
    SELECT
        shk_id,
        srid,
        return_dt,
        return_status,
        sale_dt,
        nm_id,
        created_dt,
        src_office_id,
        payment_type,
        seller_id,
        seller_country,
        source_row_created
    FROM remoteSecure(remote_ch4, db='buffer', table='foreign_returns_pos');
    ''')
    # ставим новые возвратные ШК на мониторинг на лейке
    copy_ch_to_ch_pipe(
        take_data='''
        SELECT
            shk_id,
            min(return_dt) AS return_dt
        FROM buffer.foreign_returns_pos AS t
        WHERE t.return_dt IS NOT NULL
          AND (shk_id, srid) NOT IN (
            SELECT
                shk_id,
                srid
            FROM datamart.foreign_returns_pos
            WHERE shk_id IN (SELECT shk_id FROM buffer.foreign_returns_pos)
        )
        GROUP BY shk_id
        FORMAT MsgPack''',
        insert_data="INSERT INTO buffer.foreign_returns_new_shks_d(shk_id, return_dt) FORMAT MsgPack",
        src_ch=CH7_CONN_ID,
        dst_ch=CH_LAKE_CONN_ID,
    )
    # UPSERT into datamart.foreign_returns_pos
    ch7_hook.exec_with_log('''
    INSERT INTO datamart.foreign_returns_pos(
        shk_id,
        srid,
        nm_id,
        created_dt,
        return_dt,
        return_status,
        sale_dt,
        src_office_id,
        payment_type,
        seller_id,
        seller_country,
        source_row_created
    )
    SELECT
        coalesce(dm.shk_id, b.shk_id) AS shk_id,
        coalesce(dm.srid, b.srid) AS srid,
        coalesce(dm.nm_id, b.nm_id) AS nm_id,
        coalesce(dm.created_dt, b.created_dt) AS created_dt,
        least(b.return_dt, dm.return_dt) AS return_dt,
        If(b.return_dt IS NULL OR dm.return_dt < b.return_dt, dm.return_status, b.return_status) AS return_status,
        least(dm.sale_dt, b.sale_dt) AS sale_dt,
        coalesce(dm.src_office_id, b.src_office_id) AS src_office_id,
        coalesce(b.payment_type, dm.payment_type) AS payment_type,
        coalesce(dm.seller_id, b.seller_id) AS seller_id,
        coalesce(dm.seller_country, b.seller_country) AS seller_country,
        b.source_row_created AS source_row_created
    FROM buffer.foreign_returns_pos b
    LEFT ANY JOIN (
        SELECT
            shk_id,
            srid,
            return_dt,
            return_status,
            sale_dt,
            src_office_id,
            payment_type,
            seller_id,
            seller_country,
            nm_id,
            created_dt
        FROM datamart.foreign_returns_pos FINAL
        WHERE (shk_id, srid) IN (SELECT shk_id, srid FROM buffer.foreign_returns_pos)
    ) AS dm
        ON b.shk_id = dm.shk_id
       AND b.srid = dm.srid;
    ''')
    return max_t


@with_db(CH_LAKE_CONN_ID, "chl")
@with_db(CH7_CONN_ID, "ch7")
@load_and_save_cutoff(
    table_name="datamart.foreign_returns_sop",
    conn_str=CH7_CONN_ID,
    field="max_dt",
)
def lake_to_ch7_load_shk_on_place(chl_hook, ch7_hook, cutoff) -> None:
    """Загружает складские статусы по ШК на мониторинге"""
    min_t, max_t, _, _ = get_ch_dwh_max_v2(
        ch_hook=chl_hook,
        changes_table_name="""view(
            SELECT *
            FROM shk_storage.shk_on_place_rc_d
            WHERE shk_id GLOBAL IN (
                SELECT shk_id FROM buffer.foreign_returns_new_shks_d
            )
            AND dt > (SELECT min(return_dt) FROM buffer.foreign_returns_new_shks_d)
            AND dt >= '2025-01-01'
            AND state_id IS NOT NULL
        )""",
        date_field="row_created",
        max_dt=cutoff,
        max_batch_records=100_000_000,
        back_seek_seconds=10,
    )
    # для ШК которые на мониторинге давно, ищем по отсечке
    chl_hook.on_cluster(
        chl_hook.exec_with_log,
        """
        TRUNCATE TABLE buffer.foreign_returns_sop;

        INSERT INTO buffer.foreign_returns_sop(shk_id, state_id, dt, office_id, source_row_created)
        SELECT
            shk_id,
            state_id,
            dt,
            office_id,
            row_created AS source_row_created
        FROM shk_storage.shk_on_place
        WHERE shk_id IN (
            SELECT shk_id FROM buffer.foreign_returns_new_shks
          )
          AND dt > (SELECT min(return_dt) FROM buffer.foreign_returns_new_shks)
          AND dt >= '2025-01-01'
          AND state_id IS NOT NULL;
        
        INSERT INTO buffer.foreign_returns_sop(shk_id, state_id, dt, office_id, source_row_created)
        SELECT
            shk_id,
            state_id,
            dt,
            office_id,
            row_created AS source_row_created
        FROM shk_storage.shk_on_place_rc
        WHERE shk_id IN (
            SELECT shk_id FROM datamart.foreign_returns_shks FINAL
            UNION ALL
            SELECT shk_id FROM buffer.foreign_returns_new_shks
          )
          AND row_created >= %(min_t)s
          AND row_created <= %(max_t)s
          AND state_id IS NOT NULL;
        """,
        parameters={"min_t": min_t, "max_t": max_t},
    )
    # Копируем результаты с лейка на CH7
    copy_ch_to_ch_pipe(
        take_data="""
            SELECT
                shk_id,
                state_id,
                dt,
                office_id,
                source_row_created
            FROM buffer.foreign_returns_sop_d
            LIMIT 1 BY shk_id, state_id, dt
            SETTINGS distributed_group_by_no_merge=1
            FORMAT MsgPack
        """,
        insert_data="""
        INSERT INTO buffer.foreign_returns_sop(
            shk_id, state_id, dt, office_id, source_row_created
        )
        FORMAT MsgPack""",
        src_ch=CH_LAKE_CONN_ID,
        dst_ch=CH7_CONN_ID,
        throw_if_empty=True,
    )
    # добавляем данные из буфера в датамарт
    ch7_hook.exec_with_log('''
        ALTER TABLE datamart.foreign_returns_sop
        ATTACH PARTITION ALL FROM buffer.foreign_returns_sop;
    ''')
    # переносим новые ШК в таблицу с ШК просто на мониторинге
    chl_hook.on_cluster(
        chl_hook.exec_with_log,
        """INSERT INTO datamart.foreign_returns_shks(shk_id, return_dt)
        SELECT shk_id, return_dt
        FROM buffer.foreign_returns_new_shks;

        TRUNCATE TABLE buffer.foreign_returns_new_shks;
        """
    )
    return max_t


@with_db(CH7_CONN_ID, "ch7")
def ch7_update_dm(ch7_hook):
    """Обновляет витрину из подготовленных данных"""
    ch7_hook.exec_with_log('''
    DROP TABLE IF EXISTS srp;
    CREATE TEMPORARY TABLE srp ENGINE=MergeTree() ORDER BY shk_id AS
    -- отсюда будем определять src_office_id, next_order_dt
    SELECT
        shk_id,
        srid,
        create_dt,
        payment_type,
        src_office_id
    FROM remote_ch.shk_rid_price_nm_v2 AS t final
    WHERE shk_id GLOBAL IN (
        SELECT shk_id FROM buffer.foreign_returns_sop
        UNION ALL
        SELECT shk_id FROM buffer.foreign_returns_pos
    )
    AND t.create_dt >= '2025-01-01';

    INSERT INTO datamart.foreign_returns(
        shk_id, srid, first_srid, nm_id, seller_id, src_office_id, shk_type,
        return_dt, return_status, return_status_name, first_return_dt,
        return_srid, fbo_dt, fbo_state, fbo_office_id, fbo_office_name,
        cancel_fbo_dt, next_order_dt, next_order_srid
    )
    WITH [
        'WBI', 'WLI', 'WLR', 'WLT', 'LPG', 'GTM', 'LIO',
        'PLN', 'ISB', 'SBN', 'WLN', 'BAN',
        'IKZ', 'IWK', 'IPG', 'LIP'
    ] AS on_sale_states
        -- в каждой строке результата видим:
        -- * return_dt - Время возврата
        -- * fbo_dt - время когда товар стал доступен для продажи (FBO)
        -- * cancel_fbo_dt - время когда товар перестал быть FBO
        -- * next_order_dt - время появления следующего клиентского заказа на ШК
        SELECT
            ret.shk_id AS shk_id,
            ret.srid AS srid,
            -- первый срид ШК
            st.first_srid AS first_srid,
            st.nm_id,
            st.seller_id,
            coalesce(st.src_office_id, soi.src_office_id) AS src_office_id,
            if(
                dictGet('dict.branch_offices', 'country_name', soi.src_office_id) IN ('Китай', 'Объединенные Арабские Эмираты'),
                'КБ',
                'ОВХ'
            ) shk_type,
            -- момент возврата
            ret.return_dt,
            ret.return_status,
            ret.return_status_name,
            ret.first_return_dt,
            If(return_srid.created_dt > ret.return_dt, return_srid.srid, null) as return_srid,
            -- когда шк становится FBO (разложен на полку и доступен для продажи)
            fbo.fbo_dt,
            fbo.fbo_state,
            fbo.fbo_office_id,
            fbo.fbo_office_name,
            -- когда шк перестает быть FBO по ШК трекеру (уходит с полки)
            least(fbo.cancel_fbo_dt, next_order_dt) as cancel_fbo_dt,
            -- время создания нового заказа после FBO
            If(next_order.create_dt > ret.return_dt and next_order.srid != ret.srid, next_order.create_dt, NULL) next_order_dt,
            If(next_order.create_dt > ret.return_dt and next_order.srid != ret.srid, next_order.srid, null) next_order_srid
        FROM (
            -- возврат шк по позишенам
            SELECT
                t.shk_id,
                t.srid,
                -- учитываем время первого возврата, если статусов несколько
                min(t.return_dt) AS return_dt,
                argMin(t.return_status, t.return_dt) AS return_status,
                format('{} - {}', toString(return_status), dictGet('dict.positions_statuses_oof', 'status_name', return_status)) AS return_status_name,
                min(return_dt) OVER (PARTITION BY shk_id) AS first_return_dt
            FROM datamart.foreign_returns_pos AS t
            WHERE t.shk_id IN (
                SELECT shk_id FROM buffer.foreign_returns_sop
                UNION ALL
                SELECT shk_id FROM buffer.foreign_returns_pos
            )
            AND t.return_dt > '2025-01-01'
            -- клиентский заказ
            AND NOT startsWith(coalesce(t.payment_type, ''), 'S')
            -- не возвратный срид
            AND NOT startsWith(t.srid, 'mp.')
            GROUP BY t.shk_id, t.srid
        ) ret
        JOIN (
            -- момент когда ШК становится доступен для продажи (fbo_dt)
            -- и когда от перестает быть доступен для продажи (cancel_dbo_dt)
            SELECT
                shk_id,
                dt AS fbo_dt,
                state_id AS fbo_state,
                office_id AS fbo_office_id,
                dictGet('dict.branch_offices', 'office_name', office_id) as fbo_office_name,
                first_value(
                    If(state_id NOT IN on_sale_states, dt, NULL)
                ) OVER(
                    PARTITION BY shk_id
                    ORDER BY dt
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ) cancel_fbo_dt
            FROM datamart.foreign_returns_sop
            WHERE shk_id IN (
                SELECT shk_id FROM buffer.foreign_returns_sop
                UNION ALL
                SELECT shk_id FROM buffer.foreign_returns_pos
            )
            QUALIFY state_id IN on_sale_states
            -- учитываем только последний fbo-статус за день
            ORDER BY fbo_dt DESC
            LIMIT 1 BY shk_id, toDate(fbo_dt)
        ) as fbo
            ON ret.shk_id=fbo.shk_id
        LEFT JOIN (
            -- специальный вид "заказа", который поставщик оформляет для возврата товара на склад
            SELECT srid, shk_id, min(created_dt) as created_dt
            FROM datamart.foreign_returns_pos
            WHERE shk_id IN (
                SELECT shk_id FROM buffer.foreign_returns_sop
                UNION ALL
                SELECT shk_id FROM buffer.foreign_returns_pos
            )
            AND startsWith(srid, 'mp.')
            GROUP BY srid, shk_id
        ) return_srid ON ret.shk_id = return_srid.shk_id
        LEFT ANY JOIN (
            -- свойства ШК которые не должны меняться от версии к версии строки
            SELECT
                shk_id,
                argMin(srid, created_dt) AS first_srid,
                any(nm_id) AS nm_id,
                any(seller_id) AS seller_id,
                argMin(src_office_id, created_dt) AS src_office_id
            FROM datamart.foreign_returns_pos
            WHERE shk_id IN (
                SELECT shk_id FROM buffer.foreign_returns_sop
                UNION ALL
                SELECT shk_id FROM buffer.foreign_returns_pos
            )
            GROUP BY shk_id
        ) st
        ON ret.shk_id = st.shk_id
        LEFT ANY JOIN (
            -- первый офис по ШК будет определяет shk_type
            SELECT
                shk_id,
                src_office_id
            FROM srp
            ORDER BY create_dt
            LIMIT 1 BY shk_id
        ) AS soi ON soi.shk_id = ret.shk_id
        LEFT JOIN (
            -- следующий клиентский заказ по ШК
            SELECT shk_id, srid, min(create_dt) create_dt
            FROM srp
            WHERE NOT startsWith(coalesce(payment_type, ''), 'S')
            GROUP BY shk_id, srid
        ) next_order
            ON next_order.shk_id = ret.shk_id
        -- ШК доступен для продажи после возврата
        WHERE fbo.fbo_dt > ret.return_dt
        -- next order должен быть после раскладки на полку
        AND (fbo.fbo_dt < next_order_dt OR next_order_dt IS NULL)
        -- возвратный срид должен появиться после возврата
        AND (return_srid.created_dt > ret.return_dt OR return_srid.srid IS NULL)
        -- ORDER BY обеспечивает правильный порядок событий:
        -- return_dt ->
        -- fbo_dt - время раскладки на полку ->
        -- next_order_dt - следующий за возвратом заказ (может и не быть) ->
        -- cancel_fbo_dt - время снятия ШК с полки.
        -- return_created_dt - время создания "возвратного" срида,
        -- если return_srid определен, то скорее всего никакого fbo_dt, cancel_fbo_dt, next_order_dt не будет!
        ORDER BY ret.return_dt, next_order_dt, fbo.fbo_dt DESC, fbo.cancel_fbo_dt, return_srid.created_dt
        LIMIT 1 BY ret.shk_id, ret.return_dt
        SETTINGS join_use_nulls=1
    ''')


@with_db(CH7_CONN_ID, "ch7")
def lake_remove_from_monitoring(ch7_hook):
    """Удаляет записи с мониторинга shk-on-place
    Если в этом больше нет смысла
    """
    # проданные ШК
    copy_ch_to_ch_pipe(
        take_data="""
        SELECT shk_id, true AS is_deleted
        FROM datamart.foreign_returns_pos
        WHERE shk_id IN (
            SELECT shk_id FROM buffer.foreign_returns_sop
            UNION ALL
            SELECT shk_id FROM buffer.foreign_returns_pos
        )
        GROUP BY shk_id
        HAVING max(sale_dt) > max(return_dt)
        FORMAT MsgPack
        """,
        insert_data="INSERT INTO datamart.foreign_returns_shks_d(shk_id, is_deleted) FORMAT MsgPack",
        src_ch=CH7_CONN_ID,
        dst_ch=CH_LAKE_CONN_ID,
    )
    # если после списания нет другого движения в течение 3 дней
    copy_ch_to_ch_pipe(
        take_data="""
        SELECT shk_id, true AS is_deleted
        FROM (
            SELECT
                shk_id,
                argMax(state_id, dt) AS last_status,
                max(dt) AS max_dt
            FROM datamart.foreign_returns_sop
            WHERE dt >= today() - interval 14 days
              AND shk_id IN (
                SELECT shk_id
                FROM datamart.foreign_returns_sop
                WHERE dt >= now() - interval 7 days
                  AND state_id IN ('ORX', 'ORZ', 'ORU', 'WUD')
              )
            GROUP BY shk_id
            HAVING last_status IN ('ORX', 'ORZ', 'ORU', 'WUD')
               AND max_dt < now() - interval 3 day
        ) t
        FORMAT MsgPack
        """,
        insert_data="INSERT INTO datamart.foreign_returns_shks_d(shk_id, is_deleted) FORMAT MsgPack",
        src_ch=CH7_CONN_ID,
        dst_ch=CH_LAKE_CONN_ID,
    )
    # очищаем буферы в конце
    ch7_hook.exec_with_log('''
    TRUNCATE TABLE buffer.foreign_returns_pos;
    TRUNCATE TABLE buffer.foreign_returns_sop;
    ''')


default_args = {
    "owner": "ivanenko",
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="ch7_foreign_returns_resale",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["foreign_returns", CH4_CONN_ID, CH7_CONN_ID, CH_LAKE_CONN_ID, "@ilushair"],
) as dag:
    ch4_positions_load_returns_task = PythonOperator(
        task_id="ch4_positions_load_returns_task",
        python_callable=ch4_positions_load_returns,
        pool=CH7_CONN_ID,
        inlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch4.positions.oof_position_status_v3_rc"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch4.remote_ch3.product_cards_nm"),
            OMEntity(entity=Entity.TABLE, key="2", fqn="do-ch4.buffer.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="3", fqn="do-ch7.buffer.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="4", fqn="do-ch7.buffer.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="4", fqn="do-ch7.datamart.foreign_returns_pos"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch4.buffer.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="2", fqn="do-ch7.buffer.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="3", fqn="do-lake-m.buffer.foreign_returns_new_shks"),
            OMEntity(entity=Entity.TABLE, key="4", fqn="do-ch7.datamart.foreign_returns_pos"),
        ],
    )
    lake_to_ch7_load_shk_on_place_task = PythonOperator(
        task_id="lake_to_ch7_load_shk_on_place_task",
        python_callable=lake_to_ch7_load_shk_on_place,
        pool=CH7_CONN_ID,
        inlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-lake-m.shk_storage.shk_on_place"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-lake-m.buffer.foreign_returns_new_shks"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-lake-m.shk_storage.shk_on_place_rc"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-lake-m.datamart.foreign_returns_shks"),
            OMEntity(entity=Entity.TABLE, key="2", fqn="do-lake-m.buffer.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="3", fqn="do-ch7.buffer.foreign_returns_sop"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-lake-m.buffer.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="2", fqn="do-ch7.buffer.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="3", fqn="do-ch7.datamart.foreign_returns_sop"),
        ],
    )
    ch7_update_dm_task = PythonOperator(
        task_id="ch7_update_dm_task",
        python_callable=ch7_update_dm,
        pool=CH7_CONN_ID,
        inlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.remote_ch.shk_rid_price_nm_v2"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.buffer.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.buffer.foreign_returns_pos"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns"),
        ],
    )
    lake_remove_from_monitoring_task = PythonOperator(
        task_id="lake_remove_from_monitoring_task",
        python_callable=lake_remove_from_monitoring,
        pool=CH_LAKE_CONN_ID,
        inlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns_pos"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.buffer.foreign_returns_sop"),
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.buffer.foreign_returns_pos"),
        ],
        outlets=[
            OMEntity(entity=Entity.TABLE, key="1", fqn="do-ch7.datamart.foreign_returns_shks"),
        ],
    )

    ch4_positions_load_returns_task >> lake_to_ch7_load_shk_on_place_task >> ch7_update_dm_task >> lake_remove_from_monitoring_task
