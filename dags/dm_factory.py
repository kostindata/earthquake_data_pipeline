import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "n.kostin"

# Используемые таблицы в DAG
SCHEMA = "dm"

# DWH
PG_CONNECT = "postgres_dwh"

DAY = "{{ data_interval_start.format('YYYY-MM-DD') }}"

# Общий фильтр витрин: только землетрясения с известной магнитудой за один день
EVENTS_WHERE = """
    type = 'earthquake'
    AND NULLIF(mag, '') IS NOT NULL
    AND left(time, 10)::date = '{day}'
"""

# Регион — последняя часть place после запятой: "5 km NE of Ridgecrest, CA" -> "CA"
REGION = "trim(regexp_replace(place, '^.*,', ''))"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def build_dm_dag(dag_id: str, select_sql: str, description: str) -> DAG:
    """Дневной пересчёт витрины dm.<dag_id> из ods.fct_earthquake.

    select_sql — SELECT за один день (плейсхолдер {day}), порядок колонок
    должен совпадать с целевой таблицей.
    """
    stg_table = f'stg."tmp_{dag_id}_{DAY}"'

    with DAG(
        dag_id=dag_id,
        schedule_interval="0 5 * * *",
        default_args=args,
        tags=["dm", "pg"],
        description=description,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
    ) as dag:
        start = EmptyOperator(task_id="start")

        sensor_on_raw_layer = ExternalTaskSensor(
            task_id="sensor_on_raw_layer",
            external_dag_id="raw_from_s3_to_pg",
            allowed_states=["success"],
            mode="reschedule",
            timeout=360000,  # длительность работы сенсора
            poke_interval=60,  # частота проверки
        )

        drop_stg_table_before = SQLExecuteQueryOperator(
            task_id="drop_stg_table_before",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"DROP TABLE IF EXISTS {stg_table}",
        )

        create_stg_table = SQLExecuteQueryOperator(
            task_id="create_stg_table",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"CREATE TABLE {stg_table} AS {select_sql.format(day=DAY)}",
        )

        drop_from_target_table = SQLExecuteQueryOperator(
            task_id="drop_from_target_table",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"DELETE FROM {SCHEMA}.{dag_id} WHERE date = '{DAY}'",
        )

        insert_into_target_table = SQLExecuteQueryOperator(
            task_id="insert_into_target_table",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"INSERT INTO {SCHEMA}.{dag_id} SELECT * FROM {stg_table}",
        )

        drop_stg_table_after = SQLExecuteQueryOperator(
            task_id="drop_stg_table_after",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"DROP TABLE IF EXISTS {stg_table}",
        )

        end = EmptyOperator(task_id="end")

        (
            start
            >> sensor_on_raw_layer
            >> drop_stg_table_before
            >> create_stg_table
            >> drop_from_target_table
            >> insert_into_target_table
            >> drop_stg_table_after
            >> end
        )

    return dag
