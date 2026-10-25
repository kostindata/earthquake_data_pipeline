# Airflow DAG: сам DAG собирает dm_factory.build_dm_dag

from dm_factory import EVENTS_WHERE, REGION, build_dm_dag

DAG_ID = "fct_region_day_earthquake"

# Только M4.5+: каталог USGS полон по всему миру начиная с этого порога, а
# слабые события в основном американские
SELECT = f"""
SELECT
    left(time, 10)::date AS date,
    {REGION} AS region,
    count(DISTINCT id) AS events_m45
FROM ods.fct_earthquake
WHERE {EVENTS_WHERE}
    AND mag::float >= 4.5
GROUP BY 1, 2
"""

dag = build_dm_dag(DAG_ID, SELECT, "События M4.5+ по регионам и дням")
