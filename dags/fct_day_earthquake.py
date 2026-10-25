# Airflow DAG: сам DAG собирает dm_factory.build_dm_dag

from dm_factory import EVENTS_WHERE, build_dm_dag

DAG_ID = "fct_day_earthquake"

# Показатели дня: число событий, средняя и максимальная магнитуда, число M5+
SELECT = f"""
SELECT
    left(time, 10)::date AS date,
    count(DISTINCT id) AS events,
    avg(mag::float) AS avg_mag,
    max(mag::float) AS max_mag,
    count(DISTINCT id) FILTER (WHERE mag::float >= 5) AS m5_events
FROM ods.fct_earthquake
WHERE {EVENTS_WHERE}
GROUP BY 1
"""

dag = build_dm_dag(DAG_ID, SELECT, "Показатели землетрясений по дням")
