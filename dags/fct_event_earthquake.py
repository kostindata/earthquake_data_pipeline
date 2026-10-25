# Airflow DAG: сам DAG собирает dm_factory.build_dm_dag

from dm_factory import EVENTS_WHERE, REGION, build_dm_dag

DAG_ID = "fct_event_earthquake"

# Типизированные события M3+ для карт и списка сильнейших землетрясений
SELECT = f"""
SELECT DISTINCT ON (id)
    left(time, 10)::date AS date,
    id,
    replace(left(time, 19), 'T', ' ')::timestamp AS event_time,
    latitude::float AS latitude,
    longitude::float AS longitude,
    depth::float AS depth_km,
    mag::float AS mag,
    place,
    {REGION} AS region
FROM ods.fct_earthquake
WHERE {EVENTS_WHERE}
    AND mag::float >= 3
ORDER BY id
"""

dag = build_dm_dag(DAG_ID, SELECT, "События M3+ с типизированными колонками")
