# Airflow DAG: сам DAG собирает dm_factory.build_dm_dag

from dm_factory import EVENTS_WHERE, build_dm_dag

DAG_ID = "fct_class_day_earthquake"

# Число событий по целому значению магнитуды (0..6, 6 = M6+) и классу глубины
SELECT = f"""
SELECT
    left(time, 10)::date AS date,
    least(floor(greatest(mag::float, 0)), 6)::smallint AS mag_floor,
    CASE
        WHEN depth::float < 70 THEN 'shallow'
        WHEN depth::float < 300 THEN 'intermediate'
        ELSE 'deep'
    END AS depth_class,
    count(DISTINCT id) AS events
FROM ods.fct_earthquake
WHERE {EVENTS_WHERE}
GROUP BY 1, 2, 3
"""

dag = build_dm_dag(DAG_ID, SELECT, "События по магнитуде и глубине по дням")
