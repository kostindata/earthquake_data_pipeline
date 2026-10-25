#!/usr/bin/env python3
"""Создаёт (или обновляет) дашборд «Землетрясения» в Metabase через REST API.

Только стандартная библиотека. Повторный запуск безопасен: коллекция, карточки
и дашборд ищутся по имени и обновляются, а не дублируются.

    MB_EMAIL=admin@example.com MB_PASSWORD='...' python3 metabase/provision_dashboard.py

Если Metabase ещё не настроен, первый запуск создаст администратора с этими
учётными данными.
"""

import json
import os
import sys
import urllib.error
import urllib.request
import uuid

MB_URL = os.environ.get("MB_URL", "http://localhost:3000").rstrip("/")
MB_EMAIL = os.environ.get("MB_EMAIL")
MB_PASSWORD = os.environ.get("MB_PASSWORD")

# Подключение Metabase -> DWH (имена внутри docker-сети)
DWH_NAME = os.environ.get("DWH_NAME", "Earthquake DWH")
DWH_DETAILS = {
    "host": os.environ.get("DWH_HOST", "postgres_dwh"),
    "port": int(os.environ.get("DWH_PORT", "5432")),
    "dbname": os.environ.get("DWH_DB", "postgres"),
    "user": os.environ.get("DWH_USER", "postgres"),
    "password": os.environ.get("DWH_PASSWORD", "postgres"),
    "ssl": False,
}

COLLECTION_NAME = "Землетрясения"
DASHBOARD_NAME = "Землетрясения: обзор"

# ODS хранит всё как varchar, поэтому приводим типы здесь. Фильтры по датам —
# необязательные блоки [[ ]]: без значения фильтра они выпадают из запроса.
SRC = """
FROM ods.fct_earthquake
WHERE type = 'earthquake'
  AND NULLIF(mag, '') IS NOT NULL
  [[AND left(time, 10)::date >= {{date_from}}]]
  [[AND left(time, 10)::date <= {{date_to}}]]
"""

# (name, display, sql, visualization_settings)
CARDS = {
    "events": (
        "События",
        "scalar",
        f"SELECT count(DISTINCT id) AS events {SRC}",
        {},
    ),
    "avg_mag": (
        "Средняя магнитуда",
        "scalar",
        f"SELECT round(avg(mag::float)::numeric, 2) AS avg_magnitude {SRC}",
        {},
    ),
    "max_mag": (
        "Максимальная магнитуда",
        "scalar",
        f"SELECT max(mag::float) AS max_magnitude {SRC}",
        {},
    ),
    "m5": (
        "Сильные события (M5+)",
        "scalar",
        f"SELECT count(DISTINCT id) AS m5_plus {SRC} AND mag::float >= 5",
        {},
    ),
    "daily_events": (
        "События по дням и скользящее среднее за 7 дней",
        "combo",
        f"""
        WITH daily AS (
            SELECT left(time, 10)::date AS day, count(DISTINCT id) AS events
            {SRC}
            GROUP BY 1
        )
        SELECT
            day,
            events,
            round(avg(events) OVER (ORDER BY day ROWS 6 PRECEDING), 1)
                AS events_7d_avg
        FROM daily
        ORDER BY day
        """,
        {
            "graph.dimensions": ["day"],
            "graph.metrics": ["events", "events_7d_avg"],
            "series_settings": {
                "events": {"display": "bar"},
                "events_7d_avg": {"display": "line"},
            },
        },
    ),
    "daily_mag": (
        "Магнитуда по дням: средняя и максимальная",
        "line",
        f"""
        SELECT
            left(time, 10)::date AS day,
            round(avg(mag::float)::numeric, 2) AS avg_magnitude,
            max(mag::float) AS max_magnitude
        {SRC}
        GROUP BY 1
        ORDER BY 1
        """,
        {
            "graph.dimensions": ["day"],
            "graph.metrics": ["avg_magnitude", "max_magnitude"],
        },
    ),
    "mag_bands": (
        "Распределение по магнитуде",
        "bar",
        f"""
        SELECT band, events FROM (
            SELECT
                CASE
                    WHEN mag::float < 1 THEN '< 1'
                    WHEN mag::float < 2 THEN '1–2'
                    WHEN mag::float < 3 THEN '2–3'
                    WHEN mag::float < 4 THEN '3–4'
                    WHEN mag::float < 5 THEN '4–5'
                    WHEN mag::float < 6 THEN '5–6'
                    ELSE '6+'
                END AS band,
                least(floor(greatest(mag::float, 0)), 6) AS sort_key,
                count(DISTINCT id) AS events
            {SRC}
            GROUP BY 1, 2
        ) t
        ORDER BY sort_key
        """,
        {"graph.dimensions": ["band"], "graph.metrics": ["events"]},
    ),
    "depth": (
        "Глубина очагов",
        "pie",
        f"""
        SELECT
            CASE
                WHEN depth::float < 70 THEN 'Неглубокие (< 70 км)'
                WHEN depth::float < 300 THEN 'Промежуточные (70–300 км)'
                ELSE 'Глубокие (> 300 км)'
            END AS depth_class,
            count(DISTINCT id) AS events
        {SRC}
          AND NULLIF(depth, '') IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        """,
        {"pie.dimension": "depth_class", "pie.metric": "events"},
    ),
    # Каталог USGS полон слабых событий в США (плотная сеть станций), поэтому
    # без порога рейтинг состоит из американских штатов. M4.5+ — порог, выше
    # которого каталог полон по всему миру.
    "regions": (
        "Топ-10 регионов мира (события M4.5+)",
        "row",
        f"""
        SELECT
            trim(regexp_replace(place, '^.*,', '')) AS region,
            count(DISTINCT id) AS events
        {SRC}
          AND NULLIF(place, '') IS NOT NULL
          AND mag::float >= 4.5
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
        """,
        {"graph.dimensions": ["region"], "graph.metrics": ["events"]},
    ),
    "strongest": (
        "Самые сильные землетрясения",
        "table",
        f"""
        SELECT
            replace(left(time, 19), 'T', ' ') AS time_utc,
            mag::float AS magnitude,
            place,
            round(NULLIF(depth, '')::float::numeric, 1) AS depth_km
        {SRC}
        ORDER BY mag::float DESC, time DESC
        LIMIT 15
        """,
        {},
    ),
    "map": (
        "Карта событий M3+ (до 2000 сильнейших)",
        "map",
        f"""
        SELECT
            latitude::float AS latitude,
            longitude::float AS longitude,
            mag::float AS magnitude,
            place
        {SRC}
          AND mag::float >= 3
          AND NULLIF(latitude, '') IS NOT NULL
          AND NULLIF(longitude, '') IS NOT NULL
        ORDER BY mag::float DESC
        LIMIT 2000
        """,
        {
            "map.type": "pin",
            "map.pin_type": "tiles",
            "map.latitude_column": "latitude",
            "map.longitude_column": "longitude",
        },
    ),
    "map_m5": (
        "Карта сильных событий (M5+)",
        "map",
        f"""
        SELECT
            latitude::float AS latitude,
            longitude::float AS longitude,
            mag::float AS magnitude,
            place
        {SRC}
          AND mag::float >= 5
          AND NULLIF(latitude, '') IS NOT NULL
          AND NULLIF(longitude, '') IS NOT NULL
        ORDER BY mag::float DESC
        LIMIT 2000
        """,
        {
            "map.type": "pin",
            "map.pin_type": "tiles",
            "map.latitude_column": "latitude",
            "map.longitude_column": "longitude",
        },
    ),
}

# Сетка Metabase — 24 колонки: (ключ карточки, row, col, ширина, высота)
LAYOUT = [
    ("events", 0, 0, 6, 3),
    ("avg_mag", 0, 6, 6, 3),
    ("max_mag", 0, 12, 6, 3),
    ("m5", 0, 18, 6, 3),
    ("daily_events", 3, 0, 24, 7),
    ("daily_mag", 10, 0, 24, 6),
    ("mag_bands", 16, 0, 12, 6),
    ("depth", 16, 12, 12, 6),
    ("regions", 22, 0, 12, 8),
    ("strongest", 22, 12, 12, 8),
    ("map", 30, 0, 24, 10),
    ("map_m5", 40, 0, 24, 10),
]

PARAMETERS = [
    {"id": "date_from", "name": "С даты", "slug": "date_from", "type": "date/single"},
    {"id": "date_to", "name": "По дату", "slug": "date_to", "type": "date/single"},
]


class Metabase:
    def __init__(self, url):
        self.url = url
        self.session = None

    def call(self, method, path, body=None, auth=True, allow=()):
        headers = {"Content-Type": "application/json"}
        if auth and self.session:
            headers["X-Metabase-Session"] = self.session
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.url}/api{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            if e.code in allow:
                return None
            sys.exit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:500]}")
        except urllib.error.URLError as e:
            sys.exit(f"Metabase недоступен по адресу {self.url}: {e.reason}")
        return json.loads(raw) if raw else None

    def login(self, email, password):
        props = self.call("GET", "/session/properties", auth=False)
        token = props.get("setup-token")
        if token:
            print("Metabase не настроен: создаю администратора")
            # 403 — пользователь уже есть (мастер настройки не завершён):
            # просто входим под ним
            self.call(
                "POST",
                "/setup",
                {
                    "token": token,
                    "user": {
                        "first_name": "Admin",
                        "last_name": "Admin",
                        "email": email,
                        "password": password,
                    },
                    "prefs": {"site_name": "Earthquakes", "allow_tracking": False},
                },
                auth=False,
                allow=(403,),
            )
        res = self.call(
            "POST", "/session", {"username": email, "password": password}, auth=False
        )
        self.session = res["id"]

    def ensure_database(self):
        dbs = self.call("GET", "/database")
        dbs = dbs["data"] if isinstance(dbs, dict) else dbs
        for db in dbs:
            if db["name"] == DWH_NAME:
                return db["id"]
        print(f"Создаю подключение «{DWH_NAME}»")
        db = self.call(
            "POST",
            "/database",
            {
                "name": DWH_NAME,
                "engine": "postgres",
                "details": DWH_DETAILS,
                "is_full_sync": True,
            },
        )
        return db["id"]

    def ensure_collection(self):
        for c in self.call("GET", "/collection"):
            if c["name"] == COLLECTION_NAME and not c.get("archived"):
                return c["id"]
        return self.call("POST", "/collection", {"name": COLLECTION_NAME})["id"]

    def upsert_card(self, existing, name, display, sql, viz, db_id, collection_id):
        tags = {
            slug: {
                "id": str(uuid.uuid4()),
                "name": slug,
                "display-name": label,
                "type": "date",
            }
            for slug, label in (("date_from", "Date from"), ("date_to", "Date to"))
        }
        body = {
            "name": name,
            "display": display,
            "collection_id": collection_id,
            "visualization_settings": viz,
            "dataset_query": {
                "type": "native",
                "database": db_id,
                "native": {"query": sql.strip(), "template-tags": tags},
            },
        }
        if name in existing:
            return self.call("PUT", f"/card/{existing[name]}", body)["id"]
        return self.call("POST", "/card", body)["id"]


def main():
    if not (MB_EMAIL and MB_PASSWORD):
        sys.exit("Задайте MB_EMAIL и MB_PASSWORD")

    mb = Metabase(MB_URL)
    mb.login(MB_EMAIL, MB_PASSWORD)
    db_id = mb.ensure_database()
    collection_id = mb.ensure_collection()

    existing = {
        c["name"]: c["id"]
        for c in mb.call("GET", "/card")
        if c.get("collection_id") == collection_id and not c.get("archived")
    }
    card_ids = {}
    for key, (name, display, sql, viz) in CARDS.items():
        card_ids[key] = mb.upsert_card(
            existing, name, display, sql, viz, db_id, collection_id
        )
        print(f"Карточка: {name}")

    mappings = lambda card_id: [  # noqa: E731
        {
            "parameter_id": p["id"],
            "card_id": card_id,
            "target": ["variable", ["template-tag", p["slug"]]],
        }
        for p in PARAMETERS
    ]
    dashcards = [
        {
            "id": -(i + 1),
            "card_id": card_ids[key],
            "row": row,
            "col": col,
            "size_x": w,
            "size_y": h,
            "parameter_mappings": mappings(card_ids[key]),
            "visualization_settings": {},
        }
        for i, (key, row, col, w, h) in enumerate(LAYOUT)
    ]

    dash_id = next(
        (
            d["id"]
            for d in mb.call("GET", "/dashboard")
            if d["name"] == DASHBOARD_NAME
            and d.get("collection_id") == collection_id
            and not d.get("archived")
        ),
        None,
    )
    if dash_id is None:
        dash_id = mb.call(
            "POST",
            "/dashboard",
            {
                "name": DASHBOARD_NAME,
                "collection_id": collection_id,
                "description": "Обзор сейсмической активности по данным USGS",
            },
        )["id"]
    mb.call(
        "PUT",
        f"/dashboard/{dash_id}",
        {"parameters": PARAMETERS, "dashcards": dashcards},
    )
    print(f"\nГотово: {MB_URL}/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
