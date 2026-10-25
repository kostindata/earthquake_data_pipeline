# Эксплуатация

Проверка результата, перезапуск и backfill, диагностика и хранение данных.

## Проверка результата

Количество строк и доступный диапазон ODS:

```bash
docker compose exec -T postgres_dwh psql -U postgres -d postgres -c \
  "SELECT count(*) AS rows,
          count(DISTINCT time::date) AS days,
          min(time::date) AS min_date,
          max(time::date) AS max_date
     FROM ods.fct_earthquake;"
```

Содержимое витрины:

```bash
docker compose exec -T postgres_dwh psql -U postgres -d postgres -c \
  "SELECT date, events, round(avg_mag::numeric, 2) AS avg_mag, max_mag
     FROM dm.fct_day_earthquake
    ORDER BY date DESC
    LIMIT 10;"
```

Сверка витрины с ODS:

```sql
SELECT
    ods.date,
    ods.event_count AS ods_count,
    dm.events AS dm_count
FROM (
    SELECT left(time, 10)::date AS date, count(DISTINCT id) AS event_count
    FROM ods.fct_earthquake
    WHERE type = 'earthquake' AND NULLIF(mag, '') IS NOT NULL
    GROUP BY 1
) AS ods
FULL JOIN dm.fct_day_earthquake AS dm USING (date)
WHERE ods.event_count IS DISTINCT FROM dm.events
ORDER BY date;
```

Пустой результат означает, что витрина совпадает с источником.

## Перезапуск и backfill

### В ODS данные есть, нужно пересчитать только DM

Очистите task instances нужного DM DAG (`fct_day_earthquake`,
`fct_class_day_earthquake`, `fct_region_day_earthquake` или
`fct_event_earthquake`) за выбранный диапазон. Airflow выполнит их повторно:

```bash
docker compose exec -T airflow-scheduler \
  airflow tasks clear fct_day_earthquake \
  --start-date 2026-06-01 \
  --end-date 2026-06-10 \
  --yes
```

### В ODS отсутствуют даты, но файлы есть в MinIO

Сначала повторно загрузите только отсутствующий диапазон:

```bash
docker compose exec -T airflow-scheduler \
  airflow tasks clear raw_from_s3_to_pg \
  --task-regex '^get_and_transfer_raw_data_to_ods_pg$' \
  --start-date 2026-06-01 \
  --end-date 2026-06-10 \
  --yes
```

После успешного заполнения ODS очистите соответствующие DM runs командой из
предыдущего раздела.

> Важно: текущая ODS-загрузка использует обычный `INSERT`, а не upsert.
> Повторный запуск уже загруженной даты создаст дубликаты и завысит агрегаты.
> Перед очисткой task instances сначала определите действительно отсутствующие
> даты.

Найти пропуски в диапазоне:

```sql
WITH expected AS (
    SELECT generate_series(
        DATE '2026-06-01',
        DATE '2026-08-25',
        INTERVAL '1 day'
    )::date AS date
),
loaded AS (
    SELECT DISTINCT time::date AS date
    FROM ods.fct_earthquake
)
SELECT expected.date
FROM expected
LEFT JOIN loaded USING (date)
WHERE loaded.date IS NULL
ORDER BY expected.date;
```

## Диагностика

### DAG зелёный, но DM-таблица пустая

Успешный SQL без выбранных строк остаётся успешной задачей. Сначала сравните
logical date запуска с датами в ODS:

```sql
SELECT time::date AS date, count(*)
FROM ods.fct_earthquake
GROUP BY 1
ORDER BY 1;
```

В логе задачи `create_stg_table` строка `Rows affected: 0` означает, что для
этой logical date исходных данных не было. Нужно восстановить ODS за эту дату,
а затем перезапустить DM DAG.

### Сенсор ждёт upstream DAG

Проверьте, что оба DAG имеют run с одинаковой logical date:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs -d raw_from_s3_to_pg

docker compose exec -T airflow-scheduler \
  airflow dags list-runs -d fct_day_earthquake
```

### Ошибка импорта DAG

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose logs --tail=200 airflow-scheduler
```

### Состояние контейнеров

```bash
docker compose ps
docker compose logs --tail=200 airflow-worker
```

## Хранение данных

MinIO сохраняет данные в каталоге `./data`, а метаданные Airflow — в именованном
volume `postgres-db-volume`.

В текущем `docker-compose.yaml` у `postgres_dwh` не задан именованный volume.
Перед длительным использованием рекомендуется добавить:

```yaml
services:
  postgres_dwh:
    volumes:
      - postgres-dwh-volume:/var/lib/postgresql/data

volumes:
  postgres-db-volume:
  postgres-dwh-volume:
```

Без этого после пересоздания контейнера Airflow может помнить старые успешные
запуски, хотя таблицы DWH уже потеряны. Именно в такой ситуации downstream DAG
может завершаться успешно с `0 rows affected`.

Остановить сервисы без явного удаления именованных volumes:

```bash
docker compose down
```

Не используйте `docker compose down -v`, если данные нужно сохранить.
