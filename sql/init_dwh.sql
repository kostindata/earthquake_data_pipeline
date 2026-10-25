CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS ods.fct_earthquake (
    time             varchar,
    latitude         varchar,
    longitude        varchar,
    depth            varchar,
    mag              varchar,
    mag_type         varchar,
    nst              varchar,
    gap              varchar,
    dmin             varchar,
    rms              varchar,
    net              varchar,
    id               varchar,
    updated          varchar,
    place            varchar,
    type             varchar,
    horizontal_error varchar,
    depth_error      varchar,
    mag_error        varchar,
    mag_nst          varchar,
    status           varchar,
    location_source  varchar,
    mag_source       varchar
);

CREATE TABLE IF NOT EXISTS dm.fct_count_day_earthquake (
    date  date,
    count bigint
);

CREATE TABLE IF NOT EXISTS dm.fct_avg_day_earthquake (
    date date,
    avg  double precision
);
