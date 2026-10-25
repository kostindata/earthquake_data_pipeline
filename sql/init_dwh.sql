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

CREATE TABLE IF NOT EXISTS dm.fct_day_earthquake (
    date      date,
    events    bigint,
    avg_mag   double precision,
    max_mag   double precision,
    m5_events bigint
);

CREATE TABLE IF NOT EXISTS dm.fct_class_day_earthquake (
    date        date,
    mag_floor   smallint,
    depth_class varchar,
    events      bigint
);

CREATE TABLE IF NOT EXISTS dm.fct_region_day_earthquake (
    date       date,
    region     varchar,
    events_m45 bigint
);

CREATE TABLE IF NOT EXISTS dm.fct_event_earthquake (
    date       date,
    id         varchar,
    event_time timestamp,
    latitude   double precision,
    longitude  double precision,
    depth_km   double precision,
    mag        double precision,
    place      varchar,
    region     varchar
);
