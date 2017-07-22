DROP KEYSPACE IF EXISTS metric_data;
CREATE KEYSPACE metric_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use metric_data;

CREATE TABLE raw_counter (
    environment text,
    application text,
    metric_name text,           // metric name
    metric_timestamp timestamp,        // timestamp generated at reporter
    count bigint,
    PRIMARY KEY ((environment, application, metric_name), metric_timestamp)
) WITH CLUSTERING ORDER BY (metric_timestamp ASC);

CREATE TABLE raw_timer (
    environment text,
    application text,
    metric_name text,
    metric_timestamp timestamp,
    count bigint,
    max float,
    mean float,
    min float,
    std_dev float,
    median float,
    p75 float,
    p95 float,
    p98 float,
    p99 float,
    p999 float,
    mean_rate float,
    one_min_rate float,
    five_min_rate float,
    fifteen_min_rate float,
    rate_unit text,
    duration_unit text,
    PRIMARY KEY ((environment, application, metric_name), metric_timestamp)
) WITH CLUSTERING ORDER BY (metric_timestamp ASC);

CREATE TABLE raw_counter_with_interval (
    environment text,
    application text,
    metric_name text,           // metric name
    metric_timestamp timestamp,        // timestamp generated at reporter
    count bigint,               // Year collected
    previous_metric_timestamp timestamp,
    previous_count bigint,
    PRIMARY KEY ((environment, application, metric_name), metric_timestamp)
) WITH CLUSTERING ORDER BY (metric_timestamp ASC);

CREATE TABLE raw_timer_with_interval (
    environment text,
    application text,
    metric_name text,
    metric_timestamp timestamp,
    count bigint,
    max float,
    mean float,
    min float,
    std_dev float,
    median float,
    p75 float,
    p95 float,
    p98 float,
    p99 float,
    p999 float,
    mean_rate float,
    one_min_rate float,
    five_min_rate float,
    fifteen_min_rate float,
    previous_metric_timestamp timestamp,
    previous_count bigint,
    rate_unit text,
    duration_unit text,
    PRIMARY KEY ((environment, application, metric_name), metric_timestamp)
) WITH CLUSTERING ORDER BY (metric_timestamp ASC);
