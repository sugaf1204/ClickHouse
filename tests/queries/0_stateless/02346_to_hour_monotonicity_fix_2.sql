DROP TABLE IF EXISTS test;

CREATE TABLE test (stamp DateTime('UTC')) ENGINE = MergeTree PARTITION BY toDate(stamp) ORDER BY tuple() as select toDateTime('2020-01-01', 'UTC')+number*60 from numbers(1e3);

SELECT count() result FROM test WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test;

CREATE TABLE test (stamp Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree PARTITION BY toDate(stamp) ORDER BY tuple() SETTINGS allow_nullable_key = 1 as select toDateTime64('2020-01-01 00:00:00', 3, 'UTC') + toIntervalMinute(number) from numbers(1e3);

SELECT count() result FROM test WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test;

CREATE TABLE test (stamp DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toDate(stamp) ORDER BY tuple() as select toDateTime64('2020-01-01 00:00:00', 3, 'UTC') + toIntervalMinute(number) from numbers(1e3);

SELECT count() result FROM test WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test;

CREATE TABLE test (stamp Nullable(DateTime('UTC'))) ENGINE = MergeTree PARTITION BY toDate(stamp) ORDER BY tuple() SETTINGS allow_nullable_key = 1 as select toDateTime('2020-01-01', 'UTC')+number*60 from numbers(1e3);

SELECT count() result FROM test WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test;
