{{ config(materialized='table', alias='forex_data_1') }}

WITH ticks AS (
    SELECT
        timestamp::timestamp AS ts,
        pair_name,
        (bid_price + ask_price)/2.0 AS price
    FROM wtc_analytics.forex_data.forex_raw 
),

bars AS (
    SELECT
        pair_name,
        'M1' AS timeframe,
        date_trunc('minute', ts) AS bar_time,
        FIRST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts) ORDER BY ts) AS open_price,
        MAX(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts)) AS high_price,
        MIN(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts)) AS low_price,
        LAST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts) ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price
    FROM ticks

    UNION ALL

    SELECT
        pair_name,
        'M30' AS timeframe,
        date_trunc('minute', ts) - INTERVAL '30 minutes') + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM ts)::int / 30) AS bar_time,
        FIRST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts - INTERVAL '30 minutes') + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM ts)::int / 30) ORDER BY ts) AS open_price,
        MAX(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts - INTERVAL '30 minutes') + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM ts)::int / 30)) AS high_price,
        MIN(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts - INTERVAL '30 minutes') + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM ts)::int / 30)) AS low_price,
        LAST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('minute', ts - INTERVAL '30 minutes') + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM ts)::int / 30) ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price
    FROM ticks

    UNION ALL

    SELECT
        pair_name,
        'H1' AS timeframe,
        date_trunc('hour', ts) AS bar_time,
        FIRST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('hour', ts) ORDER BY ts) AS open_price,
        MAX(price) OVER (PARTITION BY pair_name, date_trunc('hour', ts)) AS high_price,
        MIN(price) OVER (PARTITION BY pair_name, date_trunc('hour', ts)) AS low_price,
        LAST_VALUE(price) OVER (PARTITION BY pair_name, date_trunc('hour', ts) ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price
    FROM ticks
),

dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY pair_name, timeframe, bar_time ORDER BY bar_time) AS rn
    FROM bars
),

clean AS (
    SELECT
        pair_name,
        timeframe,
        bar_time,
        open_price,
        high_price,
        low_price,
        close_price,
        high_price - low_price AS tr
    FROM dedup WHERE rn = 1
)

SELECT
    pair_name,
    timeframe,
    bar_time,
    ROUND(open_price::numeric, 6)  AS open_price,
    ROUND(high_price::numeric, 6)  AS high_price,
    ROUND(low_price::numeric, 6)   AS low_price,
    ROUND(close_price::numeric, 6)   AS close_price,
    ROUND(AVG(open_price) OVER (PARTITION BY pair_name, timeframe ORDER BY bar_time ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)::numeric, 6) AS ema_8,
    ROUND(AVG(open_price) OVER (PARTITION BY pair_name, timeframe ORDER BY bar_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)::numeric, 6) AS ema_21,
    ROUND(AVG(tr) OVER (PARTITION BY pair_name, timeframe ORDER BY bar_time ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)::numeric, 6) AS atr_8,
    ROUND(AVG(tr) OVER (PARTITION BY pair_name, timeframe ORDER BY bar_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)::numeric, 6) AS atr_21,
    CURRENT_TIMESTAMP AS ingested_at
FROM clean
ORDER BY pair_name, timeframe, bar_time;