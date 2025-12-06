{{ config(
    materialized='table',
    alias='cdr_data_2',
    schema='prepared_layers'
) }}

WITH ordered AS (
    SELECT
        msisdn,
        tower_id,
        event_datetime AS ts
    FROM wtc_analytics.cdr_data.cdr_records          -- will update according to naming given after ticket is completed
    WHERE tower_id IS NOT NULL
      AND msisdn IS NOT NULL
    ORDER BY msisdn, tower_id, ts
),


gaps AS (
    SELECT
        msisdn,
        tower_id,
        ts,
        CASE 
            WHEN LAG(tower_id) OVER (PARTITION BY msisdn ORDER BY ts) = tower_id
             AND ts - LAG(ts) OVER (PARTITION BY msisdn ORDER BY ts) <= INTERVAL '10 minutes'
            THEN 0
            ELSE 1
        END AS new_session
    FROM ordered
),


sessions AS (
    SELECT
        msisdn,
        tower_id,
        ts,
        SUM(new_session) OVER (PARTITION BY msisdn ORDER BY ts ROWS UNBOUNDED PRECEDING) AS session_id
    FROM gaps
),

with_count AS (
    SELECT
        msisdn,
        tower_id,
        session_id,
        COUNT(*) AS interaction_count,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end
    FROM sessions
    GROUP BY msisdn, tower_id, session_id
)

SELECT
    msisdn,
    tower_id,
    session_start,
    session_end,
    interaction_count,
    CURRENT_TIMESTAMP AS ingested_at
FROM with_count
WHERE interaction_count >= 3
ORDER BY session_start DESC;