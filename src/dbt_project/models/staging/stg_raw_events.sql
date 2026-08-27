{{ config(materialized='view') }}

WITH raw AS (
    SELECT
        offset_id,
        event_time,
        payload->'payload'->>'op' as operation,
        (payload->'payload'->'source'->>'lsn')::bigint as lsn,
        CASE WHEN jsonb_typeof(payload->'payload'->'after') = 'null' 
             THEN payload->'payload'->'before' 
             ELSE payload->'payload'->'after' 
        END as data
    FROM bronze.raw_events
)
SELECT
    offset_id,
    event_time,
    operation,
    lsn,
    (data->>'order_id')::uuid as order_id,
    (data->>'user_id')::uuid as user_id,
    (data->>'amount')::numeric as amount,
    data->>'status' as status,
    to_timestamp((data->>'created_at')::numeric / 1000000.0) as created_at,
    to_timestamp((data->>'updated_at')::numeric / 1000000.0) as updated_at
FROM raw
WHERE data IS NOT NULL
