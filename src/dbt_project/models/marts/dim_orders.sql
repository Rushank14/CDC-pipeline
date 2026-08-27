{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        merge_update_columns=['user_id', 'amount', 'status', 'updated_at', '_cdc_lsn', '_cdc_updated_at']
    )
}}

-- Here we prove correctness:
-- 1. We deduplicate within the current batch using ROW_NUMBER() over LSN (Log Sequence Number).
-- 2. We resolve out-of-order events across batches by joining against `this` and strictly enforcing `n._cdc_lsn > t._cdc_lsn`.
-- This guarantees that an older UPDATE arriving late will never overwrite a newer UPDATE that arrived early.

WITH new_events AS (
    SELECT
        order_id,
        user_id,
        amount,
        status,
        created_at,
        updated_at,
        lsn as _cdc_lsn,
        event_time as _cdc_updated_at,
        operation,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC) as rn
    FROM {{ ref('stg_raw_events') }}
    {% if is_incremental() %}
    -- Only scan new events to keep incremental runs fast
    WHERE event_time >= (SELECT COALESCE(MAX(_cdc_updated_at), '1970-01-01') FROM {{ this }})
    {% endif %}
)
SELECT
    n.order_id,
    n.user_id,
    n.amount,
    n.status,
    n.created_at,
    n.updated_at,
    n._cdc_lsn,
    n._cdc_updated_at
FROM new_events n
{% if is_incremental() %}
LEFT JOIN {{ this }} t ON n.order_id = t.order_id
WHERE n.rn = 1 
  -- Out-of-order resolution: only apply if the new event's LSN is strictly greater
  AND (t.order_id IS NULL OR n._cdc_lsn > t._cdc_lsn)
  AND n.operation != 'd'
{% else %}
WHERE n.rn = 1 
  AND n.operation != 'd'
{% endif %}
