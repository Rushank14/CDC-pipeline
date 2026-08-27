# Project Postmortem & Architecture Decisions

This document records the actual architectural pivots and debugging sessions encountered while building the pipeline.

## 1. Consumer Uncommitted Batch Loss (The "Zero Rows" Bug)
**Symptoms:** The chaos test `test_chaos.py` would consistently fail with 0 rows in the `silver.dim_orders` warehouse table, even though manual queries verified the pipeline was working.
**Root Cause:** The Python consumer was batching events and only committing the transaction to the Postgres Bronze table when it hit exactly 100 messages. During the chaos test, `docker stop consumer` sent a `SIGTERM` signal. The consumer instantly shut down and rolled back the uncommitted Postgres transaction, completely discarding the inflight messages. By the time `dbt run` kicked in, the `bronze.raw_events` table was legitimately empty.
**Resolution:** Added a 1-second timeout flush to the consumer. If `poll()` times out and there are uncommitted messages, it aggressively commits the partial batch to Postgres to ensure low latency and prevent data loss on sudden crashes.

## 2. Kafka Rebalance Timeout Blocking Reads
**Symptoms:** After fixing the batch bug, the test failed again. The source database had 64 rows, but the warehouse only had 49.
**Root Cause:** The `docker stop` command gives containers a 10-second grace period before `SIGKILL`. Even after changing the test to instantly `docker kill` the generator, the restarted Kafka consumer was taking too long to process the remaining messages. `confluent-kafka` consumers have a default `session.timeout.ms` of 45 seconds. When the crash was simulated, the Kafka broker's group coordinator didn't kick the consumer out immediately; it waited 45 seconds. When the consumer was restarted, it was blocked from re-joining the group and didn't start reading the backlog until *after* `dbt run` had completed.
**Resolution:** Added `session.timeout.ms: 10000` (10 seconds) to the consumer's configuration for faster failure detection, and increased the test script's wait time to 60 seconds to guarantee the new consumer instance successfully completes the group rebalance protocol before running `dbt`.

## 3. The JSONB Null Pointer Trap (The "Phantom Deletes" Bug)
**Symptoms:** The test passed for Inserts and Updates, but failed because 3 rows existed in the warehouse that were supposed to be deleted.
**Root Cause:** PostgreSQL's `replica identity default` setting means that for `DELETE` events, only the Primary Key is included in the Debezium `before` state. Furthermore, the `after` field for a DELETE event is explicitly set to the JSON value `null` (not a SQL `NULL`). In the dbt staging view, the logic `COALESCE(payload->'payload'->'after', payload->'payload'->'before')` failed because PostgreSQL's JSONB operator `->` returns a JSON `null` object instead of a true SQL `NULL`. `COALESCE` completely ignored the `before` object. The parsed `order_id` became blank, the deduplication logic failed, and the delete events were never correctly applied.
**Resolution:** Rewrote the staging extraction logic to explicitly check the JSON type instead of relying on `COALESCE`:
```sql
CASE WHEN jsonb_typeof(payload->'payload'->'after') = 'null' 
     THEN payload->'payload'->'before' 
     ELSE payload->'payload'->'after' 
END as data
```
