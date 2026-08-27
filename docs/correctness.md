# Correctness Guarantees

## The Myth of Exactly-Once Processing
In distributed systems, true "exactly-once" delivery is often a myth or requires extremely heavy coordination mechanisms (like Kafka Transactions coupled with Flink checkpoints). When network partitions happen, a producer must retry sending a message. If the first message actually succeeded but the acknowledgment was lost, the broker receives the message twice.

Because we cannot guarantee exactly-once *delivery*, this pipeline is designed to achieve **effectively-once processing** through **idempotent operations**. No matter how many times an event is redelivered, the final state of the warehouse remains perfectly consistent.

## 1. Handling Duplicate Delivery
**The Problem:** The Kafka consumer reads a batch of messages, writes them to the database, but crashes before committing its offset to Redpanda. Upon restart, it will read the same batch again.

**The Solution:** 
At the ingestion layer (Python to Bronze), we extract the Kafka `topic`, `partition`, and `offset`. We use this composite string as the primary key `offset_id` in the `bronze.raw_events` table. 
We perform an `INSERT ... ON CONFLICT (offset_id) DO NOTHING`. This guarantees that even if a batch is replayed 1,000 times, the events only land in the raw table exactly once.

## 2. Handling Out-of-Order Events
**The Problem:** Due to network jitter, concurrent processing, or historical backfills, an `UPDATE` event for an order might arrive *before* the initial `INSERT` event. If a naive UPSERT is used, the `INSERT` might later overwrite the newer `UPDATE`, corrupting the data back in time.

**The Solution:**
Wall-clock time is unreliable. Instead, we rely on the Postgres Log Sequence Number (`LSN`), which Debezium attaches to every event. The LSN represents the absolute byte-offset in the WAL, giving us strict causal ordering.

In the dbt `silver` layer, our incremental merge logic does two things:
1. Within a single batch, if multiple events exist for the same `order_id`, it uses `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC)` to pick only the latest state.
2. Across batches, when merging into the target table, it joins the incoming events against the existing target table and enforces `n._cdc_lsn > t._cdc_lsn`. An event is only applied if its transaction happened *after* the one currently stored in the warehouse.

## 3. Handling Backfills
**The Problem:** Reprocessing a historical table snapshot while the live streaming pipeline is running can cause massive out-of-order collisions.

**The Solution:** Because our pipeline relies purely on LSN for ordering and `offset_id` for deduplication, backfilling is inherently safe. You can drop the `bronze` table and reset the Kafka offset, and the pipeline will rebuild the `silver` state exactly as it was, silently discarding any stale data that attempts to overwrite newer state.
