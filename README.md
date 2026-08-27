# Fault-Tolerant CDC Pipeline

A production-grade Change Data Capture (CDC) pipeline designed to demonstrate advanced data engineering patterns. Rather than focusing on a "happy path" tutorial, this repository is built to survive the chaos of real-world distributed systems: network partitions, consumer crashes, duplicate deliveries, and out-of-order events.

![Architecture Diagram](docs/architecture.png)

## The Core Engineering Problem

Most entry-level data pipelines assume that events arrive precisely once and perfectly in order. In production, this is rarely true. This project implements a robust streaming architecture that explicitly guarantees data integrity under failure.

Key capabilities demonstrated:
1. **Effectively-Once Processing:** Achieves exact-once semantics without the massive overhead of distributed transactions by utilizing deterministic, idempotent ingestion via Kafka offsets.
2. **Out-of-Order Event Resolution:** Discards unreliable wall-clock timestamps in favor of strictly monotonic Postgres Log Sequence Numbers (LSN) to safely merge late-arriving updates without corrupting historical state.
3. **Crash Resilience (Chaos Tested):** Includes an automated chaos suite that physically terminates containers mid-flight to prove zero data loss and zero duplication upon recovery.
4. **Hard Delete Propagation:** Correctly propagates physical deletions from the source database to the analytical warehouse while safely navigating JSONB null-pointer traps in the Debezium WAL envelope.

## Architecture & Data Flow

The infrastructure is orchestrated entirely in Docker and consists of the following flow:
* **Source System:** A simulated transactional PostgreSQL database (`source_db`) generating highly concurrent e-commerce order events (Inserts, Updates, Deletes).
* **CDC Extraction:** Debezium Kafka Connect attaches to a logical replication slot on the source database, reading the Write-Ahead Log (WAL) and emitting JSON events.
* **Message Broker:** Redpanda (a lightweight, Kafka-compatible broker) durably buffers the CDC events.
* **Ingestion Worker:** A highly resilient Python Kafka consumer that batches messages, extracts offsets for deduplication, and commits them transactionally to a Bronze raw layer.
* **Transformation (dbt):** A dbt project that parses the complex Debezium payload envelope, flattens the JSON, and performs an LSN-aware incremental merge into a final Silver domain model.

For a deeper dive into the component roles, please refer to the [Architecture Documentation](docs/architecture.md).

## Documentation & Reading Material

To fully understand the engineering decisions made in this repository, please review the following documents:
* [The Myth of Exactly-Once Delivery](docs/correctness.md) - An explanation of how idempotent merges solve duplicate deliveries and network jitter.
* [Project Postmortem](docs/postmortem.md) - A detailed record of three catastrophic bugs encountered during development (including the "Zero Rows" batch bug and the JSONB Null Pointer Trap) and how they were systematically resolved.
* [dbt Schema Definitions](src/dbt_project/models/schema.yml) - The documented schema of the data warehouse.

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- `make`

### Spin Up the Stack

```bash
make up
```
*Starts the Postgres instances, Redpanda, Debezium, the Synthetic Data Generator, and the Python Consumer. Automatically registers the Debezium connector.*

### Run the Pipeline Transformations

```bash
make dbt-run
```
*Triggers the dbt container to process the bronze raw events and incrementally update the silver domain models.*

### The Proof: Running the Chaos Test

To prove the pipeline's resilience, run the automated chaos test. 

```bash
make chaos
```

**What this does:**
1. Allows the generator to build up transactional load.
2. Violently `SIGKILL`s the Kafka consumer in the middle of processing a batch.
3. Allows the source database to continue accepting transactions while the pipeline is broken.
4. Restarts the consumer, forces a Kafka group rebalance, and allows it to clear the backlog.
5. Executes `dbt run`.
6. Performs a row-by-row cryptographic assertion between the Source database and the Warehouse database to prove they perfectly mirror each other.

### Teardown

```bash
make down
```

## Production Considerations

While this pipeline demonstrates core CDC logic, certain architectural compromises were made to ensure it can run entirely on a local laptop via `docker-compose`:
- **High Availability:** Redpanda and Postgres are running as single nodes. In production, these would be highly available, multi-AZ clusters.
- **Orchestration:** dbt is executed manually or via the test script. In a live environment, the raw ingestion would stream continuously, and dbt would be orchestrated on a micro-batch schedule (e.g., via Airflow or Dagster) or replaced by a continuous streaming engine like Apache Flink.
- **Schema Registry:** To reduce the local footprint, this setup relies on raw JSON. In an enterprise environment, utilizing Avro or Protobuf combined with a Confluent Schema Registry is critical for safely managing upstream schema evolution.
