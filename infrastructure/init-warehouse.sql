CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze table stores raw Kafka events exactly as they arrive.
-- offset_id (topic-partition-offset) ensures we deduplicate at the ingestion layer.
CREATE TABLE IF NOT EXISTS bronze.raw_events (
    offset_id VARCHAR(255) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
