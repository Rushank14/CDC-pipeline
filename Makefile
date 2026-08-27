.PHONY: up down restart-consumer chaos config-debezium dbt-run

up:
	docker compose up -d
	@echo "Waiting for Debezium to be ready..."
	@sleep 10
	@make config-debezium

down:
	docker compose down -v

restart-consumer:
	docker compose restart consumer

config-debezium:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @infrastructure/debezium/postgres-connector.json

chaos:
	python tests/chaos/test_chaos.py

dbt-run:
	docker run --rm --user root --network host -e DB_HOST=localhost -e DB_NAME=warehouse_db -e DB_USER=user -e DB_PASSWORD=password -v $$(pwd)/src/dbt_project:/dbt:z -w /dbt ghcr.io/dbt-labs/dbt-postgres:1.7.3 run --profiles-dir .
