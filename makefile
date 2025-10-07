# Makefile for running Spark + MongoDB job

APP ?= ml/03_random_forest_mongo_write.py
PKG = org.mongodb.spark:mongo-spark-connector_2.12:10.3.0

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker logs -f mongo

run:
	spark-submit --packages $(PKG) $(APP)