
# Default Values
MONTH ?= 2015_01
SPLIT ?= 0.8
ZONE  ?= 237

# Mongo Spark connector for Spark 3.5 / Scala 2.12
PKG = org.mongodb.spark:mongo-spark-connector_2.12:10.3.0

# Prefer venv spark-submit if available
SPARK := $(shell if [ -x .venv/bin/spark-submit ]; then echo .venv/bin/spark-submit; else echo spark-submit; fi)


# ----- Targets -----
.PHONY: all etl baseline run-lr run-rf ml plot mongo-ping clean help

all: up mongo-ping etl baseline run-lr run-rf plot


# ---------- Docker Compose (Mongo) ----------
up:
	@echo "==> Starting Docker Compose (Mongo)..."
	@docker compose up -d
	@$(WAIT_FOR_MONGO)

down:
	@echo "==> Stopping Docker Compose..."
	@$(COMPOSE) down

logs:
	@$(COMPOSE) logs -f


mongo-ping:
	@$(WAIT_FOR_MONGO)

# ---------- ETL ----------
etl:
	@echo "==> Running ETL pipeline for MONTH=$(MONTH)"
	@python etl/00_fetch_raw.py $(MONTH)
	@python etl/01_ingest.py $(MONTH)
	@python etl/02_build_fact.py $(MONTH)
	@python etl/03_build_features.py $(MONTH)
	@python etl/04_build_lag_features.py $(MONTH)

# ---------- Baseline ----------
baseline: etl
	@echo "==> Running Baseline Naive for MONTH=$(MONTH)"
	@python ml/01_baseline_naive.py $(MONTH)

# ---------- ML ----------
ml: run-lr run-rf

run-lr: etl
	@echo "==> Running Linear Regression for MONTH=$(MONTH) SPLIT=$(SPLIT)"
	@$(SPARK) --packages $(PKG) ml/02_linear_regression_mongo_write.py $(MONTH) $(SPLIT)

run-rf: etl
	@echo "==> Running Random Forest for MONTH=$(MONTH) SPLIT=$(SPLIT)"
	@$(SPARK) --packages $(PKG) ml/03_random_forest_mongo_write.py $(MONTH) $(SPLIT)

# ---------- Viz ----------
plot: ml
	@echo "==> Plotting RF vs LR for MONTH=$(MONTH) ZONE=$(ZONE)"
	@$(SPARK) --packages $(PKG) viz/plot_predictions_mongo.py $(MONTH) $(ZONE)

# ---------- Misc ----------
mongo-ping:
	@echo "==> Checking MongoDB on localhost:27017"
	@mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null \
	  && echo "MongoDB reachable ✅" \
	  || (echo "MongoDB not reachable on localhost:27017 ❌"; exit 1)

clean:
	@echo "==> Cleaning local prediction/model parquet outputs"
	@rm -rf data/predictions/rf_* data/predictions/lr_* data/models/rf_* data/models/lr_* || true

help:
	@echo "Targets:"
	@echo "  make all            # ETL -> baseline -> LR -> RF -> plot"
	@echo "  make etl            # run the ETL pipeline"
	@echo "  make baseline       # run baseline naive model"
	@echo "  make run-lr         # run linear regression"
	@echo "  make run-rf         # run random forest"
	@echo "  make ml             # run both LR and RF"
	@echo "  make plot           # render combined plot from Mongo"
	@echo "  make clean          # remove local Parquet/model outputs"
	@echo ""
	@echo "Variables (override on CLI):"
	@echo "  MONTH=YYYY_MM (default: 2015_01)"
	@echo "  SPLIT=float   (default: 0.8)"
	@echo "  ZONE=int      (default: 237)"
	@echo ""
	@echo "Examples:"
	@echo "  make all MONTH=2015_02 ZONE=145"
	@echo "  make run-rf MONTH=2015_03 SPLIT=0.9"