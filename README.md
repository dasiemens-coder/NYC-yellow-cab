# NYC Yellow Cab — Demand Forecasting with PySpark

This project implements an **end-to-end pipeline** in PySpark to forecast hourly taxi demand in New York City.  
It covers data ingestion, feature engineering (including lag and rolling features), and machine learning models (baseline, Linear Regression, Random Forest).  
Future integration with **Apache Kafka** will allow real-time streaming of demand data.

---

## Project structure

---

## Data
- Source: [NYC TLC Trip Record Data](https://www.nyc.gov/assets/tlc/pages/about/tlc-trip-record-data.page)  
- Format: Parquet  
- Period used: January 2015, January–March 2016 (for cross-month evaluation)  

---

## ETL pipeline
1. **00_fetch_raw.py** — downloads raw parquet files  
2. **01_ingest.py** — ingests raw → silver (cleaned parquet)  
3. **02_build_fact.py** — aggregates trips → hourly pickups per zone  
4. **03_build_features.py** — adds calendar features (hour, day of week, weekend flag)  
5. **04_build_lag_features.py** — adds lag features (1, 24, 168 hours) and rolling averages  

Output:  
- **silver/** → cleaned raw  
- **fact/** → demand per zone/hour  
- **gold/** → fact + calendar features  
- **gold_lag/** → gold + lag/rolling features  

---

## Machine Learning
Scripts in `ml/`:

- **01_baseline_naive.py**: naïve lag-24 baseline  
- **02_linear_regression.py**: Linear Regression with lag + calendar + rolling  
- **03_random_forest.py**: Random Forest Regressor with same features  

Metric: **RMSE** (root mean squared error)  
Models show clear improvement over the baseline.  
Feature importance (RF) confirms `lag1`, `lag24`, `lag168` as the strongest predictors.

---

## Visualization
Use `viz/plot_predictions.py` to visualize actual vs predicted demand:

```bash
python viz/plot_predictions.py rf 2015_01 237
python viz/plot_predictions.py lr 2016_03 132

##Typical workflow (example for January 2015):

# ETL
python etl/00_fetch_raw.py 2015_01
python etl/01_ingest.py 2015_01
python etl/02_build_fact.py 2015_01
python etl/03_build_features.py 2015_01
python etl/04_build_lag_features.py 2015_01

# ML
python ml/01_baseline_naive.py 2015_01
python ml/02_linear_regression.py 2015_01
python ml/03_random_forest.py 2015_01

##Future work KAFKA
{ "zone_id": 237, "ts_hour": "2016-03-01T10:00:00", "pickups": 125 }
