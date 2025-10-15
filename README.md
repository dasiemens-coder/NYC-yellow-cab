# NYC Yellow Cab — Demand Forecasting with PySpark

This project implements an **end-to-end pipeline** in PySpark to forecast hourly taxi demand in New York City.  
It covers data ingestion, feature engineering (including lag and rolling features), and machine learning models (baseline, Linear Regression, Random Forest).  

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

## Persiting and Visualization
The outputs from `02_linear_regression.py` and `03_random_forest.py` get persisted in MongoDB. To simply the setup, MongoDB runs in a docker container. `viz/plot_predictions.py` then plots the results. 

---
## Execution Instructions

This project requires **Python 3.13.x**. It is recommended to use a virtual environment for isolation.  

### Prerequisites
1. Ensure **mongosh** is installed on your machine.  
2. Verify that **Docker** is running.  
3. Install the required dependencies by running:  
    ```bash
    pip install -r requirements.txt
    ```

### Makefile Instructions

The project includes a `Makefile` to simplify execution. Below are the details for the `make all` target and the `ZONE` and `MONTH` parameters:

#### `make all`
This target runs the entire pipeline for a specified month and zone. It includes ETL, feature engineering, and machine learning steps.

#### Parameters
- `ZONE`: The taxi zone ID for which the pipeline will be executed. (Default `237`)
    - A description of the zones can be found [here](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
- `MONTH`: The month in `YYYY_MM` format (Default `2015_01`)
    - For this project, the valid range is from `2016_01` (January 2016) to `2016_12` (December 2016), and `2015_01` (January 2015). 

#### Example Usage
```bash
make all ZONE=237 MONTH=2015_01
```
This command runs the pipeline for zone `237` and January 2015.
