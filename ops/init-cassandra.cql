CREATE KEYSPACE IF NOT EXISTS nyctaxi
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS nyctaxi.rf_predictions (
  month text,
  zone_id int,
  ts_hour timestamp,
  pickups_d double,
  prediction double,
  PRIMARY KEY ((month, zone_id), ts_hour)
) WITH CLUSTERING ORDER BY (ts_hour ASC);

CREATE TABLE IF NOT EXISTS nyctaxi.rf_runs (
  month text,
  split_q double,
  trained_at timestamp,
  num_trees int,
  max_depth int,
  rmse double,
  features list<text>,
  model_uri text,
  PRIMARY KEY ((month), trained_at)
);