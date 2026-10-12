-- Illustrative logical schema; adapt catalog/schema and constraints to your platform.

CREATE TABLE mdm_source_record (
  run_id STRING,
  source_system STRING,
  source_record_id STRING,
  source_file STRING,
  ingested_at TIMESTAMP,
  raw_payload STRING
);

CREATE TABLE mdm_standardized_customer (
  run_id STRING,
  source_system STRING,
  source_record_id STRING,
  name STRING,
  email_normalized STRING,
  phone_normalized STRING,
  address_normalized STRING,
  country_code STRING,
  updated_at TIMESTAMP
);

CREATE TABLE mdm_quality_result (
  run_id STRING,
  source_system STRING,
  source_record_id STRING,
  rule_id STRING,
  rule_status STRING,
  reason STRING
);

CREATE TABLE mdm_golden_customer (
  golden_id STRING,
  name STRING,
  email_normalized STRING,
  phone_normalized STRING,
  address_normalized STRING,
  country_code STRING,
  updated_at TIMESTAMP
);

CREATE TABLE mdm_golden_xref (
  golden_id STRING,
  source_system STRING,
  source_record_id STRING,
  match_rule STRING,
  decision STRING,
  run_id STRING
);

CREATE TABLE mdm_run_metric (
  run_id STRING,
  stage_name STRING,
  metric_name STRING,
  metric_value BIGINT,
  captured_at TIMESTAMP
);
