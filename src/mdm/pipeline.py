"""Explainable batch MDM demo implemented with PySpark and Delta Lake."""
import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def standardize(df: DataFrame, run_id: str, source_file: str) -> DataFrame:
    required = {"source_system","source_record_id","name","email","phone","address","country_code","updated_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input schema is missing required columns: {sorted(missing)}")

    return (
        df.withColumn("source_system", F.upper(F.trim("source_system")))
          .withColumn("source_record_id", F.trim("source_record_id"))
          .withColumn("name", F.regexp_replace(F.trim("name"), r"\\s+", " "))
          .withColumn("name", F.when(F.col("name") == "", None).otherwise(F.col("name")))
          .withColumn("email_normalized", F.lower(F.trim("email")))
          .withColumn("email_normalized", F.when(F.col("email_normalized") == "", None).otherwise(F.col("email_normalized")))
          .withColumn("phone_normalized", F.regexp_replace(F.coalesce(F.col("phone").cast("string"), F.lit("")), r"[^0-9]", ""))
          .withColumn("phone_normalized", F.when(F.col("phone_normalized") == "", None).otherwise(F.col("phone_normalized")))
          .withColumn("address_normalized", F.lower(F.regexp_replace(F.trim("address"), r"\\s+", " ")))
          .withColumn("country_code", F.upper(F.trim("country_code")))
          .withColumn("updated_at", F.to_timestamp("updated_at"))
          .withColumn("run_id", F.lit(run_id))
          .withColumn("source_file", F.lit(source_file))
          .withColumn("ingested_at", F.current_timestamp())
    )


def evaluate_rules(df: DataFrame, config_path: str):
    """Apply JSON-configured rules; return valid, quarantine and rule-level results."""
    rules = json.loads(Path(config_path).read_text(encoding="utf-8"))["rules"]
    error_conditions, result_frames = [], []

    for rule in rules:
        column, kind = rule["column"], rule["type"]
        if column not in df.columns:
            raise ValueError(f"Rule {rule['rule_id']} references missing column {column}")

        if kind == "not_null":
            passed = F.col(column).isNotNull() & (F.trim(F.col(column).cast("string")) != "")
        elif kind == "email_if_present":
            passed = F.col(column).isNull() | F.col(column).rlike(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$")
        elif kind == "allowed_values":
            passed = F.col(column).isNull() | F.col(column).isin(rule.get("values", []))
        else:
            raise ValueError(f"Unsupported rule type: {kind}")

        result_frames.append(
            df.select("run_id","source_system","source_record_id")
              .withColumn("rule_id", F.lit(rule["rule_id"]))
              .withColumn("rule_status", F.when(passed, "PASS").otherwise("FAIL"))
              .withColumn("reason", F.when(passed, F.lit(None).cast("string"))
                                      .otherwise(F.lit(f"{rule['rule_id']}:{kind}:{column}")))
        )
        if rule.get("severity", "ERROR").upper() == "ERROR":
            error_conditions.append((rule["rule_id"], ~passed))

    reject_expr = F.lit(False)
    for _, condition in error_conditions:
        reject_expr = reject_expr | condition

    reason_parts = [F.when(condition, F.lit(rule_id)) for rule_id, condition in error_conditions]
    checked = df.withColumn("_reject", reject_expr).withColumn(
        "rejection_reason", F.concat_ws(";", *reason_parts)
    )
    valid = checked.filter(~F.col("_reject")).drop("_reject","rejection_reason")
    quarantine = checked.filter(F.col("_reject")).drop("_reject")

    quality_results = result_frames[0]
    for frame in result_frames[1:]:
        quality_results = quality_results.unionByName(frame)
    return valid, quarantine, quality_results


def resolve_identities(valid: DataFrame) -> DataFrame:
    """
    Demo-only deterministic matching. Equal non-empty normalized email or phone links records.
    The minimum source key becomes a batch cluster label. Production needs a persistent
    cross-run identity map and a scalable connected-components implementation.
    """
    records = valid.dropDuplicates(["source_system","source_record_id"])
    keys = records.select(
        "source_system","source_record_id","email_normalized","phone_normalized"
    )
    email_keys = keys.filter(F.col("email_normalized").isNotNull()).select(
        "source_system","source_record_id",
        F.concat(F.lit("EMAIL:"),F.col("email_normalized")).alias("match_key")
    )
    phone_keys = keys.filter(F.col("phone_normalized").isNotNull()).select(
        "source_system","source_record_id",
        F.concat(F.lit("PHONE:"),F.col("phone_normalized")).alias("match_key")
    )
    all_keys = email_keys.unionByName(phone_keys)
    mapping = all_keys.groupBy("match_key").agg(
        F.min(F.concat_ws(":", "source_system","source_record_id")).alias("cluster_key")
    )
    # If a row matches both an email cluster and a phone cluster, choose a deterministic label.
    row_clusters = all_keys.join(mapping, "match_key").groupBy(
        "source_system","source_record_id"
    ).agg(F.min("cluster_key").alias("cluster_key"))
    nodes = records.join(row_clusters, ["source_system","source_record_id"], "left").withColumn(
        "cluster_key",
        F.coalesce(F.col("cluster_key"), F.concat_ws(":", "source_system","source_record_id"))
    )
    return nodes.withColumn("golden_id", F.sha2("cluster_key", 256))


def create_golden_records(resolved: DataFrame, config_path: str) -> DataFrame:
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    attributes = ["name","email_normalized","phone_normalized","address_normalized","country_code"]
    output = None
    for attribute in attributes:
        priorities = cfg.get("attribute_source_precedence", {}).get(
            attribute, cfg.get("default_source_precedence", {})
        )
        pairs = [v for source, rank in priorities.items() for v in (F.lit(source), F.lit(rank))]
        rank_map = F.create_map(*pairs) if pairs else F.create_map(F.lit(""), F.lit(999))
        ranked = resolved.withColumn("_rank", F.coalesce(rank_map[F.col("source_system")], F.lit(999)))
        win = Window.partitionBy("golden_id").orderBy(
            F.when(F.col(attribute).isNull() | (F.trim(F.col(attribute).cast("string")) == ""), 1).otherwise(0),
            F.col("_rank").asc(),
            F.col("updated_at").desc_nulls_last(),
            F.col("source_record_id").asc()
        )
        winner = ranked.withColumn("_rn", F.row_number().over(win)).filter(F.col("_rn") == 1).select(
            "golden_id", F.col(attribute).alias(attribute)
        )
        output = winner if output is None else output.join(winner, "golden_id", "inner")
    return output


def write_delta(df: DataFrame, path: str):
    df.write.format("delta").mode("overwrite").save(path)


def run(args):
    spark = SparkSession.builder.appName("explainable-enterprise-mdm").getOrCreate()
    run_id = str(uuid.uuid4())
    raw = spark.read.option("header", True).option("inferSchema", True).csv(args.input)
    bronze = raw.withColumn("run_id", F.lit(run_id)).withColumn("source_file", F.lit(args.input)).withColumn("ingested_at", F.current_timestamp())
    silver = standardize(raw, run_id, args.input)
    valid, quarantine, quality_results = evaluate_rules(silver, args.rules)
    resolved = resolve_identities(valid)
    golden = create_golden_records(resolved, args.survivorship)

    outputs = {
        "bronze": bronze,
        "silver": silver,
        "valid": valid,
        "quarantine": quarantine,
        "golden": golden,
        "lineage": resolved.select("run_id","source_system","source_record_id","golden_id")
                    .withColumn("match_rule", F.lit("exact email or phone; batch demo")),
        "quality_results": quality_results
    }
    for name, frame in outputs.items():
        write_delta(frame, f"{args.output}/{name}")

    counts = [
        ("source", raw.count()),
        ("bronze", bronze.count()),
        ("silver", silver.count()),
        ("valid", valid.count()),
        ("quarantine", quarantine.count()),
        ("golden", golden.count()),
        ("duplicates_collapsed", valid.count() - golden.count())
    ]
    metrics = spark.createDataFrame(counts, ["stage_name","record_count"]).withColumn(
        "run_id", F.lit(run_id)
    ).withColumn("captured_at", F.current_timestamp())
    write_delta(metrics, f"{args.output}/metrics")
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rules", default="config/quality_rules.json")
    parser.add_argument("--survivorship", default="config/survivorship.json")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
