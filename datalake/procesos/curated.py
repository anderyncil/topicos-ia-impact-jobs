#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CURATED - Limpieza y estandarización
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, regexp_replace, lit
)
from pyspark.sql.types import IntegerType, DoubleType

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', default='TopicosB')
    parser.add_argument('--username', default='hadoop')
    parser.add_argument('--base_path', default='/user')
    return parser.parse_args()

def create_spark():
    return SparkSession.builder \
        .appName("Curated-AI-Impact-Jobs") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    args = parse_arguments()
    spark = create_spark()

    try:
        db_curated = f"{args.env.lower()}_curated"
        db_landing = f"{args.env.lower()}_landing"

        spark.sql(f"DROP DATABASE IF EXISTS {db_curated} CASCADE")
        spark.sql(f"""
            CREATE DATABASE {db_curated}
            LOCATION '{args.base_path}/{args.username}/datalake/{db_curated}'
        """)

        df = spark.table(f"{db_landing}.ai_impact_jobs")

        df_clean = (
            df
            .withColumn("posting_year", col("posting_year").cast(IntegerType()))
            .withColumn("country", upper(trim(col("country"))))
            .withColumn("region", upper(trim(col("region"))))
            .withColumn("industry", upper(trim(col("industry"))))
            .withColumn("seniority_level", upper(trim(col("seniority_level"))))
            .withColumn(
                "ai_mentioned",
                when(col("ai_mentioned").isin("True", "true", "1"), "YES").otherwise("NO")
            )
            .withColumn(
                "reskilling_required",
                when(col("reskilling_required").isin("True", "true", "1"), "YES").otherwise("NO")
            )
            .withColumn("salary_usd", col("salary_usd").cast(DoubleType()))
            .withColumn("ai_intensity_score", col("ai_intensity_score").cast(DoubleType()))
            .withColumn("automation_risk_score", col("automation_risk_score").cast(DoubleType()))
            .withColumn("ai_job_displacement_risk", col("ai_job_displacement_risk").cast(DoubleType()))
            .withColumn(
                "risk_category",
                when(col("automation_risk_score") >= 0.7, "ALTO")
                .when(col("automation_risk_score") >= 0.4, "MEDIO")
                .otherwise("BAJO")
            )
        )

        df_clean.write.mode("overwrite").saveAsTable(
            f"{db_curated}.ai_impact_jobs"
        )

        spark.sql(f"SELECT COUNT(*) FROM {db_curated}.ai_impact_jobs").show()

    except Exception as e:
        print("❌ Error CURATED:", e)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()