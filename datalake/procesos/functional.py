#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script PySpark - Capa FUNCTIONAL
Proyecto: topicos-ia-impact-jobs
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, count, round as spark_round,
    expr
)

# =============================================================================
# 1. Argumentos
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description="Proceso FUNCTIONAL")
    parser.add_argument("--env", type=str, default="TopicosB")
    parser.add_argument("--username", type=str, default="hadoop")
    parser.add_argument("--base_path", type=str, default="/user")
    return parser.parse_args()

# =============================================================================
# 2. Spark Session
# =============================================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Functional-Analytics-IA-Impact-Jobs")
        .enableHiveSupport()
        .getOrCreate()
    )

# =============================================================================
# 3. Main
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()

    try:
        db_source = f"{args.env.lower()}_landing"
        db_target = f"{args.env.lower()}_functional"

        # ---------------------------------------------------------
        # Crear base FUNCTIONAL
        # ---------------------------------------------------------
        spark.sql(f"DROP DATABASE IF EXISTS {db_target} CASCADE")
        spark.sql(
            f"""
            CREATE DATABASE {db_target}
            LOCATION '{args.base_path}/{args.username}/datalake/{db_target}'
            """
        )
        print(f"Base de datos creada: {db_target}")

        # ---------------------------------------------------------
        # Leer datos LANDING
        # ---------------------------------------------------------
        print(f"Leyendo datos desde: {db_source}.AI_IMPACT_JOBS")

        df = spark.table(f"{db_source}.ai_impact_jobs")

        print(f"Registros leídos: {df.count()}")

        # ---------------------------------------------------------
        # Derivar risk_category (SOLUCIÓN CLAVE)
        # ---------------------------------------------------------
        df = df.withColumn(
            "risk_category",
            when(col("automation_risk_score").cast("double") >= 0.66, "HIGH")
            .when(col("automation_risk_score").cast("double") >= 0.33, "MEDIUM")
            .otherwise("LOW")
        )

        # ---------------------------------------------------------
        # Tabla agregada principal
        # ---------------------------------------------------------
        agg_df = (
            df.groupBy(
                "posting_year",
                "country",
                "region",
                "industry",
                "seniority_level",
                "risk_category"
            )
            .agg(
                spark_round(avg(col("salary_usd").cast("double")), 2).alias("avg_salary_usd"),
                spark_round(expr("percentile(salary_usd, 0.5)"), 2).alias("median_salary_usd"),
                spark_round(avg(col("ai_intensity_score").cast("double")), 3).alias("avg_ai_intensity"),
                spark_round(avg(col("automation_risk_score").cast("double")), 3).alias("avg_automation_risk"),
                spark_round(avg(col("ai_job_displacement_risk").cast("double")), 3).alias("avg_displacement_risk"),
                count("*").alias("job_count"),
                spark_round(
                    (count(expr("CASE WHEN ai_mentioned = 'True' THEN 1 END")) / count("*")) * 100,
                    2
                ).alias("pct_ai_mentioned"),
                count("job_title").alias("unique_job_titles")
            )
        )

        # ---------------------------------------------------------
        # Guardar tabla FUNCTIONAL
        # ---------------------------------------------------------
        agg_df.write.mode("overwrite").format("parquet").saveAsTable(
            f"{db_target}.ai_impact_aggregated"
        )

        print("Tabla creada: ai_impact_aggregated")

        spark.sql(f"SHOW TABLES IN {db_target}").show(truncate=False)

        print("\n🎉 FUNCTIONAL ejecutado correctamente")

    except Exception as e:
        print("❌ Error en FUNCTIONAL")
        raise e
    finally:
        spark.stop()

# =============================================================================

if __name__ == "__main__":
    main()