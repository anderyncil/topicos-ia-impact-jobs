#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark - Capa FUNCTIONAL (Analytics) - Enriquecimiento y Métricas
Proyecto: topicos-ia-impact-jobs
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, median, count, when, sum, lit, round, year,
    countDistinct, max, min, expr
)
from pyspark.sql.types import DoubleType

# =============================================================================
# 1. Argumentos
# =============================================================================
def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso Functional - Enriquecimiento y Analytics')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base HDFS')
    return parser.parse_args()

# =============================================================================
# 2. Spark Session
# =============================================================================
def create_spark_session(app_name="Functional-Analytics-IA-Impact-Jobs"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

# =============================================================================
# 3. Crear base de datos Functional
# =============================================================================
def crear_database(spark, env, username, base_path):
    db_name = f"{env}_functional"
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"Database '{db_name}' creada/asegurada en: {db_location}")
    return db_name

# =============================================================================
# 4. Enriquecimiento y agregaciones principales
# =============================================================================
def enriquecer_y_agregar(df):
    # Métricas por país, año e industria (para análisis global de impacto IA)
    df_agg = df.groupBy(
        "posting_year", "country", "region", "industry", "seniority_level", "risk_category"
    ).agg(
        round(avg("salary_usd"), 2).alias("avg_salary_usd"),
        round(median("salary_usd"), 2).alias("median_salary_usd"),
        round(avg("ai_intensity_score"), 3).alias("avg_ai_intensity"),
        round(avg("automation_risk_score"), 3).alias("avg_automation_risk"),
        round(avg("ai_job_displacement_risk"), 3).alias("avg_displacement_risk"),
        count("*").alias("job_count"),
        (sum(when(col("ai_mentioned") == "YES", 1).otherwise(0)) / count("*") * 100).alias("pct_ai_mentioned"),
        countDistinct("job_title").alias("unique_job_titles")
    ).withColumn(
        "salary_premium_ai", 
        when(col("avg_ai_intensity") > 0.5, 
             round((col("avg_salary_usd") / avg("avg_salary_usd").over()) - 1, 3) * 100
        ).otherwise(lit(0.0))
    ).orderBy("posting_year", "country", "industry")

    # Vista global por año (evolución temporal)
    df_yearly = df.groupBy("posting_year").agg(
        round(avg("salary_usd"), 2).alias("global_avg_salary"),
        round(avg("automation_risk_score"), 3).alias("global_avg_risk"),
        round(avg("ai_intensity_score"), 3).alias("global_avg_intensity"),
        count("*").alias("total_jobs"),
        (sum(when(col("ai_mentioned") == "YES", 1).otherwise(0)) / count("*") * 100).alias("global_pct_ai_mentioned")
    ).orderBy("posting_year")

    return df_agg, df_yearly

# =============================================================================
# 5. Crear y cargar tablas Functional (Parquet)
# =============================================================================
def crear_tabla_functional(spark, db_name, table_name, df, location):
    # Crear tabla externa Parquet
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        posting_year INT,
        country STRING,
        region STRING,
        industry STRING,
        seniority_level STRING,
        risk_category STRING,
        avg_salary_usd DOUBLE,
        median_salary_usd DOUBLE,
        avg_ai_intensity DOUBLE,
        avg_automation_risk DOUBLE,
        avg_displacement_risk DOUBLE,
        job_count BIGINT,
        pct_ai_mentioned DOUBLE,
        unique_job_titles BIGINT,
        salary_premium_ai DOUBLE
    )
    STORED AS PARQUET
    LOCATION '{location}'
    """
    spark.sql(create_sql)

    df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
    print(f"Tabla '{db_name}.{table_name}' creada y poblada en Parquet")

def crear_tabla_yearly(spark, db_name, df, location):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.GLOBAL_TRENDS_YEARLY (
        posting_year INT,
        global_avg_salary DOUBLE,
        global_avg_risk DOUBLE,
        global_avg_intensity DOUBLE,
        total_jobs BIGINT,
        global_pct_ai_mentioned DOUBLE
    )
    STORED AS PARQUET
    LOCATION '{location}'
    """
    spark.sql(create_sql)
    df.write.mode("overwrite").saveAsTable(f"{db_name}.GLOBAL_TRENDS_YEARLY")
    print("Tabla GLOBAL_TRENDS_YEARLY creada para evolución temporal")

# =============================================================================
# 6. Main
# =============================================================================
def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        db_functional = crear_database(spark, args.env, args.username, args.base_path)
        db_source = f"{args.env.lower()}_curated"
        
        # Leer desde Curated (Gold)
        source_table = f"{db_source}.AI_IMPACT_JOBS"
        df_raw = spark.table(source_table)
        print(f"Registros leídos de Curated {source_table}: {df_raw.count()}")
        
        # Enriquecer y agregar
        df_agg, df_yearly = enriquecer_y_agregar(df_raw)
        print(f"Registros agregados por dimensión: {df_agg.count()}")
        print(f"Registros globales por año: {df_yearly.count()}")
        
        # Muestras
        df_agg.show(10, truncate=False)
        df_yearly.show(truncate=False)
        
        # Guardar en Functional
        location_agg = f"{args.base_path}/{args.username}/datalake/{db_functional}/ai_impact_aggregated"
        crear_tabla_functional(spark, db_functional, "AI_IMPACT_AGGREGATED", df_agg, location_agg)
        
        location_yearly = f"{args.base_path}/{args.username}/datalake/{db_functional}/global_trends_yearly"
        crear_tabla_yearly(spark, db_functional, df_yearly, location_yearly)
        
        # Verificación final
        spark.sql(f"SELECT * FROM {db_functional}.AI_IMPACT_AGGREGATED LIMIT 5").show(truncate=False)
        spark.sql(f"SELECT * FROM {db_functional}.GLOBAL_TRENDS_YEARLY LIMIT 5").show(truncate=False)
        
    except Exception as e:
        print(f"Error grave: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()