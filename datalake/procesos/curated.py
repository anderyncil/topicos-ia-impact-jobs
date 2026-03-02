#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark - Capa CURATED (Gold) - Limpieza y Validación de Calidad
Proyecto: topicos-ia-impact-jobs
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, length, isnan, isnull,
    regexp_replace, to_date, year, to_timestamp, lit, coalesce
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# =============================================================================
# 1. Argumentos
# =============================================================================
def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso Curated - Limpieza Gold')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base HDFS')
    return parser.parse_args()

# =============================================================================
# 2. Spark Session
# =============================================================================
def create_spark_session(app_name="Curated-Gold-IA-Impact-Jobs"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

# =============================================================================
# 3. Crear base de datos Curated
# =============================================================================
def crear_database(spark, env, username, base_path):
    db_name = f"{env}_curated"
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"Database '{db_name}' creada/asegurada en: {db_location}")
    return db_name

# =============================================================================
# 4. Limpieza y transformación principal
# =============================================================================
def limpiar_y_enriquecer(df):
    df_clean = df \
        .withColumn("job_id", trim(col("job_id"))) \
        .withColumn("posting_year", col("posting_year").cast(IntegerType())) \
        .withColumn("country", upper(trim(col("country")))) \
        .withColumn("region", upper(trim(col("region")))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("company_name", trim(col("company_name"))) \
        .withColumn("company_size", upper(trim(regexp_replace(col("company_size"), r'[^a-zA-Z0-9\s]', '')))) \
        .withColumn("industry", upper(trim(col("industry")))) \
        .withColumn("job_title", trim(col("job_title"))) \
        .withColumn("seniority_level", upper(trim(col("seniority_level")))) \
        .withColumn("ai_mentioned", when(col("ai_mentioned").isin("true", "1", "yes"), lit("YES")).otherwise(lit("NO"))) \
        .withColumn("ai_intensity_score", col("ai_intensity_score").cast(DoubleType())) \
        .withColumn("salary_usd", regexp_replace(col("salary_usd"), "[^0-9.]", "").cast(DoubleType())) \
        .withColumn("salary_change_vs_prev_year_percent", col("salary_change_vs_prev_year_percent").cast(DoubleType())) \
        .withColumn("automation_risk_score", col("automation_risk_score").cast(DoubleType())) \
        .withColumn("ai_job_displacement_risk", col("ai_job_displacement_risk").cast(DoubleType())) \
        .withColumn("reskilling_required", when(col("reskilling_required").isin("true", "1", "yes"), lit("YES")).otherwise(lit("NO"))) \
        # Manejo de nulos en scores críticos
        .withColumn("ai_intensity_score", coalesce(col("ai_intensity_score"), lit(0.0))) \
        .withColumn("automation_risk_score", coalesce(col("automation_risk_score"), lit(0.0))) \
        # Filtro básico de calidad
        .filter(
            (col("posting_year").between(2010, 2025)) &
            (length(col("job_id")) > 5) &
            (~isnull(col("job_title"))) &
            (~isnull(col("industry")))
        ) \
        # Eliminar duplicados por job_id (mantener el más reciente si hay posting_year diferente)
        .dropDuplicates(["job_id"])

    # Agregar columna derivada simple (ejemplo: categoría de riesgo)
    df_enriched = df_clean.withColumn(
        "risk_category",
        when(col("automation_risk_score") >= 0.7, "ALTO")
         .when(col("automation_risk_score").between(0.4, 0.69), "MEDIO")
         .otherwise("BAJO")
    )

    return df_enriched

# =============================================================================
# 5. Crear y cargar tabla Curated (Parquet recomendado)
# =============================================================================
def crear_tabla_curated(spark, db_name, table_name, df, location):
    # Crear tabla externa Parquet
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        job_id STRING,
        posting_year INT,
        country STRING,
        region STRING,
        city STRING,
        company_name STRING,
        company_size STRING,
        industry STRING,
        job_title STRING,
        seniority_level STRING,
        ai_mentioned STRING,
        ai_keywords STRING,
        ai_intensity_score DOUBLE,
        core_skills STRING,
        ai_skills STRING,
        salary_usd DOUBLE,
        salary_change_vs_prev_year_percent DOUBLE,
        automation_risk_score DOUBLE,
        reskilling_required STRING,
        ai_job_displacement_risk DOUBLE,
        job_description_embedding_cluster STRING,
        industry_ai_adoption_stage STRING,
        risk_category STRING
    )
    STORED AS PARQUET
    LOCATION '{location}'
    """
    spark.sql(create_sql)

    # Insertar con overwrite (o partition si agregas posting_year)
    df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
    
    print(f"Tabla '{db_name}.{table_name}' creada y poblada en Parquet")

# =============================================================================
# 6. Main
# =============================================================================
def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        db_curated = crear_database(spark, args.env, args.username, args.base_path)
        db_source = f"{args.env.lower()}_landing"
        
        # Leer desde landing
        source_table = f"{db_source}.AI_IMPACT_JOBS"
        df_raw = spark.table(source_table)
        print(f"Registros leídos de {source_table}: {df_raw.count()}")
        
        # Limpiar y enriquecer
        df_gold = limpiar_y_enriquecer(df_raw)
        print(f"Registros después de limpieza: {df_gold.count()}")
        
        # Mostrar muestra
        df_gold.show(10, truncate=False)
        
        # Guardar en Curated
        location = f"{args.base_path}/{args.username}/datalake/{db_curated}/ai_impact_jobs"
        crear_tabla_curated(spark, db_curated, "AI_IMPACT_JOBS", df_gold, location)
        
        # Verificación final
        spark.sql(f"SELECT * FROM {db_curated}.AI_IMPACT_JOBS LIMIT 5").show(truncate=False)
        
    except Exception as e:
        print(f"Error grave: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()