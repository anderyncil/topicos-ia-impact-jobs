#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Workload
Adaptado desde Hive SQL original
Proyecto: topicos-ia-impact-jobs
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Workload')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument(
        '--local_data_path',
        type=str,
        default='file:/home/hadoop/topicos-ia-impact-jobs/datalake/dataset',
        help='Ruta local de datos'
    )
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession con soporte Hive
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Workload-IA-Impact-Jobs"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.sql.legacy.charVarcharCodegen", "true") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    db_name = f"{env}_workload"
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def leer_archivo_csv(spark, ruta_local):
    return spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("inferSchema", "false") \
        .csv(ruta_local)
        
def crear_tabla_external(spark, db_name, table_name, df, location, spark_schema):
    
    df.createOrReplaceTempView(f"tmp_{table_name}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {', '.join([f'{field.name} STRING' for field in spark_schema.fields])}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES(
        'skip.header.line.count'='1'
    )
    """
    spark.sql(create_table_sql)
    
    spark.sql(f"""
        INSERT OVERWRITE TABLE {db_name}.{table_name}
        SELECT * FROM tmp_{table_name}
    """)
    
    print(f"✅ Tabla '{db_name}.{table_name}' desplegada en: {location}")

# =============================================================================
# @section 4. Definición de esquema único (AI Impact Jobs)
# =============================================================================

SCHEMAS = {
    "AI_IMPACT_JOBS": StructType([
        StructField("job_id", StringType(), True),
        StructField("posting_year", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("company_size", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("seniority_level", StringType(), True),
        StructField("ai_mentioned", StringType(), True),
        StructField("ai_keywords", StringType(), True),
        StructField("ai_intensity_score", StringType(), True),
        StructField("core_skills", StringType(), True),
        StructField("ai_skills", StringType(), True),
        StructField("salary_usd", StringType(), True),
        StructField("salary_change_vs_prev_year_percent", StringType(), True),
        StructField("automation_risk_score", StringType(), True),
        StructField("reskilling_required", StringType(), True),
        StructField("ai_job_displacement_risk", StringType(), True),
        StructField("job_description_embedding_cluster", StringType(), True),
        StructField("industry_ai_adoption_stage", StringType(), True)
    ])
}

# =============================================================================
# @section 5. Proceso principal de carga
# =============================================================================

def procesar_tabla(spark, args, db_name, table_name, archivo_datos, esquema):
    
    ruta_local = f"{args.local_data_path}/{archivo_datos}"
    ruta_hdfs = f"{args.base_path}/{args.username}/datalake/{db_name}/{table_name.lower()}"
    
    print(f"📥 Procesando tabla: {table_name} | Archivo: {archivo_datos}")
    
    df = leer_archivo_csv(spark, ruta_local)
    
    crear_tabla_external(spark, db_name, table_name, df, ruta_hdfs, esquema)
    
    print(f"🔍 Muestra de datos - {table_name}:")
    spark.sql(f"SELECT * FROM {db_name}.{table_name} LIMIT 10").show(truncate=False)
    print("-" * 80)

# =============================================================================
# @section 6. Ejecución principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        db_name = crear_database(spark, args.env, args.username, args.base_path)
        
        tablas_config = [
            {"nombre": "AI_IMPACT_JOBS", 
             "archivo": "ai_impact_jobs_2010_2025.data", 
             "schema": SCHEMAS["AI_IMPACT_JOBS"]}
        ]
        
        for config in tablas_config:
            procesar_tabla(
                spark=spark,
                args=args,
                db_name=db_name,
                table_name=config["nombre"],
                archivo_datos=config["archivo"],
                esquema=config["schema"]
            )
        
        print("\n🎉 Proceso completado exitosamente!")
        spark.sql(f"SHOW TABLES IN {db_name}").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error en el proceso: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()