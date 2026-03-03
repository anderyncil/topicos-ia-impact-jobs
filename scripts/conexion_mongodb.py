# 7. conexión mongo 
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

database='ai_impact_jobs'
collection='gold'

try:
    # Configuración explícita de Spark
    conf = SparkConf() \
        .set("spark.mongodb.connection.uri", "mongodb://172.17.208.1:27017/") \
        .set("spark.mongodb.database", database) \
        .set("spark.mongodb.collection", collection) \
        .set("spark.mongodb.write.mode", "overwrite") \
        .set("spark.mongodb.output.autoIndexCreation", "true")

    # Crear sesión Spark con paquetes MongoDB
    spark = SparkSession.builder \
        .appName("TopicosIAImpactJobs_CSV_to_MongoDB_Gold") \
        .config(conf=conf) \
        .getOrCreate()

    logger.info("✅ Sesión Spark creada exitosamente")

            # Leer CSV con manejo de errores
    csv_path = "file:/home/hadoop/topicos-ia-impact-jobs/datalake/temp_csv/part-*.csv"
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(csv_path)

    logger.info(f"✅ CSV leído desde: {csv_path}")

    # Validar que el DataFrame no esté vacío
    record_count = df.count()
    logger.info(f"📊 Registros leídos: {record_count}")
    
    if record_count == 0:
        raise ValueError("⚠️ El DataFrame está vacío. Verifica el archivo CSV.")

    # Mostrar esquema y muestra de datos (debug)
    logger.info("🔍 Esquema del DataFrame:")
    df.printSchema()
    logger.info("🔍 Primeras 5 filas:")
    df.show(5, truncate=False)

    # Escribir en MongoDB
    logger.info(f"💾 Escribiendo en MongoDB: {database}.{collection} ...")
    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.database", database) \
        .option("spark.mongodb.collection", collection) \
        .option("spark.mongodb.write.mode", "overwrite") \
        .save()

    logger.info("✅ Datos escritos exitosamente en MongoDB")

    # Verificación opcional: contar documentos en MongoDB
    df_verify = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.database", database) \
        .option("spark.mongodb.collection", collection) \
        .load()
    
    logger.info(f"🔍 Verificación: {df_verify.count()} documentos en {database}.{collection}")

except Exception as e:
    logger.error(f"❌ Error durante la ejecución: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
    logger.info("🛑 Sesión Spark cerrada")


