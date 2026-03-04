#6. EXPORTAR GOLD TO CSV
import shutil
import os
from pyspark.sql import SparkSession

# Limpiar directorio antes de escribir
output_path_local = "/home/hadoop/topicos-ia-impact-jobs/datalake/temp_csv"
if os.path.exists(output_path_local):
    shutil.rmtree(output_path_local)

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export-Functional-To-AI-Impact-Jobs") \
    .enableHiveSupport() \
    .getOrCreate()

database = "topicosb_functional"
table = "ai_impact_aggregated"

df = spark.table(f"{database}.{table}")

output_path = "file:/home/hadoop/topicos-ia-impact-jobs/datalake/temp_csv"

df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(output_path)

print("Exportación completada")
spark.stop()
