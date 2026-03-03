#6. EXPORTAR GOLD TO CSV 
from pyspark.sql import SparkSession

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export-Functional-To-AI-Impact-Jobs") \
    .enableHiveSupport() \
    .getOrCreate()

# Cambia por tu base y tabla
database = "topicosb_functional"
table = "ai_impact_aggregated"

# Leer tabla Hive
df = spark.table(f"{database}.{table}")

# Ruta dentro de tu proyecto (WSL)
output_path = "file:/home/hadoop/topicos-ia-impact-jobs/datalake/temp_csv"

# Guardar como CSV
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(output_path)

print("Exportación completada")

spark.stop()