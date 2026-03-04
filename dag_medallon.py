"""
DAG: etl_ai_impact_jobs_pipeline
Proyecto: topicos-ia-impact-jobs
Pipeline: workload → landing → functional → curated → gold_csv → gold_mongo
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =============================================================================
# Configuración base
# =============================================================================

REPO     = "/home/hadoop/topicos-ia-impact-jobs"
PROCESOS = f"{REPO}/datalake/procesos"
SCRIPTS  = f"{REPO}/scripts"
VENV     = "/home/hadoop/venv/bin"
ENV      = "TopicosB"
USERNAME = "hadoop"
BASE     = "/user"

# Conector MongoDB para Spark
MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

# MongoDB local en WSL (siempre funciona sin importar IP)
MONGO_URI = "mongodb://127.0.0.1:27017/"

# Ruta del schema AVRO en HDFS
SCHEMA_HDFS = f"{BASE}/{USERNAME}/datalake/schema"

# =============================================================================
# Argumentos por defecto del DAG
# =============================================================================

default_args = {
    "owner"           : "hadoop",
    "depends_on_past" : False,
    "start_date"      : datetime(2026, 3, 1),
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=2),
}

# =============================================================================
# Definición del DAG
# =============================================================================

with DAG(
    dag_id              = "etl_ai_impact_jobs_pipeline",
    default_args        = default_args,
    description         = "Pipeline ETL Medallón - AI Impact Jobs",
    schedule_interval   = None,          # Solo manual (Trigger)
    catchup             = False,
    tags                = ["etl", "spark", "hive", "mongodb", "topicos"],
) as dag:

    # -------------------------------------------------------------------------
    # TAREA 0: Iniciar servicios Hadoop + Hive
    # -------------------------------------------------------------------------
    iniciar_hadoop = BashOperator(
        task_id = "iniciar_hadoop",
        bash_command = """
            echo "🚀 Iniciando servicios Hadoop..."
            # Iniciar HDFS si no está corriendo
            if ! hdfs dfsadmin -report > /dev/null 2>&1; then
                start-dfs.sh
                sleep 10
            fi
            # Iniciar YARN si no está corriendo
            if ! yarn node -list > /dev/null 2>&1; then
                start-yarn.sh
                sleep 5
            fi
            echo "✅ Hadoop listo"
            hdfs dfs -mkdir -p /user/hadoop/datalake/schema
            hdfs dfs -put -f /home/hadoop/topicos-ia-impact-jobs/datalake/schema/ai_impact_jobs.avsc \
                /user/hadoop/datalake/schema/ || true
            echo "✅ Schema AVRO subido a HDFS"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "PATH": "/opt/hadoop/bin:/opt/hadoop/sbin:/opt/hive/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
    )

    # -------------------------------------------------------------------------
    # TAREA 1: capa_workload
    # -------------------------------------------------------------------------
    capa_workload = BashOperator(
        task_id = "capa_workload",
        bash_command = f"""
            echo "📦 Iniciando capa WORKLOAD..."
            spark-submit \
                --master yarn \
                --deploy-mode client \
                --conf spark.sql.legacy.charVarcharCodegen=true \
                {PROCESOS}/workload.py \
                --env {ENV} \
                --username {USERNAME} \
                --base_path {BASE} \
                --local_data_path file:/home/hadoop/topicos-ia-impact-jobs/datalake/dataset
            echo "✅ WORKLOAD completado"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # TAREA 2: capa_landing
    # -------------------------------------------------------------------------
    capa_landing = BashOperator(
        task_id = "capa_landing",
        bash_command = f"""
            echo "🛬 Iniciando capa LANDING..."
            spark-submit \
                --master yarn \
                --deploy-mode client \
                --packages org.apache.spark:spark-avro_2.12:3.5.0 \
                {PROCESOS}/landing.py \
                --env {ENV} \
                --username {USERNAME} \
                --base_path {BASE} \
                --schema_path {SCHEMA_HDFS} \
                --source_db workload
            echo "✅ LANDING completado"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # TAREA 3: capa_functional
    # -------------------------------------------------------------------------
    capa_functional = BashOperator(
        task_id = "capa_functional",
        bash_command = f"""
            echo "⚙️  Iniciando capa FUNCTIONAL..."
            spark-submit \
                --master yarn \
                --deploy-mode client \
                {PROCESOS}/functional.py \
                --env {ENV} \
                --username {USERNAME} \
                --base_path {BASE}
            echo "✅ FUNCTIONAL completado"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # TAREA 4: capa_curated
    # -------------------------------------------------------------------------
    capa_curated = BashOperator(
        task_id = "capa_curated",
        bash_command = f"""
            echo "🧹 Iniciando capa CURATED..."
            spark-submit \
                --master yarn \
                --deploy-mode client \
                --packages org.apache.spark:spark-avro_2.12:3.5.0 \
                {PROCESOS}/curated.py \
                --env {ENV} \
                --username {USERNAME} \
                --base_path {BASE}
            echo "✅ CURATED completado"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=15),
    )

    # -------------------------------------------------------------------------
    # TAREA 5: export_gold_csv
    # Exporta functional a CSV local para que MongoDB lo pueda leer
    # -------------------------------------------------------------------------
    export_gold_csv = BashOperator(
        task_id = "export_gold_csv",
        bash_command = f"""
            echo "📤 Exportando Gold a CSV local..."
            spark-submit \
                --master yarn \
                --deploy-mode client \
                {SCRIPTS}/migration_data_mongodb.py
            echo "✅ CSV exportado a local"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=10),
    )

    # -------------------------------------------------------------------------
    # TAREA 6: export_gold_mongo
    # Usa --master local[*] para que el worker pueda acceder a MongoDB local
    # -------------------------------------------------------------------------
    export_gold_mongo = BashOperator(
        task_id = "export_gold_mongo",
        bash_command = f"""
            echo "🍃 Exportando Gold a MongoDB..."
            spark-submit \
                --master local[*] \
                --packages {MONGO_CONNECTOR} \
                {SCRIPTS}/conexion_mongodb.py
            echo "✅ EXPORT MONGODB completado"
        """,
        env = {"JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
               "HADOOP_HOME": "/opt/hadoop",
               "HIVE_HOME": "/opt/hive",
               "SPARK_HOME": "/opt/spark",
               "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
               "PATH": "/opt/spark/bin:/opt/hadoop/bin:/opt/hive/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
               "PYSPARK_PYTHON": "/usr/bin/python3"},
        execution_timeout = timedelta(minutes=10),
    )

    # -------------------------------------------------------------------------
    # Definir secuencia del pipeline
    # -------------------------------------------------------------------------
    iniciar_hadoop >> capa_workload >> capa_landing >> capa_functional >> capa_curated >> export_gold_csv >> export_gold_mongo
