# PROYECTO DE IMPACTO GLOBAL DE LA IA EN EL EMPLEO

## Descripcion
Análisis del impacto global de la Inteligencia Artificial en el mercado laboral durante el periodo 2010–2025. El proyecto implementa una arquitectura de Data Lake por capas (Workload → Landing → Curated → Functional → Gold) procesada con Apache Spark, almacenada en HDFS, gestionada con Hive y exportada a MongoDB. Los resultados finales se visualizan mediante reportes en Power BI.

---

## Arquitectura del Pipeline

```
Dataset (.data / Avro)
        │
        ▼
  ┌─────────────┐
  │  WORKLOAD   │  ← Ingestión raw desde HDFS
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   LANDING   │  ← Serialización Avro + Snappy
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   CURATED   │  ← Limpieza y validación (Parquet)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │ FUNCTIONAL  │  ← Transformaciones analíticas (Parquet)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │    GOLD     │  ← Exportación CSV + Carga MongoDB
  └──────┬──────┘
         │
         ▼
      Power BI
```

## Estructura del Proyecto

```
/
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
├── documentation/
│   └── informe.pdf
├── reports/
│   └── powerbi.pdf
├── scripts/
│   ├── conexion_mongodb.py
│   └── migration_data_mongodb.py
├── datalake/
│   ├── gold.csv
│   ├── info.txt
│   ├── temp_csv/
│   │   ├── _SUCCESS
│   │   └── part-00000-....csv
│   ├── data/
│   │   └── dataset/
│   │       └── ai_impact_jobs_2010_2025.data
│   ├── procesos/
│   │   ├── landing.py
│   │   ├── curated.py
│   │   ├── workload.py
│   │   └── functional.py
│   └── schema/
│       └── ai_impact_jobs.avsc
├── datalakehouse/
│   ├── info.txt
│   └── pyspark.ipynb
└── airflow/
    └── venv/
```

---

## Fuente de Datos

**Kaggle:** [AI Society – Impact on Jobs 2010–2025](https://www.kaggle.com/datasets/sarcasmos/ai-society/data)

### Schema del Dataset (`ai_impact_jobs_2010_2025.data`)

| Campo | Tipo |
|---|---|
| `job_id` | string / null |
| `posting_year` | string / null |
| `country` | string / null |
| `region` | string / null |
| `city` | string / null |
| `company_name` | string / null |
| `company_size` | string / null |
| `industry` | string / null |

El schema completo se encuentra en `datalake/schema/ai_impact_jobs.avsc`.

---

## Tecnologías Utilizadas

| Herramienta | Uso |
|---|---|
| Apache Hadoop (HDFS + YARN) | Almacenamiento distribuido y gestión de recursos |
| Apache Spark (PySpark) | Procesamiento por capas del Data Lake |
| Apache Hive | Metastore y HiveServer2 |
| Apache Avro | Formato de serialización (capa Landing) |
| Apache Parquet (Snappy) | Formato columnar comprimido (capas Curated/Functional) |
| MongoDB | Almacenamiento NoSQL de la capa Gold |
| Apache Airflow | Orquestación del pipeline |
| Power BI | Visualización de resultados |
| Python 3 | Scripts de migración y conexión |

---

## Prerrequisitos

- Hadoop 3.x con HDFS y YARN configurados
- Apache Spark 3.5.x con soporte para Avro y MongoDB Connector
- Apache Hive con Metastore y HiveServer2
- MongoDB corriendo en `172.17.208.1:27017`
- Python 3.x con las dependencias de `requirements.txt`
- Apache Airflow (con entorno virtual en `airflow/venv/`)

---

Autores:
```bash
-GONZALES RAICO FRANKY
-GONZALES SANCHEZ LUIS BRUNO
-MOLINA GUERRERO WILSER BLADIMIRO
-VILLANUEVA MENDOZA ROSSELLY
-YNCIL RAMIREZ DIEGO ANDERSON
```



© Copyright UNC - 2026
Este trabajo está bajo una licencia [Creative Commons Attribution 4.0 Internacional](LICENSE).
