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
---

| Capa        | Formato    | Almacenamiento              | Descripción                                             |
|-------------|------------|-----------------------------|---------------------------------------------------------|
| Workload    | CSV / TEXT | HDFS `/user/hadoop/dataset` | Ingesta del dataset original sin transformaciones       |
| Landing     | AVRO       | HDFS `TOPICOSB_LANDING/`    | Estructuración con esquema AVRO + compresión Snappy     |
| Curated     | Parquet    | HDFS `topicosb_curated/`    | Limpieza, conversión de tipos y normalización           |
| Functional  | Parquet    | HDFS `topicosb_functional/` | Agregaciones estadísticas para análisis                 |
| Gold        | MongoDB    | Colección `gold`            | Resultado final exportado (4,764 documentos)            |

---
## Estructura del Proyecto

```
topicos-ia-impact-jobs
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

## 📊 Dataset

- **Nombre:** AI Impact on Jobs 2010–2025
- **Fuente:** [Kaggle – sarcasmos/ai-society](https://www.kaggle.com/datasets/sarcasmos/ai-society/data)
- **Archivo principal:** `ai_impact_jobs_2010_2025.data`
- **Esquema:** Definido en `datalake/schema/ai_impact_jobs.avsc` (formato AVRO, 22 campos)

### Campos principales

| Campo                              | Tipo    | Descripción                                    |
|------------------------------------|---------|------------------------------------------------|
| `job_id`                           | String  | Identificador único de la oferta               |
| `posting_year`                     | Integer | Año de publicación                             |
| `country` / `region` / `city`      | String  | Ubicación geográfica                           |
| `industry`                         | String  | Sector económico                               |
| `job_title` / `seniority_level`    | String  | Cargo y nivel de seniority                     |
| `ai_mentioned`                     | Boolean | Si la oferta menciona IA                       |
| `ai_intensity_score`               | Double  | Intensidad de IA en la descripción (0–1)       |
| `automation_risk_score`            | Double  | Riesgo de automatización (0–1)                 |
| `salary_usd`                       | Double  | Salario anual en USD                           |
| `ai_job_displacement_risk`         | Double  | Riesgo de desplazamiento laboral por IA        |
| `reskilling_required`              | Boolean | Si se requiere recapacitación                  |
| `industry_ai_adoption_stage`       | String  | Etapa de adopción de IA en la industria        |

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

## Requisitos Previos del Sistema

| Componente | Versión mínima | Notas |
|---|---|---|
| Java (JDK) | 8 u 11 | Requerido por Hadoop y Spark |
| Apache Hadoop | 3.x | HDFS + YARN en modo pseudo-distribuido o cluster |
| Apache Spark | 3.5.0 | Con soporte YARN y PySpark |
| Apache Hive | 3.x | Metastore + HiveServer2 activos |
| Python | 3.8+ | Con `pip` disponible |
| MongoDB | 6.x | Corriendo en `172.17.208.1:27017` |
| Apache Airflow | 2.9.1 | Instalado en entorno virtual `airflow/venv/` |

---

## ✅ Controles de Calidad

El pipeline implementa controles específicos por capa:

- **Workload:** Validación de estructura CSV y esquema de 22 campos.
- **Landing:** Confirmación de tablas Hive en formato AVRO con compresión Snappy.
- **Curated:** Conversión de tipos, normalización de texto (`upper()` + `trim()`), estandarización de booleanos y generación de columna derivada `risk_category` (ALTO / MEDIO / BAJO basado en `automation_risk_score`).
- **Functional:** Verificación de conteo de registros y completitud de migración a MongoDB (4,764 documentos confirmados con `db.gold.countDocuments()`).

---

## 🔗 Repositorio

```
https://github.com/anderyncil/topicos-ia-impact-jobs
```

## 👥 Autores:
```bash
-GONZALES RAICO FRANKY
-GONZALES SANCHEZ LUIS BRUNO
-MOLINA GUERRERO WILSER BLADIMIRO
-VILLANUEVA MENDOZA ROSSELLY
-YNCIL RAMIREZ DIEGO ANDERSON
```


## 📄 Licencia
© Copyright UNC - 2026
Este trabajo está bajo una licencia [Creative Commons Attribution 4.0 Internacional](LICENSE).
