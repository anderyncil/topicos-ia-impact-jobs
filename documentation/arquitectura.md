# Arquitectura del Proyecto - Impacto de la IA en Empleos

## Descripción General

Este proyecto analiza el impacto de la inteligencia artificial en el mercado laboral,
utilizando datasets públicos de Kaggle como fuente principal de datos.

---

## Flujo de Datos

```
[Kaggle Dataset]
      │
      ▼
[datalake/raw/]          ← Datos crudos, sin modificar
      │
      ▼
[scripts/etl.py]         ← Limpieza y transformación
      │
      ▼
[datalakehouse/clean/]   ← Datos procesados y listos para análisis
      │
      ▼
[scripts/analisis.py]    ← Análisis y generación de resultados
      │
      ▼
[reports/]               ← Visualizaciones, notebooks y reportes finales
```

---

## Descripción de Capas

### 1. `datalake/` — Capa de datos crudos
- Contiene los archivos descargados directamente de Kaggle sin ninguna modificación.
- Formatos esperados: `.csv`, `.json`, `.xlsx`
- **No se deben modificar estos archivos.**

### 2. `scripts/` — Capa de procesamiento
- Scripts Python encargados de limpiar, transformar y preparar los datos.
- Archivos principales:
  - `etl.py` → Extracción, transformación y carga
  - `analisis.py` → Análisis estadístico y generación de métricas

### 3. `datalakehouse/` — Capa de datos procesados
- Datos limpios y estructurados, listos para análisis o visualización.
- Organizados por temática o fecha de procesamiento.

### 4. `reports/` — Capa de resultados
- Notebooks Jupyter, gráficas y reportes finales del análisis.
- Aquí se presentan las conclusiones del proyecto.

---

## Tecnologías Utilizadas

| Herramienta   | Uso                              |
|---------------|----------------------------------|
| Python        | Lenguaje principal               |
| Pandas        | Manipulación de datos            |
| Matplotlib / Seaborn | Visualización             |
| Jupyter       | Notebooks de análisis            |
| Kaggle API    | Descarga de datasets             |
| Git / GitHub  | Control de versiones             |

---

## Fuentes de Datos

| Dataset | Fuente | Descripción |
|---------|--------|-------------|
| *Por definir* | Kaggle | Dataset sobre empleos e impacto de IA |

> 📌 Actualizar esta tabla cuando se confirme el dataset de Kaggle a utilizar.
