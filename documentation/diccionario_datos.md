# Diccionario de Datos - Impacto de la IA en Empleos

## Descripción

Este documento describe las columnas, tipos de datos y significado de cada campo
utilizado en el proyecto. Se actualiza cada vez que se incorpora un nuevo dataset.

---

## Dataset Principal

> 📌 Nombre del dataset: *Por definir (Kaggle)*
> 📌 Última actualización: *Por definir*
> 📌 Ruta en el proyecto: `datalake/raw/`

---

## Tabla de Columnas

| Columna | Tipo de dato | Descripción | Ejemplo | Notas |
|---------|-------------|-------------|---------|-------|
| `job_title` | string | Nombre del puesto de trabajo | `Data Analyst` | Puede estar en inglés |
| `company` | string | Nombre de la empresa | `Google` | — |
| `location` | string | Ciudad o país del puesto | `New York, USA` | Puede incluir "Remote" |
| `salary` | float | Salario ofrecido o reportado | `75000.0` | En USD, puede tener nulos |
| `ai_impact_score` | float | Puntuación de riesgo de automatización por IA | `0.87` | Rango: 0.0 (bajo) a 1.0 (alto) |
| `required_skills` | string | Habilidades requeridas para el puesto | `Python, SQL, ML` | Separadas por coma |
| `education_level` | string | Nivel educativo requerido | `Bachelor's` | — |
| `industry` | string | Industria o sector del empleo | `Technology` | — |
| `year` | integer | Año del registro | `2023` | — |
| `employment_type` | string | Tipo de empleo | `Full-time` | Full-time, Part-time, Contract |

---

## Valores Especiales

| Valor | Significado |
|-------|-------------|
| `NaN` / `null` | Dato no disponible o no reportado |
| `-1` | Valor inválido o pendiente de revisión |
| `Unknown` | Categoría no identificada |

---

## Columnas Derivadas (generadas por scripts)

Estas columnas **no vienen del dataset original**, sino que son creadas durante el proceso ETL:

| Columna | Descripción | Script que la genera |
|---------|-------------|----------------------|
| `salary_range` | Categoría de salario (Bajo / Medio / Alto) | `etl.py` |
| `risk_category` | Categoría de riesgo IA (Bajo / Medio / Alto) | `etl.py` |
| `processed_date` | Fecha en que se procesó el registro | `etl.py` |

---

## Notas Generales

- Los datos provienen de Kaggle y son de uso público.
- Antes de usar cualquier columna, verificar si tiene valores nulos con `df.isnull().sum()`.
- Este diccionario debe actualizarse cada vez que se agregue o elimine una columna.

---

> Mantenido por: Gonzzly  
> Repositorio: [topicos-ia-impact-jobs](https://github.com/Gonzzly/topicos-ia-impact-jobs)
