# 📊 Pipeline de Calidad de Datos - Data Warehouse Académico

## 📌 Descripción

Este proyecto implementa un pipeline de calidad de datos basado en la metodología ETL (Extract, Transform, Load), utilizando tecnologías modernas de procesamiento de datos.

El objetivo es procesar, limpiar, validar e integrar datos académicos provenientes de múltiples fuentes, para almacenarlos en un Data Warehouse en PostgreSQL.

---

## 🏗️ Arquitectura

El pipeline sigue la metodología de Kimball para Data Warehousing:

- **Fuentes de datos**: 13 archivos (CSV, Excel)
- **Procesamiento**: PySpark
- **Almacenamiento intermedio**: Delta Lake
- **Destino final**: PostgreSQL

---

## 📂 Estructura del proyecto
proyecto/
├── datos/ # Datos fuente
├── delta/ # Tablas en Delta Lake
├── drivers/ # Driver JDBC PostgreSQL
├── venv/ # Entorno virtual
├── main.py # Script principal ETL


---

## ⚙️ Tecnologías utilizadas

- Python 3.11
- Apache Spark (PySpark 3.5.3)
- Delta Lake 3.2.0
- PostgreSQL
- Pandas
- OpenPyXL

---

## 🔄 Flujo del Pipeline

1. **Extracción**
   - Lectura de archivos CSV y Excel

2. **Perfilado**
   - Análisis de nulos, tipos y estructura

3. **Validación**
   - Reglas de negocio
   - Integridad referencial
   - Rangos y dominios

4. **Limpieza**
   - Normalización de datos
   - Eliminación de duplicados
   - Corrección de valores

5. **Integración**
   - Construcción de modelo dimensional (Kimball)
   - Tablas de hechos y dimensiones

6. **Carga en Delta Lake**
   - Persistencia en formato ACID

7. **Carga en PostgreSQL**
   - Escritura mediante JDBC

---

## 🧠 Modelo de Datos

- Dimensiones:
  - dim_estudiante
  - dim_carrera_escuela_facultad
  - dim_ciudad_provincia

- Hechos:
  - fact_matricula
  - fact_graduados

---

## 🛠️ Configuración

### Instalar dependencias:

```bash
pip install pyspark==3.5.3 pandas openpyxl

Ejecutar proyecto:
python main.py