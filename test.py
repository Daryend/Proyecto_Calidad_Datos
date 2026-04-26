import os
import sys
import ctypes

# ==============================
# CARGAR HADOOP DLL ANTES DE SPARK
# ==============================
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
ctypes.CDLL("C:\\hadoop\\bin\\hadoop.dll")
print("hadoop.dll cargado OK")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

# ==============================
# SESION SPARK
# ==============================
spark = (
    SparkSession.builder
    .appName("Carga_PostgreSQL")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.jars", "drivers/postgresql-42.7.10.jar")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ==============================
# CONFIGURACION POSTGRESQL
# ==============================
jdbc_url = "jdbc:postgresql://localhost:5432/dw_academico"
connection_properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

# ==============================
# FUNCION AUXILIAR
# ==============================
def cargar_tabla(nombre_delta, nombre_tabla):
    try:
        df = spark.read.format("delta").load(f"delta/{nombre_delta}")
        df.write.jdbc(
            url=jdbc_url,
            table=f"dsa.{nombre_tabla}",
            mode="overwrite",
            properties=connection_properties
        )
        print(f"[OK] {nombre_tabla} cargada ({df.count()} filas)")
    except Exception as e:
        print(f"[ERROR] {nombre_tabla}: {e}")

# ==============================
# CARGA DE TODAS LAS TABLAS
# ==============================
cargar_tabla("provincias",                  "provincias")
cargar_tabla("sexo",                        "sexo")
cargar_tabla("ciudades",                    "ciudades")
cargar_tabla("facultades",                  "facultades")
cargar_tabla("escuelas",                    "escuelas")
cargar_tabla("carreras",                    "carreras")
cargar_tabla("periodos",                    "periodos")
cargar_tabla("proyectos",                   "proyectos")
cargar_tabla("titulo_carrera",              "titulo_carrera")
cargar_tabla("materia",                     "materia")
cargar_tabla("dim_ciudad_provincia",        "dim_ciudad_provincia")
cargar_tabla("dim_carrera_escuela_facultad","dim_carrera_escuela_facultad")
cargar_tabla("fact_matricula",              "fact_matricula")
cargar_tabla("fact_graduados",              "fact_graduados")

print("\nProceso de carga a PostgreSQL terminado.")
spark.stop()