from pyspark.sql import SparkSession
import pandas as pd
import os
import sys
from pyspark.sql.functions import (
    col, trim, upper, when, count, to_date,
    create_map, lit, regexp_replace
)
from pyspark.sql.window import Window
from itertools import chain
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

# ==============================
# 1. SESION SPARK
# ==============================
spark = (
    SparkSession.builder
    .appName("Pipeline_Delta")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.jars", "drivers/postgresql-42.7.10.jar")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ==============================
# 2. LEER ARCHIVOS
# ==============================
df_ciudades            = spark.read.option("header", True).option("inferSchema", True).csv("datos/ciudades.csv")
df_escuelas            = spark.read.option("header", True).option("inferSchema", True).csv("datos/escuelas.csv")
df_estudiante          = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("datos/estudiante.csv")
df_facultades          = spark.read.option("header", True).option("inferSchema", True).csv("datos/facultades.csv")
df_matricula_aprb_repr = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("datos/matricula_aprb_repr.csv")
df_periodos            = spark.read.option("header", True).option("inferSchema", True).option("sep", ";").csv("datos/periodos.csv")
df_provincias          = spark.read.option("header", True).option("inferSchema", True).csv("datos/provincias.csv")
df_sexo                = spark.read.option("header", True).option("inferSchema", True).csv("datos/sexo.csv")

df_carreras       = spark.createDataFrame(pd.read_excel("datos/carreras.xlsx"))
df_titulo_carrera = spark.createDataFrame(pd.read_excel("datos/titulo_carrera.xlsx"))
df_graduados      = spark.createDataFrame(pd.read_excel("datos/graduados.xlsx"))
df_materia        = spark.createDataFrame(pd.read_excel("datos/materia.xlsx"))
df_proyectos      = spark.createDataFrame(pd.read_excel("datos/proyectos.xlsx"))

# ==============================
# 3. PERFILADO BASICO
# ==============================
for nombre, df in [
    ("CIUDADES", df_ciudades), ("ESCUELAS", df_escuelas), ("CARRERAS", df_carreras),
    ("ESTUDIANTE", df_estudiante), ("FACULTADES", df_facultades), ("GRADUADOS", df_graduados),
    ("MATERIA", df_materia), ("MATRICULA_APRB_REPR", df_matricula_aprb_repr),
    ("PERIODOS", df_periodos), ("PROVINCIAS", df_provincias), ("PROYECTOS", df_proyectos),
    ("SEXO", df_sexo), ("TITULO_CARRERA", df_titulo_carrera),
]:
    print(f"===={nombre}====")
    df.printSchema()
    df.show()
    df.describe().show()

def contar_nulos(df, nombre):
    print(f"=== NULOS {nombre} ===")
    df.select([
        count(when(col(c).isNull() | (trim(col(c).cast("string")) == ""), c)).alias(c)
        for c in df.columns
    ]).show()

for nombre, df in [
    ("CIUDADES", df_ciudades), ("ESCUELAS", df_escuelas), ("FACULTADES", df_facultades),
    ("PROVINCIAS", df_provincias), ("SEXO", df_sexo), ("PERIODOS", df_periodos),
    ("CARRERAS", df_carreras), ("TITULO_CARRERA", df_titulo_carrera), ("MATERIA", df_materia),
    ("MATRICULA_APRB_REPR", df_matricula_aprb_repr), ("GRADUADOS", df_graduados),
    ("PROYECTOS", df_proyectos), ("ESTUDIANTE", df_estudiante),
]:
    contar_nulos(df, nombre)

# ==============================
# MAPA DE PERIODOS NUMERICOS
# ==============================

MAPA_PERIODOS_NUMERICOS = {
    "1":  "P0001", "2":  "P0002", "3":  "P0003", "4":  "P0004",
    "5":  "P0005", "6":  "P0006", "7":  "P0007", "8":  "P0008",
    "9":  "P0009", "10": "R001",  "11": "P0010", "12": "P0011",
    "13": "R002",  "14": "P0012", "15": "P0013", "16": "R003",
    "17": "P0014", "18": "P0015", "19": "P0016", "20": "P0017",
    "21": "P0018", "22": "P0019", "23": "P0020",
}
mapa_expr_periodos = create_map([lit(x) for x in chain(*MAPA_PERIODOS_NUMERICOS.items())])
CODIGOS_PERIODOS_OFICIALES = list(MAPA_PERIODOS_NUMERICOS.values())

def normalizar_col_periodo(column):
    """
    1. trim + cast a string
    2. Si es numerico 1-23 -> mapear al codigo oficial
    3. Si ya tiene formato P/p + digitos -> upper (normaliza p0011 -> P0011)
    4. Cualquier otro valor (900-911) queda como esta
       y es descartado por el filter posterior
    """
    col_str = trim(column.cast("string"))
    return when(
        col_str.isin(list(MAPA_PERIODOS_NUMERICOS.keys())),
        mapa_expr_periodos[col_str]
    ).otherwise(upper(col_str))

# Codigos de las 24 provincias del Ecuador
CODIGOS_PROVINCIAS_ECUADOR = [
    "AZU", "BOL", "CAÑAR", "CARCH", "CHI", "ELORO", "ESM",
    "GAPAG", "GUA", "IMB", "LOJ", "LRIOS", "MANA", "MONAS",
    "NAPO", "ORELA", "PAST", "PIC", "PP", "SAEL", "SUCBI",
    "TUN", "ZAMO", "ZARUM"
]

# ==============================
# 4. VALIDACION
# ==============================

# --- PROVINCIAS ---
w_prov = Window.partitionBy("strCodigo")
df_provincias_validado = (
    df_provincias
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("es_provincia_ecuador_ok",
        when(trim(col("strCodigo")).isin(CODIGOS_PROVINCIAS_ECUADOR), True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
)
print("=== VALIDACION PROVINCIAS ===")
df_provincias_validado.show(50)

# --- SEXO ---
w_sexo = Window.partitionBy("strCodigo")
df_sexo_validado = (
    df_sexo
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_sexo) == 1, True).otherwise(False))
    .withColumn("nombre_dominio_ok",
        when(col("strNombre").isin("FEMENINO", "MASCULINO"), True).otherwise(False))
)
print("=== VALIDACION SEXO ===")
df_sexo_validado.show()

# --- CIUDADES ---
w_ciu = Window.partitionBy("strCodigo")
codigos_provincias_raw = [r[0] for r in df_provincias.select("strCodigo").collect()]
df_ciudades_validado = (
    df_ciudades
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_ciu) == 1, True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
    .withColumn("fk_provincia_ok",
        when(col("strCodProv").isin(codigos_provincias_raw), True).otherwise(False))
)
print("=== VALIDACION CIUDADES ===")
df_ciudades_validado.show()

# --- FACULTADES ---
w_fac = Window.partitionBy("strCodigo")
df_facultades_validado = (
    df_facultades
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_fac) == 1, True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
)
print("=== VALIDACION FACULTADES ===")
df_facultades_validado.show()

# --- ESCUELAS ---
w_esc = Window.partitionBy("strCodigo")
codigos_facultades = [r[0] for r in df_facultades.select("strCodigo").collect()]
df_escuelas_validado = (
    df_escuelas
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_esc) == 1, True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
    .withColumn("fk_facultad_ok",
        when(col("strCodFacultad").isin(codigos_facultades), True).otherwise(False))
)
print("=== VALIDACION ESCUELAS ===")
df_escuelas_validado.show()

# --- CARRERAS ---
w_car = Window.partitionBy("strCodigo")
codigos_escuelas = [r[0] for r in df_escuelas.select("strCodigo").collect()]
df_carreras_validado = (
    df_carreras
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_car) == 1, True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
    .withColumn("fk_escuela_ok",
        when(col("strCodEscuela").isin(codigos_escuelas), True).otherwise(False))
)
print("=== VALIDACION CARRERAS ===")
df_carreras_validado.show()

# --- PERIODOS ---
w_per = Window.partitionBy("strCodigo")
df_periodos_validado = (
    df_periodos
    .withColumn("dtFechaInic_parsed", to_date(col("dtFechaInic"), "d/MM/yy"))
    .withColumn("dtFechaFin_parsed",  to_date(col("dtFechaFin"),  "d/MM/yy"))
    .withColumn("codigo_ok",
        when(col("strCodigo").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("strCodigo").over(w_per) == 1, True).otherwise(False))
    .withColumn("fecha_inic_ok",
        when(col("dtFechaInic").isNotNull(), True).otherwise(False))
    .withColumn("fecha_fin_ok",
        when(col("dtFechaFin").isNotNull(), True).otherwise(False))
    .withColumn("rango_fechas_ok",
        when(col("dtFechaInic_parsed") < col("dtFechaFin_parsed"), True).otherwise(False))
)
print("=== VALIDACION PERIODOS ===")
df_periodos_validado.show()

# --- PROYECTOS ---
w_proy = Window.partitionBy("intCodProyecto")
df_proyectos_validado = (
    df_proyectos
    .withColumn("codigo_ok",
        when(col("intCodProyecto").isNotNull(), True).otherwise(False))
    .withColumn("codigo_unico_ok",
        when(count("intCodProyecto").over(w_proy) == 1, True).otherwise(False))
    .withColumn("tema_ok",
        when(col("txtTema").isNotNull() & (trim(col("txtTema")) != ""), True).otherwise(False))
)
print("=== VALIDACION PROYECTOS ===")
df_proyectos_validado.show()

# --- TITULO_CARRERA ---

codigos_carreras = [r[0] for r in df_carreras.select("strCodigo").collect()]
df_titulo_carrera_validado = (
    df_titulo_carrera
    .withColumn("cod_titulo_ok",
        when(col("COD_TITULO").isNotNull(), True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""), True).otherwise(False))
    .withColumn("fk_carrera_referencial",
        when(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras), True).otherwise(False))
)
print("=== VALIDACION TITULO_CARRERA ===")
df_titulo_carrera_validado.show()

# --- MATERIA ---
# Duplicados: filas completamente iguales en (carrera+codigo+nombre)
w_mat_dup = Window.partitionBy("COD_CARRERA_PROGRAMA", "COD_MATERIA")
df_materia_validado = (
    df_materia
    .withColumn("cod_materia_ok",
        when(col("COD_MATERIA").isNotNull(), True).otherwise(False))
    .withColumn("nombre_ok",
        when(col("MATERIA").isNotNull() & (trim(col("MATERIA")) != ""), True).otherwise(False))
    .withColumn("fk_carrera_ok",
        when(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras), True).otherwise(False))
    .withColumn("duplicado_ok",
        when(count("*").over(w_mat_dup) == 1, True).otherwise(False))
)
print("=== VALIDACION MATERIA ===")
df_materia_validado.show()

# --- MATRICULA_APRB_REPR ---
w_mat = Window.partitionBy("Estudiante", "strCodPeriodo", "COD_CARRERA_PROGRAMA", "strCodMateria")
df_matricula_validado = (
    df_matricula_aprb_repr
    .withColumn("strCodPeriodo_norm", normalizar_col_periodo(col("strCodPeriodo")))
    .withColumn("estudiante_ok",
        when(col("Estudiante").isNotNull() & (col("Estudiante") != 0), True).otherwise(False))
    .withColumn("fk_periodo_ok",
        when(col("strCodPeriodo_norm").isin(CODIGOS_PERIODOS_OFICIALES), True).otherwise(False))
    .withColumn("fk_carrera_ok",
        when(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras), True).otherwise(False))
    .withColumn("materia_ok",
        when(col("strCodMateria").isNotNull(), True).otherwise(False))
    .withColumn("forma_aprob_ok",
        when(col("strCodFormaAprob").isin("PRI", "REP", "CON"), True).otherwise(False))
    .withColumn("duplicado_ok",
        when(count("*").over(w_mat) == 1, True).otherwise(False))
)
print("=== VALIDACION MATRICULA_APRB_REPR ===")
df_matricula_validado.show()

# --- ESTUDIANTE ---
w_est = Window.partitionBy("cedula")
df_estudiante_validado = (
    df_estudiante
    .withColumn("cedula_ok",
        when(col("cedula").isNotNull(), True).otherwise(False))
    .withColumn("cedula_unica_ok",
        when(count("cedula").over(w_est) == 1, True).otherwise(False))
    .withColumn("ciudad_ok",
        when(
            col("strCodCiudadProc").isNotNull() &
            (trim(col("strCodCiudadProc")) != "") &
            (trim(col("strCodCiudadProc")) != "ELCON"),
            True
        ).otherwise(False))
    .withColumn("fecha_nac_ok",
        when(
            col("dtFechaNac").isNotNull() &
            (to_date(col("dtFechaNac").cast("string").substr(1,10), "yyyy-MM-dd") >= "1930-01-01"),
            True
        ).otherwise(False))
)
print("=== VALIDACION ESTUDIANTE ===")
df_estudiante_validado.show()

# --- GRADUADOS ---
codigos_proyectos = [r[0] for r in df_proyectos.select("intCodProyecto").collect()]
w_grad = Window.partitionBy("estudiante", "COD_CARRERA_PROGRAMA", "strCodTitulo")
df_graduados_validado = (
    df_graduados
    .withColumn("estudiante_ok",
        when(col("estudiante").isNotNull(), True).otherwise(False))
    .withColumn("titulo_ok",
        when(col("strCodTitulo").isNotNull(), True).otherwise(False))
    .withColumn("fecha_grado_ok",
        when(col("dtFechaGrado").isNotNull() & (col("dtFechaGrado") >= "1990-01-01"), True).otherwise(False))
    .withColumn("fk_carrera_ok",
        when(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras), True).otherwise(False))
    .withColumn("fk_proyecto_ok",
        when(col("intCodProyecto").isin(codigos_proyectos), True).otherwise(False))
    .withColumn("duplicado_ok",
        when(count("*").over(w_grad) == 1, True).otherwise(False))
)
print("=== VALIDACION GRADUADOS ===")
df_graduados_validado.show()

# ==============================
# 5. LIMPIEZA
# ==============================

# --- PROVINCIAS ---
df_provincias_limpio = (
    df_provincias
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .filter(trim(col("strCodigo")).isin(CODIGOS_PROVINCIAS_ECUADOR))
    .dropDuplicates(["strCodigo"])
)
print("=== PROVINCIAS LIMPIAS ===")
df_provincias_limpio.show(30)

# --- SEXO ---
df_sexo_limpio = (
    df_sexo
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .dropDuplicates(["strCodigo"])
    .filter(col("strNombre").isin("FEMENINO", "MASCULINO"))
)

# --- CIUDADES ---
df_ciudades_limpio = (
    df_ciudades
    .withColumn("strCodigo",  trim(col("strCodigo")))
    .withColumn("strNombre",  trim(upper(col("strNombre"))))
    .withColumn("strCodProv", trim(col("strCodProv")))
    .dropDuplicates(["strCodigo"])
    .filter(col("strCodigo").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
)

# --- FACULTADES ---
df_facultades_limpio = (
    df_facultades
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .dropDuplicates(["strCodigo"])
    .filter(col("strCodigo").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
)

# --- ESCUELAS ---
df_escuelas_limpio = (
    df_escuelas
    .withColumn("strCodigo",      trim(col("strCodigo")))
    .withColumn("strNombre",      trim(upper(col("strNombre"))))
    .withColumn("strCodFacultad", trim(col("strCodFacultad")))
    .dropDuplicates(["strCodigo"])
    .filter(col("strCodigo").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
)

# --- CARRERAS ---
df_carreras_limpio = (
    df_carreras
    .withColumn("strCodigo",     trim(col("strCodigo")))
    .withColumn("strNombre",     trim(upper(col("strNombre"))))
    .withColumn("strCodEscuela", trim(col("strCodEscuela")))
    .dropDuplicates(["strCodigo"])
    .filter(col("strCodigo").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
)
codigos_carreras_limpios = [r[0] for r in df_carreras_limpio.select("strCodigo").collect()]

# --- PERIODOS ---
df_periodos_limpio = (
    df_periodos
    .withColumn("strCodigo",          trim(upper(col("strCodigo"))))
    .withColumn("dtFechaInic_parsed", to_date(col("dtFechaInic"), "d/MM/yy"))
    .withColumn("dtFechaFin_parsed",  to_date(col("dtFechaFin"),  "d/MM/yy"))
    .dropDuplicates(["strCodigo"])
    .filter(col("strCodigo").isNotNull())
    .filter(col("dtFechaInic_parsed").isNotNull())
    .filter(col("dtFechaFin_parsed").isNotNull())
    .filter(col("dtFechaInic_parsed") < col("dtFechaFin_parsed"))
)

# --- PROYECTOS ---
df_proyectos_limpio = (
    df_proyectos
    .withColumn("txtTema", trim(col("txtTema")))
    .dropDuplicates(["intCodProyecto"])
    .filter(col("intCodProyecto").isNotNull())
    .filter(col("txtTema").isNotNull() & (trim(col("txtTema")) != ""))
)

# --- TITULO_CARRERA ---
df_titulo_carrera_limpio = (
    df_titulo_carrera
    .withColumn("COD_TITULO",           trim(upper(col("COD_TITULO"))))
    .withColumn("strNombre",            trim(upper(col("strNombre"))))
    .withColumn("COD_CARRERA_PROGRAMA", trim(upper(col("COD_CARRERA_PROGRAMA"))))
    .dropDuplicates(["COD_TITULO", "COD_CARRERA_PROGRAMA"])
    .filter(col("COD_TITULO").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
)
print("=== TITULO_CARRERA LIMPIO ===")
df_titulo_carrera_limpio.show()

# --- MATERIA ---
df_materia_limpio = (
    df_materia
    .withColumn("COD_MATERIA",          trim(col("COD_MATERIA")))
    .withColumn("MATERIA",              trim(upper(col("MATERIA"))))
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .dropDuplicates(["COD_CARRERA_PROGRAMA", "COD_MATERIA"])
    .filter(col("COD_MATERIA").isNotNull())
    .filter(col("MATERIA").isNotNull() & (trim(col("MATERIA")) != ""))
)

# --- ESTUDIANTE ---
df_estudiante_limpio = (
    df_estudiante
    .withColumn("strNombres",       trim(upper(col("strNombres"))))
    .withColumn("strApellidos",     trim(upper(col("strApellidos"))))
    .withColumn("strCodSexo",       trim(upper(col("strCodSexo"))))
    .withColumn("strCodCiudadProc", trim(col("strCodCiudadProc")))

    # Columna auxiliar: solo letras mayusculas para detectar el patron base
    .withColumn("_nac_base",
        regexp_replace(upper(trim(col("strNacionalidad"))), r"[^A-Z]", "")
    )

    .withColumn("strNacionalidad",
        when(
            col("_nac_base").rlike(r"^ECU[A-Z]*$"),
            lit("ECUATORIANA")
        )
        .when(col("_nac_base") == lit("RIOBAMBA"),    lit("ECUATORIANA"))
        .when(col("_nac_base").like("%AUSTRIAC%"),    lit("AUSTRIACA"))
        .when(col("_nac_base").like("%CHIN%"),        lit("CHINA"))
        .when(col("_nac_base").like("%COLOMB%"),      lit("COLOMBIANA"))
        .otherwise(trim(upper(col("strNacionalidad"))))
    )
    .drop("_nac_base")

    .withColumn("strCodCiudadProc",
        when(
            col("strCodCiudadProc").isNull() |
            (trim(col("strCodCiudadProc")) == "") |
            (trim(col("strCodCiudadProc")) == "ELCON"),
            lit("DES")
        ).otherwise(col("strCodCiudadProc"))
    )

    # Parsear fecha y filtrar absurdos
    .withColumn("_fecha_nac_parsed",
        to_date(col("dtFechaNac").cast("string").substr(1, 10), "yyyy-MM-dd")
    )
    .filter(col("_fecha_nac_parsed").isNotNull())
    .filter(col("_fecha_nac_parsed") >= lit("1930-01-01"))
    .drop("_fecha_nac_parsed")
    .dropDuplicates(["cedula"])

    # Filtros obligatorios
    .filter(col("cedula").isNotNull())
    .filter(col("strCodigo").isNotNull())
    .filter(col("strNombres").isNotNull()   & (trim(col("strNombres"))   != ""))
    .filter(col("strApellidos").isNotNull() & (trim(col("strApellidos")) != ""))
    .filter(col("strCodSexo").isin("FEM", "MAS"))
)
print("=== ESTUDIANTE LIMPIO ===")
df_estudiante_limpio.show()
print("Distribucion de nacionalidades:")
df_estudiante_limpio.groupBy("strNacionalidad").count().orderBy(col("count").desc()).show()

# --- MATRICULA_APRB_REPR ---
df_matricula_limpio = (
    df_matricula_aprb_repr
    .withColumn("strCodPeriodo",        normalizar_col_periodo(col("strCodPeriodo")))
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .withColumn("strCodMateria",        trim(col("strCodMateria")))
    .filter(col("Estudiante").isNotNull() & (col("Estudiante") != 0))
    .filter(col("strCodPeriodo").isin(CODIGOS_PERIODOS_OFICIALES))
    .filter(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras_limpios))
    .filter(col("strCodMateria").isNotNull())
    .filter(col("strCodFormaAprob").isin("PRI", "REP", "CON"))
    .dropDuplicates(["Estudiante", "strCodPeriodo", "COD_CARRERA_PROGRAMA", "strCodMateria"])
)
print("=== MATRICULA LIMPIA ===")
df_matricula_limpio.show()

# --- GRADUADOS ---
df_graduados_limpio = (
    df_graduados
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .withColumn("strCodTitulo",         trim(upper(col("strCodTitulo"))))
    .filter(col("estudiante").isNotNull())
    .filter(col("strCodTitulo").isNotNull())
    .filter(col("dtFechaGrado").isNotNull() & (col("dtFechaGrado") >= "1990-01-01"))
    .filter(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras_limpios))
    .dropDuplicates(["estudiante", "COD_CARRERA_PROGRAMA", "strCodTitulo"])
)

# ==============================
# 6. INTEGRACION
# ==============================

# DIM: Ciudad enriquecida con Provincia
df_ciudad_provincia = (
    df_ciudades_limpio.alias("c")
    .join(df_provincias_limpio.alias("p"),
          col("c.strCodProv") == col("p.strCodigo"), how="left")
    .select(
        col("c.strCodigo").alias("cod_ciudad"),
        col("c.strNombre").alias("ciudad"),
        col("p.strCodigo").alias("cod_provincia"),
        col("p.strNombre").alias("provincia")
    )
)
print("=== DIM CIUDAD-PROVINCIA ===")
df_ciudad_provincia.show()

# DIM: Carrera enriquecida con Escuela y Facultad
df_carrera_escuela_facultad = (
    df_carreras_limpio.alias("car")
    .join(df_escuelas_limpio.alias("esc"),
          col("car.strCodEscuela") == col("esc.strCodigo"), how="left")
    .join(df_facultades_limpio.alias("fac"),
          col("esc.strCodFacultad") == col("fac.strCodigo"), how="left")
    .select(
        col("car.strCodigo").alias("cod_carrera"),
        col("car.strNombre").alias("carrera"),
        col("esc.strCodigo").alias("cod_escuela"),
        col("esc.strNombre").alias("escuela"),
        col("fac.strCodigo").alias("cod_facultad"),
        col("fac.strNombre").alias("facultad")
    )
)
print("=== DIM CARRERA-ESCUELA-FACULTAD ===")
df_carrera_escuela_facultad.show()

# DIM: Estudiante enriquecido con Ciudad y Provincia
df_estudiante_final = (
    df_estudiante_limpio.alias("e")
    .join(
        df_ciudad_provincia.alias("cp"),
        col("e.strCodCiudadProc") == col("cp.cod_ciudad"), how="left"
    )
    .join(
        df_sexo_limpio.select(
            col("strCodigo").alias("strCodSexo"),
            col("strNombre").alias("nombre_sexo")
        ).alias("s"),
        on="strCodSexo", how="left"
    )
    .select(
        col("e.cedula"),
        col("e.strCodigo").alias("cod_estudiante"),
        col("e.strNombres").alias("nombres"),
        col("e.strApellidos").alias("apellidos"),
        col("e.dtFechaNac").alias("fecha_nacimiento"),
        col("e.strNacionalidad").alias("nacionalidad"),
        col("e.strCodSexo").alias("cod_sexo"),
        col("s.nombre_sexo").alias("sexo"),
        col("e.strCodCiudadProc").alias("cod_ciudad_procedencia"),
        col("cp.ciudad").alias("ciudad_procedencia"),
        col("cp.cod_provincia").alias("cod_provincia_procedencia"),
        col("cp.provincia").alias("provincia_procedencia")
    )
)
print("=== DIM ESTUDIANTE FINAL ===")
df_estudiante_final.show()

# FACT: Matricula enriquecida con periodo, carrera, escuela, facultad y materia
df_matricula_final = (
    df_matricula_limpio.alias("m")
    .join(
        df_periodos_limpio.select(
            col("strCodigo").alias("strCodPeriodo"),
            col("dtFechaInic_parsed").alias("fecha_inicio_periodo"),
            col("dtFechaFin_parsed").alias("fecha_fin_periodo")
        ).alias("p"),
        on="strCodPeriodo", how="left"
    )
    .join(
        df_carrera_escuela_facultad.alias("cef"),
        col("m.COD_CARRERA_PROGRAMA") == col("cef.cod_carrera"), how="left"
    )
    .join(
        df_materia_limpio.select(
            col("COD_MATERIA").alias("strCodMateria"),
            col("MATERIA").alias("nombre_materia")
        ).alias("mat"),
        on="strCodMateria", how="left"
    )
    .select(
        col("m.Estudiante").alias("cod_estudiante"),
        col("m.strCodPeriodo").alias("cod_periodo"),
        col("p.fecha_inicio_periodo"),
        col("p.fecha_fin_periodo"),
        col("m.COD_CARRERA_PROGRAMA").alias("cod_carrera"),
        col("cef.carrera"),
        col("cef.cod_escuela"),
        col("cef.escuela"),
        col("cef.cod_facultad"),
        col("cef.facultad"),
        col("m.strCodMateria").alias("cod_materia"),
        col("mat.nombre_materia"),
        col("m.strCodFormaAprob").alias("forma_aprobacion")
    )
)
print("=== FACT MATRICULA FINAL ===")
df_matricula_final.show()

# FACT: Graduados enriquecidos con carrera, titulo y proyecto
df_graduados_final = (
    df_graduados_limpio.alias("g")
    .join(
        df_carrera_escuela_facultad.alias("cef"),
        col("g.COD_CARRERA_PROGRAMA") == col("cef.cod_carrera"), how="left"
    )
    .join(
        df_titulo_carrera_limpio.select(
            col("COD_TITULO").alias("strCodTitulo"),
            col("strNombre").alias("nombre_titulo")
        ).alias("t"),
        on="strCodTitulo", how="left"
    )
    .join(
        df_proyectos_limpio.select(
            col("intCodProyecto").alias("intCodProyecto_proy"),
            col("txtTema").alias("tema_proyecto")
        ).alias("proy"),
        col("g.intCodProyecto") == col("proy.intCodProyecto_proy"), how="left"
    )
    .select(
        col("g.estudiante").alias("cod_estudiante"),
        col("g.COD_CARRERA_PROGRAMA").alias("cod_carrera"),
        col("cef.carrera"),
        col("cef.cod_escuela"),
        col("cef.escuela"),
        col("cef.cod_facultad"),
        col("cef.facultad"),
        col("g.strCodTitulo").alias("cod_titulo"),
        col("t.nombre_titulo"),
        col("g.dtFechaGrado").alias("fecha_graduacion"),
        col("g.intCodProyecto").alias("cod_proyecto"),
        col("proy.tema_proyecto")
    )
)
print("=== FACT GRADUADOS FINAL ===")
df_graduados_final.show()

print("=== PIPELINE COMPLETADO ===")

# ==============================
# 7. GUARDAR EN DELTA LAKE
# ==============================
tablas_delta = [
    ("provincias",                   df_provincias_limpio),
    ("sexo",                         df_sexo_limpio),
    ("ciudades",                     df_ciudades_limpio),
    ("estudiante",                   df_estudiante_limpio),
    ("facultades",                   df_facultades_limpio),
    ("escuelas",                     df_escuelas_limpio),
    ("carreras",                     df_carreras_limpio),
    ("periodos",                     df_periodos_limpio),
    ("proyectos",                    df_proyectos_limpio),
    ("titulo_carrera",               df_titulo_carrera_limpio),
    ("materia",                      df_materia_limpio),
    ("dim_ciudad_provincia",         df_ciudad_provincia),
    ("dim_carrera_escuela_facultad", df_carrera_escuela_facultad),
    ("dim_estudiante",               df_estudiante_final),
    ("fact_matricula",               df_matricula_final),
    ("fact_graduados",               df_graduados_final),
]
for nombre, df in tablas_delta:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"delta/{nombre}")
    print(f"Delta: {nombre} guardada")

print("Delta Lake: todas las tablas guardadas correctamente.")

# ==============================
# 8. GUARDAR EN POSTGRESQL
# ==============================
jdbc_url = "jdbc:postgresql://localhost:5432/dw_academico"
connection_properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

spark.read.format("delta").load("delta/provincias").write.jdbc(
    url=jdbc_url, table="dsa.provincias", mode="overwrite", properties=connection_properties)
print("PostgreSQL: provincias cargada")

spark.read.format("delta").load("delta/sexo").write.jdbc(
    url=jdbc_url, table="dsa.sexo", mode="overwrite", properties=connection_properties)
print("PostgreSQL: sexo cargada")

spark.read.format("delta").load("delta/ciudades").write.jdbc(
    url=jdbc_url, table="dsa.ciudades", mode="overwrite", properties=connection_properties)
print("PostgreSQL: ciudades cargada")

spark.read.format("delta").load("delta/estudiante").write.jdbc(
    url=jdbc_url, table="dsa.estudiante", mode="overwrite", properties=connection_properties)
print("PostgreSQL: estudiante cargada")

spark.read.format("delta").load("delta/facultades").write.jdbc(
    url=jdbc_url, table="dsa.facultades", mode="overwrite", properties=connection_properties)
print("PostgreSQL: facultades cargada")

spark.read.format("delta").load("delta/escuelas").write.jdbc(
    url=jdbc_url, table="dsa.escuelas", mode="overwrite", properties=connection_properties)
print("PostgreSQL: escuelas cargada")

spark.read.format("delta").load("delta/carreras").write.jdbc(
    url=jdbc_url, table="dsa.carreras", mode="overwrite", properties=connection_properties)
print("PostgreSQL: carreras cargada")

spark.read.format("delta").load("delta/periodos").write.jdbc(
    url=jdbc_url, table="dsa.periodos", mode="overwrite", properties=connection_properties)
print("PostgreSQL: periodos cargada")

spark.read.format("delta").load("delta/proyectos").write.jdbc(
    url=jdbc_url, table="dsa.proyectos", mode="overwrite", properties=connection_properties)
print("PostgreSQL: proyectos cargada")

spark.read.format("delta").load("delta/titulo_carrera").write.jdbc(
    url=jdbc_url, table="dsa.titulo_carrera", mode="overwrite", properties=connection_properties)
print("PostgreSQL: titulo_carrera cargada")

spark.read.format("delta").load("delta/materia").write.jdbc(
    url=jdbc_url, table="dsa.materia", mode="overwrite", properties=connection_properties)
print("PostgreSQL: materia cargada")

spark.read.format("delta").load("delta/dim_ciudad_provincia").write.jdbc(
    url=jdbc_url, table="dsa.dim_ciudad_provincia", mode="overwrite", properties=connection_properties)
print("PostgreSQL: dim_ciudad_provincia cargada")

spark.read.format("delta").load("delta/dim_carrera_escuela_facultad").write.jdbc(
    url=jdbc_url, table="dsa.dim_carrera_escuela_facultad", mode="overwrite", properties=connection_properties)
print("PostgreSQL: dim_carrera_escuela_facultad cargada")

spark.read.format("delta").load("delta/dim_estudiante").write.jdbc(
    url=jdbc_url, table="dsa.dim_estudiante", mode="overwrite", properties=connection_properties)
print("PostgreSQL: dim_estudiante cargada")

spark.read.format("delta").load("delta/fact_matricula").write.jdbc(
    url=jdbc_url, table="dsa.fact_matricula", mode="overwrite", properties=connection_properties)
print("PostgreSQL: fact_matricula cargada")

spark.read.format("delta").load("delta/fact_graduados").write.jdbc(
    url=jdbc_url, table="dsa.fact_graduados", mode="overwrite", properties=connection_properties)
print("PostgreSQL: fact_graduados cargada")

print("\nProceso de carga a PostgreSQL terminado.")

spark.stop()