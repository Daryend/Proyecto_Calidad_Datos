# =============================================================================
# PIPELINE ETL - DSA (Data Staging Area) - Universidad ESPOCH
# Flujo: RAW → Perfilado → Validación → Limpieza → Delta Lake → PostgreSQL DSA
# Metodología: Kimball | Versión: 3.0
# =============================================================================

from pyspark.sql import SparkSession
import pandas as pd
import os, sys, re
from pyspark.sql.functions import (
    col, trim, upper, when, count, to_date,
    create_map, lit, regexp_replace, concat, date_format
)
from pyspark.sql.window import Window
from itertools import chain
import ctypes

# ── Entorno Windows ──────────────────────────────────────────────────────────
os.environ["HADOOP_HOME"]           = "C:\\hadoop"
os.environ["PATH"]                  = "C:\\hadoop\\bin;" + os.environ["PATH"]
ctypes.CDLL("C:\\hadoop\\bin\\hadoop.dll")
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ── Sesión Spark ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ETL_DSA_ESPOCH")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.jars",          "drivers/postgresql-42.7.10.jar")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
print("[OK] Spark iniciado")

# =============================================================================
# CONSTANTES
# =============================================================================

# Mapa de periodos: número ordinal → código oficial
# Los periodos 900-911 no tienen equivalente y se descartan
MAPA_PERIODOS = {
    "1":"P0001","2":"P0002","3":"P0003","4":"P0004","5":"P0005",
    "6":"P0006","7":"P0007","8":"P0008","9":"P0009","10":"R001",
    "11":"P0010","12":"P0011","13":"R002","14":"P0012","15":"P0013",
    "16":"R003","17":"P0014","18":"P0015","19":"P0016","20":"P0017",
    "21":"P0018","22":"P0019","23":"P0020",
}
mapa_expr_periodos = create_map([lit(x) for x in chain(*MAPA_PERIODOS.items())])
CODIGOS_PERIODO_OK = list(MAPA_PERIODOS.values())

# Códigos DPA de las 24 provincias Ecuador (verificados contra DPA INEC 2024)
PROVINCIAS_EC = [
    "AZU","BOL","CAÑAR","CARCH","CHI","ELORO","ESM","GAPAG","GUA","IMB",
    "LOJ","LRIOS","MANA","MONAS","NAPO","ORELA","PAST","PIC","PP","SAEL",
    "SUCBI","TUN","ZAMO","ZARUM",
]

# Dummies Kimball para valores desconocidos
DUMMY_FECHA    = lit("1900-01-01")
DUMMY_CIUDAD   = lit("DES")
DUMMY_APELLIDO = lit("DESCONOCIDO")

# =============================================================================
# FUNCIONES
# =============================================================================

def normalizar_periodo(c):
    """Numérico 1-23 → código oficial. p0011 → P0011. 900-911 sin cambio."""
    s = trim(c.cast("string"))
    return when(s.isin(list(MAPA_PERIODOS.keys())), mapa_expr_periodos[s]) \
           .otherwise(upper(s))

def normalizar_nacionalidad(c):
    """Estandariza nacionalidad: todas las variantes ECU* → ECUATORIANA."""
    base = regexp_replace(upper(trim(c)), r"[^A-Z]", "")
    return (
        when(base.isNull() | (base == ""),   lit("DESCONOCIDA"))
        .when(base.rlike(r"^ECU[A-Z]*$"),    lit("ECUATORIANA"))
        .when(base == "RIOBAMBA",            lit("ECUATORIANA"))
        .when(base.like("%AUSTRIAC%"),       lit("AUSTRIACA"))
        .when(base.like("%CHIN%"),           lit("CHINA"))
        .when(base.like("%COLOMB%"),         lit("COLOMBIANA"))
        .otherwise(trim(upper(c)))
    )

# =============================================================================
# PASO 1 — PARSEAR DPA EXTERNO (INEC 2024)
# El archivo cge2024.xls tiene estructura narrativa sin headers estándar.
# Se parsea manualmente para extraer provincias, cantones y parroquias.
# =============================================================================
print("\n[1] Parseando DPA INEC 2024...")

df_dpa_raw = pd.read_excel("datos/dpa.xlsx", header=None)

dpa_provincias, dpa_cantones, dpa_parroquias = [], [], []

for i in range(len(df_dpa_raw)):
    row = df_dpa_raw.iloc[i].dropna().tolist()
    row = [str(v).strip() for v in row if str(v).strip() not in ["", "nan", " "]]
    if not row:
        continue

    # Provincia: exactamente 2 elementos → código 2 dígitos + "PROVINCIA..."
    if (len(row) == 2 and
        re.match(r"^\d{2}$", row[0]) and
        "PROVINCIA" in row[1].upper()):
        nombre = re.sub(r"PROVINCIA\s+(DE(L?|LA)?\s+)?", "", row[1],
                        flags=re.IGNORECASE).strip().upper()
        dpa_provincias.append({"cod_dpa": row[0], "nombre_provincia": nombre})

    # Cantón: exactamente 3 elementos → cod_prov + cod_cant + "CANTÓN..."
    elif (len(row) == 3 and
          re.match(r"^\d{2}$", row[0]) and
          re.match(r"^\d{2}$", row[1]) and
          "CANTÓN" in row[2].upper()):
        nombre = row[2].replace("CANTÓN ", "").strip().upper()
        # Limpiar marcador de cambio histórico '*'
        nombre = nombre.lstrip("*").strip()
        dpa_cantones.append({
            "cod_provincia_dpa": row[0],
            "cod_canton":        row[0] + row[1],
            "nombre_canton":     nombre,
        })

    # Parroquia: filas con 4 o 8 elementos (hasta 2 parroquias por fila)
    elif len(row) >= 4:
        for j in range(0, len(row) - 3, 4):
            try:
                cp, cc, cpar, nombre = row[j], row[j+1], row[j+2], row[j+3]
                if (re.match(r"^\d{2}$", cp) and
                    re.match(r"^\d{2}$", cc) and
                    re.match(r"^\d{1,2}$", str(cpar)) and
                    len(nombre) > 2 and
                    "COMPRENDE" not in nombre.upper()):
                    nombre = nombre.lstrip("*").strip().upper()
                    dpa_parroquias.append({
                        "cod_provincia_dpa": cp,
                        "cod_canton":        cp + cc,
                        "cod_parroquia":     cp + cc + str(cpar).zfill(2),
                        "nombre_parroquia":  nombre,
                    })
            except (IndexError, ValueError):
                pass

df_dpa_prov    = spark.createDataFrame(pd.DataFrame(dpa_provincias))
df_dpa_cantones = spark.createDataFrame(pd.DataFrame(dpa_cantones))
df_dpa_parroquias = spark.createDataFrame(pd.DataFrame(dpa_parroquias))

print(f"  DPA provincias: {df_dpa_prov.count()}")
print(f"  DPA cantones:   {df_dpa_cantones.count()}")
print(f"  DPA parroquias: {df_dpa_parroquias.count()}")

# =============================================================================
# PASO 2 — LECTURA DE FUENTES INTERNAS
# =============================================================================
print("\n[2] Leyendo fuentes internas...")

df_ciudades   = spark.read.option("header",True).option("inferSchema",True).csv("datos/ciudades.csv")
df_escuelas   = spark.read.option("header",True).option("inferSchema",True).csv("datos/escuelas.csv")
df_estudiante = spark.read.option("header",True).option("inferSchema",True).option("sep",";").csv("datos/estudiante.csv")
df_facultades = spark.read.option("header",True).option("inferSchema",True).csv("datos/facultades.csv")
df_matricula  = spark.read.option("header",True).option("inferSchema",True).option("sep",";").csv("datos/matricula_aprb_repr.csv")
df_periodos   = spark.read.option("header",True).option("inferSchema",True).option("sep",";").csv("datos/periodos.csv")
df_provincias = spark.read.option("header",True).option("inferSchema",True).csv("datos/provincias.csv")
df_sexo       = spark.read.option("header",True).option("inferSchema",True).csv("datos/sexo.csv")

df_carreras       = spark.createDataFrame(pd.read_excel("datos/carreras.xlsx"))
df_titulo_carrera = spark.createDataFrame(pd.read_excel("datos/titulo_carrera.xlsx"))
df_graduados      = spark.createDataFrame(pd.read_excel("datos/graduados.xlsx"))
df_materia        = spark.createDataFrame(pd.read_excel("datos/materia.xlsx"))
df_proyectos      = spark.createDataFrame(pd.read_excel("datos/proyectos.xlsx"))

print("[OK] Fuentes cargadas")

# =============================================================================
# PASO 3 — PERFILADO
# =============================================================================
print("\n[3] Perfilado básico...")

def perfilar(df, nombre):
    print(f"\n=== {nombre} ===")
    df.printSchema()
    df.show(5, truncate=False)
    df.select([
        count(when(col(c).isNull() | (trim(col(c).cast("string")) == ""), c)).alias(c)
        for c in df.columns
    ]).show()

for nombre, df in [
    ("PROVINCIAS", df_provincias), ("CIUDADES", df_ciudades),
    ("SEXO", df_sexo), ("FACULTADES", df_facultades), ("ESCUELAS", df_escuelas),
    ("CARRERAS", df_carreras), ("PERIODOS", df_periodos), ("PROYECTOS", df_proyectos),
    ("TITULO_CARRERA", df_titulo_carrera), ("MATERIA", df_materia),
    ("ESTUDIANTE", df_estudiante), ("MATRICULA", df_matricula),
    ("GRADUADOS", df_graduados),
]:
    perfilar(df, nombre)

# =============================================================================
# PASO 4 — VALIDACIÓN
# =============================================================================
print("\n[4] Validando reglas de negocio...")

# PROVINCIAS: solo las 24 DPA Ecuador
w = Window.partitionBy("strCodigo")
df_provincias.withColumn("r_dpa", when(trim(col("strCodigo")).isin(PROVINCIAS_EC), lit("OK")).otherwise(lit("FUERA_DPA"))) \
             .withColumn("r_dup", when(count("strCodigo").over(w)==1, lit("OK")).otherwise(lit("DUP"))) \
             .show(50, truncate=False)

# CIUDADES: FK provincia válida (excepto DES dummy)
codigos_prov_raw = [r[0] for r in df_provincias.select("strCodigo").collect()]
w = Window.partitionBy("strCodigo")
df_ciudades.withColumn("r_fk", when(col("strCodProv").isin(codigos_prov_raw) | (col("strCodigo")=="DES"), lit("OK")).otherwise(lit("FK_INV"))) \
           .filter(col("r_fk") == "FK_INV").show(truncate=False)

# PERIODOS: rango de fechas válido
df_per_v = df_periodos \
    .withColumn("_fi", to_date(col("dtFechaInic"), "d/MM/yy")) \
    .withColumn("_ff", to_date(col("dtFechaFin"),  "d/MM/yy")) \
    .withColumn("r_rango", when(col("_fi") < col("_ff"), lit("OK")).otherwise(lit("INV")))
df_per_v.select("strCodigo","dtFechaInic","dtFechaFin","r_rango").show(30, truncate=False)

# MATRICULA: resumen de validación por regla
df_mat_v = df_matricula \
    .withColumn("periodo_norm", normalizar_periodo(col("strCodPeriodo"))) \
    .withColumn("r_est",   when(col("Estudiante").isNotNull() & (col("Estudiante")!=0), lit("OK")).otherwise(lit("INV"))) \
    .withColumn("r_per",   when(col("periodo_norm").isin(CODIGOS_PERIODO_OK), lit("OK")).otherwise(lit("INV"))) \
    .withColumn("r_forma", when(col("strCodFormaAprob").isin("PRI","REP","CON"), lit("OK")).otherwise(lit("INV")))
print("Validación MATRICULA - Periodos:")
df_mat_v.groupBy("r_per").count().show()
print("Validación MATRICULA - Estudiante=0:")
df_mat_v.groupBy("r_est").count().show()

# ESTUDIANTE: resumen ciudad inválida
codigos_ciu = [r[0] for r in df_ciudades.select("strCodigo").collect()]
df_est_v = df_estudiante \
    .withColumn("r_ciu", when(col("strCodCiudadProc").isin(codigos_ciu), lit("OK")).otherwise(lit("INV→DES")))
print("Validación ESTUDIANTE - Ciudades:")
df_est_v.groupBy("r_ciu").count().show()

# MATERIA: duplicados por carrera+codigo
w = Window.partitionBy("COD_CARRERA_PROGRAMA","COD_MATERIA")
df_mat_dup = df_materia.withColumn("r_dup", when(count("*").over(w)==1, lit("OK")).otherwise(lit("DUP")))
print("Validación MATERIA - Duplicados:")
df_mat_dup.groupBy("r_dup").count().show()

# GRADUADOS: fecha válida
df_grad_v = df_graduados \
    .withColumn("r_fecha", when(col("dtFechaGrado").isNotNull() & (col("dtFechaGrado")>="1990-01-01"), lit("OK")).otherwise(lit("INV")))
print("Validación GRADUADOS - Fechas:")
df_grad_v.groupBy("r_fecha").count().show()

# =============================================================================
# PASO 5 — LIMPIEZA
# =============================================================================
print("\n[5] Limpiando fuentes...")

# ── PROVINCIAS ────────────────────────────────────────────────────────────────
# Conservar solo las 24 provincias DPA Ecuador
df_provincias_l = (df_provincias
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .filter(col("strCodigo").isin(PROVINCIAS_EC))
    .dropDuplicates(["strCodigo"])
)
print(f"  provincias: {df_provincias_l.count()}")

# ── SEXO ──────────────────────────────────────────────────────────────────────
df_sexo_l = (df_sexo
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .filter(col("strNombre").isin("FEMENINO","MASCULINO"))
    .dropDuplicates(["strCodigo"])
)
print(f"  sexo: {df_sexo_l.count()}")

# ── CIUDADES ──────────────────────────────────────────────────────────────────
# Eliminar ciudades de provincias extranjeras. Conservar DES (dummy Kimball).
codigos_prov_l = [r[0] for r in df_provincias_l.select("strCodigo").collect()]
df_ciudades_l = (df_ciudades
    .withColumn("strCodigo",  trim(col("strCodigo")))
    .withColumn("strNombre",  trim(upper(col("strNombre"))))
    .withColumn("strCodProv", trim(col("strCodProv")))
    .filter(col("strCodProv").isin(codigos_prov_l) | (col("strCodigo")=="DES"))
    .filter(col("strCodigo").isNotNull())
    .dropDuplicates(["strCodigo"])
)
print(f"  ciudades: {df_ciudades_l.count()}")

# ── FACULTADES ────────────────────────────────────────────────────────────────
df_facultades_l = (df_facultades
    .withColumn("strCodigo", trim(col("strCodigo")))
    .withColumn("strNombre", trim(upper(col("strNombre"))))
    .filter(col("strCodigo").isNotNull())
    .dropDuplicates(["strCodigo"])
)
print(f"  facultades: {df_facultades_l.count()}")

# ── ESCUELAS ──────────────────────────────────────────────────────────────────
df_escuelas_l = (df_escuelas
    .withColumn("strCodigo",      trim(col("strCodigo")))
    .withColumn("strNombre",      trim(upper(col("strNombre"))))
    .withColumn("strCodFacultad", trim(col("strCodFacultad")))
    .filter(col("strCodigo").isNotNull())
    .dropDuplicates(["strCodigo"])
)
print(f"  escuelas: {df_escuelas_l.count()}")

# ── CARRERAS ──────────────────────────────────────────────────────────────────
df_carreras_l = (df_carreras
    .withColumn("strCodigo",     trim(col("strCodigo")))
    .withColumn("strNombre",     trim(upper(col("strNombre"))))
    .withColumn("strCodEscuela", trim(col("strCodEscuela")))
    .filter(col("strCodigo").isNotNull())
    .dropDuplicates(["strCodigo"])
)
codigos_carreras_ok = [r[0] for r in df_carreras_l.select("strCodigo").collect()]
print(f"  carreras: {df_carreras_l.count()}")

# ── PERIODOS ──────────────────────────────────────────────────────────────────
# Estandarizar descripción al formato "MARZO 2004 - AGOSTO 2004"
MAPA_MESES = {
    "01":"ENERO","02":"FEBRERO","03":"MARZO","04":"ABRIL",
    "05":"MAYO","06":"JUNIO","07":"JULIO","08":"AGOSTO",
    "09":"SEPTIEMBRE","10":"OCTUBRE","11":"NOVIEMBRE","12":"DICIEMBRE"
}
meses_map = create_map([lit(x) for x in chain(*MAPA_MESES.items())])

df_periodos_l = (df_periodos
    .withColumn("strCodigo", trim(upper(col("strCodigo"))))
    .withColumn("fi", to_date(col("dtFechaInic"), "d/MM/yy"))
    .withColumn("ff", to_date(col("dtFechaFin"),  "d/MM/yy"))
    .filter(col("fi").isNotNull() & col("ff").isNotNull() & (col("fi") < col("ff")))
    .dropDuplicates(["strCodigo"])
    .withColumn("strDescripcion",
        when(col("strCodigo").startswith("R"),
             concat(lit("REMEDIAL "), date_format(col("fi"),"yyyy"))
        ).otherwise(
             concat(meses_map[date_format(col("fi"),"MM")], lit(" "), date_format(col("fi"),"yyyy"),
                    lit(" - "),
                    meses_map[date_format(col("ff"),"MM")], lit(" "), date_format(col("ff"),"yyyy"))
        )
    )
    .select("strCodigo","strDescripcion",
            col("fi").alias("dtFechaInic_parsed"),
            col("ff").alias("dtFechaFin_parsed"))
    .orderBy("dtFechaInic_parsed")
)
print(f"  periodos: {df_periodos_l.count()}")
df_periodos_l.show(30, truncate=False)

# ── PROYECTOS ─────────────────────────────────────────────────────────────────
# Quitar comillas al inicio/fin del tema ("TEMA" → TEMA)
df_proyectos_l = (df_proyectos
    .withColumn("txtTema",
        trim(regexp_replace(regexp_replace(trim(col("txtTema")), r'^"',""), r'"$',"")))
    .filter(col("intCodProyecto").isNotNull())
    .filter(col("txtTema").isNotNull() & (trim(col("txtTema")) != ""))
    .dropDuplicates(["intCodProyecto"])
)
print(f"  proyectos: {df_proyectos_l.count()}")

# ── TITULO_CARRERA ────────────────────────────────────────────────────────────
# COD_CARRERA_PROGRAMA usa códigos históricos → NO filtrar por FK
df_titulo_carrera_l = (df_titulo_carrera
    .withColumn("COD_TITULO",           trim(upper(col("COD_TITULO"))))
    .withColumn("COD_CARRERA_PROGRAMA", trim(upper(col("COD_CARRERA_PROGRAMA"))))
    .withColumn("strNombre",            trim(upper(col("strNombre"))))
    .filter(col("COD_TITULO").isNotNull())
    .filter(col("strNombre").isNotNull() & (trim(col("strNombre")) != ""))
    .dropDuplicates(["COD_TITULO","COD_CARRERA_PROGRAMA"])
)
print(f"  titulo_carrera: {df_titulo_carrera_l.count()}")

# ── MATERIA ───────────────────────────────────────────────────────────────────
# Duplicados solo cuando coinciden carrera+codigo (mismo código en distinta carrera es válido)
df_materia_l = (df_materia
    .withColumn("COD_MATERIA",          trim(col("COD_MATERIA")))
    .withColumn("MATERIA",              trim(upper(col("MATERIA"))))
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .filter(col("COD_MATERIA").isNotNull())
    .filter(col("MATERIA").isNotNull() & (trim(col("MATERIA")) != ""))
    .dropDuplicates(["COD_CARRERA_PROGRAMA","COD_MATERIA"])
)
print(f"  materia: {df_materia_l.count()}")

# ── ESTUDIANTE ────────────────────────────────────────────────────────────────
# PK real = cedula (strCodigo es número de matrícula, puede repetirse)
# EST-03: apellido nulo → DESCONOCIDO (Kimball dummy)
# EST-04: fecha nula   → 1900-01-01  (Kimball dummy)
# EST-05: nacionalidad normalizada    (ECU* → ECUATORIANA)
# EST-07: ciudad nula o inválida      → DES  (Kimball dummy)
df_estudiante_l = (df_estudiante
    .withColumn("strNombres",   trim(upper(col("strNombres"))))
    .withColumn("strApellidos",
        when(col("strApellidos").isNull() | (trim(col("strApellidos"))==""),
             DUMMY_APELLIDO)
        .otherwise(trim(upper(col("strApellidos"))))
    )
    .withColumn("strCodSexo",        trim(upper(col("strCodSexo"))))
    .withColumn("strNacionalidad",   normalizar_nacionalidad(col("strNacionalidad")))
    .withColumn("strCodCiudadProc",
        when(col("strCodCiudadProc").isNull() |
             (trim(col("strCodCiudadProc"))=="") |
             (~col("strCodCiudadProc").isin(
                 [r[0] for r in df_ciudades_l.select("strCodigo").collect()])),
             DUMMY_CIUDAD)
        .otherwise(trim(col("strCodCiudadProc")))
    )
    .withColumn("dtFechaNac",
        when(col("dtFechaNac").isNull(), DUMMY_FECHA)
        .otherwise(to_date(col("dtFechaNac").cast("string").substr(1,10),"yyyy-MM-dd").cast("string"))
    )
    .dropDuplicates(["cedula"])
    .filter(col("cedula").isNotNull())
    .filter(col("strNombres").isNotNull() & (trim(col("strNombres")) != ""))
    .filter(col("strCodSexo").isin("FEM","MAS"))
)
print(f"  estudiante: {df_estudiante_l.count()}")
print("  Distribución nacionalidades:")
df_estudiante_l.groupBy("strNacionalidad").count().orderBy(col("count").desc()).show()

# ── MATRICULA ─────────────────────────────────────────────────────────────────
# Estudiante=0 inválido. Periodos 900-911 descartados (no existen en catálogo).
# PRI=aprobado primera vez, REP=reprobado, CON=convalidado (todos válidos)
df_matricula_l = (df_matricula
    .withColumn("strCodPeriodo",        normalizar_periodo(col("strCodPeriodo")))
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .withColumn("strCodMateria",        trim(col("strCodMateria")))
    .filter(col("Estudiante").isNotNull() & (col("Estudiante") != 0))
    .filter(col("strCodPeriodo").isin(CODIGOS_PERIODO_OK))
    .filter(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras_ok))
    .filter(col("strCodMateria").isNotNull())
    .filter(col("strCodFormaAprob").isin("PRI","REP","CON"))
    .dropDuplicates(["Estudiante","strCodPeriodo","COD_CARRERA_PROGRAMA","strCodMateria"])
)
print(f"  matricula: {df_matricula_l.count()}")

# ── GRADUADOS ─────────────────────────────────────────────────────────────────
# Graduaciones con distinto título en la misma carrera son válidas (tecn→ing)
df_graduados_l = (df_graduados
    .withColumn("COD_CARRERA_PROGRAMA", trim(col("COD_CARRERA_PROGRAMA")))
    .withColumn("strCodTitulo",         trim(upper(col("strCodTitulo"))))
    .filter(col("estudiante").isNotNull())
    .filter(col("strCodTitulo").isNotNull())
    .filter(col("dtFechaGrado").isNotNull() & (col("dtFechaGrado") >= "1990-01-01"))
    .filter(col("COD_CARRERA_PROGRAMA").isin(codigos_carreras_ok))
    .dropDuplicates(["estudiante","COD_CARRERA_PROGRAMA","strCodTitulo"])
)
print(f"  graduados: {df_graduados_l.count()}")

# ── DPA LIMPIOS ───────────────────────────────────────────────────────────────
# Las 3 tablas del DPA se guardan con sus propios códigos INEC.
# El cruce con provincias.csv lo hará el PDI en la capa EDW.

df_dpa_prov_l = (df_dpa_prov
    .withColumn("cod_dpa",          trim(col("cod_dpa")))
    .withColumn("nombre_provincia", trim(upper(col("nombre_provincia"))))
    .dropDuplicates(["cod_dpa"])
    .filter(col("cod_dpa").isNotNull())
)
print(f"  dpa_provincias: {df_dpa_prov_l.count()}")

df_dpa_cantones_l = (df_dpa_cantones
    .withColumn("cod_provincia_dpa", trim(col("cod_provincia_dpa")))
    .withColumn("cod_canton",        trim(col("cod_canton")))
    .withColumn("nombre_canton",
        trim(regexp_replace(upper(col("nombre_canton")), r"^\*\*?", "")))
    .dropDuplicates(["cod_canton"])
    .filter(col("cod_canton").isNotNull())
)
print(f"  dpa_cantones: {df_dpa_cantones_l.count()}")

df_dpa_parroquias_l = (df_dpa_parroquias
    .withColumn("cod_provincia_dpa", trim(col("cod_provincia_dpa")))
    .withColumn("cod_canton",        trim(col("cod_canton")))
    .withColumn("cod_parroquia",     trim(col("cod_parroquia")))
    .withColumn("nombre_parroquia",
        trim(regexp_replace(upper(col("nombre_parroquia")), r"^\*\*?", "")))
    .dropDuplicates(["cod_parroquia"])
    .filter(col("cod_parroquia").isNotNull())
)
print(f"  dpa_parroquias: {df_dpa_parroquias_l.count()}")

# =============================================================================
# PASO 6 — INTEGRACIÓN
# Siguiendo el ejemplo de la guía (cliente→ciudad, venta→cliente),
# aquí enriquecemos las tablas principales con sus catálogos.
# Solo se construyen las integraciones que el DSA necesita para el EDW.
# =============================================================================
print("\n[6] Integrando tablas...")

# Estudiante enriquecido con ciudad y sexo (listo para d_estudiante en EDW)
df_estudiante_final = (
    df_estudiante_l.alias("e")
    .join(df_ciudades_l.select(
              col("strCodigo").alias("strCodCiudadProc"),
              col("strNombre").alias("ciudad_procedencia"),
              col("strCodProv").alias("cod_provincia")
          ).alias("c"), on="strCodCiudadProc", how="left")
    .join(df_provincias_l.select(
              col("strCodigo").alias("cod_provincia"),
              col("strNombre").alias("provincia_procedencia")
          ).alias("p"), on="cod_provincia", how="left")
    .join(df_sexo_l.select(
              col("strCodigo").alias("strCodSexo"),
              col("strNombre").alias("sexo")
          ).alias("s"), on="strCodSexo", how="left")
    .select(
        col("e.cedula"),
        col("e.strCodigo").alias("cod_estudiante"),
        col("e.strNombres").alias("nombres"),
        col("e.strApellidos").alias("apellidos"),
        col("e.dtFechaNac").alias("fecha_nacimiento"),
        col("e.strNacionalidad").alias("nacionalidad"),
        col("e.strCodSexo").alias("cod_sexo"),
        col("s.sexo"),
        col("e.strCodCiudadProc").alias("cod_ciudad"),
        col("c.ciudad_procedencia"),
        col("c.cod_provincia"),
        col("p.provincia_procedencia"),
    )
)
print(f"  estudiante_final: {df_estudiante_final.count()}")

# Carrera enriquecida con escuela y facultad (jerarquía académica completa)
df_carrera_final = (
    df_carreras_l.alias("car")
    .join(df_escuelas_l.alias("esc"),
          col("car.strCodEscuela")==col("esc.strCodigo"), how="left")
    .join(df_facultades_l.alias("fac"),
          col("esc.strCodFacultad")==col("fac.strCodigo"), how="left")
    .select(
        col("car.strCodigo").alias("cod_carrera"),
        col("car.strNombre").alias("carrera"),
        col("esc.strCodigo").alias("cod_escuela"),
        col("esc.strNombre").alias("escuela"),
        col("fac.strCodigo").alias("cod_facultad"),
        col("fac.strNombre").alias("facultad"),
    )
)
print(f"  carrera_final: {df_carrera_final.count()}")

# Matrícula enriquecida con periodo, carrera y materia (tabla de hechos)
df_matricula_final = (
    df_matricula_l.alias("m")
    .join(df_periodos_l.select(
              col("strCodigo").alias("strCodPeriodo"),
              col("strDescripcion").alias("descripcion_periodo"),
              col("dtFechaInic_parsed"),
              col("dtFechaFin_parsed")
          ).alias("p"), on="strCodPeriodo", how="left")
    .join(df_carrera_final.alias("cef"),
          col("m.COD_CARRERA_PROGRAMA")==col("cef.cod_carrera"), how="left")
    .join(df_materia_l.select(
              col("COD_MATERIA").alias("strCodMateria"),
              col("MATERIA").alias("nombre_materia")
          ).alias("mat"), on="strCodMateria", how="left")
    .select(
        col("m.Estudiante").alias("cod_estudiante"),
        col("m.strCodPeriodo").alias("cod_periodo"),
        col("p.descripcion_periodo"),
        col("p.dtFechaInic_parsed").alias("fecha_inicio"),
        col("p.dtFechaFin_parsed").alias("fecha_fin"),
        col("m.COD_CARRERA_PROGRAMA").alias("cod_carrera"),
        col("cef.carrera"), col("cef.escuela"), col("cef.facultad"),
        col("m.strCodMateria").alias("cod_materia"),
        col("mat.nombre_materia"),
        col("m.strCodFormaAprob").alias("forma_aprobacion"),
    )
)
print(f"  matricula_final: {df_matricula_final.count()}")

# Graduados enriquecidos con carrera, título y proyecto
df_graduados_final = (
    df_graduados_l.alias("g")
    .join(df_carrera_final.alias("cef"),
          col("g.COD_CARRERA_PROGRAMA")==col("cef.cod_carrera"), how="left")
    .join(df_titulo_carrera_l.select(
              col("COD_TITULO").alias("strCodTitulo"),
              col("strNombre").alias("nombre_titulo")
          ).alias("t"), on="strCodTitulo", how="left")
    .join(df_proyectos_l.select(
              col("intCodProyecto").alias("_cp"),
              col("txtTema").alias("tema_proyecto")
          ).alias("proy"),
          col("g.intCodProyecto")==col("proy._cp"), how="left")
    .select(
        col("g.estudiante").alias("cod_estudiante"),
        col("g.COD_CARRERA_PROGRAMA").alias("cod_carrera"),
        col("cef.carrera"), col("cef.escuela"), col("cef.facultad"),
        col("g.strCodTitulo").alias("cod_titulo"),
        col("t.nombre_titulo"),
        col("g.dtFechaGrado").alias("fecha_graduacion"),
        col("g.intCodProyecto").alias("cod_proyecto"),
        col("proy.tema_proyecto"),
    )
)
print(f"  graduados_final: {df_graduados_final.count()}")

# =============================================================================
# PASO 7 — GUARDAR EN DELTA LAKE
# Solo se persisten las tablas limpias base + las 3 fuentes DPA.
# Las integraciones (enriquecidas) también se guardan para el EDW.
# =============================================================================
print("\n[7] Guardando en Delta Lake...")

tablas_delta = [
    # Catálogos limpios
    ("provincias",       df_provincias_l),
    ("sexo",             df_sexo_l),
    ("ciudades",         df_ciudades_l),
    ("facultades",       df_facultades_l),
    ("escuelas",         df_escuelas_l),
    ("carreras",         df_carreras_l),
    ("periodos",         df_periodos_l),
    ("proyectos",        df_proyectos_l),
    ("titulo_carrera",   df_titulo_carrera_l),
    ("materia",          df_materia_l),
    # Transaccionales limpias
    ("estudiante",       df_estudiante_l),
    ("matricula",        df_matricula_l),
    ("graduados",        df_graduados_l),
    # DPA externo limpio (3 tablas)
    ("dpa_provincias",   df_dpa_prov_l),
    ("dpa_cantones",     df_dpa_cantones_l),
    ("dpa_parroquias",   df_dpa_parroquias_l),
    # Integradas para el EDW
    ("estudiante_final", df_estudiante_final),
    ("carrera_final",    df_carrera_final),
    ("matricula_final",  df_matricula_final),
    ("graduados_final",  df_graduados_final),
]

for nombre, df in tablas_delta:
    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema","true").save(f"delta/{nombre}")
    print(f"  [Delta] {nombre}")

print("[OK] Delta Lake completo")

# =============================================================================
# PASO 8 — CARGAR EN POSTGRESQL - Esquema DSA
# Se lee desde Delta para garantizar integridad.
# =============================================================================
print("\n[8] Cargando en PostgreSQL DSA...")

jdbc_url = "jdbc:postgresql://localhost:5432/dw_academico"
props = {"user":"postgres","password":"123456","driver":"org.postgresql.Driver"}

tablas_pg = [
    # Catálogos
    ("provincias",       "provincias"),
    ("sexo",             "sexo"),
    ("ciudades",         "ciudades"),
    ("facultades",       "facultades"),
    ("escuelas",         "escuelas"),
    ("carreras",         "carreras"),
    ("periodos",         "periodos"),
    ("proyectos",        "proyectos"),
    ("titulo_carrera",   "titulo_carrera"),
    ("materia",          "materia"),
    # Transaccionales
    ("estudiante",       "estudiante"),
    ("matricula",        "matricula"),
    ("graduados",        "graduados"),
    # DPA
    ("dpa_provincias",   "dpa_provincias"),
    ("dpa_cantones",     "dpa_cantones"),
    ("dpa_parroquias",   "dpa_parroquias"),
    # Integradas
    ("estudiante_final", "estudiante_final"),
    ("carrera_final",    "carrera_final"),
    ("matricula_final",  "matricula_final"),
    ("graduados_final",  "graduados_final"),
]

for nombre_delta, tabla_pg in tablas_pg:
    df = spark.read.format("delta").load(f"delta/{nombre_delta}")
    n  = df.count()
    df.write.jdbc(url=jdbc_url, table=f"dsa.{tabla_pg}",
                  mode="overwrite", properties=props)
    print(f"  [PG] dsa.{tabla_pg}: {n} filas")

print("[OK] PostgreSQL DSA completo")

# =============================================================================
# PASO 9 — REPORTE FINAL DE CALIDAD
# =============================================================================
print("\n" + "="*65)
print("REPORTE FINAL DE CALIDAD - DSA")
print("="*65)
print(f"{'Fuente':<26} {'Original':>10} {'Limpio':>10} {'Eliminados':>12}")
print("-"*60)

reporte = [
    ("provincias.csv",      df_provincias,  df_provincias_l),
    ("ciudades.csv",        df_ciudades,    df_ciudades_l),
    ("sexo.csv",            df_sexo,        df_sexo_l),
    ("facultades.csv",      df_facultades,  df_facultades_l),
    ("escuelas.csv",        df_escuelas,    df_escuelas_l),
    ("carreras.xlsx",       df_carreras,    df_carreras_l),
    ("periodos.csv",        df_periodos,    df_periodos_l),
    ("proyectos.xlsx",      df_proyectos,   df_proyectos_l),
    ("titulo_carrera.xlsx", df_titulo_carrera, df_titulo_carrera_l),
    ("materia.xlsx",        df_materia,     df_materia_l),
    ("estudiante.csv",      df_estudiante,  df_estudiante_l),
    ("matricula_aprb.csv",  df_matricula,   df_matricula_l),
    ("graduados.xlsx",      df_graduados,   df_graduados_l),
]

for fuente, df_orig, df_limp in reporte:
    o = df_orig.count()
    l = df_limp.count()
    print(f"{fuente:<26} {o:>10} {l:>10} {o-l:>12}")

print(f"\n{'DPA (fuente externa)':<26}")
print(f"  dpa_provincias:  {df_dpa_prov_l.count()}")
print(f"  dpa_cantones:    {df_dpa_cantones_l.count()}")
print(f"  dpa_parroquias:  {df_dpa_parroquias_l.count()}")

print("\n[PIPELINE COMPLETADO]")
spark.stop()