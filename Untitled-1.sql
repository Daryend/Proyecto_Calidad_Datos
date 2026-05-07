/*Codigo SQL */

/*SCRIPT DE CARGA DE UBICACION ACADEMICA*/
	SELECT
    fa."strCodigo"  AS cod_facultad,
    fa."strNombre"  AS facultad,
    esc."strCodigo" AS cod_escuela,
    esc."strNombre" AS escuela,
    ca."strCodigo"  AS cod_carrera,
    ca."strNombre"  AS carrera,
    now()           AS etl_fecha_carga
FROM dsa.carreras AS ca
INNER JOIN dsa.escuelas AS esc
    ON ca."strCodEscuela" = esc."strCodigo"  
INNER JOIN dsa.facultades AS fa
    ON esc."strCodFacultad" = fa."strCodigo" 

/*SCRIPT DE CARGA DE MATERIAS*/
SELECT
	ma."COD_MATERIA" AS "cod_materia",
	ma."MATERIA" AS "materia",
	now() AS "etl_fecha_carga"
FROM dsa.materia AS ma

/*SCRIPT DE CARGA DE TITULO*/
SELECT
	ti."COD_TITULO" AS "cod_titulo",
	ti."COD_CARRERA_PROGRAMA" AS "cod_carrera",
	ti."strNombre" AS "titulo",
	now() AS "etl_fecha_carga"
FROM dsa.titulo_carrera AS ti

/*SCRIPT DE CARGA DE PERIODICIDAD*/
SELECT
	per."strCodigo" AS "cod_periodocad",
	per."strDescripcion" AS "periodo",
	per."dtFechaInic_parsed" AS "fecha_ini",
	per."dtFechaFin_parsed" AS "fecha_fin",
	now() AS "etl_fecha_carga"
FROM dsa.periodos AS per

/*SCRIPT DE CARGA DE PROYECTO*/
SELECT
	pr."intCodProyecto" AS "cod_proyecto",
	pr."txtTema" AS "proyecto",
	now() AS "etl_fecha_carga"
FROM dsa.proyectos AS pr

/*SCRIPT DE CARGA DE ESTUDIANTE*/
SELECT DISTINCT ON (es."strCodigo")
    es."cedula" AS cedula,
    es."strCodigo" AS codestud,
    CONCAT(es."strNombres",' ',es."strApellidos") AS estudiante,
    es."dtFechaNac" AS fecha_nacimiento,
    se."strNombre" AS sexo,
    'ND' AS discapacidad,
    'ND' AS etnia,
    CASE
        WHEN es."strNacionalidad" = 'ECUATORIANA'
            THEN ug.id_ubicgeografica
        ELSE 1   
    END AS id_ubicgeografica,
    now() AS etl_fecha_carga
FROM dsa.estudiante AS es
INNER JOIN dsa.sexo AS se
    ON es."strCodSexo" = se."strCodigo"
LEFT JOIN dsa.ciudades AS c
    ON es."strCodCiudadProc" = c."strCodigo"
LEFT JOIN edw.d_ubicgeografica AS ug
    ON c."strCodProv" = ug.cod_provincia

/*SCRIPT DE CARGA DE UBICACION GEOGRAFICA*/
SELECT
    'EC' AS cod_pais,
    'ECUADOR' AS pais,
    dp."cod_dpa" AS dpa,
    dp."cod_dpa" AS cod_provincia,
    dp."nombre_provincia" AS provincia,
    dc."cod_canton" AS cod_canton,
    dc."nombre_canton" AS canton,
    dpar."cod_parroquia" AS cod_parroquia,
    dpar."nombre_parroquia" AS parroquia,
    NOW()::timestamp AS etl_fecha_ini,
    NOW()::timestamp AS etl_fecha_fin,
    NOW()::timestamp AS etl_fecha_carga
FROM dsa.dpa_provincias AS dp
INNER JOIN dsa.dpa_cantones AS dc
    ON dc."cod_provincia_dpa" = dp."cod_dpa"
INNER JOIN dsa.dpa_parroquias AS dpar
    ON dpar."cod_canton" = dc."cod_canton"
UNION ALL


//SCRIPTS DE DUMMYS

//SCRIPT PARA DUMMYS DE UBICACION ACADEMICA
//Pasos para hacer los Dummys:
//1. Primero hacemos un Job en el PDI
//2. Luego hacemos un SCRIPT PARA TRUNCATE LA TABLA DE DESTINO
TRUNCATE TABLE edw.d_ubicacion_academica CASCADE;
ALTER SEQUENCE edw.d_ubicacion_academica_id_ubicacademica_seq RESTART WITH 1;

//3. Luego hacemos un SCRIPT PARA INSERTAR LOS DUMMYS
INSERT INTO edw.d_ubicacion_academica(
    id_ubicacademica, etl_fecha_carga, cod_escuela, cod_carrera,
    facultad, escuela, carrera, cod_facultad, etl_version, date_to
)
VALUES (
    0, NOW()::timestamp, 'ND', 'ND',
    'DESCONOCIDO', 'DESCONOCIDO', 'DESCONOCIDO', 'ND', 0, '2199-12-31 23:59:59'
);

//SCRIP DUMMY PARA TITULO 
TRUNCATE TABLE edw.d_titulo CASCADE;
ALTER SEQUENCE edw.d_titulo_id_titulo_seq RESTART WITH 1;

INSERT INTO edw.d_titulo(
	id_titulo, cod_carrera, carrera, cod_titulo, titulo, etl_fecha_carga, version, date_to, date_from
)VALUES (
	0, 'ND', 'ND', 'ND', 'DESCONOCIDO', NOW()::timestamp, 0, '2199-12-31 23:59:59', '1900-01-01 00:00:00'
);


//
//
//
-- Procedimiento para generar la dimensión tiempo
CREATE OR REPLACE PROCEDURE edw.generar_calendario(
    fecha_inicio DATE,
    fecha_fin    DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    fecha_actual DATE;
BEGIN

    FOR fecha_actual IN
        SELECT generate_series(fecha_inicio, fecha_fin, interval '1 day')::date
    LOOP
        INSERT INTO edw.d_tiempo (
            id_tiempo,
            anio,
            semestre,
            semestre_nombre,
            mes,
            mes_nombre,
            dia
        )
        VALUES (
            TO_CHAR(fecha_actual, 'YYYYMMDD')::INT,
            EXTRACT(YEAR FROM fecha_actual)::INT,
            CASE
                WHEN EXTRACT(MONTH FROM fecha_actual) <= 6 THEN 1
                ELSE 2
            END,
            CASE
                WHEN EXTRACT(MONTH FROM fecha_actual) <= 6
                    THEN 'PRIMER SEM.'
                ELSE 'SEGUNDO SEM.'
            END,
            EXTRACT(MONTH FROM fecha_actual)::INT,
            CASE EXTRACT(MONTH FROM fecha_actual)::INT
                WHEN 1  THEN 'ENERO'
                WHEN 2  THEN 'FEBRERO'
                WHEN 3  THEN 'MARZO'
                WHEN 4  THEN 'ABRIL'
                WHEN 5  THEN 'MAYO'
                WHEN 6  THEN 'JUNIO'
                WHEN 7  THEN 'JULIO'
                WHEN 8  THEN 'AGOSTO'
                WHEN 9  THEN 'SEPTIEMBRE'
                WHEN 10 THEN 'OCTUBRE'
                WHEN 11 THEN 'NOVIEMBRE'
                WHEN 12 THEN 'DICIEMBRE'
            END,
            -- Nombre del día según ISO 8601 (lunes=1, domingo=7)
            CASE EXTRACT(ISODOW FROM fecha_actual)::INT
                WHEN 1 THEN 'LUNES'
                WHEN 2 THEN 'MARTES'
                WHEN 3 THEN 'MIERCOLES'
                WHEN 4 THEN 'JUEVES'
                WHEN 5 THEN 'VIERNES'
                WHEN 6 THEN 'SABADO'
                WHEN 7 THEN 'DOMINGO'
            END
        )
        -- Si el id_tiempo ya existe, actualiza todos los campos
        ON CONFLICT (id_tiempo) DO UPDATE SET
            anio            = EXCLUDED.anio,
            semestre        = EXCLUDED.semestre,
            semestre_nombre = EXCLUDED.semestre_nombre,
            mes             = EXCLUDED.mes,
            mes_nombre      = EXCLUDED.mes_nombre,
            dia             = EXCLUDED.dia;

    END LOOP;
END $$;

CALL edw.generar_calendario('2000-01-01', '2030-12-31');
