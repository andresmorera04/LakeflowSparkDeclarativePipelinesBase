# Manual Tecnico — Pipeline LSDP Laboratorio Basico

**Proyecto**: LakeflowSparkDeclarativePipelinesBase
**Plataforma**: Databricks con Lakeflow Spark Declarative Pipelines (LSDP)
**Arquitectura**: Medallion (Bronce / Plata / Oro)
**Almacenamiento**: Azure Data Lake Storage Gen2 con Unity Catalog
**Fecha**: 2026-04-01

Este manual documenta de forma tecnica y detallada todos los componentes del pipeline LSDP: generadores de datos AS400 simulados, utilidades de infraestructura, transformaciones por capa del medallion, el paradigma declarativo LSDP, las propiedades Delta aplicadas y la estrategia de pruebas TDD.

---

## Dependencias de Infraestructura Azure

El pipeline LSDP requiere los siguientes 6 recursos de Azure configurados y operativos antes de ejecutar cualquier notebook o pipeline.

### Azure Data Lake Storage Gen2 (ADLS Gen2)

- **Recurso**: Storage Account con namespace jerarquico habilitado
- **Protocolo de acceso**: exclusivamente `abfss://contenedor@cuenta.dfs.core.windows.net/`
- **Protocolo prohibido**: `/mnt/` (legacy DBFS mounts, incompatible con Serverless)

**Contenedores requeridos**:

| Contenedor | Proposito |
|------------|-----------|
| bronce | Almacena los parquets AS400 fuente y los checkpoints de AutoLoader |
| plata | Reservado para artefactos futuros de la capa plata |
| oro | Reservado para artefactos futuros de la capa oro |
| control | Almacena metadatos de control del pipeline |

### Azure SQL Free Tier

- **Proposito**: Almacena la tabla de configuracion `dbo.Parametros` que centraliza todos los parametros del pipeline
- **Base de datos**: `Configuracion`
- **Tabla**: `dbo.Parametros` con estructura clave-valor (columnas `Clave` y `Valor`)
- **Acceso**: via JDBC con autenticacion de usuario y contrasena

### Azure Key Vault

- **Proposito**: Almacena los secretos de conexion a Azure SQL de forma segura
- **Secretos requeridos**:

| Nombre del Secreto | Contenido |
|--------------------|-----------|
| `sr-jdbc-asql-asqlmetadatos-adminpd` | Cadena JDBC completa sin el segmento de contrasena |
| `sr-asql-asqlmetadatos-adminpd` | Contrasena de la base de datos Azure SQL |

### Databricks Premium con Unity Catalog

- **Tier requerido**: Premium (necesario para Unity Catalog y External Locations)
- **Compute**: Serverless Compute para pipelines LSDP y notebooks
- **Unity Catalog**: habilitado con metastore regional

### External Locations

- **Proposito**: Permiten a Unity Catalog acceder a los contenedores ADLS Gen2 bajo las rutas `abfss://`
- **Configuracion**: Una External Location por cada contenedor de ADLS Gen2 utilizado

### Catalogos Unity Catalog

Cada capa del medallion tiene su propio catalogo de Unity Catalog:

| Catalogo | Esquema | Proposito |
|----------|---------|-----------|
| `bronce_dev` | `regional` | Streaming tables de ingesta desde parquets AS400 |
| `plata_dev` | `regional` | Vistas materializadas de transformacion y enriquecimiento |
| `oro_dev` | `regional` | Vistas materializadas de agregacion y producto de datos final |
| `control_dev` | `regional` | Tablas de metadatos y control del pipeline |

---

## Parametros

El pipeline LSDP externaliza toda la configuracion en dos fuentes: la tabla `dbo.Parametros` en Azure SQL y los parametros del pipeline declarados al crear o actualizar el pipeline en la interfaz de Databricks.

### Tabla dbo.Parametros en Azure SQL

La tabla `dbo.Parametros` almacena parametros de infraestructura que identifican los recursos Azure. La funcion `leer_parametros_azure_sql` lee el conjunto completo sin filtros y retorna un diccionario Python; cada script consume las claves que necesita.

| Clave | Tipo | Ejemplo | Descripcion |
|-------|------|---------|-------------|
| `catalogoBronce` | string | `bronce_dev` | Nombre del catalogo Unity Catalog de bronce |
| `contenedorBronce` | string | `bronce` | Nombre del contenedor ADLS Gen2 donde se almacenan los parquets y checkpoints |
| `datalake` | string | `adlsg2datalakedev` | Nombre del Storage Account de ADLS Gen2 |
| `DirectorioBronce` | string | `archivos` | Directorio raiz dentro del contenedor de bronce |
| `catalogoPlata` | string | `plata_dev` | Nombre del catalogo Unity Catalog de plata |
| `catalogoOro` | string | `oro_dev` | Nombre del catalogo Unity Catalog de oro |

### Parametros del Pipeline LSDP

Los parametros del pipeline se configuran en la definicion del pipeline LSDP y se leen en tiempo de ejecucion con `spark.conf.get("pipelines.parameters.nombreParametro")`.

| Parametro | Tipo | Ejemplo | Descripcion |
|-----------|------|---------|-------------|
| `nombreScopeSecret` | string | `sc-kv-laboratorio` | Nombre del Scope Secret de Databricks vinculado al Azure Key Vault |
| `esquema_plata` | string | `regional` | Nombre del esquema dentro del catalogo de plata para las vistas materializadas |
| `esquema_oro` | string | `regional` | Nombre del esquema dentro del catalogo de oro para las vistas materializadas |
| `rutaCompletaMaestroCliente` | string | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa al directorio raiz del parquet de Maestro de Clientes |
| `rutaCompletaTransaccional` | string | `LSDP_Base/As400/Transaccional/` | Ruta relativa al directorio del parquet Transaccional |
| `rutaCompletaSaldoCliente` | string | `LSDP_Base/As400/SaldoCliente/` | Ruta relativa al directorio del parquet de Saldos de Clientes |
| `rutaCheckpointCmstfl` | string | `LSDP_Base/Checkpoints/Bronce/cmstfl/` | Ruta relativa del checkpoint de AutoLoader para la tabla cmstfl |
| `rutaCheckpointTrxpfl` | string | `LSDP_Base/Checkpoints/Bronce/trxpfl/` | Ruta relativa del checkpoint de AutoLoader para la tabla trxpfl |
| `rutaCheckpointBlncfl` | string | `LSDP_Base/Checkpoints/Bronce/blncfl/` | Ruta relativa del checkpoint de AutoLoader para la tabla blncfl |

Las rutas relativas se combinan con los parametros de `dbo.Parametros` usando `LsdpConstructorRutasAbfss` para producir rutas `abfss://` completas en tiempo de ejecucion.

---

## Notebooks de Generacion de Parquets

Los tres notebooks en `scripts/GenerarParquets/` simulan los archivos fuente AS400 que servirian como datos de ingesta del pipeline LSDP. Cada notebook genera un archivo parquet en ADLS Gen2 con la estructura y volumetria especificadas.

### NbGenerarMaestroCliente.py

**Proposito**: Genera el parquet que simula la tabla CMSTFL (Customer Master File) de AS400. Produce 5 millones de registros en la primera ejecucion y aplica mutaciones e incrementos en re-ejecuciones.

**Estructura**: 70 columnas — 42 textuales (StringType), 18 fechas (DateType), 8 enteros (LongType), 2 decimales (DoubleType).

**Volumetria**:
- Primera ejecucion: 5.000.000 de registros con CUSTIDs secuenciales desde el offset configurado
- Re-ejecucion: conserva los clientes existentes + muta el 20% en 15 campos demograficos + agrega un 0,60% de clientes nuevos

**Nombres generados**: Exclusivamente de catalogos no latinos — 100 nombres hebreos (50 masculinos y 50 femeninos), 100 nombres egipcios (50 de cada genero) y 100 nombres en ingles (50 de cada genero). Esta restriccion garantiza que los datos sinteticos sean claramente identificables como simulacion.

**Formato de salida**: Parquet particionado (20 particiones por defecto) escrito en modo overwrite o append segun sea primera ejecucion o re-ejecucion.

**Widgets (17 parametros de entrada)**:

| Widget | Valor por Defecto | Formato | Descripcion |
|--------|-------------------|---------|-------------|
| `ruta_salida_parquet` | `abfss://...` | abfss:// | Ruta de escritura del parquet de salida |
| `cantidad_registros_base` | `5000000` | entero > 0 | Numero de registros en primera ejecucion |
| `pct_incremento` | `0.006` | decimal 0-1 | Fraccion de clientes nuevos en re-ejecucion |
| `pct_mutacion` | `0.20` | decimal 0-1 | Fraccion de clientes a mutar en re-ejecucion |
| `offset_custid` | `100000001` | entero > 0 | CUSTID del primer cliente generado |
| `ruta_parquet_existente` | `""` | abfss:// o vacio | Ruta del parquet previo (vacio = primera ejecucion) |
| `num_particiones` | `20` | entero > 0 | Numero de particiones del parquet de salida |
| `rango_credlmt_min` | `1000` | decimal | Limite de credito minimo |
| `rango_credlmt_max` | `500000` | decimal | Limite de credito maximo |
| `rango_avlbal_min` | `0` | decimal | Saldo disponible minimo |
| `rango_avlbal_max` | `250000` | decimal | Saldo disponible maximo |
| `rango_income_min` | `12000` | decimal | Ingreso anual minimo |
| `rango_income_max` | `500000` | decimal | Ingreso anual maximo |
| `rango_loan_min` | `0` | decimal | Saldo de prestamo minimo |
| `rango_loan_max` | `1000000` | decimal | Saldo de prestamo maximo |
| `rango_ins_min` | `0` | decimal | Monto de seguro minimo |
| `rango_ins_max` | `50000` | decimal | Monto de seguro maximo |

### NbGenerarTransaccionalCliente.py

**Proposito**: Genera el parquet que simula la tabla TRXPFL (Transaction Processing File) de AS400. Produce 15 millones de transacciones por cada ejecucion.

**Estructura**: 60 columnas — 9 textuales (StringType), 19 fechas (DateType), 2 timestamps (TimestampType), 2 enteros (LongType), 28 decimales (DoubleType).

**Volumetria**: 15.000.000 de registros nuevos por ejecucion. No es acumulativo — cada ejecucion reemplaza el parquet anterior.

**Distribucion de tipos de transaccion (15 codigos con distribucion ponderada)**:

| Banda | Codigos | Peso Default |
|-------|---------|--------------|
| Alta frecuencia | CATM, DATM, CMPR, TINT, DPST | ~60% |
| Media frecuencia | PGSL, TEXT, RTRO, PGSV, NMNA, INTR | ~30% |
| Baja frecuencia | ADSL, IMPT, DMCL, CMSN | ~10% |

- **CATM**: Credito por ATM — deposito en cajero automatico
- **DATM**: Debito por ATM — retiro en cajero automatico
- **CMPR**: Compra POS — pago en punto de venta
- **TINT**: Transferencia Interna
- **DPST**: Deposito en Sucursal
- **PGSL**: Pago al Saldo — utilizado en agregaciones de oro

**Prerequisito**: El parquet del Maestro de Clientes debe existir en la ruta configurada. El notebook lee los CUSTIDs del maestro para asignar clientes a las transacciones.

**Widgets (33 parametros de entrada)**:

| Widget | Valor por Defecto | Descripcion |
|--------|-------------------|-------------|
| `ruta_salida_parquet` | `abfss://...` | Ruta de escritura del parquet transaccional |
| `ruta_maestro_clientes` | `abfss://...` | Ruta del parquet del Maestro de Clientes |
| `cantidad_registros` | `15000000` | Total de registros a generar |
| `fecha_transaccion` | `""` | Fecha de transaccion en formato YYYY-MM-DD (obligatoria) |
| `offset_trxid` | `1` | TRXID del primer registro |
| `num_particiones` | `50` | Particiones del parquet de salida |
| `pesos_alta` | `60` | Peso porcentual de la banda de alta frecuencia |
| `pesos_media` | `30` | Peso porcentual de la banda de media frecuencia |
| `pesos_baja` | `10` | Peso porcentual de la banda de baja frecuencia |
| `rango_catm_min/max` | `10 / 1000` | Rango de montos para depositos ATM |
| `rango_datm_min/max` | `10 / 1000` | Rango de montos para retiros ATM |
| `rango_pgsl_min/max` | `100 / 25000` | Rango de montos para pagos al saldo |
| *(9 rangos adicionales)* | *varios* | Rangos de montos para los demas tipos de transaccion |

### NbGenerarSaldosCliente.py

**Proposito**: Genera el parquet que simula la tabla BLNCFL (Balance File) de AS400. Mantiene exactamente un registro de saldo por cada cliente del Maestro (relacion 1:1).

**Estructura**: 100 columnas — 2 enteros (LongType), 29 textuales (StringType), 34 decimales (DoubleType), 35 fechas (DateType).

**Volumetria**: Un registro por cada cliente existente en el Maestro de Clientes. No es acumulativo — se regenera completamente en cada ejecucion.

**Tipos de cuenta generados**: AHRO (Ahorro, 40%), CRTE (Corriente, 30%), PRES (Prestamo, 20%), INVR (Inversion, 10%).

**Prerequisito**: El parquet del Maestro de Clientes debe existir. El notebook lee los CUSTIDs del maestro para generar exactamente un registro de saldo por cliente.

**Widgets (11 parametros de entrada)**:

| Widget | Valor por Defecto | Descripcion |
|--------|-------------------|-------------|
| `ruta_salida_parquet` | `abfss://...` | Ruta de escritura del parquet de saldos |
| `ruta_maestro_clientes` | `abfss://...` | Ruta del parquet del Maestro de Clientes |
| `num_particiones` | `20` | Particiones del parquet de salida |
| `rango_ahorro_min/max` | `0 / 500000` | Rango de saldo para cuentas de ahorro |
| `rango_corriente_min/max` | `0 / 250000` | Rango de saldo para cuentas corrientes |
| `rango_credito_min/max` | `0 / 100000` | Rango de saldo para cuentas de credito |
| `rango_prestamo_min/max` | `1000 / 1000000` | Rango de saldo para prestamos |

---

## Utilidades LSDP

Las tres utilidades en `src/LSDP_Laboratorio_Basico/utilities/` son funciones Python puras reutilizables. Se importan a nivel de modulo en cada script de transformacion y sus resultados quedan capturados por closure para evitar `spark.sparkContext` (incompatible con Serverless).

### LsdpConexionAzureSql.py

**Proposito**: Lee todos los parametros de configuracion desde la tabla `dbo.Parametros` en Azure SQL usando JDBC autenticado con dos secretos de Azure Key Vault.

**Firma de la funcion**:

```python
def leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret) -> dict
```

**Parametros**:

| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| `spark` | SparkSession | Sesion Spark activa del notebook o pipeline |
| `dbutils` | DBUtils | Objeto dbutils de Databricks (no disponible automaticamente en modulos Python) |
| `nombre_scope_secret` | str | Nombre del Scope Secret de Databricks vinculado al Azure Key Vault |

**Valor de retorno**: `dict` con TODAS las claves de `dbo.Parametros` sin filtro. El consumidor selecciona las claves que necesita.

```python
{
    "catalogoBronce"   : "bronce_dev",
    "contenedorBronce" : "bronce",
    "datalake"         : "adlsg2datalakedev",
    "DirectorioBronce" : "archivos",
    "catalogoPlata"    : "plata_dev",
    "catalogoOro"      : "oro_dev"
}
```

**Logica interna**:
1. Obtiene la cadena JDBC sin contrasena del secreto `sr-jdbc-asql-asqlmetadatos-adminpd`
2. Obtiene la contrasena del secreto `sr-asql-asqlmetadatos-adminpd`
3. Concatena `cadena_jdbc + ";password=" + password` para formar la URL JDBC completa
4. Lee `dbo.Parametros` con `spark.read.format("jdbc")` usando driver `com.microsoft.sqlserver.jdbc.SQLServerDriver`
5. Convierte el DataFrame clave-valor a diccionario Python con `{fila["Clave"]: fila["Valor"] for fila in df.collect()}`

**Patron de uso closure**: Se invoca a nivel de modulo (fuera de las funciones decoradas) para que los valores queden capturados en el closure de la funcion `@dp.table` o `@dp.materialized_view`. Este patron es el reemplazo de `spark.sparkContext.broadcast()` en Serverless.

**Restriccion de compatibilidad**: No usa `spark.sparkContext`. Compatible con Computo Serverless de Databricks.

### LsdpConstructorRutasAbfss.py

**Proposito**: Construye rutas `abfss://` completas para ADLS Gen2 combinando los cuatro componentes de la ruta.

**Firma de la funcion**:

```python
def construir_ruta_abfss(contenedor, datalake, directorio_raiz, ruta_relativa) -> str
```

**Parametros**:

| Parametro | Fuente | Ejemplo |
|-----------|--------|---------|
| `contenedor` | `dbo.Parametros["contenedorBronce"]` | `"bronce"` |
| `datalake` | `dbo.Parametros["datalake"]` | `"adlsg2datalakedev"` |
| `directorio_raiz` | `dbo.Parametros["DirectorioBronce"]` | `"archivos"` |
| `ruta_relativa` | Parametro del pipeline LSDP | `"LSDP_Base/As400/MaestroCliente/"` |

**Valor de retorno**: `str` con la ruta `abfss://` completa.

```
abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/
```

**Formato**: `abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}`

**Uso en bronce**: Se invoca 6 veces por pipeline — 3 rutas de parquets (MaestroCliente, Transaccional, SaldoCliente) y 3 rutas de checkpoints (cmstfl, trxpfl, blncfl). Reutilizable para plata y oro cambiando el contenedor y directorio raiz.

**Restriccion**: Nunca produce rutas con `/mnt/` (protocolo legacy prohibido en Serverless).

### LsdpReordenarColumnasLiquidCluster.py

**Proposito**: Reordena las columnas de un DataFrame para que las columnas del Liquid Cluster queden entre las primeras 32 posiciones, requisito de Delta Lake para la recopilacion de estadisticas min/max.

**Firma de la funcion**:

```python
def reordenar_columnas_liquid_cluster(df, columnas_liquid_cluster) -> DataFrame
```

**Parametros**:

| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| `df` | DataFrame | DataFrame de PySpark (batch o streaming) |
| `columnas_liquid_cluster` | list[str] | Nombres de columnas que forman el Liquid Cluster |

**Valor de retorno**: DataFrame con columnas reordenadas — primero las del Liquid Cluster en el orden dado, luego el resto de columnas en su orden original.

**Logica**:
1. Valida que todas las columnas del Liquid Cluster existen en el DataFrame (lanza `ValueError` con las columnas faltantes si no)
2. Construye `orden_final = columnas_liquid_cluster + [c for c in df.columns if c not in columnas_liquid_cluster]`
3. Retorna `df.select(orden_final)`

**Restriccion tecnica**: Delta Lake recopila estadisticas de minimo y maximo solo para las primeras 32 columnas de una tabla. Si las columnas del Liquid Cluster estan fuera de ese rango, Delta lanza el error `DELTA_CLUSTERING_COLUMN_MISSING_STATS`. Esta funcion previene ese error colocando las columnas de clustering al inicio.

**Compatibilidad**: Solo usa `df.select()` — compatible con DataFrames de streaming (usado en bronce) y batch (usado en plata y oro).

---

## Transformaciones — Medalla de Bronce

Los tres scripts de bronce en `src/LSDP_Laboratorio_Basico/transformations/` ingestan los parquets AS400 a tablas streaming Delta en el catalogo `bronce_dev.regional`. Cada script sigue el mismo patron estructural.

### LsdpBronceCmstfl.py

**Proposito**: Ingestar el parquet del Maestro de Clientes (CMSTFL) a la tabla streaming `bronce_dev.regional.cmstfl` via AutoLoader con acumulacion historica incremental.

**Tabla creada**: `bronce_dev.regional.cmstfl`

**Decorador**: `@dp.table(name="cmstfl", ...)`

**Columnas**: 72 (70 campos AS400 + `FechaIngestaDatos` + `_rescued_data`)

**Liquid Cluster**: `["FechaIngestaDatos", "CUSTID"]`

**Configuracion AutoLoader**:

| Opcion | Valor | Descripcion |
|--------|-------|-------------|
| `cloudFiles.format` | `"parquet"` | Formato de los archivos fuente |
| `cloudFiles.schemaEvolutionMode` | `"addNewColumns"` | Incorpora nuevas columnas automaticamente sin fallo |
| `cloudFiles.schemaLocation` | ruta `abfss://` dinamica | Checkpoint donde AutoLoader persiste el esquema inferido |

**Columnas adicionales**:
- `FechaIngestaDatos` (timestamp): marca de tiempo generada con `current_timestamp()` en el momento de la ingesta. Permite acumulacion historica y es la primera columna del Liquid Cluster.
- `_rescued_data` (string): columna automatica de AutoLoader que captura datos que no encajan en el esquema inferido.

**Propiedades Delta (5)**:

| Propiedad | Valor |
|-----------|-------|
| `delta.enableChangeDataFeed` | `"true"` |
| `delta.autoOptimize.autoCompact` | `"true"` |
| `delta.autoOptimize.optimizeWrite` | `"true"` |
| `delta.deletedFileRetentionDuration` | `"interval 30 days"` |
| `delta.logRetentionDuration` | `"interval 60 days"` |

**Patron closure**: Los valores `ruta_parquets_maestro` y `ruta_checkpoint_cmstfl` se calculan a nivel de modulo y quedan capturados por closure en la funcion `tabla_bronce_cmstfl()`. cloudpickle serializa automaticamente las variables del modulo sin necesidad de `spark.sparkContext.broadcast()`.

**Operaciones PySpark utilizadas**: `spark.readStream`, `.format()`, `.option()`, `.load()`, `.withColumn()`, `reordenar_columnas_liquid_cluster()`.

### LsdpBronceTrxpfl.py

**Proposito**: Ingestar el parquet Transaccional (TRXPFL) a la tabla streaming `bronce_dev.regional.trxpfl` via AutoLoader.

**Tabla creada**: `bronce_dev.regional.trxpfl`

**Decorador**: `@dp.table(name="trxpfl", ...)`

**Columnas**: 62 (60 campos AS400 + `FechaIngestaDatos` + `_rescued_data`)

**Liquid Cluster**: `["TRXDT", "CUSTID", "TRXTYP"]`

- `TRXDT`: fecha de transaccion, patron de consulta mas frecuente por rangos de fecha
- `CUSTID`: clave foranea al Maestro, optimiza joins y filtros por cliente
- `TRXTYP`: tipo de transaccion, optimiza filtros por CATM/DATM/PGSL en la capa oro

**Configuracion AutoLoader**: identica a cmstfl (cloudFiles.format=parquet, schemaEvolutionMode=addNewColumns, schemaLocation dinamico).

**Propiedades Delta**: las 5 propiedades estandar del proyecto (identicas a cmstfl).

**Patron closure**: `ruta_parquets_transaccional` y `ruta_checkpoint_trxpfl` capturadas por closure.

**Operaciones PySpark utilizadas**: `spark.readStream`, `.format()`, `.option()`, `.load()`, `.withColumn()`, `reordenar_columnas_liquid_cluster()`.

### LsdpBronceBlncfl.py

**Proposito**: Ingestar el parquet de Saldos de Clientes (BLNCFL) a la tabla streaming `bronce_dev.regional.blncfl` via AutoLoader.

**Tabla creada**: `bronce_dev.regional.blncfl`

**Decorador**: `@dp.table(name="blncfl", ...)`

**Columnas**: 102 (100 campos AS400 + `FechaIngestaDatos` + `_rescued_data`)

**Liquid Cluster**: `["FechaIngestaDatos", "CUSTID"]`

- `FechaIngestaDatos`: requerido como primera columna del Liquid Cluster por convencion del proyecto
- `CUSTID`: clave de dimension tipo 1 usada en el JOIN con cmstfl en la capa plata

**Configuracion AutoLoader**: identica a los otros scripts de bronce.

**Propiedades Delta**: las 5 propiedades estandar del proyecto.

**Patron closure**: `ruta_parquets_saldos` y `ruta_checkpoint_blncfl` capturadas por closure.

**Operaciones PySpark utilizadas**: `spark.readStream`, `.format()`, `.option()`, `.load()`, `.withColumn()`, `reordenar_columnas_liquid_cluster()`.

---

## Transformaciones — Medalla de Plata

Los dos scripts de plata crean vistas materializadas que transforman y enriquecen los datos de bronce. Usan `@dp.materialized_view` para lectura batch gestionada por LSDP.

### LsdpPlataClientesSaldos.py

**Proposito**: Crear la vista materializada `clientes_saldos_consolidados` consolidando `cmstfl` y `blncfl` como Dimension Tipo 1 con 175 columnas y 4 campos calculados.

**Vista creada**: `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados`

**Decoradores aplicados**:
- `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados", ...)`: nombre dinamico construido desde las variables de modulo
- `@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")`: elimina registros con identificador nulo

**Columnas**: 175 (71 de cmstfl renombradas + 100 de blncfl renombradas + 4 campos calculados)

**Liquid Cluster**: `["huella_identificacion_cliente", "identificador_cliente"]`

**Fuentes de lectura**: `spark.read.table(f"{catalogo_bronce}.regional.cmstfl")` y `spark.read.table(f"{catalogo_bronce}.regional.blncfl")` — lectura batch, LSDP gestiona la incrementalidad.

**Estrategia Dimension Tipo 1** (una fila por CUSTID con los datos mas recientes):
1. Window function `Window.partitionBy("CUSTID").orderBy(desc("FechaIngestaDatos"))`
2. `row_number().over(window_spec)` en cada tabla fuente
3. Filtrar `_rn == 1` para conservar solo el registro mas reciente por CUSTID
4. LEFT JOIN `cmstfl_latest` -> `blncfl_latest` por `col("c.CUSTID") == col("b.CUSTID")`

**Exclusion de columnas**: `_rescued_data`, `año`, `mes`, `dia` se excluyen antes del procesamiento.

**Campos calculados (4)**:

| Campo | Tipo | Logica |
|-------|------|--------|
| `clasificacion_riesgo_cliente` | string | CASE secuencial con 5 umbrales: RIESGO_CRITICO (nivel 04/05 AND puntaje < 500 AND mora = SI), RIESGO_ALTO (nivel 03/04/05 AND puntaje < 600 AND mora = SI), RIESGO_MEDIO (nivel 03/04/05 OR puntaje < 650 AND mora = SI), RIESGO_BAJO (nivel = 02 OR puntaje < 700), SIN_RIESGO |
| `categoria_saldo_disponible` | string | CASE secuencial con 5 umbrales: SALDO_PREMIUM (saldo >= 200000 AND limite >= 100000 AND segmento = VIP), SALDO_ALTO (>= 100000 AND >= 50000 AND segmento VIP/PREM), SALDO_MEDIO (>= 25000 AND >= 10000), SALDO_BAJO (>= 5000 OR limite >= 5000 AND segmento STD/BAS), SALDO_CRITICO |
| `perfil_actividad_bancaria` | string | Variable intermedia `dias_sin_transaccion = datediff(current_date(), fecha_ultima_transaccion)`. CASE: CLIENTE_INTEGRAL (dias <= 30 AND productos >= 8 AND estado = AC), ACTIVIDAD_ALTA (dias <= 90 AND productos >= 5), ACTIVIDAD_MEDIA (dias <= 180 AND productos >= 3), ACTIVIDAD_BAJA (dias <= 365 AND productos >= 1 AND estado AC/IN), INACTIVO |
| `huella_identificacion_cliente` | string | `sha2(col("identificador_cliente").cast("string"), 256)` — SHA2-256 del identificador; usada como primera columna del Liquid Cluster |

**Operaciones PySpark utilizadas**: `spark.read.table()`, `select`, `join`, `filter`, `withColumn`, `when/otherwise`, `sha2`, `datediff`, `current_date`, `abs`, `col`, `lit`, `row_number`, `desc`, `Window.partitionBy().orderBy()`, `isin`, `cast`.

### LsdpPlataTransacciones.py

**Proposito**: Crear la vista materializada `transacciones_enriquecidas` enriqueciendo `trxpfl` con 4 campos calculados numericos. Lectura batch sin filtros.

**Vista creada**: `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas`

**Decorador**: `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas", ...)`

**Columnas**: 65 (61 de trxpfl renombradas + 4 campos calculados)

**Liquid Cluster**: `["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]`

**Fuente de lectura**: `spark.read.table(f"{catalogo_bronce}.regional.trxpfl")` — lectura batch sin filtros. LSDP gestiona la actualizacion incremental automaticamente.

**Exclusion de columnas**: `_rescued_data`, `año`, `mes`, `dia`.

**Campos calculados (4)**:

| Campo | Tipo | Formula | Manejo de Nulos |
|-------|------|---------|-----------------|
| `monto_neto_comisiones` | double | `NETAMT - (FEEAMT + TAXAMT)` | `coalesce` a 0 en cada operando |
| `porcentaje_comision_sobre_monto` | double | `(FEEAMT / ORGAMT) * 100` | `when(ORGAMT IS NULL OR == 0, 0)` para division por cero |
| `variacion_saldo_transaccion` | double | `abs(BLNAFT - BLNBFR)` | `coalesce` a 0 en cada operando |
| `indicador_impacto_financiero` | double | `TRXAMT + FEEAMT + TAXAMT + PNLAMT` | `coalesce` a 0 en cada operando |

**Operaciones PySpark utilizadas**: `spark.read.table()`, `select`, `withColumn`, `when`, `otherwise`, `coalesce`, `abs`, `col`, `lit`, `reordenar_columnas_liquid_cluster()`.

---

## Transformaciones — Medalla de Oro

El script `LsdpOroClientes.py` crea dos vistas materializadas de la capa de oro en un mismo archivo, compartiendo el mismo bloque de lectura de parametros (closure compartido).

### LsdpOroClientes.py

**Proposito**: Crear las 2 vistas materializadas de la medalla de oro que representan el producto de datos final del pipeline LSDP.

#### Vista 1: comportamiento_atm_cliente

**Vista creada**: `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente`

**Decoradores aplicados**:
- `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente", ...)`
- `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")`

**Columnas**: 6

**Liquid Cluster**: `["identificador_cliente"]`

**Fuente**: `spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")` — lectura batch sin filtros.

**Logica de agregacion**: `groupBy("identificador_cliente").agg(...)` en una sola pasada sobre los datos usando agregaciones condicionales:

| Metrica | Formula de Agregacion |
|---------|----------------------|
| `cantidad_depositos_atm` | `count(when(tipo_transaccion == "CATM", 1))` — retorna 0 si no hay CATM |
| `cantidad_retiros_atm` | `count(when(tipo_transaccion == "DATM", 1))` — retorna 0 si no hay DATM |
| `promedio_monto_depositos_atm` | `coalesce(avg(when("CATM", coalesce(monto, 0))), 0)` — doble coalesce |
| `promedio_monto_retiros_atm` | `coalesce(avg(when("DATM", coalesce(monto, 0))), 0)` — doble coalesce |
| `total_pagos_saldo_cliente` | `coalesce(spark_sum(when("PGSL", coalesce(monto, 0))), 0)` — doble coalesce |

**Doble coalesce en avg/sum**: el primer coalesce convierte montos nulos a 0 antes del calculo; el segundo coalesce convierte el NULL resultante de `avg()`/`sum()` sobre un grupo sin coincidencias en 0.

**Operaciones PySpark utilizadas**: `spark.read.table()`, `groupBy`, `agg`, `count`, `avg`, `spark_sum`, `when`, `coalesce`, `col`, `lit`, `reordenar_columnas_liquid_cluster()`.

#### Vista 2: resumen_integral_cliente

**Vista creada**: `{catalogoOro}.{esquema_oro}.resumen_integral_cliente`

**Decorador**: `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente", ...)`

**Columnas**: 22

**Liquid Cluster**: `["huella_identificacion_cliente", "identificador_cliente"]`

**Fuentes**:
1. `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` — se leen 17 columnas seleccionadas
2. `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente` — 5 metricas ATM

**Dependencia DAG**: LSDP resuelve automaticamente la dependencia entre vistas del mismo pipeline — `comportamiento_atm_cliente` se procesa antes que `resumen_integral_cliente` porque esta aparece primero en el archivo y es referenciada como fuente.

**LEFT JOIN**: `clientes_saldos_consolidados.join(comportamiento_atm_cliente, on="identificador_cliente", how="left")`. Preserva TODOS los clientes de plata incluyendo los sin transacciones ATM o PGSL.

**Coalesce a 0 para las 5 metricas**: los clientes sin transacciones tienen NULL en las columnas del ATM despues del LEFT JOIN. Las 5 metricas se coalescan a 0 para facilitar el consumo downstream.

**Campos seleccionados de plata (17)**: 5 identificativos (`identificador_cliente`, `huella_identificacion_cliente`, `nombre_completo_cliente`, `tipo_documento_identidad`, `numero_documento_identidad`), 4 demograficos (`segmento_cliente`, `categoria_cliente`, `ciudad_residencia`, `pais_residencia`), 3 de clasificacion calculados en plata (`clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria`), 5 financieros (`saldo_disponible`, `saldo_actual`, `limite_credito`, `puntaje_crediticio`, `ingreso_anual_declarado`).

**Operaciones PySpark utilizadas**: `spark.read.table()`, `select`, `join`, `withColumn`, `coalesce`, `col`, `lit`, `reordenar_columnas_liquid_cluster()`.

---

## Paradigma Declarativo LSDP

### Decoradores @dp.table y @dp.materialized_view

El modulo `pyspark.pipelines` (importado como `dp`) expone los decoradores de la API de Lakeflow Spark Declarative Pipelines. Un script decorado con `@dp.table` o `@dp.materialized_view` define la logica de transformacion como una funcion Python que retorna un DataFrame; LSDP se encarga de crear la tabla, gestionar la escritura incremental y orquestar las dependencias.

| Decorador | Tipo de tabla Delta | Modo de lectura de fuentes | Actualizacion |
|-----------|--------------------|-----------------------------|---------------|
| `@dp.table` | Streaming Table | Streaming (readStream + AutoLoader) | Incremental append-only |
| `@dp.materialized_view` | Materialized View | Batch (spark.read.table) | Refresco completo gestionado por LSDP |

### Orquestacion del DAG de Dependencias

LSDP construye automaticamente el grafo aciclico dirigido (DAG) de dependencias entre tablas y vistas del mismo pipeline. Las dependencias se infieren de los nombres de tabla referenciados en `spark.read.table()` dentro de cada funcion decorada. No es necesario declarar dependencias explicitamente.

**Orden de ejecucion en el pipeline LSDP**:
1. Bronce: cmstfl, trxpfl, blncfl (pueden ejecutar en paralelo — sin dependencias entre si)
2. Plata: clientes_saldos_consolidados (depende de cmstfl y blncfl), transacciones_enriquecidas (depende de trxpfl)
3. Oro: comportamiento_atm_cliente (depende de transacciones_enriquecidas), resumen_integral_cliente (depende de clientes_saldos_consolidados y comportamiento_atm_cliente)

### Streaming Tables versus Vistas Materializadas

| Caracteristica | Streaming Table (@dp.table) | Materialized View (@dp.materialized_view) |
|----------------|-----------------------------|--------------------------------------------|
| Modo de escritura | Append-only incremental | Refresco completo por LSDP |
| Modo de lectura de fuentes | Streaming (AutoLoader) | Batch (spark.read.table) |
| Caso de uso en LSDP | Ingesta de parquets AS400 (bronce) | Transformaciones y agregaciones (plata, oro) |
| Acumulacion historica | Si — cada ingestión agrega filas con FechaIngestaDatos | No — la vista refleja el estado actual |

### Patron Closure como Reemplazo de Broadcast

En Computo Serverless de Databricks el acceso a `spark.sparkContext` esta prohibido (error `JVM_ATTRIBUTE_NOT_SUPPORTED`). El patron closure usado en LSDP es el reemplazo aprobado para distribuir datos a las funciones worker:

1. Los parametros de `dbo.Parametros` y las rutas `abfss://` se calculan a nivel de modulo (una sola vez al inicializar el pipeline)
2. Las variables del modulo quedan accesibles desde el scope de las funciones decoradas
3. cloudpickle (el serializador de Databricks) serializa automaticamente las variables capturadas por closure al distribuir la funcion a los workers

Este patron evita `spark.sparkContext.broadcast()` y es compatible con Serverless.

### Declaracion de Tablas en Catalogos Multiples

Cuando el nombre de la tabla en `@dp.materialized_view` incluye el catalogo y esquema completo (ej: `plata_dev.regional.clientes_saldos_consolidados`), la tabla se crea en ese catalogo/esquema independientemente del catalogo por defecto configurado en el pipeline. Esto permite que un unico pipeline LSDP escriba en bronce, plata y oro simultaneamente.

Para leer desde un catalogo diferente al por defecto en `spark.read.table()`, se usa el nombre completo: `spark.read.table(f"{catalogo_bronce}.regional.cmstfl")`.

---

## Propiedades Delta

Todas las tablas y vistas del pipeline LSDP aplican las mismas 5 propiedades Delta estandar configuradas en el parametro `table_properties` de los decoradores.

### Change Data Feed (enableChangeDataFeed)

```
"delta.enableChangeDataFeed": "true"
```

Habilita el feed de cambios Delta (CDF) para la tabla. Permite a los consumidores downstream leer solo los cambios (inserts, updates, deletes) en lugar de la tabla completa, habilitando procesamiento incremental eficiente.

### autoOptimize.autoCompact

```
"delta.autoOptimize.autoCompact": "true"
```

Activa la compactacion automatica de archivos Parquet pequenos en la tabla Delta tras cada escritura. Mejora el rendimiento de lectura al reducir el numero de archivos que Spark debe abrir.

### autoOptimize.optimizeWrite

```
"delta.autoOptimize.optimizeWrite": "true"
```

Optimiza automaticamente el tamano de los archivos escritos para que sean cercanos al tamano optimo (128 MB por defecto). Evita el problema de archivos demasiado pequenos en escrituras de alta frecuencia.

### Liquid Cluster (cluster_by)

Liquid Cluster es el mecanismo de clustering de datos de Delta Lake que reemplaza a ZOrder y PartitionBy:

- **Por que se usa en lugar de ZOrder**: Liquid Cluster es incremental y no requiere `OPTIMIZE` manual periodico. ZOrder requiere reorganizar toda la tabla.
- **Por que se usa en lugar de PartitionBy**: PartitionBy no es adecuado para columnas de alta cardinalidad como CUSTIDs. Liquid Cluster funciona con cualquier cardinalidad.
- **Como se define**: con el parametro `cluster_by=["col1", "col2"]` en el decorador `@dp.table` o `@dp.materialized_view`.
- **Restriccion estadisticas min/max**: Delta Lake recopila estadisticas solo para las primeras 32 columnas. Las columnas del cluster deben estar entre las primeras 32 para que Delta pueda usarlas en file skipping. La funcion `reordenar_columnas_liquid_cluster()` garantiza esta condicion.

### delta.deletedFileRetentionDuration

```
"delta.deletedFileRetentionDuration": "interval 30 days"
```

Retiene los archivos Parquet eliminados logicamente durante 30 dias. Permite ejecutar consultas de `time travel` hasta 30 dias en el pasado y protege contra eliminaciones accidentales.

### delta.logRetentionDuration

```
"delta.logRetentionDuration": "interval 60 days"
```

Retiene el log de transacciones Delta durante 60 dias. Habilita auditoria, time travel de larga duracion y recuperacion ante fallos hasta 60 dias atras.

---

## Estrategia de Pruebas TDD

El proyecto sigue la metodologia Test-Driven Development (TDD). Las pruebas se implementan como notebooks Databricks en la carpeta `tests/` y se ejecutan usando la extension de Databricks para VS Code.

### Que se Prueba por Capa

| Capa | Pruebas realizadas |
|------|-------------------|
| Generacion de Parquets | Validacion de esquema, volumetria (5M/15M/1:1), tipos de datos, que los CUSTIDs son unicos en maestro, que los CUSTIDs en transaccional existen en el maestro, que el formato del parquet es correcto |
| Utilidades | Firma de funcion, retorno correcto de tipos, manejo de parametros invalidos, comportamiento del closure, compatibilidad Serverless (sin sparkContext) |
| Bronce | Que la tabla se crea con el numero correcto de columnas, que FechaIngestaDatos tiene valores no nulos, que el esquema coincide con el esperado, que el nombre de la tabla en Unity Catalog es correcto |
| Plata | Que la vista tiene el numero correcto de columnas y filas, que los campos calculados retornan los valores esperados para casos conocidos, que expect_or_drop elimina registros con CUSTID nulo, que el LEFT JOIN conserva todos los clientes del maestro |
| Oro | Que las metricas ATM se calculan correctamente para clientes con y sin transacciones, que el coalesce a 0 funciona para clientes sin CATM/DATM/PGSL, que resumen_integral_cliente tiene exactamente 22 columnas |

### Estructura de la Carpeta tests/

```
tests/
  GenerarParquets/
    NbTestMaestroCliente.py        -- Pruebas para NbGenerarMaestroCliente.py
    NbTestSaldosCliente.py         -- Pruebas para NbGenerarSaldosCliente.py
    NbTestTransaccionalCliente.py  -- Pruebas para NbGenerarTransaccionalCliente.py
  LSDP_Laboratorio_Basico/
    NbTestBronceCmstfl.py          -- Pruebas para LsdpBronceCmstfl.py
    NbTestBronceTrxpfl.py          -- Pruebas para LsdpBronceTrxpfl.py
    NbTestBronceBlncfl.py          -- Pruebas para LsdpBronceBlncfl.py
    NbTestPlataClientesSaldos.py   -- Pruebas para LsdpPlataClientesSaldos.py
    NbTestPlataTransacciones.py    -- Pruebas para LsdpPlataTransacciones.py
    NbTestOroClientes.py           -- Pruebas para LsdpOroClientes.py
    NbTestConexionAzureSql.py      -- Pruebas para LsdpConexionAzureSql.py
    NbTestConexionAzureSqlV4.py    -- Pruebas de regresion V4 para la conexion SQL
    NbTestConstructorRutasAbfss.py -- Pruebas para LsdpConstructorRutasAbfss.py
    NbTestReordenarColumnasLC.py   -- Pruebas para LsdpReordenarColumnasLiquidCluster.py
```

### Como Ejecutar las Pruebas

1. Abrir VS Code con la extension de Databricks instalada y configurada
2. Conectar el workspace de Databricks en la extension
3. Navegar al notebook de prueba en `tests/`
4. Ejecutar el notebook usando "Run All" en la interfaz de la extension o celda por celda para depuracion
5. Revisar la salida de cada celda — las pruebas fallidas muestran el mensaje de error y el valor esperado vs. el obtenido
