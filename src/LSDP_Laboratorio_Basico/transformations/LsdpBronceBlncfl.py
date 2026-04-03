# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # LsdpBronceBlncfl — Tabla Streaming de Bronce: Saldos de Clientes
# MAGIC
# MAGIC **Proposito**: Ingestar los parquets de Saldos de Clientes (BLNCFL — Balance Client File)
# MAGIC desde ADLS Gen2 hacia la tabla streaming `bronce_dev.regional.blncfl` usando AutoLoader
# MAGIC con acumulacion historica incremental.
# MAGIC
# MAGIC **Patron — Closure**:
# MAGIC Los parametros de Azure SQL y las rutas `abfss://` se calculan UNA SOLA VEZ
# MAGIC a nivel de modulo (al inicializar el pipeline). Los valores quedan capturados
# MAGIC como variables de modulo en el closure de la funcion decorada con `@dp.table`.
# MAGIC Este patron reemplaza `spark.sparkContext.broadcast()` que no es compatible
# MAGIC con Computo Serverless (RF-011).
# MAGIC
# MAGIC **Resultado esperado**:
# MAGIC - Tabla: `bronce_dev.regional.blncfl`
# MAGIC - Columnas: 102 (100 AS400 + FechaIngestaDatos + _rescued_data)
# MAGIC - Liquid Cluster: [`FechaIngestaDatos`, `CUSTID`]
# MAGIC - Acumulacion: Append-only historica via AutoLoader incremental

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importacion de Utilidades LSDP
# MAGIC
# MAGIC Se importan directamente las funciones de utilidad desde el paquete `utilities`.
# MAGIC - `LsdpConexionAzureSql`: importa `leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)`
# MAGIC - `LsdpConstructorRutasAbfss`: importa `construir_ruta_abfss(contenedor, datalake, directorio_raiz, ruta_relativa)`
# MAGIC - `LsdpReordenarColumnasLiquidCluster`: importa `reordenar_columnas_liquid_cluster(df, columnas_liquid_cluster)`

# COMMAND ----------

from utilities.LsdpConexionAzureSql import leer_parametros_azure_sql
from utilities.LsdpConstructorRutasAbfss import construir_ruta_abfss
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones de la API LSDP y PySpark

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura de Parametros del Pipeline (Nivel de Modulo)
# MAGIC
# MAGIC Este bloque se ejecuta UNA SOLA VEZ al inicializar el pipeline.
# MAGIC Lee el nombre del Scope Secret configurado como parametro del pipeline y usa
# MAGIC la funcion `leer_parametros_azure_sql` para obtener los 4 parametros de configuracion
# MAGIC de Azure SQL: `catalogoBronce`, `contenedorBronce`, `datalake`, `DirectorioBronce`.

# COMMAND ----------

# Leer el nombre del Scope Secret desde los parametros del pipeline LSDP
# Parametro del pipeline: nombreScopeSecret (ej: "sc-kv-laboratorio")
nombre_scope_secret = spark.conf.get("pipelines.parameters.nombreScopeSecret")

# Invocar la funcion de conexion Azure SQL para obtener los 4 parametros de configuracion.
# La funcion lee dbo.Parametros via JDBC usando 2 secretos del Scope Secret.
# Retorna: {"catalogoBronce": ..., "contenedorBronce": ..., "datalake": ..., "DirectorioBronce": ...}
parametros_sql = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construccion de Rutas abfss:// (Nivel de Modulo)
# MAGIC
# MAGIC Combina los parametros de Azure SQL con los parametros de ruta del pipeline para
# MAGIC construir las 2 rutas `abfss://` necesarias para la tabla `blncfl`:
# MAGIC - `ruta_parquets_saldos`: directorio de parquets de Saldos de Clientes
# MAGIC - `ruta_checkpoint_blncfl`: directorio de checkpoint (schemaLocation de AutoLoader)
# MAGIC
# MAGIC Los resultados quedan capturados por closure en la funcion `@dp.table` (RF-002).

# COMMAND ----------

# Leer las rutas relativas configuradas como parametros del pipeline LSDP
# Parametro del pipeline: rutaCompletaSaldoCliente (ej: "LSDP_Base/As400/SaldoCliente/")
ruta_relativa_parquets = spark.conf.get("pipelines.parameters.rutaCompletaSaldoCliente")

# Parametro del pipeline: rutaCheckpointBlncfl (ej: "LSDP_Base/Checkpoints/Bronce/blncfl/")
ruta_relativa_checkpoint = spark.conf.get("pipelines.parameters.rutaCheckpointBlncfl")

# Construir la ruta abfss:// completa al directorio de parquets de Saldos de Clientes.
# Ejemplo resultado: abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/SaldoCliente/
ruta_parquets_saldos = construir_ruta_abfss(
    parametros_sql["contenedorBronce"],
    parametros_sql["datalake"],
    parametros_sql["DirectorioBronce"],
    ruta_relativa_parquets
)

# Construir la ruta abfss:// completa para el checkpoint (schemaLocation) de blncfl.
# El checkpoint almacena la informacion de esquema inferida por AutoLoader (R7-D1).
# Ejemplo resultado: abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/Checkpoints/Bronce/blncfl/
ruta_checkpoint_blncfl = construir_ruta_abfss(
    parametros_sql["contenedorBronce"],
    parametros_sql["datalake"],
    parametros_sql["DirectorioBronce"],
    ruta_relativa_checkpoint
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicion de la Tabla Streaming `blncfl`
# MAGIC
# MAGIC Tabla `@dp.table` que ingesta los parquets de Saldos de Clientes via AutoLoader.
# MAGIC
# MAGIC **Configuracion AutoLoader** (R7-D1):
# MAGIC - `cloudFiles.format = "parquet"`: formato de los archivos fuente generados en V2
# MAGIC - `cloudFiles.schemaEvolutionMode = "addNewColumns"`: incorpora columnas nuevas automaticamente
# MAGIC - `cloudFiles.schemaLocation`: ruta `abfss://` dinamica para el checkpoint del esquema
# MAGIC
# MAGIC **Sin `schema` en `@dp.table`** (R7-D2): inferencia automatica de los parquets.
# MAGIC `_rescued_data` se incluye automaticamente por la inferencia de esquema.
# MAGIC
# MAGIC **Sin `@dp.expect`**: bronce es la capa de aterrizaje sin validacion de calidad.
# MAGIC
# MAGIC **Liquid Cluster** (R6-D1): [`FechaIngestaDatos`, `CUSTID`]
# MAGIC - `FechaIngestaDatos`: primer campo por requisito del usuario, optimiza consultas temporales
# MAGIC - `CUSTID`: FK al Maestro, clave de dimension tipo 1 en plata para la consolidacion cliente+saldo

# COMMAND ----------

@dp.table(
    name="blncfl",
    comment=(
        "Tabla streaming de bronce — Saldos de Clientes (Balance Client File). "
        "Ingesta historica acumulativa desde parquets AS400 via AutoLoader."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days"
    },
    cluster_by=["FechaIngestaDatos", "CUSTID"]
)
def tabla_bronce_blncfl():
    """
    Lee los parquets de Saldos de Clientes en modo streaming via AutoLoader
    y agrega la columna FechaIngestaDatos con la marca de tiempo de la ingesta.

    Las variables ruta_parquets_saldos y ruta_checkpoint_blncfl son del modulo
    y quedan capturadas por closure (cloudpickle) — sin spark.sparkContext (RF-011).

    Resultado: 102 columnas (100 AS400 + FechaIngestaDatos + _rescued_data automatica).
    """
    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_checkpoint_blncfl)
        .load(ruta_parquets_saldos)
        .withColumn("FechaIngestaDatos", current_timestamp())
    )
    return reordenar_columnas_liquid_cluster(df_stream, ["FechaIngestaDatos", "CUSTID"])
