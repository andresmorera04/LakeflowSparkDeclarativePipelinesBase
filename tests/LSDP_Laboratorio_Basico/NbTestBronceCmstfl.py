# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestBronceCmstfl — Suite de Pruebas TDD: Tabla Streaming cmstfl
# MAGIC
# MAGIC **Proposito**: Validar la correcta creacion e ingesta de la tabla streaming
# MAGIC `bronce_dev.regional.cmstfl` (Maestro de Clientes) por el pipeline LSDP de bronce.
# MAGIC
# MAGIC **Prerequisito**: Ejecutar el pipeline LSDP de bronce al menos una vez antes de
# MAGIC correr estas pruebas. La tabla `bronce_dev.regional.cmstfl` debe existir en Unity Catalog.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks.
# MAGIC
# MAGIC **Cobertura**:
# MAGIC 1. Tabla bronce_dev.regional.cmstfl existe en Unity Catalog
# MAGIC 2. Columna FechaIngestaDatos presente con tipo timestamp
# MAGIC 3. Columna _rescued_data presente con tipo string
# MAGIC 4. Propiedades Delta: CDF, autoOptimize, retenciones correctas
# MAGIC 5. Liquid Cluster configurado por [FechaIngestaDatos, CUSTID]
# MAGIC 6. Datos ingestados correctamente (conteo > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — La tabla bronce_dev.regional.cmstfl existe en Unity Catalog
# MAGIC
# MAGIC Verifica que el pipeline de bronce creo exitosamente la streaming table
# MAGIC en el catalogo y esquema configurados por defecto del pipeline.

# COMMAND ----------

try:
    df_cmstfl = spark.table("bronce_dev.regional.cmstfl")
    print("PRUEBA 1 PASA: La tabla bronce_dev.regional.cmstfl existe en Unity Catalog.")
except Exception as error:
    raise AssertionError(
        "ERROR — Prueba 1: La tabla bronce_dev.regional.cmstfl NO existe en Unity Catalog. "
        f"Error: {error}. "
        "Verificar que el pipeline LSDP de bronce se ejecuto correctamente y que "
        "las credenciales de Unity Catalog tienen permisos de lectura."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — Columna FechaIngestaDatos presente con tipo timestamp
# MAGIC
# MAGIC Verifica que la columna de marca de tiempo de ingesta existe y tiene el tipo correcto.
# MAGIC Esta columna es agregada por el pipeline via `withColumn("FechaIngestaDatos", current_timestamp())`.

# COMMAND ----------

columnas_actuales = df_cmstfl.columns

assert "FechaIngestaDatos" in columnas_actuales, (
    "ERROR — Prueba 2: La columna 'FechaIngestaDatos' no existe en la tabla cmstfl. "
    "Verificar que el pipeline incluye '.withColumn(\"FechaIngestaDatos\", current_timestamp())'."
)

tipo_fecha_ingesta = dict(df_cmstfl.dtypes)["FechaIngestaDatos"]
assert tipo_fecha_ingesta == "timestamp", (
    f"ERROR — Prueba 2: Se esperaba tipo 'timestamp' para FechaIngestaDatos "
    f"pero se encontro '{tipo_fecha_ingesta}'. "
    f"Verificar que current_timestamp() esta siendo usado en el pipeline."
)

print(f"PRUEBA 2 PASA: Columna 'FechaIngestaDatos' existe con tipo '{tipo_fecha_ingesta}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — Columna _rescued_data presente con tipo string
# MAGIC
# MAGIC Verifica que la columna de rescate de datos existe y tiene el tipo string.
# MAGIC Esta columna es agregada automaticamente por AutoLoader al inferir el esquema sin
# MAGIC proporcionarlo explicitamente en `@dp.table` (R7-D2).

# COMMAND ----------

assert "_rescued_data" in columnas_actuales, (
    "ERROR — Prueba 3: La columna '_rescued_data' no existe en la tabla cmstfl. "
    "Verificar que el pipeline no tiene un schema explicito en @dp.table (R7-D2) "
    "para permitir la inferencia automatica de esquema por AutoLoader."
)

tipo_rescued = dict(df_cmstfl.dtypes)["_rescued_data"]
assert tipo_rescued == "string", (
    f"ERROR — Prueba 3: Se esperaba tipo 'string' para _rescued_data "
    f"pero se encontro '{tipo_rescued}'."
)

print(f"PRUEBA 3 PASA: Columna '_rescued_data' existe con tipo '{tipo_rescued}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Propiedades Delta verificables
# MAGIC
# MAGIC Verifica que las propiedades Delta requeridas estan configuradas en la tabla:
# MAGIC Change Data Feed, autoOptimize (autoCompact y optimizeWrite), y retenciones.

# COMMAND ----------

# Obtener las propiedades de la tabla via DESCRIBE DETAIL
df_detalle = spark.sql("DESCRIBE DETAIL bronce_dev.regional.cmstfl")
propiedades_tabla = df_detalle.select("properties").collect()[0]["properties"]

# Verificar Change Data Feed habilitado
assert propiedades_tabla.get("delta.enableChangeDataFeed") == "true", (
    f"ERROR — Prueba 4: delta.enableChangeDataFeed no esta habilitado. "
    f"Propiedades actuales: {propiedades_tabla}"
)
print("  delta.enableChangeDataFeed = true  [PASA]")

# Verificar autoOptimize.autoCompact habilitado
assert propiedades_tabla.get("delta.autoOptimize.autoCompact") == "true", (
    f"ERROR — Prueba 4: delta.autoOptimize.autoCompact no esta habilitado. "
    f"Propiedades actuales: {propiedades_tabla}"
)
print("  delta.autoOptimize.autoCompact = true  [PASA]")

# Verificar autoOptimize.optimizeWrite habilitado
assert propiedades_tabla.get("delta.autoOptimize.optimizeWrite") == "true", (
    f"ERROR — Prueba 4: delta.autoOptimize.optimizeWrite no esta habilitado. "
    f"Propiedades actuales: {propiedades_tabla}"
)
print("  delta.autoOptimize.optimizeWrite = true  [PASA]")

# Verificar retencion de archivos eliminados (30 dias)
assert propiedades_tabla.get("delta.deletedFileRetentionDuration") == "interval 30 days", (
    f"ERROR — Prueba 4: delta.deletedFileRetentionDuration no es 'interval 30 days'. "
    f"Valor actual: '{propiedades_tabla.get('delta.deletedFileRetentionDuration')}'"
)
print("  delta.deletedFileRetentionDuration = interval 30 days  [PASA]")

# Verificar retencion de log (60 dias)
assert propiedades_tabla.get("delta.logRetentionDuration") == "interval 60 days", (
    f"ERROR — Prueba 4: delta.logRetentionDuration no es 'interval 60 days'. "
    f"Valor actual: '{propiedades_tabla.get('delta.logRetentionDuration')}'"
)
print("  delta.logRetentionDuration = interval 60 days  [PASA]")

print("PRUEBA 4 PASA: Todas las propiedades Delta estan correctamente configuradas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 5 — Liquid Cluster configurado por [FechaIngestaDatos, CUSTID]
# MAGIC
# MAGIC Verifica que las columnas de Liquid Cluster coinciden con la decision R6-D1:
# MAGIC FechaIngestaDatos (primer campo) y CUSTID (clave primaria de negocio).

# COMMAND ----------

df_detalle_cluster = spark.sql("DESCRIBE DETAIL bronce_dev.regional.cmstfl")
columnas_cluster = df_detalle_cluster.select("clusteringColumns").collect()[0]["clusteringColumns"]

columnas_cluster_esperadas = ["FechaIngestaDatos", "CUSTID"]

assert columnas_cluster == columnas_cluster_esperadas, (
    f"ERROR — Prueba 5: Las columnas de Liquid Cluster no coinciden.\n"
    f"  Esperadas: {columnas_cluster_esperadas}\n"
    f"  Actuales:  {columnas_cluster}\n"
    f"Verificar el parametro cluster_by en @dp.table del pipeline (R6-D1)."
)

print(f"PRUEBA 5 PASA: Liquid Cluster configurado correctamente por {columnas_cluster}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 6 — Datos ingestados correctamente (conteo > 0)
# MAGIC
# MAGIC Verifica que el pipeline ingesto registros en la tabla. Al menos 1 registro debe
# MAGIC existir para confirmar que AutoLoader proceso los parquets fuente correctamente.

# COMMAND ----------

conteo_registros = df_cmstfl.count()

assert conteo_registros > 0, (
    "ERROR — Prueba 6: La tabla cmstfl esta vacia (0 registros). "
    "Verificar que: (1) los parquets fuente existen en la ruta abfss:// configurada, "
    "(2) el pipeline se ejecuto correctamente, "
    "(3) la External Location en Unity Catalog permite acceso a los parquets."
)

print(f"PRUEBA 6 PASA: La tabla cmstfl tiene {conteo_registros:,} registros ingestados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Resultados

# COMMAND ----------

print("=" * 60)
print("RESUMEN — NbTestBronceCmstfl")
print("=" * 60)
print("Prueba 1: Tabla cmstfl existe en Unity Catalog      [PASA]")
print("Prueba 2: FechaIngestaDatos tipo timestamp          [PASA]")
print("Prueba 3: _rescued_data tipo string                 [PASA]")
print("Prueba 4: Propiedades Delta correctas               [PASA]")
print("Prueba 5: Liquid Cluster [FechaIngestaDatos, CUSTID][PASA]")
print("Prueba 6: Datos ingestados (conteo > 0)             [PASA]")
print("=" * 60)
print("RESULTADO FINAL: 6 DE 6 PRUEBAS PASARON.")
