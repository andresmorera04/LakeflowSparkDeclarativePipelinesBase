# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestReordenarColumnasLC — Suite de Pruebas TDD: Reordenamiento de Columnas para Liquid Cluster
# MAGIC
# MAGIC **Proposito**: Validar el correcto funcionamiento de la funcion `reordenar_columnas_liquid_cluster`
# MAGIC del archivo `LsdpReordenarColumnasLiquidCluster.py`, que reordena las columnas de un DataFrame
# MAGIC para garantizar que las columnas del Liquid Cluster queden entre las primeras 32 columnas
# MAGIC de la tabla Delta (requisito para que tengan estadisticas min/max).
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks.
# MAGIC
# MAGIC **Cobertura**:
# MAGIC 1. Las columnas del Liquid Cluster quedan al inicio del DataFrame
# MAGIC 2. El resto de columnas mantienen su orden original
# MAGIC 3. No se pierden ni duplican columnas despues del reordenamiento
# MAGIC 4. Error descriptivo cuando una columna del LC no existe en el DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de la Funcion a Probar
# MAGIC
# MAGIC Se carga el archivo Python `LsdpReordenarColumnasLiquidCluster.py` agregando el directorio
# MAGIC de utilidades al `sys.path` y usando `import` estandar de Python.
# MAGIC La ruta del directorio se recibe via el widget `rutaUtilidades`.

# COMMAND ----------

# Widget para la ruta del directorio de utilidades (.py) en el Workspace
# Ejemplo: /Workspace/Users/usuario@dominio.com/LSDP_Laboratorio_Basico/src/LSDP_Laboratorio_Basico/utilities
dbutils.widgets.text(
    "rutaUtilidades",
    "",
    "Ruta del directorio de utilidades (.py)"
)

ruta_utilidades = dbutils.widgets.get("rutaUtilidades").strip().rstrip("/")
assert ruta_utilidades, (
    "ERROR: El widget 'rutaUtilidades' esta vacio. "
    "Proporcionar la ruta del directorio que contiene los archivos .py de utilidades "
    "(ej: /Workspace/Users/usuario@dominio.com/LSDP_Laboratorio_Basico/src/LSDP_Laboratorio_Basico/utilities)."
)

import sys

# Agregar el directorio de utilidades al path de Python para importar archivos .py
if ruta_utilidades not in sys.path:
    sys.path.insert(0, ruta_utilidades)

from LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

print(f"Funcion 'reordenar_columnas_liquid_cluster' importada exitosamente desde: {ruta_utilidades}/LsdpReordenarColumnasLiquidCluster.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion de Datos de Prueba
# MAGIC
# MAGIC Se crea un DataFrame de prueba que simula la estructura de una tabla de bronce
# MAGIC con muchas columnas, donde las columnas del Liquid Cluster NO estan al inicio.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# Simular un DataFrame con 40 columnas donde FechaIngestaDatos y CUSTID NO estan al inicio
columnas_as400 = [f"COL_{str(i).zfill(2)}" for i in range(1, 39)]
datos = [tuple(f"val_{i}" for i in range(1, 39))]

df_prueba_base = spark.createDataFrame(datos, columnas_as400)

# Agregar las columnas del LC al final (como ocurre naturalmente con withColumn)
df_prueba = (
    df_prueba_base
    .withColumn("FechaIngestaDatos", current_timestamp())
    .withColumn("CUSTID", lit("C12345"))
)

print(f"Columnas totales del DF de prueba: {len(df_prueba.columns)}")
print(f"Primeras 5 columnas ANTES del reordenamiento: {df_prueba.columns[:5]}")
print(f"Ultimas 5 columnas ANTES del reordenamiento: {df_prueba.columns[-5:]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Columnas del Liquid Cluster quedan al inicio del DataFrame
# MAGIC
# MAGIC Verifica que despues del reordenamiento, las columnas del LC son las primeras del schema.

# COMMAND ----------

columnas_lc = ["FechaIngestaDatos", "CUSTID"]
df_reordenado = reordenar_columnas_liquid_cluster(df_prueba, columnas_lc)

# Verificar que las primeras 2 columnas son las del LC en el orden dado
assert df_reordenado.columns[0] == "FechaIngestaDatos", (
    f"ERROR — Prueba 1: La primera columna deberia ser 'FechaIngestaDatos' pero es '{df_reordenado.columns[0]}'."
)
assert df_reordenado.columns[1] == "CUSTID", (
    f"ERROR — Prueba 1: La segunda columna deberia ser 'CUSTID' pero es '{df_reordenado.columns[1]}'."
)

print(f"PRUEBA 1 PASA: Las columnas del LC estan al inicio: {df_reordenado.columns[:2]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — El resto de columnas mantienen su orden original
# MAGIC
# MAGIC Verifica que las columnas que NO son del LC conservan el mismo orden relativo.

# COMMAND ----------

columnas_restantes = df_reordenado.columns[2:]
columnas_originales_sin_lc = [c for c in df_prueba.columns if c not in columnas_lc]

assert columnas_restantes == columnas_originales_sin_lc, (
    f"ERROR — Prueba 2: El orden de las columnas restantes no coincide con el original.\n"
    f"  Esperado: {columnas_originales_sin_lc[:5]}...\n"
    f"  Obtenido: {columnas_restantes[:5]}..."
)

print(f"PRUEBA 2 PASA: Las {len(columnas_restantes)} columnas restantes mantienen su orden original.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — No se pierden ni duplican columnas
# MAGIC
# MAGIC Verifica que el numero total de columnas es el mismo y no hay duplicados.

# COMMAND ----------

assert len(df_reordenado.columns) == len(df_prueba.columns), (
    f"ERROR — Prueba 3: El numero de columnas cambio despues del reordenamiento. "
    f"Original: {len(df_prueba.columns)}, Reordenado: {len(df_reordenado.columns)}."
)

columnas_unicas = set(df_reordenado.columns)
assert len(columnas_unicas) == len(df_reordenado.columns), (
    f"ERROR — Prueba 3: Hay columnas duplicadas despues del reordenamiento."
)

print(f"PRUEBA 3 PASA: {len(df_reordenado.columns)} columnas sin perdidas ni duplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Error descriptivo para columna inexistente en el LC
# MAGIC
# MAGIC Verifica que la funcion lanza un ValueError claro cuando se pasa una columna
# MAGIC del Liquid Cluster que no existe en el DataFrame.

# COMMAND ----------

columnas_lc_invalidas = ["FechaIngestaDatos", "COLUMNA_QUE_NO_EXISTE"]
error_capturado = False

try:
    reordenar_columnas_liquid_cluster(df_prueba, columnas_lc_invalidas)
except ValueError as error_esperado:
    error_capturado = True
    mensaje = str(error_esperado)
    assert "COLUMNA_QUE_NO_EXISTE" in mensaje, (
        f"ERROR — Prueba 4: El mensaje de error no menciona la columna faltante."
    )
    print(f"PRUEBA 4 PASA: Error descriptivo capturado para columna inexistente.")
    print(f"  Mensaje: {mensaje[:200]}")

assert error_capturado, (
    f"ERROR — Prueba 4: La funcion no lanzo ValueError para columna inexistente."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 5 — Funciona con 3 columnas de LC (patron trxpfl)
# MAGIC
# MAGIC Verifica que el reordenamiento funciona correctamente con 3 columnas de LC,
# MAGIC simulando el patron de la tabla trxpfl con TRXDT, CUSTID, TRXTYP.

# COMMAND ----------

df_prueba_trx = (
    df_prueba_base
    .withColumn("TRXDT", lit("2026-01-15"))
    .withColumn("CUSTID", lit("C12345"))
    .withColumn("TRXTYP", lit("CATM"))
)

columnas_lc_trx = ["TRXDT", "CUSTID", "TRXTYP"]
df_reord_trx = reordenar_columnas_liquid_cluster(df_prueba_trx, columnas_lc_trx)

assert df_reord_trx.columns[:3] == columnas_lc_trx, (
    f"ERROR — Prueba 5: Las primeras 3 columnas no son las del LC. "
    f"Obtenido: {df_reord_trx.columns[:3]}."
)
assert len(df_reord_trx.columns) == len(df_prueba_trx.columns), (
    f"ERROR — Prueba 5: Se perdieron o duplicaron columnas."
)

print(f"PRUEBA 5 PASA: Reordenamiento correcto con 3 columnas de LC: {df_reord_trx.columns[:3]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Pruebas

# COMMAND ----------

print("=" * 60)
print("RESUMEN: NbTestReordenarColumnasLC")
print("=" * 60)
print("  Prueba 1: Columnas LC al inicio del DataFrame        — PASA")
print("  Prueba 2: Resto de columnas mantienen orden original  — PASA")
print("  Prueba 3: Sin perdidas ni duplicados de columnas      — PASA")
print("  Prueba 4: Error descriptivo para columna inexistente  — PASA")
print("  Prueba 5: Funciona con 3 columnas LC (patron trxpfl)  — PASA")
print("=" * 60)
print("RESULTADO: TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
