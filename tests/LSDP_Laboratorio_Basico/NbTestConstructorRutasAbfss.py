# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestConstructorRutasAbfss — Suite de Pruebas TDD: Constructor de Rutas abfss://
# MAGIC
# MAGIC **Proposito**: Validar el correcto funcionamiento de la funcion `construir_ruta_abfss`
# MAGIC del archivo `LsdpConstructorRutasAbfss.py`, que construye rutas `abfss://` completas
# MAGIC para Azure Data Lake Storage Gen2 combinando 4 componentes.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks (estas pruebas no requieren infraestructura
# MAGIC externa — solo prueban la logica de construccion de rutas).
# MAGIC
# MAGIC **Cobertura**:
# MAGIC 1. Formato abfss:// correcto con parametros de ejemplo
# MAGIC 2. La ruta resultante NO contiene /mnt/ (protocolo legacy prohibido)
# MAGIC 3. Componentes correctos en la ruta (protocolo, contenedor, storage account, directorio, relativa)
# MAGIC 4. Funciona tanto para rutas de parquets como para rutas de checkpoints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracion de Parametros

# COMMAND ----------

# Widget para la ruta del directorio de utilidades (.py) en el Workspace
# Ejemplo: /Workspace/Users/usuario@dominio.com/LSDP_Laboratorio_Basico/src/LSDP_Laboratorio_Basico/utilities
dbutils.widgets.text(
    "rutaUtilidades",
    "",
    "Ruta del directorio de utilidades (.py)"
)

# COMMAND ----------

# Lectura y validacion del parametro de entrada
ruta_utilidades = dbutils.widgets.get("rutaUtilidades").strip().rstrip("/")
assert ruta_utilidades, (
    "ERROR: El widget 'rutaUtilidades' esta vacio. "
    "Proporcionar la ruta del directorio que contiene los archivos .py de utilidades "
    "(ej: /Workspace/Users/usuario@dominio.com/LSDP_Laboratorio_Basico/src/LSDP_Laboratorio_Basico/utilities)."
)

print(f"Ruta de utilidades: '{ruta_utilidades}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de la Funcion a Probar
# MAGIC
# MAGIC Se carga el archivo Python `LsdpConstructorRutasAbfss.py` agregando el directorio
# MAGIC de utilidades al `sys.path` y usando `import` estandar de Python.
# MAGIC La ruta del directorio se recibe via el widget `rutaUtilidades`.

# COMMAND ----------

import sys

# Agregar el directorio de utilidades al path de Python para importar archivos .py
if ruta_utilidades not in sys.path:
    sys.path.insert(0, ruta_utilidades)

from LsdpConstructorRutasAbfss import construir_ruta_abfss

print(f"Funcion 'construir_ruta_abfss' importada exitosamente desde: {ruta_utilidades}/LsdpConstructorRutasAbfss.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros de Prueba (Valores de Ejemplo del Contrato)
# MAGIC
# MAGIC Los valores de prueba corresponden a los ejemplos documentados en
# MAGIC `specs/003-lsdp-medalla-bronce/data-model.md` y `contracts/pipeline-lsdp-bronce.md`.

# COMMAND ----------

# Parametros de ejemplo (equivalentes a los valores de dbo.Parametros en dev)
contenedor_prueba = "bronce"
datalake_prueba = "adlsg2datalakedev"
directorio_prueba = "archivos"

# Rutas relativas de ejemplo para parquets (del contrato del pipeline)
ruta_relativa_maestro = "LSDP_Base/As400/MaestroCliente/"
ruta_relativa_transaccional = "LSDP_Base/As400/Transaccional/"
ruta_relativa_saldos = "LSDP_Base/As400/SaldoCliente/"

# Rutas relativas de ejemplo para checkpoints (del contrato del pipeline)
ruta_relativa_checkpoint_cmstfl = "LSDP_Base/Checkpoints/Bronce/cmstfl/"
ruta_relativa_checkpoint_trxpfl = "LSDP_Base/Checkpoints/Bronce/trxpfl/"
ruta_relativa_checkpoint_blncfl = "LSDP_Base/Checkpoints/Bronce/blncfl/"

print("Parametros de prueba configurados correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Formato abfss:// correcto con parametros de ejemplo
# MAGIC
# MAGIC Verifica que la funcion retorna la ruta en el formato exacto requerido:
# MAGIC `abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}`

# COMMAND ----------

ruta_esperada_maestro = "abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/"

ruta_obtenida_maestro = construir_ruta_abfss(
    contenedor_prueba,
    datalake_prueba,
    directorio_prueba,
    ruta_relativa_maestro
)

assert ruta_obtenida_maestro == ruta_esperada_maestro, (
    f"ERROR — Prueba 1: La ruta generada no coincide con la esperada.\n"
    f"  Esperada: '{ruta_esperada_maestro}'\n"
    f"  Obtenida: '{ruta_obtenida_maestro}'"
)

print(f"PRUEBA 1 PASA: Formato abfss:// correcto.")
print(f"  Ruta generada: '{ruta_obtenida_maestro}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — La ruta resultante NO contiene /mnt/
# MAGIC
# MAGIC Verifica que ninguna de las 6 rutas posibles del pipeline contiene `/mnt/`.
# MAGIC Las rutas con `/mnt/` son el protocolo legacy de DBFS mounts, prohibido en
# MAGIC el proyecto (RF-011, Restriccion Critica: solo abfss://).

# COMMAND ----------

# Construir todas las rutas del pipeline de bronce (3 parquets + 3 checkpoints)
todas_las_rutas = [
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_maestro),
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_transaccional),
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_saldos),
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_checkpoint_cmstfl),
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_checkpoint_trxpfl),
    construir_ruta_abfss(contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_checkpoint_blncfl),
]

for ruta in todas_las_rutas:
    assert "/mnt/" not in ruta, (
        f"ERROR — Prueba 2: La ruta '{ruta}' contiene '/mnt/' que es el protocolo "
        f"legacy de DBFS mounts. Las rutas deben usar exclusivamente 'abfss://'."
    )
    print(f"  Ruta sin /mnt/: '{ruta}' [PASA]")

print(f"PRUEBA 2 PASA: Ninguna de las {len(todas_las_rutas)} rutas contiene /mnt/.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — Componentes correctos en la ruta
# MAGIC
# MAGIC Verifica que cada componente de la ruta esta correctamente posicionado:
# MAGIC protocolo `abfss://`, contenedor, `@`, storage account, `.dfs.core.windows.net/`,
# MAGIC directorio raiz y ruta relativa.

# COMMAND ----------

ruta_test = construir_ruta_abfss(
    contenedor_prueba,
    datalake_prueba,
    directorio_prueba,
    ruta_relativa_maestro
)

# Verificar que la ruta comienza con el protocolo abfss://
assert ruta_test.startswith("abfss://"), (
    f"ERROR — Prueba 3: La ruta no comienza con 'abfss://'. Ruta: '{ruta_test}'"
)

# Verificar que el contenedor esta correctamente posicionado despues de abfss://
assert f"abfss://{contenedor_prueba}@" in ruta_test, (
    f"ERROR — Prueba 3: El contenedor '{contenedor_prueba}' no esta correctamente "
    f"posicionado en la ruta. Ruta: '{ruta_test}'"
)

# Verificar que el storage account esta correctamente referenciado
assert f"{datalake_prueba}.dfs.core.windows.net" in ruta_test, (
    f"ERROR — Prueba 3: El storage account '{datalake_prueba}.dfs.core.windows.net' "
    f"no esta presente en la ruta. Ruta: '{ruta_test}'"
)

# Verificar que el directorio raiz esta incluido
assert f"/{directorio_prueba}/" in ruta_test, (
    f"ERROR — Prueba 3: El directorio raiz '{directorio_prueba}' no esta correctamente "
    f"incluido en la ruta. Ruta: '{ruta_test}'"
)

# Verificar que la ruta relativa esta al final
assert ruta_test.endswith(ruta_relativa_maestro), (
    f"ERROR — Prueba 3: La ruta relativa '{ruta_relativa_maestro}' no esta al "
    f"final de la ruta. Ruta: '{ruta_test}'"
)

print(f"PRUEBA 3 PASA: Todos los componentes de la ruta estan correctamente posicionados.")
print(f"  Protocolo: abfss://   [PASA]")
print(f"  Contenedor: {contenedor_prueba}  [PASA]")
print(f"  Storage Account: {datalake_prueba}.dfs.core.windows.net  [PASA]")
print(f"  Directorio raiz: {directorio_prueba}  [PASA]")
print(f"  Ruta relativa: {ruta_relativa_maestro}  [PASA]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Funciona tanto para rutas de parquets como para rutas de checkpoints
# MAGIC
# MAGIC Verifica que la misma funcion produce rutas validas para los 2 tipos de uso
# MAGIC del pipeline de bronce: directorios de parquets fuente y directorios de checkpoints.

# COMMAND ----------

# Prueba con ruta de parquet (directorio fuente)
ruta_parquet = construir_ruta_abfss(
    contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_maestro
)
assert "As400" in ruta_parquet, (
    f"ERROR — Prueba 4: La ruta de parquet no contiene 'As400'. Ruta: '{ruta_parquet}'"
)
print(f"  Ruta parquet generada: '{ruta_parquet}' [PASA]")

# Prueba con ruta de checkpoint (schemaLocation de AutoLoader)
ruta_checkpoint = construir_ruta_abfss(
    contenedor_prueba, datalake_prueba, directorio_prueba, ruta_relativa_checkpoint_cmstfl
)
assert "Checkpoints" in ruta_checkpoint, (
    f"ERROR — Prueba 4: La ruta de checkpoint no contiene 'Checkpoints'. "
    f"Ruta: '{ruta_checkpoint}'"
)
print(f"  Ruta checkpoint generada: '{ruta_checkpoint}' [PASA]")

# Verificar que las 2 rutas son diferentes (usan rutas relativas distintas)
assert ruta_parquet != ruta_checkpoint, (
    "ERROR — Prueba 4: Las rutas de parquet y checkpoint son identicas. "
    "Deben ser diferentes porque usan rutas relativas distintas."
)

print(f"PRUEBA 4 PASA: La funcion genera correctamente tanto rutas de parquets como de checkpoints.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Resultados

# COMMAND ----------

print("=" * 60)
print("RESUMEN — NbTestConstructorRutasAbfss")
print("=" * 60)
print("Prueba 1: Formato abfss:// correcto                [PASA]")
print("Prueba 2: Sin /mnt/ en ninguna ruta                [PASA]")
print("Prueba 3: Componentes correctamente posicionados   [PASA]")
print("Prueba 4: Funciona para parquets y checkpoints     [PASA]")
print("=" * 60)
print("RESULTADO FINAL: 4 DE 4 PRUEBAS PASARON.")
