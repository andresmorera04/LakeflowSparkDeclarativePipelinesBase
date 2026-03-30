# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestConexionAzureSql — Suite de Pruebas TDD: Conexion Azure SQL
# MAGIC
# MAGIC **Proposito**: Validar el correcto funcionamiento de la funcion `leer_parametros_azure_sql`
# MAGIC del archivo `LsdpConexionAzureSql.py`, que lee los parametros de configuracion desde
# MAGIC la tabla `dbo.Parametros` en Azure SQL Server usando JDBC con 2 secretos.
# MAGIC
# MAGIC **Prerequisito**: Infraestructura configurada (Azure SQL, Key Vault, Scope Secret).
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks (sin spark.sparkContext).
# MAGIC
# MAGIC **Cobertura**:
# MAGIC 1. Conexion JDBC exitosa con los 2 secretos del Scope Secret
# MAGIC 2. Lectura de dbo.Parametros retorna las 4 claves esperadas
# MAGIC 3. Valores no vacios para cada clave retornada
# MAGIC 4. Manejo de error si nombre_scope_secret es invalido o inexistente

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracion de Parametros de Prueba

# COMMAND ----------

# Widget para parametrizar el nombre del Scope Secret en las pruebas
dbutils.widgets.text(
    "nombre_scope_secret",
    "sc-kv-laboratorio",
    "Nombre del Scope Secret de Databricks"
)

# Widget para la ruta del directorio de utilidades (.py) en el Workspace
# Ejemplo: /Workspace/Users/usuario@dominio.com/LSDP_Laboratorio_Basico/src/LSDP_Laboratorio_Basico/utilities
dbutils.widgets.text(
    "rutaUtilidades",
    "",
    "Ruta del directorio de utilidades (.py)"
)

# COMMAND ----------

# Lectura del parametro de prueba
nombre_scope_secret_prueba = dbutils.widgets.get("nombre_scope_secret")

# Validacion del parametro de entrada
assert nombre_scope_secret_prueba.strip() != "", (
    "ERROR: El parametro 'nombre_scope_secret' no puede estar vacio. "
    "Configurar el widget con el nombre del Scope Secret correcto."
)

print(f"Scope Secret a usar en las pruebas: '{nombre_scope_secret_prueba}'")

# Lectura de la ruta del directorio de utilidades
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
# MAGIC Se carga el archivo Python `LsdpConexionAzureSql.py` agregando el directorio
# MAGIC de utilidades al `sys.path` y usando `import` estandar de Python.
# MAGIC La ruta del directorio se recibe via el widget `rutaUtilidades`.

# COMMAND ----------

import sys

# Agregar el directorio de utilidades al path de Python para importar archivos .py
if ruta_utilidades not in sys.path:
    sys.path.insert(0, ruta_utilidades)

from LsdpConexionAzureSql import leer_parametros_azure_sql

print(f"Funcion 'leer_parametros_azure_sql' importada exitosamente desde: {ruta_utilidades}/LsdpConexionAzureSql.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Conexion JDBC exitosa con los 2 secretos del Scope Secret
# MAGIC
# MAGIC Verifica que la funcion puede obtener correctamente los 2 secretos del Key Vault
# MAGIC y establecer la conexion JDBC con Azure SQL Server sin errores.

# COMMAND ----------

try:
    resultado_prueba_1 = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret_prueba)
    prueba_1_exitosa = True
    print(f"PRUEBA 1 PASA: Conexion JDBC exitosa con el Scope Secret '{nombre_scope_secret_prueba}'.")
    print(f"  Claves retornadas: {list(resultado_prueba_1.keys())}")
except Exception as error:
    prueba_1_exitosa = False
    print(f"PRUEBA 1 FALLA: No se pudo conectar a Azure SQL con el Scope Secret '{nombre_scope_secret_prueba}'.")
    print(f"  Error: {error}")

assert prueba_1_exitosa, (
    f"FALLO FATAL — Prueba 1: La funcion leer_parametros_azure_sql no pudo conectarse "
    f"a Azure SQL usando el Scope Secret '{nombre_scope_secret_prueba}'. "
    f"Verificar: (1) que el Scope Secret existe, (2) que los secretos "
    f"sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd estan configurados, "
    f"(3) que la cadena JDBC es valida y que Azure SQL es accesible."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — Lectura de dbo.Parametros retorna exactamente las 4 claves esperadas
# MAGIC
# MAGIC Verifica que el diccionario retornado contiene exactamente las 4 claves que
# MAGIC el pipeline de bronce necesita para construir rutas abfss:// y acceder a Unity Catalog.

# COMMAND ----------

claves_esperadas = ["catalogoBronce", "contenedorBronce", "datalake", "DirectorioBronce"]
parametros_obtenidos = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret_prueba)

# Verificar que el resultado es un diccionario
assert isinstance(parametros_obtenidos, dict), (
    f"ERROR — Prueba 2: Se esperaba un dict pero se obtuvo {type(parametros_obtenidos)}."
)

# Verificar que contiene exactamente las 4 claves requeridas
for clave in claves_esperadas:
    assert clave in parametros_obtenidos, (
        f"ERROR — Prueba 2: La clave '{clave}' no esta presente en el diccionario retornado. "
        f"Claves disponibles: {list(parametros_obtenidos.keys())}. "
        f"Verificar que dbo.Parametros contiene el registro con Clave='{clave}'."
    )

print(f"PRUEBA 2 PASA: El diccionario contiene las 4 claves requeridas: {claves_esperadas}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — Valores no vacios para cada clave retornada
# MAGIC
# MAGIC Verifica que ninguna de las 4 claves tiene un valor nulo o cadena vacia.
# MAGIC Los valores vacios causarian rutas abfss:// malformadas o errores de conexion.

# COMMAND ----------

parametros_validar = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret_prueba)

for clave in claves_esperadas:
    valor = parametros_validar[clave]
    assert valor is not None, (
        f"ERROR — Prueba 3: El valor de la clave '{clave}' es None. "
        f"Verificar que dbo.Parametros tiene un valor no nulo para Clave='{clave}'."
    )
    assert str(valor).strip() != "", (
        f"ERROR — Prueba 3: El valor de la clave '{clave}' esta vacio o solo tiene espacios. "
        f"Verificar el registro correspondiente en dbo.Parametros."
    )
    print(f"  Clave '{clave}': valor presente (no vacio). [PASA]")

print(f"PRUEBA 3 PASA: Los 4 valores de configuracion son no vacios.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Manejo de error con Scope Secret invalido o inexistente
# MAGIC
# MAGIC Verifica que la funcion lanza un error apropiado cuando se proporciona un nombre
# MAGIC de Scope Secret que no existe en Databricks. Esto valida el manejo de errores
# MAGIC de acceso al Key Vault.

# COMMAND ----------

nombre_scope_invalido = "scope-que-no-existe-xyz-prueba-tdd"
error_capturado = False

try:
    leer_parametros_azure_sql(spark, dbutils, nombre_scope_invalido)
except Exception as error_esperado:
    error_capturado = True
    print(f"PRUEBA 4 PASA: La funcion lanza error correctamente para scope invalido.")
    print(f"  Tipo de error: {type(error_esperado).__name__}")
    print(f"  Mensaje: {str(error_esperado)[:200]}")

assert error_capturado, (
    "ERROR — Prueba 4: La funcion NO lanzo ningun error al usar un Scope Secret inexistente. "
    "Se esperaba que dbutils.secrets.get falle con un scope invalido."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Resultados

# COMMAND ----------

print("=" * 60)
print("RESUMEN — NbTestConexionAzureSql")
print("=" * 60)
print("Prueba 1: Conexion JDBC exitosa                    [PASA]")
print("Prueba 2: Retorna las 4 claves esperadas           [PASA]")
print("Prueba 3: Valores no vacios para cada clave        [PASA]")
print("Prueba 4: Error con Scope Secret invalido          [PASA]")
print("=" * 60)
print("RESULTADO FINAL: 4 DE 4 PRUEBAS PASARON.")
