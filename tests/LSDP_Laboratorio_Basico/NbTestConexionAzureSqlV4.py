# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestConexionAzureSqlV4 — Suite de Pruebas TDD: Refactorizacion RF-019
# MAGIC
# MAGIC **Proposito**: Validar que la funcion `leer_parametros_azure_sql` refactorizada (RF-019)
# MAGIC retorna el diccionario COMPLETO de dbo.Parametros sin filtro, incluyendo la clave
# MAGIC `catalogoPlata` (V4) y manteniendo las 4 claves de bronce V3.
# MAGIC
# MAGIC **Prerequisito**: Infraestructura configurada (Azure SQL, Key Vault, Scope Secret).
# MAGIC La clave `catalogoPlata` debe estar insertada en dbo.Parametros.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks (sin spark.sparkContext).
# MAGIC
# MAGIC **Cobertura** (RF-019, CE-013):
# MAGIC 1. Diccionario retornado contiene las 4 claves de bronce V3
# MAGIC 2. Diccionario retornado contiene la clave `catalogoPlata` (nueva V4)
# MAGIC 3. La funcion no filtra claves — retorna todas las de dbo.Parametros
# MAGIC 4. No se lanza ValueError por claves faltantes (comportamiento V3 eliminado)
# MAGIC 5. Valor de `catalogoPlata` es una cadena no vacia

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

# Lectura y validacion de parametros de prueba
nombre_scope_secret_prueba = dbutils.widgets.get("nombre_scope_secret")
assert nombre_scope_secret_prueba.strip() != "", (
    "ERROR: El parametro 'nombre_scope_secret' no puede estar vacio. "
    "Configurar el widget con el nombre del Scope Secret correcto."
)

ruta_utilidades = dbutils.widgets.get("rutaUtilidades").strip().rstrip("/")
assert ruta_utilidades, (
    "ERROR: El widget 'rutaUtilidades' esta vacio. "
    "Proporcionar la ruta del directorio que contiene los archivos .py de utilidades."
)

print(f"Scope Secret a usar en las pruebas: '{nombre_scope_secret_prueba}'")
print(f"Ruta de utilidades: '{ruta_utilidades}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de la Funcion a Probar (RF-019 refactorizada)

# COMMAND ----------

import sys

# Agregar el directorio de utilidades al path de Python
if ruta_utilidades not in sys.path:
    sys.path.insert(0, ruta_utilidades)

from LsdpConexionAzureSql import leer_parametros_azure_sql

print(f"Funcion 'leer_parametros_azure_sql' importada exitosamente desde: {ruta_utilidades}/LsdpConexionAzureSql.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invocacion de la Funcion (Una Sola Vez para Todas las Pruebas)
# MAGIC
# MAGIC Se invoca la funcion una vez y se reutiliza el resultado en todas las pruebas
# MAGIC para evitar multiples conexiones JDBC a Azure SQL.

# COMMAND ----------

try:
    parametros_completos = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret_prueba)
    print(f"Funcion ejecutada exitosamente. Claves obtenidas: {list(parametros_completos.keys())}")
except Exception as error:
    raise AssertionError(
        "ERROR — Invocacion: La funcion leer_parametros_azure_sql fallo al ejecutarse. "
        f"Error: {error}. "
        "Verificar conexion JDBC a Azure SQL y que los secretos del Scope Secret esten configurados."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Diccionario contiene las 4 claves de bronce V3 (Compatibilidad)
# MAGIC
# MAGIC Verifica que la refactorizacion no rompe la compatibilidad con los scripts de bronce V3.
# MAGIC Las 4 claves originales deben seguir presentes en el diccionario retornado.

# COMMAND ----------

claves_bronce_v3 = ["catalogoBronce", "contenedorBronce", "datalake", "DirectorioBronce"]

for clave in claves_bronce_v3:
    assert clave in parametros_completos, (
        f"ERROR — Prueba 1: La clave '{clave}' de bronce V3 NO esta presente en el "
        f"diccionario retornado. Claves disponibles: {list(parametros_completos.keys())}. "
        f"La refactorizacion RF-019 no debe eliminar las claves existentes de bronce V3."
    )
    assert parametros_completos[clave] is not None and str(parametros_completos[clave]).strip() != "", (
        f"ERROR — Prueba 1: La clave '{clave}' esta presente pero su valor es nulo o vacio. "
        f"Verificar que dbo.Parametros tiene valores correctos para todas las claves de bronce V3."
    )

print(f"PRUEBA 1 PASA: Las 4 claves de bronce V3 estan presentes con valores no vacios: {claves_bronce_v3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — Diccionario contiene la clave `catalogoPlata` (Nueva V4)
# MAGIC
# MAGIC Verifica que la funcion refactorizada retorna la nueva clave `catalogoPlata`
# MAGIC insertada en dbo.Parametros para la medalla de plata V4.

# COMMAND ----------

assert "catalogoPlata" in parametros_completos, (
    "ERROR — Prueba 2: La clave 'catalogoPlata' NO esta presente en el diccionario retornado. "
    f"Claves disponibles: {list(parametros_completos.keys())}. "
    "Verificar que se ejecuto el INSERT en dbo.Parametros: "
    "INSERT INTO dbo.Parametros (Clave, Valor) VALUES ('catalogoPlata', 'plata_dev')."
)

valor_catalogo_plata = parametros_completos["catalogoPlata"]
assert valor_catalogo_plata is not None and str(valor_catalogo_plata).strip() != "", (
    f"ERROR — Prueba 2: La clave 'catalogoPlata' esta presente pero su valor es nulo o vacio. "
    f"Verificar el valor insertado en dbo.Parametros."
)

print(f"PRUEBA 2 PASA: Clave 'catalogoPlata' presente con valor: '{valor_catalogo_plata}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — La funcion retorna mas de 4 claves (No filtra — RF-019)
# MAGIC
# MAGIC Verifica que la funcion refactorizada retorna TODAS las claves de dbo.Parametros
# MAGIC (5 o mas: las 4 de bronce V3 + catalogoPlata V4), no solo las 4 originales.
# MAGIC Esto confirma que el filtro de V3 fue eliminado correctamente.

# COMMAND ----------

total_claves = len(parametros_completos)
assert total_claves >= 5, (
    f"ERROR — Prueba 3: El diccionario retornado contiene {total_claves} clave(s), "
    f"pero se esperaban al menos 5 (4 de bronce V3 + catalogoPlata V4). "
    f"Claves obtenidas: {list(parametros_completos.keys())}. "
    f"Verificar que la funcion retorna params_dict sin filtro (RF-019)."
)

print(f"PRUEBA 3 PASA: La funcion retorna {total_claves} claves sin filtro. RF-019 verificado.")
print(f"Claves en el diccionario: {list(parametros_completos.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — No se lanza ValueError por claves faltantes (Comportamiento V3 eliminado)
# MAGIC
# MAGIC Verifica que la funcion refactorizada NO lanza ValueError al encontrar claves
# MAGIC adicionales o al no tener la lista fija de claves requeridas. La funcion
# MAGIC debe aceptar cualquier conjunto de claves en dbo.Parametros.

# COMMAND ----------

# Si llegamos aqui sin excepcion, la prueba pasa (ya se invoco exitosamente arriba)
# Verificar adicionalmente que la funcion es un callable que retorna dict
assert isinstance(parametros_completos, dict), (
    f"ERROR — Prueba 4: La funcion no retorno un dict. Tipo obtenido: {type(parametros_completos)}."
)

print("PRUEBA 4 PASA: La funcion retorna un dict sin lanzar ValueError. Comportamiento V3 (filtro fijo) eliminado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 5 — Tipos de datos correctos para claves criticas
# MAGIC
# MAGIC Verifica que los valores retornados son cadenas de texto (str), no None ni otros tipos.

# COMMAND ----------

claves_criticas = ["catalogoBronce", "catalogoPlata"]

for clave in claves_criticas:
    valor = parametros_completos[clave]
    assert isinstance(valor, str), (
        f"ERROR — Prueba 5: El valor de '{clave}' deberia ser una cadena str "
        f"pero se obtuvo tipo {type(valor)} con valor: {valor}. "
        f"Verificar el tipo de dato en la tabla dbo.Parametros."
    )

print("PRUEBA 5 PASA: Valores de claves criticas son cadenas str correctas.")
print(f"  catalogoBronce  = '{parametros_completos['catalogoBronce']}'")
print(f"  catalogoPlata   = '{parametros_completos['catalogoPlata']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la Suite TDD

# COMMAND ----------

print("=" * 60)
print("RESUMEN — NbTestConexionAzureSqlV4")
print("=" * 60)
print("PRUEBA 1 PASA: 4 claves bronce V3 presentes (compatibilidad)")
print("PRUEBA 2 PASA: clave 'catalogoPlata' presente (nueva V4)")
print("PRUEBA 3 PASA: funcion retorna todas las claves sin filtro (RF-019)")
print("PRUEBA 4 PASA: sin ValueError por filtro fijo (comportamiento V3 eliminado)")
print("PRUEBA 5 PASA: tipos de datos correctos para claves criticas")
print("=" * 60)
print("TODAS LAS PRUEBAS PASARON. Refactorizacion RF-019 validada.")
print(f"Total de claves en dbo.Parametros: {len(parametros_completos)}")
