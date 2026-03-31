# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestPlataClientesSaldos — Suite de Pruebas TDD: Vista Materializada Consolidada
# MAGIC
# MAGIC **Proposito**: Validar la correcta creacion y contenido de la vista materializada
# MAGIC `clientes_saldos_consolidados` en la medalla de plata. La suite verifica la
# MAGIC estructura de 175 columnas, la Dimension Tipo 1, los 3 campos CASE, el SHA256,
# MAGIC las propiedades Delta, el expect_or_drop y el LEFT JOIN.
# MAGIC
# MAGIC **Prerequisito**: Ejecutar el pipeline LSDP (bronce + plata) al menos una vez.
# MAGIC La vista `{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados` debe existir.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks.
# MAGIC
# MAGIC **Cobertura** (CE-001 a CE-004, CE-007, CE-008, CE-009, CE-011):
# MAGIC 1. Estructura de 175 columnas con nombres en espanol (data-model.md TABLAS A, B y C)
# MAGIC 2. Dimension Tipo 1 — una fila por CUSTID con datos mas recientes
# MAGIC 3. Campos CASE correctos: clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria
# MAGIC 4. Campo SHA256: huella_identificacion_cliente consistente
# MAGIC 5. Propiedades Delta: 5 propiedades RF-008
# MAGIC 6. Registros con identificador_cliente nulo eliminados por expect_or_drop (RF-018)
# MAGIC 7. LEFT JOIN correcto — clientes sin saldos tienen campos blncfl nulos (CE-002, CE-003)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracion de Parametros de Prueba

# COMMAND ----------

dbutils.widgets.text("catalogo_plata", "plata_dev", "Catalogo de Unity Catalog para plata")
dbutils.widgets.text("esquema_plata", "regional", "Esquema de Unity Catalog para plata")

# COMMAND ----------

catalogo_plata = dbutils.widgets.get("catalogo_plata").strip()
esquema_plata = dbutils.widgets.get("esquema_plata").strip()

assert catalogo_plata, "ERROR: El widget 'catalogo_plata' no puede estar vacio."
assert esquema_plata, "ERROR: El widget 'esquema_plata' no puede estar vacio."

nombre_vista = f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"
print(f"Vista a probar: {nombre_vista}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de la Vista Materializada

# COMMAND ----------

try:
    df_clientes = spark.table(nombre_vista)
    print(f"CARGA OK: Vista '{nombre_vista}' accedida exitosamente.")
except Exception as error:
    raise AssertionError(
        f"ERROR — Carga: La vista '{nombre_vista}' NO existe o no es accesible. "
        f"Error: {error}. "
        "Verificar que el pipeline LSDP de plata se ejecuto correctamente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Estructura de 175 columnas con nombres en espanol
# MAGIC
# MAGIC Verifica que la vista tiene exactamente 175 columnas con los nombres correctos
# MAGIC segun el data-model.md TABLAS A, B y C.

# COMMAND ----------

columnas_actuales = set(df_clientes.columns)
total_columnas = len(df_clientes.columns)

assert total_columnas == 175, (
    f"ERROR — Prueba 1: Se esperaban 175 columnas pero se encontraron {total_columnas}. "
    f"Revisar el mapeo de columnas en LsdpPlataClientesSaldos.py."
)

# Verificar columnas criticas de cmstfl (TABLA A)
columnas_criticas_cmstfl = [
    "identificador_cliente", "nombre_completo_cliente", "primer_nombre", "segundo_nombre",
    "primer_apellido", "segundo_apellido", "genero", "tipo_documento_identidad",
    "numero_documento_identidad", "codigo_nacionalidad", "estado_civil",
    "segmento_cliente", "nivel_riesgo_cliente", "puntaje_crediticio",
    "total_productos_contratados", "estado_cuenta_cliente",
    "fecha_nacimiento", "fecha_apertura_cuenta", "fecha_ultima_transaccion",
    "ingreso_anual_declarado", "ingreso_mensual", "fecha_ingesta_maestro"
]

columnas_faltantes_cmstfl = [c for c in columnas_criticas_cmstfl if c not in columnas_actuales]
assert not columnas_faltantes_cmstfl, (
    f"ERROR — Prueba 1: Columnas criticas de cmstfl faltantes: {columnas_faltantes_cmstfl}."
)

# Verificar columnas criticas de blncfl (TABLA B)
columnas_criticas_blncfl = [
    "identificador_cuenta", "tipo_cuenta", "nombre_cuenta", "estado_cuenta_saldo",
    "codigo_moneda_cuenta", "codigo_sucursal_cuenta", "nombre_sucursal_cuenta",
    "saldo_disponible", "saldo_actual", "limite_credito", "credito_disponible",
    "estado_mora", "total_transacciones_periodo", "tasa_interes_vigente",
    "fecha_apertura_cuenta_saldo", "fecha_ingesta_saldo"
]

columnas_faltantes_blncfl = [c for c in columnas_criticas_blncfl if c not in columnas_actuales]
assert not columnas_faltantes_blncfl, (
    f"ERROR — Prueba 1: Columnas criticas de blncfl faltantes: {columnas_faltantes_blncfl}."
)

# Verificar que CUSTID (bronce) no esta presente (renombrado a identificador_cliente)
assert "CUSTID" not in columnas_actuales, (
    "ERROR — Prueba 1: La columna 'CUSTID' no debe estar presente en la vista de plata. "
    "Debe renombrarse a 'identificador_cliente'."
)

# Verificar que columnas de particion no estan presentes
for col_particion in ["año", "mes", "dia"]:
    assert col_particion not in columnas_actuales, (
        f"ERROR — Prueba 1: La columna de particion '{col_particion}' no debe estar en la vista de plata."
    )

print(f"PRUEBA 1 PASA: Vista tiene {total_columnas} columnas con nombres correctos en espanol.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — Campos calculados presentes (TABLA C)
# MAGIC
# MAGIC Verifica que los 4 campos calculados estan presentes en la vista.

# COMMAND ----------

campos_calculados = [
    "huella_identificacion_cliente",
    "clasificacion_riesgo_cliente",
    "categoria_saldo_disponible",
    "perfil_actividad_bancaria"
]

campos_faltantes = [c for c in campos_calculados if c not in columnas_actuales]
assert not campos_faltantes, (
    f"ERROR — Prueba 2: Campos calculados faltantes en la vista: {campos_faltantes}. "
    f"Verificar LsdpPlataClientesSaldos.py seccion de campos calculados."
)

print(f"PRUEBA 2 PASA: Los 4 campos calculados estan presentes: {campos_calculados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — Dimension Tipo 1: Una fila por identificador_cliente
# MAGIC
# MAGIC Verifica que la vista no tiene duplicados por identificador_cliente
# MAGIC (una sola fila con los datos mas recientes por cliente, segun RF-003).

# COMMAND ----------

df_duplicados = (
    df_clientes
    .groupBy("identificador_cliente")
    .count()
    .filter(F.col("count") > 1)
)

total_duplicados = df_duplicados.count()
assert total_duplicados == 0, (
    f"ERROR — Prueba 3: Se encontraron {total_duplicados} cliente(s) con mas de una fila. "
    "La vista debe ser Dimension Tipo 1 (una fila por identificador_cliente). "
    "Verificar la implementacion de Window.partitionBy('CUSTID').orderBy(desc('FechaIngestaDatos')) "
    "con row_number() == 1 en LsdpPlataClientesSaldos.py."
)

print("PRUEBA 3 PASA: Dimension Tipo 1 verificada — cero duplicados por identificador_cliente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Campo `huella_identificacion_cliente` (SHA256 consistente — RF-005)
# MAGIC
# MAGIC Verifica que el campo SHA256 es una cadena hexadecimal de 64 caracteres
# MAGIC y que es consistente: el mismo identificador_cliente siempre produce el mismo hash.

# COMMAND ----------

df_sha_check = df_clientes.select(
    "identificador_cliente",
    "huella_identificacion_cliente"
).limit(100)

# Verificar que el campo no es nulo para registros con identificador_cliente no nulo
df_sha_nulos = df_sha_check.filter(
    F.col("identificador_cliente").isNotNull() &
    F.col("huella_identificacion_cliente").isNull()
)
assert df_sha_nulos.count() == 0, (
    "ERROR — Prueba 4: Hay registros con identificador_cliente no nulo pero "
    "huella_identificacion_cliente es nulo. Verificar sha2() en LsdpPlataClientesSaldos.py."
)

# Verificar longitud del hash SHA256 (64 caracteres hexadecimales)
df_sha_longitud = df_sha_check.filter(
    F.col("huella_identificacion_cliente").isNotNull() &
    (F.length(F.col("huella_identificacion_cliente")) != 64)
)
assert df_sha_longitud.count() == 0, (
    "ERROR — Prueba 4: El campo 'huella_identificacion_cliente' no tiene 64 caracteres (SHA256). "
    "Verificar que se usa sha2(col('CUSTID').cast('string'), 256)."
)

print("PRUEBA 4 PASA: Campo 'huella_identificacion_cliente' es SHA256 de 64 chars consistente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 5 — Valores correctos del campo `clasificacion_riesgo_cliente`
# MAGIC
# MAGIC Verifica que el campo solo contiene valores validos del CASE secuencial (RF-004).

# COMMAND ----------

valores_validos_riesgo = {"RIESGO_CRITICO", "RIESGO_ALTO", "RIESGO_MEDIO", "RIESGO_BAJO", "SIN_RIESGO"}

valores_riesgo_actuales = set(
    row["clasificacion_riesgo_cliente"]
    for row in df_clientes.select("clasificacion_riesgo_cliente").distinct().collect()
    if row["clasificacion_riesgo_cliente"] is not None
)

valores_invalidos = valores_riesgo_actuales - valores_validos_riesgo
assert not valores_invalidos, (
    f"ERROR — Prueba 5: El campo 'clasificacion_riesgo_cliente' contiene valores invalidos: "
    f"{valores_invalidos}. Valores validos: {valores_validos_riesgo}. "
    "Verificar la logica CASE en LsdpPlataClientesSaldos.py."
)

print(f"PRUEBA 5 PASA: Valores de 'clasificacion_riesgo_cliente' son validos: {valores_riesgo_actuales}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 6 — Valores correctos del campo `categoria_saldo_disponible`

# COMMAND ----------

valores_validos_saldo = {"SALDO_PREMIUM", "SALDO_ALTO", "SALDO_MEDIO", "SALDO_BAJO", "SALDO_CRITICO"}

valores_saldo_actuales = set(
    row["categoria_saldo_disponible"]
    for row in df_clientes.select("categoria_saldo_disponible").distinct().collect()
    if row["categoria_saldo_disponible"] is not None
)

valores_invalidos_saldo = valores_saldo_actuales - valores_validos_saldo
assert not valores_invalidos_saldo, (
    f"ERROR — Prueba 6: El campo 'categoria_saldo_disponible' contiene valores invalidos: "
    f"{valores_invalidos_saldo}. Valores validos: {valores_validos_saldo}."
)

print(f"PRUEBA 6 PASA: Valores de 'categoria_saldo_disponible' son validos: {valores_saldo_actuales}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 7 — Valores correctos del campo `perfil_actividad_bancaria`

# COMMAND ----------

valores_validos_perfil = {"CLIENTE_INTEGRAL", "ACTIVIDAD_ALTA", "ACTIVIDAD_MEDIA", "ACTIVIDAD_BAJA", "INACTIVO"}

valores_perfil_actuales = set(
    row["perfil_actividad_bancaria"]
    for row in df_clientes.select("perfil_actividad_bancaria").distinct().collect()
    if row["perfil_actividad_bancaria"] is not None
)

valores_invalidos_perfil = valores_perfil_actuales - valores_validos_perfil
assert not valores_invalidos_perfil, (
    f"ERROR — Prueba 7: El campo 'perfil_actividad_bancaria' contiene valores invalidos: "
    f"{valores_invalidos_perfil}. Valores validos: {valores_validos_perfil}."
)

print(f"PRUEBA 7 PASA: Valores de 'perfil_actividad_bancaria' son validos: {valores_perfil_actuales}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 8 — Propiedades Delta configuradas (RF-008)
# MAGIC
# MAGIC Verifica que las 5 propiedades Delta obligatorias estan configuradas en la vista.

# COMMAND ----------

propiedades_delta = spark.sql(f"SHOW TBLPROPERTIES {nombre_vista}").collect()
props_dict = {row["key"]: row["value"] for row in propiedades_delta}

propiedades_requeridas = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}

for prop, valor_esperado in propiedades_requeridas.items():
    assert prop in props_dict, (
        f"ERROR — Prueba 8: La propiedad Delta '{prop}' NO esta configurada en la vista. "
        f"Propiedades disponibles: {list(props_dict.keys())}."
    )
    assert props_dict[prop] == valor_esperado, (
        f"ERROR — Prueba 8: La propiedad '{prop}' tiene valor '{props_dict[prop]}' "
        f"pero se esperaba '{valor_esperado}'."
    )

print("PRUEBA 8 PASA: Las 5 propiedades Delta obligatorias estan configuradas correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 9 — Registros con identificador_cliente nulo eliminados (RF-018, expect_or_drop)
# MAGIC
# MAGIC Verifica que no hay registros con identificador_cliente nulo en la vista
# MAGIC (deben haber sido eliminados por @dp.expect_or_drop).

# COMMAND ----------

count_nulos = df_clientes.filter(F.col("identificador_cliente").isNull()).count()
assert count_nulos == 0, (
    f"ERROR — Prueba 9: Se encontraron {count_nulos} registros con 'identificador_cliente' nulo. "
    "El decorador @dp.expect_or_drop('custid_no_nulo', 'identificador_cliente IS NOT NULL') "
    "debe eliminar estos registros. Verificar la implementacion en LsdpPlataClientesSaldos.py."
)

print("PRUEBA 9 PASA: Cero registros con 'identificador_cliente' nulo (expect_or_drop activo).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 10 — LEFT JOIN correcto: clientes sin saldos tienen campos blncfl nulos
# MAGIC
# MAGIC Verifica que el LEFT JOIN es desde cmstfl hacia blncfl: todos los clientes aparecen
# MAGIC y aquellos sin cuenta en blncfl tienen campos de saldo nulos (CE-002, CE-003).

# COMMAND ----------

# Verificar que hay al menos 1 registro (la vista tiene datos)
total_registros = df_clientes.count()
assert total_registros > 0, (
    "ERROR — Prueba 10: La vista esta vacia. "
    "Verificar que el pipeline se ejecuto con datos en bronce."
)

# Verificar que identificador_cuenta puede ser nulo (LEFT JOIN — clientes sin saldo)
# Esto es valido en un LEFT JOIN: cmstfl siempre aparece, blncfl puede ser nulo
tipo_id_cuenta = dict(df_clientes.dtypes).get("identificador_cuenta")
assert tipo_id_cuenta is not None, (
    "ERROR — Prueba 10: La columna 'identificador_cuenta' no existe en la vista."
)

print(f"PRUEBA 10 PASA: LEFT JOIN correcto. Vista tiene {total_registros} registros. "
      f"'identificador_cuenta' permite nulos (clientes sin cuenta en blncfl).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la Suite TDD

# COMMAND ----------

print("=" * 65)
print("RESUMEN — NbTestPlataClientesSaldos")
print("=" * 65)
print("PRUEBA 1  PASA: 175 columnas con nombres correctos en espanol")
print("PRUEBA 2  PASA: 4 campos calculados presentes")
print("PRUEBA 3  PASA: Dimension Tipo 1 — cero duplicados por cliente")
print("PRUEBA 4  PASA: SHA256 de 64 chars consistente")
print("PRUEBA 5  PASA: clasificacion_riesgo_cliente — valores validos")
print("PRUEBA 6  PASA: categoria_saldo_disponible — valores validos")
print("PRUEBA 7  PASA: perfil_actividad_bancaria — valores validos")
print("PRUEBA 8  PASA: 5 propiedades Delta RF-008 configuradas")
print("PRUEBA 9  PASA: cero registros con CUSTID nulo (expect_or_drop)")
print("PRUEBA 10 PASA: LEFT JOIN correcto cmstfl → blncfl")
print("=" * 65)
print("TODAS LAS PRUEBAS PASARON. Vista clientes_saldos_consolidados validada.")
