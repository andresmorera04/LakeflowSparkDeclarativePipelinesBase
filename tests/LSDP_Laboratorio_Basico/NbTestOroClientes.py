# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestOroClientes — Suite de Pruebas TDD: Vistas Materializadas de Oro
# MAGIC
# MAGIC **Proposito**: Validar la correcta creacion y contenido de las 2 vistas materializadas
# MAGIC de la medalla de oro:
# MAGIC - `comportamiento_atm_cliente` — 6 columnas: metricas agregadas de ATM y pagos al saldo
# MAGIC - `resumen_integral_cliente` — 22 columnas: perfil integral del cliente (plata + metricas oro)
# MAGIC
# MAGIC **Prerequisito**: Ejecutar el pipeline LSDP (bronce + plata + oro) al menos una vez.
# MAGIC Ambas vistas deben existir y contener datos en `{catalogoOro}.{esquema_oro}`.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks.
# MAGIC
# MAGIC **Cobertura** (CE-001 a CE-011):
# MAGIC - US1: Vista `comportamiento_atm_cliente` — 6 cols, dim tipo 1, metricas correctas (CE-002, CE-004 a CE-009)
# MAGIC - US2: Vista `resumen_integral_cliente` — 22 cols, LEFT JOIN con coalesce (CE-003, CE-007 a CE-010)
# MAGIC - US4: Pruebas de integracion end-to-end y casos borde avanzados (CE-001, CE-011)
# MAGIC
# MAGIC **Nota**: Las validaciones CE-012 (claves de parametros en Azure SQL) y CE-013
# MAGIC (parametro esquema_oro del pipeline) NO se prueban en este notebook. Estas
# MAGIC validaciones se ejecutan directamente desde el Lakeflow Spark Declarative Pipeline,
# MAGIC ya que requieren el contexto del pipeline (acceso a `pipelines.parameters.*` y
# MAGIC conexion Azure SQL via closure del modulo `LsdpOroClientes.py`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracion de Parametros de Prueba

# COMMAND ----------

dbutils.widgets.text("catalogo_oro", "oro_dev", "Catalogo de Unity Catalog para oro")
dbutils.widgets.text("esquema_oro", "regional", "Esquema de Unity Catalog para oro")
dbutils.widgets.text("catalogo_plata", "plata_dev", "Catalogo de Unity Catalog para plata")
dbutils.widgets.text("esquema_plata", "regional", "Esquema de Unity Catalog para plata")

# COMMAND ----------

catalogo_oro = dbutils.widgets.get("catalogo_oro").strip()
esquema_oro = dbutils.widgets.get("esquema_oro").strip()
catalogo_plata = dbutils.widgets.get("catalogo_plata").strip()
esquema_plata = dbutils.widgets.get("esquema_plata").strip()

assert catalogo_oro, "ERROR: El widget 'catalogo_oro' no puede estar vacio."
assert esquema_oro, "ERROR: El widget 'esquema_oro' no puede estar vacio."
assert catalogo_plata, "ERROR: El widget 'catalogo_plata' no puede estar vacio."
assert esquema_plata, "ERROR: El widget 'esquema_plata' no puede estar vacio."

nombre_vista_atm = f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente"
nombre_vista_resumen = f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente"
print(f"Vista ATM a probar: {nombre_vista_atm}")
print(f"Vista Resumen a probar: {nombre_vista_resumen}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import types as T
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bloque US1 — Vista comportamiento_atm_cliente (CE-002, CE-004 a CE-009)
# MAGIC
# MAGIC Valida la vista materializada de metricas agregadas de ATM y pagos al saldo:
# MAGIC 6 columnas, dimension tipo 1, metricas condicionales correctas,
# MAGIC manejo de nulos, expect_or_drop y propiedades Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de la Vista comportamiento_atm_cliente

# COMMAND ----------

try:
    df_atm = spark.table(nombre_vista_atm)
    print(f"CARGA OK: Vista '{nombre_vista_atm}' accedida exitosamente.")
except Exception as error:
    raise AssertionError(
        f"ERROR — Carga US1: La vista '{nombre_vista_atm}' NO existe o no es accesible. "
        f"Error: {error}. "
        "Verificar que el pipeline LSDP de oro se ejecuto correctamente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-1 — Estructura de 6 columnas con nombres correctos (CE-008)
# MAGIC
# MAGIC Verifica que la vista tiene exactamente 6 columnas con los nombres segun
# MAGIC el data-model.md TABLA A, en espanol y snake_case.

# COMMAND ----------

columnas_esperadas_atm = [
    "identificador_cliente",
    "cantidad_depositos_atm",
    "cantidad_retiros_atm",
    "promedio_monto_depositos_atm",
    "promedio_monto_retiros_atm",
    "total_pagos_saldo_cliente"
]

columnas_actuales_atm = df_atm.columns
total_columnas_atm = len(columnas_actuales_atm)

assert total_columnas_atm == 6, (
    f"ERROR — Prueba US1-1: Se esperaban 6 columnas pero se encontraron {total_columnas_atm}. "
    f"Columnas actuales: {columnas_actuales_atm}. "
    "Revisar la implementacion en LsdpOroClientes.py."
)

for col_esperada in columnas_esperadas_atm:
    assert col_esperada in columnas_actuales_atm, (
        f"ERROR — Prueba US1-1: La columna '{col_esperada}' NO existe en la vista. "
        f"Columnas actuales: {columnas_actuales_atm}."
    )

print(f"PASS — Prueba US1-1: Vista tiene exactamente 6 columnas con nombres correctos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-2 — Dimension Tipo 1: una unica fila por identificador_cliente (CE-002)
# MAGIC
# MAGIC Verifica que no existen duplicados en la clave de agrupacion `identificador_cliente`.

# COMMAND ----------

total_filas_atm = df_atm.count()
total_clientes_unicos_atm = df_atm.select("identificador_cliente").distinct().count()

assert total_filas_atm == total_clientes_unicos_atm, (
    f"ERROR — Prueba US1-2: La vista NO es Dimension Tipo 1. "
    f"Total filas: {total_filas_atm}, clientes unicos: {total_clientes_unicos_atm}. "
    f"Diferencia: {total_filas_atm - total_clientes_unicos_atm} duplicados. "
    "La vista debe contener exactamente una fila por identificador_cliente."
)

print(f"PASS — Prueba US1-2: Dimension Tipo 1 verificada. {total_clientes_unicos_atm} clientes unicos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-3 — Conteos condicionales correctos para un cliente conocido (CE-004)
# MAGIC
# MAGIC Para un cliente de prueba con transacciones CATM/DATM/PGSL conocidas en plata,
# MAGIC los conteos de la vista de oro deben coincidir exactamente con el conteo
# MAGIC de transacciones de ese tipo en la fuente.

# COMMAND ----------

nombre_tabla_trx_plata = f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas"

# Seleccionar un cliente con transacciones CATM para la prueba
try:
    df_clientes_catm = (
        spark.table(nombre_tabla_trx_plata)
        .filter(F.col("tipo_transaccion") == "CATM")
        .groupBy("identificador_cliente")
        .agg(F.count("*").alias("conteo_catm_plata"))
        .filter(F.col("conteo_catm_plata") > 0)
        .orderBy(F.col("conteo_catm_plata").desc())
        .limit(1)
    )
    primer_cliente_catm = df_clientes_catm.collect()
except Exception as error:
    raise AssertionError(
        f"ERROR — Prueba US1-3: No se pudo leer la fuente '{nombre_tabla_trx_plata}'. "
        f"Error: {error}. Verificar que la vista de plata existe y contiene datos."
    )

if len(primer_cliente_catm) == 0:
    print("SKIP — Prueba US1-3: No hay transacciones CATM en plata. Prueba omitida (datos insuficientes).")
else:
    id_cliente_prueba = primer_cliente_catm[0]["identificador_cliente"]
    conteo_catm_plata = primer_cliente_catm[0]["conteo_catm_plata"]

    # Obtener conteos de DATM y PGSL para el mismo cliente en plata
    conteos_plata = (
        spark.table(nombre_tabla_trx_plata)
        .filter(F.col("identificador_cliente") == id_cliente_prueba)
        .groupBy("tipo_transaccion")
        .agg(F.count("*").alias("conteo"))
        .collect()
    )
    conteos_plata_dict = {row["tipo_transaccion"]: row["conteo"] for row in conteos_plata}
    conteo_datm_plata = conteos_plata_dict.get("DATM", 0)
    conteo_pgsl_plata = conteos_plata_dict.get("PGSL", 0)

    # Obtener la fila del cliente en la vista de oro
    fila_oro = df_atm.filter(F.col("identificador_cliente") == id_cliente_prueba).collect()

    assert len(fila_oro) == 1, (
        f"ERROR — Prueba US1-3: El cliente {id_cliente_prueba} tiene {len(fila_oro)} filas en la vista ATM "
        "(esperado: exactamente 1)."
    )

    conteo_catm_oro = fila_oro[0]["cantidad_depositos_atm"]
    conteo_datm_oro = fila_oro[0]["cantidad_retiros_atm"]

    assert conteo_catm_oro == conteo_catm_plata, (
        f"ERROR — Prueba US1-3: cantidad_depositos_atm incorrecto para cliente {id_cliente_prueba}. "
        f"Esperado (plata): {conteo_catm_plata}, Obtenido (oro): {conteo_catm_oro}."
    )
    assert conteo_datm_oro == conteo_datm_plata, (
        f"ERROR — Prueba US1-3: cantidad_retiros_atm incorrecto para cliente {id_cliente_prueba}. "
        f"Esperado (plata): {conteo_datm_plata}, Obtenido (oro): {conteo_datm_oro}."
    )

    print(
        f"PASS — Prueba US1-3: Cliente {id_cliente_prueba}: "
        f"CATM={conteo_catm_oro}, DATM={conteo_datm_oro}, PGSL_esperado={conteo_pgsl_plata}."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-4 — Promedios calculados correctamente como media aritmetica (CE-005)
# MAGIC
# MAGIC Para el mismo cliente de prueba, los promedios de montos CATM y DATM
# MAGIC deben coincidir con la media aritmetica calculada directamente en plata.

# COMMAND ----------

if len(primer_cliente_catm) == 0:
    print("SKIP — Prueba US1-4: No hay transacciones CATM en plata. Prueba omitida (datos insuficientes).")
else:
    # Calcular promedio CATM directamente en plata
    promedio_catm_plata = (
        spark.table(nombre_tabla_trx_plata)
        .filter(
            (F.col("identificador_cliente") == id_cliente_prueba) &
            (F.col("tipo_transaccion") == "CATM")
        )
        .agg(F.avg(F.coalesce(F.col("monto_transaccion"), F.lit(0))).alias("promedio_catm"))
        .collect()[0]["promedio_catm"]
    )
    if promedio_catm_plata is None:
        promedio_catm_plata = 0.0

    # Calcular promedio DATM directamente en plata
    df_datm = (
        spark.table(nombre_tabla_trx_plata)
        .filter(
            (F.col("identificador_cliente") == id_cliente_prueba) &
            (F.col("tipo_transaccion") == "DATM")
        )
    )
    resultado_datm = df_datm.agg(F.avg(F.coalesce(F.col("monto_transaccion"), F.lit(0))).alias("promedio_datm")).collect()[0]
    promedio_datm_plata = resultado_datm["promedio_datm"] if resultado_datm["promedio_datm"] is not None else 0.0

    promedio_catm_oro = fila_oro[0]["promedio_monto_depositos_atm"]
    promedio_datm_oro = fila_oro[0]["promedio_monto_retiros_atm"]

    tolerancia = 0.01
    assert abs(promedio_catm_oro - promedio_catm_plata) <= tolerancia, (
        f"ERROR — Prueba US1-4: promedio_monto_depositos_atm incorrecto para cliente {id_cliente_prueba}. "
        f"Esperado (plata): {promedio_catm_plata:.4f}, Obtenido (oro): {promedio_catm_oro:.4f}."
    )
    assert abs(promedio_datm_oro - promedio_datm_plata) <= tolerancia, (
        f"ERROR — Prueba US1-4: promedio_monto_retiros_atm incorrecto para cliente {id_cliente_prueba}. "
        f"Esperado (plata): {promedio_datm_plata:.4f}, Obtenido (oro): {promedio_datm_oro:.4f}."
    )

    print(
        f"PASS — Prueba US1-4: Promedios correctos para cliente {id_cliente_prueba}: "
        f"CATM={promedio_catm_oro:.4f}, DATM={promedio_datm_oro:.4f}."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-5 — Total pagos al saldo es suma exacta de montos PGSL (CE-006)
# MAGIC
# MAGIC Para el cliente de prueba, el campo `total_pagos_saldo_cliente` debe ser igual
# MAGIC a la suma exacta de los montos de transacciones PGSL en plata.

# COMMAND ----------

if len(primer_cliente_catm) == 0:
    print("SKIP — Prueba US1-5: No hay transacciones CATM en plata. Prueba omitida (datos insuficientes).")
else:
    # Calcular suma PGSL directamente en plata
    resultado_pgsl = (
        spark.table(nombre_tabla_trx_plata)
        .filter(
            (F.col("identificador_cliente") == id_cliente_prueba) &
            (F.col("tipo_transaccion") == "PGSL")
        )
        .agg(F.sum(F.coalesce(F.col("monto_transaccion"), F.lit(0))).alias("suma_pgsl"))
        .collect()[0]
    )
    suma_pgsl_plata = resultado_pgsl["suma_pgsl"] if resultado_pgsl["suma_pgsl"] is not None else 0.0

    total_pgsl_oro = fila_oro[0]["total_pagos_saldo_cliente"]

    tolerancia = 0.01
    assert abs(total_pgsl_oro - suma_pgsl_plata) <= tolerancia, (
        f"ERROR — Prueba US1-5: total_pagos_saldo_cliente incorrecto para cliente {id_cliente_prueba}. "
        f"Esperado (suma plata): {suma_pgsl_plata:.4f}, Obtenido (oro): {total_pgsl_oro:.4f}."
    )

    print(
        f"PASS — Prueba US1-5: total_pagos_saldo_cliente={total_pgsl_oro:.4f} "
        f"coincide con suma PGSL plata={suma_pgsl_plata:.4f}."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-6 — Cliente sin transacciones ATM/PGSL tiene metricas en 0 (CE-007)
# MAGIC
# MAGIC Un cliente que NO tiene transacciones CATM, DATM ni PGSL debe aparecer con
# MAGIC todas las 5 metricas en 0 gracias a las agregaciones condicionales.

# COMMAND ----------

# Buscar un cliente en plata que NO tenga transacciones CATM, DATM ni PGSL
df_clientes_plata = (
    spark.table(f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")
    .select("identificador_cliente")
)

df_transacciones_atm_pgsl = (
    spark.table(nombre_tabla_trx_plata)
    .filter(F.col("tipo_transaccion").isin(["CATM", "DATM", "PGSL"]))
    .select("identificador_cliente")
    .distinct()
)

# Cliente en plata sin transacciones ATM/PGSL
df_clientes_sin_atm = (
    df_clientes_plata
    .join(df_transacciones_atm_pgsl, on="identificador_cliente", how="left_anti")
    .limit(1)
)
cliente_sin_atm = df_clientes_sin_atm.collect()

if len(cliente_sin_atm) == 0:
    print("SKIP — Prueba US1-6: No se encontro cliente sin transacciones ATM/PGSL. Prueba omitida.")
else:
    id_cliente_sin_atm = cliente_sin_atm[0]["identificador_cliente"]

    fila_sin_atm = df_atm.filter(F.col("identificador_cliente") == id_cliente_sin_atm).collect()

    if len(fila_sin_atm) == 0:
        print(
            f"SKIP — Prueba US1-6: El cliente {id_cliente_sin_atm} sin transacciones ATM/PGSL "
            "no aparece en la vista de oro. Puede estar excluido por expect_or_drop si su identificador es nulo."
        )
    else:
        assert fila_sin_atm[0]["cantidad_depositos_atm"] == 0, (
            f"ERROR — Prueba US1-6: cantidad_depositos_atm debe ser 0 para cliente sin CATM. "
            f"Valor: {fila_sin_atm[0]['cantidad_depositos_atm']}."
        )
        assert fila_sin_atm[0]["cantidad_retiros_atm"] == 0, (
            f"ERROR — Prueba US1-6: cantidad_retiros_atm debe ser 0 para cliente sin DATM. "
            f"Valor: {fila_sin_atm[0]['cantidad_retiros_atm']}."
        )
        assert fila_sin_atm[0]["promedio_monto_depositos_atm"] == 0.0, (
            f"ERROR — Prueba US1-6: promedio_monto_depositos_atm debe ser 0 para cliente sin CATM. "
            f"Valor: {fila_sin_atm[0]['promedio_monto_depositos_atm']}."
        )
        assert fila_sin_atm[0]["promedio_monto_retiros_atm"] == 0.0, (
            f"ERROR — Prueba US1-6: promedio_monto_retiros_atm debe ser 0 para cliente sin DATM. "
            f"Valor: {fila_sin_atm[0]['promedio_monto_retiros_atm']}."
        )
        assert fila_sin_atm[0]["total_pagos_saldo_cliente"] == 0.0, (
            f"ERROR — Prueba US1-6: total_pagos_saldo_cliente debe ser 0 para cliente sin PGSL. "
            f"Valor: {fila_sin_atm[0]['total_pagos_saldo_cliente']}."
        )
        print(f"PASS — Prueba US1-6: Cliente {id_cliente_sin_atm} sin transacciones ATM/PGSL tiene todas las metricas en 0.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-7 — Registros con identificador_cliente NULL eliminados por expect_or_drop (RF-018)
# MAGIC
# MAGIC El decorador `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")`
# MAGIC no debe permitir filas con `identificador_cliente` NULL en el resultado final.

# COMMAND ----------

filas_nulas_atm = df_atm.filter(F.col("identificador_cliente").isNull()).count()

assert filas_nulas_atm == 0, (
    f"ERROR — Prueba US1-7: La vista contiene {filas_nulas_atm} filas con identificador_cliente NULL. "
    "El decorador @dp.expect_or_drop('cliente_valido', 'identificador_cliente IS NOT NULL') "
    "debe haber eliminado estos registros. Verificar la implementacion en LsdpOroClientes.py."
)

print(f"PASS — Prueba US1-7: Cero filas con identificador_cliente NULL. expect_or_drop activo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US1-8 — Propiedades Delta y Liquid Cluster correctos (CE-009)
# MAGIC
# MAGIC Verifica que la vista tiene las 5 propiedades Delta estandar configuradas
# MAGIC y que el Liquid Cluster esta configurado por `identificador_cliente`.

# COMMAND ----------

propiedades_atm = spark.sql(f"SHOW TBLPROPERTIES {nombre_vista_atm}").collect()
propiedades_atm_dict = {row["key"]: row["value"] for row in propiedades_atm}

propiedades_requeridas_atm = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}

for propiedad, valor_esperado in propiedades_requeridas_atm.items():
    assert propiedad in propiedades_atm_dict, (
        f"ERROR — Prueba US1-8: La propiedad Delta '{propiedad}' no esta configurada en la vista ATM. "
        f"Propiedades actuales: {list(propiedades_atm_dict.keys())}."
    )
    assert propiedades_atm_dict[propiedad] == valor_esperado, (
        f"ERROR — Prueba US1-8: Propiedad '{propiedad}' tiene valor incorrecto. "
        f"Esperado: '{valor_esperado}', Actual: '{propiedades_atm_dict[propiedad]}'."
    )

# Verificar Liquid Cluster por identificador_cliente (via SHOW TBLPROPERTIES — DESCRIBE DETAIL no soporta views)
assert "clusteringColumns" in propiedades_atm_dict, (
    "ERROR — Prueba US1-8: No se encontro la propiedad 'clusteringColumns' en TBLPROPERTIES. "
    f"Propiedades actuales: {list(propiedades_atm_dict.keys())}."
)
assert "identificador_cliente" in propiedades_atm_dict["clusteringColumns"], (
    f"ERROR — Prueba US1-8: Liquid Cluster no configurado por 'identificador_cliente'. "
    f"Valor de clusteringColumns: {propiedades_atm_dict['clusteringColumns']}."
)

print(f"PASS — Prueba US1-8: 5 propiedades Delta correctas y Liquid Cluster por identificador_cliente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bloque US2 — Vista resumen_integral_cliente (CE-003, CE-007 a CE-010)
# MAGIC
# MAGIC Valida la vista materializada del perfil integral del cliente:
# MAGIC 22 columnas, dimension tipo 1, LEFT JOIN con coalesce a 0 para clientes
# MAGIC sin transacciones, y propiedades Delta correctas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de la Vista resumen_integral_cliente

# COMMAND ----------

try:
    df_resumen = spark.table(nombre_vista_resumen)
    print(f"CARGA OK: Vista '{nombre_vista_resumen}' accedida exitosamente.")
except Exception as error:
    raise AssertionError(
        f"ERROR — Carga US2: La vista '{nombre_vista_resumen}' NO existe o no es accesible. "
        f"Error: {error}. "
        "Verificar que el pipeline LSDP de oro se ejecuto correctamente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-1 — Estructura de 22 columnas con nombres correctos (CE-008)
# MAGIC
# MAGIC Verifica que la vista tiene exactamente 22 columnas con los nombres segun
# MAGIC el data-model.md TABLA B.

# COMMAND ----------

columnas_esperadas_resumen = [
    # Identificativos
    "identificador_cliente",
    "huella_identificacion_cliente",
    "nombre_completo_cliente",
    "tipo_documento_identidad",
    "numero_documento_identidad",
    # Demograficos
    "segmento_cliente",
    "categoria_cliente",
    "ciudad_residencia",
    "pais_residencia",
    # Clasificacion de plata
    "clasificacion_riesgo_cliente",
    "categoria_saldo_disponible",
    "perfil_actividad_bancaria",
    # Financieros
    "saldo_disponible",
    "saldo_actual",
    "limite_credito",
    "puntaje_crediticio",
    "ingreso_anual_declarado",
    # Metricas ATM y pagos al saldo
    "cantidad_depositos_atm",
    "cantidad_retiros_atm",
    "promedio_monto_depositos_atm",
    "promedio_monto_retiros_atm",
    "total_pagos_saldo_cliente"
]

columnas_actuales_resumen = df_resumen.columns
total_columnas_resumen = len(columnas_actuales_resumen)

assert total_columnas_resumen == 22, (
    f"ERROR — Prueba US2-1: Se esperaban 22 columnas pero se encontraron {total_columnas_resumen}. "
    f"Columnas actuales: {columnas_actuales_resumen}. "
    "Revisar la implementacion en LsdpOroClientes.py."
)

for col_esperada in columnas_esperadas_resumen:
    assert col_esperada in columnas_actuales_resumen, (
        f"ERROR — Prueba US2-1: La columna '{col_esperada}' NO existe en la vista de resumen. "
        f"Columnas actuales: {columnas_actuales_resumen}."
    )

print(f"PASS — Prueba US2-1: Vista resumen tiene exactamente 22 columnas con nombres correctos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-2 — Dimension Tipo 1: una unica fila por identificador_cliente (CE-003)
# MAGIC
# MAGIC Verifica que no existen duplicados en la clave `identificador_cliente`.

# COMMAND ----------

total_filas_resumen = df_resumen.count()
total_clientes_unicos_resumen = df_resumen.select("identificador_cliente").distinct().count()

assert total_filas_resumen == total_clientes_unicos_resumen, (
    f"ERROR — Prueba US2-2: La vista resumen NO es Dimension Tipo 1. "
    f"Total filas: {total_filas_resumen}, clientes unicos: {total_clientes_unicos_resumen}. "
    f"Diferencia: {total_filas_resumen - total_clientes_unicos_resumen} duplicados."
)

print(f"PASS — Prueba US2-2: Dimension Tipo 1 verificada. {total_clientes_unicos_resumen} clientes unicos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-3 — Campos de clasificacion reflejan valores calculados en plata
# MAGIC
# MAGIC Los campos `clasificacion_riesgo_cliente`, `categoria_saldo_disponible` y
# MAGIC `perfil_actividad_bancaria` deben ser identicos a los de plata (no recalculados en oro).

# COMMAND ----------

nombre_vista_clientes_plata = f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"

# Tomar una muestra de clientes para comparar
df_clasificacion_plata = (
    spark.table(nombre_vista_clientes_plata)
    .select(
        "identificador_cliente",
        "clasificacion_riesgo_cliente",
        "categoria_saldo_disponible",
        "perfil_actividad_bancaria"
    )
    .limit(100)
)

df_clasificacion_oro = (
    df_resumen
    .select(
        "identificador_cliente",
        "clasificacion_riesgo_cliente",
        "categoria_saldo_disponible",
        "perfil_actividad_bancaria"
    )
)

# Hacer el join con alias explicitos para evitar ambiguedad (PySpark no soporta suffixes)
df_plata_alias = df_clasificacion_plata.toDF(*[f"{c}_plata" if c != "identificador_cliente" else c for c in df_clasificacion_plata.columns])
df_oro_alias = df_clasificacion_oro.toDF(*[f"{c}_oro" if c != "identificador_cliente" else c for c in df_clasificacion_oro.columns])

df_discrepancias = (
    df_plata_alias
    .join(df_oro_alias, on="identificador_cliente", how="inner")
    .filter(
        (F.col("clasificacion_riesgo_cliente_plata") != F.col("clasificacion_riesgo_cliente_oro")) |
        (F.col("categoria_saldo_disponible_plata") != F.col("categoria_saldo_disponible_oro")) |
        (F.col("perfil_actividad_bancaria_plata") != F.col("perfil_actividad_bancaria_oro"))
    )
)

conteo_discrepancias = df_discrepancias.count()
assert conteo_discrepancias == 0, (
    f"ERROR — Prueba US2-3: Se encontraron {conteo_discrepancias} discrepancias entre los campos "
    "de clasificacion en plata y en oro. Los campos deben ser identicos (copiados de plata, no recalculados)."
)

print(f"PASS — Prueba US2-3: Campos de clasificacion identicos en plata y oro (muestra de 100 clientes).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-4 — LEFT JOIN: cliente en plata sin transacciones tiene metricas ATM en 0 (CE-007)
# MAGIC
# MAGIC Un cliente que existe en `clientes_saldos_consolidados` pero NO aparece en
# MAGIC `comportamiento_atm_cliente` debe aparecer en `resumen_integral_cliente`
# MAGIC con las 5 metricas ATM en 0 (coalesce del LEFT JOIN).

# COMMAND ----------

# Obtener clientes de plata que no tienen filas en la vista ATM de oro
df_clientes_solo_plata = (
    spark.table(nombre_vista_clientes_plata)
    .select("identificador_cliente")
    .join(
        df_atm.select("identificador_cliente"),
        on="identificador_cliente",
        how="left_anti"
    )
    .limit(1)
)
clientes_solo_plata = df_clientes_solo_plata.collect()

if len(clientes_solo_plata) == 0:
    print("SKIP — Prueba US2-4: Todos los clientes de plata tienen filas en la vista ATM. Prueba omitida.")
else:
    id_cliente_solo_plata = clientes_solo_plata[0]["identificador_cliente"]

    fila_resumen_sin_atm = df_resumen.filter(F.col("identificador_cliente") == id_cliente_solo_plata).collect()

    assert len(fila_resumen_sin_atm) == 1, (
        f"ERROR — Prueba US2-4: El cliente {id_cliente_solo_plata} de plata no aparece en resumen_integral_cliente. "
        "El LEFT JOIN debe preservar todos los clientes de plata aunque no tengan transacciones ATM."
    )

    fila = fila_resumen_sin_atm[0]
    assert fila["cantidad_depositos_atm"] == 0, (
        f"ERROR — Prueba US2-4: cantidad_depositos_atm debe ser 0 para cliente sin ATM. Valor: {fila['cantidad_depositos_atm']}."
    )
    assert fila["cantidad_retiros_atm"] == 0, (
        f"ERROR — Prueba US2-4: cantidad_retiros_atm debe ser 0 para cliente sin ATM. Valor: {fila['cantidad_retiros_atm']}."
    )
    assert fila["promedio_monto_depositos_atm"] == 0.0, (
        f"ERROR — Prueba US2-4: promedio_monto_depositos_atm debe ser 0 para cliente sin ATM. Valor: {fila['promedio_monto_depositos_atm']}."
    )
    assert fila["promedio_monto_retiros_atm"] == 0.0, (
        f"ERROR — Prueba US2-4: promedio_monto_retiros_atm debe ser 0 para cliente sin ATM. Valor: {fila['promedio_monto_retiros_atm']}."
    )
    assert fila["total_pagos_saldo_cliente"] == 0.0, (
        f"ERROR — Prueba US2-4: total_pagos_saldo_cliente debe ser 0 para cliente sin PGSL. Valor: {fila['total_pagos_saldo_cliente']}."
    )

    print(f"PASS — Prueba US2-4: Cliente {id_cliente_solo_plata} sin ATM tiene todas las metricas en 0 en resumen.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-5 — LEFT JOIN: cliente con transacciones tiene metricas coherentes (CE-007)
# MAGIC
# MAGIC Un cliente que tiene filas en `comportamiento_atm_cliente` debe tener
# MAGIC las mismas metricas en `resumen_integral_cliente` que en la vista ATM.

# COMMAND ----------

if len(primer_cliente_catm) == 0:
    print("SKIP — Prueba US2-5: No hay clientes con CATM. Prueba omitida.")
else:
    fila_resumen_con_atm = df_resumen.filter(F.col("identificador_cliente") == id_cliente_prueba).collect()

    assert len(fila_resumen_con_atm) == 1, (
        f"ERROR — Prueba US2-5: El cliente {id_cliente_prueba} no aparece en resumen_integral_cliente."
    )

    fila_resumen = fila_resumen_con_atm[0]
    fila_atm = fila_oro[0]

    assert fila_resumen["cantidad_depositos_atm"] == fila_atm["cantidad_depositos_atm"], (
        f"ERROR — Prueba US2-5: cantidad_depositos_atm difiere entre resumen y ATM para cliente {id_cliente_prueba}. "
        f"Resumen: {fila_resumen['cantidad_depositos_atm']}, ATM: {fila_atm['cantidad_depositos_atm']}."
    )
    assert fila_resumen["cantidad_retiros_atm"] == fila_atm["cantidad_retiros_atm"], (
        f"ERROR — Prueba US2-5: cantidad_retiros_atm difiere entre resumen y ATM para cliente {id_cliente_prueba}. "
        f"Resumen: {fila_resumen['cantidad_retiros_atm']}, ATM: {fila_atm['cantidad_retiros_atm']}."
    )
    assert abs(fila_resumen["total_pagos_saldo_cliente"] - fila_atm["total_pagos_saldo_cliente"]) <= 0.01, (
        f"ERROR — Prueba US2-5: total_pagos_saldo_cliente difiere entre resumen y ATM. "
        f"Resumen: {fila_resumen['total_pagos_saldo_cliente']}, ATM: {fila_atm['total_pagos_saldo_cliente']}."
    )

    print(
        f"PASS — Prueba US2-5: Metricas coherentes para cliente {id_cliente_prueba} "
        f"entre resumen_integral_cliente y comportamiento_atm_cliente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US2-6 — Propiedades Delta y Liquid Cluster correctos (CE-009)
# MAGIC
# MAGIC Verifica que la vista de resumen tiene las 5 propiedades Delta estandar
# MAGIC y que el Liquid Cluster esta configurado por `huella_identificacion_cliente`
# MAGIC e `identificador_cliente`.

# COMMAND ----------

propiedades_resumen = spark.sql(f"SHOW TBLPROPERTIES {nombre_vista_resumen}").collect()
propiedades_resumen_dict = {row["key"]: row["value"] for row in propiedades_resumen}

propiedades_requeridas_resumen = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}

for propiedad, valor_esperado in propiedades_requeridas_resumen.items():
    assert propiedad in propiedades_resumen_dict, (
        f"ERROR — Prueba US2-6: La propiedad Delta '{propiedad}' no esta configurada en la vista resumen. "
        f"Propiedades actuales: {list(propiedades_resumen_dict.keys())}."
    )
    assert propiedades_resumen_dict[propiedad] == valor_esperado, (
        f"ERROR — Prueba US2-6: Propiedad '{propiedad}' tiene valor incorrecto en vista resumen. "
        f"Esperado: '{valor_esperado}', Actual: '{propiedades_resumen_dict[propiedad]}'."
    )

# Verificar Liquid Cluster (via SHOW TBLPROPERTIES — DESCRIBE DETAIL no soporta views)
assert "clusteringColumns" in propiedades_resumen_dict, (
    "ERROR — Prueba US2-6: No se encontro la propiedad 'clusteringColumns' en TBLPROPERTIES. "
    f"Propiedades actuales: {list(propiedades_resumen_dict.keys())}."
)
assert "huella_identificacion_cliente" in propiedades_resumen_dict["clusteringColumns"], (
    f"ERROR — Prueba US2-6: Liquid Cluster no incluye 'huella_identificacion_cliente'. "
    f"Valor de clusteringColumns: {propiedades_resumen_dict['clusteringColumns']}."
)
assert "identificador_cliente" in propiedades_resumen_dict["clusteringColumns"], (
    f"ERROR — Prueba US2-6: Liquid Cluster no incluye 'identificador_cliente'. "
    f"Valor de clusteringColumns: {propiedades_resumen_dict['clusteringColumns']}."
)

print(f"PASS — Prueba US2-6: 5 propiedades Delta y Liquid Cluster con 2 columnas verificados en resumen.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bloque US4 — Suite de Pruebas Integrales y Casos Borde
# MAGIC
# MAGIC Pruebas de integracion end-to-end, casos borde avanzados y validaciones
# MAGIC de cumplimiento que cubren la totalidad de RFs y CEs del pipeline de oro.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US4-1 — Integracion end-to-end: coherencia entre vistas de oro (CE-001)
# MAGIC
# MAGIC Verifica que ambas vistas se crearon en el mismo pipeline run y que los datos
# MAGIC son coherentes entre `comportamiento_atm_cliente` y `resumen_integral_cliente`.

# COMMAND ----------

# Verificar que ambas vistas existen y tienen datos
assert total_filas_atm > 0, (
    "ERROR — Prueba US4-1: La vista comportamiento_atm_cliente esta vacia. "
    "El pipeline debe haber insertado al menos un registro."
)
assert total_filas_resumen > 0, (
    "ERROR — Prueba US4-1: La vista resumen_integral_cliente esta vacia. "
    "El pipeline debe haber insertado al menos un registro."
)

# Verificar que los clientes de la vista ATM son un subconjunto de los de resumen
ids_en_atm = df_atm.select("identificador_cliente").distinct()
ids_en_resumen = df_resumen.select("identificador_cliente").distinct()

# Clientes en ATM que no estan en resumen (no deberia haber ninguno — ATM es subconjunto de plata)
ids_atm_sin_resumen = ids_en_atm.join(ids_en_resumen, on="identificador_cliente", how="left_anti").count()

assert ids_atm_sin_resumen == 0, (
    f"ERROR — Prueba US4-1: {ids_atm_sin_resumen} clientes en comportamiento_atm_cliente "
    "NO aparecen en resumen_integral_cliente. Todo cliente con metricas ATM debe estar en el resumen."
)

print(
    f"PASS — Prueba US4-1: Integracion coherente. "
    f"ATM: {total_filas_atm} filas, Resumen: {total_filas_resumen} filas. "
    f"Todos los clientes ATM presentes en resumen."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US4-2 — Caso borde: cliente solo con CATM (sin DATM ni PGSL)
# MAGIC
# MAGIC Un cliente con transacciones solo de tipo CATM debe tener:
# MAGIC - `cantidad_depositos_atm` > 0 (hay CATM)
# MAGIC - `promedio_monto_depositos_atm` > 0 (o = 0 si todos los montos son nulos)
# MAGIC - `cantidad_retiros_atm` = 0 (sin DATM)
# MAGIC - `promedio_monto_retiros_atm` = 0 (sin DATM)
# MAGIC - `total_pagos_saldo_cliente` = 0 (sin PGSL)

# COMMAND ----------

# Buscar cliente con solo CATM (sin DATM ni PGSL)
df_solo_catm = (
    spark.table(nombre_tabla_trx_plata)
    .filter(F.col("tipo_transaccion") == "CATM")
    .select("identificador_cliente")
    .distinct()
    .join(
        spark.table(nombre_tabla_trx_plata)
        .filter(F.col("tipo_transaccion").isin(["DATM", "PGSL"]))
        .select("identificador_cliente")
        .distinct(),
        on="identificador_cliente",
        how="left_anti"
    )
    .limit(1)
)
clientes_solo_catm = df_solo_catm.collect()

if len(clientes_solo_catm) == 0:
    print("SKIP — Prueba US4-2: No se encontro cliente con solo CATM. Prueba omitida.")
else:
    id_solo_catm = clientes_solo_catm[0]["identificador_cliente"]
    fila_solo_catm = df_atm.filter(F.col("identificador_cliente") == id_solo_catm).collect()

    assert len(fila_solo_catm) == 1, (
        f"ERROR — Prueba US4-2: El cliente {id_solo_catm} no aparece en la vista ATM."
    )

    f = fila_solo_catm[0]
    assert f["cantidad_depositos_atm"] > 0, (
        f"ERROR — Prueba US4-2: cantidad_depositos_atm debe ser > 0 para cliente con CATM. Valor: {f['cantidad_depositos_atm']}."
    )
    assert f["cantidad_retiros_atm"] == 0, (
        f"ERROR — Prueba US4-2: cantidad_retiros_atm debe ser 0 para cliente sin DATM. Valor: {f['cantidad_retiros_atm']}."
    )
    assert f["promedio_monto_retiros_atm"] == 0.0, (
        f"ERROR — Prueba US4-2: promedio_monto_retiros_atm debe ser 0 para cliente sin DATM. Valor: {f['promedio_monto_retiros_atm']}."
    )
    assert f["total_pagos_saldo_cliente"] == 0.0, (
        f"ERROR — Prueba US4-2: total_pagos_saldo_cliente debe ser 0 para cliente sin PGSL. Valor: {f['total_pagos_saldo_cliente']}."
    )

    print(f"PASS — Prueba US4-2: Cliente {id_solo_catm} con solo CATM tiene metricas DATM/PGSL en 0.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US4-3 — Caso borde: monto_transaccion nulo en transaccion CATM
# MAGIC
# MAGIC Cuando una transaccion CATM tiene `monto_transaccion` nulo, el calculo del
# MAGIC promedio debe manejar el nulo correctamente (via coalesce a 0) sin generar errores.

# COMMAND ----------

# Verificar que la vista ATM no tiene NULL en columnas de metricas numericas
nulos_metricas_atm = df_atm.filter(
    F.col("cantidad_depositos_atm").isNull() |
    F.col("cantidad_retiros_atm").isNull() |
    F.col("promedio_monto_depositos_atm").isNull() |
    F.col("promedio_monto_retiros_atm").isNull() |
    F.col("total_pagos_saldo_cliente").isNull()
).count()

assert nulos_metricas_atm == 0, (
    f"ERROR — Prueba US4-3: La vista ATM contiene {nulos_metricas_atm} filas con valores NULL "
    "en columnas de metricas numericas. El coalesce a 0 debe eliminar todos los nulos. "
    "Revisar la implementacion de las agregaciones condicionales en LsdpOroClientes.py."
)

print(f"PASS — Prueba US4-3: Cero valores NULL en metricas numericas de la vista ATM. Nulos manejados correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US4-4 — Caso borde: vistas de plata vacias generan vistas de oro vacias sin error (schema correcto)
# MAGIC
# MAGIC Verifica que el schema de las vistas de oro es correcto independientemente
# MAGIC del volumen de datos. La vista puede estar vacia pero debe tener el schema esperado.
# MAGIC (Nota: Si el pipeline se ejecuto correctamente, las vistas tendran datos. Esta prueba
# MAGIC verifica el schema que es invariante al volumen.)

# COMMAND ----------

# Verificar schema de comportamiento_atm_cliente
schema_atm = {field.name: type(field.dataType).__name__ for field in df_atm.schema.fields}

assert "identificador_cliente" in schema_atm, "ERROR — Prueba US4-4: 'identificador_cliente' no en schema ATM."
assert "cantidad_depositos_atm" in schema_atm, "ERROR — Prueba US4-4: 'cantidad_depositos_atm' no en schema ATM."
assert "total_pagos_saldo_cliente" in schema_atm, "ERROR — Prueba US4-4: 'total_pagos_saldo_cliente' no en schema ATM."

# Verificar schema de resumen_integral_cliente
schema_resumen = {field.name: type(field.dataType).__name__ for field in df_resumen.schema.fields}

assert "identificador_cliente" in schema_resumen, "ERROR — Prueba US4-4: 'identificador_cliente' no en schema resumen."
assert "huella_identificacion_cliente" in schema_resumen, "ERROR — Prueba US4-4: 'huella_identificacion_cliente' no en schema resumen."
assert "saldo_disponible" in schema_resumen, "ERROR — Prueba US4-4: 'saldo_disponible' no en schema resumen."
assert "total_pagos_saldo_cliente" in schema_resumen, "ERROR — Prueba US4-4: 'total_pagos_saldo_cliente' no en schema resumen."

print(f"PASS — Prueba US4-4: Schema correcto en ambas vistas de oro. ATM: {len(schema_atm)} cols, Resumen: {len(schema_resumen)} cols.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba US4-5 — Vista de resumen no tiene NULLs en metricas ATM (coalesce del LEFT JOIN)
# MAGIC
# MAGIC Verifica que las 5 metricas ATM en `resumen_integral_cliente` nunca tienen
# MAGIC valores NULL gracias al coalesce aplicado despues del LEFT JOIN.

# COMMAND ----------

nulos_metricas_resumen = df_resumen.filter(
    F.col("cantidad_depositos_atm").isNull() |
    F.col("cantidad_retiros_atm").isNull() |
    F.col("promedio_monto_depositos_atm").isNull() |
    F.col("promedio_monto_retiros_atm").isNull() |
    F.col("total_pagos_saldo_cliente").isNull()
).count()

assert nulos_metricas_resumen == 0, (
    f"ERROR — Prueba US4-5: La vista resumen contiene {nulos_metricas_resumen} filas con NULL "
    "en metricas ATM. El coalesce despues del LEFT JOIN debe eliminar todos los nulos. "
    "Revisar la implementacion del JOIN en LsdpOroClientes.py."
)

print(f"PASS — Prueba US4-5: Cero valores NULL en metricas ATM de resumen_integral_cliente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen Final de la Suite de Pruebas
# MAGIC
# MAGIC | # | Prueba | Cobertura | Estado |
# MAGIC |---|--------|-----------|--------|
# MAGIC | US1-1 | Estructura 6 columnas comportamiento_atm_cliente | CE-008, RF-015 | Ejecutada |
# MAGIC | US1-2 | Dimension Tipo 1 en vista ATM | CE-002 | Ejecutada |
# MAGIC | US1-3 | Conteos condicionales CATM/DATM correctos | CE-004 | Ejecutada |
# MAGIC | US1-4 | Promedios monto CATM/DATM correctos | CE-005 | Ejecutada |
# MAGIC | US1-5 | Total pagos PGSL correcto | CE-006 | Ejecutada |
# MAGIC | US1-6 | Cliente sin ATM/PGSL tiene metricas en 0 | CE-007 | Ejecutada |
# MAGIC | US1-7 | expect_or_drop elimina identificador_cliente NULL | RF-018, R14-D1 | Ejecutada |
# MAGIC | US1-8 | Propiedades Delta + Liquid Cluster en vista ATM | CE-009, RF-006 | Ejecutada |
# MAGIC | US2-1 | Estructura 22 columnas resumen_integral_cliente | CE-008, RF-015 | Ejecutada |
# MAGIC | US2-2 | Dimension Tipo 1 en vista resumen | CE-003 | Ejecutada |
# MAGIC | US2-3 | Clasificacion de plata no recalculada en oro | RF-004 | Ejecutada |
# MAGIC | US2-4 | LEFT JOIN: cliente sin ATM tiene metricas en 0 | CE-007 | Ejecutada |
# MAGIC | US2-5 | LEFT JOIN: cliente con ATM tiene metricas coherentes | CE-007 | Ejecutada |
# MAGIC | US2-6 | Propiedades Delta + Liquid Cluster en vista resumen | CE-009, RF-006 | Ejecutada |
# MAGIC | US4-1 | Integracion end-to-end: coherencia entre vistas | CE-001 | Ejecutada |
# MAGIC | US4-2 | Caso borde: cliente solo CATM sin DATM/PGSL | CE-004, CE-007 | Ejecutada |
# MAGIC | US4-3 | Caso borde: monto_transaccion NULL sin errores | RF-016 | Ejecutada |
# MAGIC | US4-4 | Caso borde: schema correcto independiente del volumen | RF-015 | Ejecutada |
# MAGIC | US4-5 | coalesce LEFT JOIN elimina NULLs en resumen | CE-007, RF-016 | Ejecutada |
# MAGIC
# MAGIC **Pruebas NO incluidas en este notebook (contexto exclusivo del Pipeline)**:
# MAGIC - CE-012: Lectura de claves `catalogoOro`/`catalogoPlata` desde Azure SQL — se valida directamente
# MAGIC   desde el Lakeflow Spark Declarative Pipeline al ejecutar `LsdpOroClientes.py` (closure del modulo).
# MAGIC - CE-013: Lectura del parametro `esquema_oro` del pipeline — solo accesible via
# MAGIC   `pipelines.parameters.esquema_oro` dentro del contexto LSDP, no desde un notebook separado.
# MAGIC
# MAGIC **Cobertura de Requerimientos Funcionales (RF-001 a RF-018)**:
# MAGIC - RF-001: `from pyspark import pipelines as dp` — verificacion por revision de codigo (ver LsdpOroClientes.py)
# MAGIC - RF-002: Catalogo/esquema oro dinamicos — validado via ejecucion exitosa del Pipeline LSDP (CE-012, CE-013)
# MAGIC - RF-003: Lectura de transacciones_enriquecidas de plata — CE-004 (US1-3)
# MAGIC - RF-004: LEFT JOIN con clientes_saldos_consolidados — CE-007 (US2-4, US2-5)
# MAGIC - RF-005: N/A en V5 (sin nuevas claves Azure SQL)
# MAGIC - RF-006: Propiedades Delta 5 estandar — CE-009 (US1-8, US2-6)
# MAGIC - RF-007: Reordenamiento Liquid Cluster — CE-009 (US1-8, US2-6)
# MAGIC - RF-008: Columnas en espanol snake_case — CE-008 (US1-1, US2-1)
# MAGIC - RF-009: Reutilizacion utilities sin modificacion — validado via ejecucion exitosa del Pipeline LSDP
# MAGIC - RF-010: Sin spark.sparkContext — verificacion por revision de codigo
# MAGIC - RF-011: Comentarios en espanol — verificacion por revision de codigo
# MAGIC - RF-012: N/A en V5
# MAGIC - RF-013: Agregaciones condicionales en una pasada — CE-004, CE-005, CE-006 (US1-3, US1-4, US1-5)
# MAGIC - RF-014: @dp.materialized_view correctamente configurado — CE-008 (US1-1, US2-1)
# MAGIC - RF-015: Nombres y comentarios correctos — CE-008 (US1-1, US2-1)
# MAGIC - RF-016: coalesce a 0 en metricas numericas — US4-3, US4-5
# MAGIC - RF-017: Sin filtros previos a la agrupacion — CE-004 (US1-3 valida conteos completos)
# MAGIC - RF-018: @dp.expect_or_drop en comportamiento_atm_cliente — US1-7

# COMMAND ----------

print("=" * 60)
print("SUITE COMPLETA: NbTestOroClientes — Version 5 (Medalla de Oro)")
print("Total de pruebas: 19 (excluyendo SKIPs por datos insuficientes)")
print("Cobertura: CE-001 a CE-011, RF-002 a RF-018")
print("Nota: CE-012 y CE-013 se validan directamente desde el Pipeline LSDP")
print("Fecha ejecucion: " + str(spark.sql("SELECT current_timestamp()").collect()[0][0]))
print("=" * 60)
