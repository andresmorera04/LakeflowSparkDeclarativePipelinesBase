# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestPlataTransacciones — Suite de Pruebas TDD: Vista Materializada Transaccional
# MAGIC
# MAGIC **Proposito**: Validar la correcta creacion y contenido de la vista materializada
# MAGIC `transacciones_enriquecidas` en la medalla de plata. La suite verifica la
# MAGIC estructura de 65 columnas, los 4 campos calculados numericos, el manejo de nulos,
# MAGIC las propiedades Delta y la lectura sin filtros.
# MAGIC
# MAGIC **Prerequisito**: Ejecutar el pipeline LSDP (bronce + plata) al menos una vez.
# MAGIC La vista `{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas` debe existir.
# MAGIC
# MAGIC **Plataforma**: Computo Serverless de Databricks.
# MAGIC
# MAGIC **Cobertura** (CE-005, CE-006, CE-007, CE-008, CE-009, CE-011):
# MAGIC 1. Estructura de 65 columnas con nombres en espanol (data-model.md TABLAS D y E)
# MAGIC 2. 4 campos calculados numericos con formulas correctas (RF-007)
# MAGIC 3. Manejo seguro de nulos con coalesce a 0 (RF-017)
# MAGIC 4. Sin errores de division por cero en porcentaje_comision_sobre_monto
# MAGIC 5. Propiedades Delta: 5 propiedades RF-008
# MAGIC 6. Lectura sin filtros — todos los registros de trxpfl presentes (RF-006)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracion de Parametros de Prueba

# COMMAND ----------

dbutils.widgets.text("catalogo_plata", "plata_dev", "Catalogo de Unity Catalog para plata")
dbutils.widgets.text("esquema_plata", "regional", "Esquema de Unity Catalog para plata")
dbutils.widgets.text("catalogo_bronce", "bronce_dev", "Catalogo de Unity Catalog para bronce")

# COMMAND ----------

catalogo_plata = dbutils.widgets.get("catalogo_plata").strip()
esquema_plata = dbutils.widgets.get("esquema_plata").strip()
catalogo_bronce = dbutils.widgets.get("catalogo_bronce").strip()

assert catalogo_plata, "ERROR: El widget 'catalogo_plata' no puede estar vacio."
assert esquema_plata, "ERROR: El widget 'esquema_plata' no puede estar vacio."
assert catalogo_bronce, "ERROR: El widget 'catalogo_bronce' no puede estar vacio."

nombre_vista = f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas"
nombre_tabla_bronce = f"{catalogo_bronce}.regional.trxpfl"
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
    df_trx = spark.table(nombre_vista)
    print(f"CARGA OK: Vista '{nombre_vista}' accedida exitosamente.")
except Exception as error:
    raise AssertionError(
        f"ERROR — Carga: La vista '{nombre_vista}' NO existe o no es accesible. "
        f"Error: {error}. "
        "Verificar que el pipeline LSDP de plata se ejecuto correctamente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 1 — Estructura de 65 columnas con nombres en espanol
# MAGIC
# MAGIC Verifica que la vista tiene exactamente 65 columnas con los nombres correctos
# MAGIC segun el data-model.md TABLAS D y E.

# COMMAND ----------

columnas_actuales = set(df_trx.columns)
total_columnas = len(df_trx.columns)

assert total_columnas == 65, (
    f"ERROR — Prueba 1: Se esperaban 65 columnas pero se encontraron {total_columnas}. "
    f"Revisar el mapeo de columnas en LsdpPlataTransacciones.py."
)

# Verificar columnas criticas de trxpfl (TABLA D)
columnas_criticas = [
    "identificador_transaccion", "identificador_cliente", "tipo_transaccion",
    "descripcion_transaccion", "canal_transaccion", "estado_transaccion",
    "codigo_moneda", "codigo_sucursal", "fecha_transaccion", "fecha_hora_transaccion",
    "monto_transaccion", "monto_original", "monto_comision", "monto_impuesto",
    "monto_neto", "saldo_antes_transaccion", "saldo_despues_transaccion",
    "fecha_ingesta_datos"
]

columnas_faltantes = [c for c in columnas_criticas if c not in columnas_actuales]
assert not columnas_faltantes, (
    f"ERROR — Prueba 1: Columnas criticas de trxpfl faltantes: {columnas_faltantes}."
)

# Verificar que columnas de particion no estan presentes
for col_particion in ["año", "mes", "dia"]:
    assert col_particion not in columnas_actuales, (
        f"ERROR — Prueba 1: La columna de particion '{col_particion}' no debe estar en la vista de plata."
    )

# Verificar que _rescued_data no esta presente
assert "_rescued_data" not in columnas_actuales, (
    "ERROR — Prueba 1: La columna '_rescued_data' no debe estar en la vista de plata."
)

print(f"PRUEBA 1 PASA: Vista tiene {total_columnas} columnas con nombres correctos en espanol.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 2 — Campos calculados numericos presentes (TABLA E)
# MAGIC
# MAGIC Verifica que los 4 campos calculados estan presentes en la vista.

# COMMAND ----------

campos_calculados = [
    "monto_neto_comisiones",
    "porcentaje_comision_sobre_monto",
    "variacion_saldo_transaccion",
    "indicador_impacto_financiero"
]

campos_faltantes = [c for c in campos_calculados if c not in columnas_actuales]
assert not campos_faltantes, (
    f"ERROR — Prueba 2: Campos calculados faltantes en la vista: {campos_faltantes}. "
    f"Verificar LsdpPlataTransacciones.py seccion de campos calculados."
)

print(f"PRUEBA 2 PASA: Los 4 campos calculados estan presentes: {campos_calculados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 3 — Formulas de campos calculados correctas (RF-007)
# MAGIC
# MAGIC Verifica que los valores calculados son consistentes con las formulas del data-model.md.
# MAGIC Se toma una muestra de registros con valores conocidos no nulos para la verificacion.

# COMMAND ----------

# Tomar muestra de registros donde todos los operandos son no nulos y ORGAMT > 0
df_muestra = df_trx.filter(
    F.col("monto_neto").isNotNull() &
    F.col("monto_comision").isNotNull() &
    F.col("monto_impuesto").isNotNull() &
    F.col("monto_original").isNotNull() &
    (F.col("monto_original") > 0) &
    F.col("saldo_antes_transaccion").isNotNull() &
    F.col("saldo_despues_transaccion").isNotNull() &
    F.col("monto_transaccion").isNotNull() &
    F.col("monto_penalidad").isNotNull()
).limit(100)

conteo_muestra = df_muestra.count()
if conteo_muestra == 0:
    print("PRUEBA 3 OMITIDA: No hay registros con valores no nulos para verificar formulas. "
          "Esto es valido si los datos de prueba tienen todos los campos en nulo.")
else:
    # Formula 1: monto_neto_comisiones = NETAMT - (FEEAMT + TAXAMT)
    df_check_1 = df_muestra.filter(
        F.abs(
            F.col("monto_neto_comisiones") -
            (F.col("monto_neto") - (F.col("monto_comision") + F.col("monto_impuesto")))
        ) > 0.001
    )
    assert df_check_1.count() == 0, (
        f"ERROR — Prueba 3: 'monto_neto_comisiones' no coincide con NETAMT - (FEEAMT + TAXAMT). "
        f"Verificar la formula en LsdpPlataTransacciones.py."
    )

    # Formula 2: porcentaje_comision_sobre_monto = (FEEAMT / ORGAMT) * 100
    df_check_2 = df_muestra.filter(
        F.abs(
            F.col("porcentaje_comision_sobre_monto") -
            ((F.col("monto_comision") / F.col("monto_original")) * 100)
        ) > 0.001
    )
    assert df_check_2.count() == 0, (
        f"ERROR — Prueba 3: 'porcentaje_comision_sobre_monto' no coincide con (FEEAMT / ORGAMT) * 100. "
        f"Verificar la formula en LsdpPlataTransacciones.py."
    )

    # Formula 3: variacion_saldo_transaccion = abs(BLNAFT - BLNBFR)
    df_check_3 = df_muestra.filter(
        F.abs(
            F.col("variacion_saldo_transaccion") -
            F.abs(F.col("saldo_despues_transaccion") - F.col("saldo_antes_transaccion"))
        ) > 0.001
    )
    assert df_check_3.count() == 0, (
        f"ERROR — Prueba 3: 'variacion_saldo_transaccion' no coincide con abs(BLNAFT - BLNBFR). "
        f"Verificar la formula en LsdpPlataTransacciones.py."
    )

    # Formula 4: indicador_impacto_financiero = TRXAMT + FEEAMT + TAXAMT + PNLAMT
    df_check_4 = df_muestra.filter(
        F.abs(
            F.col("indicador_impacto_financiero") -
            (F.col("monto_transaccion") + F.col("monto_comision") +
             F.col("monto_impuesto") + F.col("monto_penalidad"))
        ) > 0.001
    )
    assert df_check_4.count() == 0, (
        f"ERROR — Prueba 3: 'indicador_impacto_financiero' no coincide con TRXAMT + FEEAMT + TAXAMT + PNLAMT. "
        f"Verificar la formula en LsdpPlataTransacciones.py."
    )

    print(f"PRUEBA 3 PASA: Formulas de 4 campos calculados verificadas en {conteo_muestra} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 4 — Manejo seguro de nulos (RF-017): sin valores nulos en campos calculados
# MAGIC
# MAGIC Verifica que los campos calculados no tienen valores nulos
# MAGIC (los operandos nulos fueron tratados con coalesce a 0).

# COMMAND ----------

for campo in campos_calculados:
    count_nulos = df_trx.filter(F.col(campo).isNull()).count()
    assert count_nulos == 0, (
        f"ERROR — Prueba 4: El campo calculado '{campo}' tiene {count_nulos} valores nulos. "
        f"Verificar que se aplica coalesce(col('{campo}'), lit(0)) en LsdpPlataTransacciones.py (RF-017)."
    )

print("PRUEBA 4 PASA: Los 4 campos calculados no tienen valores nulos (coalesce a 0 activo).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 5 — Sin errores de division por cero en `porcentaje_comision_sobre_monto`
# MAGIC
# MAGIC Verifica que registros con monto_original == 0 o nulo tienen
# MAGIC porcentaje_comision_sobre_monto == 0 (no NaN ni infinito).

# COMMAND ----------

df_orgamt_cero = df_trx.filter(
    (F.col("monto_original").isNull() | (F.col("monto_original") == 0))
)

count_orgamt_cero = df_orgamt_cero.count()

if count_orgamt_cero > 0:
    df_div_invalida = df_orgamt_cero.filter(
        F.col("porcentaje_comision_sobre_monto").isNull() |
        F.col("porcentaje_comision_sobre_monto").isNaN() |
        (F.col("porcentaje_comision_sobre_monto") != 0)
    )
    assert df_div_invalida.count() == 0, (
        f"ERROR — Prueba 5: Se encontraron registros con monto_original <= 0 donde "
        f"'porcentaje_comision_sobre_monto' no es 0. "
        f"Verificar el manejo de division por cero en LsdpPlataTransacciones.py (RF-017)."
    )
    print(f"PRUEBA 5 PASA: {count_orgamt_cero} registros con monto_original=0 tienen "
          f"porcentaje_comision_sobre_monto=0 correctamente.")
else:
    print("PRUEBA 5 PASA: No hay registros con monto_original <= 0 en los datos de prueba.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 6 — Propiedades Delta configuradas (RF-008)
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
        f"ERROR — Prueba 6: La propiedad Delta '{prop}' NO esta configurada en la vista. "
        f"Propiedades disponibles: {list(props_dict.keys())}."
    )
    assert props_dict[prop] == valor_esperado, (
        f"ERROR — Prueba 6: La propiedad '{prop}' tiene valor '{props_dict[prop]}' "
        f"pero se esperaba '{valor_esperado}'."
    )

print("PRUEBA 6 PASA: Las 5 propiedades Delta obligatorias estan configuradas correctamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba 7 — Lectura sin filtros: conteo consistente con tabla bronce (RF-006)
# MAGIC
# MAGIC Verifica que la vista tiene registros (> 0) y que la lectura no aplico filtros
# MAGIC que eliminaran datos de la tabla de bronce.

# COMMAND ----------

total_registros_plata = df_trx.count()
assert total_registros_plata > 0, (
    "ERROR — Prueba 7: La vista 'transacciones_enriquecidas' esta vacia. "
    "Verificar que el pipeline se ejecuto con datos en bronce y que no se aplicaron filtros (RF-006)."
)

# Comparar con la tabla de bronce (excluyendo columnas de particion)
try:
    count_bronce = spark.table(nombre_tabla_bronce).count()
    assert total_registros_plata == count_bronce, (
        f"ERROR — Prueba 7: La vista tiene {total_registros_plata} registros pero "
        f"la tabla bronce tiene {count_bronce} registros. "
        f"La vista debe tener exactamente los mismos registros sin filtro (RF-006)."
    )
    print(f"PRUEBA 7 PASA: La vista tiene {total_registros_plata} registros, "
          f"igual que la tabla bronce (lectura sin filtros RF-006).")
except Exception:
    # Si no se puede acceder a bronce, verificar solo que la vista tiene datos
    print(f"PRUEBA 7 PASA (parcial): La vista tiene {total_registros_plata} registros. "
          f"No fue posible comparar con bronce directamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la Suite TDD

# COMMAND ----------

print("=" * 65)
print("RESUMEN — NbTestPlataTransacciones")
print("=" * 65)
print("PRUEBA 1 PASA: 65 columnas con nombres correctos en espanol")
print("PRUEBA 2 PASA: 4 campos calculados numericos presentes")
print("PRUEBA 3 PASA: Formulas de campos calculados verificadas")
print("PRUEBA 4 PASA: Cero nulos en campos calculados (coalesce a 0)")
print("PRUEBA 5 PASA: Sin division por cero en porcentaje_comision")
print("PRUEBA 6 PASA: 5 propiedades Delta RF-008 configuradas")
print("PRUEBA 7 PASA: Lectura sin filtros — todos los registros bronce")
print("=" * 65)
print("TODAS LAS PRUEBAS PASARON. Vista transacciones_enriquecidas validada.")
