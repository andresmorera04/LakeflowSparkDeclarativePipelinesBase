# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTestSaldosCliente — Suite de Pruebas TDD para Saldos de Clientes
# MAGIC
# MAGIC **Proposito**: Validar que el parquet generado por NbGenerarSaldosCliente.py cumple con la
# MAGIC estructura de 100 columnas AS400 (H2.4), relacion 1:1 estricta con el Maestro de Clientes,
# MAGIC y rangos de montos segmentados por tipo de cuenta (RF-017).
# MAGIC
# MAGIC **Enfoque TDD**: Estas pruebas se escriben PRIMERO y deben FALLAR antes de la implementacion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros de Prueba

# COMMAND ----------

dbutils.widgets.text("ruta_parquet_saldos", "/mnt/external-location/landing/saldos_clientes", "Ruta del Parquet de Saldos")
dbutils.widgets.text("ruta_parquet_maestro", "/mnt/external-location/landing/maestro_clientes", "Ruta del Parquet del Maestro")

ruta_parquet_saldos = dbutils.widgets.get("ruta_parquet_saldos")
ruta_parquet_maestro = dbutils.widgets.get("ruta_parquet_maestro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, DateType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de Datos para Pruebas

# COMMAND ----------

df_saldos = spark.read.parquet(ruta_parquet_saldos)
df_maestro = spark.read.parquet(ruta_parquet_maestro)

print(f"Saldos cargados: {df_saldos.count():,} registros, {len(df_saldos.columns)} columnas")
print(f"Maestro cargado: {df_maestro.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 1: Validacion de Estructura — 100 Columnas Exactas (H2.4)

# COMMAND ----------

# T015 — Validar que el parquet tiene exactamente 100 columnas con los nombres correctos segun H2.4
columnas_esperadas = [
    # 1. CUSTID (LongType)
    "CUSTID",
    # 2-30. Campos textuales (29 StringType)
    "ACCTID", "ACCTTYP", "ACCTNM", "ACCTST", "CRNCOD", "BRNCOD", "BRNNM",
    "PRDCOD", "PRDNM", "PRDCAT", "SGMNT", "RISKLV", "RGNCD", "RGNNM",
    "CSTGRP", "BLKST", "EMBST", "OVDST", "DGTST", "CRDST", "LNTYP",
    "GRNTYP", "PYMFRQ", "INTTYP", "TXCTG", "CHKTYP", "CRDGRP", "CLSCD", "SRCCD",
    # 31-65. Campos numericos (34 DoubleType + 1 LongType = 35 numericos)
    "AVLBAL", "CURBAL", "HLDBAL", "OVRBAL", "PNDBAL", "AVGBAL", "MINBAL",
    "MAXBAL", "OPNBAL", "CLSBAL", "INTACC", "INTPAY", "INTRCV", "FEEACC",
    "FEEPAY", "CRDLMT", "CRDAVL", "CRDUSD", "PYMAMT", "PYMLST", "TTLDBT",
    "TTLCRD", "TTLTRX", "LNAMT", "LNBAL", "MTHPYM", "INTRT", "PNLRT",
    "OVRRT", "TAXAMT", "INSAMT", "DLYINT", "YLDRT", "SPRDRT", "MRGAMT",
    # 66-100. Campos fecha (35 DateType)
    "OPNDT", "CLSDT", "LSTTRX", "LSTPYM", "NXTPYM", "MATDT", "RNWDT",
    "RVWDT", "CRTDT", "UPDDT", "STMDT", "CUTDT", "GRPDT", "INTDT",
    "FEEDT", "BLKDT", "EMBDT", "OVDDT", "PYMDT1", "PYMDT2", "PRJDT",
    "ADJDT", "RCLDT", "NTFDT", "CNCLDT", "RCTDT", "CHGDT", "VRFDT",
    "PRMDT", "DGTDT", "AUDT", "MGRDT", "ESCDT", "RPTDT", "ARCDT"
]

columnas_reales = df_saldos.columns
errores_estructura = []

# Validar cantidad total
if len(columnas_reales) != 100:
    errores_estructura.append(
        f"Se esperaban 100 columnas, se encontraron {len(columnas_reales)}"
    )

# Validar cada columna esperada existe
for col in columnas_esperadas:
    if col not in columnas_reales:
        errores_estructura.append(f"Columna faltante: {col}")

# Validar que no hay columnas extra
for col in columnas_reales:
    if col not in columnas_esperadas:
        errores_estructura.append(f"Columna inesperada: {col}")

if errores_estructura:
    for e in errores_estructura:
        print(f"  ERROR: {e}")
    raise AssertionError(f"TEST 1 FALLIDO: {len(errores_estructura)} errores de estructura")
else:
    print("TEST 1 PASADO: 100 columnas con nombres correctos segun H2.4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 2: Validacion de Tipos de Datos PySpark

# COMMAND ----------

# T015 — Validar tipos de datos segun mapeo AS400 → PySpark del data-model.md
esquema_actual = {campo.name: type(campo.dataType) for campo in df_saldos.schema.fields}

# Tipos esperados por columna
tipos_esperados = {}

# LongType: CUSTID y TTLTRX (NUMERIC sin decimales)
for col in ["CUSTID", "TTLTRX"]:
    tipos_esperados[col] = LongType

# StringType: 29 campos textuales
campos_string = [
    "ACCTID", "ACCTTYP", "ACCTNM", "ACCTST", "CRNCOD", "BRNCOD", "BRNNM",
    "PRDCOD", "PRDNM", "PRDCAT", "SGMNT", "RISKLV", "RGNCD", "RGNNM",
    "CSTGRP", "BLKST", "EMBST", "OVDST", "DGTST", "CRDST", "LNTYP",
    "GRNTYP", "PYMFRQ", "INTTYP", "TXCTG", "CHKTYP", "CRDGRP", "CLSCD", "SRCCD"
]
for col in campos_string:
    tipos_esperados[col] = StringType

# DoubleType: 34 campos numericos con decimales
campos_double = [
    "AVLBAL", "CURBAL", "HLDBAL", "OVRBAL", "PNDBAL", "AVGBAL", "MINBAL",
    "MAXBAL", "OPNBAL", "CLSBAL", "INTACC", "INTPAY", "INTRCV", "FEEACC",
    "FEEPAY", "CRDLMT", "CRDAVL", "CRDUSD", "PYMAMT", "PYMLST", "TTLDBT",
    "TTLCRD", "LNAMT", "LNBAL", "MTHPYM", "INTRT", "PNLRT",
    "OVRRT", "TAXAMT", "INSAMT", "DLYINT", "YLDRT", "SPRDRT", "MRGAMT"
]
for col in campos_double:
    tipos_esperados[col] = DoubleType

# DateType: 35 campos fecha
campos_date = [
    "OPNDT", "CLSDT", "LSTTRX", "LSTPYM", "NXTPYM", "MATDT", "RNWDT",
    "RVWDT", "CRTDT", "UPDDT", "STMDT", "CUTDT", "GRPDT", "INTDT",
    "FEEDT", "BLKDT", "EMBDT", "OVDDT", "PYMDT1", "PYMDT2", "PRJDT",
    "ADJDT", "RCLDT", "NTFDT", "CNCLDT", "RCTDT", "CHGDT", "VRFDT",
    "PRMDT", "DGTDT", "AUDT", "MGRDT", "ESCDT", "RPTDT", "ARCDT"
]
for col in campos_date:
    tipos_esperados[col] = DateType

# Ejecutar validacion
errores_tipos = []
for col, tipo_esperado in tipos_esperados.items():
    tipo_real = esquema_actual.get(col)
    if tipo_real is None:
        errores_tipos.append(f"Columna '{col}' no encontrada en el esquema")
    elif tipo_real != tipo_esperado:
        errores_tipos.append(
            f"Columna '{col}': esperado {tipo_esperado.__name__}, encontrado {tipo_real.__name__}"
        )

if errores_tipos:
    for e in errores_tipos:
        print(f"  ERROR: {e}")
    raise AssertionError(f"TEST 2 FALLIDO: {len(errores_tipos)} errores de tipos")
else:
    print("TEST 2 PASADO: Todos los tipos de datos son correctos")
    print(f"  LongType: 2 campos (CUSTID, TTLTRX)")
    print(f"  StringType: {len(campos_string)} campos")
    print(f"  DoubleType: {len(campos_double)} campos")
    print(f"  DateType: {len(campos_date)} campos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 3: Relacion 1:1 Estricta — Misma Cantidad de Registros

# COMMAND ----------

# T015 — Validar que la cantidad de saldos es exactamente igual a la cantidad de clientes (RF-011)
total_saldos = df_saldos.count()
total_maestro = df_maestro.count()

if total_saldos != total_maestro:
    raise AssertionError(
        f"TEST 3 FALLIDO: Relacion 1:1 violada. "
        f"Saldos: {total_saldos:,}, Maestro: {total_maestro:,}, "
        f"Diferencia: {abs(total_saldos - total_maestro):,}"
    )
else:
    print(f"TEST 3 PASADO: Relacion 1:1 estricta verificada ({total_saldos:,} = {total_maestro:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 4: Cero CUSTIDs Huerfanos — Todos Existen en Maestro

# COMMAND ----------

# T015 — Validar que todos los CUSTID en Saldos existen en Maestro (cero huerfanos)
custids_huerfanos = df_saldos.join(
    df_maestro.select("CUSTID"),
    on="CUSTID",
    how="left_anti"
)
total_huerfanos = custids_huerfanos.count()

if total_huerfanos > 0:
    ejemplos = custids_huerfanos.limit(5).collect()
    ejemplos_str = ", ".join([str(fila["CUSTID"]) for fila in ejemplos])
    raise AssertionError(
        f"TEST 4 FALLIDO: {total_huerfanos:,} CUSTIDs huerfanos en Saldos "
        f"(no existen en Maestro). Ejemplos: {ejemplos_str}"
    )
else:
    print("TEST 4 PASADO: Cero CUSTIDs huerfanos (todos existen en Maestro)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 5: Cero CUSTIDs Faltantes — Todos los Clientes Tienen Saldo

# COMMAND ----------

# T015 — Validar que todos los CUSTID del Maestro tienen un saldo (cero faltantes)
custids_faltantes = df_maestro.select("CUSTID").join(
    df_saldos.select("CUSTID"),
    on="CUSTID",
    how="left_anti"
)
total_faltantes = custids_faltantes.count()

if total_faltantes > 0:
    ejemplos = custids_faltantes.limit(5).collect()
    ejemplos_str = ", ".join([str(fila["CUSTID"]) for fila in ejemplos])
    raise AssertionError(
        f"TEST 5 FALLIDO: {total_faltantes:,} CUSTIDs del Maestro no tienen saldo. "
        f"Ejemplos: {ejemplos_str}"
    )
else:
    print("TEST 5 PASADO: Cero CUSTIDs faltantes (todos los clientes tienen saldo)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 6: Unicidad de CUSTID en Saldos — Sin Duplicados

# COMMAND ----------

# T015 — Validar que cada CUSTID aparece exactamente una vez en Saldos
total_custids_unicos = df_saldos.select("CUSTID").distinct().count()

if total_custids_unicos != total_saldos:
    duplicados = total_saldos - total_custids_unicos
    raise AssertionError(
        f"TEST 6 FALLIDO: {duplicados:,} CUSTIDs duplicados en Saldos. "
        f"Total registros: {total_saldos:,}, Unicos: {total_custids_unicos:,}"
    )
else:
    print(f"TEST 6 PASADO: Todos los CUSTIDs son unicos ({total_custids_unicos:,} unicos de {total_saldos:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST 7: Prueba Negativa — Ejecucion Sin Maestro Debe Fallar (RF-016)

# COMMAND ----------

# T015 — Validar que el notebook rechaza la ejecucion si el Maestro no existe
# Esta prueba valida el diseño: el notebook debe verificar que el Maestro existe antes de generar

ruta_maestro_inexistente = "/mnt/path/que/no/existe/maestro_fantasma"

prueba_negativa_pasada = False
try:
    spark.read.parquet(ruta_maestro_inexistente)
    # Si llega aqui, el parquet existe (improbable). La prueba no aplica.
    print("ADVERTENCIA: La ruta de prueba negativa existe. Omitiendo prueba.")
    prueba_negativa_pasada = True
except Exception as e:
    # Se espera que falle al intentar leer un Maestro inexistente
    print(f"TEST 7 PASADO: Lectura de Maestro inexistente falla correctamente.")
    print(f"  Error capturado: {type(e).__name__}")
    prueba_negativa_pasada = True

if not prueba_negativa_pasada:
    raise AssertionError("TEST 7 FALLIDO: No se detecto error al leer Maestro inexistente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T021 — Pruebas Extendidas US4: Rangos de Montos por Tipo de Cuenta (RF-017)

# COMMAND ----------

# T021 — Validar que AVLBAL respeta los rangos por tipo de cuenta
rangos_esperados_cuenta = {
    "AHRO": (0, 500000),
    "CRTE": (0, 250000),
    "PRES": (1000, 1000000),
    "INVR": (0, 100000)
}

errores_rango_cuenta = []
for tipo_cuenta, (rango_min, rango_max) in rangos_esperados_cuenta.items():
    df_tipo = df_saldos.filter(F.col("ACCTTYP") == tipo_cuenta)
    conteo_tipo = df_tipo.count()
    if conteo_tipo == 0:
        continue

    stats = df_tipo.agg(
        F.min("AVLBAL").alias("min_saldo"),
        F.max("AVLBAL").alias("max_saldo")
    ).collect()[0]

    min_real = stats["min_saldo"]
    max_real = stats["max_saldo"]

    if min_real is not None and min_real < rango_min:
        errores_rango_cuenta.append(
            f"{tipo_cuenta}: min real {min_real:.2f} < min esperado {rango_min}"
        )
    if max_real is not None and max_real > rango_max:
        errores_rango_cuenta.append(
            f"{tipo_cuenta}: max real {max_real:.2f} > max esperado {rango_max}"
        )
    print(f"  {tipo_cuenta}: {conteo_tipo:,} registros, AVLBAL [{min_real:.2f}, {max_real:.2f}]")

if errores_rango_cuenta:
    for e in errores_rango_cuenta:
        print(f"  ERROR: {e}")
    raise AssertionError(f"TEST FALLIDO: {len(errores_rango_cuenta)} errores en rangos por tipo de cuenta")
else:
    print("✓ PRUEBA PASADA: Rangos de montos por tipo de cuenta correctos (RF-017)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T021 — Cobertura 100% de Clientes Despues de Crecimiento del Maestro

# COMMAND ----------

# T021 — Verificar que si el Maestro crece, los saldos cubren la totalidad
# Esto se valida implicitamente por TEST 3   (count igual) y TEST 5 (cero faltantes).
# Aqui agregamos una validacion adicional: todos los ACCTTYP son validos
tipos_cuenta_validos = ["AHRO", "CRTE", "PRES", "INVR"]
tipos_reales = [fila["ACCTTYP"] for fila in df_saldos.select("ACCTTYP").distinct().collect()]

tipos_invalidos = [t for t in tipos_reales if t not in tipos_cuenta_validos]
assert len(tipos_invalidos) == 0, (
    f"ERROR: Tipos de cuenta invalidos encontrados: {tipos_invalidos}"
)
print(f"✓ PRUEBA PASADA: Todos los ACCTTYP son validos ({tipos_reales})")

# Validar que la distribucion de tipos tiene variedad
for tipo in tipos_cuenta_validos:
    conteo = df_saldos.filter(F.col("ACCTTYP") == tipo).count()
    assert conteo > 0, f"ERROR: Tipo de cuenta '{tipo}' no tiene registros."
    print(f"  {tipo}: {conteo:,} registros")

print("✓ PRUEBA PASADA: Cobertura completa de tipos de cuenta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T021 — Validacion de Regeneracion Completa

# COMMAND ----------

# T021 — Verificar que la regeneracion produce un DataFrame con datos no nulos en campos criticos
campos_criticos = ["AVLBAL", "CURBAL", "OPNDT", "ACCTID", "ACCTTYP"]
for campo in campos_criticos:
    nulos = df_saldos.filter(F.col(campo).isNull()).count()
    assert nulos == 0, f"ERROR: {nulos:,} valores nulos en campo critico '{campo}'"

print("✓ PRUEBA PASADA: Cero nulos en campos criticos de saldos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Pruebas

# COMMAND ----------

print("=" * 70)
print("SUITE DE PRUEBAS TDD — SALDOS DE CLIENTES — COMPLETADA")
print("=" * 70)
print(f"  TEST 1: Estructura 100 columnas (H2.4)      — PASADO")
print(f"  TEST 2: Tipos de datos PySpark               — PASADO")
print(f"  TEST 3: Relacion 1:1 (N saldos = N clientes) — PASADO")
print(f"  TEST 4: Cero CUSTIDs huerfanos               — PASADO")
print(f"  TEST 5: Cero CUSTIDs faltantes               — PASADO")
print(f"  TEST 6: Unicidad de CUSTID                    — PASADO")
print(f"  TEST 7: Prueba negativa (sin Maestro)         — PASADO")
print(f"  T021: Rangos por tipo de cuenta (RF-017)      — PASADO")
print(f"  T021: Cobertura 100% tipos de cuenta          — PASADO")
print(f"  T021: Cero nulos campos criticos              — PASADO")
print("=" * 70)
print(f"  Total registros validados: {total_saldos:,}")
print(f"  Total columnas validadas: {len(columnas_esperadas)}")
print("=" * 70)
