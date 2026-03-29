# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarTransaccionalCliente — Generador del Parquet Transaccional
# MAGIC
# MAGIC **Proposito**: Generar un archivo parquet simulando la tabla TRXPFL (Transaction Processing File)
# MAGIC de AS400 para una entidad bancaria.
# MAGIC
# MAGIC **Estructura**: 60 columnas (30 numericos, 21 fechas, 9 textuales) segun H2.2.
# MAGIC
# MAGIC **Volumetria**: 15,000,000 de registros nuevos por cada ejecucion (no acumulativo).
# MAGIC
# MAGIC **Distribucion TRXTYP**: ~60% alta frecuencia, ~30% media, ~10% baja (ponderada, parametrizable).
# MAGIC
# MAGIC **Parametrizacion**: Todos los valores via `dbutils.widgets.text()` (Decision V2-R2-D1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## T011 — Definicion de Parametros (33 Widgets)

# COMMAND ----------

# Parametros de rutas y volumetria
dbutils.widgets.text("ruta_salida_parquet", "abfss://container@storageaccount.dfs.core.windows.net/landing/transaccional", "Ruta de Salida del Parquet")
dbutils.widgets.text("ruta_maestro_clientes", "abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes", "Ruta del Parquet del Maestro")
dbutils.widgets.text("cantidad_registros", "15000000", "Cantidad de Registros a Generar")
dbutils.widgets.text("fecha_transaccion", "", "Fecha de Transaccion (YYYY-MM-DD)")
dbutils.widgets.text("offset_trxid", "1", "Offset Inicial del TRXID Secuencial")
dbutils.widgets.text("num_particiones", "50", "Numero de Particiones del Parquet")

# Parametros de pesos de distribucion por banda de frecuencia
dbutils.widgets.text("pesos_alta", "60", "Peso Banda Alta Frecuencia (%)")
dbutils.widgets.text("pesos_media", "30", "Peso Banda Media Frecuencia (%)")
dbutils.widgets.text("pesos_baja", "10", "Peso Banda Baja Frecuencia (%)")

# Parametros de rangos de montos por tipo de transaccion (RF-017)
dbutils.widgets.text("rango_catm_min", "10", "CATM Monto Minimo")
dbutils.widgets.text("rango_catm_max", "1000", "CATM Monto Maximo")
dbutils.widgets.text("rango_datm_min", "10", "DATM Monto Minimo")
dbutils.widgets.text("rango_datm_max", "1000", "DATM Monto Maximo")
dbutils.widgets.text("rango_cmpr_min", "5", "CMPR Monto Minimo")
dbutils.widgets.text("rango_cmpr_max", "15000", "CMPR Monto Maximo")
dbutils.widgets.text("rango_tint_min", "50", "TINT Monto Minimo")
dbutils.widgets.text("rango_tint_max", "50000", "TINT Monto Maximo")
dbutils.widgets.text("rango_dpst_min", "50", "DPST Monto Minimo")
dbutils.widgets.text("rango_dpst_max", "100000", "DPST Monto Maximo")
dbutils.widgets.text("rango_pgsl_min", "100", "PGSL Monto Minimo")
dbutils.widgets.text("rango_pgsl_max", "25000", "PGSL Monto Maximo")
dbutils.widgets.text("rango_text_min", "50", "TEXT Monto Minimo")
dbutils.widgets.text("rango_text_max", "50000", "TEXT Monto Maximo")
dbutils.widgets.text("rango_rtro_min", "50", "RTRO Monto Minimo")
dbutils.widgets.text("rango_rtro_max", "100000", "RTRO Monto Maximo")
dbutils.widgets.text("rango_pgsv_min", "10", "PGSV Monto Minimo")
dbutils.widgets.text("rango_pgsv_max", "5000", "PGSV Monto Maximo")
dbutils.widgets.text("rango_nmna_min", "1000", "NMNA Monto Minimo")
dbutils.widgets.text("rango_nmna_max", "15000", "NMNA Monto Maximo")
dbutils.widgets.text("rango_intr_min", "1", "INTR Monto Minimo")
dbutils.widgets.text("rango_intr_max", "5000", "INTR Monto Maximo")
dbutils.widgets.text("rango_adsl_min", "500", "ADSL Monto Minimo")
dbutils.widgets.text("rango_adsl_max", "10000", "ADSL Monto Maximo")
dbutils.widgets.text("rango_impt_min", "1", "IMPT Monto Minimo")
dbutils.widgets.text("rango_impt_max", "2000", "IMPT Monto Maximo")
dbutils.widgets.text("rango_dmcl_min", "20", "DMCL Monto Minimo")
dbutils.widgets.text("rango_dmcl_max", "3000", "DMCL Monto Maximo")
dbutils.widgets.text("rango_cmsn_min", "1", "CMSN Monto Minimo")
dbutils.widgets.text("rango_cmsn_max", "500", "CMSN Monto Maximo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura y Validacion de Parametros (RF-016)

# COMMAND ----------

import datetime

# Lectura de parametros principales
ruta_salida_parquet = dbutils.widgets.get("ruta_salida_parquet")
ruta_maestro_clientes = dbutils.widgets.get("ruta_maestro_clientes")
cantidad_registros = dbutils.widgets.get("cantidad_registros")
fecha_transaccion = dbutils.widgets.get("fecha_transaccion")
offset_trxid = dbutils.widgets.get("offset_trxid")
num_particiones = dbutils.widgets.get("num_particiones")
pesos_alta = dbutils.widgets.get("pesos_alta")
pesos_media = dbutils.widgets.get("pesos_media")
pesos_baja = dbutils.widgets.get("pesos_baja")

# Validacion de rutas (obligatorias)
if not ruta_salida_parquet or ruta_salida_parquet.strip() == "":
    raise ValueError("ERROR: El parametro 'ruta_salida_parquet' no puede estar vacio.")
if not ruta_maestro_clientes or ruta_maestro_clientes.strip() == "":
    raise ValueError("ERROR: El parametro 'ruta_maestro_clientes' no puede estar vacio.")

# Validacion de fecha de transaccion (formato YYYY-MM-DD obligatorio)
if not fecha_transaccion or fecha_transaccion.strip() == "":
    raise ValueError("ERROR: El parametro 'fecha_transaccion' no puede estar vacio. Formato requerido: YYYY-MM-DD")
try:
    fecha_trx_parsed = datetime.datetime.strptime(fecha_transaccion.strip(), "%Y-%m-%d").date()
except ValueError:
    raise ValueError(
        f"ERROR: El formato de 'fecha_transaccion' es invalido: '{fecha_transaccion}'. "
        f"Formato requerido: YYYY-MM-DD (ejemplo: 2026-01-15)"
    )

# Validacion de parametros numericos
try:
    cantidad_registros = int(cantidad_registros)
    assert cantidad_registros > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'cantidad_registros' debe ser un entero positivo. Detalle: {e}")

try:
    offset_trxid = int(offset_trxid)
    assert offset_trxid >= 0, "Debe ser mayor o igual a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'offset_trxid' debe ser un entero no negativo. Detalle: {e}")

try:
    num_particiones = int(num_particiones)
    assert num_particiones > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'num_particiones' debe ser un entero positivo. Detalle: {e}")

# Validacion de pesos de distribucion
try:
    pesos_alta = float(pesos_alta)
    pesos_media = float(pesos_media)
    pesos_baja = float(pesos_baja)
    total_pesos = pesos_alta + pesos_media + pesos_baja
    assert total_pesos == 100, f"La suma de pesos debe ser 100, actualmente es {total_pesos}."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Los pesos de distribucion son invalidos. Detalle: {e}")

# Lectura y validacion de rangos de montos por tipo de transaccion
rangos_montos = {}
tipos_rango = [
    "catm", "datm", "cmpr", "tint", "dpst", "pgsl", "text",
    "rtro", "pgsv", "nmna", "intr", "adsl", "impt", "dmcl", "cmsn"
]
for tipo in tipos_rango:
    try:
        rango_min = float(dbutils.widgets.get(f"rango_{tipo}_min"))
        rango_max = float(dbutils.widgets.get(f"rango_{tipo}_max"))
        assert rango_min <= rango_max, f"Minimo ({rango_min}) mayor que maximo ({rango_max})."
        rangos_montos[tipo.upper()] = (rango_min, rango_max)
    except (ValueError, AssertionError) as e:
        raise ValueError(f"ERROR: Rango de montos para '{tipo.upper()}' invalido. Detalle: {e}")

print("=" * 70)
print("PARAMETROS VALIDADOS EXITOSAMENTE")
print("=" * 70)
print(f"  Ruta de salida: {ruta_salida_parquet}")
print(f"  Ruta del Maestro: {ruta_maestro_clientes}")
print(f"  Registros a generar: {cantidad_registros:,}")
print(f"  Fecha de transaccion: {fecha_trx_parsed}")
print(f"  Offset TRXID: {offset_trxid:,}")
print(f"  Particiones: {num_particiones}")
print(f"  Pesos: Alta={pesos_alta}%, Media={pesos_media}%, Baja={pesos_baja}%")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificacion de Existencia del Maestro de Clientes

# COMMAND ----------

# Verificar que el Maestro de Clientes existe antes de continuar
try:
    df_maestro = spark.read.parquet(ruta_maestro_clientes)
    total_clientes = df_maestro.count()
    print(f"Maestro de Clientes cargado exitosamente: {total_clientes:,} clientes.")
except Exception as e:
    raise ValueError(
        f"ERROR: No se puede leer el Maestro de Clientes en '{ruta_maestro_clientes}'. "
        f"Ejecute primero NbGenerarMaestroCliente.py. Detalle: {e}"
    )

# Obtener lista de CUSTIDs para asignacion aleatoria
lista_custids = [fila["CUSTID"] for fila in df_maestro.select("CUSTID").collect()]
print(f"  CUSTIDs cargados: {len(lista_custids):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, DateType, TimestampType
)
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## T012 — Definicion del Esquema StructType Explicito (60 Campos)

# COMMAND ----------

# T012 — Esquema StructType con 60 StructField segun H2.2
# Distribucion: 9 StringType + 19 DateType + 2 TimestampType + 2 LongType + 28 DoubleType = 60 campos
esquema_transaccional = StructType([
    # --- Identificadores (LongType) ---
    StructField("TRXID", LongType(), False),          # 1. Identificador unico de transaccion
    StructField("CUSTID", LongType(), False),          # 2. Identificador del cliente (FK)
    # --- Campos textuales (7 StringType) ---
    StructField("TRXTYP", StringType(), True),         # 3. Codigo del tipo de transaccion
    StructField("TRXDSC", StringType(), True),         # 4. Descripcion del tipo de transaccion
    StructField("CHNLCD", StringType(), True),         # 5. Canal de la transaccion
    StructField("TRXSTS", StringType(), True),         # 6. Estado de la transaccion
    StructField("CRNCOD", StringType(), True),         # 7. Codigo de moneda
    StructField("BRNCOD", StringType(), True),         # 8. Codigo de sucursal
    StructField("ATMID", StringType(), True),          # 9. Identificador del ATM
    # --- Campos fecha (19 DateType + 2 TimestampType = 21 fechas) ---
    StructField("TRXDT", DateType(), True),            # 10. Fecha de la transaccion
    StructField("TRXTM", TimestampType(), True),       # 11. Fecha y hora completa
    StructField("PRCDT", DateType(), True),            # 12. Fecha de procesamiento
    StructField("PRCTM", TimestampType(), True),       # 13. Fecha y hora de procesamiento
    StructField("VLDT", DateType(), True),             # 14. Fecha valor
    StructField("STLDT", DateType(), True),            # 15. Fecha de liquidacion
    StructField("PSTDT", DateType(), True),            # 16. Fecha de contabilizacion
    StructField("CRTDT", DateType(), True),            # 17. Fecha de creacion del registro
    StructField("LSTUDT", DateType(), True),           # 18. Fecha de ultima actualizacion
    StructField("AUTHDT", DateType(), True),           # 19. Fecha de autorizacion
    StructField("CNFRDT", DateType(), True),           # 20. Fecha de confirmacion
    StructField("EXPDT", DateType(), True),            # 21. Fecha de expiracion
    StructField("RVRSDT", DateType(), True),           # 22. Fecha de reverso
    StructField("RCLDT", DateType(), True),            # 23. Fecha de reconciliacion
    StructField("NTFDT", DateType(), True),            # 24. Fecha de notificacion
    StructField("CLRDT", DateType(), True),            # 25. Fecha de compensacion
    StructField("DSPDT", DateType(), True),            # 26. Fecha de disputa
    StructField("RSLTDT", DateType(), True),           # 27. Fecha de resolucion
    StructField("BTCHDT", DateType(), True),           # 28. Fecha de lote de procesamiento
    StructField("EFCDT", DateType(), True),            # 29. Fecha efectiva
    StructField("ARCDT", DateType(), True),            # 30. Fecha de archivado
    # --- Campos numericos (28 DoubleType = 30 numericos total incluyendo 2 LongType) ---
    StructField("TRXAMT", DoubleType(), True),         # 31. Monto de la transaccion
    StructField("ORGAMT", DoubleType(), True),         # 32. Monto original
    StructField("FEEAMT", DoubleType(), True),         # 33. Monto de comision
    StructField("TAXAMT", DoubleType(), True),         # 34. Monto de impuesto
    StructField("NETAMT", DoubleType(), True),         # 35. Monto neto
    StructField("BLNBFR", DoubleType(), True),         # 36. Saldo antes
    StructField("BLNAFT", DoubleType(), True),         # 37. Saldo despues
    StructField("XCHGRT", DoubleType(), True),         # 38. Tasa de cambio
    StructField("CVTAMT", DoubleType(), True),         # 39. Monto convertido
    StructField("INTAMT", DoubleType(), True),         # 40. Monto de intereses
    StructField("DSCAMT", DoubleType(), True),         # 41. Monto de descuento
    StructField("PNLAMT", DoubleType(), True),         # 42. Monto de penalidad
    StructField("REFAMT", DoubleType(), True),         # 43. Monto de referencia
    StructField("LIMAMT", DoubleType(), True),         # 44. Limite permitido
    StructField("AVLAMT", DoubleType(), True),         # 45. Monto disponible
    StructField("HLDAMT", DoubleType(), True),         # 46. Monto retenido
    StructField("OVRAMT", DoubleType(), True),         # 47. Monto de sobregiro
    StructField("MINAMT", DoubleType(), True),         # 48. Monto minimo requerido
    StructField("MAXAMT", DoubleType(), True),         # 49. Monto maximo permitido
    StructField("AVGAMT", DoubleType(), True),         # 50. Monto promedio diario
    StructField("CSHREC", DoubleType(), True),         # 51. Monto recibido en efectivo
    StructField("CSHGVN", DoubleType(), True),         # 52. Monto entregado en efectivo
    StructField("TIPAMT", DoubleType(), True),         # 53. Monto de propina
    StructField("RNDAMT", DoubleType(), True),         # 54. Monto de redondeo
    StructField("SURCHG", DoubleType(), True),         # 55. Recargo adicional
    StructField("INSAMT", DoubleType(), True),         # 56. Monto de seguro
    StructField("ADJAMT", DoubleType(), True),         # 57. Monto de ajuste
    StructField("DLYACM", DoubleType(), True),         # 58. Acumulado diario
    StructField("WKACM", DoubleType(), True),          # 59. Acumulado semanal
    StructField("MTHACM", DoubleType(), True),         # 60. Acumulado mensual
])

print(f"Esquema definido con {len(esquema_transaccional.fields)} campos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T012 — Catalogo de Tipos de Transaccion (15 Codigos con Distribucion Ponderada)

# COMMAND ----------

# Catalogo segun H2.3 de specs/001-research-inicial-v1/research.md
# y distribucion ponderada segun RF-008
catalogo_transacciones = {
    # Alta frecuencia (~60%): CATM, DATM, CMPR, TINT, DPST
    "CATM": {"nombre": "Credito por ATM", "categoria": "ALTA"},
    "DATM": {"nombre": "Debito por ATM", "categoria": "ALTA"},
    "CMPR": {"nombre": "Compra POS", "categoria": "ALTA"},
    "TINT": {"nombre": "Transferencia Interna", "categoria": "ALTA"},
    "DPST": {"nombre": "Deposito en Sucursal", "categoria": "ALTA"},
    # Media frecuencia (~30%): PGSL, TEXT, RTRO, PGSV, NMNA, INTR
    "PGSL": {"nombre": "Pago al Saldo", "categoria": "MEDIA"},
    "TEXT": {"nombre": "Transferencia Externa", "categoria": "MEDIA"},
    "RTRO": {"nombre": "Retiro en Sucursal", "categoria": "MEDIA"},
    "PGSV": {"nombre": "Pago de Servicios", "categoria": "MEDIA"},
    "NMNA": {"nombre": "Deposito de Nomina", "categoria": "MEDIA"},
    "INTR": {"nombre": "Interes Generado", "categoria": "MEDIA"},
    # Baja frecuencia (~10%): ADSL, IMPT, DMCL, CMSN
    "ADSL": {"nombre": "Adelanto Salarial", "categoria": "BAJA"},
    "IMPT": {"nombre": "Impuesto Retenido", "categoria": "BAJA"},
    "DMCL": {"nombre": "Domiciliacion", "categoria": "BAJA"},
    "CMSN": {"nombre": "Comision Bancaria", "categoria": "BAJA"},
}

# Calcular pesos individuales por tipo
tipos_alta = [k for k, v in catalogo_transacciones.items() if v["categoria"] == "ALTA"]
tipos_media = [k for k, v in catalogo_transacciones.items() if v["categoria"] == "MEDIA"]
tipos_baja = [k for k, v in catalogo_transacciones.items() if v["categoria"] == "BAJA"]

peso_por_tipo_alta = pesos_alta / len(tipos_alta)    # ~12% cada uno
peso_por_tipo_media = pesos_media / len(tipos_media)  # ~5% cada uno
peso_por_tipo_baja = pesos_baja / len(tipos_baja)    # ~2.5% cada uno

# Construir lista ordenada de tipos y sus pesos para muestreo
lista_tipos_ordenada = tipos_alta + tipos_media + tipos_baja
lista_pesos_ordenada = (
    [peso_por_tipo_alta] * len(tipos_alta) +
    [peso_por_tipo_media] * len(tipos_media) +
    [peso_por_tipo_baja] * len(tipos_baja)
)

# Normalizar pesos para que sumen 1.0
suma_pesos = sum(lista_pesos_ordenada)
lista_pesos_normalizados = [p / suma_pesos for p in lista_pesos_ordenada]

# Canales de transaccion asociados a cada tipo
canales_por_tipo = {
    "CATM": "ATM", "DATM": "ATM", "CMPR": "POS", "TINT": "WEB",
    "DPST": "SUC", "PGSL": "WEB", "TEXT": "WEB", "RTRO": "SUC",
    "PGSV": "WEB", "NMNA": "ACH", "INTR": "SYS", "ADSL": "WEB",
    "IMPT": "SYS", "DMCL": "ACH", "CMSN": "SYS"
}

print(f"Catalogo de {len(catalogo_transacciones)} tipos de transaccion configurado.")
print(f"  Alta ({len(tipos_alta)} tipos): {peso_por_tipo_alta:.1f}% c/u = {pesos_alta:.0f}%")
print(f"  Media ({len(tipos_media)} tipos): {peso_por_tipo_media:.1f}% c/u = {pesos_media:.0f}%")
print(f"  Baja ({len(tipos_baja)} tipos): {peso_por_tipo_baja:.1f}% c/u = {pesos_baja:.0f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T013 — Generacion de 15M Transacciones

# COMMAND ----------

# Datos capturados por closure para generacion distribuida (compatible con Serverless)
datos_transaccional = {
    "custids": list(lista_custids),
    "tipos": list(lista_tipos_ordenada),
    "pesos": list(lista_pesos_normalizados),
    "rangos": dict(rangos_montos),
    "catalogo": dict(catalogo_transacciones),
    "canales": dict(canales_por_tipo),
    "fecha_trx": fecha_trx_parsed
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funcion de Generacion Distribuida de Transacciones

# COMMAND ----------

def generar_registros_transaccional(iterador_particiones, offset_base, prefijo_fecha):
    """
    Funcion generadora que produce registros transaccionales.
    T013 — Asignacion aleatoria de CUSTID, seleccion ponderada de TRXTYP,
    montos segmentados por tipo.
    T014 — TRXID con prefijo de fecha + secuencial, TRXTM con horas/minutos/segundos.
    """
    custids = datos_transaccional["custids"]
    tipos = datos_transaccional["tipos"]
    pesos = datos_transaccional["pesos"]
    rangos = datos_transaccional["rangos"]
    catalogo = datos_transaccional["catalogo"]
    canales = datos_transaccional["canales"]
    fecha_trx = datos_transaccional["fecha_trx"]

    lista_estados_trx = ["OK", "OK", "OK", "OK", "OK", "OK", "OK", "OK", "RV", "PN"]
    lista_monedas = ["USD", "USD", "USD", "EUR", "GBP", "ILS", "EGP"]
    lista_sucursales = ["SUC001", "SUC002", "SUC003", "SUC004", "SUC005", "SUC006", "SUC007", "SUC008"]
    lista_atm_ids = [f"ATM{str(i).zfill(6)}" for i in range(1, 201)]

    for pdf in iterador_particiones:
        n = len(pdf)
        rng = np.random.default_rng()

        # T014 — Generar TRXID con prefijo de fecha YYYYMMDD + secuencial
        trxids = pdf["id_registro"].values + offset_base
        trxids_con_prefijo = [int(f"{prefijo_fecha}{str(tid).zfill(9)}") for tid in trxids]

        # T013 — Asignacion aleatoria de CUSTID desde el Maestro (RF-009)
        idx_custid = rng.integers(0, len(custids), size=n)
        custid_asignados = [custids[i] for i in idx_custid]

        # T013 — Seleccion ponderada de TRXTYP (RF-008)
        idx_tipo = rng.choice(len(tipos), size=n, p=pesos)
        trxtyp = [tipos[i] for i in idx_tipo]
        trxdsc = [catalogo[t]["nombre"] for t in trxtyp]
        chnlcd = [canales.get(t, "WEB") for t in trxtyp]

        # Generar estados, monedas, sucursales, ATM
        trxsts = rng.choice(lista_estados_trx, size=n).tolist()
        crncod = rng.choice(lista_monedas, size=n).tolist()
        brncod = rng.choice(lista_sucursales, size=n).tolist()
        atmid = [rng.choice(lista_atm_ids) if trxtyp[i] in ("CATM", "DATM") else "" for i in range(n)]

        # Fechas: TRXDT es la fecha parametrizada (igual para todos)
        trxdt = [fecha_trx] * n

        # T014 — TRXTM con horas, minutos y segundos aleatorios
        horas = rng.integers(0, 24, size=n)
        minutos = rng.integers(0, 60, size=n)
        segundos = rng.integers(0, 60, size=n)
        trxtm = [
            datetime.datetime(fecha_trx.year, fecha_trx.month, fecha_trx.day,
                              int(horas[i]), int(minutos[i]), int(segundos[i]))
            for i in range(n)
        ]

        # Fechas de procesamiento y auxiliares (cercanas a TRXDT)
        prcdt = [fecha_trx] * n
        prctm = [
            datetime.datetime(fecha_trx.year, fecha_trx.month, fecha_trx.day,
                              int(rng.integers(0, 24)), int(rng.integers(0, 60)), int(rng.integers(0, 60)))
            for _ in range(n)
        ]
        # Demas fechas: cercanas a TRXDT con pequenas variaciones
        vldt = [fecha_trx + datetime.timedelta(days=int(rng.integers(0, 3))) for _ in range(n)]
        stldt = [fecha_trx + datetime.timedelta(days=int(rng.integers(1, 5))) for _ in range(n)]
        pstdt = [fecha_trx] * n
        crtdt = [fecha_trx] * n
        lstudt = [fecha_trx] * n
        authdt = [fecha_trx] * n
        cnfrdt = [fecha_trx] * n
        expdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(30, 365))) for _ in range(n)]
        rvrsdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(0, 30))) if rng.random() > 0.95 else None for _ in range(n)]
        rcldt = [fecha_trx + datetime.timedelta(days=int(rng.integers(1, 7))) for _ in range(n)]
        ntfdt = [fecha_trx] * n
        clrdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(1, 3))) for _ in range(n)]
        dspdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(1, 90))) if rng.random() > 0.98 else None for _ in range(n)]
        rsltdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(1, 180))) if rng.random() > 0.98 else None for _ in range(n)]
        btchdt = [fecha_trx] * n
        efcdt = [fecha_trx] * n
        arcdt = [fecha_trx + datetime.timedelta(days=int(rng.integers(30, 365))) for _ in range(n)]

        # T013 — Montos con rangos segmentados por tipo de transaccion (RF-017)
        trxamt = np.zeros(n)
        for i in range(n):
            tipo = trxtyp[i]
            rango = rangos[tipo]
            trxamt[i] = round(rng.uniform(rango[0], rango[1]), 2)

        trxamt = trxamt.tolist()
        # Campos monetarios derivados del monto principal
        orgamt = trxamt[:]
        feeamt = [round(t * rng.uniform(0.001, 0.03), 2) for t in trxamt]
        taxamt = [round(t * rng.uniform(0.0, 0.015), 2) for t in trxamt]
        netamt = [round(trxamt[i] - feeamt[i] - taxamt[i], 2) for i in range(n)]
        blnbfr = rng.uniform(1000.0, 500000.0, size=n).round(2).tolist()
        blnaft = [round(blnbfr[i] - trxamt[i] if trxtyp[i] in ("DATM", "RTRO", "CMPR", "PGSV", "DMCL", "CMSN", "IMPT") else blnbfr[i] + trxamt[i], 2) for i in range(n)]
        xchgrt = rng.uniform(0.8, 1.5, size=n).round(6).tolist()
        cvtamt = [round(trxamt[i] * xchgrt[i], 2) for i in range(n)]
        intamt = [round(trxamt[i] * rng.uniform(0.0, 0.05), 2) for i in range(n)]
        dscamt = [round(trxamt[i] * rng.uniform(0.0, 0.1), 2) if rng.random() > 0.8 else 0.0 for i in range(n)]
        pnlamt = [round(rng.uniform(0.0, 50.0), 2) if rng.random() > 0.95 else 0.0 for _ in range(n)]
        refamt = trxamt[:]
        limamt = rng.uniform(5000.0, 500000.0, size=n).round(2).tolist()
        avlamt = [round(limamt[i] - trxamt[i], 2) for i in range(n)]
        hldamt = [round(rng.uniform(0.0, trxamt[i] * 0.1), 2) for i in range(n)]
        ovramt = [round(rng.uniform(0.0, 1000.0), 2) if blnaft[i] < 0 else 0.0 for i in range(n)]
        minamt = rng.uniform(10.0, 100.0, size=n).round(2).tolist()
        maxamt = rng.uniform(50000.0, 500000.0, size=n).round(2).tolist()
        avgamt = [round((blnbfr[i] + blnaft[i]) / 2.0, 2) for i in range(n)]
        cshrec = [round(trxamt[i], 2) if trxtyp[i] in ("CATM", "DPST") else 0.0 for i in range(n)]
        cshgvn = [round(trxamt[i], 2) if trxtyp[i] in ("DATM", "RTRO") else 0.0 for i in range(n)]
        tipamt = rng.uniform(0.0, 5.0, size=n).round(2).tolist()
        rndamt = rng.uniform(-0.5, 0.5, size=n).round(2).tolist()
        surchg = [round(rng.uniform(0.0, 25.0), 2) if rng.random() > 0.7 else 0.0 for _ in range(n)]
        insamt = [round(rng.uniform(0.0, 100.0), 2) if rng.random() > 0.8 else 0.0 for _ in range(n)]
        adjamt = [round(rng.uniform(-50.0, 50.0), 2) if rng.random() > 0.9 else 0.0 for _ in range(n)]
        dlyacm = rng.uniform(0.0, 100000.0, size=n).round(2).tolist()
        wkacm = [round(d * 7, 2) for d in dlyacm]
        mthacm = [round(d * 30, 2) for d in dlyacm]

        # Construir DataFrame de pandas
        resultado = pd.DataFrame({
            "TRXID": trxids_con_prefijo,
            "CUSTID": custid_asignados,
            "TRXTYP": trxtyp, "TRXDSC": trxdsc, "CHNLCD": chnlcd,
            "TRXSTS": trxsts, "CRNCOD": crncod, "BRNCOD": brncod, "ATMID": atmid,
            "TRXDT": trxdt, "TRXTM": trxtm, "PRCDT": prcdt, "PRCTM": prctm,
            "VLDT": vldt, "STLDT": stldt, "PSTDT": pstdt, "CRTDT": crtdt,
            "LSTUDT": lstudt, "AUTHDT": authdt, "CNFRDT": cnfrdt, "EXPDT": expdt,
            "RVRSDT": rvrsdt, "RCLDT": rcldt, "NTFDT": ntfdt, "CLRDT": clrdt,
            "DSPDT": dspdt, "RSLTDT": rsltdt, "BTCHDT": btchdt, "EFCDT": efcdt,
            "ARCDT": arcdt,
            "TRXAMT": trxamt, "ORGAMT": orgamt, "FEEAMT": feeamt, "TAXAMT": taxamt,
            "NETAMT": netamt, "BLNBFR": blnbfr, "BLNAFT": blnaft, "XCHGRT": xchgrt,
            "CVTAMT": cvtamt, "INTAMT": intamt, "DSCAMT": dscamt, "PNLAMT": pnlamt,
            "REFAMT": refamt, "LIMAMT": limamt, "AVLAMT": avlamt, "HLDAMT": hldamt,
            "OVRAMT": ovramt, "MINAMT": minamt, "MAXAMT": maxamt, "AVGAMT": avgamt,
            "CSHREC": cshrec, "CSHGVN": cshgvn, "TIPAMT": tipamt, "RNDAMT": rndamt,
            "SURCHG": surchg, "INSAMT": insamt, "ADJAMT": adjamt,
            "DLYACM": dlyacm, "WKACM": wkacm, "MTHACM": mthacm
        })

        yield resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecutar Generacion de Transacciones

# COMMAND ----------

# T014 — Prefijo de fecha para TRXID (YYYYMMDD)
prefijo_fecha = fecha_trx_parsed.strftime("%Y%m%d")
print(f"Generando {cantidad_registros:,} transacciones con prefijo TRXID: {prefijo_fecha}...")

# Crear DataFrame base con indices secuenciales
df_indices = spark.range(0, cantidad_registros).withColumnRenamed("id", "id_registro")

# Generar registros usando mapInPandas
df_transaccional = df_indices.repartition(num_particiones).mapInPandas(
    lambda iterador: generar_registros_transaccional(iterador, offset_trxid, prefijo_fecha),
    schema=esquema_transaccional
)

print(f"DataFrame generado con esquema de {len(esquema_transaccional.fields)} columnas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T014 — Escritura del Parquet

# COMMAND ----------

# Escritura con reparticion controlada
print(f"Escribiendo parquet en: {ruta_salida_parquet}")
print(f"  Particiones: {num_particiones}")

df_transaccional.repartition(num_particiones).write.mode("overwrite").parquet(ruta_salida_parquet)

# Verificacion post-escritura
df_verificacion = spark.read.parquet(ruta_salida_parquet)
total_escrito = df_verificacion.count()
columnas_escritas = len(df_verificacion.columns)

print("=" * 70)
print("GENERACION COMPLETADA EXITOSAMENTE")
print("=" * 70)
print(f"  Ruta: {ruta_salida_parquet}")
print(f"  Registros escritos: {total_escrito:,}")
print(f"  Columnas: {columnas_escritas}")
print(f"  Particiones: {num_particiones}")
print(f"  Fecha de transaccion: {fecha_trx_parsed}")
print(f"  Prefijo TRXID: {prefijo_fecha}")
print("=" * 70)
