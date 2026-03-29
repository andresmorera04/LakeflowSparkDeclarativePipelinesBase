# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarSaldosCliente — Generador del Parquet de Saldos de Clientes
# MAGIC
# MAGIC **Proposito**: Generar un archivo parquet simulando la tabla BLNCFL (Balance File)
# MAGIC de AS400 para una entidad bancaria.
# MAGIC
# MAGIC **Estructura**: 100 columnas (29 textuales, 36 numericos, 35 fechas) segun H2.4.
# MAGIC
# MAGIC **Volumetria**: Exactamente 1 registro por cada cliente del Maestro (relacion 1:1, RF-011).
# MAGIC
# MAGIC **Ejecucion**: Regeneracion completa en cada ejecucion (no acumulativo).
# MAGIC
# MAGIC **Parametrizacion**: Todos los valores via `dbutils.widgets.text()` (Decision V2-R2-D1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## T016 — Definicion de Parametros (11 Widgets)

# COMMAND ----------

# Parametros de rutas
dbutils.widgets.text("ruta_salida_parquet", "/mnt/external-location/landing/saldos_clientes", "Ruta de Salida del Parquet")
dbutils.widgets.text("ruta_maestro_clientes", "/mnt/external-location/landing/maestro_clientes", "Ruta del Parquet del Maestro")
dbutils.widgets.text("num_particiones", "20", "Numero de Particiones del Parquet")

# Parametros de rangos de montos por tipo de cuenta (RF-017)
dbutils.widgets.text("rango_ahorro_min", "0", "Ahorro Monto Minimo")
dbutils.widgets.text("rango_ahorro_max", "500000", "Ahorro Monto Maximo")
dbutils.widgets.text("rango_corriente_min", "0", "Corriente Monto Minimo")
dbutils.widgets.text("rango_corriente_max", "250000", "Corriente Monto Maximo")
dbutils.widgets.text("rango_credito_min", "0", "Credito Monto Minimo")
dbutils.widgets.text("rango_credito_max", "100000", "Credito Monto Maximo")
dbutils.widgets.text("rango_prestamo_min", "1000", "Prestamo Monto Minimo")
dbutils.widgets.text("rango_prestamo_max", "1000000", "Prestamo Monto Maximo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura y Validacion de Parametros (RF-016)

# COMMAND ----------

# Lectura de parametros principales
ruta_salida_parquet = dbutils.widgets.get("ruta_salida_parquet")
ruta_maestro_clientes = dbutils.widgets.get("ruta_maestro_clientes")
num_particiones = dbutils.widgets.get("num_particiones")

# Validacion de rutas (obligatorias)
if not ruta_salida_parquet or ruta_salida_parquet.strip() == "":
    raise ValueError("ERROR: El parametro 'ruta_salida_parquet' no puede estar vacio.")
if not ruta_maestro_clientes or ruta_maestro_clientes.strip() == "":
    raise ValueError("ERROR: El parametro 'ruta_maestro_clientes' no puede estar vacio.")

# Validacion de num_particiones
try:
    num_particiones = int(num_particiones)
    assert num_particiones > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'num_particiones' debe ser un entero positivo. Detalle: {e}")

# Lectura y validacion de rangos por tipo de cuenta
rangos_cuenta = {}
tipos_cuenta_config = [
    ("ahorro", "AHRO"),
    ("corriente", "CRTE"),
    ("credito", "CRED"),
    ("prestamo", "PRES")
]
for tipo_config, tipo_codigo in tipos_cuenta_config:
    try:
        rango_min = float(dbutils.widgets.get(f"rango_{tipo_config}_min"))
        rango_max = float(dbutils.widgets.get(f"rango_{tipo_config}_max"))
        assert rango_min <= rango_max, f"Minimo ({rango_min}) mayor que maximo ({rango_max})."
        rangos_cuenta[tipo_codigo] = (rango_min, rango_max)
    except (ValueError, AssertionError) as e:
        raise ValueError(f"ERROR: Rango de montos para '{tipo_config}' invalido. Detalle: {e}")

print("=" * 70)
print("PARAMETROS VALIDADOS EXITOSAMENTE")
print("=" * 70)
print(f"  Ruta de salida: {ruta_salida_parquet}")
print(f"  Ruta del Maestro: {ruta_maestro_clientes}")
print(f"  Particiones: {num_particiones}")
for tipo_config, tipo_codigo in tipos_cuenta_config:
    rango = rangos_cuenta[tipo_codigo]
    print(f"  Rango {tipo_config} ({tipo_codigo}): {rango[0]:,.0f} - {rango[1]:,.0f}")
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

# Obtener DataFrame de CUSTIDs para generacion 1:1
df_custids = df_maestro.select("CUSTID")
print(f"  CUSTIDs para generacion 1:1: {total_clientes:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, DateType
)
import numpy as np
import pandas as pd
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## T017 — Definicion del Esquema StructType Explicito (100 Campos)

# COMMAND ----------

# T017 — Esquema StructType con 100 StructField segun H2.4
# Distribucion: 2 LongType + 29 StringType + 34 DoubleType + 35 DateType = 100 campos
esquema_saldos = StructType([
    # --- Identificador principal (LongType) ---
    StructField("CUSTID", LongType(), False),           # 1. Identificador del cliente (FK)
    # --- Campos textuales (29 StringType) ---
    StructField("ACCTID", StringType(), True),           # 2. Identificador de la cuenta
    StructField("ACCTTYP", StringType(), True),          # 3. Tipo de cuenta (AHRO/CRTE/PRES/INVR)
    StructField("ACCTNM", StringType(), True),           # 4. Nombre descriptivo de la cuenta
    StructField("ACCTST", StringType(), True),           # 5. Estado de la cuenta
    StructField("CRNCOD", StringType(), True),           # 6. Codigo de moneda (ISO 4217)
    StructField("BRNCOD", StringType(), True),           # 7. Codigo de sucursal
    StructField("BRNNM", StringType(), True),            # 8. Nombre de la sucursal
    StructField("PRDCOD", StringType(), True),           # 9. Codigo de producto
    StructField("PRDNM", StringType(), True),            # 10. Nombre del producto
    StructField("PRDCAT", StringType(), True),           # 11. Categoria del producto
    StructField("SGMNT", StringType(), True),            # 12. Segmento asignado
    StructField("RISKLV", StringType(), True),           # 13. Nivel de riesgo
    StructField("RGNCD", StringType(), True),            # 14. Codigo de region
    StructField("RGNNM", StringType(), True),            # 15. Nombre de region
    StructField("CSTGRP", StringType(), True),           # 16. Grupo de cliente
    StructField("BLKST", StringType(), True),            # 17. Estado de bloqueo
    StructField("EMBST", StringType(), True),            # 18. Estado de embargo
    StructField("OVDST", StringType(), True),            # 19. Estado de mora
    StructField("DGTST", StringType(), True),            # 20. Estado digital
    StructField("CRDST", StringType(), True),            # 21. Estado crediticio
    StructField("LNTYP", StringType(), True),            # 22. Tipo de linea de credito
    StructField("GRNTYP", StringType(), True),           # 23. Tipo de garantia
    StructField("PYMFRQ", StringType(), True),           # 24. Frecuencia de pago
    StructField("INTTYP", StringType(), True),           # 25. Tipo de tasa de interes (FIJ/VAR/MIX)
    StructField("TXCTG", StringType(), True),            # 26. Categoria fiscal
    StructField("CHKTYP", StringType(), True),           # 27. Tipo de chequera
    StructField("CRDGRP", StringType(), True),           # 28. Grupo crediticio
    StructField("CLSCD", StringType(), True),            # 29. Codigo de clasificacion
    StructField("SRCCD", StringType(), True),            # 30. Codigo fuente
    # --- Campos numericos (34 DoubleType) ---
    StructField("AVLBAL", DoubleType(), True),           # 31. Saldo disponible
    StructField("CURBAL", DoubleType(), True),           # 32. Saldo actual
    StructField("HLDBAL", DoubleType(), True),           # 33. Saldo retenido
    StructField("OVRBAL", DoubleType(), True),           # 34. Saldo en sobregiro
    StructField("PNDBAL", DoubleType(), True),           # 35. Saldo pendiente
    StructField("AVGBAL", DoubleType(), True),           # 36. Saldo promedio del periodo
    StructField("MINBAL", DoubleType(), True),           # 37. Saldo minimo del periodo
    StructField("MAXBAL", DoubleType(), True),           # 38. Saldo maximo del periodo
    StructField("OPNBAL", DoubleType(), True),           # 39. Saldo de apertura del periodo
    StructField("CLSBAL", DoubleType(), True),           # 40. Saldo de cierre del periodo
    StructField("INTACC", DoubleType(), True),           # 41. Intereses acumulados
    StructField("INTPAY", DoubleType(), True),           # 42. Intereses pagados
    StructField("INTRCV", DoubleType(), True),           # 43. Intereses recibidos
    StructField("FEEACC", DoubleType(), True),           # 44. Comisiones acumuladas
    StructField("FEEPAY", DoubleType(), True),           # 45. Comisiones pagadas
    StructField("CRDLMT", DoubleType(), True),           # 46. Limite de credito
    StructField("CRDAVL", DoubleType(), True),           # 47. Credito disponible
    StructField("CRDUSD", DoubleType(), True),           # 48. Credito utilizado
    StructField("PYMAMT", DoubleType(), True),           # 49. Monto de pago minimo
    StructField("PYMLST", DoubleType(), True),           # 50. Ultimo pago realizado
    StructField("TTLDBT", DoubleType(), True),           # 51. Total debitos del periodo
    StructField("TTLCRD", DoubleType(), True),           # 52. Total creditos del periodo
    # --- TTLTRX: NUMERIC(9,0) → LongType (entero puro sin decimales) ---
    StructField("TTLTRX", LongType(), True),             # 53. Total de transacciones del periodo
    StructField("LNAMT", DoubleType(), True),            # 54. Monto del prestamo
    StructField("LNBAL", DoubleType(), True),            # 55. Saldo del prestamo
    StructField("MTHPYM", DoubleType(), True),           # 56. Pago mensual
    StructField("INTRT", DoubleType(), True),            # 57. Tasa de interes vigente
    StructField("PNLRT", DoubleType(), True),            # 58. Tasa de penalidad
    StructField("OVRRT", DoubleType(), True),            # 59. Tasa de sobregiro
    StructField("TAXAMT", DoubleType(), True),           # 60. Impuestos acumulados
    StructField("INSAMT", DoubleType(), True),           # 61. Seguros acumulados
    StructField("DLYINT", DoubleType(), True),           # 62. Interes diario
    StructField("YLDRT", DoubleType(), True),            # 63. Tasa de rendimiento
    StructField("SPRDRT", DoubleType(), True),           # 64. Spread de la tasa
    StructField("MRGAMT", DoubleType(), True),           # 65. Monto de margen
    # --- Campos fecha (35 DateType) ---
    StructField("OPNDT", DateType(), True),              # 66. Fecha de apertura de cuenta
    StructField("CLSDT", DateType(), True),              # 67. Fecha de cierre (si aplica)
    StructField("LSTTRX", DateType(), True),             # 68. Fecha de ultima transaccion
    StructField("LSTPYM", DateType(), True),             # 69. Fecha de ultimo pago
    StructField("NXTPYM", DateType(), True),             # 70. Fecha de proximo pago
    StructField("MATDT", DateType(), True),              # 71. Fecha de vencimiento
    StructField("RNWDT", DateType(), True),              # 72. Fecha de renovacion
    StructField("RVWDT", DateType(), True),              # 73. Fecha de revision
    StructField("CRTDT", DateType(), True),              # 74. Fecha de creacion del registro
    StructField("UPDDT", DateType(), True),              # 75. Fecha de ultima actualizacion
    StructField("STMDT", DateType(), True),              # 76. Fecha de estado de cuenta
    StructField("CUTDT", DateType(), True),              # 77. Fecha de corte
    StructField("GRPDT", DateType(), True),              # 78. Fecha de periodo de gracia
    StructField("INTDT", DateType(), True),              # 79. Fecha de calculo de intereses
    StructField("FEEDT", DateType(), True),              # 80. Fecha de cobro de comisiones
    StructField("BLKDT", DateType(), True),              # 81. Fecha de bloqueo (si aplica)
    StructField("EMBDT", DateType(), True),              # 82. Fecha de embargo (si aplica)
    StructField("OVDDT", DateType(), True),              # 83. Fecha de inicio de mora
    StructField("PYMDT1", DateType(), True),             # 84. Fecha de primer pago
    StructField("PYMDT2", DateType(), True),             # 85. Fecha de segundo pago
    StructField("PRJDT", DateType(), True),              # 86. Fecha de proyeccion
    StructField("ADJDT", DateType(), True),              # 87. Fecha de ajuste
    StructField("RCLDT", DateType(), True),              # 88. Fecha de reconciliacion
    StructField("NTFDT", DateType(), True),              # 89. Fecha de notificacion
    StructField("CNCLDT", DateType(), True),             # 90. Fecha de cancelacion
    StructField("RCTDT", DateType(), True),              # 91. Fecha de reactivacion
    StructField("CHGDT", DateType(), True),              # 92. Fecha de cambio de condiciones
    StructField("VRFDT", DateType(), True),              # 93. Fecha de verificacion
    StructField("PRMDT", DateType(), True),              # 94. Fecha de promocion
    StructField("DGTDT", DateType(), True),              # 95. Fecha de ultimo acceso digital
    StructField("AUDT", DateType(), True),               # 96. Fecha de auditoria
    StructField("MGRDT", DateType(), True),              # 97. Fecha de migracion
    StructField("ESCDT", DateType(), True),              # 98. Fecha de escalamiento
    StructField("RPTDT", DateType(), True),              # 99. Fecha de reporte regulatorio
    StructField("ARCDT", DateType(), True),              # 100. Fecha de archivado
])

print(f"Esquema definido con {len(esquema_saldos.fields)} campos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos de Referencia para Generacion

# COMMAND ----------

# Tipos de cuenta y configuraciones asociadas
tipos_cuenta = ["AHRO", "CRTE", "PRES", "INVR"]
pesos_tipos_cuenta = [0.40, 0.30, 0.20, 0.10]

# Nombres descriptivos por tipo de cuenta
nombres_cuenta = {
    "AHRO": "Cuenta de Ahorro",
    "CRTE": "Cuenta Corriente",
    "PRES": "Prestamo Personal",
    "INVR": "Cuenta de Inversion"
}

# Estados de cuenta
estados_cuenta = ["AC", "IN", "BL", "CN"]
pesos_estados = [0.85, 0.08, 0.05, 0.02]

# Monedas
monedas = ["USD", "USD", "USD", "EUR", "GBP", "ILS", "EGP"]

# Sucursales
sucursales = [f"SUC{str(i).zfill(3)}" for i in range(1, 51)]
nombres_sucursales = [f"Sucursal {str(i).zfill(3)}" for i in range(1, 51)]

# Productos por tipo de cuenta
productos_por_tipo = {
    "AHRO": [("AH001", "Ahorro Basico", "AHRB"), ("AH002", "Ahorro Premium", "AHRP"), ("AH003", "Ahorro Infantil", "AHRI")],
    "CRTE": [("CT001", "Corriente Basica", "CRTB"), ("CT002", "Corriente Empresarial", "CRTE"), ("CT003", "Corriente Digital", "CRTD")],
    "PRES": [("PR001", "Prestamo Personal", "PRSP"), ("PR002", "Prestamo Hipotecario", "PRSH"), ("PR003", "Prestamo Vehicular", "PRSV")],
    "INVR": [("IN001", "Inversion Plazo Fijo", "INVF"), ("IN002", "Inversion Fondos", "INVD"), ("IN003", "Inversion Mixta", "INVM")]
}

# Segmentos, niveles de riesgo, regiones
segmentos = ["PERS", "PYME", "CORP", "PRIV"]
niveles_riesgo = ["01", "02", "03", "04", "05"]
regiones = [
    ("R001", "Norte"), ("R002", "Sur"), ("R003", "Este"), ("R004", "Oeste"),
    ("R005", "Centro"), ("R006", "Noroeste"), ("R007", "Sureste"), ("R008", "Capital")
]
grupos_cliente = ["GRP1", "GRP2", "GRP3", "GRP4"]

# Tipos de linea/garantia/frecuencia/tasa
tipos_linea = ["RVLV", "FIJA", "ADMN", "NONE"]
tipos_garantia = ["REAL", "PRND", "FIDC", "NONE"]
frecuencias_pago = ["MEN", "QUI", "SEM", "ANU"]
tipos_interes = ["FIJ", "VAR", "MIX"]
categorias_fiscal = ["A01", "B01", "C01"]
tipos_chequera = ["STD", "EMP", "NON"]
grupos_crediticio = ["GC01", "GC02", "GC03", "GC04"]
codigos_clasificacion = ["CL01", "CL02", "CL03", "CL04"]
codigos_fuente = ["SRC1", "SRC2", "SRC3", "SRC4"]

print("Datos de referencia cargados para generacion de saldos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T018 — Generacion de Saldos 1:1 con mapInPandas

# COMMAND ----------

# Broadcast de datos necesarios para la generacion distribuida
bc_rangos_cuenta = spark.sparkContext.broadcast(rangos_cuenta)
bc_tipos_cuenta = spark.sparkContext.broadcast(tipos_cuenta)
bc_pesos_tipos = spark.sparkContext.broadcast(pesos_tipos_cuenta)
bc_nombres_cuenta = spark.sparkContext.broadcast(nombres_cuenta)
bc_estados_cuenta = spark.sparkContext.broadcast(estados_cuenta)
bc_pesos_estados = spark.sparkContext.broadcast(pesos_estados)
bc_monedas = spark.sparkContext.broadcast(monedas)
bc_sucursales = spark.sparkContext.broadcast(sucursales)
bc_nombres_sucursales = spark.sparkContext.broadcast(nombres_sucursales)
bc_productos = spark.sparkContext.broadcast(productos_por_tipo)
bc_segmentos = spark.sparkContext.broadcast(segmentos)
bc_niveles_riesgo = spark.sparkContext.broadcast(niveles_riesgo)
bc_regiones = spark.sparkContext.broadcast(regiones)
bc_grupos_cliente = spark.sparkContext.broadcast(grupos_cliente)
bc_tipos_linea = spark.sparkContext.broadcast(tipos_linea)
bc_tipos_garantia = spark.sparkContext.broadcast(tipos_garantia)
bc_frecuencias_pago = spark.sparkContext.broadcast(frecuencias_pago)
bc_tipos_interes = spark.sparkContext.broadcast(tipos_interes)
bc_categorias_fiscal = spark.sparkContext.broadcast(categorias_fiscal)
bc_tipos_chequera = spark.sparkContext.broadcast(tipos_chequera)
bc_grupos_crediticio = spark.sparkContext.broadcast(grupos_crediticio)
bc_codigos_clasificacion = spark.sparkContext.broadcast(codigos_clasificacion)
bc_codigos_fuente = spark.sparkContext.broadcast(codigos_fuente)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funcion de Generacion Distribuida de Saldos

# COMMAND ----------

def generar_registros_saldos(iterador_particiones):
    """
    Funcion generadora que produce registros de saldos 1:1 con los CUSTIDs recibidos.
    T018 — Cada CUSTID del Maestro genera exactamente un registro de saldo.
    """
    rangos = bc_rangos_cuenta.value
    tipos_cta = bc_tipos_cuenta.value
    pesos_cta = bc_pesos_tipos.value
    nombres_cta = bc_nombres_cuenta.value
    estados = bc_estados_cuenta.value
    pesos_est = bc_pesos_estados.value
    monedas_l = bc_monedas.value
    sucursales_l = bc_sucursales.value
    nombres_suc_l = bc_nombres_sucursales.value
    productos_l = bc_productos.value
    segmentos_l = bc_segmentos.value
    niveles_riesgo_l = bc_niveles_riesgo.value
    regiones_l = bc_regiones.value
    grupos_l = bc_grupos_cliente.value
    tipos_linea_l = bc_tipos_linea.value
    tipos_garantia_l = bc_tipos_garantia.value
    frecuencias_l = bc_frecuencias_pago.value
    tipos_int_l = bc_tipos_interes.value
    cat_fiscal_l = bc_categorias_fiscal.value
    tipos_chq_l = bc_tipos_chequera.value
    grupos_crd_l = bc_grupos_crediticio.value
    codigos_cls_l = bc_codigos_clasificacion.value
    codigos_src_l = bc_codigos_fuente.value

    # Rango de cuenta CRED se combina con el tipo de cuenta INVR para las cuentas que no
    # tienen un rango propio: INVR usa rango_credito
    rangos_por_tipo = {
        "AHRO": rangos.get("AHRO", (0, 500000)),
        "CRTE": rangos.get("CRTE", (0, 250000)),
        "PRES": rangos.get("PRES", (1000, 1000000)),
        "INVR": rangos.get("CRED", (0, 100000))
    }

    # Fechas de referencia
    hoy = datetime.date.today()
    fecha_base = datetime.date(1995, 1, 1)
    dias_rango_apertura = (hoy - fecha_base).days

    for pdf in iterador_particiones:
        n = len(pdf)
        rng = np.random.default_rng()

        # CUSTID viene del DataFrame de entrada
        custids = pdf["CUSTID"].values.tolist()

        # Asignar tipo de cuenta con pesos
        idx_tipo = rng.choice(len(tipos_cta), size=n, p=pesos_cta)
        accttyp = [tipos_cta[i] for i in idx_tipo]

        # Generar ACCTID basado en tipo + secuencial
        acctid = [f"{accttyp[i]}{str(custids[i]).zfill(15)}" for i in range(n)]
        acctnm = [nombres_cta[accttyp[i]] for i in range(n)]

        # Estados de cuenta
        idx_estado = rng.choice(len(estados), size=n, p=pesos_est)
        acctst = [estados[i] for i in idx_estado]

        # Moneda, sucursal
        crncod = rng.choice(monedas_l, size=n).tolist()
        idx_suc = rng.integers(0, len(sucursales_l), size=n)
        brncod = [sucursales_l[i] for i in idx_suc]
        brnnm = [nombres_suc_l[i] for i in idx_suc]

        # Producto segun tipo de cuenta
        prdcod = []
        prdnm = []
        prdcat = []
        for i in range(n):
            prods = productos_l.get(accttyp[i], productos_l["AHRO"])
            prod = prods[rng.integers(0, len(prods))]
            prdcod.append(prod[0])
            prdnm.append(prod[1])
            prdcat.append(prod[2])

        # Segmento, riesgo, region, grupo
        sgmnt = rng.choice(segmentos_l, size=n).tolist()
        risklv = rng.choice(niveles_riesgo_l, size=n).tolist()
        idx_reg = rng.integers(0, len(regiones_l), size=n)
        rgncd = [regiones_l[i][0] for i in idx_reg]
        rgnnm = [regiones_l[i][1] for i in idx_reg]
        cstgrp = rng.choice(grupos_l, size=n).tolist()

        # Estados binarios (SI/NO como 2 caracteres)
        blkst = rng.choice(["NO", "SI"], size=n, p=[0.97, 0.03]).tolist()
        embst = rng.choice(["NO", "SI"], size=n, p=[0.99, 0.01]).tolist()
        ovdst = rng.choice(["NO", "SI"], size=n, p=[0.92, 0.08]).tolist()
        dgtst = rng.choice(["AC", "IN"], size=n, p=[0.75, 0.25]).tolist()
        crdst = rng.choice(["OK", "MR", "DL"], size=n, p=[0.85, 0.10, 0.05]).tolist()

        # Tipos y configuraciones
        lntyp = rng.choice(tipos_linea_l, size=n).tolist()
        grntyp = rng.choice(tipos_garantia_l, size=n).tolist()
        pymfrq = rng.choice(frecuencias_l, size=n).tolist()
        inttyp = rng.choice(tipos_int_l, size=n).tolist()
        txctg = rng.choice(cat_fiscal_l, size=n).tolist()
        chktyp = rng.choice(tipos_chq_l, size=n).tolist()
        crdgrp = rng.choice(grupos_crd_l, size=n).tolist()
        clscd = rng.choice(codigos_cls_l, size=n).tolist()
        srccd = rng.choice(codigos_src_l, size=n).tolist()

        # --- Campos numericos (34 DoubleType + 1 LongType) ---
        # Saldos principales basados en tipo de cuenta y rangos parametrizables (RF-017)
        avlbal = np.zeros(n)
        for i in range(n):
            rango = rangos_por_tipo.get(accttyp[i], (0, 500000))
            avlbal[i] = round(rng.uniform(rango[0], rango[1]), 2)
        avlbal = avlbal.tolist()

        curbal = [round(avlbal[i] * rng.uniform(0.95, 1.05), 2) for i in range(n)]
        hldbal = [round(avlbal[i] * rng.uniform(0.0, 0.1), 2) for i in range(n)]
        ovrbal = [round(rng.uniform(0.0, 5000.0), 2) if rng.random() > 0.92 else 0.0 for _ in range(n)]
        pndbal = [round(avlbal[i] * rng.uniform(0.0, 0.05), 2) for i in range(n)]
        avgbal = [round((avlbal[i] + curbal[i]) / 2.0, 2) for i in range(n)]
        minbal = [round(avlbal[i] * rng.uniform(0.3, 0.8), 2) for i in range(n)]
        maxbal = [round(avlbal[i] * rng.uniform(1.1, 1.5), 2) for i in range(n)]
        opnbal = [round(avlbal[i] * rng.uniform(0.5, 1.2), 2) for i in range(n)]
        clsbal = curbal[:]

        # Intereses y comisiones
        intacc = [round(avlbal[i] * rng.uniform(0.001, 0.05), 2) for i in range(n)]
        intpay = [round(intacc[i] * rng.uniform(0.5, 1.0), 2) for i in range(n)]
        intrcv = [round(avlbal[i] * rng.uniform(0.0, 0.03), 2) for i in range(n)]
        feeacc = [round(rng.uniform(0.0, 500.0), 2) for _ in range(n)]
        feepay = [round(feeacc[i] * rng.uniform(0.5, 1.0), 2) for i in range(n)]

        # Credito
        crdlmt_vals = [round(rng.uniform(5000.0, 200000.0), 2) if accttyp[i] in ("CRTE", "INVR") else 0.0 for i in range(n)]
        crdavl = [round(crdlmt_vals[i] * rng.uniform(0.2, 0.9), 2) for i in range(n)]
        crdusd = [round(crdlmt_vals[i] - crdavl[i], 2) for i in range(n)]

        # Pagos
        pymamt = [round(rng.uniform(50.0, 5000.0), 2) for _ in range(n)]
        pymlst = [round(rng.uniform(100.0, 10000.0), 2) for _ in range(n)]

        # Totales del periodo
        ttldbt = [round(rng.uniform(1000.0, 500000.0), 2) for _ in range(n)]
        ttlcrd = [round(rng.uniform(1000.0, 500000.0), 2) for _ in range(n)]
        ttltrx = rng.integers(1, 500, size=n).tolist()

        # Prestamos
        lnamt = [round(rng.uniform(rangos_por_tipo["PRES"][0], rangos_por_tipo["PRES"][1]), 2) if accttyp[i] == "PRES" else 0.0 for i in range(n)]
        lnbal = [round(lnamt[i] * rng.uniform(0.1, 0.95), 2) for i in range(n)]
        mthpym = [round(lnamt[i] / max(rng.integers(12, 360), 1), 2) for i in range(n)]

        # Tasas
        intrt = rng.uniform(0.01, 0.25, size=n).round(4).tolist()
        pnlrt = rng.uniform(0.0, 0.05, size=n).round(4).tolist()
        ovrrt = rng.uniform(0.0, 0.10, size=n).round(4).tolist()

        # Impuestos, seguros, intereses diarios y spreads
        taxamt = [round(rng.uniform(0.0, 5000.0), 2) for _ in range(n)]
        insamt = [round(rng.uniform(0.0, 3000.0), 2) for _ in range(n)]
        dlyint = [round(intrt[i] / 365.0 * avlbal[i], 4) for i in range(n)]
        yldrt = rng.uniform(0.0, 0.12, size=n).round(4).tolist()
        sprdrt = rng.uniform(0.0, 0.05, size=n).round(4).tolist()
        mrgamt = [round(rng.uniform(0.0, 50000.0), 2) for _ in range(n)]

        # --- Campos fecha (35 DateType) ---
        # Fecha de apertura (ultimos 30 anos)
        opndt = [(fecha_base + datetime.timedelta(days=int(rng.integers(0, dias_rango_apertura)))) for _ in range(n)]
        clsdt = [(opndt[i] + datetime.timedelta(days=int(rng.integers(365, 3650)))) if acctst[i] == "CN" else None for i in range(n)]
        lsttrx = [(hoy - datetime.timedelta(days=int(rng.integers(0, 90)))) for _ in range(n)]
        lstpym = [(hoy - datetime.timedelta(days=int(rng.integers(0, 60)))) for _ in range(n)]
        nxtpym = [(hoy + datetime.timedelta(days=int(rng.integers(1, 45)))) for _ in range(n)]
        matdt = [(opndt[i] + datetime.timedelta(days=int(rng.integers(365, 7300)))) for i in range(n)]
        rnwdt = [(hoy + datetime.timedelta(days=int(rng.integers(30, 365)))) for _ in range(n)]
        rvwdt = [(hoy + datetime.timedelta(days=int(rng.integers(30, 180)))) for _ in range(n)]
        crtdt = opndt[:]
        upddt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        stmdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        cutdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        grpdt = [(hoy + datetime.timedelta(days=int(rng.integers(0, 15)))) for _ in range(n)]
        intdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        feedt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        blkdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) if blkst[i] == "SI" else None for i in range(n)]
        embdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) if embst[i] == "SI" else None for i in range(n)]
        ovddt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 180)))) if ovdst[i] == "SI" else None for i in range(n)]
        pymdt1 = [(opndt[i] + datetime.timedelta(days=int(rng.integers(30, 60)))) for i in range(n)]
        pymdt2 = [(opndt[i] + datetime.timedelta(days=int(rng.integers(60, 90)))) for i in range(n)]
        prjdt = [(hoy + datetime.timedelta(days=int(rng.integers(30, 365)))) for _ in range(n)]
        adjdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 90)))) if rng.random() > 0.8 else None for _ in range(n)]
        rcldt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        ntfdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) for _ in range(n)]
        cncldt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) if acctst[i] == "CN" else None for i in range(n)]
        rctdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) if rng.random() > 0.95 else None for _ in range(n)]
        chgdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 180)))) if rng.random() > 0.7 else None for _ in range(n)]
        vrfdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) for _ in range(n)]
        prmdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) if rng.random() > 0.8 else None for _ in range(n)]
        dgtdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 30)))) if dgtst[i] == "AC" else None for i in range(n)]
        audt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 365)))) for _ in range(n)]
        mgrdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 1825)))) if rng.random() > 0.7 else None for _ in range(n)]
        escdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 180)))) if rng.random() > 0.9 else None for _ in range(n)]
        rptdt = [(hoy - datetime.timedelta(days=int(rng.integers(0, 90)))) for _ in range(n)]
        arcdt = [(hoy + datetime.timedelta(days=int(rng.integers(365, 1825)))) for _ in range(n)]

        # Construir DataFrame de pandas
        resultado = pd.DataFrame({
            "CUSTID": custids,
            # 29 StringType
            "ACCTID": acctid, "ACCTTYP": accttyp, "ACCTNM": acctnm, "ACCTST": acctst,
            "CRNCOD": crncod, "BRNCOD": brncod, "BRNNM": brnnm,
            "PRDCOD": prdcod, "PRDNM": prdnm, "PRDCAT": prdcat,
            "SGMNT": sgmnt, "RISKLV": risklv, "RGNCD": rgncd, "RGNNM": rgnnm,
            "CSTGRP": cstgrp, "BLKST": blkst, "EMBST": embst, "OVDST": ovdst,
            "DGTST": dgtst, "CRDST": crdst, "LNTYP": lntyp, "GRNTYP": grntyp,
            "PYMFRQ": pymfrq, "INTTYP": inttyp, "TXCTG": txctg, "CHKTYP": chktyp,
            "CRDGRP": crdgrp, "CLSCD": clscd, "SRCCD": srccd,
            # 34 DoubleType + 1 LongType
            "AVLBAL": avlbal, "CURBAL": curbal, "HLDBAL": hldbal, "OVRBAL": ovrbal,
            "PNDBAL": pndbal, "AVGBAL": avgbal, "MINBAL": minbal, "MAXBAL": maxbal,
            "OPNBAL": opnbal, "CLSBAL": clsbal, "INTACC": intacc, "INTPAY": intpay,
            "INTRCV": intrcv, "FEEACC": feeacc, "FEEPAY": feepay, "CRDLMT": crdlmt_vals,
            "CRDAVL": crdavl, "CRDUSD": crdusd, "PYMAMT": pymamt, "PYMLST": pymlst,
            "TTLDBT": ttldbt, "TTLCRD": ttlcrd, "TTLTRX": ttltrx, "LNAMT": lnamt,
            "LNBAL": lnbal, "MTHPYM": mthpym, "INTRT": intrt, "PNLRT": pnlrt,
            "OVRRT": ovrrt, "TAXAMT": taxamt, "INSAMT": insamt, "DLYINT": dlyint,
            "YLDRT": yldrt, "SPRDRT": sprdrt, "MRGAMT": mrgamt,
            # 35 DateType
            "OPNDT": opndt, "CLSDT": clsdt, "LSTTRX": lsttrx, "LSTPYM": lstpym,
            "NXTPYM": nxtpym, "MATDT": matdt, "RNWDT": rnwdt, "RVWDT": rvwdt,
            "CRTDT": crtdt, "UPDDT": upddt, "STMDT": stmdt, "CUTDT": cutdt,
            "GRPDT": grpdt, "INTDT": intdt, "FEEDT": feedt, "BLKDT": blkdt,
            "EMBDT": embdt, "OVDDT": ovddt, "PYMDT1": pymdt1, "PYMDT2": pymdt2,
            "PRJDT": prjdt, "ADJDT": adjdt, "RCLDT": rcldt, "NTFDT": ntfdt,
            "CNCLDT": cncldt, "RCTDT": rctdt, "CHGDT": chgdt, "VRFDT": vrfdt,
            "PRMDT": prmdt, "DGTDT": dgtdt, "AUDT": audt, "MGRDT": mgrdt,
            "ESCDT": escdt, "RPTDT": rptdt, "ARCDT": arcdt
        })

        yield resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecutar Generacion de Saldos 1:1

# COMMAND ----------

print(f"Generando saldos 1:1 para {total_clientes:,} clientes...")

# Generar registros usando mapInPandas directamente sobre los CUSTIDs del Maestro
df_saldos = df_custids.repartition(num_particiones).mapInPandas(
    generar_registros_saldos,
    schema=esquema_saldos
)

print(f"DataFrame generado con esquema de {len(esquema_saldos.fields)} columnas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura del Parquet

# COMMAND ----------

# Escritura con reparticion controlada
print(f"Escribiendo parquet en: {ruta_salida_parquet}")
print(f"  Particiones: {num_particiones}")

df_saldos.repartition(num_particiones).write.mode("overwrite").parquet(ruta_salida_parquet)

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
print(f"  Relacion 1:1 con Maestro: {total_escrito:,} saldos = {total_clientes:,} clientes")
print("=" * 70)
