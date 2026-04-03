# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # LsdpPlataTransacciones — Vista Materializada: Transacciones Enriquecidas
# MAGIC
# MAGIC **Proposito**: Crear la vista materializada `transacciones_enriquecidas` en la medalla
# MAGIC de plata, enriqueciendo trxpfl con 4 campos calculados numericos (65 columnas total:
# MAGIC 61 trxpfl + 4 calculados), lectura batch sin filtros de la tabla de bronce,
# MAGIC manejo seguro de nulos con coalesce a 0, y Liquid Cluster.
# MAGIC
# MAGIC **Patron — Closure**:
# MAGIC Los parametros de Azure SQL y el esquema del pipeline se calculan UNA SOLA VEZ
# MAGIC a nivel de modulo (al inicializar el pipeline). Los valores quedan capturados
# MAGIC como variables de modulo en el closure de la funcion decorada con @dp.materialized_view.
# MAGIC Este patron reemplaza spark.sparkContext.broadcast() que no es compatible
# MAGIC con Computo Serverless (RF-011).
# MAGIC
# MAGIC **Resultado esperado**:
# MAGIC - Vista: `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas`
# MAGIC - Columnas: 65 (61 trxpfl + 4 calculados)
# MAGIC - Lectura sin filtros de bronce_dev.regional.trxpfl (RF-006)
# MAGIC - Liquid Cluster: [fecha_transaccion, identificador_cliente, tipo_transaccion]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importacion de Utilidades LSDP

# COMMAND ----------

from utilities.LsdpConexionAzureSql import leer_parametros_azure_sql
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones de la API LSDP y PySpark

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, lit, abs as spark_abs, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura de Parametros del Pipeline (Nivel de Modulo)
# MAGIC
# MAGIC Este bloque se ejecuta UNA SOLA VEZ al inicializar el pipeline.
# MAGIC Lee el nombre del Scope Secret configurado como parametro del pipeline y usa
# MAGIC la funcion `leer_parametros_azure_sql` para obtener TODAS las claves de dbo.Parametros
# MAGIC incluyendo `catalogoPlata` (RF-019). El esquema de plata se obtiene del parametro
# MAGIC del pipeline `esquema_plata`.

# COMMAND ----------

# Leer el nombre del Scope Secret desde los parametros del pipeline LSDP
nombre_scope_secret = spark.conf.get("pipelines.parameters.nombreScopeSecret")

# Invocar la funcion de conexion Azure SQL — retorna diccionario COMPLETO (RF-019)
# Incluye: catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, ...
parametros_sql = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)

# Obtener el catalogo de plata desde dbo.Parametros (no es parametro del pipeline — RF-019)
catalogo_plata = parametros_sql["catalogoPlata"]

# Obtener el esquema de plata desde los parametros del pipeline LSDP
# Parametro del pipeline: esquema_plata (ej: "regional")
esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicion de la Vista Materializada: transacciones_enriquecidas
# MAGIC
# MAGIC Decorador aplicado:
# MAGIC - `@dp.materialized_view`: nombre dinamico, comment, 5 propiedades Delta, Liquid Cluster
# MAGIC
# MAGIC Estrategia de lectura (RF-006):
# MAGIC - Lectura batch de bronce_dev.regional.trxpfl SIN filtros
# MAGIC - LSDP gestiona la actualizacion incremental automaticamente
# MAGIC
# MAGIC Campos calculados (RF-007, TABLA E):
# MAGIC - monto_neto_comisiones: NETAMT - (FEEAMT + TAXAMT)
# MAGIC - porcentaje_comision_sobre_monto: (FEEAMT / ORGAMT) * 100 con manejo division por cero
# MAGIC - variacion_saldo_transaccion: abs(BLNAFT - BLNBFR)
# MAGIC - indicador_impacto_financiero: TRXAMT + FEEAMT + TAXAMT + PNLAMT

# COMMAND ----------

@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas",
    comment="Vista materializada de plata V4 — Transacciones enriquecidas de trxpfl. "
            "Lectura batch sin filtros con 4 campos calculados numericos: monto neto de comisiones, "
            "porcentaje de comision, variacion de saldo e indicador de impacto financiero. "
            "Nulos tratados con coalesce a 0 en todos los campos numericos calculados.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days"
    },
    cluster_by=["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]
)
def transacciones_enriquecidas():

    # -------------------------------------------------------------------------
    # Paso 1: Leer tabla bronce trxpfl con lectura batch SIN filtros (RF-006)
    # LSDP gestiona la incrementalidad automaticamente — no se aplican filtros.
    # -------------------------------------------------------------------------
    catalogo_bronce = parametros_sql["catalogoBronce"]
    df_trxpfl = spark.read.table(f"{catalogo_bronce}.regional.trxpfl")

    # -------------------------------------------------------------------------
    # Paso 2: Excluir columnas no deseadas
    # - _rescued_data: columna interna de AutoLoader
    # - año, mes, dia: columnas de particion del lazy evaluation de bronce
    # -------------------------------------------------------------------------
    columnas_excluir = {"_rescued_data", "año", "mes", "dia"}
    columnas_limpias = [c for c in df_trxpfl.columns if c not in columnas_excluir]
    df_trxpfl = df_trxpfl.select(columnas_limpias)

    # -------------------------------------------------------------------------
    # Paso 3: Renombrar 61 columnas de trxpfl a espanol (TABLA D — data-model.md)
    # -------------------------------------------------------------------------
    df_renombrado = df_trxpfl.select(
        col("TRXID").alias("identificador_transaccion"),
        col("CUSTID").alias("identificador_cliente"),
        col("TRXTYP").alias("tipo_transaccion"),
        col("TRXDSC").alias("descripcion_transaccion"),
        col("CHNLCD").alias("canal_transaccion"),
        col("TRXSTS").alias("estado_transaccion"),
        col("CRNCOD").alias("codigo_moneda"),
        col("BRNCOD").alias("codigo_sucursal"),
        col("ATMID").alias("identificador_atm"),
        col("TRXDT").alias("fecha_transaccion"),
        col("TRXTM").alias("fecha_hora_transaccion"),
        col("PRCDT").alias("fecha_procesamiento"),
        col("PRCTM").alias("fecha_hora_procesamiento"),
        col("VLDT").alias("fecha_valor"),
        col("STLDT").alias("fecha_liquidacion"),
        col("PSTDT").alias("fecha_contabilizacion"),
        col("CRTDT").alias("fecha_creacion_registro"),
        col("LSTUDT").alias("fecha_ultima_actualizacion"),
        col("AUTHDT").alias("fecha_autorizacion"),
        col("CNFRDT").alias("fecha_confirmacion"),
        col("EXPDT").alias("fecha_expiracion"),
        col("RVRSDT").alias("fecha_reverso"),
        col("RCLDT").alias("fecha_reconciliacion"),
        col("NTFDT").alias("fecha_notificacion"),
        col("CLRDT").alias("fecha_compensacion"),
        col("DSPDT").alias("fecha_disputa"),
        col("RSLTDT").alias("fecha_resolucion"),
        col("BTCHDT").alias("fecha_lote_procesamiento"),
        col("EFCDT").alias("fecha_efectiva"),
        col("ARCDT").alias("fecha_archivado"),
        col("TRXAMT").alias("monto_transaccion"),
        col("ORGAMT").alias("monto_original"),
        col("FEEAMT").alias("monto_comision"),
        col("TAXAMT").alias("monto_impuesto"),
        col("NETAMT").alias("monto_neto"),
        col("BLNBFR").alias("saldo_antes_transaccion"),
        col("BLNAFT").alias("saldo_despues_transaccion"),
        col("XCHGRT").alias("tasa_cambio"),
        col("CVTAMT").alias("monto_convertido"),
        col("INTAMT").alias("monto_intereses"),
        col("DSCAMT").alias("monto_descuento"),
        col("PNLAMT").alias("monto_penalidad"),
        col("REFAMT").alias("monto_referencia"),
        col("LIMAMT").alias("limite_permitido"),
        col("AVLAMT").alias("monto_disponible"),
        col("HLDAMT").alias("monto_retenido"),
        col("OVRAMT").alias("monto_sobregiro"),
        col("MINAMT").alias("monto_minimo_requerido"),
        col("MAXAMT").alias("monto_maximo_permitido"),
        col("AVGAMT").alias("monto_promedio_diario"),
        col("CSHREC").alias("monto_efectivo_recibido"),
        col("CSHGVN").alias("monto_efectivo_entregado"),
        col("TIPAMT").alias("monto_propina"),
        col("RNDAMT").alias("monto_redondeo"),
        col("SURCHG").alias("recargo_adicional"),
        col("INSAMT").alias("monto_seguro"),
        col("ADJAMT").alias("monto_ajuste"),
        col("DLYACM").alias("acumulado_diario"),
        col("WKACM").alias("acumulado_semanal"),
        col("MTHACM").alias("acumulado_mensual"),
        col("FechaIngestaDatos").alias("fecha_ingesta_datos")
    )

    # -------------------------------------------------------------------------
    # Paso 4: Agregar 4 campos calculados numericos (TABLA E — data-model.md, RF-007)
    # Todos los operandos se protegen con coalesce a 0 para evitar nulos (RF-017).
    # -------------------------------------------------------------------------

    # Campo 1: monto_neto_comisiones = NETAMT - (FEEAMT + TAXAMT)
    # Manejo de nulos: coalesce a 0 para cada operando individualmente
    monto_neto_comisiones = (
        coalesce(col("monto_neto"), lit(0.0)) -
        (coalesce(col("monto_comision"), lit(0.0)) + coalesce(col("monto_impuesto"), lit(0.0)))
    )

    # Campo 2: porcentaje_comision_sobre_monto = (FEEAMT / ORGAMT) * 100
    # Manejo de division por cero: cuando ORGAMT == 0 o es nulo, resultado es 0
    porcentaje_comision = when(
        col("monto_original").isNull() | (col("monto_original") == 0),
        lit(0.0)
    ).otherwise(
        (coalesce(col("monto_comision"), lit(0.0)) / col("monto_original")) * 100
    )

    # Campo 3: variacion_saldo_transaccion = abs(BLNAFT - BLNBFR)
    # Manejo de nulos: coalesce a 0 para cada operando individualmente
    variacion_saldo = spark_abs(
        coalesce(col("saldo_despues_transaccion"), lit(0.0)) -
        coalesce(col("saldo_antes_transaccion"), lit(0.0))
    )

    # Campo 4: indicador_impacto_financiero = TRXAMT + FEEAMT + TAXAMT + PNLAMT
    # Manejo de nulos: coalesce a 0 para cada operando individualmente
    indicador_impacto = (
        coalesce(col("monto_transaccion"), lit(0.0)) +
        coalesce(col("monto_comision"), lit(0.0)) +
        coalesce(col("monto_impuesto"), lit(0.0)) +
        coalesce(col("monto_penalidad"), lit(0.0))
    )

    df_con_calculados = (
        df_renombrado
        .withColumn("monto_neto_comisiones", monto_neto_comisiones)
        .withColumn("porcentaje_comision_sobre_monto", porcentaje_comision)
        .withColumn("variacion_saldo_transaccion", variacion_saldo)
        .withColumn("indicador_impacto_financiero", indicador_impacto)
    )

    # -------------------------------------------------------------------------
    # Paso 5: Reordenar columnas para Liquid Cluster (RF-009)
    # Las columnas de Liquid Cluster deben estar entre las primeras 32
    # para que Delta pueda recopilar estadisticas (min/max).
    # -------------------------------------------------------------------------
    df_final = reordenar_columnas_liquid_cluster(
        df_con_calculados,
        ["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]
    )

    return df_final
