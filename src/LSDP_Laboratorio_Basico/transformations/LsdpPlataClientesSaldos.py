# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # LsdpPlataClientesSaldos — Vista Materializada: Clientes y Saldos Consolidados
# MAGIC
# MAGIC **Proposito**: Crear la vista materializada `clientes_saldos_consolidados` en la medalla
# MAGIC de plata, consolidando cmstfl + blncfl como Dimension Tipo 1 con 175 columnas
# MAGIC (71 cmstfl + 100 blncfl + 4 calculados), window functions para obtener el registro
# MAGIC mas reciente por cliente, 3 campos CASE + 1 SHA256, validacion expect_or_drop
# MAGIC para CUSTID nulo, y Liquid Cluster.
# MAGIC
# MAGIC **Patron — Closure**:
# MAGIC Los parametros de Azure SQL y el esquema del pipeline se calculan UNA SOLA VEZ
# MAGIC a nivel de modulo (al inicializar el pipeline). Los valores quedan capturados
# MAGIC como variables de modulo en el closure de la funcion decorada con @dp.materialized_view.
# MAGIC Este patron reemplaza spark.sparkContext.broadcast() que no es compatible
# MAGIC con Computo Serverless (RF-011).
# MAGIC
# MAGIC **Resultado esperado**:
# MAGIC - Vista: `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados`
# MAGIC - Columnas: 175 (71 cmstfl + 100 blncfl + 4 calculados)
# MAGIC - Dimension Tipo 1: una fila por CUSTID con datos mas recientes
# MAGIC - Liquid Cluster: [huella_identificacion_cliente, identificador_cliente]
# MAGIC - Expectativa: expect_or_drop para CUSTID nulo

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
from pyspark.sql.functions import (
    col, sha2, when, datediff, current_date, abs as spark_abs
)
from pyspark.sql.window import Window

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
# MAGIC ## Definicion de la Vista Materializada: clientes_saldos_consolidados
# MAGIC
# MAGIC Decoradores aplicados:
# MAGIC - `@dp.materialized_view`: nombre dinamico, comment, 5 propiedades Delta, Liquid Cluster
# MAGIC - `@dp.expect_or_drop`: elimina registros con CUSTID nulo (RF-018)
# MAGIC
# MAGIC Estrategia de deduplicacion (Dimension Tipo 1, RF-003, R10-D1):
# MAGIC - Window function por CUSTID ordenado por FechaIngestaDatos descendente
# MAGIC - Filtrar row_number == 1 en cada tabla fuente antes del JOIN
# MAGIC - LEFT JOIN cmstfl_latest → blncfl_latest por CUSTID

# COMMAND ----------

@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados",
    comment="Vista materializada de plata V4 — Consolidacion Dimension Tipo 1 de clientes y saldos. "
            "Combina cmstfl + blncfl via LEFT JOIN conservando el registro mas reciente por CUSTID. "
            "Incluye 3 campos CASE de clasificacion y 1 campo SHA256 para Liquid Cluster.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days"
    },
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"]
)
@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")
def clientes_saldos_consolidados():

    # -------------------------------------------------------------------------
    # Paso 1: Leer fuentes de bronce con lectura batch
    # Se usa spark.read.table() para lectura batch de las streaming tables de bronce.
    # -------------------------------------------------------------------------
    catalogo_bronce = parametros_sql["catalogoBronce"]
    df_cmstfl = spark.read.table(f"{catalogo_bronce}.regional.cmstfl")
    df_blncfl = spark.read.table(f"{catalogo_bronce}.regional.blncfl")

    # -------------------------------------------------------------------------
    # Paso 2: Excluir columnas no deseadas de ambas tablas fuente
    # - _rescued_data: columna interna de AutoLoader
    # - año, mes, dia: columnas de particion del lazy evaluation de bronce
    # -------------------------------------------------------------------------
    columnas_excluir = {"_rescued_data", "año", "mes", "dia"}

    columnas_cmstfl_limpias = [c for c in df_cmstfl.columns if c not in columnas_excluir]
    columnas_blncfl_limpias = [c for c in df_blncfl.columns if c not in columnas_excluir]

    df_cmstfl = df_cmstfl.select(columnas_cmstfl_limpias)
    df_blncfl = df_blncfl.select(columnas_blncfl_limpias)

    # -------------------------------------------------------------------------
    # Paso 3: Window functions para obtener el registro mas reciente por CUSTID
    # Dimension Tipo 1: una fila por CUSTID con los datos mas recientes (RF-003, R10-D1)
    # -------------------------------------------------------------------------
    from pyspark.sql.functions import row_number, desc

    window_spec = Window.partitionBy("CUSTID").orderBy(desc("FechaIngestaDatos"))

    df_cmstfl_latest = (
        df_cmstfl
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    df_blncfl_latest = (
        df_blncfl
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # -------------------------------------------------------------------------
    # Paso 4: LEFT JOIN cmstfl_latest → blncfl_latest por CUSTID (R10-D2)
    # Todos los clientes del maestro aparecen; los sin cuenta tienen campos blncfl nulos.
    #
    # IMPORTANTE — por qué se usa col("c.CUSTID") == col("b.CUSTID"):
    # Tras aplicar .alias(), Spark crea un nuevo nodo en el plan lógico. Referenciar
    # el DataFrame original (df_cmstfl_latest["CUSTID"]) genera MISSING_ATTRIBUTES
    # porque el optimizador no puede reconciliar el atributo pre-alias con el nodo
    # aliasado. Usar col("alias.columna") resuelve la referencia directamente sobre
    # el plan aliasado, sin ambigüedad.
    #
    # b.CUSTID no se incluye en el select() del Paso 5 — queda excluido de la salida.
    # -------------------------------------------------------------------------
    df_joined = df_cmstfl_latest.alias("c").join(
        df_blncfl_latest.alias("b"),
        col("c.CUSTID") == col("b.CUSTID"),
        how="left"
    )

    # -------------------------------------------------------------------------
    # Paso 5: Renombrar 71 columnas de cmstfl a espanol (TABLA A — data-model.md)
    # -------------------------------------------------------------------------
    df_renombrado = df_joined.select(
        # TABLA A — cmstfl (71 columnas)
        col("c.CUSTID").alias("identificador_cliente"),
        col("c.CUSTNM").alias("nombre_completo_cliente"),
        col("c.FRSTNM").alias("primer_nombre"),
        col("c.MDLNM").alias("segundo_nombre"),
        col("c.LSTNM").alias("primer_apellido"),
        col("c.SCNDLN").alias("segundo_apellido"),
        col("c.GNDR").alias("genero"),
        col("c.IDTYPE").alias("tipo_documento_identidad"),
        col("c.IDNMBR").alias("numero_documento_identidad"),
        col("c.NATNLT").alias("codigo_nacionalidad"),
        col("c.MRTLST").alias("estado_civil"),
        col("c.ADDR1").alias("direccion_linea_uno"),
        col("c.ADDR2").alias("direccion_linea_dos"),
        col("c.CITY").alias("ciudad_residencia"),
        col("c.STATE").alias("estado_provincia"),
        col("c.ZPCDE").alias("codigo_postal"),
        col("c.CNTRY").alias("pais_residencia"),
        col("c.PHONE1").alias("telefono_principal"),
        col("c.PHONE2").alias("telefono_secundario"),
        col("c.EMAIL").alias("correo_electronico"),
        col("c.OCCPTN").alias("ocupacion_profesion"),
        col("c.EMPLYR").alias("nombre_empleador"),
        col("c.EMPADS").alias("direccion_empleador"),
        col("c.BRNCOD").alias("codigo_sucursal_cliente"),        # duplicada: calificador _cliente
        col("c.BRNNM").alias("nombre_sucursal_cliente"),         # duplicada: calificador _cliente
        col("c.SGMNT").alias("segmento_cliente"),                # duplicada: calificador _cliente
        col("c.CSTCAT").alias("categoria_cliente"),
        col("c.RISKLV").alias("nivel_riesgo_cliente"),           # duplicada: calificador _cliente
        col("c.PRDTYP").alias("tipo_producto_principal"),
        col("c.ACCTST").alias("estado_cuenta_cliente"),          # duplicada: calificador _cliente
        col("c.TXID").alias("identificador_fiscal"),
        col("c.LGLLNM").alias("nombre_legal_completo"),
        col("c.MTHNM").alias("nombre_madre"),
        col("c.FTHNM").alias("nombre_padre"),
        col("c.CNTPRS").alias("persona_contacto_emergencia"),
        col("c.CNTPH").alias("telefono_contacto_emergencia"),
        col("c.PREFNM").alias("nombre_preferido"),
        col("c.LANG").alias("idioma_preferido"),
        col("c.EDLVL").alias("nivel_educativo"),
        col("c.INCSRC").alias("fuente_ingresos"),
        col("c.RELTYP").alias("tipo_relacion_bancaria"),
        col("c.NTFPRF").alias("preferencia_notificacion"),
        col("c.BRTDT").alias("fecha_nacimiento"),
        col("c.OPNDT").alias("fecha_apertura_cuenta"),           # duplicada: calificador maestro
        col("c.LSTTRX").alias("fecha_ultima_transaccion"),       # duplicada: calificador maestro
        col("c.LSTUPD").alias("fecha_ultima_actualizacion"),
        col("c.CRTNDT").alias("fecha_creacion_registro"),
        col("c.EXPDT").alias("fecha_vencimiento_documento"),
        col("c.EMPSDT").alias("fecha_inicio_empleo"),
        col("c.LSTLGN").alias("fecha_ultimo_login_digital"),
        col("c.RVWDT").alias("fecha_revision_kyc"),              # duplicada: calificador _kyc
        col("c.VLDDT").alias("fecha_validacion_datos"),
        col("c.ENRLDT").alias("fecha_enrolamiento_digital"),
        col("c.CNCLDT").alias("fecha_cancelacion_maestro"),      # duplicada: calificador _maestro
        col("c.RJCTDT").alias("fecha_rechazo"),
        col("c.PRMDT").alias("fecha_promocion_segmento"),        # duplicada: calificador _segmento
        col("c.CHGDT").alias("fecha_cambio_estado"),             # duplicada: calificador _estado
        col("c.LSTCDT").alias("fecha_ultimo_contacto"),
        col("c.NXTRVW").alias("fecha_proxima_revision"),
        col("c.BKRLDT").alias("fecha_relacion_banco"),
        col("c.ANNLINC").alias("ingreso_anual_declarado"),
        col("c.MNTHINC").alias("ingreso_mensual"),
        col("c.CRDSCR").alias("puntaje_crediticio"),
        col("c.DPNDNT").alias("numero_dependientes"),
        col("c.TTLPRD").alias("total_productos_contratados"),
        col("c.FNCLYR").alias("anio_fiscal_vigente"),
        col("c.AGECST").alias("edad_cliente"),
        col("c.YRBNKG").alias("anios_relacion_bancaria"),
        col("c.RSKSCR").alias("score_riesgo_calculado"),
        col("c.NUMPHN").alias("cantidad_telefonos_registrados"),
        col("c.FechaIngestaDatos").alias("fecha_ingesta_maestro"),  # duplicada: calificador _maestro
        # TABLA B — blncfl (100 columnas, sin CUSTID)
        col("b.ACCTID").alias("identificador_cuenta"),
        col("b.ACCTTYP").alias("tipo_cuenta"),
        col("b.ACCTNM").alias("nombre_cuenta"),
        col("b.ACCTST").alias("estado_cuenta_saldo"),            # duplicada: calificador _saldo
        col("b.CRNCOD").alias("codigo_moneda_cuenta"),
        col("b.BRNCOD").alias("codigo_sucursal_cuenta"),         # duplicada: calificador _cuenta
        col("b.BRNNM").alias("nombre_sucursal_cuenta"),          # duplicada: calificador _cuenta
        col("b.PRDCOD").alias("codigo_producto"),
        col("b.PRDNM").alias("nombre_producto"),
        col("b.PRDCAT").alias("categoria_producto"),
        col("b.SGMNT").alias("segmento_cuenta"),                 # duplicada: calificador _cuenta
        col("b.RISKLV").alias("nivel_riesgo_cuenta"),            # duplicada: calificador _cuenta
        col("b.RGNCD").alias("codigo_region"),
        col("b.RGNNM").alias("nombre_region"),
        col("b.CSTGRP").alias("grupo_cliente"),
        col("b.BLKST").alias("estado_bloqueo"),
        col("b.EMBST").alias("estado_embargo"),
        col("b.OVDST").alias("estado_mora"),
        col("b.DGTST").alias("estado_digital"),
        col("b.CRDST").alias("estado_crediticio"),
        col("b.LNTYP").alias("tipo_linea_credito"),
        col("b.GRNTYP").alias("tipo_garantia"),
        col("b.PYMFRQ").alias("frecuencia_pago"),
        col("b.INTTYP").alias("tipo_tasa_interes"),
        col("b.TXCTG").alias("categoria_fiscal"),
        col("b.CHKTYP").alias("tipo_chequera"),
        col("b.CRDGRP").alias("grupo_crediticio"),
        col("b.CLSCD").alias("codigo_clasificacion"),
        col("b.SRCCD").alias("codigo_fuente"),
        col("b.AVLBAL").alias("saldo_disponible"),
        col("b.CURBAL").alias("saldo_actual"),
        col("b.HLDBAL").alias("saldo_retenido"),
        col("b.OVRBAL").alias("saldo_sobregiro"),
        col("b.PNDBAL").alias("saldo_pendiente"),
        col("b.AVGBAL").alias("saldo_promedio_periodo"),
        col("b.MINBAL").alias("saldo_minimo_periodo"),
        col("b.MAXBAL").alias("saldo_maximo_periodo"),
        col("b.OPNBAL").alias("saldo_apertura_periodo"),
        col("b.CLSBAL").alias("saldo_cierre_periodo"),
        col("b.INTACC").alias("intereses_acumulados"),
        col("b.INTPAY").alias("intereses_pagados"),
        col("b.INTRCV").alias("intereses_recibidos"),
        col("b.FEEACC").alias("comisiones_acumuladas"),
        col("b.FEEPAY").alias("comisiones_pagadas"),
        col("b.CRDLMT").alias("limite_credito"),
        col("b.CRDAVL").alias("credito_disponible"),
        col("b.CRDUSD").alias("credito_utilizado"),
        col("b.PYMAMT").alias("monto_pago_minimo"),
        col("b.PYMLST").alias("ultimo_pago_realizado"),
        col("b.TTLDBT").alias("total_debitos_periodo"),
        col("b.TTLCRD").alias("total_creditos_periodo"),
        col("b.TTLTRX").alias("total_transacciones_periodo"),
        col("b.LNAMT").alias("monto_prestamo"),
        col("b.LNBAL").alias("saldo_prestamo"),
        col("b.MTHPYM").alias("pago_mensual"),
        col("b.INTRT").alias("tasa_interes_vigente"),
        col("b.PNLRT").alias("tasa_penalidad"),
        col("b.OVRRT").alias("tasa_sobregiro"),
        col("b.TAXAMT").alias("impuestos_acumulados"),
        col("b.INSAMT").alias("seguros_acumulados"),
        col("b.DLYINT").alias("interes_diario"),
        col("b.YLDRT").alias("tasa_rendimiento"),
        col("b.SPRDRT").alias("spread_tasa"),
        col("b.MRGAMT").alias("monto_margen"),
        col("b.OPNDT").alias("fecha_apertura_cuenta_saldo"),     # duplicada: calificador _saldo
        col("b.CLSDT").alias("fecha_cierre_cuenta"),
        col("b.LSTTRX").alias("fecha_ultima_transaccion_saldo"), # duplicada: calificador _saldo
        col("b.LSTPYM").alias("fecha_ultimo_pago"),
        col("b.NXTPYM").alias("fecha_proximo_pago"),
        col("b.MATDT").alias("fecha_vencimiento_cuenta"),
        col("b.RNWDT").alias("fecha_renovacion"),
        col("b.RVWDT").alias("fecha_revision_cuenta"),           # duplicada: calificador _cuenta
        col("b.CRTDT").alias("fecha_creacion_registro_saldo"),
        col("b.UPDDT").alias("fecha_actualizacion_saldo"),
        col("b.STMDT").alias("fecha_estado_cuenta"),
        col("b.CUTDT").alias("fecha_corte"),
        col("b.GRPDT").alias("fecha_periodo_gracia"),
        col("b.INTDT").alias("fecha_calculo_intereses"),
        col("b.FEEDT").alias("fecha_cobro_comisiones"),
        col("b.BLKDT").alias("fecha_bloqueo"),
        col("b.EMBDT").alias("fecha_embargo"),
        col("b.OVDDT").alias("fecha_inicio_mora"),
        col("b.PYMDT1").alias("fecha_primer_pago"),
        col("b.PYMDT2").alias("fecha_segundo_pago"),
        col("b.PRJDT").alias("fecha_proyeccion"),
        col("b.ADJDT").alias("fecha_ajuste"),
        col("b.RCLDT").alias("fecha_reconciliacion"),
        col("b.NTFDT").alias("fecha_notificacion_cuenta"),
        col("b.CNCLDT").alias("fecha_cancelacion_cuenta"),       # duplicada: calificador _cuenta
        col("b.RCTDT").alias("fecha_reactivacion"),
        col("b.CHGDT").alias("fecha_cambio_condiciones"),        # duplicada: calificador _condiciones
        col("b.VRFDT").alias("fecha_verificacion"),
        col("b.PRMDT").alias("fecha_promocion_cuenta"),          # duplicada: calificador _cuenta
        col("b.DGTDT").alias("fecha_acceso_digital"),
        col("b.AUDT").alias("fecha_auditoria"),
        col("b.MGRDT").alias("fecha_migracion"),
        col("b.ESCDT").alias("fecha_escalamiento"),
        col("b.RPTDT").alias("fecha_reporte_regulatorio"),
        col("b.ARCDT").alias("fecha_archivado_cuenta"),
        col("b.FechaIngestaDatos").alias("fecha_ingesta_saldo")  # duplicada: calificador _saldo
    )

    # -------------------------------------------------------------------------
    # Paso 6: Agregar 4 campos calculados (TABLA C — data-model.md)
    # -------------------------------------------------------------------------

    # Variable intermedia para perfil_actividad_bancaria
    dias_sin_transaccion = datediff(current_date(), col("fecha_ultima_transaccion"))

    # Campo 1: clasificacion_riesgo_cliente — CASE secuencial con 5 umbrales (RF-004)
    clasificacion_riesgo = (
        when(
            (col("nivel_riesgo_cliente").isin("04", "05")) &
            (col("puntaje_crediticio") < 500) &
            (col("estado_mora") == "SI"),
            "RIESGO_CRITICO"
        )
        .when(
            (col("nivel_riesgo_cliente").isin("03", "04", "05")) &
            (col("puntaje_crediticio") < 600) &
            (col("estado_mora") == "SI"),
            "RIESGO_ALTO"
        )
        .when(
            col("nivel_riesgo_cliente").isin("03", "04", "05") |
            ((col("puntaje_crediticio") < 650) & (col("estado_mora") == "SI")),
            "RIESGO_MEDIO"
        )
        .when(
            (col("nivel_riesgo_cliente") == "02") | (col("puntaje_crediticio") < 700),
            "RIESGO_BAJO"
        )
        .otherwise("SIN_RIESGO")
    )

    # Campo 2: categoria_saldo_disponible — CASE secuencial con 5 umbrales (RF-004)
    categoria_saldo = (
        when(
            (col("saldo_disponible") >= 200000) &
            (col("limite_credito") >= 100000) &
            (col("segmento_cliente") == "VIP"),
            "SALDO_PREMIUM"
        )
        .when(
            (col("saldo_disponible") >= 100000) &
            (col("limite_credito") >= 50000) &
            (col("segmento_cliente").isin("VIP", "PREM")),
            "SALDO_ALTO"
        )
        .when(
            (col("saldo_disponible") >= 25000) & (col("limite_credito") >= 10000),
            "SALDO_MEDIO"
        )
        .when(
            (col("saldo_disponible") >= 5000) |
            ((col("limite_credito") >= 5000) & col("segmento_cliente").isin("STD", "BAS")),
            "SALDO_BAJO"
        )
        .otherwise("SALDO_CRITICO")
    )

    # Campo 3: perfil_actividad_bancaria — CASE secuencial con variable intermedia (RF-004)
    perfil_actividad = (
        when(
            (dias_sin_transaccion <= 30) &
            (col("total_productos_contratados") >= 8) &
            (col("estado_cuenta_cliente") == "AC"),
            "CLIENTE_INTEGRAL"
        )
        .when(
            (dias_sin_transaccion <= 90) &
            (col("total_productos_contratados") >= 5) &
            (col("estado_cuenta_cliente") == "AC"),
            "ACTIVIDAD_ALTA"
        )
        .when(
            (dias_sin_transaccion <= 180) &
            (col("total_productos_contratados") >= 3) &
            (col("estado_cuenta_cliente") == "AC"),
            "ACTIVIDAD_MEDIA"
        )
        .when(
            (dias_sin_transaccion <= 365) &
            (col("total_productos_contratados") >= 1) &
            (col("estado_cuenta_cliente").isin("AC", "IN")),
            "ACTIVIDAD_BAJA"
        )
        .otherwise("INACTIVO")
    )

    # Campo 4: huella_identificacion_cliente — SHA256 del CUSTID (RF-005)
    huella_sha256 = sha2(col("identificador_cliente").cast("string"), 256)

    df_con_calculados = (
        df_renombrado
        .withColumn("clasificacion_riesgo_cliente", clasificacion_riesgo)
        .withColumn("categoria_saldo_disponible", categoria_saldo)
        .withColumn("perfil_actividad_bancaria", perfil_actividad)
        .withColumn("huella_identificacion_cliente", huella_sha256)
    )

    # -------------------------------------------------------------------------
    # Paso 7: Reordenar columnas para Liquid Cluster (RF-009)
    # Las columnas de Liquid Cluster deben estar entre las primeras 32
    # para que Delta pueda recopilar estadisticas (min/max).
    # -------------------------------------------------------------------------
    df_final = reordenar_columnas_liquid_cluster(
        df_con_calculados,
        ["huella_identificacion_cliente", "identificador_cliente"]
    )

    return df_final
