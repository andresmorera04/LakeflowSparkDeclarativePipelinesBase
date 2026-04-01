# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # LsdpOroClientes — Vistas Materializadas de Oro: Comportamiento ATM y Resumen Integral
# MAGIC
# MAGIC **Proposito**: Crear las 2 vistas materializadas de la medalla de oro en el pipeline LSDP V5:
# MAGIC
# MAGIC 1. `comportamiento_atm_cliente` — 6 columnas con metricas agregadas de comportamiento
# MAGIC    ATM y pagos al saldo por cliente. Usa agregaciones condicionales en una sola pasada
# MAGIC    sobre `transacciones_enriquecidas` de plata (`count(when(...))`, `avg(when(...))`,
# MAGIC    `sum(when(...))`). Incluye `@dp.expect_or_drop` para eliminar clientes con
# MAGIC    `identificador_cliente` nulo del resultado agrupado (R14-D1).
# MAGIC
# MAGIC 2. `resumen_integral_cliente` — 22 columnas que combinan 17 campos dimensionales
# MAGIC    de `clientes_saldos_consolidados` de plata con 5 metricas transaccionales de
# MAGIC    `comportamiento_atm_cliente` via LEFT JOIN. Coalesce a 0 para los clientes
# MAGIC    sin transacciones ATM ni PGSL.
# MAGIC
# MAGIC **Patron — Closure Compartido**:
# MAGIC Los parametros de Azure SQL y los esquemas del pipeline se calculan UNA SOLA VEZ
# MAGIC a nivel de modulo (al inicializar el pipeline). Los valores quedan capturados
# MAGIC como variables de modulo en el closure de AMBAS funciones decoradas con
# MAGIC `@dp.materialized_view`. Este patron reemplaza `spark.sparkContext.broadcast()`
# MAGIC que no es compatible con Computo Serverless (RF-010).
# MAGIC
# MAGIC **Resultado esperado**:
# MAGIC - Vista 1: `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente` (6 columnas)
# MAGIC - Vista 2: `{catalogoOro}.{esquema_oro}.resumen_integral_cliente` (22 columnas)
# MAGIC - Dimension Tipo 1 en ambas vistas (datos mas recientes, sin duplicados por cliente)
# MAGIC - Liquid Cluster en ambas vistas para optimizacion de consultas

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
from pyspark.sql.functions import col, count, when, avg, coalesce, lit
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura de Parametros del Pipeline (Nivel de Modulo — Closure Pattern)
# MAGIC
# MAGIC Este bloque se ejecuta UNA SOLA VEZ al inicializar el pipeline.
# MAGIC Lee el nombre del Scope Secret configurado como parametro del pipeline y usa
# MAGIC la funcion `leer_parametros_azure_sql` para obtener TODAS las claves de
# MAGIC `dbo.Parametros` incluyendo `catalogoOro` (destino) y `catalogoPlata` (fuente).
# MAGIC
# MAGIC Los esquemas de plata y oro se leen de los parametros del pipeline LSDP:
# MAGIC - `esquema_plata` (ej: "regional") — configurado desde V4
# MAGIC - `esquema_oro` (ej: "regional") — nuevo en V5
# MAGIC
# MAGIC Las variables calculadas aqui (`catalogo_plata`, `catalogo_oro`, `esquema_plata`,
# MAGIC `esquema_oro`, `table_properties`) quedan disponibles para AMBAS funciones
# MAGIC decoradas via closure (R15-D1, Principio I).

# COMMAND ----------

# Leer el nombre del Scope Secret desde los parametros del pipeline LSDP
nombre_scope_secret = spark.conf.get("pipelines.parameters.nombreScopeSecret")

# Invocar la funcion de conexion Azure SQL — retorna diccionario COMPLETO sin filtro (RF-009)
# Incluye: catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, catalogoOro, ...
parametros_sql = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)

# Fuente: catalogo de plata desde dbo.Parametros (clave catalogoPlata)
catalogo_plata = parametros_sql["catalogoPlata"]

# Destino: catalogo de oro desde dbo.Parametros (clave catalogoOro, RF-002)
catalogo_oro = parametros_sql["catalogoOro"]

# Fuente: esquema de plata desde los parametros del pipeline LSDP (configurado en V4)
esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")

# Destino: esquema de oro desde los parametros del pipeline LSDP (nuevo en V5, RF-002)
esquema_oro = spark.conf.get("pipelines.parameters.esquema_oro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicion de Propiedades Delta Estandar
# MAGIC
# MAGIC Diccionario con las 5 propiedades Delta estandar del proyecto LSDP (RF-006).
# MAGIC Se define una sola vez y se reutiliza en AMBAS vistas materializadas.
# MAGIC
# MAGIC Propiedades configuradas:
# MAGIC - `enableChangeDataFeed`: Habilita el feed de cambios para consumo incremental aguas abajo
# MAGIC - `autoCompact`: Compactacion automatica de archivos pequenos para mejorar rendimiento
# MAGIC - `optimizeWrite`: Optimizacion automatica de escritura para archivos de tamano optimo
# MAGIC - `deletedFileRetentionDuration`: Retiene archivos eliminados 30 dias para time travel
# MAGIC - `logRetentionDuration`: Retiene el log de transacciones 60 dias para auditoria

# COMMAND ----------

table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vista Materializada 1: comportamiento_atm_cliente
# MAGIC
# MAGIC **Objetivo**: Agregar el comportamiento transaccional ATM y pagos al saldo por cliente.
# MAGIC
# MAGIC **Fuente**: `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas` leida
# MAGIC con `spark.read.table()` sin filtros previos (RF-017). LSDP gestiona la
# MAGIC actualizacion incremental automaticamente.
# MAGIC
# MAGIC **Logica de agregacion (R13-D1)**:
# MAGIC Se usa `groupBy("identificador_cliente").agg(...)` con agregaciones condicionales
# MAGIC en una SOLA PASADA sobre los datos (eficiencia para 15M+ transacciones). Los tipos
# MAGIC de transaccion (CATM, DATM, PGSL) se filtran dentro de las funciones de agregacion:
# MAGIC - `count(when(tipo == "CATM", 1))` — retorna 0 nativamente si no hay coincidencias
# MAGIC - `avg(when(tipo == "CATM", coalesce(monto, 0)))` — nulos del monto tratados como 0
# MAGIC - `sum(when(tipo == "PGSL", coalesce(monto, 0)))` — nulos del monto tratados como 0
# MAGIC
# MAGIC **Expectativa (R14-D1)**:
# MAGIC `@dp.expect_or_drop` se aplica sobre el DataFrame AGRUPADO final. Si existen
# MAGIC transacciones con `identificador_cliente` nulo en la fuente, el `groupBy` crea
# MAGIC un grupo para NULL, y la expectation lo elimina del resultado.
# MAGIC
# MAGIC **Resultado**: 6 columnas, 1 fila por cliente (Dimension Tipo 1),
# MAGIC Liquid Cluster por `identificador_cliente`.

# COMMAND ----------

@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente",
    comment=(
        "Vista materializada de metricas agregadas de comportamiento ATM y pagos al saldo "
        "por cliente. Fuente: transacciones_enriquecidas de plata. "
        "Agrupacion por identificador_cliente con conteos, promedios y sumas condicionales "
        "en una sola pasada (count/avg/sum de when). "
        "Tipos de transaccion: CATM (deposito ATM), DATM (retiro ATM), PGSL (pago al saldo). "
        "Expectativa: expect_or_drop elimina clientes con identificador_cliente NULL."
    ),
    table_properties=table_properties,
    cluster_by=["identificador_cliente"]
)
@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")
def comportamiento_atm_cliente():
    # -------------------------------------------------------------------------
    # Paso 1: Leer ALL las transacciones enriquecidas de plata sin filtros (RF-017)
    # spark.read.table() para lectura batch — LSDP gestiona la actualizacion
    # incremental automaticamente (R15-D1).
    # -------------------------------------------------------------------------
    df_transacciones = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas"
    )

    # -------------------------------------------------------------------------
    # Paso 2: Agrupar por identificador_cliente y calcular 5 metricas condicionales
    # en una SOLA PASADA sobre los datos (R13-D1, RF-016).
    #
    # Metrica 1: cantidad_depositos_atm
    #   count(when("CATM", 1)) — retorna 0 si no hay transacciones CATM
    #
    # Metrica 2: cantidad_retiros_atm
    #   count(when("DATM", 1)) — retorna 0 si no hay transacciones DATM
    #
    # Metrica 3: promedio_monto_depositos_atm
    #   coalesce(avg(when("CATM", coalesce(monto, 0))), 0)
    #   - coalesce interno: trata monto NULL como 0 en el calculo del promedio
    #   - coalesce externo: retorna 0 si no hay transacciones CATM (avg de vacio = NULL)
    #
    # Metrica 4: promedio_monto_retiros_atm
    #   coalesce(avg(when("DATM", coalesce(monto, 0))), 0)
    #   - misma logica que metrica 3 para tipo DATM
    #
    # Metrica 5: total_pagos_saldo_cliente
    #   coalesce(spark_sum(when("PGSL", coalesce(monto, 0))), 0)
    #   - coalesce interno: trata monto NULL como 0 en la suma
    #   - coalesce externo: retorna 0 si no hay transacciones PGSL (sum de vacio = NULL)
    # -------------------------------------------------------------------------
    df_agrupado = df_transacciones.groupBy("identificador_cliente").agg(
        # Conteo de depositos ATM (tipo CATM)
        count(when(col("tipo_transaccion") == "CATM", 1)).alias("cantidad_depositos_atm"),
        # Conteo de retiros ATM (tipo DATM)
        count(when(col("tipo_transaccion") == "DATM", 1)).alias("cantidad_retiros_atm"),
        # Promedio de montos de depositos ATM — coalesce doble para manejar nulos
        coalesce(
            avg(when(
                col("tipo_transaccion") == "CATM",
                coalesce(col("monto_transaccion"), lit(0))
            )),
            lit(0)
        ).alias("promedio_monto_depositos_atm"),
        # Promedio de montos de retiros ATM — coalesce doble para manejar nulos
        coalesce(
            avg(when(
                col("tipo_transaccion") == "DATM",
                coalesce(col("monto_transaccion"), lit(0))
            )),
            lit(0)
        ).alias("promedio_monto_retiros_atm"),
        # Suma total de pagos al saldo (tipo PGSL) — coalesce doble para manejar nulos
        coalesce(
            spark_sum(when(
                col("tipo_transaccion") == "PGSL",
                coalesce(col("monto_transaccion"), lit(0))
            )),
            lit(0)
        ).alias("total_pagos_saldo_cliente")
    )

    # -------------------------------------------------------------------------
    # Paso 3: Reordenar columnas para optimizacion del Liquid Cluster (RF-007)
    # La columna de clustering debe ir primero para maximizar la eficiencia.
    # -------------------------------------------------------------------------
    df_final = reordenar_columnas_liquid_cluster(df_agrupado, ["identificador_cliente"])

    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vista Materializada 2: resumen_integral_cliente
# MAGIC
# MAGIC **Objetivo**: Crear el perfil integral del cliente combinando datos dimensionales
# MAGIC de plata con las metricas transaccionales de la vista de oro.
# MAGIC
# MAGIC **Fuentes**:
# MAGIC 1. `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` — 17 campos
# MAGIC    seleccionados: 5 identificativos + 4 demograficos + 3 clasificacion + 5 financieros
# MAGIC 2. `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente` — 5 metricas ATM
# MAGIC    (leida via `spark.read.table()` — LSDP resuelve la dependencia DAG automaticamente
# MAGIC    dentro del mismo pipeline, R15-D1)
# MAGIC
# MAGIC **Logica de JOIN**:
# MAGIC LEFT JOIN desde `clientes_saldos_consolidados` hacia `comportamiento_atm_cliente`
# MAGIC por `identificador_cliente`. El LEFT JOIN preserva TODOS los clientes de plata,
# MAGIC incluyendo los que no tienen transacciones ATM/PGSL. Para estos clientes, los
# MAGIC valores de las 5 metricas se coalescan a 0 (RF-016).
# MAGIC
# MAGIC **Resultado**: 22 columnas, 1 fila por cliente (Dimension Tipo 1),
# MAGIC Liquid Cluster por `huella_identificacion_cliente` e `identificador_cliente`.

# COMMAND ----------

@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente",
    comment=(
        "Vista materializada de resumen integral del cliente. "
        "Combina datos dimensionales de plata (clientes_saldos_consolidados) con "
        "metricas transaccionales agregadas de oro (comportamiento_atm_cliente) via LEFT JOIN. "
        "Contiene 17 campos de plata (identificativos, demograficos, clasificacion, financieros) "
        "y 5 metricas ATM (coalesce a 0 para clientes sin transacciones). "
        "Punto de consumo final del producto de datos LSDP V5."
    ),
    table_properties=table_properties,
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"]
)
def resumen_integral_cliente():
    # -------------------------------------------------------------------------
    # Paso 1: Leer datos dimensionales de clientes y saldos de plata (RF-004)
    # Lectura batch sin filtros — se seleccionan 17 columnas del total de 175
    # disponibles en la vista de plata (R15-D1).
    # -------------------------------------------------------------------------
    df_clientes = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"
    ).select(
        # Campos identificativos (5)
        "identificador_cliente",
        "huella_identificacion_cliente",
        "nombre_completo_cliente",
        "tipo_documento_identidad",
        "numero_documento_identidad",
        # Campos demograficos (4)
        "segmento_cliente",
        "categoria_cliente",
        "ciudad_residencia",
        "pais_residencia",
        # Campos de clasificacion calculados en plata (3) — NO recalculados en oro
        "clasificacion_riesgo_cliente",
        "categoria_saldo_disponible",
        "perfil_actividad_bancaria",
        # Campos financieros (5)
        "saldo_disponible",
        "saldo_actual",
        "limite_credito",
        "puntaje_crediticio",
        "ingreso_anual_declarado"
    )

    # -------------------------------------------------------------------------
    # Paso 2: Leer metricas de comportamiento ATM de la vista de oro (R15-D1)
    # LSDP resuelve automaticamente la dependencia DAG entre vistas del mismo
    # pipeline: comportamiento_atm_cliente se procesa primero (ya esta decorada
    # arriba), y resumen_integral_cliente la lee como fuente.
    # -------------------------------------------------------------------------
    df_atm = spark.read.table(
        f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente"
    )

    # -------------------------------------------------------------------------
    # Paso 3: LEFT JOIN de clientes (plata) con metricas ATM (oro) (RF-004)
    # El LEFT JOIN preserva TODOS los clientes de plata aunque no tengan
    # transacciones ATM o PGSL. Para estos clientes, las 5 columnas de metricas
    # apareceran como NULL despues del JOIN (manejado en el paso 4).
    # -------------------------------------------------------------------------
    df_resultado = df_clientes.join(
        df_atm,
        on="identificador_cliente",
        how="left"
    )

    # -------------------------------------------------------------------------
    # Paso 4: Coalesce a 0 para las 5 metricas del LEFT JOIN (RF-016)
    # Los clientes sin transacciones ATM/PGSL tienen NULL en estas columnas
    # despues del JOIN. El coalesce las convierte a 0 para facilitar el consumo.
    # -------------------------------------------------------------------------
    df_resultado = (
        df_resultado
        .withColumn("cantidad_depositos_atm", coalesce(col("cantidad_depositos_atm"), lit(0)))
        .withColumn("cantidad_retiros_atm", coalesce(col("cantidad_retiros_atm"), lit(0)))
        .withColumn("promedio_monto_depositos_atm", coalesce(col("promedio_monto_depositos_atm"), lit(0.0)))
        .withColumn("promedio_monto_retiros_atm", coalesce(col("promedio_monto_retiros_atm"), lit(0.0)))
        .withColumn("total_pagos_saldo_cliente", coalesce(col("total_pagos_saldo_cliente"), lit(0.0)))
    )

    # -------------------------------------------------------------------------
    # Paso 5: Reordenar columnas para optimizacion del Liquid Cluster (RF-007)
    # Las columnas de clustering deben ir primero para maximizar la eficiencia.
    # Cluster por: huella_identificacion_cliente (SHA-256) e identificador_cliente
    # -------------------------------------------------------------------------
    df_final = reordenar_columnas_liquid_cluster(
        df_resultado,
        ["huella_identificacion_cliente", "identificador_cliente"]
    )

    return df_final
