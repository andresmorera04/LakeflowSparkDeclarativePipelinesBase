# Modelo de Datos — Version 5: LSDP Medalla de Oro

> **Feature:** `005-lsdp-medalla-oro` | **Fecha:** 2026-03-31 | **Base:** Decisiones R1-D1 a R15-D1

---

## Indice

1. [Fuentes de Plata](#1-fuentes-de-plata)
2. [Vistas Materializadas de Oro](#2-vistas-materializadas-de-oro)
3. [Parametros del Pipeline](#3-parametros-del-pipeline)
4. [Propiedades Delta Estandar](#4-propiedades-delta-estandar)
5. [Diagrama de Flujo de Datos](#5-diagrama-de-flujo-de-datos)

---

## 1. Fuentes de Plata

### 1.1 Tabla `dbo.Parametros` (Azure SQL) — Lectura V5

La tabla de parametros en Azure SQL contiene pares clave/valor que el pipeline consume a traves de la funcion `leer_parametros_azure_sql()`.

| # | Clave | Valor (ejemplo) | Version |
|---|-------|------------------|---------|
| 1 | `catalogoBronce` | `"bronce_dev"` | V3 (existente) |
| 2 | `contenedorBronce` | `"bronce"` | V3 (existente) |
| 3 | `datalake` | `"stlakedev"` | V3 (existente) |
| 4 | `DirectorioBronce` | `"/mnt/bronce"` | V3 (existente) |
| 5 | `catalogoPlata` | `"plata_dev"` | V4 (existente) |
| 6 | `catalogoOro` | `"oro_dev"` | V5 (existente) |

> **Nota (RF-019 V4):** La funcion `leer_parametros_azure_sql()` retorna **todas** las claves sin filtro. El codigo consumidor selecciona las claves que necesita del diccionario resultante. Para V5, basta con acceder a la clave `catalogoOro` del diccionario ya existente. **No se requiere modificar** la funcion.

### 1.2 Vistas Fuente de Plata (resumen)

Las dos vistas materializadas de plata que alimentan la capa de oro son:

| Vista Plata | Esquema | Columnas Totales | Desglose |
|---|---|---|---|
| `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` | Clientes + Saldos (Dim Tipo 1) | 175 | 71 cmstfl + 100 blncfl + 4 calculados |
| `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas` | Transacciones enriquecidas | 65 | 61 trxpfl + 4 calculados |

> **Nota importante:** Las vistas de oro leen de plata (no de bronce). Los nombres de columnas ya estan en espanol y snake_case desde plata.

---

## 2. Vistas Materializadas de Oro

### 2.1 Vista 1: `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente`

| Propiedad | Valor |
|---|---|
| **Tipo** | Vista Materializada (`@dp.materialized_view`) |
| **Fuente** | `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas` via `spark.read.table()` (R13-D1) |
| **Comportamiento** | Dimension Tipo 1 (heredada de plata — siempre los datos mas recientes) |
| **Expectativa** | `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")` — filtra el grupo NULL del resultado agrupado (R14-D1) |
| **Liquid Cluster** | `["identificador_cliente"]` |
| **Total de columnas** | **6** |
| **Patron de lectura** | `spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")` (R15-D1) |
| **Patron de agregacion** | `groupBy("identificador_cliente").agg(...)` con agregaciones condicionales `count(when(...))`, `avg(when(...))`, `sum(when(...))` (R13-D1) |

#### TABLA A — Columnas de `comportamiento_atm_cliente` (6 columnas)

| # | Nombre Oro | Tipo | Fuente Plata | Formula / Logica | Manejo de nulos | Descripcion |
|---|---|---|---|---|---|---|
| 1 | `identificador_cliente` | `LongType` | `identificador_cliente` (transacciones_enriquecidas) | Clave de agrupacion | Eliminado por `@dp.expect_or_drop` si es NULL (R14-D1) | Identificador unico del cliente. Clave de agrupacion |
| 2 | `cantidad_depositos_atm` | `LongType` | `tipo_transaccion` (transacciones_enriquecidas) | `count(when(col("tipo_transaccion") == "CATM", 1))` | Retorna 0 nativamente si no hay coincidencias | Cantidad de transacciones de tipo credito ATM (deposito) |
| 3 | `cantidad_retiros_atm` | `LongType` | `tipo_transaccion` (transacciones_enriquecidas) | `count(when(col("tipo_transaccion") == "DATM", 1))` | Retorna 0 nativamente si no hay coincidencias | Cantidad de transacciones de tipo debito ATM (retiro) |
| 4 | `promedio_monto_depositos_atm` | `DoubleType` | `tipo_transaccion`, `monto_transaccion` (transacciones_enriquecidas) | `coalesce(avg(when(col("tipo_transaccion") == "CATM", coalesce(col("monto_transaccion"), lit(0)))), lit(0))` | `coalesce` interno trata NULL como 0 en el calculo; `coalesce` externo a 0 si no hay transacciones CATM | Promedio del monto de depositos realizados via ATM |
| 5 | `promedio_monto_retiros_atm` | `DoubleType` | `tipo_transaccion`, `monto_transaccion` (transacciones_enriquecidas) | `coalesce(avg(when(col("tipo_transaccion") == "DATM", coalesce(col("monto_transaccion"), lit(0)))), lit(0))` | `coalesce` interno trata NULL como 0 en el calculo; `coalesce` externo a 0 si no hay transacciones DATM | Promedio del monto de retiros realizados via ATM |
| 6 | `total_pagos_saldo_cliente` | `DoubleType` | `tipo_transaccion`, `monto_transaccion` (transacciones_enriquecidas) | `coalesce(spark_sum(when(col("tipo_transaccion") == "PGSL", coalesce(col("monto_transaccion"), lit(0)))), lit(0))` | `coalesce` interno trata NULL como 0 en la suma; `coalesce` externo a 0 si no hay transacciones PGSL | Total sumarizado de pagos al saldo del cliente |

> **Nota (RF-017):** La vista NO aplica filtros previos a la agrupacion. Lee TODAS las transacciones de plata y usa agregaciones condicionales (`count(when(...))`, `avg(when(...))`, `sum(when(...))`) para filtrar por tipo durante la agregacion en una sola pasada sobre los datos.

> **Nota (RF-018, R14-D1):** El decorador `@dp.expect_or_drop` se evalua sobre las filas del DataFrame FINAL (agrupado). Si existen transacciones con `identificador_cliente` nulo, el `groupBy` crea un grupo para NULL, y la expectation lo elimina del resultado final. LSDP registra las metricas de eliminacion automaticamente.

---

### 2.2 Vista 2: `{catalogoOro}.{esquema_oro}.resumen_integral_cliente`

| Propiedad | Valor |
|---|---|
| **Tipo** | Vista Materializada (`@dp.materialized_view`) |
| **Fuentes** | (1) `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` + (2) `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente` |
| **Comportamiento** | Dimension Tipo 1 (heredada de plata y de la vista de oro) |
| **Expectativa** | Ninguna (la validacion de nulos se aplica en la vista fuente `comportamiento_atm_cliente`) |
| **Liquid Cluster** | `["huella_identificacion_cliente", "identificador_cliente"]` |
| **Total de columnas** | **22** |
| **Patron de lectura** | `spark.read.table()` para ambas fuentes (R15-D1) |
| **Patron de JOIN** | LEFT JOIN desde `clientes_saldos_consolidados` hacia `comportamiento_atm_cliente` por `identificador_cliente` |

#### TABLA B — Columnas de `resumen_integral_cliente` (22 columnas)

| # | Nombre Oro | Tipo | Fuente | Campo Fuente | Manejo de nulos | Descripcion |
|---|---|---|---|---|---|---|
|  | **Campos identificativos** | | | | | |
| 1 | `identificador_cliente` | `LongType` | clientes_saldos_consolidados | `identificador_cliente` | No aplica (PK) | Identificador unico del cliente |
| 2 | `huella_identificacion_cliente` | `StringType` | clientes_saldos_consolidados | `huella_identificacion_cliente` | No aplica | Huella digital SHA-256 del identificador (calculada en plata) |
| 3 | `nombre_completo_cliente` | `StringType` | clientes_saldos_consolidados | `nombre_completo_cliente` | No aplica | Nombre completo del cliente |
| 4 | `tipo_documento_identidad` | `StringType` | clientes_saldos_consolidados | `tipo_documento_identidad` | No aplica | Tipo de documento de identidad |
| 5 | `numero_documento_identidad` | `StringType` | clientes_saldos_consolidados | `numero_documento_identidad` | No aplica | Numero de documento de identidad |
|  | **Campos demograficos** | | | | | |
| 6 | `segmento_cliente` | `StringType` | clientes_saldos_consolidados | `segmento_cliente` | No aplica | Segmento comercial del cliente (VIP/PREM/STD/BAS) |
| 7 | `categoria_cliente` | `StringType` | clientes_saldos_consolidados | `categoria_cliente` | No aplica | Categoria asignada al cliente |
| 8 | `ciudad_residencia` | `StringType` | clientes_saldos_consolidados | `ciudad_residencia` | No aplica | Ciudad de residencia del cliente |
| 9 | `pais_residencia` | `StringType` | clientes_saldos_consolidados | `pais_residencia` | No aplica | Pais de residencia del cliente |
|  | **Campos de clasificacion de plata** | | | | | |
| 10 | `clasificacion_riesgo_cliente` | `StringType` | clientes_saldos_consolidados | `clasificacion_riesgo_cliente` | No aplica | Clasificacion del nivel de riesgo (calculada en plata con 3 campos CASE) |
| 11 | `categoria_saldo_disponible` | `StringType` | clientes_saldos_consolidados | `categoria_saldo_disponible` | No aplica | Categorizacion del saldo disponible (calculada en plata con 3 campos CASE) |
| 12 | `perfil_actividad_bancaria` | `StringType` | clientes_saldos_consolidados | `perfil_actividad_bancaria` | No aplica | Perfil de actividad bancaria (calculado en plata con 3 campos CASE) |
|  | **Campos financieros** | | | | | |
| 13 | `saldo_disponible` | `DoubleType` | clientes_saldos_consolidados | `saldo_disponible` | No aplica | Saldo disponible de la cuenta principal |
| 14 | `saldo_actual` | `DoubleType` | clientes_saldos_consolidados | `saldo_actual` | No aplica | Saldo actual de la cuenta |
| 15 | `limite_credito` | `DoubleType` | clientes_saldos_consolidados | `limite_credito` | No aplica | Limite de credito de la cuenta |
| 16 | `puntaje_crediticio` | `LongType` | clientes_saldos_consolidados | `puntaje_crediticio` | No aplica | Puntaje crediticio del cliente (rango 300-850) |
| 17 | `ingreso_anual_declarado` | `DoubleType` | clientes_saldos_consolidados | `ingreso_anual_declarado` | No aplica | Ingreso anual declarado por el cliente |
|  | **Metricas agregadas ATM y pagos al saldo** | | | | | |
| 18 | `cantidad_depositos_atm` | `LongType` | comportamiento_atm_cliente | `cantidad_depositos_atm` | `coalesce` a 0 (LEFT JOIN) | Cantidad de depositos realizados via ATM (CATM) |
| 19 | `cantidad_retiros_atm` | `LongType` | comportamiento_atm_cliente | `cantidad_retiros_atm` | `coalesce` a 0 (LEFT JOIN) | Cantidad de retiros realizados via ATM (DATM) |
| 20 | `promedio_monto_depositos_atm` | `DoubleType` | comportamiento_atm_cliente | `promedio_monto_depositos_atm` | `coalesce` a 0 (LEFT JOIN) | Promedio del monto de depositos ATM |
| 21 | `promedio_monto_retiros_atm` | `DoubleType` | comportamiento_atm_cliente | `promedio_monto_retiros_atm` | `coalesce` a 0 (LEFT JOIN) | Promedio del monto de retiros ATM |
| 22 | `total_pagos_saldo_cliente` | `DoubleType` | comportamiento_atm_cliente | `total_pagos_saldo_cliente` | `coalesce` a 0 (LEFT JOIN) | Total sumarizado de pagos al saldo |

> **Nota (RF-004):** El JOIN es LEFT JOIN desde `clientes_saldos_consolidados` (175 columnas) hacia `comportamiento_atm_cliente` (6 columnas) por `identificador_cliente`. Los 5 campos de metricas usan `coalesce(campo, lit(0))` para garantizar que clientes sin transacciones ATM/PGSL tengan metricas en 0.

> **Nota (R15-D1):** La lectura de `comportamiento_atm_cliente` usa `spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")` con el nombre completo. LSDP resuelve la dependencia DAG automaticamente ya que ambas vistas estan en el mismo archivo.

---

**Resumen de columnas — `resumen_integral_cliente`:**

| Componente | Cantidad |
|---|---|
| Campos identificativos (de plata) | 5 |
| Campos demograficos (de plata) | 4 |
| Campos de clasificacion (de plata) | 3 |
| Campos financieros (de plata) | 5 |
| Metricas agregadas ATM (de oro) | 5 |
| **Total** | **22** |

---

## 3. Parametros del Pipeline

### 3.1 Parametros existentes V3/V4 (sin cambio)

| Parametro | Tipo | Version | Descripcion |
|---|---|---|---|
| `rutaCompletaMaestroCliente` | `string` | V3 | Ruta ABFSS completa al directorio Parquet del maestro de clientes |
| `rutaCompletaSaldoCliente` | `string` | V3 | Ruta ABFSS completa al directorio Parquet de saldos de clientes |
| `rutaCompletaTransaccional` | `string` | V3 | Ruta ABFSS completa al directorio Parquet de transacciones |
| `rutaCheckpointCmstfl` | `string` | V3 | Ruta de checkpoint para streaming de cmstfl |
| `rutaCheckpointTrxpfl` | `string` | V3 | Ruta de checkpoint para streaming de trxpfl |
| `rutaCheckpointBlncfl` | `string` | V3 | Ruta de checkpoint para streaming de blncfl |
| `nombreScopeSecret` | `string` | V3 | Nombre del scope de Databricks Secrets para credenciales de Azure SQL |
| `esquema_plata` | `string` | V4 | Esquema de Unity Catalog para las vistas de plata (ej: "regional") |

### 3.2 Parametro nuevo V5

| Parametro | Tipo | Ejemplo | Descripcion |
|---|---|---|---|
| `esquema_oro` | `string` | `"regional"` | Esquema de Unity Catalog para las vistas materializadas de oro |

### 3.3 Parametros de Azure SQL (via `leer_parametros_azure_sql`)

Valores obtenidos de la tabla `dbo.Parametros` en Azure SQL:

| Clave | Ejemplo | Version | Descripcion |
|---|---|---|---|
| `catalogoBronce` | `"bronce_dev"` | V3 (existente) | Catalogo de Unity Catalog para las tablas de bronce |
| `contenedorBronce` | `"bronce"` | V3 (existente) | Contenedor de Azure Data Lake para bronce |
| `datalake` | `"stlakedev"` | V3 (existente) | Nombre de la cuenta de Azure Data Lake Storage |
| `DirectorioBronce` | `"/mnt/bronce"` | V3 (existente) | Directorio base de bronce |
| `catalogoPlata` | `"plata_dev"` | V4 (existente) | Catalogo de Unity Catalog para las vistas de plata |
| `catalogoOro` | `"oro_dev"` | V5 (existente) | Catalogo de Unity Catalog para las vistas de oro |

> **Nota:** La funcion `leer_parametros_azure_sql()` **no requiere modificacion** para V5. Ya retorna TODAS las claves desde V4 (RF-019). La clave `catalogoOro` ya existe en la tabla `dbo.Parametros` de Azure SQL — solo se lee del diccionario retornado.

---

## 4. Propiedades Delta Estandar

Segun el requisito **RF-006**, todas las vistas materializadas de oro se crean con las siguientes propiedades Delta:

```python
table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}
```

El Liquid Cluster se configura via el parametro `cluster_by` del decorador `@dp.materialized_view`:

| Vista | Liquid Cluster |
|---|---|
| `comportamiento_atm_cliente` | `["identificador_cliente"]` |
| `resumen_integral_cliente` | `["huella_identificacion_cliente", "identificador_cliente"]` |

> **Nota (RF-007):** Ambas vistas usan la funcion existente `reordenar_columnas_liquid_cluster()` de utilities/ para garantizar que las columnas del Liquid Cluster queden dentro de las primeras 32 posiciones del DataFrame.

---

## 5. Diagrama de Flujo de Datos

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONCE (V3)                                                                │
│                                                                             │
│  bronce_dev.regional.cmstfl ──┐                                             │
│  bronce_dev.regional.blncfl ──┼──→ PLATA (V4)                               │
│  bronce_dev.regional.trxpfl ──┘                                             │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  PLATA (V4)                                                                 │
│                                                                             │
│  {catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados (175 cols)    │
│       │  Dim Tipo 1 — cmstfl + blncfl + 3 CASE + 1 SHA256                  │
│       │                                                                     │
│  {catalogoPlata}.{esquema_plata}.transacciones_enriquecidas (65 cols)       │
│       │  trxpfl + 4 campos calculados numericos                             │
│       │                                                                     │
├───────┼─────────────────────────────────────────────────────────────────────┤
│  ORO (V5)                                                                   │
│       │                                                                     │
│       │   transacciones_enriquecidas ──→ groupBy(identificador_cliente)      │
│       │       count(when CATM), count(when DATM)                            │
│       │       avg(when CATM), avg(when DATM)                                │
│       │       sum(when PGSL)                                                │
│       │       @dp.expect_or_drop(identificador_cliente IS NOT NULL)          │
│       │                                                                     │
│       ▼                                                                     │
│  {catalogoOro}.{esquema_oro}.comportamiento_atm_cliente (6 cols)            │
│       │  Consumo directo — metricas ATM + pagos al saldo                    │
│       │                                                                     │
│  clientes_saldos_consolidados ──LEFT JOIN──→ comportamiento_atm_cliente     │
│       │       coalesce(metricas, 0) para clientes sin transacciones         │
│       ▼                                                                     │
│  {catalogoOro}.{esquema_oro}.resumen_integral_cliente (22 cols)             │
│       Consumo directo — perfil completo del cliente                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Configuracion dinamica:**

```text
Azure SQL dbo.Parametros ──→ leer_parametros_azure_sql() ──→ diccionario
    catalogoPlata = parametros_sql["catalogoPlata"]    (fuentes)
    catalogoOro   = parametros_sql["catalogoOro"]      (destino)

Pipeline Parameters:
    esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")  (fuentes)
    esquema_oro   = spark.conf.get("pipelines.parameters.esquema_oro")    (destino)
```
