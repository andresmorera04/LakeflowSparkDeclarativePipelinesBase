# Contrato del Pipeline LSDP — Medalla de Oro

**Feature**: 005-lsdp-medalla-oro
**Fecha**: 2026-03-31
**Tipo de Contrato**: Pipeline Lakeflow Spark Declarative Pipelines (LSDP)

---

## Descripcion General

El pipeline LSDP de la Version 5 extiende el pipeline existente de las Versiones 3 (bronce) y 4 (plata) agregando la capa de oro mediante 2 vistas materializadas. La capa de oro contiene datos altamente agregados y optimizados para consumo directo, comportandose como dimension tipo 1 (siempre datos mas recientes). El pipeline se ejecuta en Computo Serverless de Databricks y publica las vistas en Unity Catalog bajo el catalogo leido desde Azure SQL (`catalogoOro`) y el esquema recibido como parametro del pipeline (`esquema_oro`).

---

## Entradas del Pipeline

### Parametros del Pipeline LSDP

| Parametro | Tipo | Obligatorio | Ejemplo | Version | Descripcion |
|-----------|------|-------------|---------|---------|-------------|
| `nombreScopeSecret` | string | SI | `"sc-kv-laboratorio"` | V3 | Scope Secret de Databricks |
| `rutaCompletaMaestroCliente` | string | SI | `"LSDP_Base/As400/MaestroCliente/"` | V3 | Ruta relativa parquets maestro |
| `rutaCompletaSaldoCliente` | string | SI | `"LSDP_Base/As400/SaldoCliente/"` | V3 | Ruta relativa parquets saldos |
| `rutaCompletaTransaccional` | string | SI | `"LSDP_Base/As400/Transaccional/"` | V3 | Ruta relativa parquets transaccional |
| `rutaCheckpointCmstfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/cmstfl/"` | V3 | Ruta checkpoint AutoLoader cmstfl |
| `rutaCheckpointTrxpfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/trxpfl/"` | V3 | Ruta checkpoint AutoLoader trxpfl |
| `rutaCheckpointBlncfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/blncfl/"` | V3 | Ruta checkpoint AutoLoader blncfl |
| `esquema_plata` | string | SI | `"regional"` | V4 | Esquema de plata en Unity Catalog |
| `esquema_oro` | string | SI | `"regional"` | **V5 (nuevo)** | Esquema de oro en Unity Catalog |

### Parametros de Azure SQL (dbo.Parametros)

| Clave | Obligatorio | Ejemplo | Version |
|-------|-------------|---------|---------|
| `catalogoBronce` | SI | `"bronce_dev"` | V3 |
| `contenedorBronce` | SI | `"bronce"` | V3 |
| `datalake` | SI | `"adlsg2datalakedev"` | V3 |
| `DirectorioBronce` | SI | `"archivos"` | V3 |
| `catalogoPlata` | SI | `"plata_dev"` | V4 |
| `catalogoOro` | SI | `"oro_dev"` | V5 (existente) |

### Secretos de Azure Key Vault

| Secreto | Contenido |
|---------|-----------|
| `sr-jdbc-asql-asqlmetadatos-adminpd` | Cadena JDBC sin password |
| `sr-asql-asqlmetadatos-adminpd` | Password de Azure SQL |

---

## Salidas del Pipeline

### Tablas Streaming de Bronce (V3 — sin cambios)

| Tabla | Catalogo.Esquema | Tipo | Columnas |
|-------|-----------------|------|----------|
| `cmstfl` | `bronce_dev.regional` | Streaming Table (`@dp.table`) | 72 |
| `trxpfl` | `bronce_dev.regional` | Streaming Table (`@dp.table`) | 62 |
| `blncfl` | `bronce_dev.regional` | Streaming Table (`@dp.table`) | 102 |

### Vistas Materializadas de Plata (V4 — sin cambios)

| Vista | Catalogo.Esquema | Tipo | Columnas | Liquid Cluster |
|-------|-----------------|------|----------|----------------|
| `clientes_saldos_consolidados` | `{catalogoPlata}.{esquema_plata}` | Materialized View | 175 | `huella_identificacion_cliente`, `identificador_cliente` |
| `transacciones_enriquecidas` | `{catalogoPlata}.{esquema_plata}` | Materialized View | 65 | `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion` |

### Vistas Materializadas de Oro (V5 — NUEVO)

| Vista | Catalogo.Esquema | Tipo | Columnas | Liquid Cluster |
|-------|-----------------|------|----------|----------------|
| `comportamiento_atm_cliente` | `{catalogoOro}.{esquema_oro}` | Materialized View (`@dp.materialized_view`) | 6 | `identificador_cliente` |
| `resumen_integral_cliente` | `{catalogoOro}.{esquema_oro}` | Materialized View (`@dp.materialized_view`) | 22 | `huella_identificacion_cliente`, `identificador_cliente` |

---

## Propiedades Delta (todas las entidades)

```python
table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}
```

---

## Validaciones de Calidad (Expectations)

| Vista | Expectation | Tipo | Constraint | Version |
|-------|-------------|------|------------|---------|
| `clientes_saldos_consolidados` | `custid_no_nulo` | `@dp.expect_or_drop` | `"identificador_cliente IS NOT NULL"` | V4 |
| `comportamiento_atm_cliente` | `cliente_valido` | `@dp.expect_or_drop` | `"identificador_cliente IS NOT NULL"` | **V5 (nuevo)** |

> **Nota (RF-018):** La expectation en `comportamiento_atm_cliente` se evalua sobre el resultado agrupado. Si transacciones con `identificador_cliente` nulo generan un grupo NULL, la expectation lo elimina. LSDP registra las metricas de eliminacion automaticamente.

---

## Logica de Procesamiento de Oro

### Vista 1: `comportamiento_atm_cliente`

```text
ENTRADA: transacciones_enriquecidas (65 cols, plata)

PROCESO:
  1. spark.read.table("{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas")
  2. groupBy("identificador_cliente")
  3. .agg(
       count(when(tipo_transaccion == "CATM", 1))     → cantidad_depositos_atm
       count(when(tipo_transaccion == "DATM", 1))     → cantidad_retiros_atm
       coalesce(avg(when(tipo_transaccion == "CATM", coalesce(monto_transaccion, 0))), 0)
                                                       → promedio_monto_depositos_atm
       coalesce(avg(when(tipo_transaccion == "DATM", coalesce(monto_transaccion, 0))), 0)
                                                       → promedio_monto_retiros_atm
       coalesce(sum(when(tipo_transaccion == "PGSL", coalesce(monto_transaccion, 0))), 0)
                                                       → total_pagos_saldo_cliente
     )
  4. reordenar_columnas_liquid_cluster(df, ["identificador_cliente"])
  5. @dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")

SALIDA: 6 columnas, 1 fila por cliente
```

### Vista 2: `resumen_integral_cliente`

```text
ENTRADAS:
  A. clientes_saldos_consolidados (175 cols, plata)
  B. comportamiento_atm_cliente (6 cols, oro)

PROCESO:
  1. spark.read.table("{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados")
  2. spark.read.table("{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente")
  3. Seleccionar 17 columnas de A (identificativos + demograficos + clasificacion + financieros)
  4. LEFT JOIN A → B por identificador_cliente
  5. coalesce(metricas_atm, 0) para clientes sin transacciones
  6. reordenar_columnas_liquid_cluster(df, ["huella_identificacion_cliente", "identificador_cliente"])

SALIDA: 22 columnas, 1 fila por cliente
```

---

## Archivos Fuente del Pipeline

### Existentes (V3/V4 — sin cambios en V5)

| Archivo | Ubicacion | Rol |
|---------|-----------|-----|
| `LsdpConexionAzureSql.py` | `utilities/` | Lectura parametros Azure SQL (YA refactorizada en V4) |
| `LsdpConstructorRutasAbfss.py` | `utilities/` | Construccion rutas abfss:// |
| `LsdpReordenarColumnasLiquidCluster.py` | `utilities/` | Reordenar columnas para Liquid Cluster |
| `__init__.py` | `utilities/` | Init del paquete |
| `LsdpBronceCmstfl.py` | `transformations/` | Tabla streaming cmstfl |
| `LsdpBronceTrxpfl.py` | `transformations/` | Tabla streaming trxpfl |
| `LsdpBronceBlncfl.py` | `transformations/` | Tabla streaming blncfl |
| `LsdpPlataClientesSaldos.py` | `transformations/` | Vista materializada consolidada |
| `LsdpPlataTransacciones.py` | `transformations/` | Vista materializada transaccional |

### Nuevos (V5)

| Archivo | Ubicacion | Rol |
|---------|-----------|-----|
| `LsdpOroClientes.py` | `transformations/` | 2 vistas materializadas de oro (comportamiento_atm_cliente + resumen_integral_cliente) |

### Pruebas TDD (V5)

| Archivo | Ubicacion | Valida |
|---------|-----------|--------|
| `NbTestOroClientes.py` | `tests/LSDP_Laboratorio_Basico/` | Metricas agregadas, LEFT JOIN, coalesce, propiedades Delta, expectation, dimension tipo 1, casos borde |

---

## Dependencias entre Vistas

```text
bronce (V3)                  plata (V4)                   oro (V5)
───────────                  ──────────                   ────────
cmstfl ──┐
blncfl ──┼──→ clientes_saldos_consolidados ──┐
         │                                    ├──→ resumen_integral_cliente
trxpfl ──┴──→ transacciones_enriquecidas ──→ comportamiento_atm_cliente ──┘
```

LSDP resuelve el DAG automaticamente. La dependencia `comportamiento_atm_cliente` → `resumen_integral_cliente` se resuelve porque ambas vistas estan en el mismo archivo `LsdpOroClientes.py` y la lectura via `spark.read.table()` permite a LSDP detectar la dependencia.

---

## Compatibilidad

| Requisito | Estado |
|-----------|--------|
| Computo Serverless | Obligatorio — sin spark.sparkContext |
| Unity Catalog | Obligatorio — catalogos bronce_dev, plata_dev, oro_dev |
| Databricks Plan Premium | Obligatorio — requerido por LSDP |
| Pipeline LSDP unico | Bronce + Plata + Oro en el mismo pipeline |
| Backward Compatibility V3/V4 | Scripts de bronce y plata NO se modifican |
| Nuevas utilidades | NO se requieren — se reutilizan las existentes |
