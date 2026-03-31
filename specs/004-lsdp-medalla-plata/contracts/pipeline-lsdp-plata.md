# Contrato del Pipeline LSDP — Medalla de Plata

**Feature**: 004-lsdp-medalla-plata
**Fecha**: 2026-03-30
**Tipo de Contrato**: Pipeline Lakeflow Spark Declarative Pipelines (LSDP)

---

## Descripcion General

El pipeline LSDP de la Version 4 extiende el pipeline existente de la Version 3 (bronce) agregando la capa de plata mediante 2 vistas materializadas. El pipeline se ejecuta en Computo Serverless de Databricks y publica las vistas en Unity Catalog.

---

## Entradas del Pipeline

### Parametros del Pipeline LSDP

| Parametro | Tipo | Obligatorio | Ejemplo | Descripcion |
|-----------|------|-------------|---------|-------------|
| `nombreScopeSecret` | string | SI | `"sc-kv-laboratorio"` | Scope Secret de Databricks vinculado a Azure Key Vault (V3) |
| `rutaCompletaMaestroCliente` | string | SI | `"LSDP_Base/As400/MaestroCliente/"` | Ruta relativa parquets maestro (V3) |
| `rutaCompletaSaldoCliente` | string | SI | `"LSDP_Base/As400/SaldoCliente/"` | Ruta relativa parquets saldos (V3) |
| `rutaCompletaTransaccional` | string | SI | `"LSDP_Base/As400/Transaccional/"` | Ruta relativa parquets transaccional (V3) |
| `rutaCheckpointCmstfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/cmstfl/"` | Ruta checkpoint AutoLoader cmstfl (V3) |
| `rutaCheckpointTrxpfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/trxpfl/"` | Ruta checkpoint AutoLoader trxpfl (V3) |
| `rutaCheckpointBlncfl` | string | SI | `"LSDP_Base/Checkpoints/Bronce/blncfl/"` | Ruta checkpoint AutoLoader blncfl (V3) |
| `esquema_plata` | string | SI | `"regional"` | **NUEVO V4** — Esquema de Unity Catalog para las vistas de plata |

### Parametros de Azure SQL (dbo.Parametros)

| Clave | Obligatorio | Ejemplo | Version |
|-------|-------------|---------|---------|
| `catalogoBronce` | SI | `"bronce_dev"` | V3 |
| `contenedorBronce` | SI | `"bronce"` | V3 |
| `datalake` | SI | `"adlsg2datalakedev"` | V3 |
| `DirectorioBronce` | SI | `"archivos"` | V3 |
| `catalogoPlata` | SI | `"plata_dev"` | **NUEVO V4** |

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

### Vistas Materializadas de Plata (V4 — NUEVO)

| Vista | Catalogo.Esquema | Tipo | Columnas | Liquid Cluster |
|-------|-----------------|------|----------|----------------|
| `clientes_saldos_consolidados` | `{catalogoPlata}.{esquema_plata}` | Materialized View (`@dp.materialized_view`) | 175 | `huella_identificacion_cliente`, `identificador_cliente` |
| `transacciones_enriquecidas` | `{catalogoPlata}.{esquema_plata}` | Materialized View (`@dp.materialized_view`) | 65 | `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion` |

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

| Vista | Expectation | Tipo | Constraint |
|-------|-------------|------|------------|
| `clientes_saldos_consolidados` | `custid_no_nulo` | `@dp.expect_or_drop` | `"identificador_cliente IS NOT NULL"` |

---

## Archivos Fuente del Pipeline

### Existentes (V3 — sin cambio excepto RF-019)

| Archivo | Ubicacion | Rol |
|---------|-----------|-----|
| `LsdpConexionAzureSql.py` | `utilities/` | Lectura parametros Azure SQL (refactorizar: retornar TODAS las claves) |
| `LsdpConstructorRutasAbfss.py` | `utilities/` | Construccion rutas abfss:// |
| `LsdpReordenarColumnasLiquidCluster.py` | `utilities/` | Reordenar columnas para Liquid Cluster |
| `__init__.py` | `utilities/` | Init del paquete |
| `LsdpBronceCmstfl.py` | `transformations/` | Tabla streaming cmstfl |
| `LsdpBronceTrxpfl.py` | `transformations/` | Tabla streaming trxpfl |
| `LsdpBronceBlncfl.py` | `transformations/` | Tabla streaming blncfl |

### Nuevos (V4)

| Archivo | Ubicacion | Rol |
|---------|-----------|-----|
| `LsdpPlataClientesSaldos.py` | `transformations/` | Vista materializada clientes_saldos_consolidados |
| `LsdpPlataTransacciones.py` | `transformations/` | Vista materializada transacciones_enriquecidas |

### Pruebas TDD (V4)

| Archivo | Ubicacion | Valida |
|---------|-----------|--------|
| `NbTestPlataClientesSaldos.py` | `tests/LSDP_Laboratorio_Basico/` | Vista consolidada, campos calculados CASE, SHA256, dimension tipo 1 |
| `NbTestPlataTransacciones.py` | `tests/LSDP_Laboratorio_Basico/` | Vista transaccional, campos calculados numericos, manejo de nulos |
| `NbTestConexionAzureSqlV4.py` | `tests/LSDP_Laboratorio_Basico/` | Refactorizacion leer_parametros_azure_sql (sin filtro, incluye catalogoPlata) |

---

## Compatibilidad

| Requisito | Estado |
|-----------|--------|
| Computo Serverless | Obligatorio — sin spark.sparkContext |
| Unity Catalog | Obligatorio — catalogos bronce_dev y plata_dev |
| Databricks Plan Premium | Obligatorio — requerido por LSDP |
| Pipeline LSDP unico | Bronce + Plata en el mismo pipeline |
| Backward Compatibility V3 | Scripts de bronce NO se modifican (excepto RF-019 en utilities) |
