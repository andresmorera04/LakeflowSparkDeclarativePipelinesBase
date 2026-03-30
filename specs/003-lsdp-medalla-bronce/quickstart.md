# Guia Rapida - Version 3: Pipeline LSDP Medalla de Bronce

**Feature**: 003-lsdp-medalla-bronce
**Fecha**: 2026-03-29
**Estado**: IMPLEMENTADA Y VERIFICADA — 3/3 decisiones V3 APROBADAS + 13/15 decisiones V1 vigentes. Suite TDD ejecutada exitosamente en Computo Serverless (2026-03-30). 6/6 notebooks de prueba, 30/30 pruebas PASADAS al 100%.
**Base**: Research V1 (15/15 decisiones aprobadas) + Research V3 (3/3 decisiones aprobadas)

---

## Objetivo de esta Version

La Version 3 crea un pipeline Lakeflow Spark Declarative Pipelines (LSDP) que ingesta los 3 parquets generados en V2 hacia 3 streaming tables Delta en la capa Bronce usando AutoLoader. Las tablas conservan los nombres originales de AS400 (cmstfl, trxpfl, blncfl) y aplican Liquid Cluster, Change Data Feed y propiedades de optimizacion automatica.

## Artefactos Generados

| Artefacto | Ruta | Descripcion |
|-----------|------|-------------|
| Especificacion | [spec.md](spec.md) | 6 historias de usuario, 17 requerimientos funcionales, 9 criterios de exito |
| Plan de Implementacion | [plan.md](plan.md) | Contexto tecnico, verificacion de constitucion, estructura del proyecto |
| Research V3 | [research.md](research.md) | 3 areas investigadas, 3 decisiones nuevas + 13 heredadas de V1 |
| Modelo de Datos | [data-model.md](data-model.md) | 3 streaming tables, 7 parametros del pipeline, esquemas completos |
| Contrato del Pipeline | [contracts/pipeline-lsdp-bronce.md](contracts/pipeline-lsdp-bronce.md) | Interfaz completa del pipeline con entradas, salidas y dependencias |
| Checklist | [checklists/requirements.md](checklists/requirements.md) | Validacion de calidad de la especificacion — 12/12 items PASS |

## Restricciones Criticas de Plataforma

### Computo Serverless — APIs Prohibidas (V2-R5-D1, vigente)

| API Prohibida | Alternativa |
|---------------|-------------|
| `spark.sparkContext.*` | Variables Python capturadas por closure (cloudpickle) |
| `import dlt` | `from pyspark import pipelines as dp` |
| `/mnt/` (DBFS mounts) | `abfss://` exclusivamente |
| `ZOrder` / `PartitionBy` | `cluster_by` (Liquid Cluster) — columnas deben estar entre las primeras 32 de la tabla |
| `pivot()` | No aplica en bronce |

### LSDP — Cambios de API Relevantes (R6)

| Cambio | Detalle |
|--------|---------|
| `temporary` → `private` | Nuevo parametro para tablas no publicadas (H6.2) |
| `cluster_by_auto` | Nuevo parametro disponible — NO usado en V3 (R6-D1: cluster_by explicito) |
| `schema` en `@dp.table` | Disponible pero NO usado en bronce (R7-D2: inferencia automatica) |

### Restriccion Critica: Liquid Cluster y Estadisticas de Columnas

**Las columnas que forman parte del Liquid Cluster DEBEN estar ubicadas entre las primeras 32 columnas de la tabla Delta.** Delta Lake recopila estadisticas (min/max) unicamente para las primeras 32 columnas por defecto, y Liquid Clustering requiere estadisticas en sus columnas de clustering. Si una columna de clustering queda fuera de las primeras 32, Delta lanza el error `DELTA_CLUSTERING_COLUMN_MISSING_STATS`.

**Solucion implementada**: La funcion de utilidad `reordenar_columnas_liquid_cluster(df, columnas_liquid_cluster)` reordena dinamicamente las columnas del DataFrame colocando las columnas de clustering al inicio. Esta funcion se invoca en cada `@dp.table` antes de retornar el DataFrame.

## Prerrequisitos

### Infraestructura Requerida

1. **Azure Databricks** con Unity Catalog habilitado y Tier Premium
2. **Computo Serverless** activo y disponible
3. **External Location** configurado en Unity Catalog para lectura de parquets y escritura de checkpoints
4. **Parquets generados por V2** en ADLS Gen2 (3 directorios: MaestroCliente, Transaccional, SaldoCliente)
5. **Azure SQL Database** (asqlmetadatos) con tabla dbo.Parametros poblada con valores de contenedor, datalake y directorio raiz
6. **Azure Key Vault** con los secretos configurados:
   - `sr-jdbc-asql-asqlmetadatos-adminpd` — cadena JDBC sin password
   - `sr-asql-asqlmetadatos-adminpd` — contrasena
7. **Scope Secret de Databricks** vinculado al Key Vault (ej: `sc-kv-laboratorio`)
8. **Extensiones VS Code** instaladas:
   - Databricks extension for Visual Studio Code (`databricks.databricks`)
   - Databricks Driver for SQLTools (`databricks.sqltools-databricks-driver`)

### Parquets de Entrada (generados por V2)

| Parquet | Columnas | Registros Estimados | Directorio Ejemplo |
|---------|----------|--------------------|--------------------|
| Maestro de Clientes | 70 | 5,000,000+ | LSDP_Base/As400/MaestroCliente/ |
| Transaccional | 60 | 15,000,000+ | LSDP_Base/As400/Transaccional/ |
| Saldos | 100 | 5,000,000+ | LSDP_Base/As400/SaldoCliente/ |

## Estructura del Proyecto V3

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── LsdpConexionAzureSql.py       # Lectura de parametros desde Azure SQL
│   ├── LsdpConstructorRutasAbfss.py   # Construccion de rutas abfss://
│   └── LsdpReordenarColumnasLiquidCluster.py  # Reordenamiento de columnas para Liquid Cluster
└── transformations/
    ├── LsdpBronceCmstfl.py            # @dp.table streaming - Maestro Clientes
    ├── LsdpBronceTrxpfl.py            # @dp.table streaming - Transaccional
    └── LsdpBronceBlncfl.py            # @dp.table streaming - Saldos

tests/LSDP_Laboratorio_Basico/
├── NbTestConexionAzureSql.py          # Pruebas conexion
├── NbTestConstructorRutasAbfss.py     # Pruebas constructor
├── NbTestReordenarColumnasLC.py       # Pruebas reordenamiento columnas LC
├── NbTestBronceCmstfl.py              # Pruebas Maestro Clientes
├── NbTestBronceTrxpfl.py              # Pruebas Transaccional
└── NbTestBronceBlncfl.py              # Pruebas Saldos
```

## Configuracion del Pipeline LSDP

### Paso 1: Crear el Pipeline en Databricks

1. En el workspace de Databricks, ir a **Jobs & Pipelines** → **ETL Pipeline**
2. Configurar:
   - **Pipeline name**: `lsdp-bronce-regional` (o nombre deseado)
   - **Product edition**: Advanced (requerido para Change Data Feed)
   - **Pipeline mode**: Triggered (o Continuous segun necesidad)
   - **Destination**: 
     - Default catalog: `bronce_dev`
     - Default schema: `regional`
   - **Compute**: Serverless
   - **Source code**: Agregar los archivos del directorio `src/LSDP_Laboratorio_Basico/` (utilities + transformations)

### Paso 2: Configurar los 7 Parametros del Pipeline

En la seccion **Configuration** del pipeline, agregar:

| Clave | Valor Ejemplo | Descripcion |
|-------|---------------|-------------|
| `rutaCompletaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa a parquets del Maestro |
| `rutaCompletaSaldoCliente` | `LSDP_Base/As400/SaldoCliente/` | Ruta relativa a parquets de Saldos |
| `rutaCompletaTransaccional` | `LSDP_Base/As400/Transaccional/` | Ruta relativa a parquets Transaccionales |
| `rutaCheckpointCmstfl` | `LSDP_Base/Checkpoints/Bronce/cmstfl/` | Ruta relativa para schemaLocation de cmstfl |
| `rutaCheckpointTrxpfl` | `LSDP_Base/Checkpoints/Bronce/trxpfl/` | Ruta relativa para schemaLocation de trxpfl |
| `rutaCheckpointBlncfl` | `LSDP_Base/Checkpoints/Bronce/blncfl/` | Ruta relativa para schemaLocation de blncfl |
| `nombreScopeSecret` | `sc-kv-laboratorio` | Scope Secret de Databricks para acceder al Key Vault |

### Paso 3: Ejecutar el Pipeline

1. Click en **Start** para ejecutar el pipeline
2. El pipeline ejecutara en este orden:
   - **Nivel de modulo**: Lectura de dbo.Parametros via JDBC (una sola vez)
   - **Nivel de modulo**: Construccion de las 6 rutas abfss:// (3 parquets + 3 checkpoints)
   - **@dp.table cmstfl**: AutoLoader ingesta parquets del Maestro → streaming table
   - **@dp.table trxpfl**: AutoLoader ingesta parquets Transaccionales → streaming table
   - **@dp.table blncfl**: AutoLoader ingesta parquets de Saldos → streaming table

### Paso 4: Verificar Resultados

1. En la interfaz del pipeline, verificar que las 3 tablas aparecen como **Streaming Tables** en estado verde
2. Verificar en Unity Catalog que existen:
   - `bronce_dev.regional.cmstfl` — columnas originales AS400 + FechaIngestaDatos + _rescued_data (inicialmente ~72)
   - `bronce_dev.regional.trxpfl` — columnas originales AS400 + FechaIngestaDatos + _rescued_data (inicialmente ~62)
   - `bronce_dev.regional.blncfl` — columnas originales AS400 + FechaIngestaDatos + _rescued_data (inicialmente ~102)
   > **Nota**: El conteo exacto de columnas puede variar debido a schema evolution (`cloudFiles.schemaEvolutionMode=addNewColumns`). Las pruebas TDD no validan conteos exactos.
3. Verificar propiedades Delta en cada tabla:
   - Change Data Feed: habilitado
   - Liquid Cluster: campos configurados segun R6-D1
   - autoOptimize: autoCompact y optimizeWrite habilitados

## Ejecucion de Pruebas TDD

1. Los notebooks de prueba estan en `tests/LSDP_Laboratorio_Basico/`
2. Ejecutar cada notebook en Databricks con **Computo Serverless**:
   - `NbTestConexionAzureSql.py` — valida lectura de parametros, manejo de secretos
   - `NbTestConstructorRutasAbfss.py` — valida construccion de rutas abfss://
   - `NbTestReordenarColumnasLC.py` — valida reordenamiento de columnas del Liquid Cluster
   - `NbTestBronceCmstfl.py` — valida existencia, columnas clave, Liquid Cluster, propiedades Delta
   - `NbTestBronceTrxpfl.py` — valida existencia, columnas clave, Liquid Cluster, propiedades Delta
   - `NbTestBronceBlncfl.py` — valida existencia, columnas clave, Liquid Cluster, propiedades Delta
3. Todas las pruebas deben pasar exitosamente

## Validaciones Clave

| Validacion | Que Verifica | Tabla/Componente |
|------------|-------------|------------------|
| Tabla cmstfl existe | Tabla creada en Unity Catalog con columnas clave (FechaIngestaDatos, _rescued_data) | cmstfl |
| Tabla trxpfl existe | Tabla creada en Unity Catalog con columnas clave (FechaIngestaDatos, _rescued_data) | trxpfl |
| Tabla blncfl existe | Tabla creada en Unity Catalog con columnas clave (FechaIngestaDatos, _rescued_data) | blncfl |
| Liquid Cluster cmstfl | FechaIngestaDatos, CUSTID (reordenadas al inicio del schema) | cmstfl |
| Liquid Cluster trxpfl | TRXDT, CUSTID, TRXTYP (reordenadas al inicio del schema) | trxpfl |
| Liquid Cluster blncfl | FechaIngestaDatos, CUSTID (reordenadas al inicio del schema) | blncfl |
| Change Data Feed | delta.enableChangeDataFeed = true | Todas |
| AutoOptimize | autoCompact + optimizeWrite habilitados | Todas |
| Retenciones Delta | 30 dias deleted files, 60 dias log | Todas |
| Conexion Azure SQL | Lectura exitosa de dbo.Parametros con 2 secretos | Utilidad conexion |
| Rutas abfss:// | Formato correcto, sin /mnt/ | Utilidad constructor |
| Compatibilidad Serverless | Sin sparkContext, sin import dlt | Todo el pipeline |
| FechaIngestaDatos | Columna de auditoria con current_timestamp() | Todas |
| _rescued_data | Columna de datos no mapeados presente | Todas |
| schemaLocation | Checkpoint configurado con ruta abfss:// del parametro | Todas |

## Dependencias con Versiones Futuras

| Version | Que Recibe de V3 | Como lo Usa |
|---------|------------------|-------------|
| V4 | 3 streaming tables Delta en bronce_dev.regional | Vistas materializadas en Plata con campos calculados |
| V5 | Vistas de Plata (via V4) | Agregaciones en Oro por cliente |

## Resultados de Pruebas TDD (2026-03-30)

> **Plataforma**: Azure Databricks con Computo Serverless
> **Resultado global**: 30/30 pruebas PASADAS al 100%

### NbTestBronceCmstfl — 6/6 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Tabla cmstfl existe en Unity Catalog | PASA |
| FechaIngestaDatos tipo timestamp | PASA |
| _rescued_data tipo string | PASA |
| Propiedades Delta correctas | PASA |
| Liquid Cluster [FechaIngestaDatos, CUSTID] | PASA |
| Datos ingestados (conteo > 0) | PASA |

### NbTestBronceTrxpfl — 6/6 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Tabla trxpfl existe en Unity Catalog | PASA |
| FechaIngestaDatos tipo timestamp | PASA |
| _rescued_data tipo string | PASA |
| Propiedades Delta correctas | PASA |
| Liquid Cluster [TRXDT, CUSTID, TRXTYP] | PASA |
| Datos ingestados (conteo > 0) | PASA |

### NbTestBronceBlncfl — 6/6 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Tabla blncfl existe en Unity Catalog | PASA |
| FechaIngestaDatos tipo timestamp | PASA |
| _rescued_data tipo string | PASA |
| Propiedades Delta correctas | PASA |
| Liquid Cluster [FechaIngestaDatos, CUSTID] | PASA |
| Datos ingestados (conteo > 0) | PASA |

### NbTestConexionAzureSql — 4/4 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Conexion JDBC exitosa | PASA |
| Retorna las 4 claves esperadas | PASA |
| Valores no vacios para cada clave | PASA |
| Error con Scope Secret invalido | PASA |

### NbTestConstructorRutasAbfss — 4/4 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Formato abfss:// correcto | PASA |
| Sin /mnt/ en ninguna ruta | PASA |
| Componentes correctamente posicionados | PASA |
| Funciona para parquets y checkpoints | PASA |

### NbTestReordenarColumnasLC — 5/5 PASADAS

| Prueba | Resultado |
|--------|-----------|
| Columnas LC al inicio del DataFrame | PASA |
| Resto de columnas mantienen orden original | PASA |
| Sin perdidas ni duplicados de columnas | PASA |
| Error descriptivo para columna inexistente | PASA |
| Funciona con 3 columnas LC (patron trxpfl) | PASA |

### Resumen de Cobertura

| Criterio de Exito | Verificado Por | Estado |
|--------------------|---------------|--------|
| CE-001: Tabla cmstfl creada con FechaIngestaDatos | NbTestBronceCmstfl | ✓ |
| CE-002: Tabla trxpfl creada con FechaIngestaDatos | NbTestBronceTrxpfl | ✓ |
| CE-003: Tabla blncfl creada con FechaIngestaDatos | NbTestBronceBlncfl | ✓ |
| CE-004: Liquid Cluster configurado correctamente | Las 3 suites de bronce | ✓ |
| CE-005: Change Data Feed habilitado | Las 3 suites de bronce | ✓ |
| CE-006: AutoOptimize habilitado | Las 3 suites de bronce | ✓ |
| CE-007: Conexion Azure SQL funcional | NbTestConexionAzureSql | ✓ |
| CE-008: 100% pruebas TDD pasan | 30/30 pruebas PASADAS | ✓ |
| CE-009: Compatible con Computo Serverless | Ejecucion exitosa en Serverless | ✓ |

> **Nota**: La validacion de `spark.sparkContext` e `import dlt` (compatibilidad Serverless) se realiza mediante revision de codigo, no mediante prueba TDD automatizada, ya que su uso produce un error directo en tiempo de ejecucion (`JVM_ATTRIBUTE_NOT_SUPPORTED`).
