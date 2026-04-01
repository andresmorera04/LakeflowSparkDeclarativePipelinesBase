# Plan de Implementacion: LSDP Medalla de Oro (Version 5)

**Branch**: `005-lsdp-medalla-oro` | **Fecha**: 2026-03-31 | **Spec**: [spec.md](spec.md)
**Input**: Incrementar el pipeline LSDP existente (V3 bronce + V4 plata) agregando la capa de oro mediante 2 vistas materializadas con datos agregados y optimizados para consumo.

## Resumen

La Version 5 extiende el pipeline LSDP de bronce (V3) y plata (V4) con 2 vistas materializadas en la medalla de oro: (1) `comportamiento_atm_cliente` — metricas agregadas de comportamiento ATM y pagos al saldo por cliente usando agregaciones condicionales (`count(when(...))`, `sum(when(...))`) con `@dp.expect_or_drop` para `identificador_cliente` nulo, y (2) `resumen_integral_cliente` — perfil completo del cliente combinando datos dimensionales de plata con metricas transaccionales de la vista de oro via LEFT JOIN con coalesce a 0. Ambas vistas en un unico archivo `LsdpOroClientes.py` con closure compartida. El catalogo de oro se lee desde `dbo.Parametros` de Azure SQL (clave `catalogoOro`) y el esquema se recibe como parametro del pipeline (`esquema_oro`). La funcion `leer_parametros_azure_sql` se reutiliza sin modificacion (ya retorna TODAS las claves desde V4).

## Contexto Tecnico

**Lenguaje/Version**: PySpark/Python (Databricks Runtime con Apache Spark 4.1+)
**Dependencias Principales**: `pyspark.pipelines as dp` (LSDP), `pyspark.sql.functions` (col, sum, count, when, avg, coalesce, lit)
**Almacenamiento**: Unity Catalog (catalogos: bronce_dev, plata_dev, oro_dev) + Azure SQL dbo.Parametros + ADLS Gen2 via abfss://
**Testing**: Notebooks de prueba TDD en Databricks (pyspark.pipelines solo disponible en contexto de pipeline)
**Plataforma**: Azure Databricks, Computo Serverless, Plan Premium
**Tipo de Proyecto**: Pipeline ETL declarativo (Lakeflow Spark Declarative Pipelines)
**Metas de Rendimiento**: Procesamiento eficiente con agregaciones condicionales en una sola pasada sobre 15M+ transacciones y 5M+ clientes
**Restricciones**: Cero hardcoded (Principio I), sin spark.sparkContext (Serverless), sin import dlt (Principio V), sin filtros previos a la agrupacion (RF-017)
**Escala/Alcance**: 2 vistas fuente plata (175 + 65 cols) → 2 vistas oro (6 + ~20 cols), 18 RFs, 13 CEs

## Constitution Check

*GATE: Debe pasar antes de la Fase 0 de research. Re-verificar despues de la Fase 1 de diseno.*

### Verificacion Pre-Research (Fase 0)

| # | Principio | Verificacion | Estado |
|---|-----------|-------------|--------|
| I | Dinamismo Absoluto | Catalogo de oro desde Azure SQL (clave `catalogoOro`), catalogo de plata desde Azure SQL (clave `catalogoPlata`), esquema de oro via parametro pipeline (`esquema_oro`), esquema de plata via parametro pipeline (`esquema_plata`), scope secret via parametro. Cero valores hardcoded. | CUMPLE |
| II | Entorno Desarrollo Databricks | Extensiones VS Code confirmadas en V1 (R4-D1), V3 (R8-D1) y V4 (R12). Sin cambios para V5. | CUMPLE |
| III | Desarrollo Guiado por Pruebas | Suite TDD planificada: notebook de prueba para las vistas materializadas de oro (metricas agregadas, propiedades Delta, casos borde). | CUMPLE |
| IV | Simulacion Datos AS400 | No aplica directamente a V5 — las tablas de bronce y vistas de plata ya contienen datos simulados de V2/V3/V4. | N/A |
| V | LSDP Estricto | `from pyspark import pipelines as dp`, `@dp.materialized_view`. Prohibido `import dlt`. Compatible con Serverless. Sin spark.sparkContext. | CUMPLE |
| VI | Arquitectura Medallon | Oro usa vistas materializadas con datos altamente agregados para consumo (Dimension Tipo 1). Hereda datos mas recientes de plata. Metricas de conteo, promedio y suma optimizadas. | CUMPLE |
| VII | Estructura Proyecto LSDP | Nuevo script en `transformations/` (`LsdpOroClientes.py`), reutilizacion de `utilities/` existentes sin modificacion, nuevas pruebas en `tests/LSDP_Laboratorio_Basico/`. | CUMPLE |
| VIII | Gobernanza Repositorio | Speckit no crea branches ni commits. Control exclusivo del usuario. | CUMPLE |
| IX | Gobernanza IA | Research obligatorio planificado (R13-R15). Las decisiones de arquitectura de la clarificacion fueron aprobadas por el usuario (5 clarificaciones respondidas). | CUMPLE |
| X | Idioma y Comunicacion | Fuentes oficiales Azure Databricks consultadas. Respuestas en espanol. Links oficiales referenciados. | CUMPLE |

**Resultado Pre-Research**: GATE APROBADO — Sin violaciones.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/005-lsdp-medalla-oro/
├── plan.md                          # Este archivo
├── spec.md                          # Especificacion (18 RFs, 13 CEs, 5 clarificaciones)
├── research.md                      # Fase 0: Research V5 (R13-R15)
├── data-model.md                    # Fase 1: Modelo de datos de las vistas de oro
├── quickstart.md                    # Fase 1: Guia rapida de implementacion
├── contracts/
│   └── pipeline-lsdp-oro.md         # Fase 1: Contrato del pipeline (entradas/salidas)
├── checklists/
│   └── requirements.md              # Checklist de requerimientos
└── tasks.md                         # Fase 2: Tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── __init__.py                                  # Sin cambio
│   ├── LsdpConexionAzureSql.py                      # Sin cambio (ya retorna ALL keys desde V4)
│   ├── LsdpConstructorRutasAbfss.py                 # Sin cambio
│   └── LsdpReordenarColumnasLiquidCluster.py        # Sin cambio (reutilizado por oro)
└── transformations/
    ├── LsdpBronceCmstfl.py                          # Sin cambio (V3)
    ├── LsdpBronceTrxpfl.py                          # Sin cambio (V3)
    ├── LsdpBronceBlncfl.py                          # Sin cambio (V3)
    ├── LsdpPlataClientesSaldos.py                   # Sin cambio (V4)
    ├── LsdpPlataTransacciones.py                    # Sin cambio (V4)
    └── LsdpOroClientes.py                           # NUEVO — 2 vistas materializadas de oro

tests/LSDP_Laboratorio_Basico/
├── NbTestBronceCmstfl.py                            # Sin cambio (V3)
├── NbTestBronceTrxpfl.py                            # Sin cambio (V3)
├── NbTestBronceBlncfl.py                            # Sin cambio (V3)
├── NbTestConexionAzureSql.py                        # Sin cambio (V3)
├── NbTestConstructorRutasAbfss.py                   # Sin cambio (V3)
├── NbTestReordenarColumnasLC.py                     # Sin cambio (V3)
├── NbTestPlataClientesSaldos.py                     # Sin cambio (V4)
├── NbTestPlataTransacciones.py                      # Sin cambio (V4)
├── NbTestConexionAzureSqlV4.py                      # Sin cambio (V4)
└── NbTestOroClientes.py                             # NUEVO — Pruebas de las 2 vistas de oro
```

**Decision de Estructura**: Se extiende el proyecto existente `LSDP_Laboratorio_Basico` siguiendo la estructura LSDP (Principio VII): 1 nueva transformacion en `transformations/` (`LsdpOroClientes.py` con 2 vistas materializadas y closure compartida), reutilizacion de `utilities/` existentes sin modificacion, 1 nuevo notebook de pruebas en `tests/LSDP_Laboratorio_Basico/`.

### Verificacion Post-Design (Fase 1)

| # | Principio | Verificacion | Estado |
|---|-----------|-------------|--------|
| I | Dinamismo Absoluto | **data-model.md**: `catalogoOro` desde dbo.Parametros (Azure SQL), `esquema_oro` desde parametro pipeline. **contracts/**: Confirma 0 valores hardcodeados. **quickstart.md**: Verificacion de clave `catalogoOro` existente como prerequisito. | CUMPLE |
| II | Entorno Desarrollo Databricks | **quickstart.md**: Extensiones VS Code y SQLTools confirmadas como prerrequisitos. Sin cambios respecto a V3/V4. | CUMPLE |
| III | Desarrollo Guiado por Pruebas | **quickstart.md**: Paso 3 define `NbTestOroClientes.py` con cobertura de metricas, propiedades Delta, casos borde, expectation, LEFT JOIN. **contracts/**: Documenta 1 archivo de prueba. | CUMPLE |
| IV | Simulacion Datos AS400 | N/A — Datos simulados ya disponibles desde V2/V3/V4 en bronce y plata. | N/A |
| V | LSDP Estricto | **data-model.md**: `@dp.materialized_view`, `@dp.expect_or_drop`. **contracts/**: Logica de procesamiento usa `groupBy().agg()` y `spark.read.table()` dentro de funciones decoradas. Sin `import dlt`. | CUMPLE |
| VI | Arquitectura Medallon | **data-model.md**: 2 vistas materializadas oro con datos altamente agregados (6 y 22 cols vs 175+65 en plata). Dimension Tipo 1. **contracts/**: Diagrama de dependencias bronce→plata→oro. | CUMPLE |
| VII | Estructura Proyecto LSDP | **quickstart.md**: `LsdpOroClientes.py` en `transformations/`, `NbTestOroClientes.py` en `tests/LSDP_Laboratorio_Basico/`. Utilities sin cambio. Prefijo `Lsdp` y `Nb` respetado. | CUMPLE |
| VIII | Gobernanza Repositorio | Ningun artefacto de Fase 1 crea branches ni commits. Control del usuario. | CUMPLE |
| IX | Gobernanza IA | **research.md**: 3 decisiones aprobadas por el usuario (R13-D1, R14-D1, R15-D1). **data-model.md**: Referencia decisiones aprobadas en cada tabla. | CUMPLE |
| X | Idioma y Comunicacion | Todos los artefactos en espanol. Columnas en espanol y snake_case. Fuentes oficiales Azure Databricks referenciadas en research.md. | CUMPLE |

**Resultado Post-Design**: GATE APROBADO — Sin violaciones. El diseno cumple todos los principios de la Constitucion v1.0.1.

## Complexity Tracking

> Sin violaciones de Constitution Check (Pre-Research ni Post-Design). No se requiere justificacion.
