# Plan de Implementacion: LSDP Medalla de Plata (Version 4)

**Branch**: `004-lsdp-medalla-plata` | **Fecha**: 2026-03-30 | **Spec**: [spec.md](spec.md)
**Input**: Incrementar el pipeline LSDP existente (V3 bronce) agregando la capa de plata mediante 2 vistas materializadas con campos calculados, lectura dinamica de catalogo desde Azure SQL y pruebas TDD.

## Resumen

La Version 4 extiende el pipeline LSDP de bronce (V3) con 2 vistas materializadas en la medalla de plata: (1) `clientes_saldos_consolidados` — consolidacion de cmstfl + blncfl como Dimension Tipo 1 con 3 campos CASE + 1 SHA256 + validacion `@dp.expect_or_drop`, y (2) `transacciones_enriquecidas` — trxpfl con 4 campos calculados numericos. El catalogo de plata se lee desde `dbo.Parametros` de Azure SQL (clave `catalogoPlata`) y el esquema se recibe como parametro del pipeline (`esquema_plata`). La funcion `leer_parametros_azure_sql` se refactoriza para retornar TODAS las claves sin filtro (RF-019). Todas las columnas de bronce se renombran a espanol (excepto año/mes/dia del lazy evaluation).

## Contexto Tecnico

**Lenguaje/Version**: PySpark/Python (Databricks Runtime con Apache Spark 4.1+)
**Dependencias Principales**: `pyspark.pipelines as dp` (LSDP), `pyspark.sql.functions`, `pyspark.sql.window`
**Almacenamiento**: Unity Catalog (catalogos: bronce_dev, plata_dev) + Azure SQL dbo.Parametros + ADLS Gen2 via abfss://
**Testing**: Notebooks de prueba TDD en Databricks (pyspark.pipelines solo disponible en contexto de pipeline)
**Plataforma**: Azure Databricks, Computo Serverless, Plan Premium
**Tipo de Proyecto**: Pipeline ETL declarativo (Lakeflow Spark Declarative Pipelines)
**Metas de Rendimiento**: Procesamiento eficiente de 5M+ registros por tabla fuente con window functions optimizadas
**Restricciones**: Cero hardcoded (Principio I), sin spark.sparkContext (Serverless), sin import dlt (Principio V)
**Escala/Alcance**: 3 tablas fuente bronce (72 + 62 + 102 cols) → 2 vistas plata (175 + 65 cols), 20 RFs, 13 CEs

## Constitution Check

*GATE: Debe pasar antes de la Fase 0 de research. Re-verificar despues de la Fase 1 de diseno.*

### Verificacion Pre-Research (Fase 0)

| # | Principio | Verificacion | Estado |
|---|-----------|-------------|--------|
| I | Dinamismo Absoluto | Catalogo de plata desde Azure SQL (clave `catalogoPlata`), esquema via parametro pipeline (`esquema_plata`), scope secret via parametro. Cero valores hardcoded. | CUMPLE |
| II | Entorno Desarrollo Databricks | Extensiones VS Code confirmadas en R4-D1 y R8-D1. Sin cambios para V4 (R12). | CUMPLE |
| III | Desarrollo Guiado por Pruebas | Suite TDD planificada: 3 notebooks de prueba (vista consolidada, vista transaccional, refactorizacion Azure SQL). | CUMPLE |
| IV | Simulacion Datos AS400 | No aplica directamente a V4 — las tablas de bronce ya contienen datos simulados de V2/V3. | N/A |
| V | LSDP Estricto | `from pyspark import pipelines as dp`, `@dp.materialized_view`. Prohibido `import dlt`. Compatible con Serverless. | CUMPLE |
| VI | Arquitectura Medallon | Plata usa vistas materializadas. Consolidado: Dimension Tipo 1. Transaccional: vista materializada con incrementalidad gestionada por LSDP. Campos calculados con 2+ campos de bronce. | CUMPLE |
| VII | Estructura Proyecto LSDP | Nuevos scripts en `transformations/`, funciones transversales en `utilities/` sin decoradores dp. | CUMPLE |
| VIII | Gobernanza Repositorio | Speckit no crea branches ni commits. Control exclusivo del usuario. | CUMPLE |
| IX | Gobernanza IA | Research obligatorio realizado (R9-R12). 4 decisiones aprobadas por el usuario (R9-D1, R10-D1, R10-D2, R11-D1). | CUMPLE |
| X | Idioma y Comunicacion | Fuentes oficiales Azure Databricks consultadas. Respuestas en espanol. Links oficiales referenciados. | CUMPLE |

### Verificacion Post-Diseno (Fase 1) — Re-evaluacion

| # | Principio | Verificacion Post-Diseno | Estado |
|---|-----------|-------------------------|--------|
| I | Dinamismo Absoluto | Data model confirma: catalogo y esquema dinamicos en `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.nombre_vista")`. | CUMPLE |
| V | LSDP Estricto | Contrato confirma: solo `@dp.materialized_view` con lectura batch `spark.read.table()`. Sin `import dlt`. | CUMPLE |
| VI | Arquitectura Medallon | Data model define 175 cols consolidadas (Dim Tipo 1) + 65 cols transaccionales. Campos calculados con 3+ y 2+ campos de bronce respectivamente. | CUMPLE |
| VII | Estructura Proyecto LSDP | 2 nuevos scripts en `transformations/`, 1 refactorizacion en `utilities/` (sin decoradores dp), 3 notebooks test en `tests/`. | CUMPLE |
| IX | Gobernanza IA | Research R9-R12 completado con 4 decisiones aprobadas por el usuario. Ningun hallazgo aplicado sin aprobacion. | CUMPLE |

**Resultado**: GATE APROBADO — Sin violaciones. Todas las verificaciones cumplen.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/004-lsdp-medalla-plata/
├── plan.md                          # Este archivo
├── spec.md                          # Especificacion (20 RFs, 13 CEs, completada y clarificada)
├── research.md                      # Fase 0: Research V4 (R9-R12, 4 decisiones aprobadas)
├── data-model.md                    # Fase 1: Mapeo completo de columnas bronce → plata
├── quickstart.md                    # Fase 1: Guia rapida de implementacion
├── contracts/
│   └── pipeline-lsdp-plata.md       # Fase 1: Contrato del pipeline (entradas/salidas)
├── checklists/
│   └── requirements.md              # Checklist de requerimientos
└── tasks.md                         # Fase 2: Tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── __init__.py                                  # Sin cambio
│   ├── LsdpConexionAzureSql.py                      # REFACTORIZAR (RF-019): retornar ALL keys
│   ├── LsdpConstructorRutasAbfss.py                 # Sin cambio
│   └── LsdpReordenarColumnasLiquidCluster.py        # Sin cambio (reutilizado por plata)
└── transformations/
    ├── LsdpBronceCmstfl.py                          # Sin cambio (V3)
    ├── LsdpBronceTrxpfl.py                          # Sin cambio (V3)
    ├── LsdpBronceBlncfl.py                          # Sin cambio (V3)
    ├── LsdpPlataClientesSaldos.py                   # NUEVO — Vista materializada consolidada
    └── LsdpPlataTransacciones.py                    # NUEVO — Vista materializada transaccional

tests/LSDP_Laboratorio_Basico/
├── NbTestBronceCmstfl.py                            # Sin cambio (V3)
├── NbTestBronceTrxpfl.py                            # Sin cambio (V3)
├── NbTestBronceBlncfl.py                            # Sin cambio (V3)
├── NbTestConexionAzureSql.py                        # Sin cambio (V3) — validar compatibilidad
├── NbTestConstructorRutasAbfss.py                   # Sin cambio (V3)
├── NbTestReordenarColumnasLC.py                     # Sin cambio (V3)
├── NbTestPlataClientesSaldos.py                     # NUEVO — Pruebas vista consolidada
├── NbTestPlataTransacciones.py                      # NUEVO — Pruebas vista transaccional
└── NbTestConexionAzureSqlV4.py                      # NUEVO — Pruebas refactorizacion RF-019
```

**Decision de Estructura**: Se extiende el proyecto existente `LSDP_Laboratorio_Basico` siguiendo la estructura LSDP (Principio VII): nuevas transformaciones en `transformations/`, reutilizacion de `utilities/` existentes, nuevas pruebas en `tests/LSDP_Laboratorio_Basico/`.

## Complexity Tracking

> Sin violaciones de Constitution Check. No se requiere justificacion.
