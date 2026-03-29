# Plan de Implementacion: Generacion de Parquets Simulando Data AS400

**Branch**: `002-generacion-parquets-as400` | **Fecha**: 2026-03-28 | **Spec**: [spec.md](spec.md)
**Input**: Especificacion del feature desde `/specs/002-generacion-parquets-as400/spec.md`

## Resumen

Crear 3 notebooks Python (.py) compatibles con Azure Databricks para generar archivos parquet simulando la data bancaria de AS400: Maestro de Clientes (70 campos, 5M registros), Transaccional (60 campos, 15M registros por ejecucion) y Saldos (100 campos, relacion 1:1 con Maestro). Los notebooks usan `dbutils.widgets` para parametrizacion completa, generan IDs secuenciales con offset parametrizable, y se acompanan de una suite de pruebas TDD ejecutable desde notebooks de prueba en Databricks.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x (compatible con Databricks Runtime Serverless)
**Dependencias Principales**: PySpark (incluido en Databricks Runtime), dbutils (nativo de Databricks)
**Almacenamiento**: Archivos Parquet en External Location (Azure Data Lake Storage Gen2)
**Testing**: Notebooks de prueba TDD ejecutados en Databricks (estrategia mixta aprobada en V1)
**Plataforma Destino**: Azure Databricks con Computo Serverless
**Tipo de Proyecto**: Notebooks de generacion de datos (simuladores de landing zone)
**Metas de Rendimiento**: Generacion de 5M/15M/5M registros en menos de 10 minutos cada uno en Computo Serverless (CE-001, CE-002, CE-003)
**Restricciones**: Compatible con Computo Serverless, cero valores hardcodeados, nombres exclusivamente hebreos/egipcios/ingleses
**Escala/Alcance**: 3 notebooks generadores + suite TDD, 70+60+100 = 230 columnas totales, volumetrias de millones de filas

## Verificacion de Constitucion (Pre-Diseno)

*GATE: Debe pasar antes de la Fase 0 de research. Se re-verifica despues de la Fase 1.*

| # | Principio | Estado | Evidencia |
|---|-----------|--------|-----------|
| I | Dinamismo Absoluto (Cero Hard-Coded) | PASA | RF-002: todos los valores via `dbutils.widgets`. RF-018: offsets parametrizables. CE-008: cero hardcoded. |
| II | Entorno de Desarrollo Databricks | PASA | Supuesto: extensiones configuradas (R4-D1 de V1). RF-015: notebooks ejecutados en Databricks. |
| III | Desarrollo Guiado por Pruebas (TDD) | PASA | RF-014: suite TDD en `tests/GenerarParquets/`. Historia 4: suite completa. CE-007: cobertura RF-001 a RF-018. |
| IV | Simulacion de Datos AS400 (Landing Zone) | PASA | RF-003/006/010: estructuras R2-D1/D2/D4 aprobadas. RF-005: nombres hebreos/egipcios/ingleses (R3-D1). RF-004: 5M + 0.60%. RF-007: 15M. RF-011: 1:1. |
| V | LSDP Estricto | PASA | RF-015: notebooks independientes de LSDP. No generan pipeline, solo datos de entrada. |
| VI | Arquitectura Medallon | N/A | Version 2 genera datos de landing zone, no pipeline medallon. Se aplica en V3-V5. |
| VII | Estructura de Proyecto LSDP | PASA | RF-012: notebooks en `scripts/GenerarParquets/`. RF-014: pruebas en `tests/GenerarParquets/`. |
| VIII | Gobernanza del Repositorio | PASA | No se crean branches ni commits automaticos. Solo directorios convencionales. |
| IX | Gobernanza de la IA | PASA | Todas las decisiones de V1 aprobadas por el usuario (R2-D1 a R2-D4, R3-D1). Clarificaciones de V2 aprobadas interactivamente. |
| X | Idioma y Comunicacion | PASA | RF-013: codigo en espanol, snake_case. RF-001: markdown explicativo y profesional. |

**Resultado del Gate**: APROBADO. Cero violaciones. Se procede a Fase 0.

## Estructura del Proyecto

### Documentacion (esta feature)

```text
specs/002-generacion-parquets-as400/
├── plan.md              # Este archivo
├── research.md          # Salida de Fase 0
├── data-model.md        # Salida de Fase 1
├── quickstart.md        # Salida de Fase 1
├── checklists/
│   └── requirements.md  # Checklist de calidad del spec
└── tasks.md             # Salida de /speckit.tasks (NO creado por /speckit.plan)
```

### Codigo Fuente (raiz del repositorio)

```text
scripts/
└── GenerarParquets/
    ├── NbGenerarMaestroCliente.py       # Notebook generador del Maestro de Clientes
    ├── NbGenerarTransaccionalCliente.py  # Notebook generador del Transaccional
    └── NbGenerarSaldosCliente.py        # Notebook generador de Saldos

tests/
└── GenerarParquets/
    ├── NbTestMaestroCliente.py          # Pruebas TDD del Maestro de Clientes
    ├── NbTestTransaccionalCliente.py    # Pruebas TDD del Transaccional
    └── NbTestSaldosCliente.py           # Pruebas TDD de Saldos
```

**Decision de Estructura**: Se sigue la estructura definida en la seccion Plan del SYSTEM.md. Los notebooks generadores van en `scripts/GenerarParquets/` y las pruebas TDD en `tests/GenerarParquets/`. Los archivos .py usan formato PascalCase con prefijo "Nb" (RF-001). No se crean directorios adicionales ya que la Version 2 no incluye LSDP.

## Verificacion de Constitucion (Post-Diseno)

*Re-verificacion despues de completar Fase 0 (Research) y Fase 1 (Diseno y Contratos).*

| # | Principio | Estado | Evidencia Post-Diseno |
|---|-----------|--------|----------------------|
| I | Dinamismo Absoluto | PASA | data-model.md define 61 widgets parametrizables (17 Maestro + 33 Transaccional + 11 Saldos). Cero hardcoded. |
| II | Entorno Databricks | PASA | quickstart.md documenta Computo Serverless. Extensiones R4-D1 referenciadas. |
| III | TDD | PASA | 3 notebooks de prueba en tests/GenerarParquets/. 21 reglas de validacion (RV-01 a RV-21). |
| IV | Simulacion AS400 | PASA | data-model.md mapea E1 (70 campos), E2 (60 campos), E3 (100 campos). Nombres R3-D1. 15 tipos R2-D3. |
| V | LSDP Estricto | PASA | research.md V2-R1 confirma independencia de LSDP. |
| VI | Arquitectura Medallon | N/A | V2 genera landing zone solamente. |
| VII | Estructura Proyecto LSDP | PASA | scripts/GenerarParquets/ + tests/GenerarParquets/ confirmados. |
| VIII | Gobernanza del Repositorio | PASA | Sin branches ni commits automaticos. |
| IX | Gobernanza de la IA | PASA | 2 decisiones APROBADAS (V2-R2-D1, V2-R3-D1) el 2026-03-28. |
| X | Idioma y Comunicacion | PASA | Artefactos en espanol. Fuentes oficiales. |

**Resultado Gate Post-Diseno**: APROBADO (10/10 PASAN o N/A). Cero violaciones.

## Artefactos Generados

| Artefacto | Ruta | Fase |
|-----------|------|------|
| Plan de Implementacion | [plan.md](plan.md) | Setup |
| Research V2 | [research.md](research.md) | Fase 0 |
| Modelo de Datos | [data-model.md](data-model.md) | Fase 1 |
| Guia Rapida | [quickstart.md](quickstart.md) | Fase 1 |
| Checklist de Calidad | [checklists/requirements.md](checklists/requirements.md) | Pre-plan |
| Contexto Agente | [../../.github/agents/copilot-instructions.md](../../.github/agents/copilot-instructions.md) | Fase 1 |

## Decisiones Aprobadas

> Todas las decisiones del research V2 han sido aprobadas por el usuario el 2026-03-28 (Principio IX de la Constitucion cumplido).

| ID | Tema | Recomendacion IA | Estado |
|----|------|------------------|--------|
| V2-R2-D1 | `dbutils.widgets.text()` como tipo principal | Aprobar | APROBADA (2026-03-28) |
| V2-R3-D1 | `write.parquet()` + StructType + reparticion | Aprobar | APROBADA (2026-03-28) |

**Todas las decisiones aprobadas por el usuario el 2026-03-28**. Detalle completo en [research.md](research.md).

## Seguimiento de Complejidad

> No se detectaron violaciones a la Constitucion. Esta seccion queda vacia.
