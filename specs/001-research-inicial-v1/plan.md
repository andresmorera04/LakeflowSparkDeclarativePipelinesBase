# Plan de Implementacion: Research Inicial y Decisiones Clave - Version 1

**Branch**: `feature/ConfiguracionesYDatosSinteticos` (gestionado por el usuario) | **Fecha**: 2026-03-27 | **Spec**: [spec.md](spec.md)
**Input**: Especificacion del feature en `/specs/001-research-inicial-v1/spec.md`

## Resumen

La Version 1 del proyecto LakeflowSparkDeclarativePipelinesBase es exclusivamente de research y toma de decisiones. No se genera ningun codigo fuente. El objetivo es investigar a fondo Lakeflow Spark Declarative Pipelines (LSDP) usando la biblioteca `pyspark.pipelines`, la estructura de tablas AS400 bancarias, catalogos de nombres no latinos (hebreos, egipcios, ingleses), extensiones de Databricks para VS Code, y el patron de conexion a Azure SQL via secretos. Cada hallazgo con opciones requiere la decision explicita del usuario antes de avanzar.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x (PySpark) sobre Databricks Runtime Serverless
**Dependencias Principales**: `pyspark.pipelines` (alias `dp`), Apache Spark 4.1+, AutoLoader (cloudFiles), Delta Lake
**Almacenamiento**: Azure Data Lake Storage Gen2 (External Locations), Unity Catalog (catalogos: bronce_dev, plata_dev, oro_dev, control_dev), Azure SQL (parametros via JDBC)
**Testing**: Estrategia mixta: pytest local para utilities/ + notebooks de prueba en Databricks para pipeline LSDP completo
**Plataforma Objetivo**: Azure Databricks Tier Premium, Computo Serverless estricto
**Tipo de Proyecto**: Pipeline de datos / ETL declarativo (Lakeflow Spark Declarative Pipelines)
**Metas de Rendimiento**: Procesamiento eficiente de 5M+ registros (Maestro), 15M registros por ejecucion (Transaccional), optimizacion via Liquid Cluster, autoOptimize
**Restricciones**: Solo Computo Serverless (sin clusters clasicos), solo `pyspark.pipelines` (NO `import dlt`), cero hard-coded, pivot() no soportado en LSDP
**Escala/Alcance**: 5M clientes iniciales (+0.60% incremental), 15M transacciones por ejecucion, 3 medallas (Bronce, Plata, Oro), 4 catalogos Unity Catalog

## Verificacion de Constitucion

*GATE: Debe pasar antes de la Fase 0. Se re-verifica despues de la Fase 1.*

| # | Principio | Estado | Justificacion |
|---|-----------|--------|---------------|
| I | Dinamismo Absoluto (Cero Hard-Coded) | PASA | V1 no genera codigo. Las decisiones de research documentaran que todo valor configurable debe ser parametrizado. |
| II | Entorno de Desarrollo Databricks | PASA | RF-005 investiga extensiones Databricks para VS Code y SQLTools. Se validaran capacidades y configuracion. |
| III | Desarrollo Guiado por Pruebas (TDD) | PASA | V1 esta exenta de pruebas (solo research). La estrategia de testing mixta (pytest + notebooks) queda documentada para V2+. |
| IV | Simulacion de Datos AS400 | PASA | RF-002 investiga estructura de tablas AS400 con distribucion exacta de campos. RF-004 investiga nombres no latinos. |
| V | LSDP Estricto | PASA | RF-001 y RF-010 investigan `pyspark.pipelines` vs `dlt`. Se documenta explicitamente la diferencia. |
| VI | Arquitectura Medallon | PASA | CE-009 documenta agregacion de Oro. El research cubre AutoLoader (Bronce), vistas materializadas (Plata/Oro), y apply_changes. |
| VII | Estructura de Proyecto LSDP | PASA | La estructura de directorios se define en este plan (carpetas utilities, transformations, etc.). |
| VIII | Gobernanza del Repositorio | PASA | El branch es gestionado por el usuario. Speckit solo crea directorios convencionales. |
| IX | Gobernanza de la IA | PASA | RF-006 exige que cada hallazgo con opciones sea decidido por el usuario. La IA propone, el usuario decide. |
| X | Idioma y Comunicacion | PASA | Todos los artefactos estan en espanol. Se usan fuentes oficiales de Azure Databricks como referencia principal. |

**Resultado del Gate**: 10/10 principios PASAN. Se procede con Fase 0.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/001-research-inicial-v1/
├── plan.md              # Este archivo (salida de /speckit.plan)
├── research.md          # Salida de Fase 0 (hallazgos de investigacion)
├── data-model.md        # Salida de Fase 1 (entidades y modelo de datos)
├── quickstart.md        # Salida de Fase 1 (guia rapida del feature)
├── checklists/
│   └── requirements.md  # Checklist de validacion de requerimientos
└── tasks.md             # Salida de /speckit.tasks (NO creado por /speckit.plan)
```

### Codigo Fuente (raiz del repositorio - aplica a V2+)

```text
scripts/
├── GenerarParquets/
│   ├── generarMaestroCliente.py
│   ├── generarTransaccionalCliente.py
│   └── generarSaldosCliente.py
src/
├── README.md
├── LSDP_Laboratorio_Basico/
│   ├── utilities/          # Funciones transversales reutilizables (principio SOLID)
│   ├── transformations/    # Scripts principales del pipeline LSDP
│   └── explorations/       # Notebooks de exploracion y validacion
docs/
├── ManualTecnico.md
tests/
├── GenerarParquets/        # TDD para generacion de parquets
├── LSDP_Laboratorio_Basico/ # TDD para pipeline LSDP
```

**Decision de Estructura**: Se adopta la estructura definida en la seccion Plan del SYSTEM.md. La Version 1 no crea ningun directorio de codigo; estos se crean a partir de V2. La estructura de utilities/, transformations/ y explorations/ sigue el patron estandar de LSDP.

## Seguimiento de Complejidad

> No hay violaciones a la constitucion. Todos los principios pasan la verificacion.
