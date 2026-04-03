# Plan de Implementacion: Lakeflow Spark Declarative Pipelines - Medalla de Bronce (Version 3)

**Branch**: `003-lsdp-medalla-bronce` | **Fecha**: 2026-03-29 | **Spec**: [spec.md](spec.md)
**Input**: Especificacion del feature desde `/specs/003-lsdp-medalla-bronce/spec.md`

## Resumen

Crear el pipeline Lakeflow Spark Declarative Pipelines (LSDP) para la medalla de bronce, incluyendo: (1) funcion de utilidad para lectura dinamica de parametros desde Azure SQL (tabla dbo.Parametros), (2) funcion de utilidad para construccion dinamica de rutas abfss://, (3) funcion de utilidad para reordenamiento de columnas del Liquid Cluster (garantiza que las columnas de clustering queden entre las primeras 32 de la tabla Delta para que tengan estadisticas), (4) tres tablas streaming de bronce (cmstfl, trxpfl, blncfl) que ingesisten los parquets del Maestro de Clientes, Transaccional y Saldos respectivamente mediante AutoLoader con marca de tiempo FechaIngestaDatos y acumulacion historica, y (5) suite de pruebas TDD.

## Contexto Tecnico

**Lenguaje/Version**: Python 3 / PySpark (Databricks Runtime Serverless)
**Dependencias Principales**: `pyspark.pipelines` (alias `dp`), `pyspark.sql`, `dbutils` (secretos y widgets)
**Almacenamiento**: Azure Data Lake Storage Gen2 (abfss://), Azure SQL (tabla dbo.Parametros), Unity Catalog (catalogo bronce_dev, esquema regional)
**Testing**: Notebooks de prueba TDD en `tests/LSDP_Laboratorio_Basico/` ejecutables via extensiones Databricks para VS Code
**Plataforma Destino**: Azure Databricks Premium con Computo Serverless
**Tipo de Proyecto**: Pipeline ETL declarativo (Lakeflow Spark Declarative Pipelines)
**Metas de Rendimiento**: Ingesta incremental de parquets (5M+ registros Maestro, 15M+ Transaccional, 5M+ Saldos) sin reprocesamiento
**Restricciones**: Prohibido spark.sparkContext; prohibido import dlt; prohibido /mnt/; solo abfss://; solo pyspark.pipelines; columnas de Liquid Cluster deben estar entre las primeras 32 columnas de la tabla Delta
**Escala/Alcance**: 3 tablas streaming de bronce, 3 funciones de utilidad, 1 suite de pruebas TDD

## Constitution Check

*GATE: Debe pasar antes de la Fase 0 de research. Se re-verifica despues del diseno en Fase 1.*

| Principio | Estado | Verificacion |
|-----------|--------|-------------|
| I. Dinamismo Absoluto (Cero Hard-Coded) | PASA | Parametros de Azure SQL (catalogoBronce, contenedorBronce, datalake, DirectorioBronce) + parametros del pipeline (rutas relativas) + Scope Secret parametrizable. Cero valores constantes. |
| II. Entorno de Desarrollo Databricks | PASA | Extensiones Databricks y SQLTools son prerequisito. Pruebas TDD ejecutables via extensiones. |
| III. Desarrollo Guiado por Pruebas (TDD) | PASA | Suite TDD en tests/LSDP_Laboratorio_Basico/ cubre RF-001 a RF-017. |
| IV. Simulacion de Datos AS400 | NO APLICA | Los parquets ya fueron generados en la Version 2. Esta version los consume. |
| V. LSDP Estricto | PASA | Exclusivamente from pyspark import pipelines as dp. Prohibido import dlt. Compatible con Computo Serverless. |
| VI. Arquitectura Medallon | PASA | Bronce con FechaIngestaDatos, acumulacion historica via AutoLoader. Plata y Oro se implementan en V4 y V5. |
| VII. Estructura de Proyecto LSDP | PASA | Carpeta utilities/ para funciones reutilizables, transformations/ para tablas streaming. |
| VIII. Gobernanza del Repositorio | PASA | Sin creacion de branches ni commits automaticos. |
| IX. Gobernanza de la IA | PASA | Research obligatorio al inicio de V3 con decisiones del usuario. |
| X. Idioma y Comunicacion | PASA | Todo en espanol. |

**Resultado del Gate (pre-Fase 0)**: TODOS PASAN. Se procede a Fase 0.

### Re-evaluacion Post-Diseno (Fase 1)

Verificacion de que los artefactos de Fase 1 (data-model.md, contracts/, quickstart.md) no introdujeron violaciones:

| Principio | Estado Post-Diseno | Verificacion |
|-----------|-------------------|-------------|
| I. Dinamismo Absoluto | PASA | 7 parametros del pipeline + 4 parametros Azure SQL documentados en data-model.md y contrato. Cero hardcoded. |
| II. Entorno Databricks | PASA | quickstart.md lista extensiones requeridas y pasos de configuracion. |
| III. TDD | PASA | 5 notebooks de prueba documentados en quickstart.md. Reglas de validacion en data-model.md. |
| IV. Simulacion AS400 | NO APLICA | V3 consume parquets generados en V2. |
| V. LSDP Estricto | PASA | Contrato documenta exclusivamente `from pyspark import pipelines as dp`. Sin import dlt. |
| VI. Arquitectura Medallon | PASA | Bronce con FechaIngestaDatos, Liquid Cluster (R6-D1), propiedades Delta (R1-D5), AutoLoader explicito (R7-D1). |
| VII. Estructura Proyecto | PASA | utilities/ + transformations/ en plan.md y quickstart.md. |
| VIII. Gobernanza Repositorio | PASA | Artefactos solo en specs/003-lsdp-medalla-bronce/. |
| IX. Gobernanza IA | PASA | research.md con 3 decisiones V3 aprobadas por el usuario (R6-D1, R7-D1, R7-D2). |
| X. Idioma | PASA | Todo en espanol. |

**Resultado del Gate Post-Diseno**: TODOS PASAN. Fase 2 puede proceder.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/003-lsdp-medalla-bronce/
  plan.md              # Este archivo
  research.md          # Fase 0
  data-model.md        # Fase 1
  quickstart.md        # Fase 1
  contracts/           # Fase 1
  checklists/
    requirements.md    # Checklist de calidad
```

### Codigo Fuente (raiz del repositorio)

```text
src/
  LSDP_Laboratorio_Basico/
    utilities/
      LsdpConexionAzureSql.py       # Funcion de utilidad: lectura de parametros Azure SQL
      LsdpConstructorRutasAbfss.py   # Funcion de utilidad: construccion de rutas abfss://
      LsdpReordenarColumnasLiquidCluster.py  # Funcion de utilidad: reordenamiento de columnas para Liquid Cluster
    transformations/
      LsdpBronceCmstfl.py            # Tabla streaming bronce: Maestro de Clientes (cmstfl)
      LsdpBronceTrxpfl.py            # Tabla streaming bronce: Transaccional (trxpfl)
      LsdpBronceBlncfl.py            # Tabla streaming bronce: Saldos de Clientes (blncfl)

tests/
  LSDP_Laboratorio_Basico/
    NbTestConexionAzureSql.py        # TDD: lectura de parametros Azure SQL
    NbTestConstructorRutasAbfss.py   # TDD: construccion de rutas abfss://
    NbTestReordenarColumnasLC.py     # TDD: reordenamiento de columnas para Liquid Cluster
    NbTestBronceCmstfl.py            # TDD: tabla streaming maestro clientes
    NbTestBronceTrxpfl.py            # TDD: tabla streaming transaccional
    NbTestBronceBlncfl.py            # TDD: tabla streaming saldos clientes
```

**Decision de Estructura**: Se sigue la organizacion estandar de Lakeflow Spark Declarative Pipelines con carpetas utilities/ y transformations/ dentro de src/LSDP_Laboratorio_Basico/. Las pruebas TDD se ubican en tests/LSDP_Laboratorio_Basico/ conforme la estructura definida en el SYSTEM.md.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| *Sin violaciones identificadas* | *N/A* | *Todos los componentes cumplen la regla de maximo 3 proyectos y evitan abstracciones innecesarias.* |
