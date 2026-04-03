# Tasks: LSDP Medalla de Bronce (Version 3)

**Input**: Documentos de diseno en `/specs/003-lsdp-medalla-bronce/`
**Prerequisites**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/

**Tests**: Incluidos — la especificacion requiere explicitamente una suite TDD (US6, RF-015, Principio III de la Constitucion).

**Organizacion**: Tareas agrupadas por historia de usuario. US1 y US2 son utilidades fundacionales. US3-US5 son tablas streaming independientes que pueden ejecutarse en paralelo. US6 es la suite TDD.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos distintos, sin dependencias pendientes)
- **[Story]**: Historia de usuario asociada (US1, US2, US3, US4, US5, US6)
- Las rutas de archivos son relativas a la raiz del repositorio

---

## Fase 1: Setup (Infraestructura Compartida)

**Proposito**: Inicializacion del proyecto y estructura de directorios

- [X] T001 Crear estructura de directorios del proyecto: src/LSDP_Laboratorio_Basico/utilities/, src/LSDP_Laboratorio_Basico/transformations/, tests/LSDP_Laboratorio_Basico/

---

## Fase 2: Historia de Usuario 1 — Lectura Dinamica de Parametros desde Azure SQL (Prioridad: P1)

**Objetivo**: Crear la funcion de utilidad que lee los 4 parametros de configuracion desde dbo.Parametros de Azure SQL usando JDBC con 2 secretos, ejecutada a nivel de modulo con closure pattern.

**Prueba Independiente**: Ejecutar la funcion de forma aislada y verificar que retorna el dict con {catalogoBronce, contenedorBronce, datalake, DirectorioBronce}.

### Implementacion de US1

- [X] T002 [US1] Implementar funcion de lectura de parametros Azure SQL en src/LSDP_Laboratorio_Basico/utilities/LsdpConexionAzureSql.py. Debe: (1) definir funcion que recibe spark y nombre_scope_secret como parametros, (2) obtener secretos sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd via dbutils.secrets.get(scope=nombre_scope_secret, key=nombre_secreto), (3) construir cadena JDBC combinando ambos secretos (la cadena JDBC del primer secreto no incluye password, se agrega el password del segundo secreto), (4) leer tabla dbo.Parametros via spark.read.format("jdbc") con driver com.microsoft.sqlserver.jdbc.SQLServerDriver, (5) retornar dict con claves {catalogoBronce, contenedorBronce, datalake, DirectorioBronce}, (6) fallar con mensaje claro si alguna clave falta en dbo.Parametros. Archivo formato notebook Databricks (.py con celdas markdown). Todo en espanol, snake_case, completamente comentado con bloques markdown explicativos por celda (RF-012). Compatible con Computo Serverless — prohibido spark.sparkContext (RF-011).

**Checkpoint**: La funcion de utilidad retorna correctamente los 4 parametros de Azure SQL.

---

## Fase 3: Historia de Usuario 2 — Construccion Dinamica de Rutas abfss:// (Prioridad: P1)

**Objetivo**: Crear la funcion de utilidad que construye rutas abfss:// combinando parametros de Azure SQL con rutas relativas del pipeline.

**Prueba Independiente**: Llamar la funcion con parametros de ejemplo y verificar que retorna la ruta abfss:// con formato correcto.

### Implementacion de US2

- [X] T003 [P] [US2] Implementar funcion de construccion de rutas abfss:// en src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutasAbfss.py. Debe: (1) definir funcion que recibe contenedor (string), datalake (string), directorio_raiz (string), ruta_relativa (string), (2) retornar string con formato abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa} (RF-005), (3) ser reutilizable para construir rutas tanto de parquets como de checkpoints (6 invocaciones en total: 3 rutas de parquets + 3 rutas de checkpoints). Archivo formato notebook Databricks (.py con celdas markdown). Todo en espanol, snake_case, completamente comentado con bloques markdown explicativos por celda (RF-012). Compatible con Computo Serverless (RF-011). Modular y reutilizable para versiones futuras de plata y oro (RF-017).

**Checkpoint**: La funcion retorna rutas abfss:// correctamente formateadas para los 6 casos de uso.

---

## Fase 4: Historia de Usuario 3 — Ingesta de Maestro de Clientes en Bronce (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear la tabla streaming cmstfl que ingesta parquets del Maestro de Clientes via AutoLoader con FechaIngestaDatos, Liquid Cluster y propiedades Delta.

**Prueba Independiente**: Ejecutar el pipeline y verificar que bronce_dev.regional.cmstfl existe con las columnas esperadas, propiedades Delta correctas y datos ingestados incrementalmente.

### Implementacion de US3

- [X] T004 [US3] Implementar tabla streaming de bronce cmstfl en src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py. Debe: (1) importar from pyspark import pipelines as dp, importar utilidades LsdpConexionAzureSql y LsdpConstructorRutasAbfss, importar current_timestamp de pyspark.sql.functions, (2) a nivel de modulo (fuera de funciones decoradas): leer parametro nombreScopeSecret del pipeline, invocar funcion de conexion Azure SQL para obtener dict de parametros {catalogoBronce, contenedorBronce, datalake, DirectorioBronce}, leer parametros rutaCompletaMaestroCliente y rutaCheckpointCmstfl del pipeline, construir ruta abfss:// de parquets y ruta abfss:// de checkpoint usando la funcion constructora — los resultados quedan como variables capturadas por closure (RF-002), (3) definir funcion decorada con @dp.table(name="cmstfl", comment="Tabla streaming de bronce — Maestro de Clientes (Customer Master File). Ingesta historica acumulativa desde parquets AS400 via AutoLoader.", table_properties={"delta.enableChangeDataFeed": "true", "delta.autoOptimize.autoCompact": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.deletedFileRetentionDuration": "interval 30 days", "delta.logRetentionDuration": "interval 60 days"}, cluster_by=["FechaIngestaDatos", "CUSTID"]), (4) dentro de la funcion decorada: return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("cloudFiles.schemaLocation", ruta_checkpoint_cmstfl).load(ruta_parquets_maestro).withColumn("FechaIngestaDatos", current_timestamp()). Sin parametro schema en el decorador (R7-D2, inferencia automatica). Sin @dp.expect (bronce sin validacion de calidad). Archivo formato notebook Databricks (.py con celdas markdown). Todo en espanol, snake_case, completamente comentado (RF-012). Compatible Serverless — prohibido sparkContext (RF-011). Resultado: 72 columnas iniciales (70 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns.

**Checkpoint**: La tabla cmstfl se crea en bronce_dev.regional con las columnas esperadas, Liquid Cluster [FechaIngestaDatos, CUSTID], propiedades Delta correctas y acumulacion historica.

---

## Fase 5: Historia de Usuario 4 — Ingesta de Transaccional en Bronce (Prioridad: P1)

**Objetivo**: Crear la tabla streaming trxpfl que ingesta parquets del Transaccional via AutoLoader con FechaIngestaDatos, Liquid Cluster y propiedades Delta.

**Prueba Independiente**: Ejecutar el pipeline y verificar que bronce_dev.regional.trxpfl existe con las columnas esperadas, propiedades Delta correctas y datos ingestados incrementalmente.

### Implementacion de US4

- [X] T005 [P] [US4] Implementar tabla streaming de bronce trxpfl en src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py. Mismo patron arquitectonico que T004 (imports, nivel de modulo con closure, @dp.table con AutoLoader) con las siguientes diferencias: (1) parametros del pipeline: rutaCompletaTransaccional y rutaCheckpointTrxpfl, (2) decorador @dp.table con name="trxpfl", comment="Tabla streaming de bronce — Transaccional (Transaction Processing File). Ingesta historica acumulativa desde parquets AS400 via AutoLoader.", mismas table_properties que T004, cluster_by=["TRXDT", "CUSTID", "TRXTYP"] (R6-D1), (3) AutoLoader apunta a ruta de parquets transaccionales y checkpoint de trxpfl. Resultado: 62 columnas iniciales (60 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns. Sin schema explicito (R7-D2). Sin @dp.expect. Archivo notebook Databricks. Todo en espanol, snake_case, completamente comentado (RF-012). Compatible Serverless (RF-011).

**Checkpoint**: La tabla trxpfl se crea en bronce_dev.regional con las columnas esperadas, Liquid Cluster [TRXDT, CUSTID, TRXTYP] y propiedades Delta correctas.

---

## Fase 6: Historia de Usuario 5 — Ingesta de Saldos en Bronce (Prioridad: P1)

**Objetivo**: Crear la tabla streaming blncfl que ingesta parquets de Saldos via AutoLoader con FechaIngestaDatos, Liquid Cluster y propiedades Delta.

**Prueba Independiente**: Ejecutar el pipeline y verificar que bronce_dev.regional.blncfl existe con las columnas esperadas, propiedades Delta correctas y datos ingestados incrementalmente.

### Implementacion de US5

- [X] T006 [P] [US5] Implementar tabla streaming de bronce blncfl en src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py. Mismo patron arquitectonico que T004 (imports, nivel de modulo con closure, @dp.table con AutoLoader) con las siguientes diferencias: (1) parametros del pipeline: rutaCompletaSaldoCliente y rutaCheckpointBlncfl, (2) decorador @dp.table con name="blncfl", comment="Tabla streaming de bronce — Saldos de Clientes (Balance Client File). Ingesta historica acumulativa desde parquets AS400 via AutoLoader.", mismas table_properties que T004, cluster_by=["FechaIngestaDatos", "CUSTID"] (R6-D1), (3) AutoLoader apunta a ruta de parquets de saldos y checkpoint de blncfl. Resultado: 102 columnas iniciales (100 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns. Sin schema explicito (R7-D2). Sin @dp.expect. Archivo notebook Databricks. Todo en espanol, snake_case, completamente comentado (RF-012). Compatible Serverless (RF-011).

**Checkpoint**: La tabla blncfl se crea en bronce_dev.regional con las columnas esperadas, Liquid Cluster [FechaIngestaDatos, CUSTID] y propiedades Delta correctas.

---

## Fase 7: Historia de Usuario 6 — Suite de Pruebas TDD (Prioridad: P2)

**Objetivo**: Crear 5 notebooks de prueba TDD que validen el correcto funcionamiento de cada componente del pipeline de bronce.

**Prueba Independiente**: Ejecutar los 5 notebooks en Computo Serverless y verificar que todas las pruebas pasan exitosamente (CE-008).

### Pruebas TDD de US6

- [X] T007 [P] [US6] Crear notebook de pruebas TDD para conexion Azure SQL en tests/LSDP_Laboratorio_Basico/NbTestConexionAzureSql.py. Validar: (1) conexion JDBC exitosa con los 2 secretos del Scope Secret, (2) lectura de dbo.Parametros retorna las 4 claves esperadas (catalogoBronce, contenedorBronce, datalake, DirectorioBronce), (3) valores no vacios para cada clave retornada, (4) manejo de error si nombre_scope_secret es invalido o inexistente. Nota: la validacion de compatibilidad Serverless (spark.sparkContext) se realiza por revision de codigo, no por prueba TDD automatizada, ya que su uso produce un error directo en tiempo de ejecucion. Notebook Databricks con celdas markdown explicativas, widgets para parametrizacion del nombre del scope secret, asserts con mensajes claros en espanol.

- [X] T008 [P] [US6] Crear notebook de pruebas TDD para constructor de rutas abfss:// en tests/LSDP_Laboratorio_Basico/NbTestConstructorRutasAbfss.py. Validar: (1) formato abfss:// correcto con parametros de ejemplo (contenedor="bronce", datalake="adlsg2datalakedev", directorio="archivos", ruta="LSDP_Base/As400/MaestroCliente/"), (2) ruta resultante NO contiene /mnt/ (V2-R5-D2), (3) componentes correctos en la ruta (protocolo, contenedor, storage account, directorio, ruta relativa), (4) funciona tanto para rutas de parquets como para rutas de checkpoints. Notebook Databricks con celdas markdown explicativas, asserts con mensajes claros en espanol.

- [X] T009 [P] [US6] Crear notebook de pruebas TDD para tabla streaming cmstfl en tests/LSDP_Laboratorio_Basico/NbTestBronceCmstfl.py. Validar: (1) tabla bronce_dev.regional.cmstfl existe en Unity Catalog, (2) columna FechaIngestaDatos presente con tipo timestamp, (3) columna _rescued_data presente con tipo string, (4) propiedades Delta verificables: Change Data Feed habilitado, autoOptimize.autoCompact habilitado, autoOptimize.optimizeWrite habilitado, retenciones 30/60 dias, (5) Liquid Cluster configurado por [FechaIngestaDatos, CUSTID] (R6-D1), (6) datos ingestados correctamente (conteo > 0). Nota: no se valida conteo exacto de columnas porque schema evolution (addNewColumns) puede modificar el total. Nota: la validacion de codigo fuente (spark.sparkContext, import dlt) se realiza por revision de codigo, no por prueba TDD automatizada, debido a incompatibilidades con Computo Serverless. Notebook Databricks con celdas markdown explicativas, asserts con mensajes claros en espanol. Ejecutar en Computo Serverless.

- [X] T010 [P] [US6] Crear notebook de pruebas TDD para tabla streaming trxpfl en tests/LSDP_Laboratorio_Basico/NbTestBronceTrxpfl.py. Validar: (1) tabla bronce_dev.regional.trxpfl existe en Unity Catalog, (2) columna FechaIngestaDatos presente con tipo timestamp, (3) columna _rescued_data presente con tipo string, (4) propiedades Delta: Change Data Feed, autoOptimize, retenciones correctas, (5) Liquid Cluster configurado por [TRXDT, CUSTID, TRXTYP] (R6-D1), (6) datos ingestados (conteo > 0). Nota: no se valida conteo exacto de columnas porque schema evolution (addNewColumns) puede modificar el total. Nota: la validacion de codigo fuente (spark.sparkContext, import dlt) se realiza por revision de codigo, no por prueba TDD automatizada, debido a incompatibilidades con Computo Serverless. Notebook Databricks, celdas markdown, asserts claros en espanol. Computo Serverless.

- [X] T011 [P] [US6] Crear notebook de pruebas TDD para tabla streaming blncfl en tests/LSDP_Laboratorio_Basico/NbTestBronceBlncfl.py. Validar: (1) tabla bronce_dev.regional.blncfl existe en Unity Catalog, (2) columna FechaIngestaDatos presente con tipo timestamp, (3) columna _rescued_data presente con tipo string, (4) propiedades Delta: Change Data Feed, autoOptimize, retenciones correctas, (5) Liquid Cluster configurado por [FechaIngestaDatos, CUSTID] (R6-D1), (6) datos ingestados (conteo > 0). Nota: no se valida conteo exacto de columnas porque schema evolution (addNewColumns) puede modificar el total. Nota: la validacion de codigo fuente (spark.sparkContext, import dlt) se realiza por revision de codigo, no por prueba TDD automatizada, debido a incompatibilidades con Computo Serverless. Notebook Databricks, celdas markdown, asserts claros en espanol. Computo Serverless.

**Checkpoint**: Los 5 notebooks de prueba TDD estan creados y listos para ejecutarse en Computo Serverless. Deben cubrir RF-001 a RF-017 con al menos una prueba asociada (CE-008).

---

## Fase 8: Polish y Validacion Cruzada

**Proposito**: Validacion integral del pipeline completo y ajustes finales

- [ ] T012 Ejecutar el pipeline completo en Databricks siguiendo los pasos de specs/003-lsdp-medalla-bronce/quickstart.md: crear ETL Pipeline en Jobs & Pipelines, configurar los 7 parametros, ejecutar y verificar que las 3 tablas streaming se crean en bronce_dev.regional con columnas, propiedades Delta y Liquid Cluster correctos (CE-001 a CE-007)
- [ ] T013 Ejecutar los 5 notebooks de prueba TDD en Computo Serverless y verificar que el 100% de las pruebas pasan exitosamente (CE-008, CE-009)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Fase 1)**: Sin dependencias — puede iniciar inmediatamente
- **US1 (Fase 2)**: Depende de Fase 1 — crea la utilidad de conexion Azure SQL
- **US2 (Fase 3)**: Depende de Fase 1 — crea la utilidad de rutas abfss:// (puede ejecutarse en paralelo con US1)
- **US3 (Fase 4)**: Depende de US1 + US2 — importa ambas utilidades para inicializacion a nivel de modulo
- **US4 (Fase 5)**: Depende de US1 + US2 — puede ejecutarse en paralelo con US3
- **US5 (Fase 6)**: Depende de US1 + US2 — puede ejecutarse en paralelo con US3 y US4
- **US6 (Fase 7)**: Depende de US3 + US4 + US5 — las pruebas validan las tablas creadas por el pipeline
- **Polish (Fase 8)**: Depende de todas las fases anteriores

### Dependencias entre Historias de Usuario

- **US1 (P1)**: Bloqueante para US3, US4, US5 (proporciona parametros Azure SQL via closure)
- **US2 (P1)**: Bloqueante para US3, US4, US5 (proporciona funcion de construccion de rutas). Independiente de US1 a nivel de codigo.
- **US3 (P1)**: Independiente de US4 y US5. Depende de US1 + US2.
- **US4 (P1)**: Independiente de US3 y US5. Depende de US1 + US2.
- **US5 (P1)**: Independiente de US3 y US4. Depende de US1 + US2.
- **US6 (P2)**: Depende de que US1-US5 esten implementados para poder probarlos.

### Dentro de Cada Historia de Usuario

- Cada historia tiene un solo archivo de implementacion (principio SOLID, RF-017)
- No hay sub-dependencias internas (es un pipeline declarativo, no una aplicacion con capas)
- Las utilidades (US1, US2) se ejecutan a nivel de modulo y quedan disponibles via closure (RF-002)
- Las transformaciones (US3-US5) importan las utilidades y construyen las rutas a nivel de modulo

### Oportunidades de Paralelismo

- **US1 y US2** pueden ejecutarse en paralelo (archivos independientes en utilities/)
- **US3, US4, US5** pueden ejecutarse en paralelo una vez US1 y US2 estan completas (archivos independientes en transformations/)
- **Los 5 notebooks TDD** (US6) pueden crearse en paralelo (archivos independientes en tests/)

---

## Ejemplo de Paralelismo: Utilidades (Fase 2-3)

```bash
# Estas dos tareas pueden ejecutarse simultaneamente:
T002: LsdpConexionAzureSql.py (US1 - conexion Azure SQL)
T003: LsdpConstructorRutasAbfss.py (US2 - constructor de rutas)
```

## Ejemplo de Paralelismo: Streaming Tables (Fase 4-6)

```bash
# Estas tres tareas pueden ejecutarse simultaneamente (una vez US1+US2 completas):
T004: LsdpBronceCmstfl.py (US3 - Maestro de Clientes)
T005: LsdpBronceTrxpfl.py (US4 - Transaccional)
T006: LsdpBronceBlncfl.py (US5 - Saldos)
```

## Ejemplo de Paralelismo: Suite TDD (Fase 7)

```bash
# Los 5 notebooks pueden crearse simultaneamente:
T007: NbTestConexionAzureSql.py (pruebas conexion Azure SQL)
T008: NbTestConstructorRutasAbfss.py (pruebas constructor rutas)
T009: NbTestBronceCmstfl.py (pruebas tabla cmstfl)
T010: NbTestBronceTrxpfl.py (pruebas tabla trxpfl)
T011: NbTestBronceBlncfl.py (pruebas tabla blncfl)
```

---

## Estrategia de Implementacion

### MVP Primero (US1 + US2 + US3)

1. Completar Fase 1: Setup (estructura de directorios)
2. Completar Fase 2: US1 (conexion Azure SQL)
3. Completar Fase 3: US2 (constructor de rutas) — en paralelo con Fase 2
4. Completar Fase 4: US3 (tabla streaming cmstfl — Maestro de Clientes)
5. **VALIDAR**: Ejecutar el pipeline y verificar que cmstfl se crea correctamente con las columnas esperadas, Liquid Cluster y propiedades Delta
6. Si es exitoso: continuar con US4, US5, US6

### Entrega Incremental

1. Setup + US1 + US2 → Utilidades listas (conexion + rutas)
2. + US3 (cmstfl) → Primera tabla streaming operativa (**MVP!**)
3. + US4 (trxpfl) → Segunda tabla streaming operativa
4. + US5 (blncfl) → Tercera tabla streaming operativa — pipeline bronce completo
5. + US6 (TDD) → Suite de pruebas completa — validacion integral
6. Cada historia agrega valor sin romper las anteriores

### Alcance MVP Sugerido

El MVP minimo es **US1 + US2 + US3** (Fases 1-4): crear las utilidades y la primera tabla streaming (cmstfl). Esto valida la arquitectura completa del pipeline: conexion Azure SQL → construccion de rutas → AutoLoader → tabla streaming Delta con Liquid Cluster y propiedades. Las tablas restantes (US4, US5) siguen exactamente el mismo patron.

---

## Notas

- Tareas [P] = archivos distintos, sin dependencias pendientes — pueden ejecutarse simultaneamente
- [Story] mapea la tarea a la historia de usuario de spec.md para trazabilidad
- Cada historia es independientemente completable y testeable
- Todo el codigo en espanol, snake_case, con prefijo Lsdp (pipeline) o Nb (notebooks) en PascalCase (RF-012)
- Prohibido: sparkContext, import dlt, /mnt/, ZOrder, PartitionBy, pivot()
- Solo protocolo abfss:// para lectura/escritura (V2-R5-D2)
- Solo biblioteca pyspark.pipelines as dp (R1-D1)
- Propiedades Delta estandar en todas las tablas (R1-D5, RF-009)
- Liquid Cluster explicito por tabla (R6-D1): cmstfl/blncfl [FechaIngestaDatos, CUSTID], trxpfl [TRXDT, CUSTID, TRXTYP]
- AutoLoader con config explicita (R7-D1): format + schemaEvolutionMode + schemaLocation
- Sin schema en @dp.table (R7-D2): inferencia automatica de parquets
- Sin @dp.expect en bronce: validacion de calidad delegada a plata (V4)
