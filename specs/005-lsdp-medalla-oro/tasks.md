# Tasks: LSDP Medalla de Oro (Version 5)

**Input**: Artefactos de diseño en `/specs/005-lsdp-medalla-oro/`
**Prerequisites**: plan.md (requerido), spec.md (requerido — 18 RFs, 13 CEs, 4 historias de usuario), research.md (R13-R15, 3 decisiones aprobadas), data-model.md (6+22 columnas oro), contracts/pipeline-lsdp-oro.md, quickstart.md

**Tests**: Incluidos — RF-013 y US4 requieren suite TDD obligatoria (Principio III de la Constitucion). Escribir tests primero (TDD), verificar que fallan, luego implementar.

**Organizacion**: Tareas agrupadas por historia de usuario. US3 (parametros) es prerequisito de US1 y US2. US2 depende de US1 (`resumen_integral_cliente` lee de `comportamiento_atm_cliente` via `spark.read.table()`). V5 NO modifica ningun archivo existente — solo agrega 2 archivos nuevos.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias pendientes)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3, US4)
- Rutas exactas incluidas en cada descripcion

---

## Phase 1: Setup

**Proposito**: Configuracion de prerequisitos de infraestructura para la medalla de oro

- [ ] T001 Agregar parametro `esquema_oro` con valor `"regional"` a la configuracion JSON del pipeline LSDP en Databricks junto al parametro existente `esquema_plata`, y agregar el archivo fuente `transformations/LsdpOroClientes.py` a la lista de source files del pipeline existente (ver quickstart.md pasos 1 y 4). Nota: la clave `catalogoOro` ya existe en la tabla `dbo.Parametros` de Azure SQL — solo se lee via `leer_parametros_azure_sql()`

---

## Phase 2: US3 — Lectura Dinamica de Parametros de Oro desde Azure SQL y Pipeline (Prioridad: P1)

**Objetivo**: Crear la estructura base del archivo `LsdpOroClientes.py` con la lectura dinamica de todos los parametros necesarios (catalogo oro desde Azure SQL, esquema oro del pipeline, catalogo plata desde Azure SQL, esquema plata del pipeline) capturados como variables de modulo por closure para ambas vistas materializadas.

**Prueba Independiente**: Verificar que: (1) catalogo de oro se lee desde dbo.Parametros con clave `catalogoOro`, (2) esquema de oro se lee del parametro del pipeline, (3) la funcion `leer_parametros_azure_sql` se reutiliza sin modificacion, (4) los valores se propagan a las funciones decoradas via closure. **Nota**: La validacion de parametros (CE-012, CE-013) NO se realiza desde el notebook de pruebas — se ejecuta directamente desde el Lakeflow Spark Declarative Pipeline, ya que requiere el contexto del pipeline para acceder a `pipelines.parameters.*` y a Azure SQL via closure.

### Tests para US3

- [X] T002 [US3] Crear estructura base del notebook de prueba TDD en `tests/LSDP_Laboratorio_Basico/NbTestOroClientes.py` — celda markdown de titulo y descripcion general del notebook de pruebas de la medalla de oro V5 + celda de imports (`pyspark.sql.types`, `pyspark.sql.functions`). **Nota**: Las pruebas de parametros (CE-012: claves Azure SQL, CE-013: parametro esquema_oro) fueron eliminadas del notebook — estas validaciones se ejecutan directamente desde el Lakeflow Spark Declarative Pipeline, ya que requieren el contexto del pipeline (`pipelines.parameters.*`) que no esta disponible en un notebook separado

### Implementacion para US3

- [X] T003 [US3] Crear estructura base del script `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — (1) celda markdown de titulo con descripcion del modulo de oro V5 que contiene 2 vistas materializadas con closure compartida, (2) celda de imports: `from pyspark import pipelines as dp` (RF-001), `from pyspark.sql.functions import col, count, when, avg, coalesce, lit` y `from pyspark.sql.functions import sum as spark_sum` (alias para evitar conflicto con builtin), imports de utilities: `from utilities.LsdpConexionAzureSql import leer_parametros_azure_sql` y `from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster` (RF-009), (3) celda markdown explicativa del closure pattern y variables disponibles, (4) celda de lectura de parametros a nivel de modulo (closure pattern R15-D1): `nombre_scope_secret = spark.conf.get("pipelines.parameters.nombreScopeSecret")`, `parametros_sql = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)`, `catalogo_plata = parametros_sql["catalogoPlata"]` (fuente), `catalogo_oro = parametros_sql["catalogoOro"]` (destino, RF-002), `esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")` (fuente), `esquema_oro = spark.conf.get("pipelines.parameters.esquema_oro")` (destino, RF-002), (5) definir diccionario `table_properties` con las 5 propiedades Delta (RF-006): `"delta.enableChangeDataFeed": "true"`, `"delta.autoOptimize.autoCompact": "true"`, `"delta.autoOptimize.optimizeWrite": "true"`, `"delta.deletedFileRetentionDuration": "interval 30 days"`, `"delta.logRetentionDuration": "interval 60 days"`. Todo completamente comentado en espanol (RF-011)

**Checkpoint**: Archivo base creado con closure — las variables `catalogo_plata`, `catalogo_oro`, `esquema_plata`, `esquema_oro` y `table_properties` estan disponibles para ambas funciones decoradas. Las validaciones de parametros (CE-012, CE-013) se ejecutan directamente desde el Pipeline LSDP al inicializar el closure del modulo.

---

## Phase 3: US1 — Vista Materializada Agregada de Comportamiento ATM por Cliente (Prioridad: P1) 🎯 MVP

**Objetivo**: Implementar la vista materializada `comportamiento_atm_cliente` con 6 columnas: `identificador_cliente` (clave de agrupacion) + 5 metricas agregadas condicionales de ATM y pagos al saldo, con `@dp.expect_or_drop` para `identificador_cliente` nulo.

**Prueba Independiente**: Ejecutar pipeline y verificar: (1) vista creada en `{catalogoOro}.{esquema_oro}`, (2) una fila por cliente (dim tipo 1), (3) 5 metricas correctas (2 conteos + 2 promedios + 1 suma), (4) promedios calculados correctamente como media aritmetica, (5) columnas en espanol y snake_case.

### Tests para US1

- [X] T004 [US1] Agregar celdas de prueba TDD para `comportamiento_atm_cliente` en `tests/LSDP_Laboratorio_Basico/NbTestOroClientes.py` — celda markdown separadora + celdas de prueba que validen: (1) la vista existe en `{catalogoOro}.{esquema_oro}` y contiene exactamente 6 columnas con nombres segun data-model.md TABLA A: `identificador_cliente`, `cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente` (CE-008), (2) dimension tipo 1 — una unica fila por `identificador_cliente` sin duplicados (CE-002), (3) para un cliente de prueba con transacciones CATM/DATM/PGSL conocidas, los conteos coinciden exactamente con el numero de transacciones de cada tipo (CE-004), (4) promedios calculados correctamente como media aritmetica de los montos (CE-005), (5) total pagos al saldo es suma exacta de montos PGSL (CE-006), (6) cliente sin transacciones ATM ni PGSL tiene todas las metricas en 0 por las agregaciones condicionales (CE-007), (7) registros con `identificador_cliente` nulo eliminados por `@dp.expect_or_drop` — no existen filas con `identificador_cliente` NULL en el resultado (RF-018), (8) las 6 propiedades Delta configuradas correctamente: enableChangeDataFeed, autoCompact, optimizeWrite, Liquid Cluster por `identificador_cliente`, retenciones 30d y 60d (CE-009)

### Implementacion para US1

- [X] T005 [US1] Implementar funcion `comportamiento_atm_cliente` en `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — (1) celda markdown explicativa de la vista de comportamiento ATM, su fuente (`transacciones_enriquecidas` de plata) y logica de agregacion condicional en una sola pasada, (2) decorador `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente", comment="Vista materializada de metricas agregadas de comportamiento ATM y pagos al saldo por cliente. Fuente: transacciones_enriquecidas de plata. Agrupacion por identificador_cliente con conteos, promedios y sumas condicionales.", table_properties=table_properties, cluster_by=["identificador_cliente"])` (RF-014, RF-015, RF-006), (3) decorador `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")` aplicado sobre el resultado agrupado (RF-018, R14-D1), (4) lectura sin filtros previos: `df_transacciones = spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")` (RF-003, RF-017, R15-D1), (5) agrupacion con 5 metricas condicionales (R13-D1): `df_transacciones.groupBy("identificador_cliente").agg(count(when(col("tipo_transaccion") == "CATM", 1)).alias("cantidad_depositos_atm"), count(when(col("tipo_transaccion") == "DATM", 1)).alias("cantidad_retiros_atm"), coalesce(avg(when(col("tipo_transaccion") == "CATM", coalesce(col("monto_transaccion"), lit(0)))), lit(0)).alias("promedio_monto_depositos_atm"), coalesce(avg(when(col("tipo_transaccion") == "DATM", coalesce(col("monto_transaccion"), lit(0)))), lit(0)).alias("promedio_monto_retiros_atm"), coalesce(spark_sum(when(col("tipo_transaccion") == "PGSL", coalesce(col("monto_transaccion"), lit(0)))), lit(0)).alias("total_pagos_saldo_cliente"))` (RF-016), (6) reordenar: `reordenar_columnas_liquid_cluster(df_agrupado, ["identificador_cliente"])` (RF-007), (7) retornar DataFrame final. Todo en espanol con comentarios detallados (RF-008, RF-011)

**Checkpoint**: Vista `comportamiento_atm_cliente` operativa — 6 columnas, 1 fila por cliente, 5 metricas correctas, `expect_or_drop` activo, Liquid Cluster configurado. Validar CE-001, CE-002, CE-004 a CE-009.

---

## Phase 4: US2 — Vista Materializada de Resumen Integral del Cliente para Consumo (Prioridad: P1)

**Objetivo**: Implementar la vista materializada `resumen_integral_cliente` con 22 columnas: 17 campos seleccionados de plata (identificativos, demograficos, clasificacion, financieros) + 5 metricas de la vista de oro (`comportamiento_atm_cliente`) via LEFT JOIN con coalesce a 0 para clientes sin transacciones.

**Prueba Independiente**: Ejecutar pipeline y verificar: (1) vista creada en `{catalogoOro}.{esquema_oro}`, (2) una fila por cliente, (3) 17 campos de plata correctos, (4) 5 metricas ATM presentes, (5) clientes sin transacciones con metricas en 0.

### Tests para US2

- [X] T006 [US2] Agregar celdas de prueba TDD para `resumen_integral_cliente` en `tests/LSDP_Laboratorio_Basico/NbTestOroClientes.py` — celda markdown separadora + celdas de prueba que validen: (1) la vista existe en `{catalogoOro}.{esquema_oro}` y contiene exactamente 22 columnas con nombres segun data-model.md TABLA B: 5 identificativos (`identificador_cliente`, `huella_identificacion_cliente`, `nombre_completo_cliente`, `tipo_documento_identidad`, `numero_documento_identidad`), 4 demograficos (`segmento_cliente`, `categoria_cliente`, `ciudad_residencia`, `pais_residencia`), 3 clasificacion (`clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria`), 5 financieros (`saldo_disponible`, `saldo_actual`, `limite_credito`, `puntaje_crediticio`, `ingreso_anual_declarado`), 5 metricas ATM (`cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente`) (CE-008), (2) dimension tipo 1 — una unica fila por `identificador_cliente` (CE-003), (3) campos de clasificacion reflejan valores calculados en plata (no recalculados en oro), (4) LEFT JOIN: cliente existente en plata sin transacciones aparece con las 5 metricas ATM en 0 (CE-007), (5) LEFT JOIN: cliente con transacciones tiene metricas ATM con valores correctos coherentes con `comportamiento_atm_cliente`, (6) las 6 propiedades Delta configuradas correctamente (CE-009), (7) Liquid Cluster verificado por `huella_identificacion_cliente` e `identificador_cliente`

### Implementacion para US2

- [X] T007 [US2] Implementar funcion `resumen_integral_cliente` en `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — (1) celda markdown explicativa de la vista de resumen integral, sus 2 fuentes (plata + oro) y la logica de LEFT JOIN con coalesce, (2) decorador `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente", comment="Vista materializada de resumen integral del cliente. Combina datos dimensionales de plata (clientes_saldos_consolidados) con metricas transaccionales agregadas de oro (comportamiento_atm_cliente) via LEFT JOIN. Punto de consumo final del producto de datos.", table_properties=table_properties, cluster_by=["huella_identificacion_cliente", "identificador_cliente"])` (RF-014, RF-015, RF-006), (3) lectura fuente plata: `df_clientes = spark.read.table(f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")` (RF-004, R15-D1), (4) seleccion de 17 columnas de plata: `.select("identificador_cliente", "huella_identificacion_cliente", "nombre_completo_cliente", "tipo_documento_identidad", "numero_documento_identidad", "segmento_cliente", "categoria_cliente", "ciudad_residencia", "pais_residencia", "clasificacion_riesgo_cliente", "categoria_saldo_disponible", "perfil_actividad_bancaria", "saldo_disponible", "saldo_actual", "limite_credito", "puntaje_crediticio", "ingreso_anual_declarado")`, (5) lectura fuente oro: `df_atm = spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")` (R15-D1 — LSDP resuelve dependencia DAG automaticamente dentro del mismo pipeline), (6) LEFT JOIN: `df_clientes.join(df_atm, on="identificador_cliente", how="left")` (RF-004), (7) coalesce a 0 para las 5 metricas del LEFT JOIN con `.withColumn("cantidad_depositos_atm", coalesce(col("cantidad_depositos_atm"), lit(0)))` y analogo para `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente` (RF-016), (8) reordenar: `reordenar_columnas_liquid_cluster(df_resultado, ["huella_identificacion_cliente", "identificador_cliente"])` (RF-007), (9) retornar DataFrame final. Todo en espanol con comentarios detallados (RF-008, RF-011)

**Checkpoint**: Vista `resumen_integral_cliente` operativa — 22 columnas, 1 fila por cliente, LEFT JOIN con coalesce, Liquid Cluster con 2 columnas. Validar CE-001, CE-003, CE-007 a CE-010.

---

## Phase 5: US4 — Suite de Pruebas TDD Completa para la Medalla de Oro (Prioridad: P2)

**Objetivo**: Completar la suite de pruebas con validaciones de integracion, casos borde avanzados y pruebas de cumplimiento que cubran la totalidad de RFs y CEs.

**Prueba Independiente**: Ejecutar notebook completo y verificar que el 100% de las pruebas pasan exitosamente.

- [X] T008 [US4] Completar suite de pruebas integrales en `tests/LSDP_Laboratorio_Basico/NbTestOroClientes.py` — agregar celdas markdown y de prueba para: (1) prueba de integracion end-to-end: verificar que ambas vistas se crearon en el mismo pipeline run y que los datos de `resumen_integral_cliente` son coherentes con `comportamiento_atm_cliente` para un cliente especifico (CE-001), (2) caso borde: cliente con transacciones solo de tipo CATM sin DATM ni PGSL — verificar `cantidad_depositos_atm` > 0 y `promedio_monto_depositos_atm` > 0, mientras `cantidad_retiros_atm` == 0, `promedio_monto_retiros_atm` == 0, `total_pagos_saldo_cliente` == 0, (3) caso borde: `monto_transaccion` nulo en transaccion CATM — verificar que el promedio y la suma manejan nulos correctamente sin errores, (4) caso borde: vistas de plata vacias — verificar que las vistas de oro se crean vacias sin errores (schema correcto, 0 filas), (5) prueba de compatibilidad Serverless: verificar ausencia de `spark.sparkContext` en el codigo fuente de `LsdpOroClientes.py` (RF-010), (6) prueba de import correcto: verificar `from pyspark import pipelines as dp` y ausencia de `import dlt` en el codigo fuente (RF-001), (7) celda markdown de resumen final con conteo total de pruebas ejecutadas y tabla de cobertura RF-001 a RF-018 mapeando cada RF a su(s) prueba(s) correspondiente(s) (CE-011)

---

## Phase 6: Polish y Validacion Cruzada

**Proposito**: Validacion final del pipeline completo (bronce + plata + oro) y ejecucion de suite de pruebas

- [ ] T009 [P] Ejecutar pipeline completo (bronce + plata + oro) en Databricks y verificar ejecucion exitosa segun quickstart.md paso 5: (1) las 2 vistas de oro se crean en `oro_dev.regional`, (2) `comportamiento_atm_cliente` tiene 6 columnas, (3) `resumen_integral_cliente` tiene 22 columnas, (4) cero errores de compatibilidad Serverless, (5) tablas de bronce y vistas de plata no afectadas (CE-010)
- [ ] T010 [P] Ejecutar notebook `tests/LSDP_Laboratorio_Basico/NbTestOroClientes.py` completo y verificar que el 100% de las pruebas pasan exitosamente (CE-011). Documentar resultados en celda markdown final del notebook con fecha de ejecucion y version del pipeline

---

## Dependencies & Execution Order

### Dependencias entre Fases

- **Setup (Phase 1)**: Sin dependencias — puede iniciar inmediatamente
- **US3 (Phase 2)**: Depende de Setup — crea la estructura base con closure de parametros
- **US1 (Phase 3)**: Depende de US3 — agrega primera vista al archivo 🎯 MVP
- **US2 (Phase 4)**: Depende de US1 — `resumen_integral_cliente` lee de `comportamiento_atm_cliente` (R15-D1)
- **US4 (Phase 5)**: Depende de US1 y US2 — pruebas de integracion requieren ambas vistas implementadas
- **Polish (Phase 6)**: Depende de US1, US2 y US4 — ejecucion final completa

### Dependencias entre User Stories

- **US3 (P1)**: Prerequisito base — configuracion dinamica de parametros. Sin dependencia en otras US.
- **US1 (P1)**: Depende de US3 — usa las variables de closure para lectura de plata y escritura en oro 🎯 MVP
- **US2 (P1)**: Depende de US1 — `resumen_integral_cliente` lee de `comportamiento_atm_cliente` via `spark.read.table()` (R15-D1). LSDP resuelve el DAG automaticamente.
- **US4 (P2)**: Depende de US1 y US2 — suite completa requiere ambas vistas para validar integracion y coherencia.

### Dentro de Cada User Story

- Tests TDD primero (DEBEN fallar antes de la implementacion)
- Implementacion despues (hasta que tests pasen)
- Validar checkpoint antes de avanzar a la siguiente fase
- Commit logico despues de cada fase completada

### Oportunidades de Paralelismo

- T009 y T010 pueden ejecutarse en paralelo (ejecucion pipeline vs ejecucion tests — actividades independientes)
- La cadena US3 → US1 → US2 es estrictamente secuencial (dependencia de datos entre vistas)

---

## Ejemplo de Paralelismo: Phase 6

```bash
# Despues de completar US4 (T008), lanzar en paralelo:
Tarea: "Ejecutar pipeline completo en Databricks" (T009)
Tarea: "Ejecutar notebook de pruebas NbTestOroClientes.py" (T010)
```

---

## Estrategia de Implementacion

### MVP Primero (US3 + US1)

1. Completar Phase 1: Setup (Azure SQL + pipeline config)
2. Completar Phase 2: US3 (closure de parametros — estructura base)
3. Completar Phase 3: US1 (vista `comportamiento_atm_cliente` — TDD)
4. **PARAR Y VALIDAR**: Ejecutar tests de US1, verificar vista en Unity Catalog
5. Deploy/demo si listo — la vista de metricas ATM ya es consumible directamente

### Entrega Incremental

1. Setup + US3 → Infraestructura de parametros lista
2. Agregar US1 → Test independiente → Deploy (**MVP — metricas ATM consumibles**)
3. Agregar US2 → Test independiente → Deploy (resumen integral del cliente listo)
4. Completar US4 → Suite TDD completa → Validacion formal
5. Cada historia agrega valor sin romper las anteriores — backward compatible

### Ruta Critica

```text
T001 → T002/T003 → T004 → T005 → T006 → T007 → T008 → T009/T010
Setup    US3        US1-test  US1-impl  US2-test  US2-impl  US4     Polish
```

---

## Notas

- [P] = archivos diferentes, sin dependencias pendientes
- [Story] vincula tarea con historia de usuario para trazabilidad RF → CE
- Ambas vistas materializadas en un unico archivo `LsdpOroClientes.py` (RF-012)
- Todas las pruebas en un unico notebook `NbTestOroClientes.py` (RF-013)
- V5 NO modifica ningun archivo existente — solo agrega 2 archivos nuevos
- La funcion `leer_parametros_azure_sql` se reutiliza desde V4 sin cambios (CE-013)
- El pipeline existente (bronce V3 + plata V4) sigue funcionando sin modificaciones
- Commit logico recomendado despues de cada fase completada
