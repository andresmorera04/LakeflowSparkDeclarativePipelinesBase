# Tasks: LSDP Medalla de Plata (Version 4)

**Input**: Artefactos de diseño en `/specs/004-lsdp-medalla-plata/`
**Prerequisites**: plan.md (requerido), spec.md (requerido — 20 RFs, 13 CEs, 4 historias de usuario), research.md (R9-R12, decisiones aprobadas), data-model.md (175+65 columnas), contracts/pipeline-lsdp-plata.md, quickstart.md

**Tests**: Incluidos — RF-015 y US4 requieren suite TDD obligatoria. Escribir tests primero (TDD), verificar que fallan, luego implementar.

**Organizacion**: Tareas agrupadas por historia de usuario. US3 (parametros) es prerequisito de US1 y US2.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias pendientes)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3, US4)
- Rutas exactas incluidas en cada descripcion

---

## Phase 1: Setup

**Proposito**: Configuracion de prerequisitos de infraestructura para la medalla de plata

- [ ] T001 Insertar clave `catalogoPlata` con valor `"plata_dev"` en tabla `dbo.Parametros` de Azure SQL via SQLTools y agregar parametro `esquema_plata` con valor `"regional"` a la configuracion del pipeline LSDP en Databricks (ver quickstart.md pasos 1 y 6)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Proposito**: Refactorizacion de la funcion de lectura de parametros Azure SQL (RF-019). DEBE completarse antes de cualquier historia de usuario — los scripts de plata dependen del diccionario completo retornado.

**⚠️ CRITICO**: Ninguna tarea de US1, US2 o US4 puede comenzar hasta que esta fase este completa.

- [X] T002 Refactorizar funcion `leer_parametros_azure_sql` en `src/LSDP_Laboratorio_Basico/utilities/LsdpConexionAzureSql.py` — eliminar la lista `claves_requeridas` de 4 claves fijas (linea 112), eliminar la validacion `claves_faltantes` (lineas 113-121), eliminar el return filtrado (linea 128) y retornar directamente `params_dict` con todas las claves de dbo.Parametros (RF-019). Los scripts de bronce V3 siguen funcionando sin modificacion porque acceden a las mismas claves del diccionario

**Checkpoint**: Funcion refactorizada — scripts de bronce V3 deben seguir funcionando sin cambios. Los scripts de plata podran acceder a la clave `catalogoPlata` del diccionario retornado.

---

## Phase 3: US3 — Lectura Dinamica de Parametros de Plata desde Azure SQL y Pipeline (Prioridad: P1)

**Objetivo**: Validar que la funcion refactorizada retorna TODAS las claves de dbo.Parametros (incluyendo `catalogoPlata`) y que los scripts de bronce V3 mantienen compatibilidad.

**Prueba Independiente**: Ejecutar NbTestConexionAzureSqlV4 y verificar: (1) el diccionario contiene las 4 claves de bronce V3 mas `catalogoPlata`, (2) la funcion no filtra ni valida claves especificas, (3) los scripts de bronce siguen accediendo a sus claves normalmente.

### Tests para US3

- [X] T003 [US3] Crear notebook de prueba TDD en `tests/LSDP_Laboratorio_Basico/NbTestConexionAzureSqlV4.py` — validar que `leer_parametros_azure_sql` retorna diccionario completo sin filtro, que la clave `catalogoPlata` esta presente con valor correcto, que las 4 claves de bronce V3 (`catalogoBronce`, `contenedorBronce`, `datalake`, `DirectorioBronce`) siguen presentes, y que no se lanza ValueError por claves faltantes (RF-019, CE-013)

**Checkpoint**: Refactorizacion validada — la funcion retorna todas las claves. Compatible con V3.

---

## Phase 4: US1 — Vista Materializada Consolidada de Clientes y Saldos (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear la vista materializada `clientes_saldos_consolidados` que consolida cmstfl + blncfl como Dimension Tipo 1 con 175 columnas (71 cmstfl + 100 blncfl + 4 calculados), 3 campos CASE + 1 SHA256, validacion `@dp.expect_or_drop` para CUSTID nulo, y Liquid Cluster.

**Prueba Independiente**: Ejecutar el pipeline y verificar: (1) vista creada en `{catalogoPlata}.{esquema_plata}`, (2) una unica fila por cliente (dim tipo 1), (3) 175 columnas con nombres en espanol, (4) campos calculados correctos, (5) registros con CUSTID nulo eliminados.

### Tests para US1

- [X] T004 [US1] Crear notebook de prueba TDD en `tests/LSDP_Laboratorio_Basico/NbTestPlataClientesSaldos.py` — validar: (1) estructura de 175 columnas con nombres en espanol segun data-model.md TABLAS A, B y C, (2) dimension tipo 1 — una fila por CUSTID con datos mas recientes, (3) 3 campos CASE (`clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria`) con valores correctos segun umbrales RF-004, (4) campo `huella_identificacion_cliente` SHA256 consistente (RF-005), (5) propiedades Delta (6 propiedades RF-008), (6) registros con CUSTID nulo eliminados por expect_or_drop (RF-018), (7) LEFT JOIN correcto — clientes sin saldos tienen campos blncfl nulos (CE-002, CE-003, CE-004)

### Implementacion para US1

- [X] T005 [US1] Crear estructura base del script `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — (1) imports de utilities (`leer_parametros_azure_sql`, `reordenar_columnas_liquid_cluster`) y LSDP (`from pyspark import pipelines as dp`, `pyspark.sql.functions`, `pyspark.sql.window`), (2) lectura de parametros a nivel de modulo via closure pattern: `nombre_scope_secret`, `parametros_sql`, `catalogo_plata = parametros_sql["catalogoPlata"]`, `esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")`, (3) decoradores `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados", comment="...", table_properties={5 props de dict RF-008}, cluster_by=["huella_identificacion_cliente", "identificador_cliente"])` y `@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")` (RF-016, RF-018, RF-020, R11-D1), (4) window functions: `Window.partitionBy("CUSTID").orderBy(desc("FechaIngestaDatos"))` con `row_number() == 1` para cmstfl y blncfl (RF-003, R10-D1), (5) LEFT JOIN `cmstfl_latest` a `blncfl_latest` por CUSTID usando alias `c` y `b` (R10-D2), (6) excluir columnas `_rescued_data` de ambas tablas y columnas de particion (`año`, `mes`, `dia`)

- [X] T006 [US1] Implementar renombrado completo de 171 columnas a espanol en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — renombrar 71 columnas de cmstfl segun data-model.md TABLA A (ej: `col("c.CUSTID").alias("identificador_cliente")`, `col("c.CUSTNM").alias("nombre_completo_cliente")`, ...) y 100 columnas de blncfl segun TABLA B (ej: `col("b.ACCTID").alias("identificador_cuenta")`, ...), resolviendo 12 columnas duplicadas con calificadores descriptivos segun R10-D2 y H10.3: SGMNT→`segmento_cliente`/`segmento_cuenta`, RISKLV→`nivel_riesgo_cliente`/`nivel_riesgo_cuenta`, BRNCOD→`codigo_sucursal_cliente`/`codigo_sucursal_cuenta`, BRNNM→`nombre_sucursal_cliente`/`nombre_sucursal_cuenta`, ACCTST→`estado_cuenta_cliente`/`estado_cuenta_saldo`, OPNDT→`fecha_apertura_cuenta`/`fecha_apertura_cuenta_saldo`, LSTTRX→`fecha_ultima_transaccion`/`fecha_ultima_transaccion_saldo`, RVWDT→`fecha_revision_kyc`/`fecha_revision_cuenta`, CNCLDT→`fecha_cancelacion_maestro`/`fecha_cancelacion_cuenta`, PRMDT→`fecha_promocion_segmento`/`fecha_promocion_cuenta`, CHGDT→`fecha_cambio_estado`/`fecha_cambio_condiciones`, FechaIngestaDatos→`fecha_ingesta_maestro`/`fecha_ingesta_saldo` (RF-010)

- [X] T007 [US1] Implementar 4 campos calculados y reordenar Liquid Cluster en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — (1) campo `clasificacion_riesgo_cliente` tipo CASE con 5 umbrales secuenciales segun RF-004 usando `when`/`otherwise` con insumos RISKLV+CRDSCR+OVDST, (2) campo `categoria_saldo_disponible` tipo CASE con 5 umbrales segun RF-004 usando AVLBAL+CRDLMT+SGMNT, (3) campo `perfil_actividad_bancaria` tipo CASE con 5 umbrales segun RF-004 usando datediff(current_date(),LSTTRX)+TTLPRD+ACCTST, (4) campo `huella_identificacion_cliente` via `sha2(col("identificador_cliente").cast("string"), 256)` (RF-005), (5) invocar `reordenar_columnas_liquid_cluster(df, ["huella_identificacion_cliente", "identificador_cliente"])` (RF-009), (6) retornar DataFrame final

**Checkpoint**: Vista consolidada operativa — 175 columnas, dim tipo 1, campos calculados correctos, expect_or_drop activo. Validar CE-001 a CE-004, CE-007 a CE-009, CE-011.

---

## Phase 5: US2 — Vista Materializada Transaccional Enriquecida (Prioridad: P1)

**Objetivo**: Crear la vista materializada `transacciones_enriquecidas` con 65 columnas (61 trxpfl + 4 calculados numericos), sin filtros en la lectura, con Liquid Cluster y manejo seguro de nulos.

**Prueba Independiente**: Ejecutar el pipeline y verificar: (1) vista creada en `{catalogoPlata}.{esquema_plata}`, (2) 65 columnas con nombres en espanol, (3) 4 campos calculados numericos correctos, (4) sin errores por nulos o division por cero.

### Tests para US2

- [X] T008 [P] [US2] Crear notebook de prueba TDD en `tests/LSDP_Laboratorio_Basico/NbTestPlataTransacciones.py` — validar: (1) estructura de 65 columnas con nombres en espanol segun data-model.md TABLAS D y E, (2) 4 campos calculados numericos (`monto_neto_comisiones`, `porcentaje_comision_sobre_monto`, `variacion_saldo_transaccion`, `indicador_impacto_financiero`) con valores correctos segun formulas RF-007, (3) manejo seguro de nulos con `coalesce` a 0 (RF-017), (4) sin errores division por cero en `porcentaje_comision_sobre_monto`, (5) propiedades Delta (6 propiedades RF-008), (6) lectura sin filtros (RF-006) (CE-005, CE-006, CE-007)

### Implementacion para US2

- [X] T009 [US2] Crear script completo `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` — (1) imports de utilities (`leer_parametros_azure_sql`, `reordenar_columnas_liquid_cluster`) y LSDP (`from pyspark import pipelines as dp`, `pyspark.sql.functions`), (2) lectura parametros modulo via closure pattern (mismo patron que LsdpPlataClientesSaldos), (3) `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas", comment="...", table_properties={5 props de dict RF-008}, cluster_by=["fecha_transaccion", "identificador_cliente", "tipo_transaccion"])` (RF-016, RF-020), (4) lectura batch `spark.read.table("bronce_dev.regional.trxpfl")` SIN filtros (RF-006), (5) excluir `_rescued_data` y columnas particion (`año`, `mes`, `dia`), (6) renombrar 61 columnas a espanol segun data-model.md TABLA D, (7) agregar 4 campos calculados numericos segun RF-007 y TABLA E: `monto_neto_comisiones = NETAMT - (FEEAMT + TAXAMT)`, `porcentaje_comision_sobre_monto = (FEEAMT / ORGAMT) * 100` con manejo division por cero, `variacion_saldo_transaccion = abs(BLNAFT - BLNBFR)`, `indicador_impacto_financiero = TRXAMT + FEEAMT + TAXAMT + PNLAMT` — todos con `coalesce` a 0 para nulos (RF-017), (8) invocar `reordenar_columnas_liquid_cluster(df, ["fecha_transaccion", "identificador_cliente", "tipo_transaccion"])` (RF-009), (9) retornar DataFrame

**Checkpoint**: Vista transaccional operativa — 65 columnas, campos calculados correctos, nulos manejados. Validar CE-005 a CE-009, CE-011.

---

## Phase 6: US4 — Suite de Pruebas TDD Completa (Prioridad: P2)

**Objetivo**: Verificar que la suite completa de pruebas TDD pasa al 100%, cubriendo RF-001 a RF-020 con al menos una prueba por requerimiento.

**Prueba Independiente**: Ejecutar los 3 notebooks de prueba y verificar que el 100% de las pruebas pasan exitosamente.

- [X] T010 [US4] Ejecutar y validar suite completa de pruebas TDD — ejecutar `tests/LSDP_Laboratorio_Basico/NbTestConexionAzureSqlV4.py`, `tests/LSDP_Laboratorio_Basico/NbTestPlataClientesSaldos.py` y `tests/LSDP_Laboratorio_Basico/NbTestPlataTransacciones.py` en Databricks. Verificar: (1) 100% de pruebas pasan (CE-010), (2) cobertura de RF-001 a RF-020, (3) campos calculados incorrectos producen fallo especifico con campo y valor esperado vs obtenido, (4) propiedades Delta faltantes producen fallo indicando propiedad ausente

**Checkpoint**: Suite TDD validada — todos los requerimientos cubiertos y verificados.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Proposito**: Validacion integral del pipeline y verificacion de criterios de exito

- [X] T011 [P] Ejecutar validacion del quickstart.md pasos 1-7 en Databricks — confirmar pipeline completo bronce+plata ejecuta sin errores en Computo Serverless (CE-009), vistas publicadas en `plata_dev.regional` (CE-001)
- [X] T012 [P] Ejecutar comandos de verificacion SQL en Databricks segun quickstart.md — `SHOW TABLES IN plata_dev.regional`, `DESCRIBE TABLE EXTENDED` para ambas vistas, verificar dim tipo 1 (zero duplicates por CUSTID), `SHOW TBLPROPERTIES` para las 6 propiedades Delta, verificar actualizacion transaccional ejecutando SELECT COUNT(*) en transacciones_enriquecidas antes y despues de agregar datos nuevos a bronce y re-ejecutar pipeline (CE-002, CE-006, CE-008)

---

## Dependencies & Execution Order

### Dependencias entre Fases

- **Setup (Phase 1)**: Sin dependencias — prerequisito de infraestructura (Azure SQL + pipeline config)
- **Foundational (Phase 2)**: Depende de Phase 1 — BLOQUEA todas las historias de usuario
- **US3 (Phase 3)**: Depende de Phase 2 — valida la refactorizacion de RF-019
- **US1 (Phase 4)**: Depende de Phase 3 — requiere `catalogoPlata` y `esquema_plata` disponibles via closure
- **US2 (Phase 5)**: Depende de Phase 3 — requiere `catalogoPlata` y `esquema_plata` disponibles via closure. **Puede ejecutarse en paralelo con US1** (archivos diferentes)
- **US4 (Phase 6)**: Depende de US1, US2 y US3 completados — ejecuta suite TDD completa
- **Polish (Phase 7)**: Depende de US4 — validacion integral final

### Dependencias entre Historias de Usuario

- **US3 (P1)**: BLOQUEA a US1 y US2 — la funcion refactorizada y los parametros son prerequisito
- **US1 (P1)**: Depende de US3. **Independiente de US2** — pueden ejecutarse en paralelo
- **US2 (P1)**: Depende de US3. **Independiente de US1** — pueden ejecutarse en paralelo
- **US4 (P2)**: Depende de US1 + US2 + US3 — requiere todas las implementaciones para ejecutar suite

### Dentro de Cada Historia de Usuario

- Tests TDD se escriben PRIMERO y deben FALLAR antes de la implementacion
- Estructura base del script antes de renombrado de columnas
- Renombrado de columnas antes de campos calculados
- Campos calculados antes de Liquid Cluster
- Script completo antes de mover al siguiente story

### Oportunidades de Paralelismo

- T008 [P]: El test de US2 puede ejecutarse en paralelo con tareas de US1 (archivos diferentes)
- **US1 y US2 en paralelo**: Despues de Phase 3 (US3), Phase 4 y Phase 5 pueden ejecutarse simultaneamente
- T011, T012 [P]: Las tareas de polish pueden ejecutarse en paralelo entre si

---

## Parallel Example: US1 + US2

```text
Phase 3 completada (US3 validado)
        │
        ├──────────────────────┬──────────────────────┐
        ▼                      ▼                      │
   [US1] T004               [US2] T008 [P]            │
   Test TDD consolidado      Test TDD transaccional   │
        │                      │                      │
        ▼                      ▼                      │
   [US1] T005               [US2] T009                │
   Script base + window       Script completo          │
        │                      │                      │
        ▼                      │                      │
   [US1] T006                  │                      │
   Renombrado 171 cols         │                      │
        │                      │                      │
        ▼                      │                      │
   [US1] T007                  │                      │
   Campos calc + LC            │                      │
        │                      │                      │
        ├──────────────────────┘                      │
        ▼                                             │
   [US4] T010                                         │
   Validar suite TDD                                  │
        │                                             │
        ▼                                             │
   T011 [P] + T012 [P]                               │
   Polish & verificacion                              │
```

---

## Implementation Strategy

### MVP First (US3 + US1)

1. Completar Phase 1: Setup (config pipeline + Azure SQL)
2. Completar Phase 2: Foundational (RF-019 refactorizacion)
3. Completar Phase 3: US3 (validar refactorizacion)
4. Completar Phase 4: US1 (vista consolidada — **pieza central del modelo de plata**)
5. **STOP y VALIDAR**: Ejecutar NbTestPlataClientesSaldos → dim tipo 1, 175 cols, campos calculados
6. Continuar con US2 si MVP aprobado

### Entrega Incremental

1. Setup + Foundational + US3 → Infraestructura de parametros lista
2. Agregar US1 → Vista consolidada operativa → Validar independientemente (MVP)
3. Agregar US2 → Vista transaccional operativa → Validar independientemente
4. Agregar US4 → Suite TDD completa → 100% pruebas pasan
5. Cada historia agrega valor sin romper las anteriores

### Estrategia Paralela

Con multiples desarrolladores despues de Phase 3:
- **Desarrollador A**: US1 (T004 → T005 → T006 → T007)
- **Desarrollador B**: US2 (T008 → T009)
- Ambas historias se completan e integran independientemente

---

## Resumen

| Fase | Historia | Tareas | Archivos |
|------|----------|--------|----------|
| 1 — Setup | — | T001 | Config Databricks + Azure SQL |
| 2 — Foundational | — | T002 | `utilities/LsdpConexionAzureSql.py` |
| 3 — US3 | P1 | T003 | `tests/.../NbTestConexionAzureSqlV4.py` |
| 4 — US1 | P1 🎯 | T004–T007 | `tests/.../NbTestPlataClientesSaldos.py`, `transformations/LsdpPlataClientesSaldos.py` |
| 5 — US2 | P1 | T008–T009 | `tests/.../NbTestPlataTransacciones.py`, `transformations/LsdpPlataTransacciones.py` |
| 6 — US4 | P2 | T010 | Ejecucion 3 notebooks de prueba |
| 7 — Polish | — | T011–T012 | Validacion quickstart + SQL |
| **Total** | | **12 tareas** | **6 archivos nuevos/modificados** |

---

## Notes

- Todas las tareas referencian artefactos especificos: data-model.md (TABLAS A-E), spec.md (RF-001 a RF-020), research.md (R9-D1 a R11-D1), contracts/pipeline-lsdp-plata.md
- El patron closure se replica del codigo existente en `LsdpBronceCmstfl.py` (V3)
- Las columnas `_rescued_data` y las columnas de particion (`año`, `mes`, `dia`) se excluyen de las vistas de plata
- La funcion `reordenar_columnas_liquid_cluster` de V3 se reutiliza sin modificacion (RF-009)
- `refresh_policy` NO se configura explicitamente — se deja `auto` por defecto (R9-D1)
- La constraint de `@dp.expect_or_drop` usa el nombre en espanol post-renombrado: `"identificador_cliente IS NOT NULL"` (R11-D1)
- RF-013 aplica de forma transversal a todas las tareas de implementacion: cada celda DEBE tener bloque markdown explicativo y el codigo DEBE estar completamente comentado al detalle
