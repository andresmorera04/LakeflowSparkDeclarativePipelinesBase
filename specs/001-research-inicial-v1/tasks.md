# Tasks: Research Inicial y Decisiones Clave - Version 1

**Input**: Design documents from `/specs/001-research-inicial-v1/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, quickstart.md

**Tests**: No aplica para V1. La Version 1 es exclusivamente de research y documentacion (RF-008). No se genera codigo fuente ni pruebas.

**Organization**: Tareas agrupadas por historia de usuario para habilitar implementacion y validacion independiente de cada story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3, US4, US5)
- Rutas exactas incluidas en cada descripcion

## Path Conventions

- **Documentacion del feature**: `specs/001-research-inicial-v1/`
- **Constitucion**: `.specify/memory/constitution.md`
- **V1 no genera codigo**: No hay rutas src/, tests/, scripts/

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Creacion del directorio del feature e inicializacion de artefactos base

> **Nota**: Las tareas de Phase 1 y Phase 2 reflejan el trabajo ya realizado por los comandos speckit previos (/speckit.specify, /speckit.plan, /speckit.clarify). Al iniciar `/speckit.implement`, estas tareas DEBEN marcarse como completadas automaticamente.

- [X] T001 Crear directorio del feature specs/001-research-inicial-v1/ y subdirectorio specs/001-research-inicial-v1/checklists/
- [X] T002 Generar spec.md con 5 historias de usuario (P1-P3), 11 requerimientos funcionales (RF-001 a RF-011), 9 criterios de exito (CE-001 a CE-009), 5 casos borde y 6 supuestos a partir de SYSTEM.md en specs/001-research-inicial-v1/spec.md
- [X] T003 [P] Generar checklists/requirements.md con criterios de validacion de calidad de la especificacion en specs/001-research-inicial-v1/checklists/requirements.md

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Artefactos de planificacion que DEBEN completarse antes de iniciar cualquier historia de usuario de research

**⚠️ CRITICO**: Ningun research puede comenzar hasta que plan.md y el esqueleto de research.md existan

- [X] T004 Generar plan.md con contexto tecnico (pyspark.pipelines, Serverless, Unity Catalog), verificacion de constitucion (10/10 principios PASA) y estructura de proyecto V2+ en specs/001-research-inicial-v1/plan.md
- [X] T005 [P] Inicializar research.md con esqueleto de 5 areas de investigacion (LSDP, AS400, Nombres, Extensiones, Azure SQL), secciones de hallazgos vacias y tabla de decisiones consolidadas en specs/001-research-inicial-v1/research.md
- [X] T006 [P] Generar data-model.md con 5 entidades (Maestro, Transaccional, Saldos, Documento Research, Registro Decisiones), relaciones (1:N Maestro-Transaccional, 1:1 Maestro-Saldos), reglas de validacion RV-01 a RV-09 y diagrama de flujo por medalla en specs/001-research-inicial-v1/data-model.md
- [X] T007 [P] Generar quickstart.md con guia rapida del feature, tabla de artefactos, flujo de trabajo para el usuario y mapa de dependencias con V2-V5 en specs/001-research-inicial-v1/quickstart.md

**Checkpoint**: Estructura documental lista — el research puede comenzar

---

## Phase 3: User Story 1 - Research de Lakeflow Spark Declarative Pipelines (Priority: P1) 🎯 MVP

**Goal**: Investigar a fondo las capacidades, limitaciones, decoradores, APIs y compatibilidad de LSDP con Computo Serverless de Azure Databricks

**Independent Test**: Verificar que research.md seccion Research 1 contiene: (1) listado de decoradores de pyspark.pipelines, (2) compatibilidad con Serverless confirmada, (3) AutoLoader para Bronce, (4) vistas materializadas para Plata/Oro, (5) create_auto_cdc_flow, (6) propiedades Delta, (7) tabla comparativa DLT vs LSDP

### Implementation for User Story 1

- [X] T008 [US1] Investigar documentacion oficial de pyspark.pipelines desde https://learn.microsoft.com/es-es/azure/databricks/ldp/ y https://docs.databricks.com/aws/en/ldp/developer/python-ref y documentar modulo oficial, importacion correcta y diferencia con dlt legado (H1.1) en specs/001-research-inicial-v1/research.md
- [X] T009 [US1] Documentar tabla completa de decoradores y APIs disponibles: @dp.table, @dp.materialized_view, @dp.temporary_view, @dp.append_flow, @dp.expect, dp.create_auto_cdc_flow, dp.create_streaming_table, dp.create_sink (H1.2) en specs/001-research-inicial-v1/research.md
- [X] T010 [US1] Documentar parametros de @dp.table (name, comment, table_properties, cluster_by, schema, private, etc.), patrones de lectura (spark.read.table, spark.readStream, AutoLoader cloudFiles) y create_auto_cdc_flow con Dimension Tipo 1 (SCD Type 1) / Tipo 2 (H1.3-H1.5) en specs/001-research-inicial-v1/research.md
- [X] T011 [US1] Documentar propiedades Delta soportadas (Change Data Feed, autoOptimize, Liquid Cluster, retention), tabla comparativa DLT vs LSDP, estrategia de tablas multi-catalogo via nombres completos en parametro name (H1.6-H1.7, H1.10) y viabilidad de la estrategia de agregacion Oro con @dp.materialized_view + groupBy + agg para cumplir CE-009 en specs/001-research-inicial-v1/research.md
- [X] T012 [US1] Documentar compatibilidad con Serverless, limitaciones (pivot no soportado, pyspark.pipelines solo en contexto de pipeline, limite 1000 archivos, materialized views sin time travel), buenas practicas de codigo LSDP (H1.8-H1.9) en specs/001-research-inicial-v1/research.md
- [X] T013 [US1] Presentar 5 decisiones al usuario (R1-D1 biblioteca LSDP, R1-D2 CDC con create_auto_cdc_flow, R1-D3 multi-catalogo, R1-D4 pivot no necesario, R1-D5 propiedades Delta) con recomendacion IA y registrar aprobacion en specs/001-research-inicial-v1/research.md

**Checkpoint**: Research LSDP completo — se puede validar independientemente que todos los hallazgos H1.1-H1.10 y 5 decisiones estan documentados

---

## Phase 4: User Story 2 - Research de Estructura de Datos AS400 Bancaria (Priority: P1)

**Goal**: Investigar la estructura real de tablas AS400 bancarias (Maestro, Transaccional, Saldos) incluyendo campos, tipos y catalogos de transacciones

**Independent Test**: Verificar que research.md seccion Research 2 contiene: (1) 70 campos de Maestro con nombres AS400, (2) 60 campos de Transaccional, (3) 100 campos de Saldos, (4) catalogo de 15 tipos de transacciones, (5) relaciones entre tablas

### Implementation for User Story 2

- [X] T014 [US2] Investigar convenciones AS400 bancarias (nombres <= 10 caracteres, abreviados) y documentar estructura completa del Maestro de Clientes: 70 campos (42 textuales CHAR, 18 fechas DATE, 10 numericos NUMERIC) con tabla detallada campo-tipo-descripcion (H2.1) en specs/001-research-inicial-v1/research.md
- [X] T015 [US2] Documentar estructura completa del Transaccional: 60 campos (30 numericos NUMERIC, 21 fechas DATE/TIMESTAMP, 9 textuales CHAR) con tabla detallada campo-tipo-descripcion (H2.2) en specs/001-research-inicial-v1/research.md
- [X] T016 [P] [US2] Documentar catalogo de 15 tipos de transacciones bancarias con codigo (CATM, DATM, TEXT, TINT, PGSL, ADSL, PGSV, CMPR, DPST, RTRO, DMCL, INTR, CMSN, NMNA, IMPT), nombre y descripcion (H2.3) en specs/001-research-inicial-v1/research.md
- [X] T017 [US2] Documentar estructura completa de Saldos de Clientes: 100 campos (30 textuales CHAR, 35 numericos NUMERIC, 35 fechas DATE) con tabla detallada campo-tipo-descripcion (H2.4) en specs/001-research-inicial-v1/research.md
- [X] T018 [US2] Presentar 4 decisiones al usuario (R2-D1 Maestro 70 campos, R2-D2 Transaccional 60 campos, R2-D3 catalogo 15 tipos, R2-D4 Saldos 100 campos) con recomendacion IA y registrar aprobacion en specs/001-research-inicial-v1/research.md

**Checkpoint**: Research AS400 completo — se puede validar que las 3 tablas tienen la distribucion exacta de campos y los 15 tipos de transaccion estan catalogados

---

## Phase 5: User Story 3 - Research de Nombres Hebreos, Egipcios e Ingleses (Priority: P2)

**Goal**: Compilar listas verificadas de nombres y apellidos no latinos para la simulacion del Maestro de Clientes

**Independent Test**: Verificar que research.md seccion Research 3 contiene: (1) 100 nombres hebreos + 50 apellidos, (2) 100 nombres egipcios + 50 apellidos, (3) 100 nombres ingleses + 50 apellidos, (4) verificacion de exclusion de origen latino, (5) fuentes de referencia

### Implementation for User Story 3

- [X] T019 [P] [US3] Compilar 100 nombres hebreos (50 masculinos + 50 femeninos) y 50 apellidos hebreos de fuentes etimologicas verificadas (Behind the Name, Jewish Virtual Library, Torah/Tanakh) en specs/001-research-inicial-v1/research.md (H3.1)
- [X] T020 [P] [US3] Compilar 100 nombres egipcios (50 masculinos + 50 femeninos) y 50 apellidos egipcios de fuentes verificadas (Ancient Egypt Online, textos historicos) en specs/001-research-inicial-v1/research.md (H3.2)
- [X] T021 [P] [US3] Compilar 100 nombres ingleses (50 masculinos + 50 femeninos) y 50 apellidos ingleses de fuentes verificadas (UK Office for National Statistics) en specs/001-research-inicial-v1/research.md (H3.3)
- [X] T022 [US3] Verificar exclusion de nombres de origen latino en los 300 nombres + 150 apellidos, documentar nombres ambiguos (Daniel, Ruth, Sarah como hebreos) y presentar decisiones R3-D1 y R3-D2 para aprobacion en specs/001-research-inicial-v1/research.md

**Checkpoint**: Catalogos de nombres completos — se valida CE-005 (300 nombres + 150 apellidos, cero latinos)

---

## Phase 6: User Story 4 - Research de Extensiones de Databricks para VS Code (Priority: P2)

**Goal**: Investigar capacidades y limitaciones de las extensiones de Databricks para VS Code para validar la estrategia de testing TDD

**Independent Test**: Verificar que research.md seccion Research 4 contiene: (1) version y capacidades de Databricks Extension, (2) capacidades de SQLTools Driver, (3) estrategia de testing mixta documentada, (4) configuracion requerida para el workspace

### Implementation for User Story 4

- [X] T023 [P] [US4] Investigar Databricks Extension for Visual Studio Code (databricks.databricks): navegacion del workspace, ejecucion de notebooks, depuracion, integracion Unity Catalog, soporte Serverless y documentar en specs/001-research-inicial-v1/research.md (H4.1)
- [X] T024 [P] [US4] Investigar Databricks Driver for SQLTools (databricks.sqltools-databricks-driver): ejecucion SQL, exploracion de catalogos, autocompletado, historial y documentar en specs/001-research-inicial-v1/research.md (H4.2)
- [X] T025 [US4] Documentar estrategia de testing mixta (pytest local para utilities/ + notebooks en Databricks para pipeline LSDP), limitacion clave de pyspark.pipelines solo en contexto de pipeline, y configuracion requerida del workspace (H4.3-H4.4) en specs/001-research-inicial-v1/research.md
- [X] T026 [US4] Presentar 2 decisiones al usuario (R4-D1 estrategia testing mixta, R4-D2 autenticacion Azure AD + PAT respaldo) con recomendacion IA y registrar aprobacion en specs/001-research-inicial-v1/research.md

**Checkpoint**: Research de extensiones completo — se puede validar que la estrategia de testing es viable y la configuracion esta documentada

---

## Phase 7: User Story 6 - Research de Patron de Conexion Azure SQL via Secretos (Priority: P2)

**Goal**: Investigar y validar el patron de conexion a Azure SQL Server via secretos de Databricks, confirmando viabilidad en Computo Serverless

**Independent Test**: Verificar que research.md seccion Research 5 contiene: (1) patron de conexion con 2 secretos documentado, (2) viabilidad en Serverless confirmada, (3) uso dentro de LSDP documentado, (4) restricciones conocidas

### Implementation for User Story 6

- [X] T027 [US6] Investigar patron de conexion Azure SQL via secretos (sr-jdbc-asql-asqlmetadatos-adminpd + sr-asql-asqlmetadatos-adminpd) con spark.read.format("jdbc"), confirmar viabilidad en Serverless y documentar uso dentro de LSDP (H5.1-H5.3) en specs/001-research-inicial-v1/research.md
- [X] T028 [US6] Presentar 2 decisiones al usuario (R5-D1 patron conexion Azure SQL, R5-D2 encapsulamiento en utilities/) con recomendacion IA y registrar aprobacion en specs/001-research-inicial-v1/research.md

**Checkpoint**: Research Azure SQL completo — se puede validar que el patron de conexion y la viabilidad en Serverless estan documentados

---

## Phase 8: User Story 5 - Consolidacion de Decisiones y Validacion de Constitucion (Priority: P3)

**Goal**: Consolidar todas las decisiones del research, validar robustez de la constitucion y confirmar estado listo para versiones V2-V5

**Independent Test**: Verificar que: (1) cada hallazgo tiene decision asociada aprobada, (2) tabla consolidada con 15/15 decisiones CERRADA, (3) constitucion validada contra decisiones, (4) cero ambiguedades pendientes

### Implementation for User Story 5

- [X] T029 [US5] Generar tabla de registro de decisiones consolidado con las 15 decisiones (R1-D1 a R5-D2), estado CERRADA y fecha de aprobacion en specs/001-research-inicial-v1/research.md
- [X] T030 [US5] Validar constitucion contra todas las decisiones aprobadas y actualizar .specify/memory/constitution.md si algun hallazgo lo requiere (con aprobacion del usuario)
- [X] T031 [US5] Confirmar 15/15 decisiones APROBADA, verificar que no existen ambiguedades pendientes y documentar estado listo como insumo para las versiones V2-V5 en specs/001-research-inicial-v1/research.md

**Checkpoint**: Consolidacion completa — CE-003 (100% decisiones aprobadas), CE-006 (constitucion actualizada), CE-007 (documento listo como insumo)

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Actualizacion final de artefactos dependientes y validacion cruzada de criterios de exito

- [X] T032 [P] Actualizar data-model.md con estructuras finales aprobadas (70+60+100 campos) y flujo por medalla confirmado en specs/001-research-inicial-v1/data-model.md
- [X] T033 [P] Actualizar quickstart.md reflejando las 15 decisiones aprobadas y mapa de dependencias V2-V5 en specs/001-research-inicial-v1/quickstart.md
- [X] T034 Ejecutar validacion de checklists/requirements.md y confirmar todos los items aprobados en specs/001-research-inicial-v1/checklists/requirements.md
- [X] T035 Validacion final: verificar cumplimiento de CE-001 a CE-009 (incluyendo CE-002: contar al menos 3 fuentes oficiales por seccion de research), confirmar RF-008 (cero archivos .py generados) y que todos los artefactos son consistentes entre si

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Sin dependencias — puede iniciar inmediatamente
- **Foundational (Phase 2)**: Depende de Setup — BLOQUEA todas las historias de usuario
- **US1 LSDP (Phase 3)**: Depende de Phase 2 — No depende de otras historias
- **US2 AS400 (Phase 4)**: Depende de Phase 2 — No depende de otras historias
- **US3 Nombres (Phase 5)**: Depende de Phase 2 — No depende de otras historias
- **US4 Extensiones (Phase 6)**: Depende de Phase 2 — No depende de otras historias
- **US6 Azure SQL (Phase 7)**: Depende de Phase 2 — No depende de otras historias
- **US5 Consolidacion (Phase 8)**: Depende de Phase 3, 4, 5, 6 y 7 — Requiere todos los research completados
- **Polish (Phase 9)**: Depende de Phase 8 — Requiere consolidacion completada

### User Story Dependencies

- **User Story 1 (P1)**: Puede iniciar tras Phase 2 — Independiente de US2, US3, US4, US6
- **User Story 2 (P1)**: Puede iniciar tras Phase 2 — Independiente de US1, US3, US4, US6
- **User Story 3 (P2)**: Puede iniciar tras Phase 2 — Independiente de US1, US2, US4, US6
- **User Story 4 (P2)**: Puede iniciar tras Phase 2 — Independiente de US1, US2, US3, US6
- **User Story 6 (P2)**: Puede iniciar tras Phase 2 — Independiente de US1, US2, US3, US4
- **User Story 5 (P3)**: Depende de US1 + US2 + US3 + US4 + US6 completadas — Actividad de cierre

### Within Each User Story

- Investigacion de fuentes oficiales primero
- Documentacion de hallazgos en research.md
- Presentacion de decisiones al usuario SIEMPRE al final de cada story
- Story completa solo cuando el usuario aprueba todas sus decisiones

### Parallel Opportunities

- Phase 2: T005, T006, T007 pueden ejecutarse en paralelo (archivos diferentes)
- Phase 3 y Phase 4: US1 y US2 (ambas P1) pueden ejecutarse en paralelo
- Phase 5, Phase 6 y Phase 7: US3, US4 y US6 (todas P2) pueden ejecutarse en paralelo
- Phase 5: T019, T020, T021 pueden ejecutarse en paralelo (secciones independientes de nombres)
- Phase 6: T023, T024 pueden ejecutarse en paralelo (extensiones independientes)
- Phase 9: T032, T033 pueden ejecutarse en paralelo (archivos diferentes)

---

## Parallel Example: User Story 1 + User Story 2

```text
# US1 y US2 son ambas P1 y pueden ejecutarse en paralelo tras Phase 2:

Thread A (US1 - LSDP):
  T008 → T009 → T010 → T011 → T012 → T013

Thread B (US2 - AS400):
  T014 → T015 → T016 (paralelo) → T017 → T018
```

## Parallel Example: User Story 3

```text
# Dentro de US3, la compilacion de nombres por origen es paralela:

Thread A: T019 (Nombres hebreos)
Thread B: T020 (Nombres egipcios)
Thread C: T021 (Nombres ingleses)
→ T022 (Verificacion y decisiones - depende de T019+T020+T021)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Completar Phase 1: Setup
2. Completar Phase 2: Foundational (CRITICO — bloquea todo)
3. Completar Phase 3: User Story 1 (Research LSDP)
4. **VALIDAR**: Revisar que hallazgos H1.1-H1.10 y 5 decisiones estan documentados
5. LSDP es el conocimiento mas critico para el proyecto completo

### Incremental Delivery

1. Setup + Foundational → Estructura documental lista
2. US1 (LSDP) → Validar independientemente → Base tecnica establecida (MVP!)
3. US2 (AS400) → Validar independientemente → Estructuras de datos definidas
4. US3 (Nombres) + US4 (Extensiones) + US6 (Azure SQL) → En paralelo → Catalogos, entorno y conexion listos
5. US5 (Consolidacion) → 15/15 decisiones aprobadas → Research V1 completo
6. Polish → Artefactos finales consistentes → Listo para V2

### Single Developer Strategy

Con un solo Ingeniero de Datos ejecutando secuencialmente:

1. Setup + Foundational (T001-T007)
2. US1 LSDP (T008-T013) → Aprobar decisiones R1-D1 a R1-D5
3. US2 AS400 (T014-T018) → Aprobar decisiones R2-D1 a R2-D4
4. US3 Nombres (T019-T022) → Aprobar decisiones R3-D1, R3-D2
5. US4 Extensiones (T023-T026) → Aprobar decisiones R4-D1, R4-D2
6. US6 Azure SQL (T027-T028) → Aprobar decisiones R5-D1, R5-D2
7. US5 Consolidacion (T029-T031) → Validacion final
8. Polish (T032-T035) → Validacion final

---

## Notes

- [P] tasks = archivos diferentes, sin dependencias entre si
- [Story] label vincula cada tarea a su historia de usuario para trazabilidad
- Cada historia de usuario es validable independientemente
- Las decisiones del usuario son BLOQUEANTES: no se avanza sin aprobacion
- V1 NO genera codigo (RF-008): todos los entregables son documentos .md
- Commit despues de cada tarea o grupo logico completado
- Detenerse en cualquier checkpoint para validar la story independientemente
