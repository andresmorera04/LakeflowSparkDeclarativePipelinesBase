# Tasks: Generacion de Parquets Simulando Data AS400

**Input**: Documentos de diseno desde `/specs/002-generacion-parquets-as400/`
**Prerequisites**: plan.md (completo), spec.md (4 historias, 18 RFs), research.md (V2, 2/2 decisiones aprobadas), data-model.md (5 entidades, mapeo AS400→PySpark)

**Tests**: INCLUIDOS — La especificacion requiere TDD obligatorio (RF-014, US4, CE-007). Cada historia de usuario incluye su notebook de pruebas antes de la implementacion.

**Organizacion**: Tareas agrupadas por historia de usuario para habilitar implementacion y pruebas independientes de cada historia.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (diferentes archivos, sin dependencias)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3, US4)
- Rutas exactas de archivos incluidas en las descripciones

## Convenciones de Rutas

- **Notebooks generadores**: `scripts/GenerarParquets/Nb*.py`
- **Notebooks de prueba TDD**: `tests/GenerarParquets/NbTest*.py`
- **Estructura de campos AS400**: `specs/001-research-inicial-v1/research.md` (H2.1, H2.2, H2.3, H2.4)
- **Catalogos de nombres**: `specs/001-research-inicial-v1/research.md` (H3.1, H3.2, H3.3)
- **Parametros por notebook**: `specs/002-generacion-parquets-as400/data-model.md` (seccion Parametros por Notebook)

---

## Fase 1: Setup (Infraestructura Compartida)

**Proposito**: Creacion de la estructura de directorios del proyecto para los notebooks generadores y pruebas TDD.

- [X] T001 Crear estructura de directorios `scripts/GenerarParquets/` y `tests/GenerarParquets/` en la raiz del repositorio

---

## Fase 2: Historia de Usuario 1 - Generacion del Parquet de Maestro de Clientes (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear el notebook `NbGenerarMaestroCliente.py` que genera un parquet con 70 columnas AS400 (42 textuales, 18 fechas, 10 numericos), 5M de registros en primera ejecucion, mutacion del 20% en re-ejecuciones y 0.60% de clientes nuevos. Nombres exclusivamente hebreos, egipcios e ingleses.

**Prueba Independiente**: Ejecutar el notebook y verificar que el parquet tiene 70 columnas con nombres R2-D1, 5M registros, nombres no latinos, y cero valores hardcodeados. En segunda ejecucion, verificar mutacion y nuevos clientes.

### Tests TDD para Historia de Usuario 1 ⚠️

> **NOTA: Escribir estos tests PRIMERO, asegurar que FALLAN antes de la implementacion**

- [X] T002 [US1] Crear notebook de pruebas con validaciones de estructura (70 columnas exactas, nombres de campos segun H2.1 de specs/001-research-inicial-v1/research.md, tipos de datos PySpark) y unicidad de CUSTID en `tests/GenerarParquets/NbTestMaestroCliente.py`
- [X] T003 [US1] Agregar pruebas de volumetria (5,000,000 registros), validacion de nombres no latinos en los 9 campos de RF-005 (FRSTNM, MDLNM, LSTNM, SCNDLN, CUSTNM, MTHNM, FTHNM, CNTPRS, PREFNM contra catalogos H3.1/H3.2/H3.3), validacion de parametros (cero hardcoded), y pruebas de rechazo de parametros invalidos (widget vacio, nulo o formato incorrecto conforme RF-016) en `tests/GenerarParquets/NbTestMaestroCliente.py`

### Implementacion de Historia de Usuario 1

- [X] T004 [US1] Crear notebook con encabezado markdown profesional, definicion de 17 widgets via `dbutils.widgets.text()` (ruta_salida_parquet, cantidad_registros_base, pct_incremento, pct_mutacion, offset_custid, ruta_parquet_existente, num_particiones, rango_credlmt_min, rango_credlmt_max, rango_avlbal_min, rango_avlbal_max, rango_income_min, rango_income_max, rango_loan_min, rango_loan_max, rango_ins_min, rango_ins_max) y bloque de validacion de parametros (RF-016) en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T005 [US1] Definir esquema StructType explicito con 70 StructField (42 StringType, 18 DateType, 10 DoubleType/LongType) segun estructura aprobada en H2.1 de specs/001-research-inicial-v1/research.md en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T006 [US1] Implementar catalogos de nombres no latinos: 300 nombres (100 hebreos H3.1 + 100 egipcios H3.2 + 100 ingleses H3.3) y 150 apellidos (50 hebreos + 50 egipcios + 50 ingleses) como listas Python en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T007 [US1] Implementar logica de primera ejecucion: generacion de 5M registros con CUSTID secuencial desde offset parametrizable, asignacion de nombres de catalogos a campos FRSTNM/MDLNM/LSTNM/SCNDLN/CUSTNM/MTHNM/FTHNM/CNTPRS/PREFNM, generacion de campos demograficos, fechas y numericos con rangos monetarios parametrizables via widgets (CRDLMT, AVLBAL, MNTHLY, LNBLNC, INSAMT) en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T008 [US1] Implementar logica de re-ejecucion: lectura del parquet existente con `spark.read.parquet()`, seleccion del 20% de registros con `sample(0.20)` para mutacion de los siguientes 15 campos demograficos (ADDR1, ADDR2, CITY, STATE, ZPCDE, PHONE1, PHONE2, EMAIL, MRTLST, OCCPTN, EMPLYR, EMPADS, SGMNT, RISKLV, RSKSCR), y generacion de 0.60% clientes nuevos con CUSTID desde `max(CUSTID)+1` en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T009 [US1] Implementar escritura del parquet con `df.repartition(num_particiones).write.mode("overwrite").parquet(ruta)` donde `num_particiones` proviene del widget parametrizable, aplicando el esquema StructType definido, en `scripts/GenerarParquets/NbGenerarMaestroCliente.py`

**Checkpoint**: La Historia de Usuario 1 debe ser completamente funcional y testeable de forma independiente. El parquet del Maestro de Clientes debe existir en la ruta configurada.

---

## Fase 3: Historia de Usuario 2 - Generacion del Parquet Transaccional (Prioridad: P1)

**Objetivo**: Crear el notebook `NbGenerarTransaccionalCliente.py` que genera un parquet con 60 columnas AS400 (30 numericos, 21 fechas, 9 textuales), 15M de registros nuevos por ejecucion, con CUSTIDs referenciados del Maestro, 15 tipos de transaccion con distribucion ponderada (~60%/~30%/~10%), y montos segmentados por tipo.

**Prueba Independiente**: Ejecutar el notebook con fecha parametrizada y verificar que el parquet tiene 60 columnas R2-D2, 15M registros, todos los CUSTID existen en el Maestro, y todos los TRXTYP pertenecen al catalogo R2-D3.

### Tests TDD para Historia de Usuario 2 ⚠️

> **NOTA: Escribir estos tests PRIMERO, asegurar que FALLAN antes de la implementacion**

- [X] T010 [US2] Crear notebook de pruebas con validaciones de estructura (60 columnas exactas segun H2.2 de specs/001-research-inicial-v1/research.md), integridad referencial (todos los CUSTID existen en Maestro), validacion de TRXTYP contra catalogo de 15 tipos (H2.3), y pruebas negativas: ejecucion sin Maestro existente debe fallar con mensaje descriptivo, y fecha con formato invalido debe ser rechazada (RF-016) en `tests/GenerarParquets/NbTestTransaccionalCliente.py`

### Implementacion de Historia de Usuario 2

- [X] T011 [US2] Crear notebook con encabezado markdown profesional, definicion de 33 widgets via `dbutils.widgets.text()` (ruta_salida_parquet, ruta_maestro_clientes, cantidad_registros, fecha_transaccion, offset_trxid, pesos_alta/media/baja, rangos de montos por tipo segun RF-017, num_particiones) y bloque de validacion de parametros incluyendo formato YYYY-MM-DD de la fecha y rechazo de fechas invalidas con mensaje descriptivo (RF-016) en `scripts/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T012 [US2] Definir esquema StructType explicito con 60 StructField (30 DoubleType, 21 DateType/TimestampType, 9 StringType) segun H2.2 y catalogo de tipos de transaccion (15 codigos con nombre, categoria y pesos parametrizables segun H2.3) en `scripts/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T013 [US2] Implementar generacion de 15M transacciones: lectura de CUSTIDs del Maestro con `spark.read.parquet()`, asignacion aleatoria de CUSTID a cada transaccion (RF-009), seleccion ponderada de TRXTYP con distribucion ~60% alta / ~30% media / ~10% baja (RF-008), y generacion de montos con rangos segmentados por tipo de transaccion parametrizables (RF-017) en `scripts/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T014 [US2] Implementar generacion de TRXID con prefijo de fecha YYYYMMDD + secuencial (RF-018), simulacion aleatoria de TRXTM (horas, minutos, segundos), asignacion de TRXDT desde parametro, y escritura del parquet con `df.repartition(num_particiones).write.mode("overwrite").parquet(ruta)` donde `num_particiones` proviene del widget parametrizable, en `scripts/GenerarParquets/NbGenerarTransaccionalCliente.py`

**Checkpoint**: Las Historias de Usuario 1 Y 2 deben funcionar de forma independiente. El Transaccional referencia correctamente clientes existentes del Maestro.

---

## Fase 4: Historia de Usuario 3 - Generacion del Parquet de Saldos (Prioridad: P1)

**Objetivo**: Crear el notebook `NbGenerarSaldosCliente.py` que genera un parquet con 100 columnas AS400 (30 textuales, 35 numericos, 35 fechas), exactamente 1 registro por cliente del Maestro (relacion 1:1), con regeneracion completa en cada ejecucion y montos segmentados por tipo de cuenta.

**Prueba Independiente**: Ejecutar el notebook y verificar que el parquet tiene 100 columnas R2-D4, la misma cantidad de registros que el Maestro, relacion 1:1 sin huerfanos ni faltantes.

### Tests TDD para Historia de Usuario 3 ⚠️

> **NOTA: Escribir estos tests PRIMERO, asegurar que FALLAN antes de la implementacion**

- [X] T015 [US3] Crear notebook de pruebas con validaciones de estructura (100 columnas exactas segun H2.4 de specs/001-research-inicial-v1/research.md), relacion 1:1 estricta (N saldos = N clientes), cero CUSTIDs huerfanos o faltantes, y pruebas negativas: ejecucion sin Maestro existente debe fallar con mensaje descriptivo (RF-016) en `tests/GenerarParquets/NbTestSaldosCliente.py`

### Implementacion de Historia de Usuario 3

- [X] T016 [US3] Crear notebook con encabezado markdown profesional, definicion de 11 widgets via `dbutils.widgets.text()` (ruta_salida_parquet, ruta_maestro_clientes, rangos de montos por tipo de cuenta: ahorro 0-500K, corriente 0-250K, credito 0-100K, prestamo 1K-1M segun RF-017, num_particiones) y bloque de validacion de parametros (RF-016) en `scripts/GenerarParquets/NbGenerarSaldosCliente.py`
- [X] T017 [US3] Definir esquema StructType explicito con 100 StructField (30 StringType, 35 DoubleType, 35 DateType) segun H2.4 de specs/001-research-inicial-v1/research.md en `scripts/GenerarParquets/NbGenerarSaldosCliente.py`
- [X] T018 [US3] Implementar generacion de saldos 1:1: lectura de todos los CUSTIDs del Maestro con `spark.read.parquet()`, generacion de exactamente un registro de saldo por cada CUSTID con montos segmentados por tipo de cuenta parametrizables (RF-017), y escritura con `df.repartition(num_particiones).write.mode("overwrite").parquet(ruta)` donde `num_particiones` proviene del widget parametrizable, en `scripts/GenerarParquets/NbGenerarSaldosCliente.py`

**Checkpoint**: Las Historias de Usuario 1, 2 Y 3 deben funcionar de forma independiente. Los 3 parquets deben existir con integridad referencial completa.

---

## Fase 5: Historia de Usuario 4 - Suite de Pruebas TDD Completa (Prioridad: P2)

**Objetivo**: Extender los notebooks de prueba con validaciones avanzadas que cubran todos los RFs (RF-001 a RF-018), incluyendo mutacion, distribucion de tipos, rangos de montos, unicidad entre ejecuciones, y auditoria de valores hardcodeados.

**Prueba Independiente**: Ejecutar la suite completa de pruebas y verificar que el 100% pasan exitosamente, cubriendo estructura, volumetria, integridad referencial, calidad de datos y exclusion de nombres latinos.

- [X] T019 [P] [US4] Extender NbTestMaestroCliente.py con pruebas de: validacion de mutacion (20% de registros con cambios en los siguientes 15 campos demograficos), validacion de incremento (0.60% clientes nuevos con CUSTID secuencial desde max+1), campos calculados CUSTNM = FRSTNM + LSTNM, y auditoria de cero valores hardcodeados en `tests/GenerarParquets/NbTestMaestroCliente.py`
- [X] T020 [P] [US4] Extender NbTestTransaccionalCliente.py con pruebas de: distribucion de pesos (~60%/~30%/~10% con tolerancia de ±5 puntos porcentuales por banda de frecuencia), cumplimiento de rangos de montos por tipo de transaccion (RF-017), unicidad de TRXID entre ejecuciones con diferente fecha, y validacion de TRXDT igual al parametro y TRXTM con horas/minutos/segundos validos en `tests/GenerarParquets/NbTestTransaccionalCliente.py`
- [X] T021 [P] [US4] Extender NbTestSaldosCliente.py con pruebas de: regeneracion completa (valores diferentes entre ejecuciones), cumplimiento de rangos de montos por tipo de cuenta (ahorro, corriente, credito, prestamo), y cobertura 100% de clientes despues del crecimiento del Maestro en `tests/GenerarParquets/NbTestSaldosCliente.py`

**Checkpoint**: La suite TDD completa valida todos los RFs (RF-001 a RF-018) con al menos una prueba asociada (CE-007).

---

## Fase 6: Polish y Validaciones Cruzadas

**Proposito**: Verificaciones finales que afectan multiples historias de usuario.

- [X] T022 [P] Auditar cero valores hardcodeados (CE-008): revisar los 3 notebooks en `scripts/GenerarParquets/`, confirmar que todos los valores configurables son parametros via `dbutils.widgets`, y verificar que ningun notebook importe `pyspark.pipelines` (RF-015)
- [X] T023 [P] Verificar formato de notebook Databricks (.py con separadores `# COMMAND ----------` y markdown `# MAGIC %md`) y codigo en espanol con snake_case (RF-001, RF-013) en todos los archivos de `scripts/GenerarParquets/` y `tests/GenerarParquets/`
- [X] T024 Ejecutar flujo de validacion completo segun quickstart.md: Maestro (primera ejecucion) → Transaccional (con fecha parametrizada) → Saldos → Suite TDD completa → Maestro (re-ejecucion) → Saldos (verificar crecimiento) → Suite TDD nuevamente. Medir y registrar el tiempo de ejecucion de cada notebook, validando que los 3 completan en menos de 10 minutos cada uno en Computo Serverless (CE-001, CE-002, CE-003)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Fase 1)**: Sin dependencias — puede iniciar inmediatamente
- **US1 Maestro (Fase 2)**: Depende de Setup — **BLOQUEA** US2 y US3 (el parquet del Maestro debe existir para testing de Transaccional y Saldos)
- **US2 Transaccional (Fase 3)**: Depende de US1 (necesita parquet del Maestro para leer CUSTIDs)
- **US3 Saldos (Fase 4)**: Depende de US1 (necesita parquet del Maestro para leer CUSTIDs)
- **US2 y US3 son independientes entre si** — pueden ejecutarse en paralelo
- **US4 Suite TDD (Fase 5)**: Depende de US1, US2 y US3 (extiende pruebas de los 3 notebooks)
- **Polish (Fase 6)**: Depende de US1, US2, US3 y US4

### Dependencias entre Historias de Usuario

- **US1 (Maestro)**: Sin dependencia en otras historias — primera en implementar
- **US2 (Transaccional)**: Depende de US1 para testing (necesita parquet del Maestro con CUSTIDs reales)
- **US3 (Saldos)**: Depende de US1 para testing (necesita parquet del Maestro con CUSTIDs reales)
- **US4 (Suite TDD)**: Depende de US1, US2 y US3 (extiende las pruebas de las 3 historias anteriores)

### Dentro de Cada Historia de Usuario

- Tests TDD se escriben PRIMERO y deben FALLAR antes de la implementacion
- Widgets y validacion de parametros antes de la logica de generacion
- Esquema StructType antes de la generacion de datos
- Logica de generacion antes de la escritura del parquet
- Commit despues de cada tarea o grupo logico

### Oportunidades de Paralelismo

- **Fase 1**: T001 es la unica tarea — sin paralelismo
- **Fase 2 (US1)**: Todas las tareas son sobre los mismos 2 archivos — ejecucion secuencial
- **Fases 3 y 4 (US2 + US3)**: Pueden ejecutarse en PARALELO ya que operan sobre archivos completamente diferentes:
  - Desarrollador A: US2 (T010-T014) sobre NbGenerarTransaccionalCliente.py + NbTestTransaccionalCliente.py
  - Desarrollador B: US3 (T015-T018) sobre NbGenerarSaldosCliente.py + NbTestSaldosCliente.py
- **Fase 5 (US4)**: T019, T020, T021 son [P] — tres archivos diferentes, pueden ejecutarse en paralelo
- **Fase 6 (Polish)**: T022 y T023 son [P] — pueden ejecutarse en paralelo. T024 es secuencial (validacion de flujo completo)

---

## Ejemplo de Paralelismo: Fases 3 + 4 (US2 + US3)

```bash
# Desarrollador A (US2 - Transaccional):
Tarea: T010 - Test notebook transaccional en tests/GenerarParquets/NbTestTransaccionalCliente.py
Tarea: T011 - Widgets y validacion en scripts/GenerarParquets/NbGenerarTransaccionalCliente.py
Tarea: T012 - Schema 60 columnas en scripts/GenerarParquets/NbGenerarTransaccionalCliente.py
Tarea: T013 - Logica de generacion en scripts/GenerarParquets/NbGenerarTransaccionalCliente.py
Tarea: T014 - TRXID y escritura en scripts/GenerarParquets/NbGenerarTransaccionalCliente.py

# Desarrollador B (US3 - Saldos) — EN PARALELO:
Tarea: T015 - Test notebook saldos en tests/GenerarParquets/NbTestSaldosCliente.py
Tarea: T016 - Widgets y validacion en scripts/GenerarParquets/NbGenerarSaldosCliente.py
Tarea: T017 - Schema 100 columnas en scripts/GenerarParquets/NbGenerarSaldosCliente.py
Tarea: T018 - Generacion 1:1 y escritura en scripts/GenerarParquets/NbGenerarSaldosCliente.py
```

---

## Ejemplo de Paralelismo: Fase 5 (US4)

```bash
# Todas en paralelo (diferentes archivos):
Tarea: T019 - Extender pruebas Maestro en tests/GenerarParquets/NbTestMaestroCliente.py
Tarea: T020 - Extender pruebas Transaccional en tests/GenerarParquets/NbTestTransaccionalCliente.py
Tarea: T021 - Extender pruebas Saldos en tests/GenerarParquets/NbTestSaldosCliente.py
```

---

## Estrategia de Implementacion

### MVP Primero (Solo Historia de Usuario 1)

1. Completar Fase 1: Setup (T001)
2. Completar Fase 2: US1 Maestro (T002-T009)
3. **DETENER Y VALIDAR**: Ejecutar NbTestMaestroCliente.py — todas las pruebas deben pasar
4. El parquet del Maestro de Clientes existe y es valido → MVP funcional

### Entrega Incremental

1. Completar Setup → Estructura lista
2. Completar US1 (Maestro) → Probar independientemente → MVP de datos base
3. Completar US2 (Transaccional) + US3 (Saldos) en paralelo → Probar independientemente → Landing zone completa
4. Completar US4 (Suite TDD extendida) → Cobertura completa RF-001 a RF-018
5. Completar Polish → Validacion final de flujo completo
6. Cada historia agrega valor sin romper las anteriores

### Estrategia de Equipo en Paralelo

Con multiples desarrolladores:

1. Equipo completa Setup juntos (T001)
2. Equipo completa US1 (Maestro) juntos — es bloqueante (T002-T009)
3. Una vez US1 completo:
   - Desarrollador A: US2 Transaccional (T010-T014)
   - Desarrollador B: US3 Saldos (T015-T018)
4. Ambos completos: US4 en paralelo (T019-T021)
5. Validacion final (T022-T024)

---

## Trazabilidad RF → Tareas

| RF | Descripcion | Tareas |
|----|------------|--------|
| RF-001 | Formato notebook .py PascalCase con Nb | T004, T011, T016, T023 |
| RF-002 | Parametrizacion completa dbutils.widgets | T004, T007, T011, T016, T022 |
| RF-003 | 70 columnas R2-D1 | T005, T009 |
| RF-004 | 5M primera ejecucion, mutacion, 0.60% nuevos | T007, T008 |
| RF-005 | Nombres no latinos R3-D1 | T003, T006 |
| RF-006 | 60 columnas R2-D2 | T012 |
| RF-007 | 15M registros, TRXDT parametro, TRXTM aleatorio | T013, T014 |
| RF-008 | 15 tipos TRXTYP con distribucion ponderada | T012, T013 |
| RF-009 | Integridad referencial CUSTID | T013 |
| RF-010 | 100 columnas R2-D4 | T017 |
| RF-011 | Relacion 1:1 saldos | T018 |
| RF-012 | Ruta scripts/GenerarParquets/ | T001 |
| RF-013 | Codigo en espanol, snake_case | T004, T011, T016, T023 |
| RF-014 | Suite TDD en tests/GenerarParquets/ | T002, T003, T010, T015, T019, T020, T021 |
| RF-015 | Independiente de LSDP | T022 |
| RF-016 | Validacion de parametros | T003, T004, T010, T011, T015, T016 |
| RF-017 | Rangos de montos segmentados | T013, T018 |
| RF-018 | IDs secuenciales con offset | T007, T014 |

---

## Notas

- Todas las tareas operan sobre archivos .py compatibles con notebooks de Azure Databricks
- Las estructuras campo por campo (70, 60, 100 columnas) estan documentadas en specs/001-research-inicial-v1/research.md secciones H2.1, H2.2, H2.4
- Los catalogos de nombres (300 + 150 entradas) estan documentados en specs/001-research-inicial-v1/research.md secciones H3.1, H3.2, H3.3
- Decisiones aprobadas: V2-R2-D1 (widgets.text) y V2-R3-D1 (write.parquet + StructType) en specs/002-generacion-parquets-as400/research.md
- Mapeo de tipos AS400 → PySpark: specs/002-generacion-parquets-as400/data-model.md
- Todo el codigo debe estar en espanol con snake_case, completamente comentado (RF-013)
- Cero valores hardcodeados bajo ninguna circunstancia (RF-002, CE-008)
- Commit despues de cada tarea o grupo logico
- Detener en cualquier checkpoint para validar la historia de forma independiente
