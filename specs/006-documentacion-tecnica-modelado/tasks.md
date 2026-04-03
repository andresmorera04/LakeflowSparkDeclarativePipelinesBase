# Tasks: Documentacion Tecnica y Modelado de Datos (Version 6)

**Input**: Documentos de diseno de `/specs/006-documentacion-tecnica-modelado/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

**Tests**: No se generan tareas de prueba. El entregable de la Version 6 es exclusivamente documentacion Markdown y no codigo ejecutable (ver supuestos del spec.md).

**Organizacion**: Las tareas estan agrupadas por historia de usuario para permitir la implementacion y validacion independiente de cada historia.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Se puede ejecutar en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario a la que pertenece la tarea (HU1, HU2, HU3)
- Se incluyen rutas exactas de archivos en las descripciones

---

## Fase 1: Setup (Infraestructura Compartida)

**Proposito**: Crear la estructura de directorios necesaria para la documentacion

- [x] T001 Crear la carpeta docs/ en la raiz del proyecto

---

## Fase 2: Foundational (Prerequisitos Bloqueantes)

**Proposito**: No aplica para esta version. La Fase 1 (Setup) es trivial y no hay prerequisitos bloqueantes entre las historias de usuario. Las HU1 y HU2 son independientes entre si (archivos separados) y pueden comenzar inmediatamente despues de T001.

**Checkpoint**: La carpeta docs/ existe — la implementacion de historias de usuario puede comenzar

---

## Fase 3: Historia de Usuario 1 — Manual Tecnico del Proyecto Completo (Prioridad: P1)

**Objetivo**: Generar el archivo docs/ManualTecnico.md con las 11 secciones principales que documentan de forma tecnica y detallada todos los componentes del proyecto LSDP.

**Prueba Independiente**: Verificar que docs/ManualTecnico.md existe, se renderiza correctamente en un visor Markdown, contiene las 11 secciones principales con todo el contenido especificado en RF-002 a RF-004, RF-010, RF-011 y RF-013, esta escrito en espanol y sin emojis.

### Implementacion de la Historia de Usuario 1

- [x] T002 [HU1] Escribir la seccion de titulo del proyecto y descripcion general del pipeline LSDP en docs/ManualTecnico.md (RF-002.1). Leer src/LSDP_Laboratorio_Basico/ para extraer el proposito general del pipeline y sus componentes principales
- [x] T003 [HU1] Escribir la seccion de Dependencias de Infraestructura Azure en docs/ManualTecnico.md (RF-002.2). Documentar los 6 recursos: ADLS Gen2 con contenedores bronce/plata/oro/control, Azure SQL Free Tier con base Configuracion y tabla dbo.Parametros, Azure Key Vault con 2 secretos (sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd), Databricks Premium con Unity Catalog, External Locations, y Catalogos bronce_dev/plata_dev/oro_dev/control_dev con esquema regional
- [x] T004 [HU1] Escribir la seccion de Parametros en docs/ManualTecnico.md (RF-002.3). Documentar la tabla dbo.Parametros con las 6 claves (catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, catalogoOro) y los 9 parametros del pipeline LSDP (nombreScopeSecret, esquema_plata, esquema_oro, rutaCompletaMaestroCliente, rutaCompletaTransaccional, rutaCompletaSaldoCliente, rutaCheckpointCmstfl, rutaCheckpointTrxpfl, rutaCheckpointBlncfl) con descripcion, tipo, ejemplo y formato esperado. Leer src/LSDP_Laboratorio_Basico/utilities/LsdpConexionAzureSql.py para confirmar la lectura de parametros
- [x] T005 [HU1] Escribir la seccion de Notebooks de Generacion de Parquets en docs/ManualTecnico.md (RF-002.4, RF-011). Leer scripts/GenerarParquets/NbGenerarMaestroCliente.py, scripts/GenerarParquets/NbGenerarTransaccionalCliente.py y scripts/GenerarParquets/NbGenerarSaldosCliente.py para documentar cada notebook con: proposito, lista completa de widgets (parametros de entrada) con su valor por defecto y formato esperado, logica de generacion de datos, volumetria, formato de salida y particularidades (mutacion de clientes, catalogos de nombres hebreos/egipcios/ingleses, tipos de transaccion, relacion 1:1 de saldos)
- [x] T006 [HU1] Escribir la seccion de Utilidades LSDP en docs/ManualTecnico.md (RF-002.5). Leer src/LSDP_Laboratorio_Basico/utilities/LsdpConexionAzureSql.py, src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutasAbfss.py y src/LSDP_Laboratorio_Basico/utilities/LsdpReordenarColumnasLiquidCluster.py para documentar cada utilidad con: proposito, firma de la funcion, parametros, valor de retorno, patron de uso closure y restricciones de compatibilidad con Computo Serverless
- [x] T007 [HU1] Escribir la seccion de Transformaciones LSDP Medalla de Bronce en docs/ManualTecnico.md (RF-002.6, RF-004, RF-010). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py, src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py y src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py para documentar cada script con: proposito, decorador @dp.table, tabla creada con nombre Unity Catalog, configuracion AutoLoader (cloudFiles.format, schemaEvolutionMode, schemaLocation), columna FechaIngestaDatos con current_timestamp(), _rescued_data, reordenacion Liquid Cluster, propiedades Delta (5), patron closure y operaciones PySpark utilizadas (readStream, format, option, load, withColumn, select)
- [x] T008 [HU1] Escribir la seccion de Transformaciones LSDP Medalla de Plata en docs/ManualTecnico.md (RF-002.7, RF-004, RF-010, RF-013). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py y src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py para documentar cada script con: proposito, decorador @dp.materialized_view, nombre parametrizado de la vista, lectura batch con spark.read.table() sin filtros, logica de transformacion (Window functions Row_Number con partitionBy/orderBy para Dimension Tipo 1, LEFT JOIN por CUSTID, renombramiento de 171 columnas a espanol, exclusion de columnas, 4 campos calculados CASE y SHA256 en clientes_saldos, 4 campos calculados numericos en transacciones con coalesce), expectativa @dp.expect_or_drop, propiedades Delta, Liquid Cluster y todas las operaciones PySpark (select, join, filter, withColumn, when/otherwise, coalesce, sha2, datediff, current_date, abs, col, lit, row_number, desc, Window.partitionBy/orderBy, isin, cast)
- [x] T009 [HU1] Escribir la seccion de Transformaciones LSDP Medalla de Oro en docs/ManualTecnico.md (RF-002.8, RF-004, RF-010, RF-013). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py para documentar las 2 vistas materializadas: comportamiento_atm_cliente (groupBy, agregaciones condicionales count/avg/sum con when, coalesce doble, expect_or_drop) y resumen_integral_cliente (LEFT JOIN plata+oro, select de 17 columnas de plata + 5 metricas ATM, coalesce a 0 para clientes sin transacciones, dependencia DAG intra-pipeline), propiedades Delta compartidas, Liquid Cluster y todas las operaciones PySpark (groupBy, agg, count, avg, spark_sum, when, coalesce, col, lit, join, withColumn, select)
- [x] T010 [HU1] Escribir la seccion del Paradigma Declarativo LSDP en docs/ManualTecnico.md (RF-002.9, RF-003). Documentar: (a) decoradores @dp.table y @dp.materialized_view y como definen la logica ETL declarativa, (b) orquestacion del DAG de dependencias por LSDP, (c) diferencia entre streaming tables (ingesta incremental via AutoLoader) y vistas materializadas (transformaciones batch), (d) patron closure como reemplazo de spark.sparkContext.broadcast() para Computo Serverless, (e) declaracion de tablas en catalogos/esquemas diferentes al por defecto del pipeline usando nombre completo en spark.read.table()
- [x] T011 [HU1] Escribir la seccion de Propiedades Delta en docs/ManualTecnico.md (RF-002.10). Documentar: Change Data Feed (enableChangeDataFeed), autoOptimize.autoCompact, autoOptimize.optimizeWrite, Liquid Cluster (por que se usa en lugar de ZOrder/PartitionBy, como se define con cluster_by, restriccion de las primeras 32 columnas para estadisticas min/max, re-ordenacion con reordenar_columnas_liquid_cluster), delta.deletedFileRetentionDuration (30 dias), delta.logRetentionDuration (60 dias)
- [x] T012 [HU1] Escribir la seccion de Estrategia de Pruebas TDD en docs/ManualTecnico.md (RF-002.11). Documentar: que se prueba en cada capa (generacion de parquets, bronce, plata, oro, utilidades), estructura de la carpeta tests/ con tests/GenerarParquets/ (NbTestMaestroCliente.py, NbTestSaldosCliente.py, NbTestTransaccionalCliente.py) y tests/LSDP_Laboratorio_Basico/ (NbTestBronceCmstfl.py, NbTestBronceTrxpfl.py, NbTestBronceBlncfl.py, NbTestPlataClientesSaldos.py, NbTestPlataTransacciones.py, NbTestOroClientes.py, NbTestConexionAzureSql.py, NbTestConexionAzureSqlV4.py, NbTestConstructorRutasAbfss.py, NbTestReordenarColumnasLC.py), y como ejecutar pruebas con la extension Databricks para VS Code. No documentar codigo interno de los tests
- [x] T013 [HU1] Verificar que las operaciones de lectura y escritura del pipeline (RF-010) estan completamente documentadas en las secciones de bronce (T007), plata (T008) y oro (T009) de docs/ManualTecnico.md: (a) lectura de parquets via AutoLoader con formato cloudFiles, (b) lectura de tablas Delta via spark.read.table(), (c) escritura en streaming tables via @dp.table, (d) escritura en vistas materializadas via @dp.materialized_view, (e) uso exclusivo del protocolo abfss://

**Checkpoint**: En este punto, docs/ManualTecnico.md debe ser un documento completo y autocontenido con las 11 secciones principales

---

## Fase 4: Historia de Usuario 2 — Documento de Modelado de Datos Completo (Prioridad: P1)

**Objetivo**: Generar el archivo docs/ModeladoDatos.md con las 6 secciones principales que documentan todas las entidades de datos del proyecto organizadas por capa del medallion.

**Prueba Independiente**: Verificar que docs/ModeladoDatos.md existe, se renderiza correctamente, documenta las 10 entidades de datos (3 parquets, 3 streaming tables, 2 vistas plata, 2 vistas oro) con nombre completo, columnas, tipos y descripciones, los 8 campos calculados tienen su logica completa, esta escrito en espanol y sin emojis.

### Implementacion de la Historia de Usuario 2

- [x] T014 [P] [HU2] Escribir la seccion de titulo y descripcion general del modelo de datos en docs/ModeladoDatos.md (RF-005.1). Describir el proposito del modelado, el alcance del medallion (Parquets AS400, Bronce, Plata, Oro) y la cantidad total de entidades documentadas (10)
- [x] T015 [P] [HU2] Escribir la seccion de Linaje de Datos en docs/ModeladoDatos.md (RF-005.2, decision R21-D1). Crear diagrama Mermaid (renderizable en mermaid.live) mostrando el flujo Parquet AS400 -> Bronce -> Plata -> Oro con las entidades involucradas en cada capa. Complementar con texto descriptivo que explique las claves de relacion (CUSTID como clave principal, renombrada a identificador_cliente en plata), los tipos de JOIN aplicados (LEFT JOIN en plata y oro), y como se transforman y consolidan los datos entre capas
- [x] T016 [HU2] Escribir la seccion de Archivos Parquet Fuente AS400 en docs/ModeladoDatos.md (RF-005.3, RF-007). Leer scripts/GenerarParquets/NbGenerarMaestroCliente.py, scripts/GenerarParquets/NbGenerarTransaccionalCliente.py y scripts/GenerarParquets/NbGenerarSaldosCliente.py para documentar: CMSTFL MaestroCliente (70 campos), TRXPFL Transaccional (60 campos) y BLNCFL SaldoCliente (100 campos). Cada parquet con nombre de archivo, tabla unica de columnas con nombre campo AS400, tipo de dato y descripcion (decision R20-D1)
- [x] T017 [HU2] Escribir la seccion de Streaming Tables de Bronce en docs/ModeladoDatos.md (RF-005.4, RF-007, RF-012). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py, LsdpBronceTrxpfl.py y LsdpBronceBlncfl.py para documentar: bronce_dev.regional.cmstfl (72 campos), bronce_dev.regional.trxpfl (62 campos) y bronce_dev.regional.blncfl (102 campos). Cada tabla con nombre completo Unity Catalog, columnas de Liquid Cluster, nota sobre las 2 columnas adicionales (FechaIngestaDatos como marca de tiempo con current_timestamp() para acumulacion historica, _rescued_data como columna de AutoLoader para datos fuera de esquema), y tabla de columnas con nombre campo, tipo de dato y descripcion
- [x] T018 [HU2] Escribir la seccion de Vistas Materializadas de Plata - clientes_saldos_consolidados en docs/ModeladoDatos.md (RF-005.5, RF-006, RF-007, decision R20-D1). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py para documentar: nombre completo parametrizado {catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados (175 columnas), tabla unica de columnas (71 de cmstfl renombradas + 100 de blncfl renombradas + 4 calculados), detalle completo de los 4 campos calculados: clasificacion_riesgo_cliente (CASE con 5 umbrales usando nivel_riesgo_cliente, puntaje_crediticio, estado_mora), categoria_saldo_disponible (CASE con 5 umbrales usando saldo_disponible, limite_credito, segmento_cliente), perfil_actividad_bancaria (CASE con dias_sin_transaccion, total_productos_contratados, estado_cuenta_cliente), huella_identificacion_cliente (SHA2-256 del identificador_cliente)
- [x] T019 [HU2] Escribir la seccion de Vistas Materializadas de Plata - transacciones_enriquecidas en docs/ModeladoDatos.md (RF-005.5, RF-006, RF-007). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py para documentar: nombre completo parametrizado {catalogoPlata}.{esquema_plata}.transacciones_enriquecidas (65 columnas), 61 columnas de trxpfl renombradas a espanol con tipo de dato y descripcion, y detalle completo de los 4 campos calculados numericos: monto_neto_comisiones (NETAMT-(FEEAMT+TAXAMT) con coalesce), porcentaje_comision_sobre_monto ((FEEAMT/ORGAMT)*100 con manejoHU division por cero), variacion_saldo_transaccion (abs(BLNAFT-BLNBFR) con coalesce), indicador_impacto_financiero (TRXAMT+FEEAMT+TAXAMT+PNLAMT con coalesce)
- [x] T020 [HU2] Escribir la seccion de Vistas Materializadas de Oro en docs/ModeladoDatos.md (RF-005.6, RF-006, RF-007). Leer src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py para documentar: comportamiento_atm_cliente con nombre completo {catalogoOro}.{esquema_oro}.comportamiento_atm_cliente (6 columnas con logica de agregacion count/avg/sum condicionales por tipo_transaccion CATM/DATM/PGSL y manejo de nulos con coalesce doble), y resumen_integral_cliente con nombre completo {catalogoOro}.{esquema_oro}.resumen_integral_cliente (22 columnas indicando 17 de plata y 5 metricas ATM con coalesce a 0 para clientes sin transacciones)

**Checkpoint**: En este punto, docs/ModeladoDatos.md debe ser un documento completo con las 10 entidades de datos documentadas incluyendo la logica de los 8 campos calculados

---

## Fase 5: Historia de Usuario 3 — Estructura docs/ y Estilo Profesional (Prioridad: P2)

**Objetivo**: Validar y ajustar el formato, estructura jerárquica y estilo profesional de ambos documentos generados en las Fases 3 y 4.

**Prueba Independiente**: Verificar que la carpeta docs/ contiene exactamente 2 archivos (ManualTecnico.md y ModeladoDatos.md), ambos se renderizan sin errores en un visor Markdown, contienen encabezados jerarquicos h1/h2/h3/h4, tablas con alineacion consistente, sin emojis y todo en espanol.

### Implementacion de la Historia de Usuario 3

- [x] T021 [HU3] Revisar y ajustar los encabezados jerarquicos de docs/ManualTecnico.md (RF-008). Verificar que se usa h1 para el titulo principal, h2 para las 11 secciones principales, h3 para subsecciones (notebooks individuales, utilidades individuales, scripts individuales), h4 si se requieren subdivisiones adicionales. Asegurar consistencia en todo el documento
- [x] T022 [HU3] Revisar y ajustar los encabezados jerarquicos de docs/ModeladoDatos.md (RF-008). Verificar que se usa h1 para el titulo principal, h2 para las 6 secciones principales, h3 para cada entidad individual (cada parquet, cada streaming table, cada vista materializada). Cada entidad usa tabla unica de columnas (decision R20-D1). Asegurar consistencia en todo el documento
- [x] T023 [HU3] Revisar la alineacion y formato de todas las tablas Markdown en docs/ManualTecnico.md y docs/ModeladoDatos.md (RF-008). Verificar que los pipes y guiones tienen alineacion consistente, que las tablas se renderizan correctamente como tablas funcionales en un visor Markdown, y que no hay caracteres mal codificados
- [x] T024 [HU3] Verificar que ambos documentos cumplen con las restricciones de contenido (RF-008, RF-009). Validar: todo en espanol, sin emojis, estilo explicativo y profesional, no se modificaron archivos en src/ ni tests/, la carpeta docs/ contiene exactamente 2 archivos

**Checkpoint**: Ambos documentos estan finalizados con formato profesional y validados

---

## Fase 6: Polish y Validacion Cruzada

**Proposito**: Validacion final de completitud y consistencia entre ambos documentos

- [x] T025 Ejecutar la validacion completa del quickstart.md verificando los 5 pasos de validacion rapida en docs/
- [x] T026 Verificar que el 100% de las claves de dbo.Parametros y parametros del pipeline estan documentados en docs/ManualTecnico.md comparando contra data-model.md seccion de Parametros (CE-004)
- [x] T027 Verificar que el 100% de las entidades de datos (3 parquets, 3 streaming tables, 2 vistas plata, 2 vistas oro) estan documentadas en docs/ModeladoDatos.md comparando contra data-model.md secciones de entidades (CE-005)
- [x] T028 Verificar que el 100% de los campos calculados (4 plata clientes + 4 plata transacciones + logica de agregacion oro) tienen su logica completa documentada en docs/ModeladoDatos.md (CE-002)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Fase 1 (Setup)**: Sin dependencias — puede comenzar inmediatamente
- **Fase 2 (Foundational)**: No aplica para esta version
- **Fase 3 (HU1 — ManualTecnico.md)**: Depende de T001 (Fase 1)
- **Fase 4 (HU2 — ModeladoDatos.md)**: Depende de T001 (Fase 1) — independiente de Fase 3
- **Fase 5 (HU3 — Estilo)**: Depende de la finalizacion de Fase 3 y Fase 4
- **Fase 6 (Polish)**: Depende de la finalizacion de Fase 5

### Dependencias entre Historias de Usuario

- **HU1 (ManualTecnico.md, P1)**: Puede iniciar tras T001. Sin dependencias con otras historias
- **HU2 (ModeladoDatos.md, P1)**: Puede iniciar tras T001. Sin dependencias con otras historias. Puede ejecutarse en paralelo con HU1
- **HU3 (Estilo, P2)**: Depende de la finalizacion de HU1 y HU2 — es una revision transversal

### Dentro de Cada Historia de Usuario

- **HU1**: Las tareas T002 a T012 son secuenciales (escriben secciones consecutivas en un mismo archivo). T013 puede integrarse en paralelo si se agrega como contenido complementario a las secciones ya escritas
- **HU2**: T014 y T015 son paralelizables (secciones independientes al inicio del archivo). T016 a T020 son secuenciales (secciones consecutivas del mismo archivo)
- **HU3**: T021 y T022 son paralelizables (archivos diferentes). T023 depende de T021 y T022. T024 depende de T023

### Oportunidades de Paralelismo

- HU1 (Fase 3) y HU2 (Fase 4) pueden ejecutarse completamente en paralelo tras T001
- Dentro de HU2: T014 y T015 marcados [P] (secciones independientes al inicio del archivo)
- Dentro de HU3: T021 y T022 pueden ejecutarse en paralelo (archivos diferentes)

---

## Ejemplo de Ejecucion Paralela: HU1 + HU2

```text
Tras T001 (Setup):

Agente A (HU1):                          Agente B (HU2):
T002 Titulo y descripcion general        T014 [P] Titulo y descripcion modelo
T003 Dependencias Azure                  T015 [P] Linaje de datos
T004 Parametros                          T016 Parquets AS400
T005 Notebooks parquets                  T017 Streaming tables bronce
T006 Utilidades LSDP                     T018 Plata clientes_saldos
T007 Bronce                              T019 Plata transacciones
T008 Plata                               T020 Oro
T009 Oro
T010 Paradigma declarativo
T011 Propiedades Delta
T012 Estrategia TDD
T013 Operaciones lectura/escritura
```

---

## Estrategia de Implementacion

### MVP Primero (HU1 Unicamente)

1. Completar Fase 1: Setup (T001)
2. Completar Fase 3: ManualTecnico.md (T002-T013)
3. **DETENER Y VALIDAR**: Verificar que ManualTecnico.md se renderiza correctamente y contiene las 11 secciones
4. Entregar si es suficiente para el usuario

### Entrega Incremental

1. Completar Setup → docs/ creada
2. Agregar HU1 (ManualTecnico.md) → Validar renderizado → Entrega parcial (guia tecnica disponible)
3. Agregar HU2 (ModeladoDatos.md) → Validar renderizado → Entrega parcial (diccionario de datos disponible)
4. Agregar HU3 (Estilo) → Validar formato → Entrega final (documentacion profesional completa)
5. Polish (Fase 6) → Validacion de completitud contra criterios de exito

### Estrategia Paralela

Con 2 agentes disponibles:

1. Ambos completan T001 (Setup)
2. Una vez docs/ existe:
   - Agente A: HU1 — ManualTecnico.md (T002-T013)
   - Agente B: HU2 — ModeladoDatos.md (T014-T020)
3. Ambos completan y se procede a HU3 (T021-T024) y Polish (T025-T028)

---

## Notas

- Las tareas [P] = archivos diferentes, sin dependencias entre si
- La etiqueta [Story] mapea cada tarea a su historia de usuario para trazabilidad
- HU1 y HU2 son independientes y pueden completarse en cualquier orden
- No se generan pruebas TDD — el entregable es documentacion Markdown exclusivamente
- No se modifican archivos en src/ ni tests/ (RF-009)
- Las decisiones R20-D1 (tabla unica por entidad) y R21-D1 (diagrama Mermaid en mermaid.live) del research.md fueron aprobadas el 2026-04-01
