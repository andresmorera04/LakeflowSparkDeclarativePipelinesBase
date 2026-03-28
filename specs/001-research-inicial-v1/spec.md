# Especificacion del Feature: Research Inicial y Decisiones Clave - Version 1

**Feature Branch**: `001-research-inicial-v1`
**Creado**: 2026-03-27
**Estado**: Borrador
**Input**: Basado en el archivo SYSTEM.md - Seccion Specify, Version 1 del Producto de Datos para Analisis de Comportamiento de Clientes (Saldos y ATM)

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Research de Lakeflow Spark Declarative Pipelines (Prioridad: P1)

Como Ingeniero de Datos, necesito investigar a fondo las capacidades, limitaciones, decoradores, APIs y compatibilidad de Lakeflow Spark Declarative Pipelines (LSDP) con Computo Serverless de Azure Databricks, para poder tomar decisiones informadas sobre la arquitectura del pipeline y garantizar que el diseno sea viable antes de escribir codigo.

**Por que esta prioridad**: LSDP es el paradigma central de todo el proyecto. Sin un entendimiento completo de sus capacidades y restricciones, no es posible disenar correctamente las medallas de Bronce, Plata y Oro ni validar que los requerimientos del negocio sean implementables.

**Prueba Independiente**: Se puede validar revisando que el documento de research contenga: (1) listado de decoradores y funciones disponibles en la biblioteca `pyspark.pipelines` (alias `dp`), incluyendo @dp.table, @dp.materialized_view, @dp.expect, (2) compatibilidad confirmada con Computo Serverless, (3) capacidades de AutoLoader para Bronce, (4) soporte para vistas materializadas en Plata y Oro, (5) soporte para apply_changes y tablas temporales, (6) propiedades soportadas (Change Data Feed, Liquid Cluster, autoOptimize), (7) diferencias documentadas entre el modulo legado `dlt` (Delta Live Tables) y el modulo actual `pyspark.pipelines` (LSDP).

**Escenarios de Aceptacion**:

1. **Dado** que no existe documentacion de research previa, **Cuando** el Ingeniero de Datos realiza la investigacion usando fuentes oficiales de Azure Databricks, **Entonces** se genera un documento de hallazgos que cubre todas las capacidades de LSDP relevantes al proyecto.
2. **Dado** que se han documentado los hallazgos de LSDP, **Cuando** el usuario revisa las opciones y restricciones presentadas, **Entonces** el usuario toma decisiones aprobando o rechazando cada hallazgo relevante.
3. **Dado** que LSDP debe ejecutarse en Computo Serverless, **Cuando** se investigan las limitaciones del runtime Serverless, **Entonces** se documenta explicitamente que funcionalidades de LSDP son compatibles y cuales no.

---

### Historia de Usuario 2 - Research de Estructura de Datos AS400 Bancaria (Prioridad: P1)

Como Ingeniero de Datos, necesito investigar la estructura real de las tablas de AS400 para una entidad bancaria (Maestro de Clientes, Transaccional y Saldos), incluyendo nombres de campos, tipos de datos y cantidad de columnas, para que los parquets simulados sean lo mas fieles posible a las tablas originales.

**Por que esta prioridad**: La simulacion de datos es el insumo de entrada de todo el pipeline. Si la estructura no refleja fielmente las tablas AS400, las transformaciones de Plata y Oro no seran representativas del escenario real.

**Prueba Independiente**: Se puede validar revisando que el documento de research contenga: (1) listado completo de campos del Maestro de Clientes de AS400 con nombres y tipos, (2) listado completo de campos del Transaccional con nombres y tipos, (3) listado completo de campos de Saldos con nombres y tipos, (4) catalogo de tipos de transacciones bancarias (Creditos ATM, Debitos ATM, Transferencias Externas, Transferencias Internas, Pagos al Saldo, Adelanto Salarial y otros), (5) relaciones entre tablas documentadas.

**Escenarios de Aceptacion**:

1. **Dado** que se necesita simular el Maestro de Clientes de AS400, **Cuando** se investigan las tablas originales de AS400 para banca, **Entonces** se documenta la lista completa de campos con nombres similares a AS400 (por ejemplo: CUSTNM, CUSTID, ADDR1, etc.) y sus tipos de datos.
2. **Dado** que el Transaccional requiere un campo de tipo de transaccion, **Cuando** se investigan los tipos de transacciones bancarias estandar, **Entonces** se documenta un catalogo completo de tipos de transaccion con su codigo y descripcion, y el usuario aprueba el catalogo final.
3. **Dado** que los Saldos mantienen relacion 1:1 con el Maestro de Clientes, **Cuando** se investigan las tablas de saldos de AS400, **Entonces** se documenta la estructura completa de campos incluyendo tipos de saldo, monedas y fechas de corte.

---

### Historia de Usuario 3 - Research de Nombres Hebreos, Egipcios e Ingleses (Prioridad: P2)

Como Ingeniero de Datos, necesito investigar y compilar listas de nombres y apellidos hebreos, egipcios e ingleses para garantizar que los datos simulados del Maestro de Clientes no contengan nombres o apellidos latinos bajo ninguna circunstancia.

**Por que esta prioridad**: Es un requisito explicito de la constitucion del proyecto. Sin esta investigacion, la generacion de datos podria violar la regla de no usar nombres latinos, invalidando los datos de prueba.

**Prueba Independiente**: Se puede validar revisando que el documento de research contenga: (1) listado de al menos 100 nombres hebreos masculinos y femeninos, (2) listado de al menos 100 nombres egipcios masculinos y femeninos, (3) listado de al menos 100 nombres ingleses masculinos y femeninos, (4) listado de al menos 50 apellidos por cada origen, (5) fuentes de referencia verificables para cada lista, (6) confirmacion de que ningun nombre o apellido de las listas tiene origen latino.

**Escenarios de Aceptacion**:

1. **Dado** que se requieren nombres no latinos para la simulacion, **Cuando** se investigan fuentes confiables de nombres hebreos, egipcios e ingleses, **Entonces** se genera un catalogo de nombres y apellidos verificados por origen con al menos 100 entradas por categoria.
2. **Dado** que el catalogo de nombres esta documentado, **Cuando** el usuario lo revisa, **Entonces** el usuario aprueba que las listas no contienen nombres de origen latino.

---

### Historia de Usuario 4 - Research de Extensiones de Databricks para VS Code (Prioridad: P2)

Como Ingeniero de Datos, necesito investigar las capacidades actuales de las extensiones de Databricks para Visual Studio Code (Databricks extension for VS Code y Databricks Driver for SQLTools), especificamente para la ejecucion de pruebas TDD y la validacion de notebooks y pipelines LSDP desde el IDE local.

**Por que esta prioridad**: Las extensiones son el mecanismo para ejecutar pruebas de forma local. Si no se comprenden sus capacidades y limitaciones, el enfoque de TDD podria ser inviable o requerir ajustes significativos.

**Prueba Independiente**: Se puede validar revisando que el documento de research contenga: (1) version actual de cada extension y compatibilidad, (2) capacidades de ejecucion de notebooks (.py) desde VS Code, (3) capacidades de ejecucion de pruebas unitarias, (4) limitaciones conocidas con Computo Serverless, (5) configuracion requerida para conectar con el workspace de Databricks.

**Escenarios de Aceptacion**:

1. **Dado** que el proyecto requiere TDD ejecutable desde VS Code, **Cuando** se investigan las extensiones de Databricks, **Entonces** se documenta como ejecutar pruebas unitarias contra un cluster Databricks desde VS Code y se identifican las limitaciones.
2. **Dado** que los hallazgos de extensiones estan documentados, **Cuando** el usuario revisa las capacidades y limitaciones, **Entonces** el usuario toma decisiones sobre el enfoque de pruebas (ejecucion local vs remota, frameworks de testing compatibles).

---

### Historia de Usuario 5 - Consolidacion de Decisiones y Validacion de Constitucion (Prioridad: P3)

Como Ingeniero de Datos, necesito que todas las decisiones tomadas durante el research se consoliden en un documento unico y se valide que la constitucion del proyecto es robusta, completa y sin ambiguedades, para que las versiones subsecuentes (V2 a V5) tengan una base solida.

**Por que esta prioridad**: Es la actividad de cierre de la Version 1. Depende de que todas las investigaciones previas esten completadas y las decisiones hayan sido tomadas por el usuario.

**Prueba Independiente**: Se puede validar revisando que: (1) cada hallazgo del research tiene una decision asociada aprobada por el usuario, (2) la constitucion refleja todas las decisiones tomadas, (3) no existen ambiguedades pendientes, (4) el documento esta listo como insumo para /speckit.plan.

**Escenarios de Aceptacion**:

1. **Dado** que se han completado todos los research (LSDP, AS400, nombres, extensiones, Azure SQL), **Cuando** se consolidan las decisiones, **Entonces** se genera un documento resumen con cada decision, su justificacion y la aprobacion del usuario.
2. **Dado** que las decisiones estan consolidadas, **Cuando** se valida la constitucion contra los hallazgos, **Entonces** la constitucion se actualiza si es necesario (con aprobacion del usuario) y se confirma que cubre todos los escenarios identificados.

---

### Historia de Usuario 6 - Research de Patron de Conexion Azure SQL via Secretos (Prioridad: P2)

Como Ingeniero de Datos, necesito investigar y validar el patron de conexion a Azure SQL Server via los secretos de Databricks, confirmando que `spark.read.format("jdbc")` funciona correctamente desde un pipeline LSDP ejecutado en Computo Serverless, para que las versiones posteriores puedan leer parametros y configuraciones desde Azure SQL de forma segura.

**Por que esta prioridad**: La conexion a Azure SQL es el mecanismo para leer tablas de parametros y configuraciones. Sin validar su viabilidad en Computo Serverless, las versiones V2-V5 podrian enfrentar bloqueos al intentar integrar fuentes externas.

**Prueba Independiente**: Se puede validar revisando que el documento de research contenga: (1) patron de conexion documentado con los dos secretos (`sr-jdbc-asql-asqlmetadatos-adminpd` y `sr-asql-asqlmetadatos-adminpd`), (2) viabilidad confirmada en Computo Serverless, (3) uso dentro de LSDP documentado, (4) restricciones conocidas.

**Escenarios de Aceptacion**:

1. **Dado** que el proyecto requiere leer parametros desde Azure SQL, **Cuando** se investiga el patron de conexion via `spark.read.format("jdbc")` con secretos de Databricks, **Entonces** se documenta la viabilidad, el patron de conexion completo y las restricciones en Computo Serverless.
2. **Dado** que los hallazgos de Azure SQL estan documentados, **Cuando** el usuario revisa el patron y las restricciones, **Entonces** el usuario toma decisiones sobre el patron de conexion y el encapsulamiento en funciones de utilidad.

---

### Casos Borde

- Que sucede si el research de LSDP revela que alguna funcionalidad requerida (por ejemplo, apply_changes o vistas materializadas en catalogos diferentes al por defecto) no es compatible con Computo Serverless?
- Que sucede si la estructura de tablas de AS400 para banca varia significativamente entre diferentes implementaciones de AS400 y no hay un estandar unico?
- Que sucede si las extensiones de Databricks para VS Code no soportan la ejecucion directa de pruebas unitarias contra pipelines LSDP?
- Que sucede si algunos nombres del catalogo tienen origen ambiguo (por ejemplo, nombres comunes entre culturas latinas y hebreas como "Daniel")?
- Que sucede si el runtime de Computo Serverless tiene una version de Databricks Runtime que no soporta alguna propiedad delta requerida (Liquid Cluster, Change Data Feed)?

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: El proceso de research DEBE producir un documento de hallazgos de Lakeflow Spark Declarative Pipelines que cubra: la biblioteca oficial `from pyspark import pipelines as dp` con sus decoradores disponibles (@dp.table, @dp.materialized_view, @dp.expect), funciones de lectura (dp.read, dp.read_stream), soporte de AutoLoader (spark.readStream.format("cloudFiles")), vistas materializadas, tablas temporales, apply_changes, y compatibilidad con Computo Serverless. Referencias oficiales principales: https://learn.microsoft.com/es-es/azure/databricks/ldp/ y https://docs.databricks.com/aws/en/ldp/developer/python-ref
- **RF-002**: El proceso de research DEBE producir un catalogo de estructura de tablas AS400 bancarias con nombres de campos en formato AS400 (maximo 10 caracteres, estilo abreviado como CUSTNM, CUSTID, TRXAMT, etc.), tipos de datos y relaciones entre tablas, respetando la siguiente distribucion exacta:
  - **Maestro de Clientes**: 70 campos/columnas (60% textuales = 42 campos, 25% fechas = 18 campos, 15% numericos con y sin decimales = 10 campos).
  - **Transaccional**: 60 campos/columnas (50% numericos con y sin decimales = 30 campos, 35% fechas y fechas con hora = 21 campos, 15% textuales = 9 campos).
  - **Saldos de Clientes**: 100 campos/columnas (30% textuales = 30 campos, 35% numericos con y sin decimales = 35 campos, 35% fechas = 35 campos).
- **RF-003**: El research DEBE producir un catalogo completo de tipos de transacciones bancarias que incluya como minimo: Creditos por ATM, Debitos por ATM, Transferencias Externas, Transferencias Internas, Pagos al Saldo del Cliente y Adelanto Salarial. El usuario DEBE aprobar el catalogo final.
- **RF-004**: El research DEBE producir listas verificadas de nombres y apellidos de origen hebreo, egipcio e ingles, con al menos 100 nombres y 50 apellidos por cada origen, acompanadas de fuentes de referencia. El usuario DEBE confirmar que no se incluyen nombres de origen latino.
- **RF-005**: El research DEBE producir un documento de capacidades y limitaciones de las extensiones de Databricks para VS Code, que incluya: version actual, soporte para ejecucion de notebooks, soporte para pruebas unitarias, y configuracion requerida para el workspace. El research DEBE validar la viabilidad de una estrategia mixta de testing: pytest local para funciones utilitarias en la carpeta `utilities/` (que no dependen de `pyspark.pipelines`) y notebooks de prueba ejecutados en Databricks para validar el pipeline LSDP completo (decoradores @dp.table, @dp.materialized_view, apply_changes, etc.).
- **RF-006**: Cada hallazgo del research que presente opciones o alternativas DEBE ser presentado al usuario con una propuesta recomendada por la IA, y el usuario DEBE tomar la decision final antes de continuar al siguiente paso.
- **RF-007**: Al finalizar todos los research, DEBE generarse un documento consolidado de decisiones que sirva de insumo para refinar la constitucion y para alimentar el proceso de /speckit.plan.
- **RF-008**: La Version 1 NO DEBE generar ningun codigo fuente. Su alcance se limita exclusivamente a research, documentacion de hallazgos y toma de decisiones.
- **RF-009**: Todos los documentos de research DEBEN basarse en fuentes oficiales de Azure Databricks, Python y Spark como primera opcion. Para LSDP especificamente, la referencia principal DEBE ser https://learn.microsoft.com/es-es/azure/databricks/ldp/ complementada con https://docs.databricks.com/aws/en/ldp/developer/python-ref. Medium y StackOverflow se usan como segunda opcion solo para ejemplos complementarios.
- **RF-010**: El research de LSDP DEBE documentar explicitamente las diferencias entre Delta Live Tables (DLT, modulo `import dlt`) y Lakeflow Spark Declarative Pipelines (modulo `from pyspark import pipelines as dp`), para asegurar que el proyecto use exclusivamente la biblioteca `pyspark.pipelines` con alias `dp` y NO use el modulo legado `dlt` bajo ninguna circunstancia.
- **RF-011**: El research de V1 DEBE incluir la validacion del patron de conexion a Azure SQL Server via los secretos de Databricks (`sr-jdbc-asql-asqlmetadatos-adminpd` para la cadena JDBC y `sr-asql-asqlmetadatos-adminpd` para la contrasena), confirmando que `spark.read.format("jdbc")` funciona correctamente desde un pipeline LSDP ejecutado en Computo Serverless. Se DEBE documentar viabilidad, restricciones conocidas y alternativas si existieran incompatibilidades.

### Entidades Clave

- **Documento de Research**: Artefacto que contiene los hallazgos de investigacion sobre un tema especifico (LSDP, AS400, nombres, extensiones). Incluye: tema investigado, fuentes consultadas, hallazgos principales, opciones identificadas, recomendacion de la IA y decision del usuario.
- **Catalogo de Tipos de Transaccion**: Listado estructurado de los tipos de transacciones bancarias que seran simulados en el parquet Transaccional. Incluye: codigo del tipo, nombre descriptivo, descripcion del comportamiento y ejemplo de uso.
- **Catalogo de Nombres No Latinos**: Listado clasificado por origen (hebreo, egipcio, ingles) de nombres y apellidos verificados. Incluye: nombre/apellido, genero (cuando aplica), origen confirmado y fuente de referencia.
- **Estructura de Tablas AS400**: Definicion de la estructura de campos de cada una de las tres tablas a simular (Maestro de Clientes, Transaccional, Saldos). Incluye: nombre de campo en formato AS400, descripcion funcional, tipo de dato logico y relaciones con otras tablas.
- **Registro de Decisiones**: Documento que consolida cada decision tomada por el usuario durante el proceso de research. Incluye: tema, opciones presentadas, recomendacion de la IA, decision del usuario y justificacion.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: El 100% de los temas de research definidos (LSDP, AS400, nombres, extensiones) tienen un documento de hallazgos completado y revisado por el usuario.
- **CE-002**: Cada documento de research contiene al menos 3 fuentes oficiales de Azure Databricks verificables.
- **CE-003**: El 100% de los hallazgos que presentan opciones o alternativas tienen una decision registrada y aprobada por el usuario.
- **CE-004**: El catalogo de tipos de transacciones contiene al menos 8 tipos diferentes de transacciones bancarias aprobados por el usuario.
- **CE-005**: El catalogo de nombres contiene al menos 300 nombres (100 por origen) y 150 apellidos (50 por origen), todos verificados como no latinos.
- **CE-006**: La constitucion del proyecto se actualiza (si es necesario) incorporando todas las decisiones tomadas, sin ambiguedades pendientes.
- **CE-007**: El documento consolidado de decisiones esta listo como insumo para las versiones V2-V5, alimentando el flujo SDD (specify → plan → tasks) de cada version subsecuente.
- **CE-008**: No se genera ningun archivo de codigo fuente (.py) como parte de esta version.
- **CE-009**: El research de la medalla de Oro documenta que las vistas materializadas deben agregar a nivel de cliente individual (una fila por cliente) con totales historicos acumulados: conteo de creditos/debitos ATM, promedio de montos retirados/depositados y total sumarizado de Pagos al Saldo.

## Supuestos

- El usuario tiene acceso a un workspace de Azure Databricks con Unity Catalog habilitado y Computo Serverless disponible para validar hallazgos del research si fuera necesario.
- El usuario tiene instaladas las extensiones de Databricks para VS Code (Databricks extension for VS Code y Databricks Driver for SQLTools) para poder validar hallazgos relacionados con el entorno de desarrollo.
- Las tablas de AS400 para banca siguen un patron general reconocible en la industria, aunque pueden existir variaciones entre implementaciones especificas. Se usara un modelo representativo que el usuario debera aprobar.
- El research se realiza sobre la documentacion oficial vigente a la fecha (marzo 2026) de Azure Databricks, y los hallazgos podrian variar si Databricks actualiza sus APIs o funcionalidades.
- El usuario esta disponible para tomar decisiones conforme se presenten los hallazgos del research, evitando bloqueos prolongados en el proceso.
- La Version 1 es estrictamente de research y no incluye configuracion de infraestructura, creacion de catalogos, ni despliegue de ningun tipo.

## Clarificaciones

### Sesion 2026-03-27

- P: La restriccion de "NO usar Delta Live Tables" implica que tampoco se puede usar el modulo `import dlt` de Python, dado que LSDP tiene su propia biblioteca? -> R: Correcto. LSDP usa la biblioteca oficial `from pyspark import pipelines as dp` con decoradores como @dp.materialized_view, @dp.table, etc. El modulo legado `import dlt` corresponde a Delta Live Tables y NO debe usarse. Referencia: https://docs.databricks.com/aws/en/ldp/developer/python-ref
- P: Cuantos campos/columnas debe tener cada tabla AS400 simulada y cual es la distribucion por tipo de dato? -> R: Maestro de Clientes: 70 campos (60% textuales, 25% fechas, 15% numericos con/sin decimales). Transaccional: 60 campos (50% numericos con/sin decimales, 35% fechas y fechas con hora, 15% textuales). Saldos de Clientes: 100 campos (30% textuales, 35% numericos con/sin decimales, 35% fechas).
- P: Cual es la estrategia de framework y patron de ejecucion de pruebas TDD para el proyecto? -> R: Estrategia mixta: pytest local para funciones utilitarias en `utilities/` (que no dependen de `pyspark.pipelines`) + notebooks de prueba ejecutados en Databricks para validar el pipeline LSDP completo (decoradores @dp.table, @dp.materialized_view, apply_changes, etc.).
- P: A que nivel de granularidad deben agregarse los datos en la medalla de Oro? -> R: Agregacion a nivel de cliente individual (una fila por cliente) sin dimension temporal, mostrando totales historicos acumulados: conteo de creditos/debitos ATM, promedio de montos retirados/depositados por ATM, y total sumarizado de Pagos al Saldo del cliente.
- P: El research de V1 debe incluir la investigacion del patron de conexion a Azure SQL via secretos de Databricks o se difiere a versiones posteriores? -> R: Incluir en V1 la validacion del patron de conexion a Azure SQL (combinacion de secretos sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd via spark.read.format("jdbc")) confirmando viabilidad en Computo Serverless, sin generar codigo.
