# Especificacion del Feature: Documentacion Tecnica y Modelado de Datos (Version 6)

**Feature Branch**: `006-documentacion-tecnica-modelado`
**Creado**: 2026-04-01
**Estado**: Borrador
**Input**: Generar los documentos ManualTecnico.md y ModeladoDatos.md en la carpeta docs/ del proyecto, documentando de forma completa y detallada los notebooks de generacion de parquets y todo el Lakeflow Spark Declarative Pipelines, incluyendo el modelado de datos desde los parquets hasta las vistas materializadas de oro. Esta version NO modifica archivos en src/ ni en tests/.

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Manual Tecnico del Proyecto Completo (Prioridad: P1)

Como Ingeniero de Datos que se incorpora al proyecto o que necesita dar mantenimiento al pipeline, necesito un documento ManualTecnico.md en la carpeta docs/ que explique de forma tecnica y detallada todos los notebooks de generacion de parquets y todo el Lakeflow Spark Declarative Pipelines, incluyendo la explicacion de los decoradores @dp, el paradigma declarativo, las propiedades de las tablas deltas streaming y vistas materializadas, todas las operaciones logicas del API de DataFrames de Spark, la tabla de parametros de Azure SQL, y la seccion de dependencias de infraestructura Azure, para poder comprender, operar y mantener la solucion sin ambiguedad.

**Por que esta prioridad**: El manual tecnico es el documento de referencia principal del proyecto. Sin el, cualquier nuevo integrante del equipo o el propio ingeniero titular no tendria una guia consolidada para comprender la arquitectura, las decisiones de diseno y la operacion de toda la solucion. Es el entregable de mayor valor de la Version 6.

**Prueba Independiente**: Se puede validar verificando que: (1) el archivo docs/ManualTecnico.md existe y se puede renderizar correctamente en un visor Markdown, (2) contiene una seccion de Dependencias de infraestructura Azure con los 6 recursos requeridos (ADLS Gen2, Azure SQL, Key Vault, Databricks Premium, External Locations, Catalogos Unity Catalog), (3) contiene explicacion tecnica de cada notebook de generacion de parquets (NbGenerarMaestroCliente, NbGenerarTransaccionalCliente, NbGenerarSaldosCliente), (4) contiene explicacion tecnica de cada script de utilities (LsdpConexionAzureSql, LsdpConstructorRutasAbfss, LsdpReordenarColumnasLiquidCluster), (5) contiene explicacion tecnica de cada script de transformations de bronce, plata y oro, (6) incluye un apartado completo de parametros del pipeline y de la tabla dbo.Parametros con todas las claves y valores, (7) explica los decoradores @dp.table y @dp.materialized_view, (8) explica el paradigma declarativo de LSDP, (9) explica las propiedades Delta (Change Data Feed, autoCompact, optimizeWrite, Liquid Cluster, retenciones), (10) esta escrito completamente en espanol.

**Escenarios de Aceptacion**:

1. **Dado** que el proyecto tiene 3 notebooks de generacion de parquets en scripts/GenerarParquets/, **Cuando** se revisa el ManualTecnico.md, **Entonces** cada notebook tiene una seccion dedicada que explica su proposito, parametros de entrada (widgets), logica de generacion de datos, volumetria, formato de salida y particularidades (mutacion de clientes, catalogos de nombres hebreos/egipcios/ingleses, tipos de transaccion, relacion 1:1 de saldos).
2. **Dado** que el proyecto tiene 3 scripts de utilidades en src/LSDP_Laboratorio_Basico/utilities/, **Cuando** se revisa el ManualTecnico.md, **Entonces** cada utilidad tiene una seccion dedicada que explica su proposito, firma de la funcion, parametros, valor de retorno, patron de uso (closure) y restricciones de compatibilidad con Computo Serverless.
3. **Dado** que el proyecto tiene 6 scripts de transformaciones en src/LSDP_Laboratorio_Basico/transformations/ (3 bronce, 2 plata, 1 oro), **Cuando** se revisa el ManualTecnico.md, **Entonces** cada script tiene una seccion dedicada que explica su proposito, decorador utilizado, tablas/vistas creadas, logica de transformacion, patron de lectura de parametros (closure), columnas de Liquid Cluster, expectativas de calidad de datos y propiedades Delta configuradas.
4. **Dado** que la tabla dbo.Parametros de Azure SQL contiene multiples claves de configuracion, **Cuando** se revisa el ManualTecnico.md, **Entonces** existe un apartado que lista todas las claves (catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, catalogoOro) con su descripcion, tipo de valor y ejemplo de uso.
5. **Dado** que el pipeline LSDP recibe parametros en tiempo de ejecucion, **Cuando** se revisa el ManualTecnico.md, **Entonces** existe un apartado que documenta todos los parametros del pipeline (nombreScopeSecret, esquema_plata, esquema_oro, y los parametros de rutas relativas de parquets y checkpoints) con su descripcion, formato esperado y ejemplo de valor.
6. **Dado** que el proyecto requiere infraestructura Azure previa, **Cuando** se revisa el ManualTecnico.md, **Entonces** la seccion de Dependencias lista los 6 recursos Azure requeridos (ADLS Gen2 con contenedores bronce/plata/oro/control, Azure SQL Free Tier con base Configuracion y tabla dbo.Parametros cargada, Azure Key Vault con 2 secretos, Databricks Premium con Unity Catalog, External Locations, Catalogos bronce_dev/plata_dev/oro_dev/control_dev con esquema regional).

---

### Historia de Usuario 2 - Documento de Modelado de Datos Completo (Prioridad: P1)

Como Ingeniero de Datos o analista de datos, necesito un documento ModeladoDatos.md en la carpeta docs/ que muestre el modelado completo de los datos del proyecto, desde la composicion de los campos/columnas de los archivos parquets (AS400), hasta las streaming tables de bronce y las vistas materializadas de plata y oro, cada uno con sus campos, tipos de datos y descripciones detalladas, incluyendo la logica de los campos calculados, para poder comprender la estructura de datos en todas las capas del medallion sin necesidad de revisar el codigo fuente.

**Por que esta prioridad**: El modelado de datos es esencial para comprender las transformaciones aplicadas en cada capa del medallion. Sin este documento, los consumidores de datos (analistas, cientificos de datos, ingenieros) no tendrian una referencia clara de que datos existen, como se relacionan y que significan los campos calculados.

**Prueba Independiente**: Se puede validar verificando que: (1) el archivo docs/ModeladoDatos.md existe y se renderiza correctamente, (2) documenta los 3 parquets de AS400 (CMSTFL con 70 campos, TRXPFL con 60 campos, BLNCFL con 100 campos) con nombre del parquet, columnas, tipos de datos y descripciones, (3) documenta las 3 streaming tables de bronce (cmstfl con 72 columnas, trxpfl con 62 columnas, blncfl con 102 columnas) con nombre completo, columnas, tipos de datos y descripciones, (4) documenta las 2 vistas materializadas de plata (clientes_saldos_consolidados con 175 columnas, transacciones_enriquecidas con 65 columnas) con nombre completo, columnas, tipos y descripciones incluyendo la logica de campos calculados, (5) documenta las 2 vistas materializadas de oro (comportamiento_atm_cliente con 6 columnas, resumen_integral_cliente con 22 columnas) con nombre completo, columnas, tipos y descripciones incluyendo la logica de agregacion, (6) los campos calculados incluyen una descripcion de la logica aplicada y los campos que intervienen, (7) cada entidad indica su nombre completo (nombre del parquet o nombre completo de la tabla/vista en Unity Catalog), (8) esta escrito completamente en espanol.

**Escenarios de Aceptacion**:

1. **Dado** que existen 3 archivos parquet de AS400 (MaestroCliente, Transaccional, SaldoCliente), **Cuando** se revisa el ModeladoDatos.md, **Entonces** cada parquet tiene una seccion con su nombre de archivo, una tabla con columnas (nombre campo AS400, tipo de dato, descripcion), y la cantidad total de campos coincide con el diseno (70, 60, 100 respectivamente).
2. **Dado** que existen 3 streaming tables de bronce en el catalogo bronce_dev.regional (cmstfl, trxpfl, blncfl), **Cuando** se revisa el ModeladoDatos.md, **Entonces** cada streaming table tiene una seccion con su nombre completo en Unity Catalog, una tabla con columnas (nombre campo, tipo de dato, descripcion), las 2 columnas adicionales respecto al parquet (FechaIngestaDatos, _rescued_data) estan documentadas, y se indican las columnas de Liquid Cluster.
3. **Dado** que existen 2 vistas materializadas de plata, **Cuando** se revisa el ModeladoDatos.md, **Entonces** clientes_saldos_consolidados documenta las 175 columnas incluyendo los 4 campos calculados (3 CASE + 1 SHA256) con la logica detallada de cada uno (campos que intervienen, condiciones, valores resultantes), y transacciones_enriquecidas documenta sus columnas incluyendo los 4 campos calculados numericos con la logica de cada uno.
4. **Dado** que existen 2 vistas materializadas de oro, **Cuando** se revisa el ModeladoDatos.md, **Entonces** comportamiento_atm_cliente documenta sus 6 columnas con la logica de agregacion (count, avg, sum condicionales por tipo_transaccion), y resumen_integral_cliente documenta sus 22 columnas indicando cuales provienen de plata y cuales son las metricas agregadas de ATM.
5. **Dado** que existen campos calculados en plata y oro, **Cuando** se revisa la descripcion de cada campo calculado en el ModeladoDatos.md, **Entonces** la descripcion incluye la formula o logica aplicada (ej: "Se calcula como SHA2-256 del campo CUSTID", "Se asigna 'Alto' cuando X > Y y Z = 'valor', 'Medio' cuando..., 'Bajo' en caso contrario") y los nombres de los campos de entrada que participan en el calculo.

---

### Historia de Usuario 3 - Estructura de la Carpeta docs/ y Estilo Profesional de los Documentos (Prioridad: P2)

Como responsable del proyecto, necesito que los documentos generados en docs/ sigan un estilo profesional, explicativo y estructurado con secciones claras, tabla de contenidos, y formato Markdown valido, para que sean presentables ante partes interesadas tecnicas y no tecnicas.

**Por que esta prioridad**: La calidad y presentacion de la documentacion impacta directamente su utilidad y adopcion. Sin embargo, el contenido tecnico correcto (HU1 y HU2) tiene prioridad sobre la forma.

**Prueba Independiente**: Se puede validar verificando que: (1) la carpeta docs/ existe en la raiz del proyecto, (2) ambos archivos (ManualTecnico.md y ModeladoDatos.md) se renderizan correctamente en un visor Markdown sin errores de formato, (3) las tablas Markdown tienen alineacion consistente y se renderizan como tablas funcionales, (4) existen secciones con encabezados jerarquicos claros (h1, h2, h3), (5) el texto no contiene emojis, (6) todo el contenido esta en espanol, (7) los bloques de markdown siguen un estilo explicativo y profesional.

**Escenarios de Aceptacion**:

1. **Dado** que se genera el ManualTecnico.md, **Cuando** se renderiza en un visor Markdown, **Entonces** las secciones principales estan organizadas con encabezados h1/h2/h3 jerarquicos, no contiene emojis, el texto es explicativo y profesional, y las tablas se renderizan correctamente.
2. **Dado** que se genera el ModeladoDatos.md, **Cuando** se renderiza en un visor Markdown, **Entonces** las tablas de columnas (nombre, tipo, descripcion) se renderizan como tablas funcionales con alineacion consistente, no contiene emojis, y cada seccion de entidad es autocontenida.
3. **Dado** que ambos documentos se generan en docs/, **Cuando** se verifica la estructura del proyecto, **Entonces** la carpeta docs/ contiene exactamente 2 archivos: ManualTecnico.md y ModeladoDatos.md.

---

### Casos Borde

- Que sucede si un campo de las tablas de bronce no tiene un equivalente directo en AS400? El ModeladoDatos.md debe indicar claramente que el campo fue agregado por el pipeline (ej: FechaIngestaDatos, _rescued_data) y no proviene del parquet original.
- Que sucede si la logica de un campo calculado es muy extensa para una celda de tabla Markdown? La descripcion en la tabla debe contener un resumen conciso de la logica y los campos involucrados, suficiente para comprender la transformacion sin revisar el codigo.
- Que sucede si se agregan nuevas claves a dbo.Parametros en versiones futuras? El ManualTecnico.md debe documentar explicitamente las claves existentes hasta la Version 5 (catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, catalogoOro) e indicar que la funcion leer_parametros_azure_sql retorna TODAS las claves sin filtro, facilitando la extension futura.
- Que sucede si las columnas de un parquet o tabla son demasiadas para una sola tabla Markdown legible? Se usa tabla unica por entidad para facilitar la busqueda con Ctrl+F (decision R20-D1).
- Que sucede si el proyecto cambia la version de la API de LSDP (de `from pyspark import pipelines as dp` a otra)? El ManualTecnico.md debe documentar la version actual de la API y los decoradores utilizados, de modo que cualquier cambio futuro sea facil de identificar y actualizar en la documentacion.

## Clarificaciones

### Sesion 2026-04-01

- Q: El ModeladoDatos.md debe incluir una seccion de linaje de datos que muestre el flujo entre capas del medallion y las claves de relacion? -> A: Si, incluir una seccion de linaje de datos al inicio del ModeladoDatos.md con descripcion textual del flujo entre capas y las claves de relacion (CUSTID).
- Q: El ManualTecnico.md debe incluir una seccion que describa la estrategia de pruebas TDD del proyecto? -> A: Si, incluir una seccion de estrategia de pruebas TDD que describa que se prueba en cada capa, la estructura de la carpeta tests/ y como ejecutar las pruebas, sin documentar el codigo interno de los tests.

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: Se DEBE crear la carpeta docs/ en la raiz del proyecto si no existe, y dentro de ella generar exactamente 2 archivos: ManualTecnico.md y ModeladoDatos.md. No se deben generar archivos adicionales ni modificar archivos existentes fuera de docs/.
- **RF-002**: El archivo ManualTecnico.md DEBE contener como minimo las siguientes secciones principales en este orden:
  1. Titulo del proyecto y descripcion general del pipeline LSDP.
  2. Seccion de Dependencias de infraestructura Azure: Azure Data Lake Storage Gen2 (con contenedores bronce, plata, oro, control), Azure SQL Free Tier (con base Configuracion y tabla dbo.Parametros cargada con todos los registros), Azure Key Vault (con 2 secretos: sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd), Azure Databricks Premium (con Unity Catalog habilitado), External Locations (referenciando a los contenedores del ADLS Gen2), y Catalogos Unity Catalog (bronce_dev, plata_dev, oro_dev, control_dev con esquema regional en cada uno).
  3. Seccion de Parametros: documentacion completa de la tabla dbo.Parametros de Azure SQL (todas las claves, sus valores de ejemplo y su descripcion funcional) y de los parametros del pipeline LSDP (nombreScopeSecret, esquema_plata, esquema_oro, y parametros de rutas relativas de parquets y checkpoints).
  4. Seccion de Notebooks de Generacion de Parquets: explicacion tecnica de NbGenerarMaestroCliente.py, NbGenerarTransaccionalCliente.py y NbGenerarSaldosCliente.py.
  5. Seccion de Utilidades LSDP: explicacion tecnica de LsdpConexionAzureSql.py, LsdpConstructorRutasAbfss.py y LsdpReordenarColumnasLiquidCluster.py.
  6. Seccion de Transformaciones LSDP — Medalla de Bronce: explicacion tecnica de LsdpBronceCmstfl.py, LsdpBronceTrxpfl.py y LsdpBronceBlncfl.py.
  7. Seccion de Transformaciones LSDP — Medalla de Plata: explicacion tecnica de LsdpPlataClientesSaldos.py y LsdpPlataTransacciones.py.
  8. Seccion de Transformaciones LSDP — Medalla de Oro: explicacion tecnica de LsdpOroClientes.py.
  9. Seccion del Paradigma Declarativo LSDP: explicacion de los decoradores @dp.table y @dp.materialized_view, como funciona el modelo declarativo frente al imperativo, y como LSDP gestiona la orquestacion, la incrementalidad y las dependencias entre tablas/vistas.
  10. Seccion de Propiedades Delta: explicacion de Change Data Feed, autoOptimize.autoCompact, autoOptimize.optimizeWrite, Liquid Cluster (por que se usa, como se define, restriccion de las primeras 32 columnas), delta.deletedFileRetentionDuration y delta.logRetentionDuration.
  11. Seccion de Estrategia de Pruebas TDD: descripcion de que se prueba en cada capa del pipeline (generacion de parquets, bronce, plata, oro, utilidades), estructura de la carpeta tests/ (tests/GenerarParquets/ y tests/LSDP_Laboratorio_Basico/) con la lista de archivos de prueba y su proposito, e instrucciones generales de como ejecutar las pruebas usando las extensiones de Databricks para VS Code. No se documenta el codigo interno de los tests.
- **RF-003**: El archivo ManualTecnico.md DEBE explicar en la seccion del paradigma declarativo: (a) que son los decoradores @dp.table y @dp.materialized_view y como definen la logica ETL de forma declarativa, (b) como LSDP orquesta la ejecucion del grafo de dependencias entre tablas y vistas, (c) la diferencia entre streaming tables (para ingesta incremental via AutoLoader) y vistas materializadas (para transformaciones batch), (d) como el patron closure reemplaza a spark.sparkContext.broadcast() para compatibilidad con Computo Serverless, (e) como se declaran tablas en catalogos/esquemas diferentes al por defecto del pipeline.
- **RF-004**: El archivo ManualTecnico.md DEBE explicar en cada script de transformaciones todas las operaciones logicas del API de DataFrames de Spark utilizadas, incluyendo pero no limitandose a: select, join, filter/where, groupBy, agg, withColumn, withColumnRenamed, when/otherwise, coalesce, sha2, window functions (Row_Number, partition_by/order_by), current_timestamp, datediff, sum, count, avg, abs, col, lit, y cualquier otra utilizada en el codigo.
- **RF-005**: El archivo ModeladoDatos.md DEBE contener como minimo las siguientes secciones principales:
  1. Titulo y descripcion general del modelo de datos del proyecto.
  2. Seccion de Linaje de Datos: descripcion textual del flujo de datos entre capas del medallion (Parquet AS400 -> Bronce -> Plata -> Oro), indicando las entidades involucradas en cada capa, las claves de relacion entre ellas (CUSTID como clave principal de union), y como se transforman y consolidan los datos a medida que avanzan por las capas. Esta seccion debe aparecer antes de las secciones de detalle por entidad para que el lector comprenda primero las relaciones de alto nivel (con diagrama Mermaid, decision R21-D1).
  3. Seccion de Archivos Parquet (Fuente AS400): una subseccion por cada parquet (MaestroCliente con 70 campos, Transaccional con 60 campos, SaldoCliente con 100 campos) con tabla de columnas (nombre del campo AS400, tipo de dato y descripcion).
  4. Seccion de Streaming Tables de Bronce: una subseccion por cada tabla (cmstfl con 72 campos, trxpfl con 62 campos, blncfl con 102 campos) con nombre completo en Unity Catalog, tabla de columnas, columnas de Liquid Cluster y nota sobre las columnas adicionales respecto al parquet (FechaIngestaDatos, _rescued_data).
  5. Seccion de Vistas Materializadas de Plata: una subseccion para clientes_saldos_consolidados (175 columnas) y otra para transacciones_enriquecidas, ambas con nombre completo en Unity Catalog, tabla de columnas y detalle de campos calculados con logica explicada.
  6. Seccion de Vistas Materializadas de Oro: una subseccion para comportamiento_atm_cliente (6 columnas) y otra para resumen_integral_cliente (22 columnas), ambas con nombre completo en Unity Catalog, tabla de columnas, logica de agregacion detallada y fuentes de datos.
- **RF-006**: En el ModeladoDatos.md, cada campo calculado DEBE tener en su descripcion la logica completa aplicada: los campos de entrada que participan, las condiciones evaluadas (para campos tipo CASE) y la formula o funcion aplicada (para campos numericos o hash). La descripcion debe ser suficiente para reproducir la logica sin consultar el codigo fuente.
- **RF-007**: En el ModeladoDatos.md, cada entidad modelada (parquet, streaming table o vista materializada) DEBE indicar su nombre completo: para parquets, el nombre del archivo; para streaming tables, el nombre completo en Unity Catalog (ej: bronce_dev.regional.cmstfl); para vistas materializadas, el nombre completo con catalogo y esquema parametrizados (ej: {catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados).
- **RF-008**: Ambos documentos DEBEN estar escritos completamente en espanol, sin emojis, con un estilo explicativo y profesional. Los bloques de Markdown deben usar encabezados jerarquicos claros. Las tablas deben tener alineacion consistente con pipes y guiones.
- **RF-009**: La Version 6 NO DEBE modificar ningun archivo existente en los directorios src/ ni tests/. El alcance se limita exclusivamente a la creacion de la carpeta docs/ y sus 2 archivos Markdown.
- **RF-010**: El ManualTecnico.md DEBE documentar las operaciones de lectura y escritura del pipeline, incluyendo: (a) la lectura de parquets via AutoLoader con formato cloudFiles, (b) la lectura de tablas Delta via spark.read.table(), (c) la escritura en tablas streaming via @dp.table, (d) la escritura en vistas materializadas via @dp.materialized_view, (e) el uso exclusivo del protocolo abfss:// para rutas de ADLS Gen2.
- **RF-011**: El ManualTecnico.md DEBE incluir en cada notebook de generacion de parquets: la lista completa de widgets (parametros de entrada), su proposito, su valor por defecto y el formato esperado.
- **RF-012**: El ModeladoDatos.md DEBE documentar para las tablas de bronce el campo FechaIngestaDatos como marca de tiempo de ingesta que permite la acumulacion historica, y el campo _rescued_data como columna de AutoLoader que captura datos que no encajan en el esquema esperado.
- **RF-013**: El ManualTecnico.md DEBE documentar las expectativas de calidad de datos (@dp.expect_or_drop) utilizadas en el pipeline, indicando en que tablas/vistas se aplican, que campo se valida y que ocurre con los registros que no cumplen la expectativa.

### Entidades Clave

- **ManualTecnico.md**: Documento Markdown completo que sirve como guia tecnica de referencia del proyecto. Contiene la explicacion detallada de todos los componentes del pipeline LSDP, las utilidades, los notebooks de generacion de parquets, los parametros de configuracion, las dependencias de infraestructura Azure y la explicacion del paradigma declarativo con sus propiedades Delta.
- **ModeladoDatos.md**: Documento Markdown completo que sirve como diccionario de datos del proyecto. Contiene la estructura de todas las entidades de datos (parquets, streaming tables, vistas materializadas) con sus campos, tipos de datos, descripciones y logica de campos calculados, organizado por capa del medallion (fuente AS400, bronce, plata, oro).
- **Carpeta docs/**: Directorio en la raiz del proyecto que contiene exclusivamente los documentos de documentacion tecnica y modelado de datos.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: Un nuevo Ingeniero de Datos puede comprender la arquitectura completa del pipeline y sus dependencias de infraestructura leyendo unicamente el ManualTecnico.md, sin necesidad de revisar el codigo fuente, en un tiempo no mayor a 60 minutos de lectura.
- **CE-002**: El 100% de los campos calculados de plata y oro estan documentados en el ModeladoDatos.md con su logica completa (campos de entrada, condiciones, formula), permitiendo reproducir la transformacion sin consultar el codigo.
- **CE-003**: Ambos documentos se renderizan correctamente en cualquier visor Markdown estandar (GitHub, VS Code, Azure DevOps) sin errores de formato, tablas rotas ni caracteres mal codificados.
- **CE-004**: El 100% de las claves de dbo.Parametros y de los parametros del pipeline LSDP estan documentados en el ManualTecnico.md con su descripcion, tipo de valor y ejemplo de uso.
- **CE-005**: El 100% de las entidades de datos del proyecto (3 parquets, 3 streaming tables, 2 vistas de plata, 2 vistas de oro) estan documentadas en el ModeladoDatos.md con su nombre completo, columnas, tipos de datos y descripciones.
- **CE-006**: Los documentos no contienen emojis, estan escritos completamente en espanol, y siguen un estilo profesional y explicativo.

## Supuestos

- Se asume que el codigo fuente del proyecto en los directorios src/ y scripts/ ya esta completo y funcional (Versiones 2 a 5 implementadas), y la documentacion se basa en el estado actual del codigo.
- Se asume que la tabla dbo.Parametros de Azure SQL contiene al menos las siguientes claves: catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata, catalogoOro.
- Se asume que los nombres de campos AS400 utilizados en los parquets (CUSTID, FRSTNM, LSTNM, TRXTYP, TRXDT, TRXTM, etc.) se mantienen tal como fueron definidos en las especificaciones de las Versiones 2 a 5.
- Se asume que la carpeta docs/ no existe actualmente en el proyecto y sera creada como parte de esta version.
- Se asume que la documentacion de pruebas se limita a una seccion de estrategia en el ManualTecnico.md (que se prueba, estructura de tests/, como ejecutar), sin documentar el codigo interno de los archivos de prueba.
- Se asume que esta version no tiene pruebas TDD propias, ya que su entregable es exclusivamente documentacion Markdown y no codigo ejecutable.
