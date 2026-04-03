# Especificacion del Feature: Lakeflow Spark Declarative Pipelines - Medalla de Bronce (Version 3)

**Feature Branch**: `003-lsdp-medalla-bronce`
**Creado**: 2026-03-29
**Estado**: Lista para Implementacion
**Input**: Crear el Lakeflow Spark Declarative Pipelines con solo el procesamiento de la medalla de bronce, incluyendo la lectura dinamica de parametros desde Azure SQL (tabla dbo.Parametros), la construccion dinamica de rutas abfss:// para los 3 parquets (MaestroCliente, SaldoCliente, Transaccional) y la ingesta mediante AutoLoader con marca de tiempo (FechaIngestaDatos) en tablas streaming de bronce.

## Escenarios de Usuario y Pruebas *(obligatorio)*
### Historia de Usuario 1 - Lectura Dinamica de Parametros desde Azure SQL (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline de Lakeflow Spark Declarative Pipelines lea dinamicamente los parametros de configuracion (catalogoBronce, contenedorBronce, datalake, DirectorioBronce) desde la tabla dbo.Parametros de Azure SQL mediante una funcion de utilidad encapsulada en la carpeta utilities/, para que las rutas de los parquets y los nombres de catalogos se construyan sin valores hardcodeados.

**Por que esta prioridad**: Sin la lectura dinamica de parametros, no es posible construir las rutas abfss:// de los parquets ni referenciar los catalogos correctos. Esta funcionalidad es la base sobre la que dependen todas las demas historias de usuario de esta version.

**Prueba Independiente**: Se puede validar ejecutando la funcion de utilidad de forma aislada y verificando que: (1) la conexion a Azure SQL se establece correctamente usando los dos secretos (sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd), (2) la tabla dbo.Parametros se lee exitosamente, (3) se obtienen los valores correctos para las claves catalogoBronce, contenedorBronce, datalake y DirectorioBronce, (4) la funcion retorna los valores en un formato consumible por los scripts de transformaciones.

**Escenarios de Aceptacion**:

1. **Dado** que la tabla dbo.Parametros contiene las claves catalogoBronce, contenedorBronce, datalake y DirectorioBronce con valores validos, **Cuando** la funcion de utilidad se ejecuta dentro del pipeline LSDP, **Entonces** retorna correctamente los valores asociados a cada clave.
2. **Dado** que la conexion a Azure SQL usa los dos secretos complementarios del Scope Secret de Databricks, **Cuando** la funcion construye la cadena JDBC combinando ambos secretos, **Entonces** la conexion se establece sin exponer credenciales en el codigo.
3. **Dado** que la funcion de utilidad se invoca desde un script de la carpeta transformations/, **Cuando** se ejecuta dentro del contexto del pipeline LSDP en Computo Serverless, **Entonces** la lectura JDBC funciona sin errores de compatibilidad.

---

### Historia de Usuario 2 - Construccion Dinamica de Rutas abfss:// (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline construya dinamicamente las rutas abfss:// combinando los parametros leidos de Azure SQL (contenedorBronce, datalake, DirectorioBronce) con los parametros recibidos por el pipeline (rutaCompletaMaestroCliente, rutaCompletaSaldoCliente, rutaCompletaTransaccional), para que la ubicacion de los parquets sea completamente configurable sin modificar codigo.

**Por que esta prioridad**: Las rutas abfss:// son el insumo directo del AutoLoader para la ingesta de bronce. Sin rutas correctamente construidas, el pipeline no puede localizar ni leer los parquets fuente.

**Prueba Independiente**: Se puede validar verificando que: (1) la ruta generada sigue el formato abfss://contenedor@datalake.dfs.core.windows.net/directorioRaiz/rutaCompleta, (2) los valores del contenedor, datalake y directorio raiz provienen de Azure SQL, (3) la rutaCompleta proviene de los parametros del pipeline, (4) la ruta resultante es accesible y apunta a archivos parquet existentes.

**Escenarios de Aceptacion**:

1. **Dado** que los parametros de Azure SQL son contenedorBronce="bronce", datalake="adlsg2datalakedev" y DirectorioBronce="archivos", y el parametro del pipeline rutaCompletaMaestroCliente="LSDP_Base/As400/MaestroCliente/", **Cuando** se construye la ruta abfss://, **Entonces** la ruta resultante es "abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/".
2. **Dado** que las mismas reglas aplican para SaldoCliente y Transaccional, **Cuando** se construyen las 3 rutas dinamicamente, **Entonces** cada ruta es correcta y apunta a la ubicacion esperada de los parquets correspondientes.
3. **Dado** que un valor de parametro cambia en la tabla dbo.Parametros (por ejemplo, datalake cambia de "adlsg2datalakedev" a "adlsg2datalakeprd"), **Cuando** se reconstruyen las rutas, **Entonces** las nuevas rutas reflejan el cambio sin necesidad de modificar codigo.

---

### Historia de Usuario 3 - Ingesta de Maestro de Clientes en Bronce con AutoLoader (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline LSDP ingeste los parquets del Maestro de Clientes desde la ruta abfss:// construida dinamicamente, usando AutoLoader (cloudFiles) con el decorador @dp.table, agregando la marca de tiempo FechaIngestaDatos, y almacenando los datos de forma historica acumulativa en una tabla streaming en el catalogo bronce_dev, para que el area de negocio cuente con un registro historico completo de todos los cambios en el maestro de clientes.

**Por que esta prioridad**: El Maestro de Clientes es la entidad central del modelo de datos. La ingesta en bronce es el primer paso del pipeline y es prerequisito para las medallas de plata y oro en versiones futuras.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la tabla streaming se crea en el catalogo bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data (el conteo exacto puede variar debido a schema evolution con addNewColumns), (2) los datos se ingesstan correctamente desde la ruta abfss://, (3) el AutoLoader procesa archivos nuevos de forma incremental, (4) la marca de tiempo FechaIngestaDatos se agrega automaticamente a cada registro, (5) las propiedades Delta estan configuradas correctamente (Change Data Feed, autoOptimize, Liquid Cluster, retenciones), (6) el schemaLocation del AutoLoader apunta a la ruta de checkpoint configurada por parametro (R7-D1).

**Escenarios de Aceptacion**:

1. **Dado** que existen archivos parquet del Maestro de Clientes en la ruta abfss:// construida dinamicamente, **Cuando** se ejecuta el pipeline LSDP por primera vez, **Entonces** se crea la tabla streaming en bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data, con la marca de tiempo de ingesta. El conteo exacto de columnas puede variar debido a schema evolution (addNewColumns).
2. **Dado** que la tabla streaming ya contiene datos de una ejecucion previa, **Cuando** se agregan nuevos archivos parquet y se ejecuta nuevamente el pipeline, **Entonces** solo los archivos nuevos se procesan (comportamiento incremental del AutoLoader) y los datos se acumulan historicamente.
3. **Dado** que la tabla streaming se crea con las propiedades Delta configuradas, **Cuando** se verifica la tabla, **Entonces** tiene habilitados Change Data Feed, autoOptimize.autoCompact, autoOptimize.optimizeWrite, Liquid Cluster y las retenciones de 30 y 60 dias respectivas.

---

### Historia de Usuario 4 - Ingesta de Transaccional en Bronce con AutoLoader (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline LSDP ingeste los parquets del Transaccional desde la ruta abfss:// construida dinamicamente, usando AutoLoader con el decorador @dp.table, agregando la marca de tiempo FechaIngestaDatos, y almacenando los datos de forma historica acumulativa en una tabla streaming en el catalogo bronce_dev, para que se conserve el historial completo de todas las transacciones procesadas.

**Por que esta prioridad**: El Transaccional contiene los datos de actividad bancaria (uso de ATM, pagos al saldo) que son el corazon del analisis requerido por el area de negocio.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la tabla streaming se crea en bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data (el conteo exacto puede variar debido a schema evolution con addNewColumns), (2) los datos se ingesstan correctamente, (3) el AutoLoader es incremental, (4) la marca de tiempo FechaIngestaDatos se agrega, (5) las propiedades Delta estan configuradas, (6) el schemaLocation del AutoLoader apunta a la ruta de checkpoint configurada por parametro (R7-D1).

**Escenarios de Aceptacion**:

1. **Dado** que existen archivos parquet del Transaccional en la ruta abfss://, **Cuando** se ejecuta el pipeline LSDP, **Entonces** se crea la tabla streaming en bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data. El conteo exacto de columnas puede variar debido a schema evolution (addNewColumns).
2. **Dado** que se agregan nuevos archivos parquet transaccionales (correspondientes a una nueva fecha), **Cuando** se ejecuta nuevamente el pipeline, **Entonces** el AutoLoader procesa solo los archivos nuevos y los datos se acumulan historicamente junto con los procesados anteriormente.
3. **Dado** que la tabla streaming tiene las propiedades Delta correctas, **Cuando** se verifica, **Entonces** Change Data Feed, autoOptimize, Liquid Cluster y retenciones estan habilitados.

---

### Historia de Usuario 5 - Ingesta de Saldos en Bronce con AutoLoader (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline LSDP ingeste los parquets de Saldos de Clientes desde la ruta abfss:// construida dinamicamente, usando AutoLoader con el decorador @dp.table, agregando la marca de tiempo FechaIngestaDatos, y almacenando los datos de forma historica acumulativa en una tabla streaming en el catalogo bronce_dev, para que se conserve un historico de las fotos de saldos procesadas.

**Por que esta prioridad**: Los Saldos son parte integral del producto de datos para el analisis del comportamiento de clientes. La ingesta en bronce permite conservar la evolucion historica de los saldos.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la tabla streaming se crea en bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data (el conteo exacto puede variar debido a schema evolution con addNewColumns), (2) los datos se ingesstan correctamente, (3) el AutoLoader es incremental, (4) la marca de tiempo se agrega, (5) las propiedades Delta estan configuradas, (6) el schemaLocation del AutoLoader apunta a la ruta de checkpoint configurada por parametro (R7-D1).

**Escenarios de Aceptacion**:

1. **Dado** que existen archivos parquet de Saldos en la ruta abfss://, **Cuando** se ejecuta el pipeline LSDP, **Entonces** se crea la tabla streaming en bronce_dev con las columnas originales AS400 mas FechaIngestaDatos y _rescued_data. El conteo exacto de columnas puede variar debido a schema evolution (addNewColumns).
2. **Dado** que se agregan nuevos archivos parquet de saldos (correspondientes a una nueva ejecucion del generador), **Cuando** se ejecuta nuevamente el pipeline, **Entonces** el AutoLoader procesa solo los archivos nuevos y los saldos se acumulan historicamente.
3. **Dado** que las propiedades Delta estan configuradas, **Cuando** se verifica la tabla, **Entonces** tiene Change Data Feed, autoOptimize, Liquid Cluster y retenciones habilitados.

---

### Historia de Usuario 6 - Suite de Pruebas TDD para la Medalla de Bronce (Prioridad: P2)

Como Ingeniero de Datos, necesito un conjunto de pruebas automatizadas (TDD) que validen el correcto funcionamiento de la medalla de bronce del pipeline LSDP, incluyendo la lectura de parametros desde Azure SQL, la construccion de rutas abfss://, la ingesta con AutoLoader y la configuracion de propiedades Delta, para asegurar que el pipeline cumple con las politicas del constitution y las decisiones aprobadas en la Version 1.

**Por que esta prioridad**: Las pruebas TDD son obligatorias para todas las versiones a partir de la Version 2 segun las politicas del constitution. Sin embargo, la funcionalidad del pipeline tiene mayor prioridad que las pruebas.

**Prueba Independiente**: Se puede validar ejecutando la suite de pruebas completa y verificando que: (1) todas las pruebas pasan exitosamente, (2) validan la lectura de parametros desde Azure SQL, (3) validan la construccion de rutas abfss://, (4) validan la estructura de las tablas streaming de bronce (columnas, tipos, FechaIngestaDatos), (5) validan las propiedades Delta de las tablas, (6) validan el comportamiento incremental del AutoLoader.

**Escenarios de Aceptacion**:

1. **Dado** que se ejecuta la suite de pruebas, **Cuando** el pipeline esta correctamente implementado, **Entonces** el 100% de las pruebas pasan exitosamente.
2. **Dado** que una tabla de bronce se crea sin la columna FechaIngestaDatos, **Cuando** se ejecutan las pruebas de estructura, **Entonces** las pruebas fallan indicando exactamente la columna faltante.
3. **Dado** que una propiedad Delta no esta configurada (por ejemplo falta Change Data Feed), **Cuando** se ejecutan las pruebas de propiedades, **Entonces** las pruebas fallan indicando la propiedad ausente.

---

### Casos Borde

- Que sucede si la tabla dbo.Parametros de Azure SQL no contiene alguna de las claves esperadas (catalogoBronce, contenedorBronce, datalake, DirectorioBronce)? El pipeline debe fallar con un mensaje claro indicando cual clave falta.
- Que sucede si la conexion a Azure SQL falla (secretos incorrectos, firewall, timeout)? El pipeline debe fallar con un mensaje descriptivo que permita diagnosticar el problema sin exponer credenciales.
- Que sucede si la ruta abfss:// construida no contiene archivos parquet? El AutoLoader debe iniciar pero sin procesar datos, quedando en espera de archivos nuevos.
- Que sucede si los parquets en la ruta tienen un esquema diferente al esperado (columnas faltantes o adicionales)? El AutoLoader con evolucion automatica de esquema (addNewColumns) incorpora las columnas nuevas automaticamente, y los datos que no coincidan con el esquema se capturan en la columna _rescued_data sin perder informacion ni romper el pipeline.
- Que sucede si se ejecuta el pipeline en un entorno que no es Computo Serverless? El codigo debe ser compatible, pero las pruebas deben ejecutarse en Computo Serverless para validar la compatibilidad.
- Que sucede si alguno de los 7 parametros del pipeline (rutaCompletaMaestroCliente, rutaCompletaSaldoCliente, rutaCompletaTransaccional, rutaCheckpointCmstfl, rutaCheckpointTrxpfl, rutaCheckpointBlncfl, nombreScopeSecret) no se proporciona? El pipeline debe fallar con un mensaje claro indicando los parametros faltantes.

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: El pipeline LSDP DEBE implementarse usando exclusivamente la biblioteca `from pyspark import pipelines as dp`. El uso de `import dlt` esta prohibido (segun decision R1-D1).
- **RF-002**: El pipeline DEBE contar con una funcion de utilidad encapsulada en la carpeta utilities/ que lea los parametros de configuracion desde la tabla dbo.Parametros de Azure SQL (segun decision R5-D2). Esta funcion DEBE:
  - Construir la cadena JDBC combinando los dos secretos del Scope Secret (sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd).
  - Usar `spark.read.format("jdbc")` para la lectura, con el driver `com.microsoft.sqlserver.jdbc.SQLServerDriver`.
  - Retornar los valores de los parametros en un formato consumible.
  - Seguir el principio SOLID de responsabilidad unica.
  - Ejecutarse a nivel de modulo (fuera de las funciones decoradas con @dp.table), de modo que se invoque una sola vez al inicio del pipeline. Los valores resultantes DEBEN quedar disponibles como variables Python capturadas por closure para todas las funciones decoradas, evitando lecturas JDBC repetidas y cumpliendo la restriccion H1.9 del research de V1.
- **RF-003**: Los valores de configuracion que DEBEN leerse desde la tabla dbo.Parametros son: catalogoBronce (nombre del catalogo de bronce), contenedorBronce (nombre del contenedor ADLS Gen2), datalake (nombre del Azure Storage Account ADLS Gen2) y DirectorioBronce (nombre del directorio raiz dentro del contenedor). Estos valores NO se reciben como parametros del pipeline.
- **RF-004**: El pipeline DEBE recibir como parametros de entrada siete parametros: tres rutas relativas de parquets — rutaCompletaMaestroCliente (ejemplo: "LSDP_Base/As400/MaestroCliente/"), rutaCompletaSaldoCliente (ejemplo: "LSDP_Base/As400/SaldoCliente/") y rutaCompletaTransaccional (ejemplo: "LSDP_Base/As400/Transaccional/") — tres rutas relativas de checkpoints para schemaLocation del AutoLoader — rutaCheckpointCmstfl (ejemplo: "LSDP_Base/Checkpoints/Bronce/cmstfl/"), rutaCheckpointTrxpfl (ejemplo: "LSDP_Base/Checkpoints/Bronce/trxpfl/") y rutaCheckpointBlncfl (ejemplo: "LSDP_Base/Checkpoints/Bronce/blncfl/") — y un parametro de seguridad: nombreScopeSecret (ejemplo: "sc-kv-laboratorio"), que identifica el Scope Secret de Databricks para acceder al Key Vault. Las rutas complementan la parte dinamica de la ruta abfss://.
- **RF-005**: La construccion de rutas abfss:// DEBE seguir el formato: `abfss://{contenedorBronce}@{datalake}.dfs.core.windows.net/{DirectorioBronce}/{rutaCompleta}`, donde contenedorBronce, datalake y DirectorioBronce provienen de Azure SQL, y rutaCompleta proviene de los parametros del pipeline.
- **RF-006**: El pipeline DEBE crear tres tablas streaming en la medalla de bronce usando el decorador @dp.table con AutoLoader (spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")), una para cada fuente de datos: Maestro de Clientes (inicialmente 70 columnas originales + FechaIngestaDatos + _rescued_data = 72), Transaccional (inicialmente 60 columnas originales + FechaIngestaDatos + _rescued_data = 62) y Saldos (inicialmente 100 columnas originales + FechaIngestaDatos + _rescued_data = 102). El conteo total de columnas puede variar debido a schema evolution. El AutoLoader DEBE configurarse con esquema inferido, evolucion automatica de esquema (`cloudFiles.schemaEvolutionMode` = `addNewColumns`) y columna de rescate (`_rescued_data`) para capturar datos que no coincidan con el esquema sin perder informacion ni romper el pipeline.
- **RF-007**: Cada tabla streaming de bronce DEBE agregar automaticamente el campo FechaIngestaDatos con la marca de tiempo del momento de la ingesta, para llevar control historico de cuando se procesaron los datos.
- **RF-008**: Las tablas streaming de bronce DEBEN acumular datos historicamente. Cada ejecucion del pipeline DEBE agregar los datos nuevos sin sobrescribir los existentes, aprovechando el comportamiento incremental del AutoLoader.
- **RF-009**: Todas las tablas streaming de bronce DEBEN tener configuradas las siguientes propiedades Delta (segun decision R1-D5):
  - Change Data Feed: `"delta.enableChangeDataFeed": "true"`
  - autoOptimize.autoCompact: `"delta.autoOptimize.autoCompact": "true"`
  - autoOptimize.optimizeWrite: `"delta.autoOptimize.optimizeWrite": "true"`
  - Liquid Cluster: via parametro `cluster_by` en el decorador (los campos se definen por tabla)
  - deletedFileRetentionDuration: `"delta.deletedFileRetentionDuration": "interval 30 days"`
  - logRetentionDuration: `"delta.logRetentionDuration": "interval 60 days"`
- **RF-010**: El catalogo y esquema por defecto del pipeline DEBEN ser "bronce_dev" y "regional" respectivamente. Las tablas de bronce se crean en este catalogo/esquema por defecto.
- **RF-011**: El pipeline DEBE ser 100% compatible con Computo Serverless de Databricks. Esta PROHIBIDO el uso de `spark.sparkContext` y cualquier acceso directo al JVM del driver.
- **RF-012**: Todo el codigo DEBE estar en espanol, con variables, funciones y objetos en formato snake_case en minuscula. Los notebooks (.py) DEBEN tener nombre en formato PascalCase con prefijo "Lsdp" (para scripts del pipeline en utilities/ y transformations/) y "Nb" (para notebooks de prueba en tests/). Cada celda DEBE tener un bloque markdown explicativo. El codigo DEBE estar completamente comentado al detalle.
- **RF-013**: La estructura de archivos DEBE respetar la organizacion de Lakeflow Spark Declarative Pipelines: carpeta utilities/ para funciones de utilidad reutilizables (conexion Azure SQL, construccion de rutas) y carpeta transformations/ para los scripts que definen las tablas streaming de bronce.
- **RF-014**: El pipeline DEBE aprovechar el lazy evaluation de Spark. Las rutas de los parquets (por ejemplo "abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/") DEBEN apuntar al directorio raiz de la fuente, permitiendo que el AutoLoader descubra automaticamente los archivos en subdirectorios (particiones por anio/mes/dia).
- **RF-015**: La Version 3 DEBE contar con una suite de pruebas TDD ubicada en `tests/LSDP_Laboratorio_Basico/` que valide: la lectura de parametros desde Azure SQL, la construccion de rutas abfss://, la estructura de las tablas streaming de bronce (columnas y tipos), la presencia del campo FechaIngestaDatos, y la configuracion de propiedades Delta. Las restricciones de compatibilidad Serverless (prohibicion de `spark.sparkContext` e `import dlt`) se verifican mediante revision de codigo y no mediante pruebas TDD automatizadas, ya que la lectura de codigo fuente de notebooks via API REST presenta incompatibilidades con Computo Serverless.
- **RF-016**: El nombre del Scope Secret de Databricks DEBE ser un parametro configurable (no hardcodeado) en la funcion de utilidad de conexion a Azure SQL, permitiendo flexibilidad entre ambientes (complementa RF-002).
- **RF-017**: Las funciones de utilidad en la carpeta utilities/ DEBEN ser modulares y reutilizables. La funcion de conexion a Azure SQL y la funcion de construccion de rutas abfss:// DEBEN estar separadas siguiendo el principio SOLID de responsabilidad unica, para que puedan ser reutilizadas por los scripts de transformaciones de las medallas de plata y oro en versiones futuras.

### Entidades Clave

- **Tabla Streaming bronce_dev.regional.cmstfl**: Tabla streaming que almacena historicamente los datos del Maestro de Clientes ingestados desde los parquets. Nombre AS400 original (Customer Master File). Contiene inicialmente 70 columnas originales AS400 (segun R2-D1) mas FechaIngestaDatos y _rescued_data (72 iniciales — sujeto a schema evolution via addNewColumns). Clave de negocio: CUSTID. Liquid Cluster por FechaIngestaDatos y CUSTID (R6-D1).
- **Tabla Streaming bronce_dev.regional.trxpfl**: Tabla streaming que almacena historicamente los datos transaccionales ingestados desde los parquets. Nombre AS400 original (Transaction Processing File). Contiene inicialmente 60 columnas originales AS400 (segun R2-D2) mas FechaIngestaDatos y _rescued_data (62 iniciales — sujeto a schema evolution via addNewColumns). Clave de negocio: TRXID. Liquid Cluster por TRXDT, CUSTID y TRXTYP (R6-D1).
- **Tabla Streaming bronce_dev.regional.blncfl**: Tabla streaming que almacena historicamente los datos de saldos ingestados desde los parquets. Nombre AS400 original (Balance Client File). Contiene inicialmente 100 columnas originales AS400 (segun R2-D4) mas FechaIngestaDatos y _rescued_data (102 iniciales — sujeto a schema evolution via addNewColumns). Clave de negocio: CUSTID. Liquid Cluster por FechaIngestaDatos y CUSTID (R6-D1).
- **Tabla dbo.Parametros (Azure SQL)**: Tabla externa que contiene los parametros de configuracion del pipeline. Estructura clave-valor con los campos "Clave" y "Valor". Las claves relevantes para esta version son: catalogoBronce, contenedorBronce, datalake, DirectorioBronce.
- **Parametros del Pipeline**: Siete parametros de entrada que el pipeline recibe: tres rutas relativas de parquets (rutaCompletaMaestroCliente, rutaCompletaSaldoCliente, rutaCompletaTransaccional), tres rutas relativas de checkpoints para schemaLocation (rutaCheckpointCmstfl, rutaCheckpointTrxpfl, rutaCheckpointBlncfl) y un parametro de seguridad (nombreScopeSecret) para identificar el Scope Secret del Key Vault.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: Las 3 tablas streaming de bronce se crean exitosamente en el catalogo bronce_dev.regional al ejecutar el pipeline LSDP por primera vez.
- **CE-002**: Los datos de los parquets se ingeststan correctamente en las tablas streaming, preservando las columnas originales AS400 (inicialmente 70 para Maestro, 60 para Transaccional, 100 para Saldos) mas FechaIngestaDatos y _rescued_data. Nota: el conteo total de columnas puede variar en el tiempo debido a schema evolution (`cloudFiles.schemaEvolutionMode=addNewColumns`), por lo que las pruebas TDD no validan conteos exactos de columnas.
- **CE-003**: El AutoLoader procesa archivos de forma incremental: en una segunda ejecucion, solo los archivos parquet nuevos se procesan, sin reprocesar los ya ingestados.
- **CE-004**: Los parametros de configuracion (catalogoBronce, contenedorBronce, datalake, DirectorioBronce) se leen correctamente desde la tabla dbo.Parametros de Azure SQL en el 100% de las ejecuciones.
- **CE-005**: Las rutas abfss:// se construyen dinamicamente combinando parametros de Azure SQL y parametros del pipeline, sin valores hardcodeados en el codigo.
- **CE-006**: El pipeline completo se ejecuta exitosamente en Computo Serverless de Databricks sin errores de compatibilidad.
- **CE-007**: Todas las tablas streaming de bronce tienen configuradas las 6 propiedades Delta requeridas (Change Data Feed, autoCompact, optimizeWrite, Liquid Cluster, retenciones de 30 y 60 dias).
- **CE-008**: La suite de pruebas TDD cubre cada uno de los requerimientos funcionales (RF-001 a RF-017) con al menos una prueba asociada.
- **CE-009**: Cero valores hardcodeados en los scripts del pipeline. Todos los valores configurables provienen de parametros del pipeline o de la tabla dbo.Parametros.

## Clarificaciones

### Sesion 2026-03-29

- P: Cual estrategia de esquema debe usar el AutoLoader para las tablas de bronce? -> R: Esquema inferido con evolucion automatica (addNewColumns) y columna de rescate (_rescued_data). La capa de bronce debe ser tolerante y flexible, garantizando que nunca se pierdan datos y que cambios en los parquets fuente no rompan el pipeline.
- P: Donde debe ejecutarse la lectura de parametros de Azure SQL dentro del pipeline LSDP? -> R: A nivel de modulo (fuera de las funciones decoradas con @dp.table). Se ejecuta una sola vez al inicio del pipeline y los valores quedan disponibles como variables Python capturadas por closure para todas las funciones decoradas, evitando lecturas JDBC repetidas y cumpliendo la restriccion H1.9 del research.
- P: Debe el pipeline de bronce incluir expectativas de calidad de datos (@dp.expect) en las tablas streaming? -> R: No. Bronce ingesta todo sin validacion de calidad. La capa de bronce funciona como zona de aterrizaje sin perdida de datos, sin usar @dp.expect. La validacion de calidad de datos se delega exclusivamente a la medalla de plata (Version 4).
- P: Cual convencion de nombrado deben seguir las tablas streaming de bronce? -> R: Nombres AS400 originales: cmstfl, trxpfl, blncfl. Las tablas de bronce reflejan el origen de los datos tal cual llegan de la fuente. Los nombres descriptivos en espanol se reservan para las medallas de plata y oro.

## Supuestos

- El workspace de Azure Databricks con Unity Catalog habilitado, Computo Serverless y plan Premium esta disponible y accesible.
- Los parquets generados en la Version 2 (Maestro de Clientes, Transaccional, Saldos) estan almacenados en las rutas abfss:// correspondientes dentro del Azure Data Lake Storage Gen2.
- El External Location esta configurado correctamente en Unity Catalog para acceder al contenedor "bronce" del Storage Account "adlsg2datalakedev".
- El catalogo "bronce_dev" y el esquema "regional" existen en Unity Catalog y tienen los permisos necesarios para crear tablas streaming.
- El Azure SQL Server tiene el firewall configurado para aceptar conexiones desde Databricks (IP ranges o Private Link).
- El Scope Secret de Databricks esta configurado y contiene los dos secretos necesarios para la conexion a Azure SQL (sr-jdbc-asql-asqlmetadatos-adminpd y sr-asql-asqlmetadatos-adminpd).
- La tabla dbo.Parametros existe en Azure SQL y contiene las claves catalogoBronce, contenedorBronce, datalake y DirectorioBronce con valores validos.
- Las decisiones aprobadas en la Version 1 (R1-D1 a R5-D2) son la fuente de verdad y no requieren cambios para esta version.
- Las extensiones de Databricks para VS Code estan configuradas correctamente para ejecutar las pruebas TDD.
- La estructura de campos de los 3 parquets sigue exactamente las definiciones aprobadas en R2-D1 (70 campos), R2-D2 (60 campos) y R2-D4 (100 campos).
