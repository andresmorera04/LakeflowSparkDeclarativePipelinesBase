# Especificacion del Feature: Lakeflow Spark Declarative Pipelines - Medalla de Plata (Version 4)

**Feature Branch**: `004-lsdp-medalla-plata`
**Creado**: 2026-03-30
**Estado**: Borrador
**Input**: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y la transformacion de la medalla de plata a traves de vistas materializadas, tomando las tablas deltas de bronce de la Version 3. La medalla de plata debe consolidar clientes y saldos como dimension tipo 1, manejar el transaccional de forma incremental, y crear campos calculados segun las reglas de negocio definidas. El catalogo de plata se lee desde la tabla dbo.Parametros de Azure SQL (clave 'catalogoPlata') y el esquema se recibe como parametro del pipeline (esquema_plata).

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Vista Materializada Consolidada de Clientes y Saldos (Prioridad: P1)

Como analista del area de negocio de clientes, necesito una vista materializada en la medalla de plata que consolide los datos del Maestro de Clientes (cmstfl) y los Saldos (blncfl) de bronce en una sola entidad con nombres descriptivos en espanol, comportandose como una dimension tipo 1 (siempre manteniendo los datos mas recientes por cliente), para poder consultar la informacion consolidada de cada cliente junto con su situacion financiera actual sin necesidad de hacer joins manuales.

**Por que esta prioridad**: La vista consolidada de clientes y saldos es la base dimensional del modelo de plata. Sin esta entidad, no es posible analizar el comportamiento de los clientes ni cruzar informacion con las transacciones. Es la pieza central que habilita el producto de datos solicitado por el area de negocio.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la vista materializada se crea en el catalogo leido de Azure SQL y el esquema recibido por parametro del pipeline, (2) contiene una unica fila por cliente (dimension tipo 1, datos mas recientes), (3) los nombres de columnas estan en espanol, snake_case y son intuitivos, (4) el join entre cmstfl y blncfl se realiza correctamente por CUSTID, (5) incluye los 3 campos calculados basados en CASE (when/otherwise) y el campo SHA2_256.

**Escenarios de Aceptacion**:

1. **Dado** que las tablas `bronce_dev.regional.cmstfl` y `bronce_dev.regional.blncfl` contienen datos historicos acumulados, **Cuando** se ejecuta el pipeline LSDP con el parametro `esquema_plata` y la clave `catalogoPlata` esta configurada en dbo.Parametros de Azure SQL, **Entonces** se crea la vista materializada consolidada en `{catalogoPlata}.{esquema_plata}` con una fila por cliente conteniendo los datos mas recientes de ambas fuentes.
2. **Dado** que un cliente tiene multiples registros historicos en bronce (por diferentes ingestas), **Cuando** la vista materializada procesa los datos, **Entonces** solo conserva el registro con la `FechaIngestaDatos` mas reciente para cada `CUSTID`, garantizando el comportamiento de dimension tipo 1.
3. **Dado** que la vista materializada incluye campos calculados basados en reglas CASE, **Cuando** se verifican los campos calculados, **Entonces** cada campo calculado utiliza al menos 3 campos/columnas de las tablas de bronce como insumo, segun las reglas de negocio definidas.
4. **Dado** que se requiere un campo de identificacion unica cifrada, **Cuando** se calcula el campo basado en SHA2_256 a partir del identificador de cliente (CUSTID), **Entonces** el campo contiene el hash SHA-256 del identificador y es consistente entre ejecuciones para el mismo cliente.

---

### Historia de Usuario 2 - Vista Materializada Transaccional Enriquecida (Prioridad: P1)

Como analista del area de negocio de clientes, necesito una vista materializada en la medalla de plata que contenga las transacciones enriquecidas con campos calculados numericos, procesandose aprovechando las capacidades de LSDP para gestion automatica de datos, para poder analizar el comportamiento transaccional de los clientes, particularmente los depositos y retiros por ATM y los pagos al saldo.

**Por que esta prioridad**: La vista transaccional es el segundo pilar del modelo de plata y contiene los datos de actividad que el area de negocio necesita analizar (uso de ATM, pagos al saldo). Sin esta entidad, no se puede generar el producto de datos requerido.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la vista materializada se crea en el catalogo leido de Azure SQL y el esquema recibido por parametro, (2) los nombres de columnas estan en espanol y snake_case, (3) contiene al menos 4 campos calculados numericos donde cada uno usa minimo 2 campos numericos de bronce, (4) la estrategia de actualizacion es gestionada por LSDP segun refresh_policy='auto', (5) no aplica filtros en la lectura de la tabla de bronce transaccional.

**Escenarios de Aceptacion**:

1. **Dado** que la tabla `bronce_dev.regional.trxpfl` contiene datos transaccionales historicos, **Cuando** se ejecuta el pipeline LSDP, **Entonces** se crea la vista materializada transaccional en `{catalogoPlata}.{esquema_plata}` con todos los registros enriquecidos con campos calculados.
2. **Dado** que se agregan nuevos datos transaccionales a bronce (nueva ejecucion del generador de parquets), **Cuando** se ejecuta nuevamente el pipeline, **Entonces** la vista materializada refleja todos los registros incluyendo los nuevos (la estrategia de actualizacion — incremental o full — es gestionada automaticamente por LSDP segun `refresh_policy='auto'`, decision R9-D1).
3. **Dado** que la vista materializada incluye campos calculados numericos, **Cuando** se verifican, **Entonces** cada campo calculado se basa en al menos 2 campos numericos de la tabla de bronce para generar un nuevo valor numerico derivado.
4. **Dado** que el pipeline de plata debe aprovechar al maximo las capacidades de LSDP, **Cuando** la vista materializada lee de la tabla de bronce transaccional, **Entonces** no aplica filtros en la lectura (el LSDP gestiona la incrementalidad automaticamente).

---

### Historia de Usuario 3 - Lectura Dinamica de Parametros de Plata desde Azure SQL y Pipeline (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline lea dinamicamente el catalogo de plata desde la tabla dbo.Parametros de Azure SQL (clave 'catalogoPlata') reutilizando la funcion existente de lectura, y que reciba el esquema de plata como parametro del pipeline LSDP (`esquema_plata`), para que la configuracion sea completamente dinamica y sin valores hardcodeados.

**Por que esta prioridad**: Sin el catalogo leido de Azure SQL y el esquema del pipeline, los scripts de plata no pueden referenciar la ubicacion correcta en Unity Catalog. Esta funcionalidad es la base de configuracion para todas las vistas materializadas de plata.

**Prueba Independiente**: Se puede validar verificando que: (1) el catalogo de plata se lee correctamente desde dbo.Parametros con la clave 'catalogoPlata', (2) el esquema de plata se lee correctamente desde los parametros del pipeline, (3) la funcion existente `leer_parametros_azure_sql` se reutiliza y se extiende para incluir la nueva clave 'catalogoPlata' sin romper la compatibilidad con los scripts de bronce, (4) los valores se propagan correctamente a las funciones decoradas via closure.

**Escenarios de Aceptacion**:

1. **Dado** que la tabla dbo.Parametros contiene la clave 'catalogoPlata' con el valor "plata_dev" y el pipeline recibe el parametro `esquema_plata` (ej: "regional"), **Cuando** los scripts de transformacion de plata se inicializan, **Entonces** el catalogo ('catalogoPlata') se obtiene del diccionario retornado por `leer_parametros_azure_sql` y el esquema se lee via `spark.conf.get("pipelines.parameters.esquema_plata")`, quedando ambos disponibles como variables de modulo capturadas por closure.
2. **Dado** que la funcion `leer_parametros_azure_sql` actualmente retorna solo las 4 claves de bronce (catalogoBronce, contenedorBronce, datalake, DirectorioBronce), **Cuando** se necesita tambien la clave 'catalogoPlata', **Entonces** la funcion DEBE ser refactorizada para retornar TODAS las claves de dbo.Parametros como diccionario sin filtro, de modo que los scripts de bronce siguen funcionando y los de plata acceden a la clave 'catalogoPlata'.
3. **Dado** que las vistas materializadas de plata se crean en un catalogo diferente al de bronce, **Cuando** se declaran con el decorador `@dp.materialized_view`, **Entonces** se especifica el catalogo y esquema completo para que se publiquen en `{catalogoPlata}.{esquema_plata}`.

---

### Historia de Usuario 4 - Suite de Pruebas TDD para la Medalla de Plata (Prioridad: P2)

Como Ingeniero de Datos, necesito un conjunto de pruebas automatizadas (TDD) que validen el correcto funcionamiento de la medalla de plata del pipeline LSDP, incluyendo los campos calculados, la dimension tipo 1, la gestion automatica del transaccional, las propiedades Delta, y la compatibilidad con Computo Serverless, para asegurar la calidad del producto de datos.

**Por que esta prioridad**: Las pruebas TDD son obligatorias segun la constitucion para todas las versiones a partir de la Version 2. Sin embargo, la funcionalidad del pipeline tiene mayor prioridad que las pruebas.

**Prueba Independiente**: Se puede validar ejecutando la suite de pruebas completa y verificando que: (1) todas las pruebas pasan exitosamente, (2) validan la estructura de las vistas materializadas de plata (nombres de columnas en espanol, tipos, campos calculados), (3) validan las propiedades Delta, (4) validan los campos calculados con valores esperados, (5) validan el comportamiento de dimension tipo 1, (6) validan que la clave 'catalogoPlata' se lee correctamente desde dbo.Parametros.

**Escenarios de Aceptacion**:

1. **Dado** que se ejecuta la suite de pruebas, **Cuando** el pipeline esta correctamente implementado, **Entonces** el 100% de las pruebas pasan exitosamente.
2. **Dado** que un campo calculado produce un valor incorrecto (por ejemplo, el hash SHA2_256 no coincide), **Cuando** se ejecutan las pruebas de campos calculados, **Entonces** las pruebas fallan indicando exactamente el campo y el valor esperado versus el obtenido.
3. **Dado** que una propiedad Delta no esta configurada, **Cuando** se ejecutan las pruebas de propiedades, **Entonces** las pruebas fallan indicando la propiedad ausente.

---

### Casos Borde

- Que sucede si un cliente existe en cmstfl pero no tiene registro en blncfl? La vista consolidada debe incluir al cliente con los campos de saldo como nulos (left join desde cmstfl).
- Que sucede si un cliente tiene registros en blncfl pero no en cmstfl? No debe incluirse en la vista consolidada (cmstfl es la tabla maestra).
- Que sucede si el campo CUSTID es nulo en alguna tabla de bronce? Los registros con CUSTID nulo deben ser excluidos de la vista consolidada y documentados como datos de baja calidad.
- Que sucede si el campo TRXAMT (monto de transaccion) es nulo o cero? Los campos calculados numericos del transaccional deben manejar valores nulos y ceros sin generar errores de division por cero o resultados nulos inesperados.
- Que sucede si las tablas de bronce estan vacias (primera ejecucion sin datos)? Las vistas materializadas de plata deben crearse vacias sin errores, listas para recibir datos en la siguiente ejecucion.
- Que sucede si la clave 'catalogoPlata' no existe en la tabla dbo.Parametros de Azure SQL? El pipeline debe fallar con un mensaje claro indicando que la clave 'catalogoPlata' no se encontro en dbo.Parametros.
- Que sucede si el parametro `esquema_plata` no se proporciona al pipeline? El pipeline debe fallar con un mensaje claro indicando el parametro faltante.
- Que sucede si el catalogo o esquema de plata no existe en Unity Catalog? El pipeline debe fallar con un error descriptivo que permita diagnosticar el problema.

## Clarifications

### Session 2026-03-30

- Q: Cuales columnas de bronce se trasladan a plata (todas vs seleccion curada vs minimo estricto)? → A: TODAS las columnas de bronce se trasladan a plata renombradas a espanol, excluyendo unicamente las columnas de año, mes y dia provenientes del lazy evaluation.
- Q: Se deben definir los umbrales numericos de los campos calculados CASE ahora o diferir a planificacion? → A: Definir umbrales especificos ahora en la spec para que sean deterministas y verificables en TDD.
- Q: Estrategia para extender leer_parametros_azure_sql: parametrizar claves o retornar todas sin filtro? → A: Retornar TODAS las claves de dbo.Parametros como diccionario sin filtro; cada script consume solo las que necesita.
- Q: Que tipo de expectation usar para CUSTID nulo: expect (dejar pasar), expect_or_drop (eliminar), o expect_or_fail (detener)? → A: `@dp.expect_or_drop` — eliminar registros con CUSTID nulo silenciosamente, con metricas de monitoreo registradas.
- Q: Deben las vistas materializadas incluir un comment descriptivo en el decorador para discoverability en Unity Catalog? → A: Si, incluir comment obligatorio en espanol en cada decorador @dp.materialized_view.

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: El pipeline LSDP DEBE implementarse usando exclusivamente la biblioteca `from pyspark import pipelines as dp`. Las vistas materializadas de plata DEBEN usar el decorador `@dp.materialized_view`. El uso de `import dlt` esta prohibido.
- **RF-002**: Las vistas materializadas de plata DEBEN crearse en un catalogo y esquema diferentes al de bronce. La configuracion se obtiene de dos fuentes distintas:
  - **Catalogo de plata**: Se lee desde la tabla `dbo.Parametros` de Azure SQL usando la clave `catalogoPlata` (ej: valor "plata_dev"). Se obtiene reutilizando la funcion existente `leer_parametros_azure_sql` de utilities/ (refactorizada segun RF-019 para retornar TODAS las claves sin filtro) y accediendo a la clave `catalogoPlata` del diccionario retornado.
  - **Esquema de plata**: Se recibe como parametro del pipeline LSDP y se lee via `spark.conf.get("pipelines.parameters.esquema_plata")` (ej: "regional").
- **RF-003**: La vista materializada consolidada de clientes y saldos DEBE comportarse como una dimension tipo 1, conservando unicamente los datos mas recientes por cada cliente (CUSTID). El procesamiento DEBE:
  - Obtener los registros con la `FechaIngestaDatos` mas reciente para cada CUSTID en cmstfl.
  - Obtener los registros con la `FechaIngestaDatos` mas reciente para cada CUSTID en blncfl.
  - Realizar un join (LEFT JOIN desde cmstfl hacia blncfl) por CUSTID.
  - El procesamiento DEBE cumplir con los mas altos niveles de optimizacion, asegurando eficiencia usando funciones de ventana (window functions) con particion por CUSTID y orden descendente por FechaIngestaDatos.
- **RF-004**: La vista materializada consolidada de clientes y saldos DEBE incluir un minimo de 3 campos calculados basados en logica condicional (equivalente a CASE de SQL, implementado con `when`/`otherwise` de PySpark). Cada campo calculado DEBE utilizar como minimo 3 campos/columnas de las tablas delta de la medalla de bronce como insumo. La evaluacion es secuencial (primer match gana). Los campos calculados con umbrales especificos son:
  - **Campo 1 — `clasificacion_riesgo_cliente`**: Insumos: RISKLV (cmstfl, StringType "01"-"05"), CRDSCR (cmstfl, LongType 300-850), OVDST (blncfl, StringType "NO"/"SI"). Umbrales secuenciales:
    1. "RIESGO_CRITICO": RISKLV in ("04","05") AND CRDSCR < 500 AND OVDST == "SI"
    2. "RIESGO_ALTO": RISKLV in ("03","04","05") AND CRDSCR < 600 AND OVDST == "SI"
    3. "RIESGO_MEDIO": RISKLV in ("03","04","05") OR (CRDSCR < 650 AND OVDST == "SI")
    4. "RIESGO_BAJO": RISKLV == "02" OR CRDSCR < 700
    5. "SIN_RIESGO": otherwise
  - **Campo 2 — `categoria_saldo_disponible`**: Insumos: AVLBAL (blncfl, DoubleType 0-500000), CRDLMT (blncfl, DoubleType variable), SGMNT (cmstfl, StringType "VIP"/"PREM"/"STD"/"BAS"). Umbrales secuenciales:
    1. "SALDO_PREMIUM": AVLBAL >= 200000 AND CRDLMT >= 100000 AND SGMNT == "VIP"
    2. "SALDO_ALTO": AVLBAL >= 100000 AND CRDLMT >= 50000 AND SGMNT in ("VIP","PREM")
    3. "SALDO_MEDIO": AVLBAL >= 25000 AND CRDLMT >= 10000
    4. "SALDO_BAJO": AVLBAL >= 5000 OR (CRDLMT >= 5000 AND SGMNT in ("STD","BAS"))
    5. "SALDO_CRITICO": otherwise
  - **Campo 3 — `perfil_actividad_bancaria`**: Insumos: LSTTRX (cmstfl, DateType ultimos 730 dias), TTLPRD (cmstfl, LongType 1-15), ACCTST (cmstfl, StringType "AC"/"IN"/"CL"/"SU"). Se calcula `dias_sin_transaccion = datediff(current_date(), LSTTRX)`. Umbrales secuenciales:
    1. "CLIENTE_INTEGRAL": dias_sin_transaccion <= 30 AND TTLPRD >= 8 AND ACCTST == "AC"
    2. "ACTIVIDAD_ALTA": dias_sin_transaccion <= 90 AND TTLPRD >= 5 AND ACCTST == "AC"
    3. "ACTIVIDAD_MEDIA": dias_sin_transaccion <= 180 AND TTLPRD >= 3 AND ACCTST == "AC"
    4. "ACTIVIDAD_BAJA": dias_sin_transaccion <= 365 AND TTLPRD >= 1 AND ACCTST in ("AC","IN")
    5. "INACTIVO": otherwise
- **RF-005**: La vista materializada consolidada de clientes y saldos DEBE incluir un campo calculado `huella_identificacion_cliente` generado a partir del identificador de cliente unico (CUSTID) usando el algoritmo SHA2_256. Este campo DEBE generarse usando la funcion `sha2(col("CUSTID").cast("string"), 256)` de PySpark.
- **RF-006**: La vista materializada transaccional DEBE manejar las transacciones como vista materializada de LSDP. La lectura de la tabla `bronce_dev.regional.trxpfl` NO DEBE aplicar filtros; la gestion de datos es manejada por el decorador `@dp.materialized_view` de Lakeflow Spark Declarative Pipelines.
- **RF-007**: La vista materializada transaccional DEBE incluir al menos 4 campos calculados numericos. Cada campo calculado DEBE basarse en minimo 2 campos/columnas de tipo numerico de la tabla de bronce para calcular un nuevo valor numerico. Los campos calculados definidos son:
  - **Campo 1 — `monto_neto_comisiones`**: Diferencia entre el monto neto de la transaccion (NETAMT) y el total de comisiones e impuestos aplicados (FEEAMT + TAXAMT). Formula: `NETAMT - (FEEAMT + TAXAMT)`.
  - **Campo 2 — `porcentaje_comision_sobre_monto`**: Porcentaje que representan las comisiones (FEEAMT) respecto al monto original de la transaccion (ORGAMT). Formula: `(FEEAMT / ORGAMT) * 100`, con manejo de division por cero.
  - **Campo 3 — `variacion_saldo_transaccion`**: Diferencia absoluta entre el saldo despues (BLNAFT) y el saldo antes (BLNBFR) de la transaccion. Formula: `abs(BLNAFT - BLNBFR)`.
  - **Campo 4 — `indicador_impacto_financiero`**: Calculo del impacto financiero total considerando el monto de la transaccion (TRXAMT), las comisiones (FEEAMT), los impuestos (TAXAMT) y las penalidades (PNLAMT). Formula: `TRXAMT + FEEAMT + TAXAMT + PNLAMT`.
- **RF-008**: Todas las vistas materializadas de plata DEBEN tener configuradas las siguientes propiedades Delta:
  - Change Data Feed: `"delta.enableChangeDataFeed": "true"`
  - autoOptimize.autoCompact: `"delta.autoOptimize.autoCompact": "true"`
  - autoOptimize.optimizeWrite: `"delta.autoOptimize.optimizeWrite": "true"`
  - Liquid Cluster: via parametro `cluster_by` en el decorador
  - deletedFileRetentionDuration: `"delta.deletedFileRetentionDuration": "interval 30 days"`
  - logRetentionDuration: `"delta.logRetentionDuration": "interval 60 days"`
- **RF-009**: Todas las vistas materializadas de plata DEBEN usar la funcion existente `reordenar_columnas_liquid_cluster` de utilities/ para garantizar que las columnas del Liquid Cluster queden dentro de las primeras 32 columnas del DataFrame, manteniendo las estadisticas min/max necesarias para el clustering.
- **RF-010**: Los nombres de las vistas materializadas y sus campos/columnas en plata DEBEN estar en espanol, en formato snake_case, con nombres intuitivos y claros, conformados por dos palabras o silabas como minimo. Los nombres de campos NO pueden ser los mismos que los usados en bronce (nombres AS400). TODAS las columnas de bronce se trasladan a plata renombradas a espanol, con la unica excepcion de las columnas de año, mes y dia provenientes del lazy evaluation, que quedan excluidas.
- **RF-011**: Los scripts de plata DEBEN reutilizar las funciones de utilidad existentes en la carpeta utilities/ (LsdpConexionAzureSql, LsdpConstructorRutasAbfss, LsdpReordenarColumnasLiquidCluster). Si se crean nuevas funciones transversales para plata, DEBEN ubicarse en utilities/ y NO DEBEN usar decoradores de pipelines de Spark (`@dp.table`, `@dp.materialized_view`, etc.).
- **RF-012**: El pipeline DEBE ser 100% compatible con Computo Serverless de Databricks. Esta PROHIBIDO el uso de `spark.sparkContext` y cualquier acceso directo al JVM del driver.
- **RF-013**: Todo el codigo DEBE estar en espanol, con variables, funciones y objetos en formato snake_case en minuscula. Los archivos .py DEBEN tener nombre en formato PascalCase con prefijo "Lsdp" (para scripts del pipeline en utilities/ y transformations/) y "Nb" (para notebooks de prueba en tests/). Cada celda DEBE tener un bloque markdown explicativo y el codigo DEBE estar completamente comentado al detalle.
- **RF-014**: La estructura de archivos DEBE respetar la organizacion de Lakeflow Spark Declarative Pipelines: carpeta utilities/ para funciones transversales reutilizables y carpeta transformations/ para los scripts que definen las vistas materializadas de plata.
- **RF-015**: La Version 4 DEBE contar con una suite de pruebas TDD ubicada en `tests/LSDP_Laboratorio_Basico/` que valide: la estructura de las vistas materializadas de plata (nombres de columnas en espanol, tipos), los campos calculados (valores correctos segun las formulas definidas), las propiedades Delta, el comportamiento de dimension tipo 1, la integridad referencial entre entidades, y que la clave 'catalogoPlata' se lee correctamente desde dbo.Parametros.
- **RF-016**: El pipeline LSDP DEBE permitir que las vistas materializadas de plata se publiquen en un catalogo y esquema diferente al elegido por defecto ("bronce_dev" / "regional"). Para ello, se DEBE usar la sintaxis de nombre completo en el decorador: `@dp.materialized_view(name="{catalogoPlata}.{esquema_plata}.nombre_vista", comment="...", ...)`, donde `catalogoPlata` proviene del diccionario de Azure SQL y `esquema_plata` del parametro del pipeline, ambos capturados como variables de modulo por closure.
- **RF-017**: Los campos calculados del transaccional DEBEN manejar valores nulos de forma segura, usando `coalesce` o `when(col.isNull(), valor_defecto)` para evitar propagacion de nulos y errores de division por cero en los calculos.
- **RF-018**: La vista materializada consolidada de clientes y saldos DEBE incluir validaciones de calidad de datos usando `@dp.expect_or_drop` para registros con CUSTID nulo, eliminandolos silenciosamente de la capa de plata. Las metricas de registros eliminados quedan registradas automaticamente por LSDP para monitoreo. Esto asegura que solo datos validos (con identificador de cliente presente) lleguen a la capa de plata sin detener la ejecucion del pipeline.
- **RF-019**: La funcion `leer_parametros_azure_sql` de `utilities/LsdpConexionAzureSql.py` DEBE ser refactorizada para retornar TODAS las claves de la tabla dbo.Parametros como diccionario, eliminando el filtro actual de 4 claves fijas (catalogoBronce, contenedorBronce, datalake, DirectorioBronce). De esta forma, cada script consume unicamente las claves que necesita del diccionario retornado. Los scripts de bronce existentes (V3) siguen funcionando sin modificacion ya que acceden a las mismas claves que antes. Los scripts de plata (V4) acceden a la clave `catalogoPlata` del mismo diccionario. Cualquier clave futura agregada a dbo.Parametros estara disponible automaticamente.
- **RF-020**: Cada vista materializada de plata DEBE incluir un parametro `comment` obligatorio en espanol dentro del decorador `@dp.materialized_view`. El comment DEBE describir de forma clara y concisa el proposito de la vista, sus fuentes de datos y su funcion dentro del modelo de plata. Este comment se muestra en Unity Catalog como documentacion de la tabla, mejorando la discoverability y la gobernanza de datos.

### Entidades Clave

- **Vista Materializada `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados`**: Vista materializada que consolida el Maestro de Clientes y los Saldos como dimension tipo 1 (datos mas recientes por cliente). Contiene TODAS las columnas de bronce renombradas a espanol (excepto año, mes y dia del lazy evaluation), 3 campos calculados por reglas CASE, 1 campo SHA2_256 y la informacion financiera actual de cada cliente. Liquid Cluster por `huella_identificacion_cliente` y `identificador_cliente`.
- **Vista Materializada `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas`**: Vista materializada que contiene las transacciones con campos calculados numericos derivados. Contiene TODAS las columnas de bronce renombradas a espanol (excepto año, mes y dia del lazy evaluation) y 4 campos calculados numericos. Liquid Cluster por `fecha_transaccion`, `identificador_cliente` y `tipo_transaccion`.
- **Tablas Fuente de Bronce**: `bronce_dev.regional.cmstfl` (72 columnas), `bronce_dev.regional.trxpfl` (62 columnas), `bronce_dev.regional.blncfl` (102 columnas) — creadas en la Version 3.
- **Tabla dbo.Parametros (Azure SQL) — claves nuevas para V4**: Ademas de las 4 claves existentes de bronce (catalogoBronce, contenedorBronce, datalake, DirectorioBronce), la tabla DEBE contener la clave `catalogoPlata` (ej: valor "plata_dev") para la medalla de plata.
- **Parametros del Pipeline (nuevos para V4)**: `esquema_plata` (ej: "regional") como parametro del pipeline LSDP, ademas del existente `nombreScopeSecret`. El catalogo de plata NO es parametro del pipeline; se lee de Azure SQL.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: Las 2 vistas materializadas de plata se crean exitosamente en el catalogo leido desde dbo.Parametros (clave 'catalogoPlata') y el esquema recibido como parametro del pipeline al ejecutar el pipeline LSDP.
- **CE-002**: La vista consolidada de clientes y saldos contiene exactamente una fila por cliente (dimension tipo 1), con los datos mas recientes de ambas fuentes de bronce.
- **CE-003**: Los 3 campos calculados por regla CASE de la vista consolidada producen valores correctos verificables para el 100% de los registros.
- **CE-004**: El campo `huella_identificacion_cliente` (SHA2_256) genera un hash consistente y determinista para cada `CUSTID` en el 100% de las ejecuciones.
- **CE-005**: Los 4 campos calculados numericos de la vista transaccional producen valores correctos verificables, sin errores por nulos o division por cero.
- **CE-006**: La vista transaccional se actualiza correctamente al agregar nuevos datos a bronce, incorporando los registros nuevos.
- **CE-007**: Todos los nombres de columnas en las vistas de plata estan en espanol, formato snake_case, son intuitivos y diferentes a los nombres AS400 de bronce.
- **CE-008**: Todas las vistas materializadas de plata tienen configuradas las 6 propiedades Delta requeridas (Change Data Feed, autoCompact, optimizeWrite, Liquid Cluster, retenciones de 30 y 60 dias).
- **CE-009**: El pipeline completo se ejecuta exitosamente en Computo Serverless de Databricks sin errores de compatibilidad.
- **CE-010**: La suite de pruebas TDD cubre cada uno de los requerimientos funcionales (RF-001 a RF-020) con al menos una prueba asociada.
- **CE-011**: Cero valores hardcodeados en los scripts de plata. El catalogo proviene de dbo.Parametros (Azure SQL) y el esquema del parametro del pipeline.
- **CE-012**: Las funciones transversales nuevas (si las hay) estan ubicadas en utilities/ y no usan decoradores de Lakeflow Spark Declarative Pipelines.
- **CE-013**: La funcion `leer_parametros_azure_sql` se extiende o refactoriza sin romper la compatibilidad con los scripts de bronce de la Version 3.

## Supuestos

- Las tablas streaming de bronce (`bronce_dev.regional.cmstfl`, `bronce_dev.regional.trxpfl`, `bronce_dev.regional.blncfl`) estan creadas y contienen datos de al menos una ejecucion de la Version 3.
- El catalogo `plata_dev` y el esquema `regional` existen en Unity Catalog y tienen los permisos necesarios para crear vistas materializadas.
- La tabla dbo.Parametros de Azure SQL contiene la clave `catalogoPlata` con el valor que corresponde al catalogo de plata en Unity Catalog (ej: "plata_dev"). Esta clave coexiste con las 4 claves existentes de bronce.
- Las funciones de utilidad existentes en utilities/ (LsdpConexionAzureSql, LsdpConstructorRutasAbfss, LsdpReordenarColumnasLiquidCluster) estan probadas y funcionales desde la Version 3.
- El Scope Secret de Databricks y la configuracion de Azure SQL siguen operativos como en la Version 3.
- El pipeline LSDP existente de la Version 3 se extiende con los nuevos scripts de plata en el mismo proyecto; no se crea un pipeline separado.
- Las decisiones aprobadas en la Version 1 (R1-D1 a R5-D2) y los requerimientos de la Version 3 siguen vigentes como fuente de verdad.
- El decorador `@dp.materialized_view` acepta los mismos parametros que `@dp.table` (segun H1.3 del research V1), incluyendo `name`, `table_properties`, `cluster_by` y `comment`.
- Las vistas materializadas de plata referencian tablas de bronce usando `spark.read.table("bronce_dev.regional.cmstfl")` (lectura batch) para la consolidacion de clientes y saldos, y el LSDP gestiona internamente la actualizacion para la vista transaccional.
