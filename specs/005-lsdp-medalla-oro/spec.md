# Especificacion del Feature: Lakeflow Spark Declarative Pipelines - Medalla de Oro (Version 5)

**Feature Branch**: `005-lsdp-medalla-oro`
**Creado**: 2026-03-31
**Estado**: Borrador
**Input**: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y la transformacion de la medalla de oro a traves de vistas materializadas para generar el producto de datos agregado de comportamiento de clientes ATM y saldos. La medalla de oro debe contener datos altamente agregados y optimizados para consumo, comportandose como dimension tipo 1 (siempre datos mas recientes). El catalogo de oro se lee desde la tabla dbo.Parametros de Azure SQL (clave 'catalogoOro') y el esquema se recibe como parametro del pipeline (esquema_oro).

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Vista Materializada Agregada de Comportamiento ATM por Cliente (Prioridad: P1)

Como analista del area de negocio de clientes, necesito una vista materializada en la medalla de oro que presente por cada cliente la cantidad de depositos (creditos ATM) y retiros (debitos ATM) realizados, junto con el promedio de los montos depositados y retirados por ATM, y el total sumarizado de los pagos al saldo del cliente, para poder analizar el comportamiento de uso de cajeros automaticos y comprender si existe correlacion entre la actividad ATM y los pagos al saldo de cada cliente.

**Por que esta prioridad**: Esta vista es el producto de datos principal solicitado por el area de negocio de clientes. Contiene las metricas agregadas que permiten el analisis de comportamiento ATM y su relacion con los pagos al saldo, que es el objetivo central de la Version 5 y del proyecto en general.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la vista materializada se crea en el catalogo leido desde Azure SQL (clave 'catalogoOro') y el esquema recibido por parametro del pipeline (esquema_oro), (2) contiene una unica fila por cliente (dimension tipo 1), (3) las metricas agregadas de creditos ATM, debitos ATM y pagos al saldo son correctas, (4) los promedios de montos se calculan correctamente, (5) los nombres de columnas estan en espanol, snake_case y son intuitivos.

**Escenarios de Aceptacion**:

1. **Dado** que la vista materializada `transacciones_enriquecidas` de plata contiene datos transaccionales con el campo `tipo_transaccion` que incluye los valores "CATM" (credito ATM), "DATM" (debito ATM) y "PGSL" (pago al saldo), **Cuando** se ejecuta el pipeline LSDP con el parametro `esquema_oro` y la clave `catalogoOro` esta configurada en dbo.Parametros de Azure SQL, **Entonces** se crea la vista materializada agregada en `{catalogoOro}.{esquema_oro}` con una fila por cliente conteniendo las metricas agregadas de comportamiento ATM y pagos al saldo.
2. **Dado** que un cliente tiene 10 transacciones de tipo "CATM" con montos variados, **Cuando** la vista materializada calcula las metricas de creditos ATM, **Entonces** la cantidad de depositos ATM es 10 y el promedio del monto depositado es la suma de los 10 montos dividida entre 10.
3. **Dado** que un cliente tiene 5 transacciones de tipo "DATM" con montos variados, **Cuando** la vista materializada calcula las metricas de debitos ATM, **Entonces** la cantidad de retiros ATM es 5 y el promedio del monto retirado es la suma de los 5 montos dividida entre 5.
4. **Dado** que un cliente tiene 3 transacciones de tipo "PGSL", **Cuando** la vista materializada calcula el total de pagos al saldo, **Entonces** el total sumarizado es la suma de los montos de las 3 transacciones de tipo "PGSL".
5. **Dado** que un cliente no tiene transacciones de tipo "CATM", "DATM" o "PGSL", **Cuando** la vista materializada calcula las metricas, **Entonces** las cantidades son 0, los promedios son 0 y el total de pagos al saldo es 0.

---

### Historia de Usuario 2 - Vista Materializada de Resumen Integral del Cliente para Consumo (Prioridad: P1)

Como analista del area de negocio de clientes, necesito una vista materializada en la medalla de oro que consolide la informacion mas relevante de cada cliente (datos demograficos clave, clasificaciones de plata y estado financiero actual) junto con las metricas transaccionales agregadas de ATM y pagos al saldo, para tener un unico punto de acceso optimizado que permita comprender el perfil completo del cliente y su comportamiento financiero sin necesidad de hacer joins entre multiples tablas.

**Por que esta prioridad**: Esta vista es el punto de consumo final del producto de datos. Centraliza la informacion del cliente con las metricas agregadas en una sola entidad altamente eficiente, habilitando dashboards y analisis directos sin joins adicionales.

**Prueba Independiente**: Se puede validar ejecutando el pipeline y verificando que: (1) la vista materializada se crea en `{catalogoOro}.{esquema_oro}`, (2) contiene una unica fila por cliente (dimension tipo 1), (3) incluye los campos identificativos y demograficos relevantes del cliente, (4) incluye las clasificaciones calculadas en plata (clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria), (5) incluye los campos financieros clave de saldo, (6) incluye las metricas agregadas de ATM y pagos al saldo, (7) los nombres de columnas estan en espanol y snake_case.

**Escenarios de Aceptacion**:

1. **Dado** que la vista `clientes_saldos_consolidados` de plata contiene la informacion dimensional del cliente y la vista `transacciones_enriquecidas` de plata contiene las transacciones, **Cuando** se ejecuta el pipeline LSDP, **Entonces** se crea la vista materializada de resumen integral en `{catalogoOro}.{esquema_oro}` con una fila por cliente que combina datos dimensionales con metricas transaccionales agregadas.
2. **Dado** que la vista materializada incluye las clasificaciones de plata (clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria), **Cuando** se consulta la vista de oro, **Entonces** estos campos reflejan los valores mas recientes de plata para cada cliente.
3. **Dado** que la vista materializada incluye campos financieros clave (saldo_disponible, saldo_actual, limite_credito, puntaje_crediticio), **Cuando** se consulta la vista, **Entonces** estos campos corresponden a los datos mas recientes del cliente en plata.
4. **Dado** que la vista materializada cruza el cliente con sus transacciones agregadas, **Cuando** un cliente no tiene transacciones registradas, **Entonces** la fila del cliente aparece con los campos de metricas transaccionales en 0 (left join desde la dimension del cliente).

---

### Historia de Usuario 3 - Lectura Dinamica de Parametros de Oro desde Azure SQL y Pipeline (Prioridad: P1)

Como Ingeniero de Datos, necesito que el pipeline lea dinamicamente el catalogo de oro desde la tabla dbo.Parametros de Azure SQL (clave 'catalogoOro') reutilizando la funcion existente de lectura (ya refactorizada en V4 para retornar TODAS las claves sin filtro), y que reciba el esquema de oro como parametro del pipeline LSDP (`esquema_oro`), para que la configuracion sea completamente dinamica y sin valores hardcodeados.

**Por que esta prioridad**: Sin el catalogo leido de Azure SQL y el esquema del pipeline, los scripts de oro no pueden referenciar la ubicacion correcta en Unity Catalog. Esta funcionalidad es la base de configuracion para todas las vistas materializadas de oro.

**Prueba Independiente**: Se puede validar verificando que: (1) el catalogo de oro se lee correctamente desde dbo.Parametros con la clave 'catalogoOro', (2) el esquema de oro se lee correctamente desde los parametros del pipeline, (3) la funcion existente `leer_parametros_azure_sql` (ya refactorizada en V4 para retornar TODAS las claves) se reutiliza sin modificacion, (4) los valores se propagan correctamente a las funciones decoradas via closure.

**Escenarios de Aceptacion**:

1. **Dado** que la tabla dbo.Parametros contiene la clave 'catalogoOro' con el valor "oro_dev" y el pipeline recibe el parametro `esquema_oro` (ej: "regional"), **Cuando** los scripts de transformacion de oro se inicializan, **Entonces** el catalogo ('catalogoOro') se obtiene del diccionario retornado por `leer_parametros_azure_sql` y el esquema se lee via `spark.conf.get("pipelines.parameters.esquema_oro")`, quedando ambos disponibles como variables de modulo capturadas por closure.
2. **Dado** que la funcion `leer_parametros_azure_sql` ya fue refactorizada en V4 para retornar TODAS las claves de dbo.Parametros como diccionario sin filtro, **Cuando** los scripts de oro consultan la clave 'catalogoOro', **Entonces** la funcion NO requiere modificacion adicional; basta con acceder a la clave 'catalogoOro' del diccionario retornado.
3. **Dado** que las vistas materializadas de oro se crean en un catalogo diferente al de bronce y plata, **Cuando** se declaran con el decorador `@dp.materialized_view`, **Entonces** se especifica el catalogo y esquema completo para que se publiquen en `{catalogoOro}.{esquema_oro}`.

---

### Historia de Usuario 4 - Suite de Pruebas TDD para la Medalla de Oro (Prioridad: P2)

Como Ingeniero de Datos, necesito un conjunto de pruebas automatizadas (TDD) que validen el correcto funcionamiento de la medalla de oro del pipeline LSDP, incluyendo las metricas agregadas, la dimension tipo 1, las propiedades Delta, la compatibilidad con Computo Serverless y la correctitud de los calculos de conteo, promedio y sumarizacion, para asegurar la calidad del producto de datos entregado al area de negocio.

**Por que esta prioridad**: Las pruebas TDD son obligatorias segun la constitucion para todas las versiones a partir de la Version 2. Sin embargo, la funcionalidad del pipeline tiene mayor prioridad que las pruebas.

**Prueba Independiente**: Se puede validar ejecutando la suite de pruebas completa y verificando que: (1) todas las pruebas pasan exitosamente, (2) validan la estructura de las vistas materializadas de oro (nombres de columnas en espanol, tipos), (3) validan las propiedades Delta, (4) validan los calculos agregados (conteos, promedios, sumas) con valores esperados, (5) validan el comportamiento de dimension tipo 1, (6) validan que la clave 'catalogoOro' se lee correctamente desde dbo.Parametros, (7) validan que clientes sin transacciones ATM o pagos al saldo reciben valores 0.

**Escenarios de Aceptacion**:

1. **Dado** que se ejecuta la suite de pruebas, **Cuando** el pipeline esta correctamente implementado, **Entonces** el 100% de las pruebas pasan exitosamente.
2. **Dado** que una metrica agregada produce un valor incorrecto (por ejemplo, el conteo de creditos ATM no coincide), **Cuando** se ejecutan las pruebas de metricas, **Entonces** las pruebas fallan indicando exactamente la metrica y el valor esperado versus el obtenido.
3. **Dado** que una propiedad Delta no esta configurada, **Cuando** se ejecutan las pruebas de propiedades, **Entonces** las pruebas fallan indicando la propiedad ausente.

---

### Casos Borde

- Que sucede si un cliente existe en `clientes_saldos_consolidados` de plata pero no tiene ninguna transaccion en `transacciones_enriquecidas`? La vista de resumen integral de oro debe incluir al cliente con todas las metricas transaccionales en 0 (left join desde la dimension del cliente).
- Que sucede si un cliente tiene transacciones pero ninguna de tipo "CATM", "DATM" ni "PGSL"? Las metricas de ATM y pagos al saldo deben ser 0, pero el cliente debe aparecer en la vista de oro.
- Que sucede si el campo `monto_transaccion` es nulo en alguna transaccion de tipo ATM o pago al saldo? El valor NULL se trata como 0 mediante un `coalesce` interno aplicado al campo `monto_transaccion` ANTES de la agregacion (`avg`/`sum`), garantizando que los montos nulos se incluyan como 0 en el calculo del promedio (afectando el denominador) y en la suma. Adicionalmente, un `coalesce` externo cubre el caso de grupo vacio (sin transacciones del tipo) retornando 0.
- Que sucede si las vistas de plata estan vacias (primera ejecucion sin datos)? Las vistas materializadas de oro deben crearse vacias sin errores, listas para recibir datos en la siguiente ejecucion.
- Que sucede si la clave 'catalogoOro' no existe en la tabla dbo.Parametros de Azure SQL? El acceso `parametros_sql["catalogoOro"]` lanza un `KeyError` nativo de Python con el nombre de la clave faltante, lo cual es suficiente como mensaje claro de error. No se requiere manejo de error personalizado adicional.
- Que sucede si el parametro `esquema_oro` no se proporciona al pipeline? La llamada `spark.conf.get("pipelines.parameters.esquema_oro")` lanza un `NoSuchElementException` nativo de Spark con el nombre del parametro faltante, lo cual es suficiente como mensaje claro de error. No se requiere manejo de error personalizado adicional.
- Que sucede si el catalogo o esquema de oro no existe en Unity Catalog? El pipeline debe fallar con un error descriptivo que permita diagnosticar el problema.
- Que sucede si un cliente tiene transacciones solamente de tipo "CATM" pero no de "DATM" ni "PGSL"? La cantidad de debitos ATM y el total de pagos al saldo deben ser 0, mientras que las metricas de creditos ATM deben reflejar los valores correctos.

## Clarifications

### Session 2026-03-31

- Q: Es `comportamiento_atm_cliente` una vista intermedia o de consumo directo? → A: Ambas vistas son de consumo directo; `comportamiento_atm_cliente` para analisis puro ATM y `resumen_integral_cliente` para perfil completo del cliente.
- Q: El script de oro lee `esquema_plata` del parametro del pipeline existente o asume mismo esquema que oro? → A: Lee `esquema_plata` del parametro del pipeline existente, independiente de `esquema_oro`, permitiendo esquemas distintos. El catalogo de oro se obtiene de dbo.Parametros clave 'catalogoOro'.
- Q: Estructura del script de oro: un archivo o dos archivos separados? → A: Un solo archivo `LsdpOroClientes.py` con ambas vistas materializadas, una sola lectura de parametros compartida por closure.
- Q: Lectura de fuentes de plata y dependencia intra-oro: `spark.read.table()` o `dp.read()`? → A: Usar `spark.read.table()` para todas las lecturas (plata y oro intra-modulo), patron consistente con V3/V4.
- Q: Agregar `@dp.expect_or_drop` en oro para `identificador_cliente` nulo o confiar en filtrado de plata? → A: Agregar `@dp.expect_or_drop` en `comportamiento_atm_cliente` para excluir transacciones con `identificador_cliente` nulo (defensa en profundidad).

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: El pipeline LSDP DEBE implementarse usando exclusivamente la biblioteca `from pyspark import pipelines as dp`. Las vistas materializadas de oro DEBEN usar el decorador `@dp.materialized_view`. El uso de `import dlt` esta prohibido.
- **RF-002**: Las vistas materializadas de oro DEBEN crearse en un catalogo y esquema diferentes al de bronce y plata. La configuracion de **destino** (oro) y de **fuentes** (plata) se obtiene asi:
  - **Catalogo de oro (destino)**: Se lee desde la tabla `dbo.Parametros` de Azure SQL usando la clave `catalogoOro` (ej: valor "oro_dev"). Se obtiene reutilizando la funcion existente `leer_parametros_azure_sql` de utilities/ y accediendo a la clave `catalogoOro` del diccionario retornado.
  - **Esquema de oro (destino)**: Se recibe como parametro del pipeline LSDP y se lee via `spark.conf.get("pipelines.parameters.esquema_oro")` (ej: "regional").
  - **Catalogo de plata (fuente)**: Se lee desde la tabla `dbo.Parametros` de Azure SQL usando la clave `catalogoPlata` del mismo diccionario retornado por `leer_parametros_azure_sql`.
  - **Esquema de plata (fuente)**: Se lee del parametro del pipeline existente `esquema_plata` via `spark.conf.get("pipelines.parameters.esquema_plata")`, independiente de `esquema_oro`. Esto permite que plata y oro usen esquemas distintos si fuera necesario.
- **RF-003**: La vista materializada de comportamiento ATM por cliente DEBE ser una vista materializada de LSDP que lea de la vista `transacciones_enriquecidas` de plata mediante `spark.read.table("{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas")` (lectura batch, patron consistente con V3/V4) y agrupe por `identificador_cliente`, calculando las siguientes metricas agregadas:
  - **cantidad_depositos_atm**: Conteo de transacciones donde `tipo_transaccion` == "CATM".
  - **cantidad_retiros_atm**: Conteo de transacciones donde `tipo_transaccion` == "DATM".
  - **promedio_monto_depositos_atm**: Promedio del `monto_transaccion` de las transacciones donde `tipo_transaccion` == "CATM". Si no hay transacciones, el valor debe ser 0.
  - **promedio_monto_retiros_atm**: Promedio del `monto_transaccion` de las transacciones donde `tipo_transaccion` == "DATM". Si no hay transacciones, el valor debe ser 0.
  - **total_pagos_saldo_cliente**: Suma del `monto_transaccion` de las transacciones donde `tipo_transaccion` == "PGSL". Si no hay transacciones, el valor debe ser 0.
  - El calculo DEBE utilizar agregaciones condicionales (sum/count con `when`) para obtener todas las metricas en una sola agrupacion por cliente, maximizando la eficiencia del procesamiento.
- **RF-004**: La vista materializada de resumen integral del cliente DEBE combinar la informacion dimensional del cliente (de `clientes_saldos_consolidados` de plata, leida via `spark.read.table("{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados")`) con las metricas transaccionales agregadas (de la vista de comportamiento ATM de oro, leida via `spark.read.table("{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente")`). Todas las lecturas usan `spark.read.table()` como patron consistente con V3/V4. DEBE incluir como minimo los siguientes campos del cliente:
  - Campos identificativos: `identificador_cliente`, `huella_identificacion_cliente`, `nombre_completo_cliente`, `tipo_documento_identidad`, `numero_documento_identidad`.
  - Campos demograficos: `segmento_cliente`, `categoria_cliente`, `ciudad_residencia`, `pais_residencia`.
  - Campos de clasificacion de plata: `clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria`.
  - Campos financieros: `saldo_disponible`, `saldo_actual`, `limite_credito`, `puntaje_crediticio`, `ingreso_anual_declarado`.
  - Metricas agregadas ATM: `cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente`.
  - El join DEBE ser LEFT JOIN desde `clientes_saldos_consolidados` hacia las metricas transaccionales, garantizando que todos los clientes aparezcan incluso sin transacciones ATM o pagos al saldo. Los campos de metricas transaccionales DEBEN usar coalesce a 0 para clientes sin transacciones.
- **RF-005**: Las vistas materializadas de oro DEBEN comportarse como dimension tipo 1, mostrando siempre la informacion mas reciente. Dado que las fuentes son las vistas materializadas de plata (que ya implementan la logica de dimension tipo 1 para clientes y saldos), las vistas de oro heredan este comportamiento al leer de plata.
- **RF-006**: Todas las vistas materializadas de oro DEBEN tener configuradas las siguientes propiedades Delta:
  - Change Data Feed: `"delta.enableChangeDataFeed": "true"`
  - autoOptimize.autoCompact: `"delta.autoOptimize.autoCompact": "true"`
  - autoOptimize.optimizeWrite: `"delta.autoOptimize.optimizeWrite": "true"`
  - Liquid Cluster: via parametro `cluster_by` en el decorador
  - deletedFileRetentionDuration: `"delta.deletedFileRetentionDuration": "interval 30 days"`
  - logRetentionDuration: `"delta.logRetentionDuration": "interval 60 days"`
- **RF-007**: Todas las vistas materializadas de oro DEBEN usar la funcion existente `reordenar_columnas_liquid_cluster` de utilities/ para garantizar que las columnas del Liquid Cluster queden dentro de las primeras 32 columnas del DataFrame, manteniendo las estadisticas min/max necesarias para el clustering.
- **RF-008**: Los nombres de las vistas materializadas y sus campos/columnas en oro DEBEN estar en espanol, en formato snake_case, con nombres intuitivos y claros, conformados por dos palabras o silabas como minimo. Los nombres de campos NO pueden ser los mismos que los usados en bronce (nombres AS400).
- **RF-009**: Los scripts de oro DEBEN reutilizar las funciones de utilidad existentes en la carpeta utilities/ (LsdpConexionAzureSql, LsdpReordenarColumnasLiquidCluster). NO se requiere crear nuevas funciones de utilidad para la medalla de oro, a menos que se identifique logica transversal reutilizable. Si se crean nuevas funciones, DEBEN ubicarse en utilities/ y NO DEBEN usar decoradores de pipelines de Spark.
- **RF-010**: El pipeline DEBE ser 100% compatible con Computo Serverless de Databricks. Esta PROHIBIDO el uso de `spark.sparkContext` y cualquier acceso directo al JVM del driver.
- **RF-011**: Todo el codigo DEBE estar en espanol, con variables, funciones y objetos en formato snake_case en minuscula. Los archivos .py DEBEN tener nombre en formato PascalCase con prefijo "Lsdp" (para scripts del pipeline en transformations/) y "Nb" (para notebooks de prueba en tests/). Cada celda DEBE tener un bloque markdown explicativo y el codigo DEBE estar completamente comentado al detalle.
- **RF-012**: La estructura de archivos DEBE respetar la organizacion de Lakeflow Spark Declarative Pipelines: carpeta transformations/ para los scripts que definen las vistas materializadas de oro. Ambas vistas materializadas de oro (`comportamiento_atm_cliente` y `resumen_integral_cliente`) DEBEN estar en un unico archivo `LsdpOroClientes.py` en la carpeta `transformations/`, compartiendo una sola lectura de parametros a nivel de modulo (closure). Esto simplifica la inicializacion y permite a LSDP resolver la dependencia entre vistas dentro del mismo modulo.
- **RF-013**: La Version 5 DEBE contar con una suite de pruebas TDD ubicada en `tests/LSDP_Laboratorio_Basico/` que valide: la estructura de las vistas materializadas de oro (nombres de columnas en espanol, tipos), las metricas agregadas (conteos, promedios, sumas con valores esperados), las propiedades Delta, el comportamiento de dimension tipo 1, los casos borde (clientes sin transacciones ATM o pagos al saldo), y que la clave 'catalogoOro' se lee correctamente desde dbo.Parametros.
- **RF-014**: El pipeline LSDP DEBE permitir que las vistas materializadas de oro se publiquen en un catalogo y esquema diferente al elegido por defecto ("bronce_dev" / "regional"). Para ello, se DEBE usar la sintaxis de nombre completo en el decorador: `@dp.materialized_view(name="{catalogoOro}.{esquema_oro}.nombre_vista", comment="...", ...)`, donde `catalogoOro` proviene del diccionario de Azure SQL y `esquema_oro` del parametro del pipeline, ambos capturados como variables de modulo por closure.
- **RF-015**: Cada vista materializada de oro DEBE incluir un parametro `comment` obligatorio en espanol dentro del decorador `@dp.materialized_view`. El comment DEBE describir de forma clara y concisa el proposito de la vista, sus fuentes de datos y su funcion dentro del modelo de oro. Este comment se muestra en Unity Catalog como documentacion de la tabla, mejorando la discoverability y la gobernanza de datos.
- **RF-016**: Los campos de metricas transaccionales agregadas DEBEN manejar valores nulos de forma segura, usando `coalesce` para garantizar que los campos nunca tengan valores nulos en la vista final de oro. Para los conteos, el valor por defecto debe ser 0. Para los promedios, el valor por defecto debe ser 0. Para las sumas, el valor por defecto debe ser 0.
- **RF-017**: La vista materializada de comportamiento ATM por cliente NO DEBE aplicar filtros previos a la agrupacion que excluyan tipos de transaccion. DEBE leer TODAS las transacciones de plata y usar agregaciones condicionales (`sum(when(...))`, `count(when(...))`) para filtrar por tipo durante la agregacion, aprovechando al maximo las capacidades de LSDP y la eficiencia de una sola pasada sobre los datos.
- **RF-018**: La vista materializada `comportamiento_atm_cliente` DEBE incluir una validacion de calidad de datos usando `@dp.expect_or_drop` para registros con `identificador_cliente` nulo, eliminandolos silenciosamente antes de la agregacion (defensa en profundidad). Aunque la capa de plata ya filtra nulos en `clientes_saldos_consolidados`, la vista transaccional de plata (`transacciones_enriquecidas`) no aplica esta expectation, por lo que transacciones con cliente nulo podrian llegar a oro. Las metricas de registros eliminados quedan registradas automaticamente por LSDP para monitoreo.

### Entidades Clave

- **Vista Materializada `{catalogoOro}.{esquema_oro}.comportamiento_atm_cliente`**: Vista materializada de **consumo directo** que contiene las metricas agregadas de comportamiento ATM y pagos al saldo por cliente. Permite al area de negocio analizar exclusivamente la actividad ATM sin sobrecarga dimensional. Incluye `@dp.expect_or_drop` para `identificador_cliente` nulo (defensa en profundidad). Campos: `identificador_cliente`, `cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente`. Liquid Cluster por `identificador_cliente`. Propiedades Delta completas (incluido Change Data Feed).
- **Vista Materializada `{catalogoOro}.{esquema_oro}.resumen_integral_cliente`**: Vista materializada de **consumo directo** que combina datos dimensionales del cliente (de plata) con metricas transaccionales agregadas (de la vista de comportamiento ATM de oro). Contiene campos identificativos, demograficos, clasificaciones de plata, financieros y metricas ATM y pagos al saldo. Liquid Cluster por `huella_identificacion_cliente` e `identificador_cliente`. Propiedades Delta completas (incluido Change Data Feed).
- **Fuentes de Plata**: `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` (175 columnas, dimension tipo 1) y `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas` (65 columnas) — creadas en la Version 4.
- **Tabla dbo.Parametros (Azure SQL) — clave existente para V5**: La tabla ya contiene la clave `catalogoOro` (valor "oro_dev") ademas de las claves existentes de bronce y plata (catalogoBronce, contenedorBronce, datalake, DirectorioBronce, catalogoPlata). No se requiere INSERT — solo se lee via `leer_parametros_azure_sql()`.
- **Parametros del Pipeline (nuevos para V5)**: `esquema_oro` (ej: "regional") como parametro del pipeline LSDP, ademas de los existentes (`nombreScopeSecret`, `esquema_plata`). El catalogo de oro NO es parametro del pipeline; se lee de Azure SQL (clave `catalogoOro`). Los scripts de oro leen tanto `esquema_plata` (para referenciar fuentes) como `esquema_oro` (para el destino) como parametros independientes del pipeline.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: Las 2 vistas materializadas de oro se crean exitosamente en el catalogo leido desde dbo.Parametros (clave 'catalogoOro') y el esquema recibido como parametro del pipeline al ejecutar el pipeline LSDP.
- **CE-002**: La vista `comportamiento_atm_cliente` contiene exactamente una fila por cliente con las 5 metricas agregadas correctamente calculadas (2 conteos, 2 promedios, 1 suma).
- **CE-003**: La vista `resumen_integral_cliente` contiene exactamente una fila por cliente con los datos dimensionales, las clasificaciones de plata y las metricas transaccionales en una sola entidad de consumo.
- **CE-004**: Para cualquier cliente de prueba, los conteos de depositos ATM y retiros ATM coinciden exactamente con el numero de transacciones de tipo "CATM" y "DATM" respectivamente en plata.
- **CE-005**: Para cualquier cliente de prueba, el promedio de montos depositados ATM y retirados ATM se calcula correctamente como la media aritmetica de los montos correspondientes.
- **CE-006**: Para cualquier cliente de prueba, el total de pagos al saldo es la suma exacta de los montos de transacciones de tipo "PGSL".
- **CE-007**: Los clientes sin transacciones ATM o pagos al saldo aparecen en ambas vistas de oro con metricas en 0, sin filas faltantes.
- **CE-008**: Todos los nombres de columnas en las vistas de oro estan en espanol, formato snake_case, son intuitivos y diferentes a los nombres AS400 de bronce.
- **CE-009**: Todas las vistas materializadas de oro tienen configuradas las 6 propiedades Delta requeridas (Change Data Feed, autoCompact, optimizeWrite, Liquid Cluster, retenciones de 30 y 60 dias).
- **CE-010**: El pipeline completo (bronce + plata + oro) se ejecuta exitosamente en Computo Serverless de Databricks sin errores de compatibilidad.
- **CE-011**: La suite de pruebas TDD cubre cada uno de los requerimientos funcionales (RF-001 a RF-018) con al menos una prueba asociada.
- **CE-012**: Cero valores hardcodeados en los scripts de oro. El catalogo proviene de dbo.Parametros (Azure SQL) y el esquema del parametro del pipeline. *(Validado directamente por la ejecucion exitosa del Pipeline LSDP — no se prueba desde el notebook TDD porque requiere el contexto del pipeline.)*
- **CE-013**: La funcion `leer_parametros_azure_sql` se reutiliza sin modificacion desde la Version 4. *(Validado directamente por la ejecucion exitosa del Pipeline LSDP — no se prueba desde el notebook TDD porque requiere el contexto del pipeline.)*

## Supuestos

- Las vistas materializadas de plata (`{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados` y `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas`) estan creadas y contienen datos de al menos una ejecucion de la Version 4.
- El catalogo `oro_dev` y el esquema `regional` existen en Unity Catalog y tienen los permisos necesarios para crear vistas materializadas.
- La tabla dbo.Parametros de Azure SQL contiene la clave `catalogoOro` con el valor que corresponde al catalogo de oro en Unity Catalog (ej: "oro_dev"). Esta clave coexiste con las claves existentes de bronce y plata.
- Las funciones de utilidad existentes en utilities/ (LsdpConexionAzureSql, LsdpReordenarColumnasLiquidCluster) estan probadas y funcionales desde las Versiones 3 y 4. La funcion `leer_parametros_azure_sql` ya fue refactorizada en V4 para retornar TODAS las claves como diccionario sin filtro, por lo que no requiere cambios adicionales.
- El Scope Secret de Databricks y la configuracion de Azure SQL siguen operativos como en las Versiones 3 y 4.
- El pipeline LSDP existente de las Versiones 3 y 4 se extiende con los nuevos scripts de oro en el mismo proyecto; no se crea un pipeline separado.
- Las decisiones aprobadas en las Versiones 1-4 siguen vigentes como fuente de verdad.
- El decorador `@dp.materialized_view` acepta los mismos parametros que en V4, incluyendo `name`, `table_properties`, `cluster_by` y `comment`.
- Los catalogos de plata y oro son diferentes (plata_dev y oro_dev), por lo que los scripts de oro necesitan leer tanto `catalogoPlata` (para las fuentes) como `catalogoOro` (para el destino) del diccionario de parametros de Azure SQL.
- El esquema de plata (para referenciar las fuentes) se lee del parametro del pipeline `esquema_plata` (ya existente en V4), de forma independiente al parametro `esquema_oro`. Esto permite que plata y oro operen con esquemas distintos si fuera necesario en el futuro.
