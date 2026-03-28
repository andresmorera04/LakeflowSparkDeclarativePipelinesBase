# Propósito de archivo SYSTEM.md
El Objetivo de este archivo SYSTEM.md es centralizar todo el Spec-Driven Development (SDD) de la solución en un solo archivo y que este sirva de base para alimentar la base de verdad que genera spec-kit a través de los comandos inciales de /speckit.constitution, /speckit.specify y /speckit.plan, facilitando la implementación de la solución y agilizando de cara al Ingeniero de Datos el SDD

# Dinámica con spec-kit
Speckit debe de seguir la siguiente dinámica de forma estricta:
- Cuando se ejecute el comando /speckit.constitution debe tomar lo especificado en la sección [Constitution](#constitution) de manera exclusiva, de las demás secciones de este archivo solo debe tomar lo mínimo necesario.
- Cuando se ejecute el comando /speckit.specify debe tomar principalmente lo especificado en la sección [Specify](#specify) de manera exclusiva, de las demás secciones de este archivo solo debe tomar lo mínimo necesario.
- Cuando se ejecute el comando /speckit.plan debe tomar principalmente lo especificado en la sección [Plan](#plan) de manera exclusiva, de las demás secciones de este archivo solo debe tomar lo mínimo necesario.

# Constitution
A continuación se definen las políticas que debe seguir el desarrollo de esta solución:
- El Código No debe ser Hardcodeado (No Hard-Coded), en todo momento se debe manejar dinamismo en el código por medio de parámetros claramente establecido, el uso de valores constantes hardcodeadas no deben darse bajo ningún escenario, no hay excepción que valga
- Como Requisito de este proyecto, las extensiones Databricks extension for Visual Studio Code y Databricks Driver for SQLTools deben estar correctamente instaladas.
- La configuración de la extensión de Visual Studio Code llamada Databricks extension for Visual Studio Code debe estar correctamente hecha, de tal manera que sea posible acceder al workspaces de databricks y enlistar y usar los Computos disponibles
- Toda la solución debe de tener pruebas (TDD) correctamente definidas y ejecutables aprovechando las extensiones de Databricks del Visual Studio Code previamente mencionadas en los puntos anteriores.
- Se deben generar archivos parquets simulando el landing zone del datalake, estos scripts de python estarán excluidos del lakeflow spark declarative pipelines, es decir se ejecutan de forma asilada como notebooks separados, estos 3 archivos deben de tener estas caracteristicas:
  - Uno que simula el Maestro de Clientes de AS400 para una entidad bancaria, incluyendo los nombres de campos lo más similares a los de AS400 y la cantidad de campos debe ser la misma que tienen las tablas originales de AS400.
  - Uno que simula el Transaccional de AS400 para una entidad bancaria, el cual se relaciona con el Maestro de Clientes de AS400 a través de la llave que representa el identificado de Cliente. Los nombres de los campos debe ser lo más similares a los de AS400 y la cantidad de campos debe ser la misma que tienen las tablas originales de AS400. Es Sumamente Importante que este tenga un campo que determine el tipo de transacción (por ejemplo: Creditos por ATM, Debitos por ATM, Transaferencias Externas (hacia otras entidades bancarias), Transferencias Internas (Entre clientes de la misma entidad bancaria), Pagos al Saldo del cliente, Adelanto Salarial, entre otros (se necesita hacer un research)).
  - El Ultimo parquet simula los Saldos de los Clientes de AS400 para una entidad bancaria, el cual se relaciona con el Maestro de Clientes de AS400 a través de la llave que representa el identificado de Cliente. Los nombres de los campos debe ser lo más similares a los de AS400 y la cantidad de campos debe ser la misma que tienen las tablas originales de AS400.
  - Para el Maestro de Cliente de AS400, la primera vez que se ejecute el proceso que genera el parquet debe generar 5 millones de registros, posteriormente cada vez que se ejecute el mismo proceso deben generar los mismos 5 millones de clientes (mismo identificador de Cliente) y con algunos datos cambiados en algunos campos, además, la cantidad de registros aumentar un 0.60% agregando nuevos clientes.
  - Para el Transaccional cada vez que se ejecute en proceso debe generar 15 millones de registros nuevos, un mismo cliente puede tener 0 transacciones o varias transacciones registradas. La Fecha de la Transacción debe brindarse vía parámetro y aplica igual para todos los registros simulados, además, solo se suminitra la fecha de esta manera: año-mes-dia (por ejemplo: 2026-01-01 ó 2026-02-15), las horas minutos y segundos se deben simular.
  - Para los Saldos, se deben generar una línea con datos de saldo por cliente, manteniendo una relacion 1 a 1, cada vez que se ejecute el proceso debe simular nuvemante y por completo los saldos y debe garantizar que todos los clientes del maestro de clientes tenga un saldo.
  - Para la información del Maestro de Cliente, los nombres y apellidos deben ser Hebreos, Egipcios e Ingleses, no deben usarse bajo ninguna circusntancia nombres y apellidos latinos (haz un research para asegurar el cumplimiento de esta regla).
- La Solución va a usar Lakeflow Spark Declarative Pipelines, por lo que este proyecto debe asegurarse de tener una compatibilidad completa.
- La Solución NO debe hacer uso de Delta Live Table, debe usar de forma estricta Lakeflow Spark Declarative Pipelines.
- Los Scripts Programados de Lakeflow Spark Declarative Pipelines serán codificados en el lenguaje de programación pyspark/python.
- Los Scripts Programados de Lakeflow Spark Declarative Pipelines serán ejecutados por Computo Serverless de forma estricta, por lo que deben ser totalmente compatibles.
- El Manejo de los Branch de git y github será controlado por el usuario, bajo ninguna circusntancia el sepeckit podrá crear nuevos branchs o hacer commits automáticos, toda la gestión del repositorio y la estrategia de branchs recae en el usuario. Speckit solo creará directorios convencionales
- LA IA no tomará ninguna decisión, por cada research que se haga deberá exigir la decisión del usuario. La IA plantea una propuesta de cual es la mejor decisión que considera, pero solo el usuario toma la decisión final. Este punto aplica en todo momento pero principalmente al ejecutar el comando /speckit.plan, ya que en esta etapa se dan lo research y no se puede continuar con el resto del proceso del plan sin que el usuario haya tomados las decisiones pertinentes resultantes del research.
- La IA No debe alucinar, debe mantenerse bajo el alcance estipulado por cada punto. Si puede hacer recomendaciones de mejora, sin embargo, solamente las puede aplicar en el proyecto bajo la autorización del usuario.
- Al Iniciar una nueva versión o incremento del proyecto, siempre debe hacer un research de Lakeflow Spark Declarative Pipelines, de los parquets simuladores de datos de AS400 y de las extensiones de databricks para visual studio code que permitan ejecutar las pruebas necesarias. Si el research encuentra nuevos hallazgos o opciones que previamente no se han decidido, el usuario debe aprobarlas o rechazarlas.
- En el procesamiento y transformación de los datos a través de Lakeflow Spark Declarative Pipelines, se le debe dar el máximo provecho a la arquitectura medallón: Bronce, Plata y Oro. Plata debe contemplar enriquecimiento de datos y calculos a nivel row by row que se generan con el uso de dos o más campos que tienen las tablas deltas de bronce, por otro lado, Oro siempre debe de ser una versión altamente eficiente para consumo y con datos agregados. Adicionalmente la estrategia de acumulación de datos seguirá este patrón:
  - Bronce llevará marca de tiempo (Campo FechaIngestaDatos) e irá acumulando datos de forma historia, aprovechando al máximo el AutoLoader.
  - En Plata se manejarán vistas materializadas y hay dos forma de manejar los datos:
    - Para el Maestro de Cliente y los Saldos del cliente se deben consolidar en una sola vista materializada y debe comportarse como una dimensión tipo 1 (Siempre manteniendo los datos más recientes), el procesamiento debe cumplir con los más altos niveles de optimización asegurando eficiencia y eficacia del proceso.
    - Para el Transaccional se manejará como una vista materializada separada la cual debe asegurarse que cada vez que se ejeucte se haga de forma incremental agregando solo los registros nuevos.
  - En Oro se deben manejar vista materializadas que siempre muestren la información más reciente (Como una dimensión Tipo 1).
- En todo momento se debe respetar por completo la estructura de archivos y carpetas que usa lakeflow spark declarative pipelines, por ejemplo la carpeta utilities, transformations, explorations, etc.
- Cualquier sugerencia que haga la IA para mejorar el Spec-Driven Development de la verdad del proyecto, es decir, los archivos de constitution.md, spec.md y plan.md asi como los demás creados en las etapas del spec-kit, en especial, las validaciones, se deben aplicar de forma general y asegurando consistencia en los archivos. La IA Aplica los cambios siempre y cuando el usuario apruebe los cambios.
- Para los Research se deben usar los Links Oficiales de Azure Databricks, python y spark, además, para ejemplos debe basarse en primera instancia en los Links Oficiales y como segunda opción en links de Medium y stackoverflow.
- Si el spec-driven development está poco conciso o tiene oportunidad de mejora porque le falta especificar más, la IA debe informar inmediatamente, a su vez, puede hacer sugerencias que permitan detallar más y generar un SDD más completo, claro y sin ambiguedad.
- Todas las respuestas de la IA deben estar en idioma español, incluyendo las respuestas de los slash commands que se usen, incluyendo los de spec-kit.


# Specify
## Objetivo
El área de negocio de clientes necesita un producto de datos que les permitan analizar el comportamiento de los clientes con respecto a sus saldos y al uso de los cajeros automáticos (ATM), por lo que el conocer la cantidad de depositos (creditos) y retiros (debitos), así como el promedio de los montos retirados y depositados es clave tenerlo por cada cliente y, por ultimo, el total sumarizado de los Pagos al Saldo del cliente. El áre de negocio de clientes necesita ver este comportamiento y comprender si de pronto el movimiento de retiro o deposito de los ATMs está asociado con los Pagos al Saldo de cada cliente o si son comportamiento aislados y sin correlación.

## Versiones del Proyecto
- **Version 1**: Hacer el Research inicial de todo lo necvesario del proyecto y tomar las decisiones claves para establecer un constitution robusto, un specify robusto y un plan robusto. Para esta versión no se debe generar ningún código.
- **Version 2**: Crear los notebooks (.py) en lenguaje python que generarán los 3 parquets simulando la data de AS400 definida en las políticas del constitution.
- **Version 3**: Crear el Lakeflow Spark Declarative Pipelines con solo el procesamiento de la medalla de bronce.
- **Version 4**: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y la transformación de la medalla de plata a través de vista materializadas (también se pueden usar tablas deltas temporales si fuera necesario).
- **Version 5**: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y la transformación de la medalla de oro a través de vista materializadas (también se pueden usar tablas deltas temporales si fuera necesario).
- **Nota Importante**: Excluyendo la versión 1, todas las demás versiones deben de tener su set de pruebas basado en Test-Driven Development (TDD)

## Reglas de Negocio
- En la Medalla de Plata, la tabla consolidada de clientes y saldos debe tener como mino 3 campos calulados, cada campo calculado debe basarse en lo equivalente a un CASE de SQL pero en pyspark, donde, deben intervenir como mínimo 3 campos/columnas de las tablas delta de la medalla de bronce.
- En la Medalla de Plata, en la tabla transaccional, deben crearse al menos 4 campos calculados, cada uno de ellos deben basarse en minimo 2 o más campos/columnas de tipo numérica para calcular un nuevo valor numérico.
- En la Medalla de Plata, la tabla consolidada de clientes y saldos se debe crear un campo calculado a partir del indicador de cliente único, este campo calculado debe ser generado a través del algortimo SHA2_256.

# Plan

## Repositorio del Proyecto
El Proyecto tendrá la siguiente estructura:

|- README.md
|- SYSTEM.md
|- <Archivos Markdown Generados por Speckit en sus directorios o en la raíz>
|- scripts/
    |- GenerarParquets/
        |- generarMaestroCliente.py
        |- generarTransaccionalCliente.py
        |- generarSaldosCliente.py
|- src/
    |- README.md
    |- LSDP_Laboratorio_Basico/
        |- <Directorios y archivos .py del Lakeflow Spark Declarative Pipelines necesarios para dar solución al proyecto>
|- docs/
    |- ManualTecnico.md
|- tests/
    |- GenerarParquets/
        |- <Archivos del TDD para probar la generación de archivos parquets>
    |- LSDP_Laboratorio_Basico/
        |- <Archivos del TDD para probar el Lakeflow Spark Declarative Pipelines>

## Stack Tecnológico
- Azure Databricks con Unity Catalog Habilitado.
- Azure Databricks Tier Premiun.
- Azure Databricks con External Location conectados a contenedores de un Azure Storage Account de tipo Data Lake Storage Gen2.
- Una Base de Datos Azure SQL con tablas que contienen parametros que permiten dinamismo al código. Esta tabla debe ser consumida desde scripts .py en la carpeta utilities de Lakeflow Spark Declarative Pipelines.
- Un Scope Secret ya configurado en Databricks asociado a un Azure Key Vault. entre los secretos está el que permite la conexión a la base de datos.
- Computo Serverless con la Versión de Databricks Runtime oficial definida para los computos Serverless
- Cuatro Catalogos: bronce_dev, plata_dev, oro_dev y control_dev
    - bronce_dev para todas las tablas deltas de la medalla de bronce
    - plata_dev para todas las tablas deltas de la medalla de plata
    - oro_dev para todas las tablas deltas de la medalla de oro
    - control_dev para tablas delta de control, bitacoras, configuraciones, etc.


## Estándar de Desarrollo
A continuación se detalla los estándares y reglas de desarrollo:
- En la medalla de plata y oro, los nombres de las tablas deltas y vistas metarializadas deben ser en español, en formato snake_case y con nombres intuitivo, además, deben estar comentada correctamente.
- En la medalla de plata y oro, los nombres de los campos/columnas de las tablas deltas y vistas metarializadas deben ser en español, en formato snake_case y con nombres intuitivos, claros, faciles de entender y conformados por dos palabras o silabas como minimo, además, deben estar comentada correctamente. No puede ocurrir que los nombres de los campos/columnas sean los mismos que usan en bronce
- El lenguaje de programación será python aprovechando spark a través de pyspark de forma estricta y absoluta.
- los notebooks serán archivos .py pero con el formato de notebooks compatibles con Azure Databricks. Todo el código debe estar en idioma español y todo comentado al detalle, además, el nombre de los archivos .py deben estar en forma PascalCase y usar "Nb" como prefijo del nombre del archivo.
- los scrtips o archivos .py de lakeflow spark declarative pipelines deben tener todo el codigo en español y todo comentado al detalle, además, el nombre de los archivos .py deben seguir el formato PascalCase y como prefijo deben usar la palabra "Lsdp".
- Todo el código en python debe estar en español, además, debe seguir el formato snake_case con todo en minuscula. Las variables, constantes, clases, funciones, metodos y objetos deben estar estrictamente en formato snake_case en minuscula.
- Los bloques de markdown debe seguir un estilo explicativo y profesional.
- en el Lakeflow Spark Declarative Pipeline, debe crear scripts en la carpeta utilities que faciliten métodos y funciones que sigan el principio SOLID de responsabilidad única y que sean reutilizables para los demás scripts de la carpeta de transformations.
- El estilo de desarrollo debe ser modular. En el directorio utilities todas las funciones o métodos que sean transversales y que no usen las bibliotecas de spark declarative pipelines. Dentros de los scripts de la carpeta transformations, también se debe modular en funciones que sean necesarias paa que el Lakeflow Spark Declarative Pipelines funciones correctamente y óptimamente.
- En todo momento se debe realizar código altamente optimizado, incluso, se deben hacer pruebas en el TDD que aseguren optimización y en el caso que las pruebas no den resultados optimos o eficientes, se deben de mejorar el codigo.
- El código generado en el Lakeflow Spark Declarative Pipeline deben ser totalmente compatible y debe de aplicar estrictamente la filosofía de este paradigma de creación de ETLs y Pipelines de Databricks.
- Para la conexión con la base de datos Azure SQL se usan dos secretos: "sr-jdbc-asql-asqlmetadatos-adminpd" este secreto tiene la cadena de conexión JDBC a excepción de la palabra "password=" y del valor de la contraseña, por otro lado el secreto "sr-asql-asqlmetadatos-adminpd" tiene estrictamente la contraseña del servidor, por lo que la conexión se crea combinando ambos secretos. Por ultimo, la conexión se hace por medio de spark.read.format("jdbc").
- El mismo Pipeline de Lakeflow Spark Declarative Pipelines debe permitir crear tablas deltas o vistas en esquemas y catalogos diferentes al elegido por defecto. El Catalogo y esquema elegido por defecto para el pipeline de tipo Lakeflow Spark Declarative Pipeline es "bronce_dev" y "regional".
- El Desarrollo del Pipeline de Lakeflow Spark Declarative Pipeline debe aprovechar al máximo las capacidades de este paradigma de desarrollo, por ejemplo: para crear la tabla o vista materializada transaccional del plata no debe aplicar filtros en la lectura de la tabla de bronce.
- Las Tablas deltas y vistas materializadas deben tener activas estas propiedades:
    - Change Data Feed
    - autoOptimize.autoCompact
    - autoOptimize.optimizeWrite
    - Liquid Cluster (La IA debe definir los campos que lo conforman basados en las recomendaciones y buenas prácticas)
    - delta.deletedFileRetentionDuration en intervalo de 30 días
    - delta.logRetentionDuration en intervalo de 60 días
- si se usan tablas delta de tipo apply_change deben ser declaradas como tablas deltas temporales y también deben tener liquid cluster

## Que NO debe hacer el proyecto
- No debe usar emojis bajo ninguna circunstancia.
- No debe hacer código espaguetti.
- No puede existir código que no sea compatible o ejeutable en el Lakeflow Spark Declarative Pipelines.
- Las tablas deltas o vista materializadas No deben usar ZOrder ni PartitionBy

