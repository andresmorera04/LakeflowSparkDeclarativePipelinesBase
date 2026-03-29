# Especificacion del Feature: Generacion de Parquets Simulando Data AS400

**Feature Branch**: `002-generacion-parquets-as400`
**Creado**: 2026-03-28
**Estado**: Completa
**Input**: Crear notebooks Python (.py) para generar 3 archivos parquet simulando la data de AS400 (Maestro de Clientes, Transaccional, Saldos) segun las politicas del constitution y las decisiones aprobadas en el research de la Version 1.

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Generacion del Parquet de Maestro de Clientes (Prioridad: P1)

Como Ingeniero de Datos, necesito ejecutar un notebook Python que genere un archivo parquet simulando el Maestro de Clientes de AS400 con 70 campos aprobados (42 textuales, 18 fechas, 10 numericos), donde la primera ejecucion genere 5 millones de registros y cada ejecucion subsecuente mantenga los mismos 5 millones de clientes (mismo CUSTID) con algunos datos modificados y agregue un 0.60% de clientes nuevos, para alimentar la capa de Bronce del pipeline.

**Por que esta prioridad**: El Maestro de Clientes es la entidad central del modelo de datos. Sin este parquet no es posible generar los parquets de Transaccional ni de Saldos, ya que ambos dependen del identificador de cliente (CUSTID).

**Prueba Independiente**: Se puede validar ejecutando el notebook y verificando: (1) el archivo parquet se genera correctamente, (2) contiene exactamente 70 columnas con los nombres AS400 aprobados en R2-D1, (3) la primera ejecucion produce 5,000,000 de registros, (4) una segunda ejecucion produce los mismos 5,000,000 clientes con datos modificados mas 30,000 clientes nuevos (0.60%), (5) los nombres y apellidos son exclusivamente hebreos, egipcios e ingleses (catalogos aprobados en R3-D1), (6) no existen valores constantes hardcodeados en el codigo.

**Escenarios de Aceptacion**:

1. **Dado** que es la primera vez que se ejecuta el notebook, **Cuando** se ejecuta el proceso de generacion, **Entonces** se crea un archivo parquet con exactamente 5,000,000 de registros y 70 columnas con nombres estilo AS400 aprobados.
2. **Dado** que ya existe un parquet previo con 5,000,000 de clientes, **Cuando** se ejecuta nuevamente el notebook, **Entonces** se genera un nuevo parquet con los mismos 5,000,000 de clientes (conservando CUSTID) donde el 20% de los registros existentes tienen mutaciones en 15 campos demograficos/actualizables (ADDR1, PHONE1, EMAIL, MRTLST, EMPLYR, SGMNT, RISKLV, etc.), mas 30,000 clientes nuevos (0.60% de incremento).
3. **Dado** que se generan nombres para los clientes, **Cuando** se verifican los 9 campos de nombres de RF-005 (FRSTNM, MDLNM, LSTNM, SCNDLN, CUSTNM, MTHNM, FTHNM, CNTPRS, PREFNM), **Entonces** todos los nombres y apellidos pertenecen exclusivamente a los catalogos hebreos, egipcios e ingleses aprobados en R3-D1.
4. **Dado** que el notebook se ejecuta, **Cuando** se revisan los parametros configurables, **Entonces** la ruta de salida del parquet, la cantidad base de registros y el porcentaje de incremento son parametrizables (no hardcodeados).

---

### Historia de Usuario 2 - Generacion del Parquet Transaccional (Prioridad: P1)

Como Ingeniero de Datos, necesito ejecutar un notebook Python que genere un archivo parquet simulando el Transaccional de AS400 con 60 campos aprobados (30 numericos, 21 fechas, 9 textuales), generando 15 millones de registros nuevos por cada ejecucion, donde cada transaccion se asocia a un cliente existente del Maestro de Clientes mediante CUSTID y el tipo de transaccion proviene del catalogo de 15 tipos aprobados, para alimentar la capa de Bronce del pipeline.

**Por que esta prioridad**: El Transaccional es el insumo directo para el analisis central del negocio (uso de ATM, Pagos al Saldo). Sin este parquet, no se puede construir la medalla de Oro con las metricas requeridas.

**Prueba Independiente**: Se puede validar ejecutando el notebook y verificando: (1) el archivo parquet se genera correctamente con 15,000,000 de registros, (2) contiene exactamente 60 columnas con los nombres AS400 aprobados en R2-D2, (3) cada CUSTID referencia un cliente existente en el Maestro de Clientes, (4) el campo TRXTYP contiene exclusivamente codigos del catalogo de 15 tipos aprobados en R2-D3, (5) la fecha de transaccion (TRXDT) se recibe via parametro en formato anio-mes-dia, (6) las horas, minutos y segundos del campo TRXTM se simulan aleatoriamente, (7) un mismo cliente puede tener 0 o varias transacciones.

**Escenarios de Aceptacion**:

1. **Dado** que existe un Maestro de Clientes generado previamente, **Cuando** se ejecuta el notebook con el parametro de fecha "2026-01-15", **Entonces** se genera un parquet con 15,000,000 de registros donde TRXDT es "2026-01-15" para todos los registros y TRXTM tiene horas, minutos y segundos simulados.
2. **Dado** que se ejecuta el notebook, **Cuando** se verifica la distribucion de CUSTID en las transacciones, **Entonces** todos los CUSTID referenciados existen en el Maestro de Clientes y la distribucion permite que un cliente tenga 0 o multiples transacciones.
3. **Dado** que se ejecuta el notebook, **Cuando** se verifica el campo TRXTYP, **Entonces** todos los codigos pertenecen al catalogo de 15 tipos aprobados (CATM, DATM, TEXT, TINT, PGSL, ADSL, PGSV, CMPR, DPST, RTRO, DMCL, INTR, CMSN, NMNA, IMPT).
4. **Dado** que se ejecuta el notebook dos veces con diferentes fechas, **Cuando** se comparan los archivos generados, **Entonces** cada ejecucion produce 15,000,000 de registros completamente nuevos (TRXID unicos), no acumulativos.

---

### Historia de Usuario 3 - Generacion del Parquet de Saldos (Prioridad: P1)

Como Ingeniero de Datos, necesito ejecutar un notebook Python que genere un archivo parquet simulando los Saldos de Clientes de AS400 con 100 campos aprobados (30 textuales, 35 numericos, 35 fechas), generando exactamente un registro por cada cliente del Maestro de Clientes (relacion 1:1), y regenerando completamente los saldos en cada ejecucion, para alimentar la capa de Bronce del pipeline.

**Por que esta prioridad**: Los Saldos se consolidan con el Maestro de Clientes en Plata y alimentan la vista de Oro. Sin este parquet, la dimension cliente queda incompleta.

**Prueba Independiente**: Se puede validar ejecutando el notebook y verificando: (1) el archivo parquet contiene exactamente la misma cantidad de registros que el Maestro de Clientes, (2) contiene exactamente 100 columnas con los nombres AS400 aprobados en R2-D4, (3) cada CUSTID del Maestro de Clientes tiene exactamente un registro de saldo (relacion 1:1), (4) cada ejecucion regenera por completo los datos de saldos, (5) no existen CUSTID huerfanos ni CUSTID faltantes.

**Escenarios de Aceptacion**:

1. **Dado** que existe un Maestro de Clientes con N registros, **Cuando** se ejecuta el notebook de Saldos, **Entonces** se genera un parquet con exactamente N registros donde cada CUSTID tiene un unico registro de saldo.
2. **Dado** que se ejecuta el notebook dos veces, **Cuando** se comparan los datos de saldos generados, **Entonces** los montos y valores numericos difieren entre ejecuciones (son regenerados completamente) pero los CUSTID cubren al 100% los clientes del Maestro.
3. **Dado** que el Maestro de Clientes crecio un 0.60% en una segunda ejecucion, **Cuando** se genera el parquet de Saldos, **Entonces** el numero de registros de saldos coincide exactamente con el numero de clientes actual del Maestro (incluyendo los nuevos).

---

### Historia de Usuario 4 - Suite de Pruebas TDD para Generadores de Parquets (Prioridad: P2)

Como Ingeniero de Datos, necesito un conjunto de pruebas automatizadas (TDD) que validen la correcta generacion de los 3 parquets (Maestro de Clientes, Transaccional y Saldos), incluyendo validaciones de estructura, volumetria, integridad referencial, calidad de datos y ausencia de nombres latinos, para asegurar que la simulacion cumple con todas las politicas del constitution.

**Por que esta prioridad**: La Version 2 requiere obligatoriamente pruebas TDD. Sin ellas, no se puede garantizar que los datos simulados cumplen con las reglas establecidas en el constitution y no se detectarian regresiones.

**Prueba Independiente**: Se puede validar ejecutando la suite de pruebas completa y verificando que: (1) todas las pruebas pasan exitosamente, (2) cubren la estructura de columnas de los 3 parquets, (3) validan la volumetria esperada, (4) verifican integridad referencial entre tablas, (5) confirman exclusion de nombres latinos, (6) validan los tipos de transacciones contra el catalogo aprobado.

**Escenarios de Aceptacion**:

1. **Dado** que se ejecuta la suite de pruebas, **Cuando** se generan los parquets correctamente, **Entonces** el 100% de las pruebas pasan exitosamente.
2. **Dado** que un parquet se genera con una columna faltante o mal nombrada, **Cuando** se ejecutan las pruebas de estructura, **Entonces** las pruebas fallan indicando exactamente cual columna es incorrecta.
3. **Dado** que un nombre latino se introduce accidentalmente en el Maestro de Clientes, **Cuando** se ejecutan las pruebas de validacion de nombres, **Entonces** la prueba detecta y reporta los nombres no permitidos.

---

### Casos Borde

- Que sucede si el Maestro de Clientes no existe al intentar generar el Transaccional o los Saldos? El notebook debe fallar con un mensaje claro indicando la dependencia.
- Que sucede si el parametro de fecha del Transaccional no se proporciona o tiene un formato invalido? El notebook debe validar el formato anio-mes-dia y rechazar formatos incorrectos.
- Que sucede si la cantidad de combinaciones de nombres (300 nombres x 150 apellidos = 45,000 combinaciones unicas) es insuficiente para 5 millones de clientes? Se deben permitir combinaciones repetidas de nombre+apellido, asegurando diversidad mediante otras variaciones (segundo nombre, segundo apellido).
- Que sucede si el 0.60% de incremento de clientes produce un numero con decimales (ej: 5,000,000 x 0.006 = 30,000)? Se debe redondear al entero mas cercano.
- Que sucede si el porcentaje de incremento se acumula en multiples ejecuciones (ej: segunda ejecucion 5,030,000, tercera ejecucion 5,030,000 + 0.60%)? El incremento se calcula sobre la base actual del parquet existente.
- Que sucede si se generan 15 millones de transacciones pero hay clientes sin ninguna transaccion? Es un comportamiento esperado y valido; un cliente puede tener 0 transacciones en un periodo dado.

## Requerimientos *(obligatorio)*

### Requerimientos Funcionales

- **RF-001**: Cada notebook generador DEBE ser un archivo .py compatible con el formato de notebooks de Azure Databricks, con bloques de markdown explicativos y profesionales, codigo en espanol, y nombre de archivo en formato PascalCase con prefijo "Nb" (NbGenerarMaestroCliente.py, NbGenerarTransaccionalCliente.py, NbGenerarSaldosCliente.py).
- **RF-002**: Todo el codigo de los notebooks DEBE ser completamente parametrizable mediante `dbutils.widgets` (mecanismo nativo de Databricks). La ruta de salida del parquet, la cantidad base de registros, el porcentaje de incremento, la fecha de transacciones, los pesos de distribucion de tipos de transaccion, los rangos de montos y cualquier otro valor configurable DEBEN definirse como widgets con valores por defecto. Se usaran `dbutils.widgets.text()` para valores textuales y numericos. No se permiten valores constantes hardcodeados bajo ninguna circunstancia.
- **RF-003**: El notebook del Maestro de Clientes DEBE generar un archivo parquet con exactamente 70 columnas siguiendo la estructura aprobada en la decision R2-D1, respetando los nombres estilo AS400, tipos de datos y distribucion (42 textuales, 18 fechas, 10 numericos).
- **RF-004**: El notebook del Maestro de Clientes DEBE generar 5,000,000 de registros en la primera ejecucion. En ejecuciones subsecuentes DEBE mantener los mismos clientes (conservando CUSTID) aplicando mutacion realista controlada: se definen los siguientes 15 campos demograficos/actualizables (ADDR1, ADDR2, CITY, STATE, ZPCDE, PHONE1, PHONE2, EMAIL, MRTLST, OCCPTN, EMPLYR, EMPADS, SGMNT, RISKLV, RSKSCR) que se mutan en el 20% de los registros existentes, simulando actualizaciones naturales del sistema bancario. Adicionalmente, DEBE agregar un 0.60% de clientes nuevos calculado sobre la base existente.
- **RF-005**: Los campos de nombres y apellidos del Maestro de Clientes (FRSTNM, MDLNM, LSTNM, SCNDLN, CUSTNM, MTHNM, FTHNM, CNTPRS, PREFNM) DEBEN usar exclusivamente nombres y apellidos de los catalogos hebreos, egipcios e ingleses aprobados en la decision R3-D1. No se permiten nombres ni apellidos de origen latino bajo ninguna circunstancia.
- **RF-006**: El notebook del Transaccional DEBE generar un archivo parquet con exactamente 60 columnas siguiendo la estructura aprobada en la decision R2-D2, respetando los nombres estilo AS400, tipos de datos y distribucion (30 numericos, 21 fechas, 9 textuales).
- **RF-007**: El notebook del Transaccional DEBE generar 15,000,000 de registros nuevos en cada ejecucion. Cada ejecucion produce un parquet independiente (no acumulativo). La fecha de transaccion (TRXDT) DEBE recibirse como parametro en formato anio-mes-dia (ejemplo: 2026-01-01). Las horas, minutos y segundos del timestamp (TRXTM) DEBEN simularse aleatoriamente.
- **RF-008**: El campo TRXTYP del Transaccional DEBE contener exclusivamente codigos del catalogo de 15 tipos de transacciones bancarias aprobado en la decision R2-D3 (CATM, DATM, TEXT, TINT, PGSL, ADSL, PGSV, CMPR, DPST, RTRO, DMCL, INTR, CMSN, NMNA, IMPT). La distribucion de tipos DEBE ser ponderada realista: los tipos mas comunes (DATM, CATM, CMPR, TINT, DPST) concentran aproximadamente el 60% del total, los de frecuencia media (PGSL, TEXT, RTRO, PGSV, NMNA, INTR) aproximadamente el 30%, y los menos comunes (ADSL, IMPT, DMCL, CMSN) aproximadamente el 10%. La tolerancia aceptable para las pruebas TDD es de ±5 puntos porcentuales por banda de frecuencia. Los pesos de distribucion DEBEN ser parametrizables.
- **RF-009**: Cada CUSTID en el parquet Transaccional DEBE corresponder a un cliente existente en el Maestro de Clientes. Un mismo cliente puede tener 0 o multiples transacciones.
- **RF-010**: El notebook de Saldos DEBE generar un archivo parquet con exactamente 100 columnas siguiendo la estructura aprobada en la decision R2-D4, respetando los nombres estilo AS400, tipos de datos y distribucion (30 textuales, 35 numericos, 35 fechas).
- **RF-011**: El notebook de Saldos DEBE generar exactamente un registro por cada cliente del Maestro de Clientes (relacion 1:1). Cada ejecucion DEBE regenerar por completo todos los datos de saldos. Todos los clientes del Maestro DEBEN tener un registro de saldo sin excepcion.
- **RF-012**: Los notebooks DEBEN estar ubicados en la ruta `scripts/GenerarParquets/` del repositorio conforme a la estructura de directorios definida en el Plan del SYSTEM.md.
- **RF-013**: Todo el codigo Python DEBE estar en espanol, con variables, funciones y objetos en formato snake_case en minuscula. Cada celda del notebook DEBE tener un bloque markdown explicativo previo. El codigo DEBE estar completamente comentado al detalle.
- **RF-014**: La Version 2 DEBE contar con una suite de pruebas TDD ubicada en `tests/GenerarParquets/` que valide: estructura de columnas, volumetria, integridad referencial entre tablas, tipos de datos, exclusion de nombres latinos, y codigos de tipos de transaccion contra el catalogo aprobado.
- **RF-015**: Los notebooks generadores de parquets son independientes del Lakeflow Spark Declarative Pipelines. Se ejecutan de forma aislada como notebooks separados en Azure Databricks.
- **RF-016**: Los notebooks DEBEN validar sus parametros de entrada al inicio de la ejecucion, rechazando valores nulos, vacios o con formato incorrecto, y mostrando mensajes de error claros y descriptivos.
- **RF-017**: Los montos numericos del Transaccional y de Saldos DEBEN generarse con rangos segmentados parametrizables. Para el Transaccional, los rangos varian segun tipo de transaccion: ATM (CATM/DATM): 10-1,000, transferencias (TEXT/TINT): 50-50,000, pagos al saldo (PGSL): 100-25,000, nomina (NMNA): 1,000-15,000, adelanto salarial (ADSL): 500-10,000, servicios (PGSV): 10-5,000, compras POS (CMPR): 5-15,000, depositos/retiros sucursal (DPST/RTRO): 50-100,000, domiciliacion (DMCL): 20-3,000, intereses (INTR): 1-5,000, comisiones (CMSN): 1-500, impuestos (IMPT): 1-2,000. Para Saldos, los rangos varian segun tipo de cuenta: ahorro (AHRO): 0-500,000, cuenta corriente (CRTE): 0-250,000, credito (linea de credito): 0-100,000, prestamo (PRES): 1,000-1,000,000. Todos los rangos DEBEN ser parametrizables.
- **RF-018**: Los identificadores CUSTID y TRXID DEBEN generarse con estrategia secuencial con offset parametrizable. Para CUSTID, el offset inicial es configurable via dbutils.widgets (ej: 100,000,001) para simular numeracion bancaria realista; los clientes nuevos en re-ejecuciones continuan la numeracion desde el maximo CUSTID existente + 1. Para TRXID, se usa un prefijo basado en la fecha de transaccion seguido de un secuencial (ej: 20260115000000001 para la fecha 2026-01-15), garantizando unicidad entre ejecuciones. Ambos offsets DEBEN ser parametrizables.

### Entidades Clave

- **Parquet Maestro de Clientes**: Archivo parquet que simula la tabla CMSTFL (Customer Master File) de AS400. Contiene 70 campos con estructura aprobada en R2-D1. Clave primaria: CUSTID. Primera ejecucion: 5,000,000 registros. Ejecuciones posteriores: mismos clientes + 0.60% nuevos.
- **Parquet Transaccional**: Archivo parquet que simula la tabla TRXPFL (Transaction Processing File) de AS400. Contiene 60 campos con estructura aprobada en R2-D2. Clave primaria: TRXID. Clave foranea: CUSTID (referencia al Maestro). Cada ejecucion genera 15,000,000 de registros nuevos. Fecha de transaccion via parametro.
- **Parquet Saldos de Clientes**: Archivo parquet que simula la tabla BLNCFL (Balance Client File) de AS400. Contiene 100 campos con estructura aprobada en R2-D4. Clave foranea: CUSTID (relacion 1:1 con Maestro). Regeneracion completa en cada ejecucion.
- **Catalogo de Tipos de Transaccion**: Conjunto de 15 codigos aprobados en R2-D3 que determinan el tipo de cada transaccion (CATM, DATM, TEXT, TINT, PGSL, ADSL, PGSV, CMPR, DPST, RTRO, DMCL, INTR, CMSN, NMNA, IMPT).
- **Catalogos de Nombres No Latinos**: Conjuntos de nombres (300 total) y apellidos (150 total) de origen hebreo, egipcio e ingles aprobados en R3-D1, utilizados para poblar los campos de nombres del Maestro de Clientes.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: El notebook del Maestro de Clientes genera un parquet con exactamente 5,000,000 de registros y 70 columnas en la primera ejecucion, completando el proceso en menos de 10 minutos en Computo Serverless. En ejecuciones subsecuentes, el numero de registros crece exactamente un 0.60% respecto a la base existente.
- **CE-002**: El notebook del Transaccional genera un parquet con exactamente 15,000,000 de registros y 60 columnas en cada ejecucion, completando el proceso en menos de 10 minutos en Computo Serverless.
- **CE-003**: El notebook de Saldos genera un parquet con la misma cantidad de registros que el Maestro de Clientes y 100 columnas, completando el proceso en menos de 10 minutos en Computo Serverless, garantizando cobertura del 100% de clientes (relacion 1:1 sin excepciones).
- **CE-004**: El 100% de los nombres y apellidos generados en el Maestro de Clientes provienen exclusivamente de los catalogos hebreos, egipcios e ingleses aprobados. Cero nombres de origen latino.
- **CE-005**: El 100% de los codigos TRXTYP en el Transaccional pertenecen al catalogo de 15 tipos aprobados.
- **CE-006**: El 100% de los CUSTID en el Transaccional y en los Saldos corresponden a clientes existentes en el Maestro de Clientes. Cero registros huerfanos.
- **CE-007**: La suite de pruebas TDD tiene una cobertura minima que valide cada uno de los requerimientos funcionales (RF-001 a RF-018) con al menos una prueba asociada.
- **CE-008**: Cero valores hardcodeados en los notebooks. Todos los valores configurables son parametros.
- **CE-009**: Los 3 notebooks se ejecutan correctamente en Computo Serverless de Azure Databricks sin errores de compatibilidad.

## Supuestos

- El workspace de Azure Databricks con Unity Catalog habilitado y Computo Serverless esta disponible y accesible para la ejecucion de los notebooks y las pruebas.
- Las extensiones de Databricks para VS Code estan configuradas correctamente segun lo validado en el research de V1 (decisiones R4-D1).
- Los catalogos de nombres (R3-D1) y la estructura de campos AS400 (R2-D1, R2-D2, R2-D4) aprobados en la Version 1 son la fuente de verdad y no requieren cambios.
- El catalogo de 15 tipos de transacciones aprobado en R2-D3 es definitivo y no requiere modificaciones para la Version 2.
- Los parquets se almacenan en un External Location de Azure Databricks conectado a un contenedor de Azure Data Lake Storage Gen2. La ruta de almacenamiento es parametrizable.
- Los notebooks se ejecutaran de forma manual o programada, no como parte del Lakeflow Spark Declarative Pipelines.
- La estrategia de pruebas TDD sigue el patron mixto aprobado en V1: pruebas ejecutables desde notebooks de prueba en Databricks.
- Para la segunda ejecucion del Maestro de Clientes (modificacion de datos existentes + nuevos clientes), el notebook lee el parquet existente para extraer los CUSTID previos y generar el nuevo parquet con la logica de incremento.

## Clarificaciones

### Sesion 2026-03-28

- P: Cuales campos del Maestro de Clientes se mutan en re-ejecuciones y en que proporcion de registros? -> R: Mutacion realista controlada. Se definen los siguientes 15 campos demograficos/actualizables (ADDR1, ADDR2, CITY, STATE, ZPCDE, PHONE1, PHONE2, EMAIL, MRTLST, OCCPTN, EMPLYR, EMPADS, SGMNT, RISKLV, RSKSCR) que se mutan en el 20% de los registros existentes, simulando actualizaciones naturales del sistema bancario.
- P: Como se distribuyen los 15 tipos de transaccion entre los 15 millones de registros del Transaccional? -> R: Distribucion ponderada realista con pesos parametrizables. Tipos comunes (DATM, CATM, CMPR, TINT, DPST) ~60% del total. Frecuencia media (PGSL, TEXT, RTRO, PGSV, NMNA, INTR) ~30%. Menos comunes (ADSL, IMPT, DMCL, CMSN) ~10%.
- P: Que rangos de valores numericos deben usarse para montos del Transaccional y Saldos? -> R: Rangos segmentados parametrizables por tipo de transaccion (ATM: 10-1,000; transferencias: 50-50,000; pagos saldo: 100-25,000; nomina: 1,000-15,000; etc.) y por tipo de cuenta para saldos (ahorro: 0-500,000; credito: 0-100,000; prestamo: 1,000-1,000,000; etc.).
- P: Cual es el mecanismo de parametrizacion de los notebooks? -> R: `dbutils.widgets` (mecanismo nativo de Databricks). Se usan `dbutils.widgets.text()` para definir parametros con valores por defecto. Compatible con UI de notebooks, jobs y Computo Serverless.
- P: Como se generan los identificadores CUSTID y TRXID para garantizar unicidad? -> R: Secuencial con offset parametrizable. CUSTID con offset configurable (ej: 100,000,001); nuevos clientes continuan desde el maximo existente + 1. TRXID con prefijo de fecha + secuencial (ej: 20260115000000001), garantizando unicidad entre ejecuciones.
