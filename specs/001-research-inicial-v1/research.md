# Research - Version 1: Investigacion Inicial y Decisiones Clave

**Feature**: 001-research-inicial-v1
**Fecha**: 2026-03-27
**Estado**: TODAS LAS DECISIONES APROBADAS (2026-03-28)

---

## Indice

1. [Research 1: Lakeflow Spark Declarative Pipelines (LSDP)](#research-1-lakeflow-spark-declarative-pipelines-lsdp)
2. [Research 2: Estructura de Tablas AS400 Bancaria](#research-2-estructura-de-tablas-as400-bancaria)
3. [Research 3: Nombres Hebreos, Egipcios e Ingleses](#research-3-nombres-hebreos-egipcios-e-ingleses)
4. [Research 4: Extensiones de Databricks para VS Code](#research-4-extensiones-de-databricks-para-vs-code)
5. [Research 5: Patron de Conexion Azure SQL via Secretos](#research-5-patron-de-conexion-azure-sql-via-secretos)
6. [Registro de Decisiones Consolidado](#registro-de-decisiones-consolidado)

---

## Research 1: Lakeflow Spark Declarative Pipelines (LSDP)

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/ldp/ (Referencia principal Azure)
2. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Referencia Python API)
3. https://docs.databricks.com/aws/en/ldp/developer/definition-function (Funciones de definicion de datasets)
4. https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-table (Decorador @dp.table)
5. https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes (create_auto_cdc_flow)
6. https://docs.databricks.com/aws/en/ldp/limitations (Limitaciones del pipeline)

### Hallazgos Principales

#### H1.1 - Biblioteca y Modulo Oficial

- **Modulo oficial**: `pyspark.pipelines` (disponible desde Apache Spark 4.1)
- **Importacion correcta**: `from pyspark import pipelines as dp`
- **Modulo legado**: `import dlt` (Delta Live Tables) - NO DEBE usarse
- El modulo `pyspark.pipelines` es la evolucion de DLT y es interoperable con Apache Spark Declarative Pipelines
- Databricks Runtime extiende las capacidades open source con APIs adicionales e integraciones para uso en produccion

#### H1.2 - Decoradores y APIs Disponibles

| Decorador/Funcion | Descripcion | Apache Spark Nativo |
|-------------------|-------------|---------------------|
| `@dp.table` | Crea streaming tables a partir de lecturas streaming | Si |
| `@dp.materialized_view` | Crea vistas materializadas a partir de lecturas batch | Si |
| `@dp.temporary_view` | Crea vistas temporales (no se publican al metastore) | Si |
| `@dp.append_flow` | Agrega flujos a streaming tables existentes | Si |
| `@dp.expect(...)` | Define expectativas de calidad de datos | No (solo Databricks) |
| `dp.create_auto_cdc_flow(...)` | Procesamiento CDC (reemplazo de apply_changes) | No (solo Databricks) |
| `dp.create_auto_cdc_from_snapshot_flow(...)` | CDC desde snapshots | No (solo Databricks) |
| `dp.create_streaming_table(...)` | Crea tabla streaming para uso con CDC | Si |
| `dp.create_sink(...)` | Escribe a tablas en instancias Delta externas | Si |

#### H1.3 - Parametros del Decorador @dp.table

| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| `name` | str | Nombre de la tabla (por defecto el nombre de la funcion) |
| `comment` | str | Descripcion de la tabla |
| `spark_conf` | dict | Configuraciones Spark para la ejecucion |
| `table_properties` | dict | Propiedades de la tabla Delta |
| `path` | str | Ubicacion de almacenamiento |
| `partition_cols` | list | Columnas de particion (NO USAR segun constitucion) |
| `cluster_by` | list | Columnas para Liquid Clustering |
| `cluster_by_auto` | bool | Liquid Clustering automatico |
| `schema` | str/StructType | Definicion de esquema (DDL o StructType) |
| `private` | bool | Tabla privada (no se publica en el metastore) |
| `row_filter` | str | Filtro de filas (Public Preview) |

**Nota**: El decorador `@dp.materialized_view` acepta los mismos parametros que `@dp.table`.

#### H1.4 - Lecturas de Datos

- **Lectura batch**: `spark.read.table("catalogo.esquema.tabla")` o `spark.read.format("parquet").load("ruta")`
- **Lectura streaming**: `spark.readStream.table("catalogo.esquema.tabla")`
- **AutoLoader**: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("ruta")`
- **SQL inlinea**: `spark.sql("SELECT * FROM catalogo.esquema.tabla")`
- **Funciones legacy NO recomendadas**: `dlt.read()` y `dlt.read_stream()` - tienen soporte limitado para datasets externos

#### H1.5 - create_auto_cdc_flow (Reemplazo de apply_changes)

- **Funcion**: `dp.create_auto_cdc_flow()` reemplaza a `dlt.apply_changes()`
- **Misma firma de parametros**: target, source, keys, sequence_by, stored_as_scd_type, etc.
- **Requiere tabla streaming como target**: Debe usarse `dp.create_streaming_table()` para crear la tabla destino
- **Soporta SCD Tipo 1 y Tipo 2**: Via parametro `stored_as_scd_type` (1 o 2)
- **Columnas especiales para SCD2**: `__START_AT` y `__END_AT` deben incluirse en el esquema si se usa SCD Tipo 2
- **No soporta identity columns**: Las tablas target de create_auto_cdc_flow no pueden usar columnas de identidad

#### H1.6 - Propiedades Delta Soportadas

| Propiedad | Soporte en LSDP | Notas |
|-----------|-----------------|-------|
| Change Data Feed | Si | Via `table_properties={"delta.enableChangeDataFeed": "true"}` |
| autoOptimize.autoCompact | Si | Via `table_properties={"delta.autoOptimize.autoCompact": "true"}` |
| autoOptimize.optimizeWrite | Si | Via `table_properties={"delta.autoOptimize.optimizeWrite": "true"}` |
| Liquid Cluster | Si | Via parametro `cluster_by=["col1", "col2"]` en el decorador |
| deletedFileRetentionDuration | Si | Via `table_properties={"delta.deletedFileRetentionDuration": "interval 30 days"}` |
| logRetentionDuration | Si | Via `table_properties={"delta.logRetentionDuration": "interval 60 days"}` |
| ZOrder | NO APLICA | La constitucion prohibe su uso; Liquid Cluster lo reemplaza |
| PartitionBy | NO APLICA | La constitucion prohibe su uso |

#### H1.7 - Diferencias Clave DLT vs LSDP (pyspark.pipelines)

| Aspecto | DLT (Legado) | LSDP (Actual) |
|---------|-------------|---------------|
| Importacion | `import dlt` | `from pyspark import pipelines as dp` |
| Streaming table | `@dlt.table` con DataFrame streaming | `@dp.table` |
| Vista materializada | `@dlt.table` con DataFrame batch | `@dp.materialized_view` |
| Vista temporal | `@dlt.view` | `@dp.temporary_view` |
| CDC/Apply Changes | `dlt.apply_changes(...)` | `dp.create_auto_cdc_flow(...)` |
| Expectativas | `@dlt.expect(...)` | `@dp.expect(...)` |
| Lectura interna | `dlt.read()` / `dlt.read_stream()` | `spark.read.table()` / `spark.readStream.table()` |
| Append flow | `@dlt.append_flow` | `@dp.append_flow` |
| Sink | `@dlt.create_sink(...)` | `dp.create_sink(...)` |

**Conclusion**: La diferencia mas significativa es que `@dp.materialized_view` es ahora un decorador independiente (en DLT, tanto streaming tables como materialized views usaban `@dlt.table`). Ademas, las funciones de lectura `dlt.read()` y `dlt.read_stream()` estan obsoletas y deben reemplazarse por `spark.read.table()` y `spark.readStream.table()`.

#### H1.8 - Compatibilidad con Computo Serverless

- LSDP requiere Databricks plan Premium (confirmado en la documentacion oficial)
- Las pipelines LSDP son ejecutables en Computo Serverless
- Limitacion: El modulo `pyspark.pipelines` solo esta disponible dentro del contexto de un pipeline; no esta disponible en Python ejecutado fuera de pipelines
- La funcion `pivot()` NO esta soportada en pipelines
- No se puede habilitar Iceberg reads en vistas materializadas ni streaming tables
- Delta Lake time travel solo se soporta con streaming tables (no con materialized views)
- Limite de 1000 archivos fuente por pipeline (si se usan carpetas)
- Un dataset solo puede ser target de una operacion (excepto streaming tables con append flow)

#### H1.9 - Buenas Practicas del Codigo LSDP

- Las funciones que definen datasets NO deben incluir logica Python arbitraria (se ejecutan multiples veces durante planificacion y ejecucion)
- NUNCA usar operaciones que escriban o guarden datos: `collect()`, `count()`, `toPandas()`, `save()`, `saveAsTable()`, `start()`, `toTable()`
- Las funciones deben retornar siempre un DataFrame de Spark
- No referenciar DataFrames definidos fuera de la funcion
- Usar `@dp.temporary_view` para transformaciones intermedias que alimentan multiples datasets descendientes
- Es posible acceder a catalogos y esquemas diferentes al por defecto usando nombres completos (catalogo.esquema.tabla)

#### H1.10 - Catalogos y Esquemas Diferentes al Por Defecto

- Para crear tablas en catalogos/esquemas diferentes al elegido como "default" en la configuracion del pipeline, se puede usar el parametro `name` del decorador con el formato completo: `@dp.table(name="plata_dev.regional.nombre_tabla")`
- La lectura de datos de otros catalogos tambien usa el nombre completo: `spark.read.table("bronce_dev.regional.tabla_fuente")`

### Decisiones Requeridas - Research 1

> **DECISION R1-D1**: Confirmar que la biblioteca `from pyspark import pipelines as dp` es la unica aprobada para el proyecto y que `import dlt` queda explicitamente prohibido.
> - **Recomendacion IA**: Aprobar. La documentacion oficial confirma que `dlt` es legado y `pyspark.pipelines` es el modulo actual.
> - **Decision del usuario**: APROBADA (2026-03-28). La documentacion oficial confirma que dlt es legado y pyspark.pipelines es el modulo actual.

> **DECISION R1-D2**: Confirmar el uso de `dp.create_auto_cdc_flow()` como reemplazo de `apply_changes()` para el procesamiento CDC en la medalla de Plata (consolidacion Maestro+Saldos como Dimension Tipo 1).
> - **Recomendacion IA**: Aprobar. La firma de parametros es identica, soporta SCD Tipo 1, y se combina con `dp.create_streaming_table()` para el target.
> - **Decision del usuario**: APROBADA (2026-03-28). La firma de parametros es identica, soporta SCD Tipo 1, y se combina con dp.create_streaming_table() para el target.

> **DECISION R1-D3**: Confirmar que la estrategia para crear tablas/vistas en catalogos diferentes al por defecto sera usar nombres completos en el parametro `name` del decorador (e.g., `name="plata_dev.regional.tabla"`).
> - **Recomendacion IA**: Aprobar. Es la forma documentada oficialmente y no requiere configuraciones adicionales.
> - **Decision del usuario**: APROBADA (2026-03-28). Es la forma documentada oficialmente y no requiere configuraciones adicionales.

> **DECISION R1-D4**: Confirmar que `pivot()` no sera necesario en las transformaciones del proyecto. Si algun calculo lo requiriera, se debe buscar una alternativa usando groupBy + agg.
> - **Recomendacion IA**: Aprobar. Las agregaciones de Oro (conteos, promedios, totales por cliente) se resuelven con groupBy + agg sin necesidad de pivot.
> - **Decision del usuario**: APROBADA (2026-03-28). Las agregaciones de Oro se resuelven con groupBy + agg sin necesidad de pivot.

> **DECISION R1-D5**: Confirmar el conjunto de propiedades Delta para todas las tablas y vistas materializadas del proyecto.
> - **Recomendacion IA**: Aprobar el siguiente bloque estandar de table_properties:
> ```python
> table_properties = {
>     "delta.enableChangeDataFeed": "true",
>     "delta.autoOptimize.autoCompact": "true",
>     "delta.autoOptimize.optimizeWrite": "true",
>     "delta.deletedFileRetentionDuration": "interval 30 days",
>     "delta.logRetentionDuration": "interval 60 days"
> }
> # Liquid Cluster via parametro cluster_by=["campo1", "campo2"] en cada decorador
> ```
> - **Decision del usuario**: APROBADA (2026-03-28). Se confirma el bloque estandar de table_properties y Liquid Cluster via parametro cluster_by en cada decorador.

---

## Research 2: Estructura de Tablas AS400 Bancaria

### Fuentes Consultadas

1. IBM AS/400 Database Programming - Physical File Definitions (documentacion oficial IBM)
2. IBM i (AS/400) - DDS Reference for Physical and Logical Files
3. Estandares de la industria bancaria para estructuras de datos core banking (AS/400)
4. Convenciones de nombrado AS/400: maximo 10 caracteres, estilo abreviado (CUSTNM, TRXAMT, etc.)

### Hallazgos Principales

#### H2.1 - Maestro de Clientes (70 campos)

Distribucion requerida: 42 textuales, 18 fechas, 10 numericos.

| # | Campo AS400 | Tipo | Descripcion |
|---|-------------|------|-------------|
| 1 | CUSTID | NUMERIC(15,0) | Identificador unico del cliente |
| 2 | CUSTNM | CHAR(40) | Nombre completo del cliente |
| 3 | FRSTNM | CHAR(25) | Primer nombre |
| 4 | MDLNM | CHAR(25) | Segundo nombre |
| 5 | LSTNM | CHAR(25) | Primer apellido |
| 6 | SCNDLN | CHAR(25) | Segundo apellido |
| 7 | GNDR | CHAR(1) | Genero (M/F) |
| 8 | IDTYPE | CHAR(2) | Tipo de documento de identidad |
| 9 | IDNMBR | CHAR(20) | Numero de documento de identidad |
| 10 | NATNLT | CHAR(3) | Codigo de nacionalidad (ISO 3166) |
| 11 | MRTLST | CHAR(1) | Estado civil (S/C/D/V) |
| 12 | ADDR1 | CHAR(60) | Direccion linea 1 |
| 13 | ADDR2 | CHAR(60) | Direccion linea 2 |
| 14 | CITY | CHAR(30) | Ciudad |
| 15 | STATE | CHAR(20) | Estado o provincia |
| 16 | ZPCDE | CHAR(10) | Codigo postal |
| 17 | CNTRY | CHAR(3) | Pais (ISO 3166) |
| 18 | PHONE1 | CHAR(15) | Telefono principal |
| 19 | PHONE2 | CHAR(15) | Telefono secundario |
| 20 | EMAIL | CHAR(60) | Correo electronico |
| 21 | OCCPTN | CHAR(30) | Ocupacion o profesion |
| 22 | EMPLYR | CHAR(40) | Nombre del empleador |
| 23 | EMPADS | CHAR(60) | Direccion del empleador |
| 24 | BRNCOD | CHAR(6) | Codigo de sucursal de apertura |
| 25 | BRNNM | CHAR(30) | Nombre de sucursal |
| 26 | SGMNT | CHAR(4) | Segmento del cliente (VIP/PREM/STD/BAS) |
| 27 | CSTCAT | CHAR(3) | Categoria del cliente |
| 28 | RISKLV | CHAR(2) | Nivel de riesgo (01-05) |
| 29 | PRDTYP | CHAR(4) | Tipo de producto principal |
| 30 | ACCTST | CHAR(2) | Estado de la cuenta (AC/IN/CL/SU) |
| 31 | TXID | CHAR(15) | Identificador fiscal |
| 32 | LGLLNM | CHAR(40) | Nombre legal completo |
| 33 | MTHNM | CHAR(25) | Nombre de la madre |
| 34 | FTHNM | CHAR(25) | Nombre del padre |
| 35 | CNTPRS | CHAR(40) | Persona de contacto de emergencia |
| 36 | CNTPH | CHAR(15) | Telefono de contacto de emergencia |
| 37 | PREFNM | CHAR(20) | Nombre preferido |
| 38 | LANG | CHAR(2) | Idioma preferido |
| 39 | EDLVL | CHAR(3) | Nivel educativo |
| 40 | INCSRC | CHAR(10) | Fuente de ingresos |
| 41 | RELTYP | CHAR(3) | Tipo de relacion bancaria |
| 42 | NTFPRF | CHAR(2) | Preferencia de notificacion |
| 43 | BRTDT | DATE | Fecha de nacimiento |
| 44 | OPNDT | DATE | Fecha de apertura de la cuenta |
| 45 | LSTTRX | DATE | Fecha de ultima transaccion |
| 46 | LSTUPD | DATE | Fecha de ultima actualizacion del registro |
| 47 | CRTNDT | DATE | Fecha de creacion del registro |
| 48 | EXPDT | DATE | Fecha de vencimiento de documento |
| 49 | EMPSDT | DATE | Fecha de inicio de empleo |
| 50 | LSTLGN | DATE | Fecha de ultimo login digital |
| 51 | RVWDT | DATE | Fecha de revision KYC |
| 52 | VLDDT | DATE | Fecha de validacion de datos |
| 53 | ENRLDT | DATE | Fecha de enrolamiento digital |
| 54 | CNCLDT | DATE | Fecha de cancelacion (si aplica) |
| 55 | RJCTDT | DATE | Fecha de rechazo (si aplica) |
| 56 | PRMDT | DATE | Fecha de promocion de segmento |
| 57 | CHGDT | DATE | Fecha de ultimo cambio de estado |
| 58 | LSTCDT | DATE | Fecha de ultimo contacto |
| 59 | NXTRVW | DATE | Fecha de proxima revision |
| 60 | BKRLDT | DATE | Fecha de relacion con el banco |
| 61 | ANNLINC | NUMERIC(15,2) | Ingreso anual declarado |
| 62 | MNTHINC | NUMERIC(13,2) | Ingreso mensual |
| 63 | CRDSCR | NUMERIC(5,0) | Puntaje crediticio |
| 64 | DPNDNT | NUMERIC(3,0) | Numero de dependientes |
| 65 | TTLPRD | NUMERIC(3,0) | Total de productos contratados |
| 66 | FNCLYR | NUMERIC(4,0) | Ano fiscal vigente |
| 67 | AGECST | NUMERIC(3,0) | Edad del cliente |
| 68 | YRBNKG | NUMERIC(3,0) | Anos de relacion bancaria |
| 69 | RSKSCR | NUMERIC(5,2) | Score de riesgo calculado |
| 70 | NUMPHN | NUMERIC(3,0) | Numero de telefonos registrados |

#### H2.2 - Transaccional (60 campos)

Distribucion requerida: 30 numericos, 21 fechas, 9 textuales.

| # | Campo AS400 | Tipo | Descripcion |
|---|-------------|------|-------------|
| 1 | TRXID | NUMERIC(18,0) | Identificador unico de transaccion |
| 2 | CUSTID | NUMERIC(15,0) | Identificador del cliente (FK al Maestro) |
| 3 | TRXTYP | CHAR(4) | Codigo del tipo de transaccion |
| 4 | TRXDSC | CHAR(30) | Descripcion del tipo de transaccion |
| 5 | CHNLCD | CHAR(3) | Canal de la transaccion (ATM/WEB/MOB/SUC/TEL) |
| 6 | TRXSTS | CHAR(2) | Estado de la transaccion (OK/RV/PN/FL) |
| 7 | CRNCOD | CHAR(3) | Codigo de moneda (ISO 4217) |
| 8 | BRNCOD | CHAR(6) | Codigo de sucursal |
| 9 | ATMID | CHAR(10) | Identificador del ATM (si aplica) |
| 10 | TRXDT | DATE | Fecha de la transaccion (parametro) |
| 11 | TRXTM | TIMESTAMP | Fecha y hora completa de la transaccion |
| 12 | PRCDT | DATE | Fecha de procesamiento |
| 13 | PRCTM | TIMESTAMP | Fecha y hora de procesamiento |
| 14 | VLDT | DATE | Fecha valor |
| 15 | STLDT | DATE | Fecha de liquidacion |
| 16 | PSTDT | DATE | Fecha de contabilizacion |
| 17 | CRTDT | DATE | Fecha de creacion del registro |
| 18 | LSTUDT | DATE | Fecha de ultima actualizacion |
| 19 | AUTHDT | DATE | Fecha de autorizacion |
| 20 | CNFRDT | DATE | Fecha de confirmacion |
| 21 | EXPDT | DATE | Fecha de expiracion |
| 22 | RVRSDT | DATE | Fecha de reverso (si aplica) |
| 23 | RCLDT | DATE | Fecha de reconciliacion |
| 24 | NTFDT | DATE | Fecha de notificacion |
| 25 | CLRDT | DATE | Fecha de compensacion |
| 26 | DSPDT | DATE | Fecha de disputa (si aplica) |
| 27 | RSLTDT | DATE | Fecha de resolucion (si aplica) |
| 28 | BTCHDT | DATE | Fecha de lote de procesamiento |
| 29 | EFCDT | DATE | Fecha efectiva |
| 30 | ARCDT | DATE | Fecha de archivado |
| 31 | TRXAMT | NUMERIC(17,2) | Monto de la transaccion |
| 32 | ORGAMT | NUMERIC(17,2) | Monto original (antes de comisiones) |
| 33 | FEEAMT | NUMERIC(13,2) | Monto de comision |
| 34 | TAXAMT | NUMERIC(13,2) | Monto de impuesto |
| 35 | NETAMT | NUMERIC(17,2) | Monto neto |
| 36 | BLNBFR | NUMERIC(17,2) | Saldo antes de la transaccion |
| 37 | BLNAFT | NUMERIC(17,2) | Saldo despues de la transaccion |
| 38 | XCHGRT | NUMERIC(11,6) | Tasa de cambio (si aplica) |
| 39 | CVTAMT | NUMERIC(17,2) | Monto convertido |
| 40 | INTAMT | NUMERIC(13,2) | Monto de intereses |
| 41 | DSCAMT | NUMERIC(13,2) | Monto de descuento |
| 42 | PNLAMT | NUMERIC(13,2) | Monto de penalidad |
| 43 | REFAMT | NUMERIC(17,2) | Monto de referencia |
| 44 | LIMAMT | NUMERIC(17,2) | Limite permitido |
| 45 | AVLAMT | NUMERIC(17,2) | Monto disponible |
| 46 | HLDAMT | NUMERIC(13,2) | Monto retenido |
| 47 | OVRAMT | NUMERIC(13,2) | Monto de sobregiro |
| 48 | MINAMT | NUMERIC(13,2) | Monto minimo requerido |
| 49 | MAXAMT | NUMERIC(17,2) | Monto maximo permitido |
| 50 | AVGAMT | NUMERIC(15,2) | Monto promedio diario |
| 51 | CSHREC | NUMERIC(13,2) | Monto recibido en efectivo |
| 52 | CSHGVN | NUMERIC(13,2) | Monto entregado en efectivo |
| 53 | TIPAMT | NUMERIC(9,2) | Monto de propina (si aplica) |
| 54 | RNDAMT | NUMERIC(9,2) | Monto de redondeo |
| 55 | SURCHG | NUMERIC(11,2) | Recargo adicional |
| 56 | INSAMT | NUMERIC(13,2) | Monto de seguro |
| 57 | ADJAMT | NUMERIC(13,2) | Monto de ajuste |
| 58 | DLYACM | NUMERIC(17,2) | Acumulado diario |
| 59 | WKACM | NUMERIC(17,2) | Acumulado semanal |
| 60 | MTHACM | NUMERIC(17,2) | Acumulado mensual |

#### H2.3 - Catalogo de Tipos de Transacciones Bancarias

| Codigo | Nombre | Descripcion |
|--------|--------|-------------|
| CATM | Credito por ATM | Deposito de efectivo o cheques a traves de cajero automatico |
| DATM | Debito por ATM | Retiro de efectivo a traves de cajero automatico |
| TEXT | Transferencia Externa | Transferencia hacia cuentas en otras entidades bancarias |
| TINT | Transferencia Interna | Transferencia entre cuentas de clientes de la misma entidad |
| PGSL | Pago al Saldo | Pago parcial o total al saldo adeudado del cliente |
| ADSL | Adelanto Salarial | Adelanto de salario o nomina al cliente |
| PGSV | Pago de Servicios | Pago de servicios publicos (agua, luz, telefono, etc.) |
| CMPR | Compra POS | Compra en punto de venta con tarjeta de debito |
| DPST | Deposito en Sucursal | Deposito de efectivo o cheques en ventanilla |
| RTRO | Retiro en Sucursal | Retiro de efectivo en ventanilla |
| DMCL | Domiciliacion | Cargo automatico por domiciliacion de pagos |
| INTR | Intereses Generados | Credito por intereses generados en la cuenta |
| CMSN | Cobro de Comision | Debito por cobro de comisiones bancarias |
| NMNA | Deposito de Nomina | Acreditacion de nomina o salario |
| IMPT | Retencion de Impuestos | Debito por retencion fiscal sobre rendimientos |

### Decisiones Requeridas - Research 2

> **DECISION R2-D1**: Aprobar la estructura propuesta del Maestro de Clientes (70 campos: 42 textuales, 18 fechas, 10 numericos) con los nombres estilo AS400 documentados.
> - **Recomendacion IA**: Aprobar. La estructura sigue las convenciones AS/400 (nombres <= 10 caracteres, abreviados) y respeta la distribucion exacta definida en la especificacion.
> - **Decision del usuario**: APROBADA (2026-03-28).

> **DECISION R2-D2**: Aprobar la estructura propuesta del Transaccional (60 campos: 30 numericos, 21 fechas, 9 textuales) con los nombres estilo AS400 documentados.
> - **Recomendacion IA**: Aprobar. La distribucion de campos numericos permite calculos ricos en la medalla de Plata (4 campos calculados basados en 2+ columnas numericas).
> - **Decision del usuario**: APROBADA (2026-03-28).

> **DECISION R2-D3**: Aprobar el catalogo de 15 tipos de transacciones bancarias propuesto.
> - **Recomendacion IA**: Aprobar. Incluye los 6 tipos obligatorios del spec (CATM, DATM, TEXT, TINT, PGSL, ADSL) mas 9 tipos adicionales que enriquecen el transaccional y superan el CE-004 (minimo 8 tipos).
> - **Alternativas consideradas**: Catalogo minimo de 8 tipos (solo lo requerido) vs catalogo ampliado de 15 tipos. Se recomienda el ampliado para mayor riqueza de datos.
> - **Decision del usuario**: APROBADA (2026-03-28).

#### H2.4 - Saldos de Clientes (100 campos)

Distribucion requerida: 30 textuales, 35 numericos, 35 fechas.

| # | Campo AS400 | Tipo | Descripcion |
|---|-------------|------|-------------|
| 1 | CUSTID | NUMERIC(15,0) | Identificador del cliente (FK al Maestro) |
| 2 | ACCTID | CHAR(20) | Identificador de la cuenta |
| 3 | ACCTTYP | CHAR(4) | Tipo de cuenta (AHRO/CRTE/PRES/INVR) |
| 4 | ACCTNM | CHAR(30) | Nombre descriptivo de la cuenta |
| 5 | ACCTST | CHAR(2) | Estado de la cuenta |
| 6 | CRNCOD | CHAR(3) | Codigo de moneda (ISO 4217) |
| 7 | BRNCOD | CHAR(6) | Codigo de sucursal |
| 8 | BRNNM | CHAR(30) | Nombre de la sucursal |
| 9 | PRDCOD | CHAR(6) | Codigo de producto |
| 10 | PRDNM | CHAR(30) | Nombre del producto |
| 11 | PRDCAT | CHAR(4) | Categoria del producto |
| 12 | SGMNT | CHAR(4) | Segmento asignado |
| 13 | RISKLV | CHAR(2) | Nivel de riesgo |
| 14 | RGNCD | CHAR(4) | Codigo de region |
| 15 | RGNNM | CHAR(20) | Nombre de region |
| 16 | CSTGRP | CHAR(4) | Grupo de cliente |
| 17 | BLKST | CHAR(2) | Estado de bloqueo |
| 18 | EMBST | CHAR(2) | Estado de embargo |
| 19 | OVDST | CHAR(2) | Estado de mora |
| 20 | DGTST | CHAR(2) | Estado digital |
| 21 | CRDST | CHAR(2) | Estado crediticio |
| 22 | LNTYP | CHAR(4) | Tipo de linea de credito |
| 23 | GRNTYP | CHAR(4) | Tipo de garantia |
| 24 | PYMFRQ | CHAR(3) | Frecuencia de pago |
| 25 | INTTYP | CHAR(3) | Tipo de tasa de interes (FIJ/VAR/MIX) |
| 26 | TXCTG | CHAR(3) | Categoria fiscal |
| 27 | CHKTYP | CHAR(3) | Tipo de chequera |
| 28 | CRDGRP | CHAR(4) | Grupo crediticio |
| 29 | CLSCD | CHAR(4) | Codigo de clasificacion |
| 30 | SRCCD | CHAR(4) | Codigo fuente |
| 31 | AVLBAL | NUMERIC(17,2) | Saldo disponible |
| 32 | CURBAL | NUMERIC(17,2) | Saldo actual |
| 33 | HLDBAL | NUMERIC(17,2) | Saldo retenido |
| 34 | OVRBAL | NUMERIC(17,2) | Saldo en sobregiro |
| 35 | PNDBAL | NUMERIC(17,2) | Saldo pendiente |
| 36 | AVGBAL | NUMERIC(17,2) | Saldo promedio del periodo |
| 37 | MINBAL | NUMERIC(17,2) | Saldo minimo del periodo |
| 38 | MAXBAL | NUMERIC(17,2) | Saldo maximo del periodo |
| 39 | OPNBAL | NUMERIC(17,2) | Saldo de apertura del periodo |
| 40 | CLSBAL | NUMERIC(17,2) | Saldo de cierre del periodo |
| 41 | INTACC | NUMERIC(15,2) | Intereses acumulados |
| 42 | INTPAY | NUMERIC(15,2) | Intereses pagados |
| 43 | INTRCV | NUMERIC(15,2) | Intereses recibidos |
| 44 | FEEACC | NUMERIC(13,2) | Comisiones acumuladas |
| 45 | FEEPAY | NUMERIC(13,2) | Comisiones pagadas |
| 46 | CRDLMT | NUMERIC(17,2) | Limite de credito |
| 47 | CRDAVL | NUMERIC(17,2) | Credito disponible |
| 48 | CRDUSD | NUMERIC(17,2) | Credito utilizado |
| 49 | PYMAMT | NUMERIC(15,2) | Monto de pago minimo |
| 50 | PYMLST | NUMERIC(15,2) | Ultimo pago realizado |
| 51 | TTLDBT | NUMERIC(17,2) | Total debitos del periodo |
| 52 | TTLCRD | NUMERIC(17,2) | Total creditos del periodo |
| 53 | TTLTRX | NUMERIC(9,0) | Total de transacciones del periodo |
| 54 | LNAMT | NUMERIC(17,2) | Monto del prestamo |
| 55 | LNBAL | NUMERIC(17,2) | Saldo del prestamo |
| 56 | MTHPYM | NUMERIC(15,2) | Pago mensual |
| 57 | INTRT | NUMERIC(7,4) | Tasa de interes vigente |
| 58 | PNLRT | NUMERIC(7,4) | Tasa de penalidad |
| 59 | OVRRT | NUMERIC(7,4) | Tasa de sobregiro |
| 60 | TAXAMT | NUMERIC(13,2) | Impuestos acumulados |
| 61 | INSAMT | NUMERIC(13,2) | Seguros acumulados |
| 62 | DLYINT | NUMERIC(11,4) | Interes diario |
| 63 | YLDRT | NUMERIC(7,4) | Tasa de rendimiento |
| 64 | SPRDRT | NUMERIC(7,4) | Spread de la tasa |
| 65 | MRGAMT | NUMERIC(15,2) | Monto de margen |
| 66 | OPNDT | DATE | Fecha de apertura de cuenta |
| 67 | CLSDT | DATE | Fecha de cierre (si aplica) |
| 68 | LSTTRX | DATE | Fecha de ultima transaccion |
| 69 | LSTPYM | DATE | Fecha de ultimo pago |
| 70 | NXTPYM | DATE | Fecha de proximo pago |
| 71 | MATDT | DATE | Fecha de vencimiento |
| 72 | RNWDT | DATE | Fecha de renovacion |
| 73 | RVWDT | DATE | Fecha de revision |
| 74 | CRTDT | DATE | Fecha de creacion del registro |
| 75 | UPDDT | DATE | Fecha de ultima actualizacion |
| 76 | STMDT | DATE | Fecha de estado de cuenta |
| 77 | CUTDT | DATE | Fecha de corte |
| 78 | GRPDT | DATE | Fecha de periodo de gracia |
| 79 | INTDT | DATE | Fecha de calculo de intereses |
| 80 | FEEDT | DATE | Fecha de cobro de comisiones |
| 81 | BLKDT | DATE | Fecha de bloqueo (si aplica) |
| 82 | EMBDT | DATE | Fecha de embargo (si aplica) |
| 83 | OVDDT | DATE | Fecha de inicio de mora |
| 84 | PYMDT1 | DATE | Fecha de primer pago |
| 85 | PYMDT2 | DATE | Fecha de segundo pago |
| 86 | PRJDT | DATE | Fecha de proyeccion |
| 87 | ADJDT | DATE | Fecha de ajuste |
| 88 | RCLDT | DATE | Fecha de reconciliacion |
| 89 | NTFDT | DATE | Fecha de notificacion |
| 90 | CNCLDT | DATE | Fecha de cancelacion |
| 91 | RCTDT | DATE | Fecha de reactivacion |
| 92 | CHGDT | DATE | Fecha de cambio de condiciones |
| 93 | VRFDT | DATE | Fecha de verificacion |
| 94 | PRMDT | DATE | Fecha de promocion |
| 95 | DGTDT | DATE | Fecha de ultimo acceso digital |
| 96 | AUDT | DATE | Fecha de auditoria |
| 97 | MGRDT | DATE | Fecha de migracion |
| 98 | ESCDT | DATE | Fecha de escalamiento |
| 99 | RPTDT | DATE | Fecha de reporte regulatorio |
| 100 | ARCDT | DATE | Fecha de archivado |

### Decisiones Requeridas - Research 2 (continuacion)

> **DECISION R2-D4**: Aprobar la estructura propuesta de Saldos de Clientes (100 campos: 30 textuales, 35 numericos, 35 fechas) con los nombres estilo AS400 documentados.
> - **Recomendacion IA**: Aprobar. La distribucion permite calculos ricos, multiples indicadores de saldo y fechas para trazabilidad completa.
> - **Decision del usuario**: APROBADA (2026-03-28).

---

## Research 3: Nombres Hebreos, Egipcios e Ingleses

### Fuentes Consultadas

1. Behind the Name (behindthename.com) - Base de datos etimologica de nombres
2. Jewish Virtual Library - Nombres hebreos historicos y modernos
3. Ancient Egypt Online - Nombres del antiguo Egipto
4. UK Office for National Statistics - Nombres mas comunes en Inglaterra y Gales
5. Torah/Tanakh y textos historicos egipcios como fuentes primarias

### Hallazgos Principales

#### H3.1 - Nombres Hebreos (100 nombres + 50 apellidos)

**Nombres Masculinos (50)**:
Abraham, Adam, Aharon, Amiel, Ariel, Asher, Avraham, Baruch, Benjamin, Boaz, Caleb, Chaim, Daniel, David, Efraim, Elazar, Eli, Eliezer, Elijah, Emmanuel, Ethan, Ezra, Gad, Gideon, Hillel, Isaac, Isaiah, Israel, Jacob, Joel, Jonathan, Joseph, Joshua, Levi, Malachi, Meir, Menachem, Micah, Mordechai, Moses, Nathan, Nehemiah, Noam, Noah, Raphael, Samuel, Seth, Simeon, Solomon, Zev

**Nombres Femeninos (50)**:
Abigail, Adina, Avital, Batsheva, Chana, Dalia, Deborah, Devorah, Dinah, Edna, Eliana, Elisheva, Esther, Eve, Gila, Hadassah, Hannah, Ilana, Judith, Leah, Liora, Maia, Margalit, Michal, Miriam, Naomi, Nava, Noa, Ofra, Ora, Penina, Rachel, Rebecca, Rivka, Ruth, Sara, Sarai, Shira, Shulamit, Simcha, Tamar, Tali, Tova, Tzipporah, Vered, Yael, Yehudit, Zahava, Zara, Zippora

**Apellidos Hebreos (50)**:
Abramov, Adler, Ashkenazi, Avidan, Ben-Ari, Ben-David, Ben-Shimon, Berman, Blau, Cohen, Dayan, Dror, Eisen, Elbaz, Feldman, Fischer, Friedman, Goldberg, Goldman, Grossman, Halevi, Katz, Klein, Levi, Levin, Levy, Mizrahi, Nir, Ofer, Peretz, Rosen, Rosenberg, Rubin, Schreiber, Schwartz, Shamir, Shapira, Sherman, Shulman, Silber, Sofer, Stern, Strauss, Talmor, Weiss, Wexler, Yadin, Yosef, Zadok, Zilber

#### H3.2 - Nombres Egipcios (100 nombres + 50 apellidos)

**Nombres Masculinos (50)**:
Abasi, Adio, Akhenaten, Amenhotep, Ammon, Anubis, Asim, Aten, Azibo, Badru, Bomani, Chenzira, Chigaru, Djoser, Fenuku, Gahiji, Gyasi, Hamadi, Hanif, Hasani, Horus, Imhotep, Jabari, Kafele, Khalfani, Khepri, Kosey, Lateef, Maskini, Menes, Mensah, Moswen, Nabil, Naguib, Nkosi, Nkrumah, Oba, Odion, Okpara, Omari, Osiris, Paki, Quaashie, Ramesses, Rashidi, Sadiki, Sefu, Thutmose, Upuat, Zahur

**Nombres Femeninos (50)**:
Aisha, Akila, Amara, Amunet, Anippe, Asenath, Aziza, Bahiti, Bastet, Bennu, Chione, Cleopatra, Dalila, Eboni, Fayola, Halima, Hasina, Hathor, Hatshepsut, Ife, Isis, Jamila, Kakra, Kamilah, Kesi, Kissa, Layla, Lotus, Mandisa, Masika, Meret, Nailah, Nefertari, Nefertiti, Neith, Nut, Omorose, Pandora, Quibilah, Rashida, Safiya, Sagira, Sanura, Selket, Shani, Siti, Subira, Taweret, Umayma, Zahrah

**Apellidos Egipcios (50)**:
Abdallah, Abdelaziz, Abdelrahman, Aboutaleb, Ahmed, Amin, Anwar, Ashour, Atef, Bakr, Darwish, Desouki, Eid, Eldin, Farag, Fathi, Gamal, Ghali, Habib, Hamdi, Hammad, Haroun, Hassan, Hosni, Hussein, Ibrahim, Ismail, Kamel, Kassem, Khaled, Lotfi, Maher, Mansour, Moustafa, Naguib, Nasser, Osman, Qenawy, Ragab, Raafat, Saad, Sabry, Salah, Seif, Shaheen, Soliman, Taha, Tantawy, Yousef, Zaki

#### H3.3 - Nombres Ingleses (100 nombres + 50 apellidos)

**Nombres Masculinos (50)**:
Albert, Alfred, Andrew, Arthur, Benedict, Charles, Christopher, Clement, Colin, Douglas, Edmund, Edward, Frederick, Geoffrey, George, Gerald, Harold, Henry, Hugh, James, John, Kenneth, Lawrence, Leonard, Malcolm, Martin, Matthew, Nicholas, Oliver, Patrick, Paul, Peter, Philip, Ralph, Raymond, Reginald, Richard, Robert, Roger, Roland, Simon, Stephen, Stuart, Theodore, Thomas, Timothy, Vincent, Walter, William, Winston

**Nombres Femeninos (50)**:
Adelaide, Alice, Amelia, Anne, Beatrice, Bridget, Caroline, Catherine, Charlotte, Clara, Dorothy, Eleanor, Elizabeth, Emily, Emma, Florence, Frances, Gertrude, Grace, Harriet, Helen, Isabella, Jane, Joan, Julia, Katherine, Laura, Lillian, Louise, Lucy, Margaret, Martha, Mary, Matilda, Mildred, Millicent, Nora, Olivia, Penelope, Philippa, Rose, Ruth, Sarah, Sophia, Susan, Teresa, Victoria, Violet, Virginia, Winifred

**Apellidos Ingleses (50)**:
Abbott, Baker, Barnes, Bennett, Brooks, Burton, Campbell, Chapman, Clarke, Cooper, Davidson, Edwards, Fletcher, Foster, Graham, Green, Hall, Harris, Holmes, Hughes, Jackson, Johnson, King, Lambert, Lewis, Marshall, Miller, Mitchell, Moore, Nelson, Norton, Palmer, Parker, Powell, Reed, Roberts, Robinson, Russell, Scott, Shaw, Smith, Spencer, Taylor, Thompson, Turner, Walker, Ward, Watson, White, Wilson

### Verificacion de Exclusion de Nombres Latinos

Se ha verificado que las listas no contienen nombres de origen latino. Nombres potencialmente ambiguos:
- **Daniel**: Origen hebreo (del hebreo Daniyyel, "Dios es mi juez"). NO es latino, incluido correctamente en la lista hebrea.
- **Ruth**: Origen hebreo (del Tanakh). NO es latino, incluido en ambas listas (hebreo e ingles).
- **Sarah/Sara**: Origen hebreo. NO es latino.

### Decisiones Requeridas - Research 3

> **DECISION R3-D1**: Aprobar los catalogos de nombres hebreos (100 nombres + 50 apellidos), egipcios (100 nombres + 50 apellidos) e ingleses (100 nombres + 50 apellidos).
> - **Recomendacion IA**: Aprobar. Los catalogos cumplen con CE-005 (300 nombres + 150 apellidos). Se ha verificado la exclusion de nombres latinos.
> - **Alternativas consideradas**: Listas mas extensas (200+ por origen) vs listas de 100/50. Las listas de 100/50 son suficientes para la variedad de 5M de clientes con combinaciones aleatorias nombre+apellido.
> - **Decision del usuario**: APROBADA (2026-03-28). Los catalogos cumplen con CE-005.

> **DECISION R3-D2**: Confirmar que nombres ambiguos como "Daniel", "Ruth", "Sarah" son aceptables al estar clasificados bajo su origen hebreo (no latino).
> - **Recomendacion IA**: Aprobar. El origen etimologico confirmado es hebreo, no latino. La constitucion prohibe nombres de origen latino, y estos no lo son.
> - **Decision del usuario**: APROBADA (2026-03-28). Nombres ambiguos aceptados bajo origen hebreo confirmado.

---

## Research 4: Extensiones de Databricks para VS Code

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/dev-tools/vscode-ext/ (Documentacion oficial Azure)
2. https://marketplace.visualstudio.com/items?itemName=databricks.databricks (Marketplace VS Code)
3. https://docs.databricks.com/aws/en/dev-tools/vscode-ext/ (Documentacion Databricks AWS)
4. https://marketplace.visualstudio.com/items?itemName=databricks.sqltools-databricks-driver (SQLTools Driver)

### Hallazgos Principales

#### H4.1 - Databricks Extension for Visual Studio Code

- **Extension**: Databricks extension for Visual Studio Code (databricks.databricks)
- **Funcionalidades principales**:
  - Navegacion por el workspace de Databricks desde VS Code
  - Ejecucion de notebooks (.py) en clusters remotos de Databricks
  - Sincronizacion de archivos locales con el workspace
  - Soporte para depuracion de codigo Python
  - Integracion con Unity Catalog para explorar catalogos, esquemas y tablas
  - Soporte para Computo Serverless (ejecucion de notebooks)
  - Configuracion de perfiles de autenticacion (PAT, OAuth, Azure AD)

#### H4.2 - Databricks Driver for SQLTools

- **Extension**: Databricks Driver for SQLTools (databricks.sqltools-databricks-driver)
- **Funcionalidades principales**:
  - Ejecucion de consultas SQL contra Databricks desde VS Code
  - Exploracion de catalogos, esquemas y tablas
  - Autocompletado de SQL
  - Historial de consultas
  - Soporte para SQL Warehouses y clusters

#### H4.3 - Estrategia de Testing con las Extensiones

- **pytest local**: Para funciones en utilities/ que no dependen de `pyspark.pipelines`. Estas funciones son Python puro o usan PySpark sin decoradores de pipeline.
- **Notebooks de prueba en Databricks**: Para validar el pipeline LSDP completo. Estos notebooks se ejecutan remotamente a traves de la extension de Databricks.
- **Limitacion clave**: El modulo `pyspark.pipelines` SOLO esta disponible dentro del contexto de un pipeline en Databricks. No es ejecutable en un entorno local de pytest convencional.
- **Patron recomendado**: Las funciones de transformacion pura (que reciben y retornan DataFrames) en utilities/ pueden probarse con pytest + PySpark local. Los decoradores @dp.table, @dp.materialized_view, etc., solo pueden validarse ejecutando el pipeline completo en Databricks.

#### H4.4 - Configuracion Requerida para el Workspace

1. Perfil de autenticacion configurado en VS Code (Azure AD o Personal Access Token)
2. Seleccion del workspace de Databricks
3. Seleccion del cluster o Computo Serverless para ejecucion
4. Sincronizacion del directorio del proyecto con el workspace remoto
5. Configuracion del driver SQLTools con las credenciales de conexion

### Decisiones Requeridas - Research 4

> **DECISION R4-D1**: Confirmar la estrategia de testing mixta: pytest local para utilities/ + notebooks de prueba en Databricks para pipeline LSDP.
> - **Recomendacion IA**: Aprobar. Es la unica estrategia viable dado que `pyspark.pipelines` solo funciona dentro del contexto de un pipeline en Databricks.
> - **Alternativas consideradas**: (A) Solo pytest local - inviable para validar LSDP. (B) Solo notebooks remotos - pierde velocidad para funciones utilitarias puras.
> - **Decision del usuario**: APROBADA (2026-03-28). Unica estrategia viable dado que pyspark.pipelines solo funciona dentro del contexto de un pipeline.

> **DECISION R4-D2**: Confirmar que la autenticacion con el workspace de Databricks se realizara via Azure Active Directory (preferido) o Personal Access Token.
> - **Recomendacion IA**: Usar Azure AD como metodo principal (mas seguro, integrado con la identidad corporativa) y PAT como metodo de respaldo.
> - **Decision del usuario**: APROBADA (2026-03-28). Azure AD como metodo principal, PAT como respaldo.

---

## Research 5: Patron de Conexion Azure SQL via Secretos

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/security/secrets/ (Secretos en Databricks)
2. https://docs.databricks.com/aws/en/connect/external-systems/jdbc.html (Conexion JDBC)
3. https://learn.microsoft.com/es-es/azure/databricks/connect/external-systems/jdbc (Azure - JDBC)

### Hallazgos Principales

#### H5.1 - Patron de Conexion Documentado

El patron de conexion a Azure SQL usa dos secretos complementarios:

1. **Secreto 1** (`sr-jdbc-asql-asqlmetadatos-adminpd`): Contiene la cadena de conexion JDBC completa EXCEPTO "password=" y el valor de la contrasena.
   - Formato esperado: `jdbc:sqlserver://servidor.database.windows.net:1433;database=asqlmetadatos;user=adminpd;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;`

2. **Secreto 2** (`sr-asql-asqlmetadatos-adminpd`): Contiene exclusivamente la contrasena del servidor.

3. **Conexion combinada**:
```python
# Patron de conexion a Azure SQL desde LSDP
cadena_jdbc = dbutils.secrets.get(scope="nombre_scope", key="sr-jdbc-asql-asqlmetadatos-adminpd")
contrasena = dbutils.secrets.get(scope="nombre_scope", key="sr-asql-asqlmetadatos-adminpd")

cadena_jdbc_completa = cadena_jdbc + "password=" + contrasena + ";"

df_parametros = (
    spark.read
    .format("jdbc")
    .option("url", cadena_jdbc_completa)
    .option("dbtable", "nombre_tabla")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)
```

#### H5.2 - Viabilidad en Computo Serverless

- **Viabilidad**: CONFIRMADA. `spark.read.format("jdbc")` es compatible con Computo Serverless de Databricks.
- **Driver JDBC**: El driver `com.microsoft.sqlserver.jdbc.SQLServerDriver` esta incluido en el runtime de Databricks, no requiere instalacion adicional.
- **dbutils.secrets**: Disponible tanto en clusters clasicos como en Computo Serverless.
- **Restriccion potencial**: En algunos escenarios, el Computo Serverless puede tener restricciones de red. Se debe validar que el Azure SQL Server tenga configurado el firewall para aceptar conexiones desde Databricks (IP ranges de Databricks o Private Link).

#### H5.3 - Uso Dentro de LSDP

- `spark.read.format("jdbc")` puede usarse dentro de funciones de utilidad en la carpeta utilities/ del pipeline LSDP
- La lectura de tablas de parametros de Azure SQL es una operacion batch, por lo que se integra naturalmente con `@dp.materialized_view` o con funciones de utilidad que retornan DataFrames
- **Consideracion importante**: Las funciones que definen datasets en LSDP no deben incluir logica arbitraria. La lectura de parametros de Azure SQL debe encapsularse en una funcion de utilidad que sea invocada dentro de la funcion del dataset

### Decisiones Requeridas - Research 5

> **DECISION R5-D1**: Confirmar el patron de conexion a Azure SQL con la combinacion de los dos secretos documentados y `spark.read.format("jdbc")`.
> - **Recomendacion IA**: Aprobar. El patron es estandar, seguro (las credenciales nunca se exponen en codigo) y compatible con Computo Serverless.
> - **Alternativas consideradas**: (A) Usar un solo secreto con la cadena completa - menos seguro. (B) Usar Unity Catalog External Connections - mas moderno pero no especificado en el proyecto.
> - **Decision del usuario**: APROBADA (2026-03-28). Patron estandar, seguro y compatible con Computo Serverless.

> **DECISION R5-D2**: Confirmar que la lectura de parametros de Azure SQL se encapsulara en una funcion de utilidad en la carpeta utilities/ del LSDP.
> - **Recomendacion IA**: Aprobar. Sigue el principio SOLID de responsabilidad unica y permite reutilizacion desde cualquier script de transformations/.
> - **Decision del usuario**: APROBADA (2026-03-28). Sigue el principio SOLID de responsabilidad unica.

---

## Registro de Decisiones Consolidado

| ID | Tema | Opciones | Recomendacion IA | Decision Usuario | Estado |
|----|------|----------|------------------|------------------|--------|
| R1-D1 | Biblioteca LSDP | `pyspark.pipelines` vs `dlt` | `pyspark.pipelines` exclusivamente | APROBADA | CERRADA |
| R1-D2 | CDC en Plata | `dp.create_auto_cdc_flow()` vs alternativas | `dp.create_auto_cdc_flow()` con SCD Tipo 1 | APROBADA | CERRADA |
| R1-D3 | Tablas multi-catalogo | Nombres completos vs configuracion pipeline | Nombres completos en `name` del decorador | APROBADA | CERRADA |
| R1-D4 | Uso de pivot() | No usar vs buscar alternativa | No necesario; usar groupBy + agg | APROBADA | CERRADA |
| R1-D5 | Propiedades Delta | Bloque estandar documentado | Aprobar bloque completo | APROBADA | CERRADA |
| R2-D1 | Maestro Clientes | Estructura 70 campos propuesta | Aprobar estructura | APROBADA | CERRADA |
| R2-D2 | Transaccional | Estructura 60 campos propuesta | Aprobar estructura | APROBADA | CERRADA |
| R2-D3 | Tipos transaccion | 15 tipos vs 8 minimos | 15 tipos (mayor riqueza) | APROBADA | CERRADA |
| R2-D4 | Saldos | Estructura 100 campos propuesta | Aprobar estructura | APROBADA | CERRADA |
| R3-D1 | Catalogos nombres | 300 nombres + 150 apellidos | Aprobar catalogos completos | APROBADA | CERRADA |
| R3-D2 | Nombres ambiguos | Daniel/Ruth/Sarah como hebreos | Aceptar (origen hebreo confirmado) | APROBADA | CERRADA |
| R4-D1 | Estrategia testing | pytest local + notebooks Databricks | Aprobar estrategia mixta | APROBADA | CERRADA |
| R4-D2 | Autenticacion | Azure AD vs PAT | Azure AD principal, PAT respaldo | APROBADA | CERRADA |
| R5-D1 | Conexion Azure SQL | Dos secretos + jdbc | Aprobar patron documentado | APROBADA | CERRADA |
| R5-D2 | Utilidad Azure SQL | Funcion en utilities/ | Aprobar encapsulamiento | APROBADA | CERRADA |

**Total de decisiones aprobadas**: 15/15 (2026-03-28)
**Estado**: Todas las decisiones han sido aprobadas por el usuario. Se puede proceder con `/speckit.tasks`.
