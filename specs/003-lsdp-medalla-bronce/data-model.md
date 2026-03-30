# Modelo de Datos - Version 3: LSDP Medalla de Bronce

**Feature**: 003-lsdp-medalla-bronce
**Fecha**: 2026-03-29
**Base**: Decisiones R1-D1 a R7-D2

---

## Indice

1. [Fuentes Externas](#fuentes-externas)
2. [Tablas Streaming de Bronce](#tablas-streaming-de-bronce)
3. [Parametros del Pipeline](#parametros-del-pipeline)
4. [Propiedades Delta Estandar](#propiedades-delta-estandar)
5. [Diagrama de Flujo de Datos](#diagrama-de-flujo-de-datos)

---

## Fuentes Externas

### Tabla dbo.Parametros (Azure SQL)

**Ubicacion**: Azure SQL Server (cadena JDBC via secretos de Databricks)
**Estructura**: Clave-Valor

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| Clave | VARCHAR | Nombre del parametro |
| Valor | VARCHAR | Valor del parametro |

**Registros relevantes para V3**:

| Clave | Ejemplo de Valor | Uso |
|-------|-----------------|-----|
| catalogoBronce | bronce_dev | Catalogo de Unity Catalog para tablas de bronce |
| contenedorBronce | bronce | Contenedor ADLS Gen2 |
| datalake | adlsg2datalakedev | Storage Account ADLS Gen2 |
| DirectorioBronce | archivos | Directorio raiz dentro del contenedor |

### Parquets Fuente (ADLS Gen2 via abfss://)

Los parquets fueron generados por la Version 2 y residen en rutas abfss:// construidas dinamicamente.

| Fuente | Ruta Ejemplo | Estructura Particiones |
|--------|-------------|----------------------|
| Maestro de Clientes | abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/ | Subdirectorios por anio/mes/dia |
| Transaccional | abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/Transaccional/ | Subdirectorios por anio/mes/dia |
| Saldos de Clientes | abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/SaldoCliente/ | Subdirectorios por anio/mes/dia |

---

## Tablas Streaming de Bronce

### bronce_dev.regional.cmstfl (Maestro de Clientes)

**Tipo**: Streaming Table (@dp.table)
**Catalogo/Esquema**: bronce_dev.regional (por defecto del pipeline)
**Nombre**: cmstfl (Customer Master File - nombre AS400 original)
**Fuente**: AutoLoader (cloudFiles) desde parquets del Maestro de Clientes
**Acumulacion**: Historica (append-only via AutoLoader incremental)
**Volumen esperado**: 5M+ registros por carga, crecimiento 0.60% por ejecucion

**Esquema** (70 columnas AS400 + FechaIngestaDatos + _rescued_data = 72 columnas iniciales — sujeto a schema evolution via `addNewColumns`):

| # | Campo | Tipo Parquet | Categoria | Descripcion |
|---|-------|-------------|-----------|-------------|
| 1 | CUSTID | NUMERIC(15,0) | Numerico | Identificador unico del cliente (clave de negocio) |
| 2 | CUSTNM | CHAR(40) | Textual | Nombre completo del cliente |
| 3 | FRSTNM | CHAR(25) | Textual | Primer nombre |
| 4 | MDLNM | CHAR(25) | Textual | Segundo nombre |
| 5 | LSTNM | CHAR(25) | Textual | Primer apellido |
| 6 | SCNDLN | CHAR(25) | Textual | Segundo apellido |
| 7 | GNDR | CHAR(1) | Textual | Genero (M/F) |
| 8 | IDTYPE | CHAR(2) | Textual | Tipo de documento de identidad |
| 9 | IDNMBR | CHAR(20) | Textual | Numero de documento de identidad |
| 10 | NATNLT | CHAR(3) | Textual | Codigo de nacionalidad (ISO 3166) |
| 11 | MRTLST | CHAR(1) | Textual | Estado civil (S/C/D/V) |
| 12 | ADDR1 | CHAR(60) | Textual | Direccion linea 1 |
| 13 | ADDR2 | CHAR(60) | Textual | Direccion linea 2 |
| 14 | CITY | CHAR(30) | Textual | Ciudad |
| 15 | STATE | CHAR(20) | Textual | Estado o provincia |
| 16 | ZPCDE | CHAR(10) | Textual | Codigo postal |
| 17 | CNTRY | CHAR(3) | Textual | Pais (ISO 3166) |
| 18 | PHONE1 | CHAR(15) | Textual | Telefono principal |
| 19 | PHONE2 | CHAR(15) | Textual | Telefono secundario |
| 20 | EMAIL | CHAR(60) | Textual | Correo electronico |
| 21 | OCCPTN | CHAR(30) | Textual | Ocupacion o profesion |
| 22 | EMPLYR | CHAR(40) | Textual | Nombre del empleador |
| 23 | EMPADS | CHAR(60) | Textual | Direccion del empleador |
| 24 | BRNCOD | CHAR(6) | Textual | Codigo de sucursal de apertura |
| 25 | BRNNM | CHAR(30) | Textual | Nombre de sucursal |
| 26 | SGMNT | CHAR(4) | Textual | Segmento del cliente (VIP/PREM/STD/BAS) |
| 27 | CSTCAT | CHAR(3) | Textual | Categoria del cliente |
| 28 | RISKLV | CHAR(2) | Textual | Nivel de riesgo (01-05) |
| 29 | PRDTYP | CHAR(4) | Textual | Tipo de producto principal |
| 30 | ACCTST | CHAR(2) | Textual | Estado de la cuenta (AC/IN/CL/SU) |
| 31 | TXID | CHAR(15) | Textual | Identificador fiscal |
| 32 | LGLLNM | CHAR(40) | Textual | Nombre legal completo |
| 33 | MTHNM | CHAR(25) | Textual | Nombre de la madre |
| 34 | FTHNM | CHAR(25) | Textual | Nombre del padre |
| 35 | CNTPRS | CHAR(40) | Textual | Persona de contacto de emergencia |
| 36 | CNTPH | CHAR(15) | Textual | Telefono de contacto de emergencia |
| 37 | PREFNM | CHAR(20) | Textual | Nombre preferido |
| 38 | LANG | CHAR(2) | Textual | Idioma preferido |
| 39 | EDLVL | CHAR(3) | Textual | Nivel educativo |
| 40 | INCSRC | CHAR(10) | Textual | Fuente de ingresos |
| 41 | RELTYP | CHAR(3) | Textual | Tipo de relacion bancaria |
| 42 | NTFPRF | CHAR(2) | Textual | Preferencia de notificacion |
| 43 | BRTDT | DATE | Fecha | Fecha de nacimiento |
| 44 | OPNDT | DATE | Fecha | Fecha de apertura de la cuenta |
| 45 | LSTTRX | DATE | Fecha | Fecha de ultima transaccion |
| 46 | LSTUPD | DATE | Fecha | Fecha de ultima actualizacion del registro |
| 47 | CRTNDT | DATE | Fecha | Fecha de creacion del registro |
| 48 | EXPDT | DATE | Fecha | Fecha de vencimiento de documento |
| 49 | EMPSDT | DATE | Fecha | Fecha de inicio de empleo |
| 50 | LSTLGN | DATE | Fecha | Fecha de ultimo login digital |
| 51 | RVWDT | DATE | Fecha | Fecha de revision KYC |
| 52 | VLDDT | DATE | Fecha | Fecha de validacion de datos |
| 53 | ENRLDT | DATE | Fecha | Fecha de enrolamiento digital |
| 54 | CNCLDT | DATE | Fecha | Fecha de cancelacion (si aplica) |
| 55 | RJCTDT | DATE | Fecha | Fecha de rechazo (si aplica) |
| 56 | PRMDT | DATE | Fecha | Fecha de promocion de segmento |
| 57 | CHGDT | DATE | Fecha | Fecha de ultimo cambio de estado |
| 58 | LSTCDT | DATE | Fecha | Fecha de ultimo contacto |
| 59 | NXTRVW | DATE | Fecha | Fecha de proxima revision |
| 60 | BKRLDT | DATE | Fecha | Fecha de relacion con el banco |
| 61 | ANNLINC | NUMERIC(15,2) | Numerico | Ingreso anual declarado |
| 62 | MNTHINC | NUMERIC(13,2) | Numerico | Ingreso mensual |
| 63 | CRDSCR | NUMERIC(5,0) | Numerico | Puntaje crediticio |
| 64 | DPNDNT | NUMERIC(3,0) | Numerico | Numero de dependientes |
| 65 | TTLPRD | NUMERIC(3,0) | Numerico | Total de productos contratados |
| 66 | FNCLYR | NUMERIC(4,0) | Numerico | Ano fiscal vigente |
| 67 | AGECST | NUMERIC(3,0) | Numerico | Edad del cliente |
| 68 | YRBNKG | NUMERIC(3,0) | Numerico | Anos de relacion bancaria |
| 69 | RSKSCR | NUMERIC(5,2) | Numerico | Score de riesgo calculado |
| 70 | NUMPHN | NUMERIC(3,0) | Numerico | Numero de telefonos registrados |
| 71 | FechaIngestaDatos | TIMESTAMP | Agregado | Marca de tiempo del momento de la ingesta (RF-007) |
| 72 | _rescued_data | STRING | Automatico | Columna de rescate de datos (inferencia automatica del AutoLoader) |

**Distribucion**: 42 textuales + 18 fechas + 10 numericos + 1 FechaIngestaDatos + 1 _rescued_data = 72 columnas

**Liquid Cluster** (R6-D1):
```python
cluster_by=["FechaIngestaDatos", "CUSTID"]
```
- `FechaIngestaDatos`: Primer campo por requisito del usuario. Optimiza consultas temporales de ingesta.
- `CUSTID`: Clave primaria de negocio. Optimiza joins con trxpfl y blncfl, y filtros por cliente.

**Propiedades Delta**: Ver seccion [Propiedades Delta Estandar](#propiedades-delta-estandar).

---

### bronce_dev.regional.trxpfl (Transaccional)

**Tipo**: Streaming Table (@dp.table)
**Catalogo/Esquema**: bronce_dev.regional (por defecto del pipeline)
**Nombre**: trxpfl (Transaction Processing File - nombre AS400 original)
**Fuente**: AutoLoader (cloudFiles) desde parquets del Transaccional
**Acumulacion**: Historica (append-only via AutoLoader incremental)
**Volumen esperado**: 15M registros nuevos por ejecucion

**Esquema** (60 columnas AS400 + FechaIngestaDatos + _rescued_data = 62 columnas iniciales — sujeto a schema evolution via `addNewColumns`):

| # | Campo | Tipo Parquet | Categoria | Descripcion |
|---|-------|-------------|-----------|-------------|
| 1 | TRXID | NUMERIC(18,0) | Numerico | Identificador unico de transaccion |
| 2 | CUSTID | NUMERIC(15,0) | Numerico | Identificador del cliente (FK al Maestro) |
| 3 | TRXTYP | CHAR(4) | Textual | Codigo del tipo de transaccion |
| 4 | TRXDSC | CHAR(30) | Textual | Descripcion del tipo de transaccion |
| 5 | CHNLCD | CHAR(3) | Textual | Canal de la transaccion (ATM/WEB/MOB/SUC/TEL) |
| 6 | TRXSTS | CHAR(2) | Textual | Estado de la transaccion (OK/RV/PN/FL) |
| 7 | CRNCOD | CHAR(3) | Textual | Codigo de moneda (ISO 4217) |
| 8 | BRNCOD | CHAR(6) | Textual | Codigo de sucursal |
| 9 | ATMID | CHAR(10) | Textual | Identificador del ATM (si aplica) |
| 10 | TRXDT | DATE | Fecha | Fecha de la transaccion (parametro) |
| 11 | TRXTM | TIMESTAMP | Fecha | Fecha y hora completa de la transaccion |
| 12 | PRCDT | DATE | Fecha | Fecha de procesamiento |
| 13 | PRCTM | TIMESTAMP | Fecha | Fecha y hora de procesamiento |
| 14 | VLDT | DATE | Fecha | Fecha valor |
| 15 | STLDT | DATE | Fecha | Fecha de liquidacion |
| 16 | PSTDT | DATE | Fecha | Fecha de contabilizacion |
| 17 | CRTDT | DATE | Fecha | Fecha de creacion del registro |
| 18 | LSTUDT | DATE | Fecha | Fecha de ultima actualizacion |
| 19 | AUTHDT | DATE | Fecha | Fecha de autorizacion |
| 20 | CNFRDT | DATE | Fecha | Fecha de confirmacion |
| 21 | EXPDT | DATE | Fecha | Fecha de expiracion |
| 22 | RVRSDT | DATE | Fecha | Fecha de reverso (si aplica) |
| 23 | RCLDT | DATE | Fecha | Fecha de reconciliacion |
| 24 | NTFDT | DATE | Fecha | Fecha de notificacion |
| 25 | CLRDT | DATE | Fecha | Fecha de compensacion |
| 26 | DSPDT | DATE | Fecha | Fecha de disputa (si aplica) |
| 27 | RSLTDT | DATE | Fecha | Fecha de resolucion (si aplica) |
| 28 | BTCHDT | DATE | Fecha | Fecha de lote de procesamiento |
| 29 | EFCDT | DATE | Fecha | Fecha efectiva |
| 30 | ARCDT | DATE | Fecha | Fecha de archivado |
| 31 | TRXAMT | NUMERIC(17,2) | Numerico | Monto de la transaccion |
| 32 | ORGAMT | NUMERIC(17,2) | Numerico | Monto original (antes de comisiones) |
| 33 | FEEAMT | NUMERIC(13,2) | Numerico | Monto de comision |
| 34 | TAXAMT | NUMERIC(13,2) | Numerico | Monto de impuesto |
| 35 | NETAMT | NUMERIC(17,2) | Numerico | Monto neto |
| 36 | BLNBFR | NUMERIC(17,2) | Numerico | Saldo antes de la transaccion |
| 37 | BLNAFT | NUMERIC(17,2) | Numerico | Saldo despues de la transaccion |
| 38 | XCHGRT | NUMERIC(11,6) | Numerico | Tasa de cambio (si aplica) |
| 39 | CVTAMT | NUMERIC(17,2) | Numerico | Monto convertido |
| 40 | INTAMT | NUMERIC(13,2) | Numerico | Monto de intereses |
| 41 | DSCAMT | NUMERIC(13,2) | Numerico | Monto de descuento |
| 42 | PNLAMT | NUMERIC(13,2) | Numerico | Monto de penalidad |
| 43 | REFAMT | NUMERIC(17,2) | Numerico | Monto de referencia |
| 44 | LIMAMT | NUMERIC(17,2) | Numerico | Limite permitido |
| 45 | AVLAMT | NUMERIC(17,2) | Numerico | Monto disponible |
| 46 | HLDAMT | NUMERIC(13,2) | Numerico | Monto retenido |
| 47 | OVRAMT | NUMERIC(13,2) | Numerico | Monto de sobregiro |
| 48 | MINAMT | NUMERIC(13,2) | Numerico | Monto minimo requerido |
| 49 | MAXAMT | NUMERIC(17,2) | Numerico | Monto maximo permitido |
| 50 | AVGAMT | NUMERIC(15,2) | Numerico | Monto promedio diario |
| 51 | CSHREC | NUMERIC(13,2) | Numerico | Monto recibido en efectivo |
| 52 | CSHGVN | NUMERIC(13,2) | Numerico | Monto entregado en efectivo |
| 53 | TIPAMT | NUMERIC(9,2) | Numerico | Monto de propina (si aplica) |
| 54 | RNDAMT | NUMERIC(9,2) | Numerico | Monto de redondeo |
| 55 | SURCHG | NUMERIC(11,2) | Numerico | Recargo adicional |
| 56 | INSAMT | NUMERIC(13,2) | Numerico | Monto de seguro |
| 57 | ADJAMT | NUMERIC(13,2) | Numerico | Monto de ajuste |
| 58 | DLYACM | NUMERIC(17,2) | Numerico | Acumulado diario |
| 59 | WKACM | NUMERIC(17,2) | Numerico | Acumulado semanal |
| 60 | MTHACM | NUMERIC(17,2) | Numerico | Acumulado mensual |
| 61 | FechaIngestaDatos | TIMESTAMP | Agregado | Marca de tiempo del momento de la ingesta (RF-007) |
| 62 | _rescued_data | STRING | Automatico | Columna de rescate de datos (inferencia automatica del AutoLoader) |

**Distribucion**: 9 textuales + 21 fechas + 30 numericos + 1 FechaIngestaDatos + 1 _rescued_data = 62 columnas

**Liquid Cluster** (R6-D1):
```python
cluster_by=["TRXDT", "CUSTID", "TRXTYP"]
```
- `TRXDT`: Fecha de la transaccion como primer campo (requisito del usuario). Optimiza consultas por rangos de fecha que son el patron de consulta mas frecuente en datos transaccionales.
- `CUSTID`: FK al Maestro de Clientes. Optimiza joins con cmstfl y filtros por cliente especifico para el analisis de comportamiento.
- `TRXTYP`: Tipo de transaccion. Optimiza filtros frecuentes por tipo (CATM, DATM, PGSL) para el analisis de ATM y pagos al saldo requerido por negocio.

**Propiedades Delta**: Ver seccion [Propiedades Delta Estandar](#propiedades-delta-estandar).

---

### bronce_dev.regional.blncfl (Saldos de Clientes)

**Tipo**: Streaming Table (@dp.table)
**Catalogo/Esquema**: bronce_dev.regional (por defecto del pipeline)
**Nombre**: blncfl (Balance Client File - nombre AS400 original)
**Fuente**: AutoLoader (cloudFiles) desde parquets de Saldos de Clientes
**Acumulacion**: Historica (append-only via AutoLoader incremental)
**Volumen esperado**: 5M+ registros por carga (1 registro por cliente)

**Esquema** (100 columnas AS400 + FechaIngestaDatos + _rescued_data = 102 columnas iniciales — sujeto a schema evolution via `addNewColumns`):

| # | Campo | Tipo Parquet | Categoria | Descripcion |
|---|-------|-------------|-----------|-------------|
| 1 | CUSTID | NUMERIC(15,0) | Numerico | Identificador del cliente (FK al Maestro) |
| 2 | ACCTID | CHAR(20) | Textual | Identificador de la cuenta |
| 3 | ACCTTYP | CHAR(4) | Textual | Tipo de cuenta (AHRO/CRTE/PRES/INVR) |
| 4 | ACCTNM | CHAR(30) | Textual | Nombre descriptivo de la cuenta |
| 5 | ACCTST | CHAR(2) | Textual | Estado de la cuenta |
| 6 | CRNCOD | CHAR(3) | Textual | Codigo de moneda (ISO 4217) |
| 7 | BRNCOD | CHAR(6) | Textual | Codigo de sucursal |
| 8 | BRNNM | CHAR(30) | Textual | Nombre de la sucursal |
| 9 | PRDCOD | CHAR(6) | Textual | Codigo de producto |
| 10 | PRDNM | CHAR(30) | Textual | Nombre del producto |
| 11 | PRDCAT | CHAR(4) | Textual | Categoria del producto |
| 12 | SGMNT | CHAR(4) | Textual | Segmento asignado |
| 13 | RISKLV | CHAR(2) | Textual | Nivel de riesgo |
| 14 | RGNCD | CHAR(4) | Textual | Codigo de region |
| 15 | RGNNM | CHAR(20) | Textual | Nombre de region |
| 16 | CSTGRP | CHAR(4) | Textual | Grupo de cliente |
| 17 | BLKST | CHAR(2) | Textual | Estado de bloqueo |
| 18 | EMBST | CHAR(2) | Textual | Estado de embargo |
| 19 | OVDST | CHAR(2) | Textual | Estado de mora |
| 20 | DGTST | CHAR(2) | Textual | Estado digital |
| 21 | CRDST | CHAR(2) | Textual | Estado crediticio |
| 22 | LNTYP | CHAR(4) | Textual | Tipo de linea de credito |
| 23 | GRNTYP | CHAR(4) | Textual | Tipo de garantia |
| 24 | PYMFRQ | CHAR(3) | Textual | Frecuencia de pago |
| 25 | INTTYP | CHAR(3) | Textual | Tipo de tasa de interes (FIJ/VAR/MIX) |
| 26 | TXCTG | CHAR(3) | Textual | Categoria fiscal |
| 27 | CHKTYP | CHAR(3) | Textual | Tipo de chequera |
| 28 | CRDGRP | CHAR(4) | Textual | Grupo crediticio |
| 29 | CLSCD | CHAR(4) | Textual | Codigo de clasificacion |
| 30 | SRCCD | CHAR(4) | Textual | Codigo fuente |
| 31-65 | (35 campos numericos) | NUMERIC | Numerico | Saldos, limites, tasas, acumulados (ver R2-D4 en research V1) |
| 66-100 | (35 campos fecha) | DATE | Fecha | Fechas de operaciones, estados, auditorias (ver R2-D4 en research V1) |
| 101 | FechaIngestaDatos | TIMESTAMP | Agregado | Marca de tiempo del momento de la ingesta (RF-007) |
| 102 | _rescued_data | STRING | Automatico | Columna de rescate de datos (inferencia automatica del AutoLoader) |

**Distribucion**: 30 textuales + 35 fechas + 35 numericos + 1 FechaIngestaDatos + 1 _rescued_data = 102 columnas

**Nota**: Los 100 campos AS400 completos estan documentados en detalle en el hallazgo H2.4 del research de la Version 1. Se resumen aqui los primeros 30 textuales por brevedad.

**Liquid Cluster** (R6-D1):
```python
cluster_by=["FechaIngestaDatos", "CUSTID"]
```
- `FechaIngestaDatos`: Primer campo por requisito del usuario. Optimiza consultas temporales de ingesta y filtros por lote de carga.
- `CUSTID`: FK al Maestro de Clientes. Optimiza joins con cmstfl para la consolidacion cliente+saldo como dimension tipo 1 en plata (V4).

**Propiedades Delta**: Ver seccion [Propiedades Delta Estandar](#propiedades-delta-estandar).

---

## Parametros del Pipeline

### Parametros Recibidos por el Pipeline LSDP

| Parametro | Tipo | Ejemplo | Descripcion |
|-----------|------|---------|-------------|
| rutaCompletaMaestroCliente | string | LSDP_Base/As400/MaestroCliente/ | Ruta relativa al directorio de parquets del maestro |
| rutaCompletaSaldoCliente | string | LSDP_Base/As400/SaldoCliente/ | Ruta relativa al directorio de parquets de saldos |
| rutaCompletaTransaccional | string | LSDP_Base/As400/Transaccional/ | Ruta relativa al directorio de parquets transaccionales |
| rutaCheckpointCmstfl | string | LSDP_Base/Checkpoints/Bronce/cmstfl/ | Ruta relativa para el schemaLocation del AutoLoader de cmstfl |
| rutaCheckpointTrxpfl | string | LSDP_Base/Checkpoints/Bronce/trxpfl/ | Ruta relativa para el schemaLocation del AutoLoader de trxpfl |
| rutaCheckpointBlncfl | string | LSDP_Base/Checkpoints/Bronce/blncfl/ | Ruta relativa para el schemaLocation del AutoLoader de blncfl |
| nombreScopeSecret | string | sc-kv-laboratorio | Nombre del Scope Secret de Databricks (RF-016) |

### Parametros Leidos de Azure SQL (dbo.Parametros)

| Clave | Ejemplo | Uso |
|-------|---------|-----|
| catalogoBronce | bronce_dev | Para referenciar el catalogo de bronce (si se necesita fuera del default) |
| contenedorBronce | bronce | Componente de la ruta abfss:// |
| datalake | adlsg2datalakedev | Componente de la ruta abfss:// |
| DirectorioBronce | archivos | Componente de la ruta abfss:// |

### Construccion de Rutas abfss://

**Patron de ruta para parquets** (RF-005):
```
abfss://{contenedorBronce}@{datalake}.dfs.core.windows.net/{DirectorioBronce}/{rutaCompleta}
```

**Patron de ruta para checkpoints/schemaLocation** (R7-D1):
```
abfss://{contenedorBronce}@{datalake}.dfs.core.windows.net/{DirectorioBronce}/{rutaCheckpoint}
```

---

## Propiedades Delta Estandar

Aplicables a las 3 tablas streaming de bronce (R1-D5, RF-009):

```python
table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}
```

El Liquid Cluster se configura via parametro `cluster_by` en el decorador (no en table_properties).

---

## Diagrama de Flujo de Datos

```
Azure SQL (dbo.Parametros)
  |
  | spark.read.format("jdbc") [nivel modulo, una sola vez]
  |
  v
+---------------------------+      Parametros Pipeline
| Variables Python (closure)|  <-- (rutas relativas, scope secret,
| catalogoBronce            |       rutas checkpoint)
| contenedorBronce          |
| datalake                  |
| DirectorioBronce          |
+---------------------------+
  |
  | Construye rutas abfss://
  |
  v
+-----------------------------------------+
| AutoLoader (cloudFiles)                 |
|  .option("cloudFiles.format", "parquet")|
|  .option("schemaEvolutionMode",         |
|          "addNewColumns")               |
|  .option("schemaLocation",              |
|          ruta_abfss_checkpoint)         |
|  .load(ruta_abfss_parquets)            |
+-----------------------------------------+
  |
  | .withColumn("FechaIngestaDatos",
  |             current_timestamp())
  |
  v
+-------------------+  +-------------------+  +-------------------+
| bronce_dev.       |  | bronce_dev.       |  | bronce_dev.       |
| regional.cmstfl   |  | regional.trxpfl   |  | regional.blncfl   |
| (72 cols)         |  | (62 cols)         |  | (102 cols)        |
| cluster_by:       |  | cluster_by:       |  | cluster_by:       |
| FechaIngestaDatos |  | TRXDT             |  | FechaIngestaDatos |
| CUSTID            |  | CUSTID            |  | CUSTID            |
|                   |  | TRXTYP            |  |                   |
+-------------------+  +-------------------+  +-------------------+
```

---

## Relaciones Entre Entidades

| Origen | Destino | Tipo | Clave de Join |
|--------|---------|------|---------------|
| cmstfl | trxpfl | 1:N | CUSTID |
| cmstfl | blncfl | 1:N | CUSTID |

**Nota**: En bronce, las relaciones son logicas (no se definen FK en Delta). Las relaciones fisicas se materializan en la medalla de plata (V4) mediante la consolidacion cliente+saldo y los joins con transaccional.

## Reglas de Validacion

En la medalla de bronce NO se aplican validaciones de calidad (@dp.expect). La validacion se delega a la medalla de plata (V4) segun la clarificacion aprobada. La unica verificacion es estructural: presencia del campo FechaIngestaDatos y propiedades Delta correctamente configuradas.

## Transiciones de Estado

Las tablas streaming de bronce son append-only (sin actualizaciones ni eliminaciones). No aplican transiciones de estado. El AutoLoader garantiza procesamiento incremental: solo archivos nuevos se procesan en cada ejecucion.
