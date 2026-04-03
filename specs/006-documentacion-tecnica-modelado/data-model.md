# Data Model: Documentacion Tecnica y Modelado de Datos (Version 6)

**Branch**: `006-documentacion-tecnica-modelado` | **Fecha**: 2026-04-01

## Resumen del Modelo

Este data-model describe las entidades que seran documentadas en ModeladoDatos.md y ManualTecnico.md. No se crean nuevas tablas ni vistas; el modelo representa la estructura de los 2 documentos Markdown y las entidades de datos existentes que seran referenciadas.

---

## Entidad 1: ManualTecnico.md (Entregable — HU1)

**Tipo**: Documento Markdown
**Ubicacion**: `docs/ManualTecnico.md`
**Estado**: Por crear en Version 6

### Estructura de Secciones

| Seccion | Nivel | Contenido Principal | RF |
|---------|-------|---------------------|----|
| Titulo y Descripcion General | h1 | Nombre del proyecto, proposito del pipeline LSDP | RF-002.1 |
| Dependencias de Infraestructura Azure | h2 | 6 recursos Azure requeridos (ADLS Gen2, Azure SQL, Key Vault, Databricks Premium, External Locations, Catalogos Unity Catalog) | RF-002.2 |
| Parametros | h2 | Tabla dbo.Parametros (6+ claves) y parametros del pipeline LSDP (nombreScopeSecret, esquema_plata, esquema_oro, rutas relativas) | RF-002.3 |
| Notebooks de Generacion de Parquets | h2 | NbGenerarMaestroCliente.py, NbGenerarTransaccionalCliente.py, NbGenerarSaldosCliente.py | RF-002.4, RF-011 |
| Utilidades LSDP | h2 | LsdpConexionAzureSql.py, LsdpConstructorRutasAbfss.py, LsdpReordenarColumnasLiquidCluster.py | RF-002.5 |
| Transformaciones — Medalla de Bronce | h2 | LsdpBronceCmstfl.py, LsdpBronceTrxpfl.py, LsdpBronceBlncfl.py | RF-002.6 |
| Transformaciones — Medalla de Plata | h2 | LsdpPlataClientesSaldos.py, LsdpPlataTransacciones.py | RF-002.7 |
| Transformaciones — Medalla de Oro | h2 | LsdpOroClientes.py (2 vistas materializadas) | RF-002.8 |
| Paradigma Declarativo LSDP | h2 | Decoradores, DAG, incrementalidad, closure, catalogos multiples | RF-002.9, RF-003 |
| Propiedades Delta | h2 | CDF, autoCompact, optimizeWrite, Liquid Cluster, retenciones | RF-002.10 |
| Estrategia de Pruebas TDD | h2 | Que se prueba por capa, estructura tests/, como ejecutar | RF-002.11 |

### Subsecciones de Parametros (RF-002.3)

| Clave dbo.Parametros | Tipo | Ejemplo | Descripcion |
|-----------------------|------|---------|-------------|
| catalogoBronce | string | bronce_dev | Nombre del catalogo Unity Catalog de bronce |
| contenedorBronce | string | bronce | Nombre del contenedor ADLS Gen2 de bronce |
| datalake | string | adlsg2datalakedev | Nombre del Storage Account |
| DirectorioBronce | string | archivos | Directorio raiz dentro del contenedor |
| catalogoPlata | string | plata_dev | Nombre del catalogo Unity Catalog de plata |
| catalogoOro | string | oro_dev | Nombre del catalogo Unity Catalog de oro |

| Parametro del Pipeline | Tipo | Ejemplo | Descripcion |
|------------------------|------|---------|-------------|
| nombreScopeSecret | string | sc-kv-laboratorio | Nombre del Scope Secret de Databricks asociado a Key Vault |
| esquema_plata | string | regional | Esquema de las vistas materializadas de plata |
| esquema_oro | string | regional | Esquema de las vistas materializadas de oro |
| rutaCompletaMaestroCliente | string | LSDP_Base/As400/MaestroCliente/ | Ruta relativa al parquet de maestro |
| rutaCompletaTransaccional | string | LSDP_Base/As400/Transaccional/ | Ruta relativa al parquet transaccional |
| rutaCompletaSaldoCliente | string | LSDP_Base/As400/SaldoCliente/ | Ruta relativa al parquet de saldos |
| rutaCheckpointCmstfl | string | LSDP_Base/Checkpoints/Bronce/cmstfl/ | Checkpoint de AutoLoader para cmstfl |
| rutaCheckpointTrxpfl | string | LSDP_Base/Checkpoints/Bronce/trxpfl/ | Checkpoint de AutoLoader para trxpfl |
| rutaCheckpointBlncfl | string | LSDP_Base/Checkpoints/Bronce/blncfl/ | Checkpoint de AutoLoader para blncfl |

---

## Entidad 2: ModeladoDatos.md (Entregable — HU2)

**Tipo**: Documento Markdown
**Ubicacion**: `docs/ModeladoDatos.md`
**Estado**: Por crear en Version 6

### Estructura de Secciones

| Seccion | Nivel | Contenido Principal | RF |
|---------|-------|---------------------|----|
| Titulo y Descripcion General | h1 | Proposito del modelado, alcance del medallion | RF-005.1 |
| Linaje de Datos | h2 | Diagrama ASCII + texto descriptivo del flujo Parquet->Bronce->Plata->Oro (sujeto a decision R21-D1) | RF-005.2 |
| Archivos Parquet — Fuente AS400 | h2 | 3 parquets: CMSTFL (70 campos), TRXPFL (60 campos), BLNCFL (100 campos) | RF-005.3 |
| Streaming Tables de Bronce | h2 | 3 tablas: cmstfl (72), trxpfl (62), blncfl (102) | RF-005.4 |
| Vistas Materializadas de Plata | h2 | 2 vistas: clientes_saldos_consolidados (175), transacciones_enriquecidas (65) | RF-005.5 |
| Vistas Materializadas de Oro | h2 | 2 vistas: comportamiento_atm_cliente (6), resumen_integral_cliente (22) | RF-005.6 |

---

## Entidades de Datos del Proyecto (Referencia para ModeladoDatos.md)

### Capa Fuente — Parquets AS400

#### CMSTFL — Maestro de Clientes (70 campos)

- **Archivo**: MaestroCliente (generado por NbGenerarMaestroCliente.py)
- **Volumetria**: 5 millones de registros base + 0.60% incremental
- **Clave primaria**: CUSTID
- **Campos representativos**: CUSTID, CUSTNM, FRSTNM, MDLNM, LSTNM, SCNDLN, GNDR, IDTYPE, IDNMBR, NATNLT, MRTLST, ADDR1, ADDR2, CITY, STATE, ZPCDE, CNTRY, PHONE1, PHONE2, EMAIL, OCCPTN, EMPLYR, EMPADS, BRNCOD, BRNNM, SGMNT, CSTCAT, RISKLV, PRDTYP, ACCTST, TXID, LGLLNM, MTHNM, FTHNM, CNTPRS, CNTPH, PREFNM, LANG, EDLVL, INCSRC, RELTYP, NTFPRF, BRTDT, OPNDT, LSTTRX, LSTUPD, CRTNDT, EXPDT, EMPSDT, LSTLGN, RVWDT, VLDDT, ENRLDT, CNCLDT, RJCTDT, PRMDT, CHGDT, LSTCDT, NXTRVW, BKRLDT, ANNLINC, MNTHINC, CRDSCR, DPNDNT, TTLPRD, FNCLYR, AGECST, YRBNKG, RSKSCR, NUMPHN
- **Total campos**: 70

#### TRXPFL — Transaccional (60 campos)

- **Archivo**: Transaccional (generado por NbGenerarTransaccionalCliente.py)
- **Volumetria**: 15 millones de registros por ejecucion
- **Clave foranea**: CUSTID (referencia a CMSTFL)
- **Campos representativos**: TRXID, CUSTID, TRXTYP, TRXDSC, CHNLCD, TRXSTS, CRNCOD, BRNCOD, ATMID, TRXDT, TRXTM, PRCDT, PRCTM, VLDT, STLDT, PSTDT, CRTDT, LSTUDT, AUTHDT, CNFRDT, EXPDT, RVRSDT, RCLDT, NTFDT, CLRDT, DSPDT, RSLTDT, BTCHDT, EFCDT, ARCDT, TRXAMT, ORGAMT, FEEAMT, TAXAMT, NETAMT, BLNBFR, BLNAFT, XCHGRT, CVTAMT, INTAMT, DSCAMT, PNLAMT, REFAMT, LIMAMT, AVLAMT, HLDAMT, OVRAMT, MINAMT, MAXAMT, AVGAMT, CSHREC, CSHGVN, TIPAMT, RNDAMT, SURCHG, INSAMT, ADJAMT, DLYACM, WKACM, MTHACM
- **Total campos**: 60

#### BLNCFL — Saldos de Clientes (100 campos)

- **Archivo**: SaldoCliente (generado por NbGenerarSaldosCliente.py)
- **Volumetria**: 1 registro por cliente (relacion 1:1 con CMSTFL)
- **Clave foranea**: CUSTID (referencia a CMSTFL)
- **Campos representativos**: CUSTID, ACCTID, ACCTTYP, ACCTNM, ACCTST, CRNCOD, BRNCOD, BRNNM, PRDCOD, PRDNM, PRDCAT, SGMNT, RISKLV, RGNCD, RGNNM, CSTGRP, BLKST, EMBST, OVDST, DGTST, CRDST, LNTYP, GRNTYP, PYMFRQ, INTTYP, TXCTG, CHKTYP, CRDGRP, CLSCD, SRCCD, AVLBAL, CURBAL, HLDBAL, OVRBAL, PNDBAL, AVGBAL, MINBAL, MAXBAL, OPNBAL, CLSBAL, INTACC, INTPAY, INTRCV, FEEACC, FEEPAY, CRDLMT, CRDAVL, CRDUSD, PYMAMT, PYMLST, TTLDBT, TTLCRD, TTLTRX, LNAMT, LNBAL, MTHPYM, INTRT, PNLRT, OVRRT, TAXAMT, INSAMT, DLYINT, YLDRT, SPRDRT, MRGAMT, OPNDT, CLSDT, LSTTRX, LSTPYM, NXTPYM, MATDT, RNWDT, RVWDT, CRTDT, UPDDT, STMDT, CUTDT, GRPDT, INTDT, FEEDT, BLKDT, EMBDT, OVDDT, PYMDT1, PYMDT2, PRJDT, ADJDT, RCLDT, NTFDT, CNCLDT, RCTDT, CHGDT, VRFDT, PRMDT, DGTDT, AUDT, MGRDT, ESCDT, RPTDT, ARCDT
- **Total campos**: 100

---

### Capa Bronce — Streaming Tables

#### bronce_dev.regional.cmstfl (72 campos)

- **Script**: LsdpBronceCmstfl.py
- **Decorador**: `@dp.table`
- **Fuente**: Parquet CMSTFL via AutoLoader (cloudFiles)
- **Columnas**: 70 AS400 + FechaIngestaDatos + _rescued_data
- **Liquid Cluster**: [FechaIngestaDatos, CUSTID]
- **Acumulacion**: Append-only historica (marca temporal FechaIngestaDatos)
- **Columnas adicionales respecto al parquet**:
  - FechaIngestaDatos (timestamp): marca de tiempo de la ingesta con current_timestamp()
  - _rescued_data (string): columna de AutoLoader que captura datos que no encajan en el esquema

#### bronce_dev.regional.trxpfl (62 campos)

- **Script**: LsdpBronceTrxpfl.py
- **Decorador**: `@dp.table`
- **Fuente**: Parquet TRXPFL via AutoLoader (cloudFiles)
- **Columnas**: 60 AS400 + FechaIngestaDatos + _rescued_data
- **Liquid Cluster**: [TRXDT, CUSTID, TRXTYP]
- **Acumulacion**: Append-only historica

#### bronce_dev.regional.blncfl (102 campos)

- **Script**: LsdpBronceBlncfl.py
- **Decorador**: `@dp.table`
- **Fuente**: Parquet BLNCFL via AutoLoader (cloudFiles)
- **Columnas**: 100 AS400 + FechaIngestaDatos + _rescued_data
- **Liquid Cluster**: [FechaIngestaDatos, CUSTID]
- **Acumulacion**: Append-only historica

---

### Capa Plata — Vistas Materializadas

#### {catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados (175 campos)

- **Script**: LsdpPlataClientesSaldos.py
- **Decorador**: `@dp.materialized_view`
- **Fuentes**: cmstfl (71 campos) LEFT JOIN blncfl (100 campos) por CUSTID
- **Expectativa**: `@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")`
- **Liquid Cluster**: [huella_identificacion_cliente, identificador_cliente]
- **Dimension Tipo 1**: Window function por CUSTID con Row_Number desc(FechaIngestaDatos), filtro rn == 1
- **Columnas**: 71 de cmstfl renombradas a espanol + 100 de blncfl renombradas a espanol + 4 calculados

**Campos calculados (4)**:

| Campo Calculado | Tipo | Logica |
|-----------------|------|--------|
| clasificacion_riesgo_cliente | string | CASE secuencial: RIESGO_CRITICO (nivel_riesgo_cliente IN (04,05) AND puntaje_crediticio < 500 AND estado_mora = SI), RIESGO_ALTO (nivel IN (03,04,05) AND puntaje < 600 AND mora = SI), RIESGO_MEDIO (nivel IN (03,04,05) OR (puntaje < 650 AND mora = SI)), RIESGO_BAJO (nivel = 02 OR puntaje < 700), SIN_RIESGO (otherwise) |
| categoria_saldo_disponible | string | CASE secuencial: SALDO_PREMIUM (saldo_disponible >= 200000 AND limite_credito >= 100000 AND segmento = VIP), SALDO_ALTO (saldo >= 100000 AND limite >= 50000 AND segmento IN (VIP,PREM)), SALDO_MEDIO (saldo >= 25000 AND limite >= 10000), SALDO_BAJO (saldo >= 5000 OR (limite >= 5000 AND segmento IN (STD,BAS))), SALDO_CRITICO (otherwise) |
| perfil_actividad_bancaria | string | CASE secuencial con variable intermedia dias_sin_transaccion = datediff(current_date(), fecha_ultima_transaccion): CLIENTE_INTEGRAL (dias <= 30 AND total_productos >= 8 AND estado_cuenta = AC), ACTIVIDAD_ALTA (dias <= 90 AND productos >= 5 AND estado = AC), ACTIVIDAD_MEDIA (dias <= 180 AND productos >= 3 AND estado = AC), ACTIVIDAD_BAJA (dias <= 365 AND productos >= 1 AND estado IN (AC,IN)), INACTIVO (otherwise) |
| huella_identificacion_cliente | string | SHA2-256 del campo identificador_cliente casteado a string: sha2(col("identificador_cliente").cast("string"), 256) |

#### {catalogoPlata}.{esquema_plata}.transacciones_enriquecidas (65 campos)

- **Script**: LsdpPlataTransacciones.py
- **Decorador**: `@dp.materialized_view`
- **Fuente**: trxpfl (lectura batch sin filtros — LSDP gestiona incrementalidad)
- **Liquid Cluster**: [fecha_transaccion, identificador_cliente, tipo_transaccion]
- **Columnas**: 61 de trxpfl renombradas a espanol + 4 calculados

**Campos calculados (4)**:

| Campo Calculado | Tipo | Logica |
|-----------------|------|--------|
| monto_neto_comisiones | double | coalesce(monto_neto, 0) - (coalesce(monto_comision, 0) + coalesce(monto_impuesto, 0)). Campos: NETAMT - (FEEAMT + TAXAMT) |
| porcentaje_comision_sobre_monto | double | when(monto_original IS NULL OR monto_original == 0, 0.0).otherwise((coalesce(monto_comision, 0) / monto_original) * 100). Campos: FEEAMT / ORGAMT * 100 |
| variacion_saldo_transaccion | double | abs(coalesce(saldo_despues_transaccion, 0) - coalesce(saldo_antes_transaccion, 0)). Campos: abs(BLNAFT - BLNBFR) |
| indicador_impacto_financiero | double | coalesce(monto_transaccion, 0) + coalesce(monto_comision, 0) + coalesce(monto_impuesto, 0) + coalesce(monto_penalidad, 0). Campos: TRXAMT + FEEAMT + TAXAMT + PNLAMT |

---

### Capa Oro — Vistas Materializadas

#### {catalogoOro}.{esquema_oro}.comportamiento_atm_cliente (6 campos)

- **Script**: LsdpOroClientes.py
- **Decorador**: `@dp.materialized_view`
- **Fuente**: transacciones_enriquecidas de plata (lectura batch sin filtros)
- **Expectativa**: `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")`
- **Liquid Cluster**: [identificador_cliente]
- **Agrupacion**: groupBy("identificador_cliente")

| Campo | Tipo | Logica de Agregacion |
|-------|------|---------------------|
| identificador_cliente | string | Clave de agrupacion (groupBy) |
| cantidad_depositos_atm | long | count(when(tipo_transaccion == "CATM", 1)). Retorna 0 si no hay transacciones CATM |
| cantidad_retiros_atm | long | count(when(tipo_transaccion == "DATM", 1)). Retorna 0 si no hay transacciones DATM |
| promedio_monto_depositos_atm | double | coalesce(avg(when(tipo_transaccion == "CATM", coalesce(monto_transaccion, 0))), 0). Doble coalesce: interno para nulos de monto, externo para clientes sin CATM |
| promedio_monto_retiros_atm | double | coalesce(avg(when(tipo_transaccion == "DATM", coalesce(monto_transaccion, 0))), 0). Doble coalesce: interno para nulos de monto, externo para clientes sin DATM |
| total_pagos_saldo_cliente | double | coalesce(spark_sum(when(tipo_transaccion == "PGSL", coalesce(monto_transaccion, 0))), 0). Doble coalesce para nulos |

#### {catalogoOro}.{esquema_oro}.resumen_integral_cliente (22 campos)

- **Script**: LsdpOroClientes.py
- **Decorador**: `@dp.materialized_view`
- **Fuentes**: clientes_saldos_consolidados (17 campos seleccionados) LEFT JOIN comportamiento_atm_cliente (5 metricas) por identificador_cliente
- **Liquid Cluster**: [huella_identificacion_cliente, identificador_cliente]
- **Dependencia DAG**: LSDP resuelve automaticamente — comportamiento_atm_cliente se procesa primero

| Campo | Origen | Tipo | Descripcion |
|-------|--------|------|-------------|
| identificador_cliente | Plata | string | CUSTID renombrado, clave de JOIN |
| huella_identificacion_cliente | Plata | string | SHA2-256 del identificador_cliente |
| nombre_completo_cliente | Plata | string | CUSTNM renombrado |
| tipo_documento_identidad | Plata | string | IDTYPE renombrado |
| numero_documento_identidad | Plata | string | IDNMBR renombrado |
| segmento_cliente | Plata | string | SGMNT renombrado |
| categoria_cliente | Plata | string | CSTCAT renombrado |
| ciudad_residencia | Plata | string | CITY renombrado |
| pais_residencia | Plata | string | CNTRY renombrado |
| clasificacion_riesgo_cliente | Plata | string | Campo calculado CASE de plata |
| categoria_saldo_disponible | Plata | string | Campo calculado CASE de plata |
| perfil_actividad_bancaria | Plata | string | Campo calculado CASE de plata |
| saldo_disponible | Plata | double | AVLBAL renombrado |
| saldo_actual | Plata | double | CURBAL renombrado |
| limite_credito | Plata | double | CRDLMT renombrado |
| puntaje_crediticio | Plata | double | CRDSCR renombrado |
| ingreso_anual_declarado | Plata | double | ANNLINC renombrado |
| cantidad_depositos_atm | Oro (ATM) | long | coalesce a 0 para clientes sin transacciones ATM |
| cantidad_retiros_atm | Oro (ATM) | long | coalesce a 0 para clientes sin transacciones ATM |
| promedio_monto_depositos_atm | Oro (ATM) | double | coalesce a 0.0 para clientes sin transacciones ATM |
| promedio_monto_retiros_atm | Oro (ATM) | double | coalesce a 0.0 para clientes sin transacciones ATM |
| total_pagos_saldo_cliente | Oro (ATM) | double | coalesce a 0.0 para clientes sin transacciones PGSL |

---

## Relaciones entre Entidades

```
CMSTFL (70)  ──[CUSTID]──> cmstfl (72) ──[CUSTID]──┐
                                                     ├──[LEFT JOIN CUSTID]──> clientes_saldos_consolidados (175)
BLNCFL (100) ──[CUSTID]──> blncfl (102) ──[CUSTID]──┘                              │
                                                                                    │ (17 cols seleccionadas)
                                                                                    ▼
                                                                        resumen_integral_cliente (22)
                                                                                    ▲
TRXPFL (60)  ──[CUSTID]──> trxpfl (62) ──────────> transacciones_enriquecidas (65)  │ (5 metricas)
                                                            │                       │
                                                            └──> comportamiento_atm_cliente (6) ──┘
```

**Clave de relacion universal**: CUSTID (identificador de cliente) — renombrado a `identificador_cliente` a partir de plata.
