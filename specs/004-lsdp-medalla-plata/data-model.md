# Modelo de Datos — Versión 4: LSDP Medalla de Plata

> **Feature:** `004-lsdp-medalla-plata` | **Fecha:** 2026-03-30 | **Base:** Decisiones R1‑D1 a R11‑D1

---

## Índice

1. [Fuentes de Bronce](#1-fuentes-de-bronce)
2. [Vistas Materializadas de Plata](#2-vistas-materializadas-de-plata)
3. [Parámetros del Pipeline](#3-parámetros-del-pipeline)
4. [Propiedades Delta Estándar](#4-propiedades-delta-estándar)
5. [Diagrama de Flujo de Datos](#5-diagrama-de-flujo-de-datos)

---

## 1. Fuentes de Bronce

### 1.1 Tabla `dbo.Parametros` (Azure SQL) — Extensión V4

La tabla de parámetros en Azure SQL contiene pares clave/valor que el pipeline consume a través de la función `leer_parametros_azure_sql()`.

| # | Clave | Valor (ejemplo) | Versión |
|---|-------|------------------|---------|
| 1 | `catalogoBronce` | `"bronce_dev"` | V3 (existente) |
| 2 | `contenedorBronce` | `"bronce"` | V3 (existente) |
| 3 | `datalake` | `"stlakedev"` | V3 (existente) |
| 4 | `DirectorioBronce` | `"/mnt/bronce"` | V3 (existente) |
| 5 | `catalogoPlata` | `"plata_dev"` | **V4 (nuevo)** |

> **Nota (RF-019):** La función `leer_parametros_azure_sql()` retorna **todas** las claves sin filtro. El código consumidor selecciona las claves que necesita del diccionario resultante.

### 1.2 Tablas Fuente de Bronce (resumen)

Las tres tablas streaming de bronce que alimentan la capa de plata son:

| Tabla Bronce | Esquema | Columnas Totales | Desglose |
|---|---|---|---|
| `bronce_dev.regional.cmstfl` | Maestro de clientes | 72 | 70 AS400 + `FechaIngestaDatos` + `_rescued_data` |
| `bronce_dev.regional.trxpfl` | Transacciones | 62 | 60 AS400 + `FechaIngestaDatos` + `_rescued_data` |
| `bronce_dev.regional.blncfl` | Saldos | 102 | 100 AS400 + `FechaIngestaDatos` + `_rescued_data` |

> **Nota importante:** Las columnas de partición (`año`, `mes`, `dia`) derivadas de la evaluación perezosa (lazy evaluation) en bronce **se excluyen** de la capa de plata.

---

## 2. Vistas Materializadas de Plata

### 2.1 Vista 1: `{catalogoPlata}.{esquema_plata}.clientes_saldos_consolidados`

| Propiedad | Valor |
|---|---|
| **Tipo** | Vista Materializada (`@dp.materialized_view`) |
| **Fuente** | `LEFT JOIN` de `cmstfl` hacia `blncfl` sobre `CUSTID` (último registro por `CUSTID` vía window function) |
| **Comportamiento** | Dimensión Tipo 1 (siempre el dato más reciente por cliente) |
| **Expectativa** | `@dp.expect_or_drop` para registros con `CUSTID` nulo (RF-018) |
| **Liquid Cluster** | `["huella_identificacion_cliente", "identificador_cliente"]` |
| **Total de columnas** | **175** (71 cmstfl + 100 blncfl − CUSTID + 4 calculados) |

#### Estrategia de deduplicación

Se aplica una función de ventana (`Window`) por cada tabla fuente para obtener el registro más reciente por `CUSTID`:

```python
window_spec = Window.partitionBy("CUSTID").orderBy(col("FechaIngestaDatos").desc())
# Se agrega row_number y se filtra row_number == 1
```

Posteriormente se ejecuta el `LEFT JOIN`:

```sql
cmstfl_latest LEFT JOIN blncfl_latest ON cmstfl.CUSTID = blncfl.CUSTID
```

---

#### TABLA A — Columnas provenientes de `cmstfl` (71 columnas)

| # | Campo Bronce | Nombre Plata (snake_case español) | Tipo | Origen | Descripción |
|---|---|---|---|---|---|
| 1 | `CUSTID` | `identificador_cliente` | `LongType` | cmstfl | Identificador único del cliente en el sistema AS400 |
| 2 | `CUSTNM` | `nombre_completo_cliente` | `StringType` | cmstfl | Nombre completo del cliente tal como aparece en el maestro |
| 3 | `FRSTNM` | `primer_nombre` | `StringType` | cmstfl | Primer nombre del cliente |
| 4 | `MDLNM` | `segundo_nombre` | `StringType` | cmstfl | Segundo nombre del cliente |
| 5 | `LSTNM` | `primer_apellido` | `StringType` | cmstfl | Primer apellido del cliente |
| 6 | `SCNDLN` | `segundo_apellido` | `StringType` | cmstfl | Segundo apellido del cliente |
| 7 | `GNDR` | `genero` | `StringType` | cmstfl | Género del cliente |
| 8 | `IDTYPE` | `tipo_documento_identidad` | `StringType` | cmstfl | Tipo de documento de identidad del cliente |
| 9 | `IDNMBR` | `numero_documento_identidad` | `StringType` | cmstfl | Número de documento de identidad del cliente |
| 10 | `NATNLT` | `codigo_nacionalidad` | `StringType` | cmstfl | Código de nacionalidad del cliente |
| 11 | `MRTLST` | `estado_civil` | `StringType` | cmstfl | Estado civil del cliente |
| 12 | `ADDR1` | `direccion_linea_uno` | `StringType` | cmstfl | Primera línea de la dirección de residencia |
| 13 | `ADDR2` | `direccion_linea_dos` | `StringType` | cmstfl | Segunda línea de la dirección de residencia |
| 14 | `CITY` | `ciudad_residencia` | `StringType` | cmstfl | Ciudad de residencia del cliente |
| 15 | `STATE` | `estado_provincia` | `StringType` | cmstfl | Estado o provincia de residencia del cliente |
| 16 | `ZPCDE` | `codigo_postal` | `StringType` | cmstfl | Código postal de la dirección del cliente |
| 17 | `CNTRY` | `pais_residencia` | `StringType` | cmstfl | País de residencia del cliente |
| 18 | `PHONE1` | `telefono_principal` | `StringType` | cmstfl | Número de teléfono principal del cliente |
| 19 | `PHONE2` | `telefono_secundario` | `StringType` | cmstfl | Número de teléfono secundario del cliente |
| 20 | `EMAIL` | `correo_electronico` | `StringType` | cmstfl | Dirección de correo electrónico del cliente |
| 21 | `OCCPTN` | `ocupacion_profesion` | `StringType` | cmstfl | Ocupación o profesión del cliente |
| 22 | `EMPLYR` | `nombre_empleador` | `StringType` | cmstfl | Nombre del empleador del cliente |
| 23 | `EMPADS` | `direccion_empleador` | `StringType` | cmstfl | Dirección del empleador del cliente |
| 24 | `BRNCOD` | `codigo_sucursal_cliente` | `StringType` | cmstfl | Código de la sucursal asignada al cliente |
| 25 | `BRNNM` | `nombre_sucursal_cliente` | `StringType` | cmstfl | Nombre de la sucursal asignada al cliente |
| 26 | `SGMNT` | `segmento_cliente` | `StringType` | cmstfl | Segmento comercial del cliente (VIP/PREM/STD/BAS) |
| 27 | `CSTCAT` | `categoria_cliente` | `StringType` | cmstfl | Categoría asignada al cliente |
| 28 | `RISKLV` | `nivel_riesgo_cliente` | `StringType` | cmstfl | Nivel de riesgo del cliente (valores 01 a 05) |
| 29 | `PRDTYP` | `tipo_producto_principal` | `StringType` | cmstfl | Tipo de producto principal contratado por el cliente |
| 30 | `ACCTST` | `estado_cuenta_cliente` | `StringType` | cmstfl | Estado de la cuenta del cliente (AC/IN/CL/SU) |
| 31 | `TXID` | `identificador_fiscal` | `StringType` | cmstfl | Identificador fiscal del cliente |
| 32 | `LGLLNM` | `nombre_legal_completo` | `StringType` | cmstfl | Nombre legal completo del cliente |
| 33 | `MTHNM` | `nombre_madre` | `StringType` | cmstfl | Nombre de la madre del cliente |
| 34 | `FTHNM` | `nombre_padre` | `StringType` | cmstfl | Nombre del padre del cliente |
| 35 | `CNTPRS` | `persona_contacto_emergencia` | `StringType` | cmstfl | Persona de contacto de emergencia del cliente |
| 36 | `CNTPH` | `telefono_contacto_emergencia` | `StringType` | cmstfl | Teléfono del contacto de emergencia |
| 37 | `PREFNM` | `nombre_preferido` | `StringType` | cmstfl | Nombre preferido por el cliente |
| 38 | `LANG` | `idioma_preferido` | `StringType` | cmstfl | Idioma de preferencia del cliente |
| 39 | `EDLVL` | `nivel_educativo` | `StringType` | cmstfl | Nivel educativo del cliente |
| 40 | `INCSRC` | `fuente_ingresos` | `StringType` | cmstfl | Fuente principal de ingresos del cliente |
| 41 | `RELTYP` | `tipo_relacion_bancaria` | `StringType` | cmstfl | Tipo de relación bancaria del cliente |
| 42 | `NTFPRF` | `preferencia_notificacion` | `StringType` | cmstfl | Preferencia de notificación del cliente |
| 43 | `BRTDT` | `fecha_nacimiento` | `DateType` | cmstfl | Fecha de nacimiento del cliente |
| 44 | `OPNDT` | `fecha_apertura_cuenta` | `DateType` | cmstfl | Fecha de apertura de la cuenta en el maestro |
| 45 | `LSTTRX` | `fecha_ultima_transaccion` | `DateType` | cmstfl | Fecha de la última transacción registrada en el maestro |
| 46 | `LSTUPD` | `fecha_ultima_actualizacion` | `DateType` | cmstfl | Fecha de la última actualización del registro maestro |
| 47 | `CRTNDT` | `fecha_creacion_registro` | `DateType` | cmstfl | Fecha de creación del registro en el maestro |
| 48 | `EXPDT` | `fecha_vencimiento_documento` | `DateType` | cmstfl | Fecha de vencimiento del documento de identidad |
| 49 | `EMPSDT` | `fecha_inicio_empleo` | `DateType` | cmstfl | Fecha de inicio de empleo del cliente |
| 50 | `LSTLGN` | `fecha_ultimo_login_digital` | `DateType` | cmstfl | Fecha del último inicio de sesión en canales digitales |
| 51 | `RVWDT` | `fecha_revision_kyc` | `DateType` | cmstfl | Fecha de la última revisión KYC del cliente |
| 52 | `VLDDT` | `fecha_validacion_datos` | `DateType` | cmstfl | Fecha de validación de datos del cliente |
| 53 | `ENRLDT` | `fecha_enrolamiento_digital` | `DateType` | cmstfl | Fecha de enrolamiento en canales digitales |
| 54 | `CNCLDT` | `fecha_cancelacion_maestro` | `DateType` | cmstfl | Fecha de cancelación del registro en el maestro |
| 55 | `RJCTDT` | `fecha_rechazo` | `DateType` | cmstfl | Fecha de rechazo del cliente |
| 56 | `PRMDT` | `fecha_promocion_segmento` | `DateType` | cmstfl | Fecha de promoción de segmento del cliente |
| 57 | `CHGDT` | `fecha_cambio_estado` | `DateType` | cmstfl | Fecha del último cambio de estado del cliente |
| 58 | `LSTCDT` | `fecha_ultimo_contacto` | `DateType` | cmstfl | Fecha del último contacto con el cliente |
| 59 | `NXTRVW` | `fecha_proxima_revision` | `DateType` | cmstfl | Fecha de la próxima revisión programada |
| 60 | `BKRLDT` | `fecha_relacion_banco` | `DateType` | cmstfl | Fecha de inicio de la relación bancaria |
| 61 | `ANNLINC` | `ingreso_anual_declarado` | `DoubleType` | cmstfl | Ingreso anual declarado por el cliente |
| 62 | `MNTHINC` | `ingreso_mensual` | `DoubleType` | cmstfl | Ingreso mensual del cliente |
| 63 | `CRDSCR` | `puntaje_crediticio` | `LongType` | cmstfl | Puntaje crediticio del cliente (rango 300–850) |
| 64 | `DPNDNT` | `numero_dependientes` | `LongType` | cmstfl | Número de dependientes del cliente |
| 65 | `TTLPRD` | `total_productos_contratados` | `LongType` | cmstfl | Total de productos contratados por el cliente (rango 1–15) |
| 66 | `FNCLYR` | `anio_fiscal_vigente` | `LongType` | cmstfl | Año fiscal vigente del registro |
| 67 | `AGECST` | `edad_cliente` | `LongType` | cmstfl | Edad del cliente en años |
| 68 | `YRBNKG` | `anios_relacion_bancaria` | `LongType` | cmstfl | Cantidad de años de relación bancaria del cliente |
| 69 | `RSKSCR` | `score_riesgo_calculado` | `DoubleType` | cmstfl | Score de riesgo calculado del cliente |
| 70 | `NUMPHN` | `cantidad_telefonos_registrados` | `LongType` | cmstfl | Cantidad de teléfonos registrados por el cliente |
| 71 | `FechaIngestaDatos` | `fecha_ingesta_maestro` | `TimestampType` | cmstfl | Fecha y hora de la ingesta del registro del maestro |

---

#### TABLA B — Columnas provenientes de `blncfl` (100 columnas — se excluye `CUSTID` por ser clave de JOIN)

| # | Campo Bronce | Nombre Plata (snake_case español) | Tipo | Origen | Descripción |
|---|---|---|---|---|---|
| 1 | `ACCTID` | `identificador_cuenta` | `StringType` | blncfl | Identificador único de la cuenta |
| 2 | `ACCTTYP` | `tipo_cuenta` | `StringType` | blncfl | Tipo de cuenta (AHRO/CRTE/PRES/INVR) |
| 3 | `ACCTNM` | `nombre_cuenta` | `StringType` | blncfl | Nombre descriptivo de la cuenta |
| 4 | `ACCTST` | `estado_cuenta_saldo` | `StringType` | blncfl | Estado de la cuenta en la tabla de saldos |
| 5 | `CRNCOD` | `codigo_moneda_cuenta` | `StringType` | blncfl | Código de la moneda de la cuenta |
| 6 | `BRNCOD` | `codigo_sucursal_cuenta` | `StringType` | blncfl | Código de la sucursal de la cuenta |
| 7 | `BRNNM` | `nombre_sucursal_cuenta` | `StringType` | blncfl | Nombre de la sucursal de la cuenta |
| 8 | `PRDCOD` | `codigo_producto` | `StringType` | blncfl | Código del producto financiero |
| 9 | `PRDNM` | `nombre_producto` | `StringType` | blncfl | Nombre del producto financiero |
| 10 | `PRDCAT` | `categoria_producto` | `StringType` | blncfl | Categoría del producto financiero |
| 11 | `SGMNT` | `segmento_cuenta` | `StringType` | blncfl | Segmento de la cuenta |
| 12 | `RISKLV` | `nivel_riesgo_cuenta` | `StringType` | blncfl | Nivel de riesgo asignado a la cuenta |
| 13 | `RGNCD` | `codigo_region` | `StringType` | blncfl | Código de la región geográfica |
| 14 | `RGNNM` | `nombre_region` | `StringType` | blncfl | Nombre de la región geográfica |
| 15 | `CSTGRP` | `grupo_cliente` | `StringType` | blncfl | Grupo al que pertenece el cliente |
| 16 | `BLKST` | `estado_bloqueo` | `StringType` | blncfl | Estado de bloqueo de la cuenta |
| 17 | `EMBST` | `estado_embargo` | `StringType` | blncfl | Estado de embargo de la cuenta |
| 18 | `OVDST` | `estado_mora` | `StringType` | blncfl | Estado de mora de la cuenta (NO/SI) |
| 19 | `DGTST` | `estado_digital` | `StringType` | blncfl | Estado de habilitación digital de la cuenta |
| 20 | `CRDST` | `estado_crediticio` | `StringType` | blncfl | Estado crediticio de la cuenta |
| 21 | `LNTYP` | `tipo_linea_credito` | `StringType` | blncfl | Tipo de línea de crédito asociada |
| 22 | `GRNTYP` | `tipo_garantia` | `StringType` | blncfl | Tipo de garantía de la cuenta |
| 23 | `PYMFRQ` | `frecuencia_pago` | `StringType` | blncfl | Frecuencia de pago de la cuenta |
| 24 | `INTTYP` | `tipo_tasa_interes` | `StringType` | blncfl | Tipo de tasa de interés (FIJ/VAR/MIX) |
| 25 | `TXCTG` | `categoria_fiscal` | `StringType` | blncfl | Categoría fiscal de la cuenta |
| 26 | `CHKTYP` | `tipo_chequera` | `StringType` | blncfl | Tipo de chequera asociada |
| 27 | `CRDGRP` | `grupo_crediticio` | `StringType` | blncfl | Grupo crediticio de la cuenta |
| 28 | `CLSCD` | `codigo_clasificacion` | `StringType` | blncfl | Código de clasificación de la cuenta |
| 29 | `SRCCD` | `codigo_fuente` | `StringType` | blncfl | Código de la fuente del registro |
| 30 | `AVLBAL` | `saldo_disponible` | `DoubleType` | blncfl | Saldo disponible de la cuenta (rango 0–500000) |
| 31 | `CURBAL` | `saldo_actual` | `DoubleType` | blncfl | Saldo actual de la cuenta |
| 32 | `HLDBAL` | `saldo_retenido` | `DoubleType` | blncfl | Saldo retenido de la cuenta |
| 33 | `OVRBAL` | `saldo_sobregiro` | `DoubleType` | blncfl | Saldo en sobregiro de la cuenta |
| 34 | `PNDBAL` | `saldo_pendiente` | `DoubleType` | blncfl | Saldo pendiente de la cuenta |
| 35 | `AVGBAL` | `saldo_promedio_periodo` | `DoubleType` | blncfl | Saldo promedio del período |
| 36 | `MINBAL` | `saldo_minimo_periodo` | `DoubleType` | blncfl | Saldo mínimo del período |
| 37 | `MAXBAL` | `saldo_maximo_periodo` | `DoubleType` | blncfl | Saldo máximo del período |
| 38 | `OPNBAL` | `saldo_apertura_periodo` | `DoubleType` | blncfl | Saldo de apertura del período |
| 39 | `CLSBAL` | `saldo_cierre_periodo` | `DoubleType` | blncfl | Saldo de cierre del período |
| 40 | `INTACC` | `intereses_acumulados` | `DoubleType` | blncfl | Intereses acumulados de la cuenta |
| 41 | `INTPAY` | `intereses_pagados` | `DoubleType` | blncfl | Intereses pagados en el período |
| 42 | `INTRCV` | `intereses_recibidos` | `DoubleType` | blncfl | Intereses recibidos en el período |
| 43 | `FEEACC` | `comisiones_acumuladas` | `DoubleType` | blncfl | Comisiones acumuladas de la cuenta |
| 44 | `FEEPAY` | `comisiones_pagadas` | `DoubleType` | blncfl | Comisiones pagadas en el período |
| 45 | `CRDLMT` | `limite_credito` | `DoubleType` | blncfl | Límite de crédito de la cuenta (variable) |
| 46 | `CRDAVL` | `credito_disponible` | `DoubleType` | blncfl | Crédito disponible de la cuenta |
| 47 | `CRDUSD` | `credito_utilizado` | `DoubleType` | blncfl | Crédito utilizado de la cuenta |
| 48 | `PYMAMT` | `monto_pago_minimo` | `DoubleType` | blncfl | Monto del pago mínimo requerido |
| 49 | `PYMLST` | `ultimo_pago_realizado` | `DoubleType` | blncfl | Monto del último pago realizado |
| 50 | `TTLDBT` | `total_debitos_periodo` | `DoubleType` | blncfl | Total de débitos en el período |
| 51 | `TTLCRD` | `total_creditos_periodo` | `DoubleType` | blncfl | Total de créditos en el período |
| 52 | `TTLTRX` | `total_transacciones_periodo` | `LongType` | blncfl | Total de transacciones en el período |
| 53 | `LNAMT` | `monto_prestamo` | `DoubleType` | blncfl | Monto del préstamo asociado |
| 54 | `LNBAL` | `saldo_prestamo` | `DoubleType` | blncfl | Saldo vigente del préstamo |
| 55 | `MTHPYM` | `pago_mensual` | `DoubleType` | blncfl | Monto del pago mensual |
| 56 | `INTRT` | `tasa_interes_vigente` | `DoubleType` | blncfl | Tasa de interés vigente de la cuenta |
| 57 | `PNLRT` | `tasa_penalidad` | `DoubleType` | blncfl | Tasa de penalidad aplicable |
| 58 | `OVRRT` | `tasa_sobregiro` | `DoubleType` | blncfl | Tasa de sobregiro aplicable |
| 59 | `TAXAMT` | `impuestos_acumulados` | `DoubleType` | blncfl | Impuestos acumulados de la cuenta |
| 60 | `INSAMT` | `seguros_acumulados` | `DoubleType` | blncfl | Seguros acumulados de la cuenta |
| 61 | `DLYINT` | `interes_diario` | `DoubleType` | blncfl | Interés diario calculado |
| 62 | `YLDRT` | `tasa_rendimiento` | `DoubleType` | blncfl | Tasa de rendimiento de la cuenta |
| 63 | `SPRDRT` | `spread_tasa` | `DoubleType` | blncfl | Spread de tasa aplicado |
| 64 | `MRGAMT` | `monto_margen` | `DoubleType` | blncfl | Monto de margen de la cuenta |
| 65 | `OPNDT` | `fecha_apertura_cuenta_saldo` | `DateType` | blncfl | Fecha de apertura de la cuenta en la tabla de saldos |
| 66 | `CLSDT` | `fecha_cierre_cuenta` | `DateType` | blncfl | Fecha de cierre de la cuenta |
| 67 | `LSTTRX` | `fecha_ultima_transaccion_saldo` | `DateType` | blncfl | Fecha de la última transacción registrada en saldos |
| 68 | `LSTPYM` | `fecha_ultimo_pago` | `DateType` | blncfl | Fecha del último pago realizado |
| 69 | `NXTPYM` | `fecha_proximo_pago` | `DateType` | blncfl | Fecha del próximo pago programado |
| 70 | `MATDT` | `fecha_vencimiento_cuenta` | `DateType` | blncfl | Fecha de vencimiento de la cuenta |
| 71 | `RNWDT` | `fecha_renovacion` | `DateType` | blncfl | Fecha de renovación de la cuenta |
| 72 | `RVWDT` | `fecha_revision_cuenta` | `DateType` | blncfl | Fecha de revisión de la cuenta |
| 73 | `CRTDT` | `fecha_creacion_registro_saldo` | `DateType` | blncfl | Fecha de creación del registro en la tabla de saldos |
| 74 | `UPDDT` | `fecha_actualizacion_saldo` | `DateType` | blncfl | Fecha de última actualización del registro de saldo |
| 75 | `STMDT` | `fecha_estado_cuenta` | `DateType` | blncfl | Fecha del último estado de cuenta emitido |
| 76 | `CUTDT` | `fecha_corte` | `DateType` | blncfl | Fecha de corte de la cuenta |
| 77 | `GRPDT` | `fecha_periodo_gracia` | `DateType` | blncfl | Fecha de inicio del período de gracia |
| 78 | `INTDT` | `fecha_calculo_intereses` | `DateType` | blncfl | Fecha del cálculo de intereses |
| 79 | `FEEDT` | `fecha_cobro_comisiones` | `DateType` | blncfl | Fecha de cobro de comisiones |
| 80 | `BLKDT` | `fecha_bloqueo` | `DateType` | blncfl | Fecha de bloqueo de la cuenta |
| 81 | `EMBDT` | `fecha_embargo` | `DateType` | blncfl | Fecha de embargo de la cuenta |
| 82 | `OVDDT` | `fecha_inicio_mora` | `DateType` | blncfl | Fecha de inicio de la mora |
| 83 | `PYMDT1` | `fecha_primer_pago` | `DateType` | blncfl | Fecha del primer pago registrado |
| 84 | `PYMDT2` | `fecha_segundo_pago` | `DateType` | blncfl | Fecha del segundo pago registrado |
| 85 | `PRJDT` | `fecha_proyeccion` | `DateType` | blncfl | Fecha de proyección financiera |
| 86 | `ADJDT` | `fecha_ajuste` | `DateType` | blncfl | Fecha del último ajuste realizado |
| 87 | `RCLDT` | `fecha_reconciliacion` | `DateType` | blncfl | Fecha de reconciliación de la cuenta |
| 88 | `NTFDT` | `fecha_notificacion_cuenta` | `DateType` | blncfl | Fecha de la última notificación enviada |
| 89 | `CNCLDT` | `fecha_cancelacion_cuenta` | `DateType` | blncfl | Fecha de cancelación de la cuenta |
| 90 | `RCTDT` | `fecha_reactivacion` | `DateType` | blncfl | Fecha de reactivación de la cuenta |
| 91 | `CHGDT` | `fecha_cambio_condiciones` | `DateType` | blncfl | Fecha del último cambio de condiciones |
| 92 | `VRFDT` | `fecha_verificacion` | `DateType` | blncfl | Fecha de verificación de la cuenta |
| 93 | `PRMDT` | `fecha_promocion_cuenta` | `DateType` | blncfl | Fecha de promoción de la cuenta |
| 94 | `DGTDT` | `fecha_acceso_digital` | `DateType` | blncfl | Fecha de último acceso digital |
| 95 | `AUDT` | `fecha_auditoria` | `DateType` | blncfl | Fecha de la última auditoría |
| 96 | `MGRDT` | `fecha_migracion` | `DateType` | blncfl | Fecha de migración de la cuenta |
| 97 | `ESCDT` | `fecha_escalamiento` | `DateType` | blncfl | Fecha de escalamiento de la cuenta |
| 98 | `RPTDT` | `fecha_reporte_regulatorio` | `DateType` | blncfl | Fecha del último reporte regulatorio |
| 99 | `ARCDT` | `fecha_archivado_cuenta` | `DateType` | blncfl | Fecha de archivado de la cuenta |
| 100 | `FechaIngestaDatos` | `fecha_ingesta_saldo` | `TimestampType` | blncfl | Fecha y hora de la ingesta del registro de saldo |

> **Nota:** La columna `CUSTID` de `blncfl` **no se incluye** en la vista materializada porque es la clave de JOIN y ya se obtiene de `cmstfl`.

---

#### TABLA C — Campos calculados de `clientes_saldos_consolidados` (4 columnas)

| # | Nombre Plata | Tipo | Fuentes | Fórmula / Lógica | Descripción |
|---|---|---|---|---|---|
| 1 | `huella_identificacion_cliente` | `StringType` | `CUSTID` (cmstfl) | `sha2(col("CUSTID").cast("string"), 256)` | Huella digital SHA-256 del identificador del cliente. Utilizada como parte del Liquid Cluster |
| 2 | `clasificacion_riesgo_cliente` | `StringType` | `RISKLV` (cmstfl), `CRDSCR` (cmstfl), `OVDST` (blncfl) | CASE secuencial (ver detalle abajo) | Clasificación calculada del nivel de riesgo del cliente |
| 3 | `categoria_saldo_disponible` | `StringType` | `AVLBAL` (blncfl), `CRDLMT` (blncfl), `SGMNT` (cmstfl) | CASE secuencial (ver detalle abajo) | Categorización del saldo disponible del cliente |
| 4 | `perfil_actividad_bancaria` | `StringType` | `LSTTRX` (cmstfl), `TTLPRD` (cmstfl), `ACCTST` (cmstfl) | CASE secuencial con variable intermedia (ver detalle abajo) | Perfil de actividad bancaria del cliente |

##### Detalle del campo `clasificacion_riesgo_cliente`

Lógica CASE secuencial evaluada en orden de prioridad:

| Orden | Resultado | Condición |
|---|---|---|
| 1 | `"RIESGO_CRITICO"` | `RISKLV IN ("04","05") AND CRDSCR < 500 AND OVDST == "SI"` |
| 2 | `"RIESGO_ALTO"` | `RISKLV IN ("03","04","05") AND CRDSCR < 600 AND OVDST == "SI"` |
| 3 | `"RIESGO_MEDIO"` | `RISKLV IN ("03","04","05") OR (CRDSCR < 650 AND OVDST == "SI")` |
| 4 | `"RIESGO_BAJO"` | `RISKLV == "02" OR CRDSCR < 700` |
| 5 | `"SIN_RIESGO"` | En cualquier otro caso (`otherwise`) |

##### Detalle del campo `categoria_saldo_disponible`

Lógica CASE secuencial evaluada en orden de prioridad:

| Orden | Resultado | Condición |
|---|---|---|
| 1 | `"SALDO_PREMIUM"` | `AVLBAL >= 200000 AND CRDLMT >= 100000 AND SGMNT == "VIP"` |
| 2 | `"SALDO_ALTO"` | `AVLBAL >= 100000 AND CRDLMT >= 50000 AND SGMNT IN ("VIP","PREM")` |
| 3 | `"SALDO_MEDIO"` | `AVLBAL >= 25000 AND CRDLMT >= 10000` |
| 4 | `"SALDO_BAJO"` | `AVLBAL >= 5000 OR (CRDLMT >= 5000 AND SGMNT IN ("STD","BAS"))` |
| 5 | `"SALDO_CRITICO"` | En cualquier otro caso (`otherwise`) |

##### Detalle del campo `perfil_actividad_bancaria`

Variable intermedia:

```python
dias_sin_transaccion = datediff(current_date(), col("LSTTRX"))
```

Lógica CASE secuencial evaluada en orden de prioridad:

| Orden | Resultado | Condición |
|---|---|---|
| 1 | `"CLIENTE_INTEGRAL"` | `dias_sin_transaccion <= 30 AND TTLPRD >= 8 AND ACCTST == "AC"` |
| 2 | `"ACTIVIDAD_ALTA"` | `dias_sin_transaccion <= 90 AND TTLPRD >= 5 AND ACCTST == "AC"` |
| 3 | `"ACTIVIDAD_MEDIA"` | `dias_sin_transaccion <= 180 AND TTLPRD >= 3 AND ACCTST == "AC"` |
| 4 | `"ACTIVIDAD_BAJA"` | `dias_sin_transaccion <= 365 AND TTLPRD >= 1 AND ACCTST IN ("AC","IN")` |
| 5 | `"INACTIVO"` | En cualquier otro caso (`otherwise`) |

---

**Resumen de columnas — `clientes_saldos_consolidados`:**

| Componente | Cantidad |
|---|---|
| Columnas de `cmstfl` | 71 |
| Columnas de `blncfl` (sin `CUSTID`) | 100 |
| Campos calculados | 4 |
| **Total** | **175** |

---

### 2.2 Vista 2: `{catalogoPlata}.{esquema_plata}.transacciones_enriquecidas`

| Propiedad | Valor |
|---|---|
| **Tipo** | Vista Materializada (`@dp.materialized_view`) |
| **Fuente** | `bronce_dev.regional.trxpfl` (lectura batch, sin filtros) |
| **Comportamiento** | Vista materializada (LSDP gestiona la actualización automáticamente) |
| **Liquid Cluster** | `["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]` |
| **Total de columnas** | **65** (61 trxpfl + 4 calculados) |

---

#### TABLA D — Columnas provenientes de `trxpfl` (61 columnas)

| # | Campo Bronce | Nombre Plata (snake_case español) | Tipo | Origen | Descripción |
|---|---|---|---|---|---|
| 1 | `TRXID` | `identificador_transaccion` | `LongType` | trxpfl | Identificador único de la transacción |
| 2 | `CUSTID` | `identificador_cliente` | `LongType` | trxpfl | Identificador del cliente asociado a la transacción |
| 3 | `TRXTYP` | `tipo_transaccion` | `StringType` | trxpfl | Tipo de transacción realizada |
| 4 | `TRXDSC` | `descripcion_transaccion` | `StringType` | trxpfl | Descripción textual de la transacción |
| 5 | `CHNLCD` | `canal_transaccion` | `StringType` | trxpfl | Canal por el cual se realizó la transacción |
| 6 | `TRXSTS` | `estado_transaccion` | `StringType` | trxpfl | Estado actual de la transacción |
| 7 | `CRNCOD` | `codigo_moneda` | `StringType` | trxpfl | Código de la moneda de la transacción |
| 8 | `BRNCOD` | `codigo_sucursal` | `StringType` | trxpfl | Código de la sucursal donde se realizó la transacción |
| 9 | `ATMID` | `identificador_atm` | `StringType` | trxpfl | Identificador del cajero automático (si aplica) |
| 10 | `TRXDT` | `fecha_transaccion` | `DateType` | trxpfl | Fecha de la transacción |
| 11 | `TRXTM` | `fecha_hora_transaccion` | `TimestampType` | trxpfl | Fecha y hora exacta de la transacción |
| 12 | `PRCDT` | `fecha_procesamiento` | `DateType` | trxpfl | Fecha de procesamiento de la transacción |
| 13 | `PRCTM` | `fecha_hora_procesamiento` | `TimestampType` | trxpfl | Fecha y hora de procesamiento de la transacción |
| 14 | `VLDT` | `fecha_valor` | `DateType` | trxpfl | Fecha valor de la transacción |
| 15 | `STLDT` | `fecha_liquidacion` | `DateType` | trxpfl | Fecha de liquidación de la transacción |
| 16 | `PSTDT` | `fecha_contabilizacion` | `DateType` | trxpfl | Fecha de contabilización de la transacción |
| 17 | `CRTDT` | `fecha_creacion_registro` | `DateType` | trxpfl | Fecha de creación del registro de la transacción |
| 18 | `LSTUDT` | `fecha_ultima_actualizacion` | `DateType` | trxpfl | Fecha de la última actualización del registro |
| 19 | `AUTHDT` | `fecha_autorizacion` | `DateType` | trxpfl | Fecha de autorización de la transacción |
| 20 | `CNFRDT` | `fecha_confirmacion` | `DateType` | trxpfl | Fecha de confirmación de la transacción |
| 21 | `EXPDT` | `fecha_expiracion` | `DateType` | trxpfl | Fecha de expiración de la transacción |
| 22 | `RVRSDT` | `fecha_reverso` | `DateType` | trxpfl | Fecha del reverso de la transacción (si aplica) |
| 23 | `RCLDT` | `fecha_reconciliacion` | `DateType` | trxpfl | Fecha de reconciliación de la transacción |
| 24 | `NTFDT` | `fecha_notificacion` | `DateType` | trxpfl | Fecha de notificación de la transacción |
| 25 | `CLRDT` | `fecha_compensacion` | `DateType` | trxpfl | Fecha de compensación de la transacción |
| 26 | `DSPDT` | `fecha_disputa` | `DateType` | trxpfl | Fecha de disputa de la transacción (si aplica) |
| 27 | `RSLTDT` | `fecha_resolucion` | `DateType` | trxpfl | Fecha de resolución de la transacción |
| 28 | `BTCHDT` | `fecha_lote_procesamiento` | `DateType` | trxpfl | Fecha del lote de procesamiento |
| 29 | `EFCDT` | `fecha_efectiva` | `DateType` | trxpfl | Fecha efectiva de la transacción |
| 30 | `ARCDT` | `fecha_archivado` | `DateType` | trxpfl | Fecha de archivado de la transacción |
| 31 | `TRXAMT` | `monto_transaccion` | `DoubleType` | trxpfl | Monto de la transacción |
| 32 | `ORGAMT` | `monto_original` | `DoubleType` | trxpfl | Monto original de la transacción |
| 33 | `FEEAMT` | `monto_comision` | `DoubleType` | trxpfl | Monto de la comisión aplicada |
| 34 | `TAXAMT` | `monto_impuesto` | `DoubleType` | trxpfl | Monto del impuesto aplicado |
| 35 | `NETAMT` | `monto_neto` | `DoubleType` | trxpfl | Monto neto de la transacción |
| 36 | `BLNBFR` | `saldo_antes_transaccion` | `DoubleType` | trxpfl | Saldo de la cuenta antes de la transacción |
| 37 | `BLNAFT` | `saldo_despues_transaccion` | `DoubleType` | trxpfl | Saldo de la cuenta después de la transacción |
| 38 | `XCHGRT` | `tasa_cambio` | `DoubleType` | trxpfl | Tasa de cambio aplicada a la transacción |
| 39 | `CVTAMT` | `monto_convertido` | `DoubleType` | trxpfl | Monto convertido a la moneda de referencia |
| 40 | `INTAMT` | `monto_intereses` | `DoubleType` | trxpfl | Monto de intereses asociado a la transacción |
| 41 | `DSCAMT` | `monto_descuento` | `DoubleType` | trxpfl | Monto de descuento aplicado |
| 42 | `PNLAMT` | `monto_penalidad` | `DoubleType` | trxpfl | Monto de penalidad aplicada |
| 43 | `REFAMT` | `monto_referencia` | `DoubleType` | trxpfl | Monto de referencia de la transacción |
| 44 | `LIMAMT` | `limite_permitido` | `DoubleType` | trxpfl | Límite permitido para la transacción |
| 45 | `AVLAMT` | `monto_disponible` | `DoubleType` | trxpfl | Monto disponible al momento de la transacción |
| 46 | `HLDAMT` | `monto_retenido` | `DoubleType` | trxpfl | Monto retenido de la transacción |
| 47 | `OVRAMT` | `monto_sobregiro` | `DoubleType` | trxpfl | Monto de sobregiro generado |
| 48 | `MINAMT` | `monto_minimo_requerido` | `DoubleType` | trxpfl | Monto mínimo requerido para la transacción |
| 49 | `MAXAMT` | `monto_maximo_permitido` | `DoubleType` | trxpfl | Monto máximo permitido para la transacción |
| 50 | `AVGAMT` | `monto_promedio_diario` | `DoubleType` | trxpfl | Monto promedio diario al momento de la transacción |
| 51 | `CSHREC` | `monto_efectivo_recibido` | `DoubleType` | trxpfl | Monto en efectivo recibido |
| 52 | `CSHGVN` | `monto_efectivo_entregado` | `DoubleType` | trxpfl | Monto en efectivo entregado |
| 53 | `TIPAMT` | `monto_propina` | `DoubleType` | trxpfl | Monto de propina asociada |
| 54 | `RNDAMT` | `monto_redondeo` | `DoubleType` | trxpfl | Monto de redondeo aplicado |
| 55 | `SURCHG` | `recargo_adicional` | `DoubleType` | trxpfl | Recargo adicional aplicado a la transacción |
| 56 | `INSAMT` | `monto_seguro` | `DoubleType` | trxpfl | Monto de seguro asociado a la transacción |
| 57 | `ADJAMT` | `monto_ajuste` | `DoubleType` | trxpfl | Monto de ajuste aplicado |
| 58 | `DLYACM` | `acumulado_diario` | `DoubleType` | trxpfl | Acumulado diario de transacciones |
| 59 | `WKACM` | `acumulado_semanal` | `DoubleType` | trxpfl | Acumulado semanal de transacciones |
| 60 | `MTHACM` | `acumulado_mensual` | `DoubleType` | trxpfl | Acumulado mensual de transacciones |
| 61 | `FechaIngestaDatos` | `fecha_ingesta_datos` | `TimestampType` | trxpfl | Fecha y hora de la ingesta del registro transaccional |

---

#### TABLA E — Campos calculados de `transacciones_enriquecidas` (4 columnas)

| # | Nombre Plata | Tipo | Fuentes | Fórmula / Lógica | Manejo de nulos | Descripción |
|---|---|---|---|---|---|---|
| 1 | `monto_neto_comisiones` | `DoubleType` | `NETAMT`, `FEEAMT`, `TAXAMT` | `NETAMT - (FEEAMT + TAXAMT)` | `coalesce` a 0 para cada operando | Monto neto después de descontar comisiones e impuestos |
| 2 | `porcentaje_comision_sobre_monto` | `DoubleType` | `FEEAMT`, `ORGAMT` | `(FEEAMT / ORGAMT) * 100` | Cuando `ORGAMT == 0` o `ORGAMT IS NULL` → resultado 0 | Porcentaje que representa la comisión sobre el monto original |
| 3 | `variacion_saldo_transaccion` | `DoubleType` | `BLNAFT`, `BLNBFR` | `abs(BLNAFT - BLNBFR)` | `coalesce` a 0 para cada operando | Variación absoluta del saldo generada por la transacción |
| 4 | `indicador_impacto_financiero` | `DoubleType` | `TRXAMT`, `FEEAMT`, `TAXAMT`, `PNLAMT` | `TRXAMT + FEEAMT + TAXAMT + PNLAMT` | `coalesce` a 0 para cada operando | Indicador del impacto financiero total de la transacción |

---

**Resumen de columnas — `transacciones_enriquecidas`:**

| Componente | Cantidad |
|---|---|
| Columnas de `trxpfl` | 61 |
| Campos calculados | 4 |
| **Total** | **65** |

---

## 3. Parámetros del Pipeline

### 3.1 Parámetros existentes V3 (sin cambio)

Estos parámetros fueron definidos en la versión 3 (medalla de bronce) y se mantienen sin modificación:

| Parámetro | Tipo | Descripción |
|---|---|---|
| `rutaCompletaMaestroCliente` | `string` | Ruta ABFSS completa al directorio Parquet del maestro de clientes |
| `rutaCompletaSaldoCliente` | `string` | Ruta ABFSS completa al directorio Parquet de saldos de clientes |
| `rutaCompletaTransaccional` | `string` | Ruta ABFSS completa al directorio Parquet de transacciones |
| `rutaCheckpointCmstfl` | `string` | Ruta de checkpoint para streaming de cmstfl |
| `rutaCheckpointTrxpfl` | `string` | Ruta de checkpoint para streaming de trxpfl |
| `rutaCheckpointBlncfl` | `string` | Ruta de checkpoint para streaming de blncfl |
| `nombreScopeSecret` | `string` | Nombre del scope de Databricks Secrets para credenciales de Azure SQL |

### 3.2 Parámetro nuevo V4

| Parámetro | Tipo | Ejemplo | Descripción |
|---|---|---|---|
| `esquema_plata` | `string` | `"regional"` | Esquema de Unity Catalog para las vistas materializadas de plata |

### 3.3 Parámetros de Azure SQL (vía `leer_parametros_azure_sql` refactorizada)

Valores obtenidos de la tabla `dbo.Parametros` en Azure SQL:

| Clave | Ejemplo | Versión | Descripción |
|---|---|---|---|
| `catalogoBronce` | `"bronce_dev"` | V3 (existente) | Catálogo de Unity Catalog para las tablas de bronce |
| `contenedorBronce` | `"bronce"` | V3 (existente) | Contenedor de Azure Data Lake para bronce |
| `datalake` | `"stlakedev"` | V3 (existente) | Nombre de la cuenta de Azure Data Lake Storage |
| `DirectorioBronce` | `"/mnt/bronce"` | V3 (existente) | Directorio base de bronce |
| `catalogoPlata` | `"plata_dev"` | **V4 (nuevo)** | Catálogo de Unity Catalog para las vistas de plata |

> **Nota (RF-019):** La función `leer_parametros_azure_sql()` retorna **todas** las claves sin filtro. El código consumidor selecciona las claves que necesita del diccionario resultante.

---

## 4. Propiedades Delta Estándar

Según el requisito **RF-008**, todas las vistas materializadas de plata se crean con las siguientes propiedades Delta:

```python
table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days"
}
```

Adicionalmente, cada decorador `@dp.materialized_view` incluye el parámetro `cluster_by` diferenciado por vista:

| Vista Materializada | `cluster_by` |
|---|---|
| `clientes_saldos_consolidados` | `["huella_identificacion_cliente", "identificador_cliente"]` |
| `transacciones_enriquecidas` | `["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]` |

---

## 5. Diagrama de Flujo de Datos

```
┌─────────────────────────────┐   ┌──────────────────────────────┐
│  dbo.Parametros (Azure SQL) │   │     Pipeline Parameters      │
│  ┌────────────────────────┐ │   │  ┌────────────────────────┐  │
│  │ catalogoBronce         │ │   │  │ esquema_plata          │  │
│  │ contenedorBronce       │ │   │  │ nombreScopeSecret      │  │
│  │ datalake               │ │   │  │ rutaCheckpoint*        │  │
│  │ DirectorioBronce       │ │   │  │ rutaCompleta*          │  │
│  │ catalogoPlata (NUEVO)  │ │   │  └────────────────────────┘  │
│  └────────────────────────┘ │   └──────────────┬───────────────┘
└──────────────┬──────────────┘                  │
               │                                 │
               ▼                                 ▼
   leer_parametros_azure_sql()           spark.conf.get()
               │                                 │
               ├── catalogo_plata ──────────────►│
               │                                 │
               ▼                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                    FUENTES DE BRONCE                             │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  bronce_dev.regional.cmstfl (72 cols)                           │
│         │                                                        │
│         ▼                                                        │
│  Window(CUSTID, desc FechaIngestaDatos) ──► cmstfl_latest       │
│                                                   │              │
│  bronce_dev.regional.blncfl (102 cols)            │              │
│         │                                         │              │
│         ▼                                         │              │
│  Window(CUSTID, desc FechaIngestaDatos) ──► blncfl_latest       │
│                                                   │              │
│                                        LEFT JOIN on CUSTID       │
│                                                   │              │
│                                                   ▼              │
│                              ┌─────────────────────────────────┐ │
│                              │ clientes_saldos_consolidados    │ │
│                              │ (175 columnas)                  │ │
│                              │ @dp.materialized_view           │ │
│                              │ @dp.expect_or_drop(CUSTID NULL) │ │
│                              │ cluster_by: [huella_...,        │ │
│                              │   identificador_cliente]        │ │
│                              └─────────────────────────────────┘ │
│                                                                  │
│  bronce_dev.regional.trxpfl (62 cols)                           │
│         │                                                        │
│         ▼                                                        │
│  spark.read.table() ──► renombrar + campos calculados           │
│                                   │                              │
│                                   ▼                              │
│                  ┌─────────────────────────────────┐             │
│                  │ transacciones_enriquecidas       │             │
│                  │ (65 columnas)                    │             │
│                  │ @dp.materialized_view            │             │
│                  │ cluster_by: [fecha_transaccion,  │             │
│                  │   identificador_cliente,         │             │
│                  │   tipo_transaccion]              │             │
│                  └─────────────────────────────────┘             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

**Resumen general del modelo de datos:**

| Vista Materializada | Fuentes | Columnas | Liquid Cluster |
|---|---|---|---|
| `clientes_saldos_consolidados` | cmstfl + blncfl | 175 | `huella_identificacion_cliente`, `identificador_cliente` |
| `transacciones_enriquecidas` | trxpfl | 65 | `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion` |
| **Total** | **3 tablas bronce** | **240** | — |
