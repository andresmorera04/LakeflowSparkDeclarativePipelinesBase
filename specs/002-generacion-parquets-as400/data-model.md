# Modelo de Datos - Version 2: Generacion de Parquets Simulando Data AS400

**Feature**: 002-generacion-parquets-as400
**Fecha**: 2026-03-28
**Estado**: COMPLETA
**Base**: Estructuras aprobadas en V1 (decisiones R2-D1 a R2-D4, R3-D1)

---

## Entidades del Dominio

> **Nota**: Las estructuras campo por campo de E1, E2 y E3 estan definidas y aprobadas en el research de V1 (specs/001-research-inicial-v1/research.md). Los hallazgos H2.1, H2.2, H2.3 y H2.4 contienen las tablas completas de campos. Esta seccion documenta el **mapeo de tipos AS400 a PySpark StructType** para la generacion de parquets.

### E1: Parquet Maestro de Clientes (CMSTFL)

| Atributo | Tipo AS400 | Tipo PySpark (StructType) | Estrategia de Generacion |
|----------|-----------|---------------------------|--------------------------|
| CUSTID | NUMERIC(15,0) | LongType() | Secuencial con offset parametrizable (RF-018). Primera ejecucion: offset inicial (ej: 100000001). Re-ejecucion: max(CUSTID)+1 para nuevos. |
| 42 campos textuales CHAR | CHAR(n) | StringType() | Valores aleatorios contextuales. FRSTNM, MDLNM, LSTNM, SCNDLN, CUSTNM, MTHNM, FTHNM, CNTPRS, PREFNM usan catalogos R3-D1 exclusivamente. Otros campos (ADDR1, CITY, STATE, etc.) generados con datos realistas. |
| 18 campos fecha DATE | DATE | DateType() | Fechas aleatorias dentro de rangos realistas (OPNDT: ultimos 30 anos, LSTACT: ultimos 2 anos, BIRTHD: 18-85 anos atras, etc.) |
| 10 campos numericos NUMERIC | NUMERIC(p,s) | DoubleType() o LongType() | DoubleType para campos con decimales (CRDLMT, AVLBAL, MNTHLY, LNBLNC, INSAMT). LongType para enteros puros (DPNDNT, YRSADR). Rangos monetarios parametrizables via widgets (rango_credlmt_*, rango_avlbal_*, rango_income_*, rango_loan_*, rango_ins_*). |

**Volumetria**:
- Primera ejecucion: 5,000,000 registros (RF-004)
- Re-ejecucion: mismos 5M clientes + 0.60% nuevos (30,000 base)
- Mutacion: 20% de registros existentes, los siguientes 15 campos demograficos (ADDR1, ADDR2, CITY, STATE, ZPCDE, PHONE1, PHONE2, EMAIL, MRTLST, OCCPTN, EMPLYR, EMPADS, SGMNT, RISKLV, RSKSCR)
- Reparticion: controlada via widget `num_particiones` (valor por defecto: 20)

**Campos con Restriccion de Catalogo (R3-D1)**:
| Campo | Fuente | Descripcion |
|-------|--------|-------------|
| FRSTNM | 300 nombres (hebreos/egipcios/ingleses) | Primer nombre |
| MDLNM | 300 nombres (hebreos/egipcios/ingleses) | Segundo nombre |
| LSTNM | 150 apellidos (hebreos/egipcios/ingleses) | Primer apellido |
| SCNDLN | 150 apellidos (hebreos/egipcios/ingleses) | Segundo apellido |
| CUSTNM | Concatenacion FRSTNM + LSTNM | Nombre completo cliente |
| MTHNM | 300 nombres (hebreos/egipcios/ingleses) | Nombre de la madre |
| FTHNM | 300 nombres (hebreos/egipcios/ingleses) | Nombre del padre |
| CNTPRS | 300 nombres + 150 apellidos | Persona de contacto |
| PREFNM | 300 nombres (hebreos/egipcios/ingleses) | Nombre preferido |

### E2: Parquet Transaccional (TRXPFL)

| Atributo | Tipo AS400 | Tipo PySpark (StructType) | Estrategia de Generacion |
|----------|-----------|---------------------------|--------------------------|
| TRXID | NUMERIC(18,0) | LongType() | Prefijo fecha + secuencial (RF-018). Formato: YYYYMMDD + 000000001. Unicidad garantizada entre ejecuciones. |
| CUSTID | NUMERIC(15,0) | LongType() | Seleccion aleatoria de CUSTIDs existentes en Maestro de Clientes (RF-009). Un cliente puede tener 0 o N transacciones. |
| TRXTYP | CHAR(4) | StringType() | Seleccion ponderada del catalogo de 15 tipos (R2-D3, RF-008). Distribucion: ~60% comunes, ~30% media, ~10% poco comunes. |
| TRXDT | DATE | DateType() | Fecha fija recibida via parametro en formato YYYY-MM-DD (RF-007). Igual para todos los registros de la ejecucion. |
| TRXTM | TIMESTAMP | TimestampType() | Hora, minuto y segundo simulados aleatoriamente (RF-007). |
| 30 campos numericos NUMERIC | NUMERIC(p,s) | DoubleType() | Montos con rangos segmentados por tipo de transaccion (RF-017). |
| 21 campos fecha DATE/TIMESTAMP | DATE/TIMESTAMP | DateType() / TimestampType() | Fechas/timestamps contextuales. |
| 9 campos textuales CHAR | CHAR(n) | StringType() | Codigos y descripciones bancarias. |

**Volumetria**:
- Cada ejecucion: 15,000,000 registros nuevos (no acumulativos) (RF-007)
- TRXID unico entre ejecuciones gracias al prefijo de fecha
- Reparticion: controlada via widget `num_particiones` (valor por defecto: 50)

**Catalogo de Tipos de Transaccion (R2-D3)**:
| Codigo | Nombre | Frecuencia | Rango Montos (RF-017) |
|--------|--------|------------|----------------------|
| CATM | Credito por ATM | Alta (~60%) | 10 - 1,000 |
| DATM | Debito por ATM | Alta (~60%) | 10 - 1,000 |
| CMPR | Compra POS | Alta (~60%) | 5 - 15,000 |
| TINT | Transferencia Interna | Alta (~60%) | 50 - 50,000 |
| DPST | Deposito en Sucursal | Alta (~60%) | 50 - 100,000 |
| PGSL | Pago al Saldo | Media (~30%) | 100 - 25,000 |
| TEXT | Transferencia Externa | Media (~30%) | 50 - 50,000 |
| RTRO | Retiro en Sucursal | Media (~30%) | 50 - 100,000 |
| PGSV | Pago de Servicios | Media (~30%) | 10 - 5,000 |
| NMNA | Deposito de Nomina | Media (~30%) | 1,000 - 15,000 |
| INTR | Interes Generado | Media (~30%) | 1 - 5,000 |
| ADSL | Adelanto Salarial | Baja (~10%) | 500 - 10,000 |
| IMPT | Impuesto Retenido | Baja (~10%) | 1 - 2,000 |
| DMCL | Domiciliacion | Baja (~10%) | 20 - 3,000 |
| CMSN | Comision Bancaria | Baja (~10%) | 1 - 500 |

### E3: Parquet Saldos de Clientes (BLNCFL)

| Atributo | Tipo AS400 | Tipo PySpark (StructType) | Estrategia de Generacion |
|----------|-----------|---------------------------|--------------------------|
| CUSTID | NUMERIC(15,0) | LongType() | Copia exacta de todos los CUSTIDs del Maestro de Clientes (RF-011). Relacion 1:1 estricta. |
| 30 campos textuales CHAR | CHAR(n) | StringType() | Codigos de cuenta, tipos de producto, estados, descripciones bancarias. |
| 35 campos numericos NUMERIC | NUMERIC(p,s) | DoubleType() | Saldos y montos con rangos segmentados por tipo de cuenta (RF-017): ahorro (0-500,000), corriente (0-250,000), credito (0-100,000), prestamo (1,000-1,000,000). |
| 35 campos fecha DATE | DATE | DateType() | Fechas de apertura, ultimo movimiento, vencimiento, etc. |

**Volumetria**:
- Cantidad = cantidad exacta de registros en Maestro de Clientes (RF-011)
- Primera ejecucion: 5,000,000 registros (1:1)
- Re-ejecucion: regeneracion completa con la cantidad actual de clientes
- Reparticion: controlada via widget `num_particiones` (valor por defecto: 20)

**Rangos de Montos por Tipo de Cuenta (RF-017)**:
| Tipo de Cuenta | Rango Minimo | Rango Maximo |
|---------------|-------------|-------------|
| Ahorro (AHRO) | 0 | 500,000 |
| Cuenta Corriente (CRTE) | 0 | 250,000 |
| Linea de Credito | 0 | 100,000 |
| Prestamo (PRES) | 1,000 | 1,000,000 |

### E4: Catalogos de Nombres No Latinos (R3-D1)

| Atributo | Tipo | Descripcion |
|----------|------|-------------|
| Origen | Enum(HEBREO, EGIPCIO, INGLES) | Cultura de origen del nombre |
| Tipo | Enum(NOMBRE, APELLIDO) | Tipo de catalogo |
| Valor | StringType() | El nombre o apellido en si |

**Totales**:
- 100 nombres hebreos + 50 apellidos hebreos
- 100 nombres egipcios + 50 apellidos egipcios
- 100 nombres ingleses + 50 apellidos ingleses
- **300 nombres + 150 apellidos = 450 entradas**
- Combinaciones unicas posibles: 300 x 150 = 45,000 (se permiten repeticiones para alcanzar 5M)

### E5: Catalogo de Tipos de Transaccion (R2-D3)

| Atributo | Tipo | Descripcion |
|----------|------|-------------|
| Codigo | StringType() | Codigo de 4 caracteres (CATM, DATM, etc.) |
| Nombre | StringType() | Descripcion legible del tipo |
| Categoria | Enum(ALTA, MEDIA, BAJA) | Banda de frecuencia |
| PesoDistribucion | DoubleType() | Peso parametrizable para distribucion ponderada |
| MontoMinimo | DoubleType() | Rango minimo del monto (parametrizable) |
| MontoMaximo | DoubleType() | Rango maximo del monto (parametrizable) |

---

## Relaciones Entre Entidades

```text
Parquet Maestro de Clientes (CUSTID) ──1:N──> Parquet Transaccional (CUSTID)
Parquet Maestro de Clientes (CUSTID) ──1:1──> Parquet Saldos (CUSTID)

[Notebooks Generadores]                    [Parquets en External Location (ADLS Gen2)]
┌──────────────────────────────────┐       ┌─────────────────────────────────────────┐
│ NbGenerarMaestroCliente.py       │       │ /{ruta_parametrizable}/maestro_clientes/ │
│ (scripts/GenerarParquets/)       │──────>│ 70 columnas, 5M+ registros              │
│ dbutils.widgets: ruta, cantidad, │       │ Snappy, StructType explicito             │
│ offset, pct_incremento           │       │ PK: CUSTID (LongType)                   │
└──────────────────────────────────┘       └──────────────────┬────────────────────────┘
                                                              │
                                            CUSTID (FK) ──────┤────── CUSTID (FK)
                                                              │                │
┌──────────────────────────────────┐       ┌──────────────────▼─────────────────────────┐
│ NbGenerarTransaccionalCliente.py │       │ /{ruta_parametrizable}/transaccional/       │
│ (scripts/GenerarParquets/)       │──────>│ 60 columnas, 15M registros por ejecucion   │
│ dbutils.widgets: ruta, fecha,    │       │ Snappy, StructType explicito                │
│ ruta_maestro, pesos              │       │ PK: TRXID (LongType), FK: CUSTID (LongType)│
└──────────────────────────────────┘       └────────────────────────────────────────────┘

┌──────────────────────────────────┐       ┌──────────────────────────────────────────────┐
│ NbGenerarSaldosCliente.py        │       │ /{ruta_parametrizable}/saldos_clientes/       │
│ (scripts/GenerarParquets/)       │──────>│ 100 columnas, N registros (N = count Maestro) │
│ dbutils.widgets: ruta,           │       │ Snappy, StructType explicito                  │
│ ruta_maestro                     │       │ FK: CUSTID (LongType), relacion 1:1           │
└──────────────────────────────────┘       └──────────────────────────────────────────────┘
```

### Flujo de Dependencias entre Notebooks

```text
                    ┌───────────────────┐
                    │ NbGenerarMaestro   │
                    │ Cliente.py         │
                    │                   │
                    │ Genera: CUSTID PK │
                    │ 5M+ registros     │
                    └────────┬──────────┘
                             │
                    CUSTID como entrada
                             │
              ┌──────────────┴──────────────┐
              │                              │
              ▼                              ▼
┌──────────────────────┐      ┌──────────────────────┐
│ NbGenerarTransaccional│      │ NbGenerarSaldos       │
│ Cliente.py            │      │ Cliente.py            │
│                      │      │                      │
│ Lee CUSTIDs del      │      │ Lee CUSTIDs del      │
│ Maestro existente    │      │ Maestro existente    │
│ 15M registros nuevos │      │ 1 registro por CUSTID│
└──────────────────────┘      └──────────────────────┘
```

**Orden de ejecucion obligatorio**: Maestro → (Transaccional | Saldos). Transaccional y Saldos pueden ejecutarse en cualquier orden despues de que Maestro exista, pero son independientes entre si.

---

## Mapeo de Tipos AS400 a PySpark StructType

| Tipo AS400 | Tipo PySpark | Uso | Notas |
|-----------|-------------|-----|-------|
| NUMERIC(p,0) donde p <= 18 | LongType() | CUSTID, TRXID, contadores enteros | Sin decimales. Cubre hasta 18 digitos. |
| NUMERIC(p,s) donde s > 0 | DoubleType() | Montos, saldos, tasas, porcentajes | Con decimales. Precision suficiente para montos bancarios. |
| CHAR(n) | StringType() | Nombres, direcciones, codigos, descripciones | Texto libre o de catalogo. |
| DATE | DateType() | Fechas sin hora (apertura, nacimiento, vencimiento) | Formato interno Spark. |
| TIMESTAMP | TimestampType() | Marca de tiempo completa (hora de transaccion) | Incluye hora, minuto, segundo. |

---

## Restricciones de Plataforma (Post-Implementacion)

### Computo Serverless — APIs Prohibidas (V2-R5-D1)

Los notebooks generadores usan `mapInPandas` para generar datos a escala. En Computo Serverless, `spark.sparkContext` esta **PROHIBIDO** (error `JVM_ATTRIBUTE_NOT_SUPPORTED`). La distribucion de datos de referencia (catalogos, rangos, pesos) a los workers se realiza mediante **closures Python** capturadas por `cloudpickle`, no mediante `broadcast()`.

**Patron implementado**:
```python
# Variables Python capturadas automaticamente por cloudpickle
datos = {"catalogos": list(...), "rangos": dict(...)}

def funcion_generadora(iterador):
    catalogos = datos["catalogos"]  # Closure — sin broadcast
    for pdf in iterador:
        yield pdf
```

### Protocolo de Almacenamiento Obligatorio (V2-R5-D2)

Todas las rutas de lectura/escritura de parquets deben usar `abfss://`:
- **Formato**: `abfss://container@storageaccount.dfs.core.windows.net/ruta/recurso`
- **Prohibido**: `/mnt/` (legacy DBFS mounts)

---

## Reglas de Validacion (V2 - Generacion de Parquets)

| Regla | Entidad | Descripcion | RF Asociado |
|-------|---------|-------------|-------------|
| RV-01 | Maestro | CUSTID unico y no nulo dentro del parquet generado | RF-018 |
| RV-02 | Maestro | FRSTNM, MDLNM, LSTNM, SCNDLN, CUSTNM, MTHNM, FTHNM, CNTPRS, PREFNM exclusivamente de catalogos R3-D1 (9 campos RF-005) | RF-005 |
| RV-03 | Maestro | Parquet con exactamente 70 columnas con nombres AS400 aprobados | RF-003 |
| RV-04 | Maestro | Primera ejecucion: exactamente 5,000,000 registros | RF-004 |
| RV-05 | Maestro | Re-ejecucion: mismos CUSTIDs + 0.60% nuevos | RF-004 |
| RV-06 | Maestro | Mutacion: 20% de registros, los siguientes 15 campos demograficos | RF-004 |
| RV-07 | Transaccional | TRXID unico y no nulo | RF-018 |
| RV-08 | Transaccional | Todos los CUSTID referenciados existen en Maestro | RF-009 |
| RV-09 | Transaccional | TRXTYP exclusivamente del catalogo de 15 tipos R2-D3 | RF-008 |
| RV-10 | Transaccional | Exactamente 15,000,000 registros por ejecucion | RF-007 |
| RV-11 | Transaccional | TRXDT = fecha del parametro para todos los registros | RF-007 |
| RV-12 | Transaccional | Parquet con exactamente 60 columnas | RF-006 |
| RV-13 | Transaccional | Montos dentro de rangos segmentados por tipo | RF-017 |
| RV-14 | Saldos | Relacion 1:1 estricta con Maestro (N registros = N clientes) | RF-011 |
| RV-15 | Saldos | Cero CUSTIDs huerfanos ni faltantes | RF-011 |
| RV-16 | Saldos | Parquet con exactamente 100 columnas | RF-010 |
| RV-17 | Saldos | Montos dentro de rangos segmentados por tipo de cuenta | RF-017 |
| RV-18 | Todos | Cero valores hardcodeados en el codigo fuente | RF-002 |
| RV-19 | Todos | Parametros validados al inicio de cada notebook | RF-016 |
| RV-20 | Todos | Esquema StructType explicito (no inferido) | V2-R3-D1 |
| RV-21 | Transaccional | Distribucion de TRXTYP por banda: ~60% alta, ~30% media, ~10% baja con tolerancia ±5 puntos porcentuales por banda | RF-008 || RV-22 | Todos | Cero uso de `spark.sparkContext` en el codigo fuente (compatibilidad Serverless) | V2-R5-D1 |
| RV-23 | Todos | Todas las rutas por defecto de widgets usan protocolo `abfss://` (cero rutas `/mnt/`) | V2-R5-D2 |
| RV-24 | Todos | Datos de referencia distribuidos a workers via closures Python (cloudpickle), no broadcast | V2-R5-D1 |
---

## Parametros por Notebook (dbutils.widgets.text)

### NbGenerarMaestroCliente.py

| Widget | Nombre | Valor por Defecto | Tipo Conversion | Descripcion |
|--------|--------|-------------------|-----------------|-------------|
| ruta_salida_parquet | ruta_salida_parquet | abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes | str | Ruta del parquet de salida en ADLS Gen2 (protocolo abfss:// obligatorio, V2-R5-D2) |
| cantidad_registros_base | cantidad_registros_base | 5000000 | int | Cantidad de registros en primera ejecucion |
| pct_incremento | pct_incremento | 0.006 | float | Porcentaje de clientes nuevos por re-ejecucion |
| pct_mutacion | pct_mutacion | 0.20 | float | Porcentaje de registros con mutacion |
| offset_custid | offset_custid | 100000001 | int | Offset inicial del CUSTID secuencial |
| ruta_parquet_existente | ruta_parquet_existente | (vacio) | str | Ruta del parquet previo para re-ejecucion (vacio = primera ejecucion) |
| num_particiones | num_particiones | 20 | int | Numero de particiones para repartition al escribir el parquet |
| rango_credlmt_min | rango_credlmt_min | 0 | float | Monto minimo limite de credito (CRDLMT) |
| rango_credlmt_max | rango_credlmt_max | 100000 | float | Monto maximo limite de credito (CRDLMT) |
| rango_avlbal_min | rango_avlbal_min | 0 | float | Monto minimo saldo disponible (AVLBAL) |
| rango_avlbal_max | rango_avlbal_max | 500000 | float | Monto maximo saldo disponible (AVLBAL) |
| rango_income_min | rango_income_min | 500 | float | Monto minimo ingreso mensual (MNTHLY) |
| rango_income_max | rango_income_max | 50000 | float | Monto maximo ingreso mensual (MNTHLY) |
| rango_loan_min | rango_loan_min | 0 | float | Monto minimo saldo prestamo (LNBLNC) |
| rango_loan_max | rango_loan_max | 1000000 | float | Monto maximo saldo prestamo (LNBLNC) |
| rango_ins_min | rango_ins_min | 0 | float | Monto minimo cobertura seguro (INSAMT) |
| rango_ins_max | rango_ins_max | 500000 | float | Monto maximo cobertura seguro (INSAMT) |

### NbGenerarTransaccionalCliente.py

| Widget | Nombre | Valor por Defecto | Tipo Conversion | Descripcion |
|--------|--------|-------------------|-----------------|-------------|
| ruta_salida_parquet | ruta_salida_parquet | abfss://container@storageaccount.dfs.core.windows.net/landing/transaccional | str | Ruta del parquet de salida (protocolo abfss:// obligatorio, V2-R5-D2) |
| ruta_maestro_clientes | ruta_maestro_clientes | abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes | str | Ruta del parquet del Maestro para leer CUSTIDs (protocolo abfss://) |
| cantidad_registros | cantidad_registros | 15000000 | int | Cantidad de transacciones a generar |
| fecha_transaccion | fecha_transaccion | 2026-01-01 | str (validar YYYY-MM-DD) | Fecha de todas las transacciones |
| offset_trxid | offset_trxid | 1 | int | Offset inicial del secuencial de TRXID (RF-018) |
| pesos_alta | pesos_alta | 60 | int | Peso porcentual de tipos de frecuencia alta |
| pesos_media | pesos_media | 30 | int | Peso porcentual de tipos de frecuencia media |
| pesos_baja | pesos_baja | 10 | int | Peso porcentual de tipos de frecuencia baja |
| rango_atm_min | rango_atm_min | 10 | float | Monto minimo ATM (CATM/DATM) |
| rango_atm_max | rango_atm_max | 1000 | float | Monto maximo ATM (CATM/DATM) |
| rango_transferencia_min | rango_transferencia_min | 50 | float | Monto minimo transferencias (TEXT/TINT) |
| rango_transferencia_max | rango_transferencia_max | 50000 | float | Monto maximo transferencias (TEXT/TINT) |
| rango_pago_saldo_min | rango_pago_saldo_min | 100 | float | Monto minimo pago al saldo (PGSL) |
| rango_pago_saldo_max | rango_pago_saldo_max | 25000 | float | Monto maximo pago al saldo (PGSL) |
| rango_nomina_min | rango_nomina_min | 1000 | float | Monto minimo nomina (NMNA) |
| rango_nomina_max | rango_nomina_max | 15000 | float | Monto maximo nomina (NMNA) |
| rango_adelanto_min | rango_adelanto_min | 500 | float | Monto minimo adelanto salarial (ADSL) |
| rango_adelanto_max | rango_adelanto_max | 10000 | float | Monto maximo adelanto salarial (ADSL) |
| rango_servicio_min | rango_servicio_min | 10 | float | Monto minimo pago de servicios (PGSV) |
| rango_servicio_max | rango_servicio_max | 5000 | float | Monto maximo pago de servicios (PGSV) |
| rango_compra_min | rango_compra_min | 5 | float | Monto minimo compra POS (CMPR) |
| rango_compra_max | rango_compra_max | 15000 | float | Monto maximo compra POS (CMPR) |
| rango_deposito_retiro_min | rango_deposito_retiro_min | 50 | float | Monto minimo deposito/retiro sucursal (DPST/RTRO) |
| rango_deposito_retiro_max | rango_deposito_retiro_max | 100000 | float | Monto maximo deposito/retiro sucursal (DPST/RTRO) |
| rango_domiciliacion_min | rango_domiciliacion_min | 20 | float | Monto minimo domiciliacion (DMCL) |
| rango_domiciliacion_max | rango_domiciliacion_max | 3000 | float | Monto maximo domiciliacion (DMCL) |
| rango_interes_min | rango_interes_min | 1 | float | Monto minimo interes generado (INTR) |
| rango_interes_max | rango_interes_max | 5000 | float | Monto maximo interes generado (INTR) |
| rango_comision_min | rango_comision_min | 1 | float | Monto minimo comision bancaria (CMSN) |
| rango_comision_max | rango_comision_max | 500 | float | Monto maximo comision bancaria (CMSN) |
| rango_impuesto_min | rango_impuesto_min | 1 | float | Monto minimo impuesto retenido (IMPT) |
| rango_impuesto_max | rango_impuesto_max | 2000 | float | Monto maximo impuesto retenido (IMPT) |
| num_particiones | num_particiones | 50 | int | Numero de particiones para repartition al escribir el parquet |

### NbGenerarSaldosCliente.py

| Widget | Nombre | Valor por Defecto | Tipo Conversion | Descripcion |
|--------|--------|-------------------|-----------------|-------------|
| ruta_salida_parquet | ruta_salida_parquet | abfss://container@storageaccount.dfs.core.windows.net/landing/saldos_clientes | str | Ruta del parquet de salida (protocolo abfss:// obligatorio, V2-R5-D2) |
| ruta_maestro_clientes | ruta_maestro_clientes | abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes | str | Ruta del parquet del Maestro para leer CUSTIDs (protocolo abfss://) |
| rango_ahorro_min | rango_ahorro_min | 0 | float | Monto minimo cuenta ahorro |
| rango_ahorro_max | rango_ahorro_max | 500000 | float | Monto maximo cuenta ahorro |
| rango_corriente_min | rango_corriente_min | 0 | float | Monto minimo cuenta corriente |
| rango_corriente_max | rango_corriente_max | 250000 | float | Monto maximo cuenta corriente |
| rango_credito_min | rango_credito_min | 0 | float | Monto minimo linea de credito |
| rango_credito_max | rango_credito_max | 100000 | float | Monto maximo linea de credito |
| rango_prestamo_min | rango_prestamo_min | 1000 | float | Monto minimo prestamo |
| rango_prestamo_max | rango_prestamo_max | 1000000 | float | Monto maximo prestamo |
| num_particiones | num_particiones | 20 | int | Numero de particiones para repartition al escribir el parquet |
