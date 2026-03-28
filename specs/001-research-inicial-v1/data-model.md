# Modelo de Datos - Version 1: Research Inicial y Decisiones Clave

**Feature**: 001-research-inicial-v1
**Fecha**: 2026-03-27
**Estado**: ESTRUCTURAS FINALES APROBADAS (2026-03-28) — todas las decisiones R2-D1 a R2-D4 CERRADAS

---

## Entidades del Dominio

> **Nota**: Las estructuras de campos de E1, E2 y E3 fueron aprobadas el 2026-03-28 (decisiones R2-D1, R2-D2, R2-D4). Los detalles completos campo por campo se encuentran en [research.md](research.md) secciones H2.1, H2.2 y H2.4 respectivamente. El catalogo de tipos de transaccion (R2-D3) se encuentra en research.md seccion H2.3.

### E1: Maestro de Clientes (Bronce -> Plata -> Oro)

| Atributo | Capa | Descripcion |
|----------|------|-------------|
| CUSTID | Bronce | Identificador unico del cliente (PK). NUMERIC(15,0). |
| 70 campos AS400 (APROBADOS) | Bronce | Estructura aprobada en R2-D1: 42 textuales CHAR, 18 fechas DATE, 10 numericos NUMERIC. Ver detalle completo en research.md H2.1. |
| FechaIngestaDatos | Bronce | Marca de tiempo de ingesta. Agregado por AutoLoader. Acumulacion historica. |

**Transformacion a Plata**: Se consolida con Saldos en una sola vista materializada. Comportamiento Dimension Tipo 1 (SCD Type 1) (siempre datos mas recientes). Se agregan minimo 3 campos calculados basados en CASE (pyspark when/otherwise) con 3+ columnas de Bronce, incluyendo un campo SHA2_256 del CUSTID.

**Transformacion a Oro**: Agregacion a nivel cliente individual (1 fila por cliente). Totales historicos acumulados.

### E2: Transaccional (Bronce -> Plata -> Oro)

| Atributo | Capa | Descripcion |
|----------|------|-------------|
| TRXID | Bronce | Identificador unico de transaccion (PK). NUMERIC(18,0). |
| CUSTID | Bronce | FK al Maestro de Clientes. NUMERIC(15,0). |
| TRXTYP | Bronce | Codigo del tipo de transaccion (15 tipos aprobados en R2-D3, ver H2.3). |
| 60 campos AS400 (APROBADOS) | Bronce | Estructura aprobada en R2-D2: 30 numericos NUMERIC, 21 fechas DATE/TIMESTAMP, 9 textuales CHAR. Ver detalle completo en research.md H2.2. |
| FechaIngestaDatos | Bronce | Marca de tiempo de ingesta. Agregado por AutoLoader. Acumulacion historica. |

**Transformacion a Plata**: Vista materializada separada con procesamiento incremental (solo registros nuevos). Se agregan minimo 4 campos calculados basados en 2+ columnas numericas.

**Transformacion a Oro**: Agregacion a nivel cliente: conteo creditos/debitos ATM, promedio montos retirados/depositados, total Pagos al Saldo.

### E3: Saldos de Clientes (Bronce -> Plata)

| Atributo | Capa | Descripcion |
|----------|------|-------------|
| CUSTID | Bronce | FK al Maestro de Clientes (relacion 1:1). NUMERIC(15,0). |
| 100 campos AS400 (APROBADOS) | Bronce | Estructura aprobada en R2-D4: 30 textuales CHAR, 35 numericos NUMERIC, 35 fechas DATE. Ver detalle completo en research.md H2.4. |
| FechaIngestaDatos | Bronce | Marca de tiempo de ingesta. Agregado por AutoLoader. |

**Transformacion a Plata**: Se consolida con Maestro de Clientes en una sola vista materializada (Dimension Tipo 1 (SCD Type 1)).

### E4: Documento de Research (solo V1)

| Atributo | Tipo | Descripcion |
|----------|------|-------------|
| Tema | Texto | Tema investigado (LSDP, AS400, nombres, extensiones, Azure SQL) |
| Fuentes | Lista | URLs oficiales consultadas |
| Hallazgos | Texto | Resultados principales de la investigacion |
| Opciones | Lista | Alternativas identificadas (si aplica) |
| Recomendacion | Texto | Propuesta de la IA |
| Decision | Texto | Decision del usuario (PENDIENTE/APROBADA/RECHAZADA) |

### E5: Registro de Decisiones (solo V1)

| Atributo | Tipo | Descripcion |
|----------|------|-------------|
| ID | Codigo | Identificador unico (formato R#-D#) |
| Tema | Texto | Descripcion corta del punto de decision |
| Opciones | Lista | Alternativas evaluadas |
| Recomendacion | Texto | Propuesta de la IA con justificacion |
| Decision | Texto | Decision final del usuario |
| Estado | Enum | ABIERTA / APROBADA / RECHAZADA |

---

## Relaciones Entre Entidades

```text
Maestro de Clientes (CUSTID) ──1:N──> Transaccional (CUSTID)
Maestro de Clientes (CUSTID) ──1:1──> Saldos (CUSTID)

[Bronce]                              [Plata]                          [Oro]
┌─────────────────┐                   ┌──────────────────────┐         ┌─────────────────────┐
│ Maestro (AS400) │──┐                │ Vista Materializada  │         │ Vista Materializada │
│ @dp.table       │  ├──Dimension Tipo 1──>│ Clientes + Saldos    │────────>│ Agregado Cliente    │
│ AutoLoader      │  │                │ @dp.materialized_view│         │ @dp.materialized_view│
└─────────────────┘  │                │ + 3 campos calculados│         │ Conteo ATM, Promedios│
                     │                │ + SHA2_256(CUSTID)   │         │ Total Pagos Saldo   │
┌─────────────────┐  │                └──────────────────────┘         └─────────────────────┘
│ Saldos (AS400)  │──┘
│ @dp.table       │
│ AutoLoader      │
└─────────────────┘

┌─────────────────┐                   ┌──────────────────────┐
│ Transaccional   │                   │ Vista Materializada  │────────>(Alimenta Oro arriba)
│ @dp.table       │──Incremental────>│ Transaccional        │
│ AutoLoader      │                   │ @dp.materialized_view│
└─────────────────┘                   │ + 4 campos calculados│
                                      └──────────────────────┘
```

---

## Reglas de Validacion

| Regla | Entidad | Descripcion |
|-------|---------|-------------|
| RV-01 | Maestro | CUSTID no puede ser nulo ni duplicado dentro de una misma ejecucion |
| RV-02 | Maestro | Los nombres FRSTNM, LSTNM deben provenir exclusivamente del catalogo de nombres no latinos |
| RV-03 | Transaccional | TRXTYP debe pertenecer al catalogo aprobado de tipos de transaccion |
| RV-04 | Transaccional | TRXAMT debe ser mayor que cero para todos los tipos de transaccion |
| RV-05 | Saldos | Relacion 1:1 estricta con Maestro: cada CUSTID debe tener exactamente un registro de saldo |
| RV-06 | Bronce | FechaIngestaDatos debe ser no nula en todas las tablas de Bronce |
| RV-07 | Plata | Los campos calculados deben usar minimo 3 columnas de Bronce (Clientes+Saldos) o 2 columnas numericas (Transaccional) |
| RV-08 | Plata | El campo SHA2_256 del CUSTID debe generarse correctamente para todos los registros |
| RV-09 | Oro | La agregacion debe producir exactamente 1 fila por cliente |

---

## Transiciones de Estado

### Flujo de Datos por Medalla

| Etapa | Entrada | Salida | Patron |
|-------|---------|--------|--------|
| Ingesta Bronce | Parquets Landing Zone (ADLS Gen2) | Tablas Delta en bronce_dev.regional | AutoLoader + @dp.table + acumulacion historica |
| Transformacion Plata (Clientes+Saldos) | bronce_dev.regional.maestro_clientes + bronce_dev.regional.saldos_clientes | plata_dev.regional.clientes_saldos_consolidados | @dp.materialized_view + Dimension Tipo 1 (SCD Type 1) |
| Transformacion Plata (Transaccional) | bronce_dev.regional.transaccional | plata_dev.regional.transaccional_enriquecido | @dp.materialized_view + incremental |
| Agregacion Oro | plata_dev.regional.* | oro_dev.regional.resumen_cliente | @dp.materialized_view + Dimension Tipo 1 (SCD Type 1) |

### Catalogos Unity Catalog

| Catalogo | Esquema | Contenido |
|----------|---------|-----------|
| bronce_dev | regional | Tablas delta raw (Maestro, Transaccional, Saldos) con datos crudos de AS400 |
| plata_dev | regional | Vistas materializadas enriquecidas (Clientes+Saldos consolidado, Transaccional incremental) |
| oro_dev | regional | Vistas materializadas agregadas (Resumen por cliente) |
| control_dev | regional | Tablas de control, bitacoras, configuraciones, parametros de Azure SQL |
