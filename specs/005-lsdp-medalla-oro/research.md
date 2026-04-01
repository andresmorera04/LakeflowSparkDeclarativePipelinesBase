# Research: LSDP Medalla de Oro (Version 5)

**Branch**: `005-lsdp-medalla-oro` | **Fecha**: 2026-03-31
**Decisiones previas vigentes**: R1-D1 a R5-D2 (V1), R6-D1 a R8-D1 (V3), R9-D1 a R12 (V4) — 15 decisiones aprobadas

---

## Research 13: Validacion de `@dp.materialized_view` con Agregaciones groupBy en LSDP

**Contexto**: La medalla de oro requiere vistas materializadas con agregaciones (`groupBy`, `count(when(...))`, `sum(when(...))`, `avg(when(...))`) — un patron no utilizado en V3 (streaming tables sin transformacion) ni V4 (transformaciones fila por fila sin agrupacion). Es necesario validar que `@dp.materialized_view` soporta DataFrames resultantes de operaciones de agrupacion.

**Fuente**: https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/python-dev (seccion "Query materialized views and streaming tables defined in your pipeline")

### Hallazgo H13.1: Soporte Nativo para Agregaciones en Vistas Materializadas

La documentacion oficial de Azure Databricks incluye un ejemplo explicito de vista materializada con agregaciones:

```python
@dp.materialized_view()
def daily_orders_by_state():
    return (spark.read.table("customer_orders")
      .groupBy("state", "order_date")
      .count().withColumnRenamed("count", "order_count")
    )
```

**Conclusion**: `@dp.materialized_view` acepta cualquier DataFrame resultante de una lectura batch, incluyendo DataFrames con `groupBy`, `agg`, `count`, `sum`, `avg`, `when`, y cualquier otra funcion de `pyspark.sql.functions`. No hay restriccion sobre el tipo de transformacion aplicada al DataFrame siempre que la funcion decorada retorne un DataFrame valido.

### Hallazgo H13.2: Agregaciones Condicionales con `when` son Operaciones Estandar de PySpark

Las funciones `count(when(...))`, `sum(when(...))` y `avg(when(...))` son operaciones nativas de PySpark (`pyspark.sql.functions`) que producen un DataFrame estandar al combinarse con `groupBy().agg()`. No son funcionalidades especificas de LSDP ni requieren configuracion adicional.

Ejemplo de patron para V5:
```python
from pyspark.sql.functions import count, sum as spark_sum, avg, when, col

df_agrupado = (
    df_transacciones
    .groupBy("identificador_cliente")
    .agg(
        count(when(col("tipo_transaccion") == "CATM", 1)).alias("cantidad_depositos_atm"),
        count(when(col("tipo_transaccion") == "DATM", 1)).alias("cantidad_retiros_atm"),
        avg(when(col("tipo_transaccion") == "CATM", col("monto_transaccion"))).alias("promedio_monto_depositos_atm"),
        avg(when(col("tipo_transaccion") == "DATM", col("monto_transaccion"))).alias("promedio_monto_retiros_atm"),
        spark_sum(when(col("tipo_transaccion") == "PGSL", col("monto_transaccion"))).alias("total_pagos_saldo_cliente"),
    )
)
```

**Conclusion**: El patron de agregaciones condicionales es 100% compatible con `@dp.materialized_view` ya que el resultado es un DataFrame estandar de PySpark.

### Hallazgo H13.3: Manejo de Nulos en Agregaciones Condicionales

Cuando un cliente no tiene transacciones de un tipo especifico (ej: no tiene "CATM"), las funciones `count(when(...))` retornan 0 (comportamiento nativo de `count`) y `avg(when(...))` y `sum(when(...))` retornan `null`. Para cumplir con RF-016 (campos nunca nulos), se debe aplicar `coalesce(campo, lit(0))` sobre los resultados de `avg` y `sum` pero NO sobre `count` (que ya retorna 0).

**Alternativas evaluadas**:
- **fillna(0)** a nivel de DataFrame: Aplica a todas las columnas incluyendo `identificador_cliente`, lo cual es incorrecto.
- **coalesce individual**: Aplica solo a las columnas de metricas que pueden ser null. **Opcion preferida**.
- **when + otherwise**: Mas verboso, mismo resultado que coalesce. Descartado por simplicidad.

### Decision R13-D1: Usar `@dp.materialized_view` con `groupBy().agg()` y Agregaciones Condicionales

- **Decision**: Usar `groupBy("identificador_cliente").agg(count(when(...)), avg(when(...)), sum(when(...)))` dentro de `@dp.materialized_view` para generar metricas agregadas en una sola pasada.
- **Razon**: Patron oficialmente documentado y verificado. Maximiza eficiencia (una sola pasada sobre los datos, sin filtros previos como indica RF-017).
- **Alternativas rechazadas**: (1) Filtrar antes de agrupar (multiples pasadas, ineficiente, viola RF-017). (2) Usar vistas temporales intermedias (complejidad innecesaria).
- **Estado**: **APROBADO POR EL USUARIO (2026-03-31)**

---

## Research 14: Aplicacion de `@dp.expect_or_drop` en Vistas con Agregaciones

**Contexto**: RF-018 requiere `@dp.expect_or_drop` para `identificador_cliente` nulo en `comportamiento_atm_cliente`. Esta vista aplica `groupBy("identificador_cliente")`, lo cual requiere validar en que momento del procesamiento LSDP aplica la expectation.

**Fuente**: https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/python-dev (seccion "Validate data with expectations"), Referencia API de expectations.

### Hallazgo H14.1: Orden de Ejecucion de Expectations en LSDP

Basado en la documentacion oficial:
- `@dp.expect_or_drop("nombre", "condicion")` se aplica como un decorador sobre la funcion.
- La expectation se evalua sobre las **filas del DataFrame final** retornado por la funcion.
- En una vista con `groupBy`, el DataFrame retornado contiene las filas AGRUPADAS, no las filas originales.

Esto significa que si se aplica `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")` sobre una funcion que retorna un DataFrame con `groupBy("identificador_cliente")`, la expectation filtraria filas donde `identificador_cliente` es NULL en el resultado AGRUPADO.

### Hallazgo H14.2: Implicacion para V5

Dado que `groupBy("identificador_cliente")` crea un grupo para `null` si existen transacciones con `identificador_cliente` nulo, la expectation `@dp.expect_or_drop` eliminaria correctamente ese grupo nulo del resultado final. El efecto es equivalente a filtrar antes del groupBy, pero con la ventaja de que LSDP registra las metricas de registros eliminados automaticamente.

**Alternativas evaluadas**:
- **Filtrar antes del groupBy** (`.filter(col("identificador_cliente").isNotNull())`): Funciona pero no registra metricas LSDP. No cumple con el espiritu de RF-018 (monitoreo).
- **expect_or_drop sobre el resultado agrupado**: Funciona correctamente ya que eliminaria la fila donde `identificador_cliente` es NULL (el grupo NULL). **Opcion preferida** — registra metricas y cumple RF-018.
- **expect_or_fail**: Demasiado agresivo. Detendria todo el pipeline por un solo nulo.

### Decision R14-D1: Aplicar `@dp.expect_or_drop` sobre la Vista Materializada con groupBy

- **Decision**: Usar `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")` como decorador sobre la funcion `comportamiento_atm_cliente`. La expectation eliminara el grupo NULL del resultado agrupado y LSDP registrara las metricas de eliminacion.
- **Razon**: Cumple RF-018 (defensa en profundidad), aprovecha el monitoreo nativo de LSDP, y es consistente con el patron de V4 (R11-D1).
- **Alternativas rechazadas**: (1) Filtro manual antes del groupBy (sin metricas LSDP). (2) expect_or_fail (demasiado agresivo).
- **Estado**: **APROBADO POR EL USUARIO (2026-03-31)**

---

## Research 15: Dependencia Intra-Pipeline entre Vistas Materializadas via `spark.read.table()`

**Contexto**: `resumen_integral_cliente` necesita leer de `comportamiento_atm_cliente` (ambas en el mismo archivo `LsdpOroClientes.py`). LSDP debe resolver la dependencia en el DAG del pipeline automaticamente.

**Fuente**: https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/python-dev (seccion "Query materialized views and streaming tables defined in your pipeline")

### Hallazgo H15.1: LSDP Resuelve Dependencias Automaticamente via DAG

La documentacion oficial incluye un ejemplo con 4 datasets definidos en el mismo archivo donde:
1. `orders` — streaming table de JSON.
2. `customers` — materialized view de CSV.
3. `customer_orders` — materialized view que lee de `orders` y `customers` via `spark.read.table()`.
4. `daily_orders_by_state` — materialized view que lee de `customer_orders` via `spark.read.table()`.

LSDP resuelve automaticamente las dependencias construyendo el DAG del pipeline. No se requiere anotacion adicional ni orden especifico en el archivo.

Cita de la documentacion:
> "SDP interprets the decorator functions from the dp module in all source code files configured in a pipeline and builds a dataflow graph."

### Hallazgo H15.2: Lectura Intra-Pipeline con `spark.read.table()`

Para leer de otra vista materializada definida en el pipeline, se usa directamente `spark.read.table("nombre_tabla")`. Si la tabla esta en el catalogo/esquema por defecto del pipeline, basta con el nombre simple. Si esta en otro catalogo/esquema, se usa el nombre completo (`catalogo.esquema.tabla`).

En V5, dado que las vistas de oro se publican en `{catalogoOro}.{esquema_oro}`, la lectura intra-oro debe usar el nombre completo:
```python
spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")
```

Esto es consistente con la clarificacion Q4 del spec (usar `spark.read.table()` para todas las lecturas).

### Hallazgo H15.3: Ambas Vistas en el Mismo Archivo — Sin Restriccion

No existe restriccion en LSDP sobre cuantas vistas materializadas pueden definirse en un mismo archivo .py. El DAG se construye a partir de TODOS los archivos configurados en el pipeline. Tener ambas vistas en `LsdpOroClientes.py` es correcto y simplifica la closure compartida.

### Decision R15-D1: Lectura Intra-Oro con `spark.read.table()` y Nombre Completo

- **Decision**: `resumen_integral_cliente` lee de `comportamiento_atm_cliente` usando `spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")`. LSDP resuelve la dependencia automaticamente en el DAG.
- **Razon**: Patron oficialmente documentado. Consistente con V3/V4 y clarificacion Q4 del spec. El nombre completo garantiza que la vista correcta se referencia incluso si el catalogo/esquema por defecto del pipeline es diferente (bronce_dev).
- **Alternativas rechazadas**: (1) `dp.read("nombre")` — no existe en la API actual de LSDP para materialized_view. (2) Nombre simple sin catalogo — ambiguo si el pipeline default es bronce_dev.
- **Estado**: **APROBADO POR EL USUARIO (2026-03-31)**

---

## Research 16: Extensiones de Databricks para Visual Studio Code (Verificacion Rutinaria V5)

**Contexto**: La constitucion (Principio IX) requiere un research de extensiones de Databricks para VS Code al iniciar cada nueva version.

**Fuente**: Resultados de V1 (R4-D1, R4-D2), V3 (R8-D1) y V4 (R12).

### Hallazgo H16.1: Sin Cambios Significativos desde V4

Las extensiones requeridas por la constitucion (Principio II) siguen siendo:
- **Databricks extension for Visual Studio Code**: Permite acceder al workspace, enlistar computos, ejecutar notebooks.
- **Databricks Driver for SQLTools**: Operativa para consultas SQL.

En V4 (R12) se confirmo que no habia cambios significativos. Para V5, la situacion es identica — las extensiones no afectan el desarrollo de la medalla de oro ya que:
- El patron de pruebas TDD sigue siendo el mismo (notebooks en Databricks).
- No se requieren nuevas capacidades de las extensiones para V5.
- La estrategia de testing mixta (R4-D2) se mantiene vigente.

**Conclusion**: Sin nuevos hallazgos ni opciones que requieran decision del usuario.

---

## Resumen de Decisiones Pendientes

| ID | Research | Decision | Estado |
|----|----------|----------|--------|
| R13-D1 | Agregaciones en `@dp.materialized_view` | Usar `groupBy().agg()` con `count(when())`, `avg(when())`, `sum(when())` | APROBADO ||
| R14-D1 | `@dp.expect_or_drop` con groupBy | Aplicar sobre la vista materializada — filtra el grupo NULL del resultado agrupado | APROBADO |
| R15-D1 | Dependencia intra-pipeline | Usar `spark.read.table()` con nombre completo `{catalogo_oro}.{esquema_oro}.tabla` | APROBADO |

**Total decisiones nuevas**: 3
**Total decisiones acumuladas del proyecto**: 15 (V1-V4) + 3 pendientes (V5) = 18
