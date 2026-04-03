# Research - Version 4: LSDP Medalla de Plata

**Feature**: 004-lsdp-medalla-plata
**Fecha**: 2026-03-30
**Estado**: TODAS LAS DECISIONES APROBADAS
**Base**: Decisiones aprobadas R1-D1 a R8-D1 de Versiones 1 y 3

---

## Indice

1. [Research 9: Validacion de @dp.materialized_view para V4](#research-9-validacion-de-dpmaterialized_view-para-v4)
2. [Research 10: Estrategia Dimension Tipo 1 con Window Functions](#research-10-estrategia-dimension-tipo-1-con-window-functions)
3. [Research 11: Expectations en Vistas Materializadas](#research-11-expectations-en-vistas-materializadas)
4. [Research 12: Extensiones de Databricks para VS Code (Actualizacion V4)](#research-12-extensiones-de-databricks-para-vs-code-actualizacion-v4)
5. [Registro de Decisiones Version 4](#registro-de-decisiones-version-4)

---

## Research 9: Validacion de @dp.materialized_view para V4

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/ldp/developer/ldp-python-ref-materialized-view (Referencia @dp.materialized_view - actualizada Feb 2026)
2. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Referencia Python API - actualizada Ene 2026)
3. https://learn.microsoft.com/es-es/azure/databricks/ldp/limitations (Limitaciones del pipeline - actualizada Mar 2026)

### Hallazgos Principales

#### H9.1 - Parametros Completos de @dp.materialized_view (Confirmacion)

La documentacion oficial actualizada confirma los parametros del decorador `@dp.materialized_view`. Todos los parametros relevantes para V4 estan disponibles:

| Parametro | Tipo | Descripcion | Uso en V4 |
|-----------|------|-------------|-----------|
| `name` | str | Nombre de la vista (default: nombre de funcion) | SI — `{catalogoPlata}.{esquema_plata}.nombre_vista` (RF-016) |
| `comment` | str | Descripcion de la tabla | SI — obligatorio en espanol (RF-020) |
| `spark_conf` | dict | Configuraciones Spark para la query | NO |
| `table_properties` | dict | Propiedades Delta de la tabla | SI — 5 propiedades Delta (RF-008) |
| `path` | str | Ubicacion de almacenamiento | NO — se usa ubicacion administrada |
| `partition_cols` | list | Columnas de particion | NO — prohibido por constitucion |
| `cluster_by_auto` | bool | Liquid Clustering automatico | NO — se usa cluster_by explicito (R6-D1) |
| `cluster_by` | list | Columnas para Liquid Clustering | SI — columnas definidas por entidad (RF-008) |
| `schema` | str/StructType | Definicion de esquema | NO — esquema determinado por el DataFrame retornado |
| `refresh_policy` | str | Politica de actualizacion (Beta) | VER H9.2 |
| `private` | bool | Vista privada (no publica en metastore) | NO — deben ser publicas |
| `row_filter` | str | Filtro de filas (Public Preview) | NO |

**Conclusion**: Todos los parametros requeridos por la spec (name, comment, table_properties, cluster_by) estan confirmados. No hay cambios respecto a los hallazgos de V1 (H1.3) para estos parametros.

#### H9.2 - Nuevo Parametro `refresh_policy` (Beta)

La documentacion introduce el parametro `refresh_policy` para vistas materializadas:

| Valor | Descripcion | Estado |
|-------|-------------|--------|
| `auto` | LSDP decide automaticamente la estrategia de actualizacion | **Default** |
| `incremental` | Fuerza actualizacion incremental | Beta |
| `incremental_strict` | Fuerza incremental estricto (sin fallback a full) | Beta |
| `full` | Fuerza actualizacion completa en cada ejecucion | Estable |

**Impacto en V4**:
- **Vista consolidada clientes_saldos_consolidados**: Usa window functions con `row_number()` para obtener el registro mas reciente por CUSTID. Esta operacion requiere lectura completa de las tablas fuente. `refresh_policy='auto'` es adecuado (LSDP decidira `full`).
- **Vista transaccional transacciones_enriquecidas**: El spec RF-006 establece que "la gestion de datos es manejada por el decorador @dp.materialized_view". Con `refresh_policy='auto'` (default), LSDP evaluara si puede aplicar incrementalidad automatica. Las vistas materializadas con lecturas batch simples (`spark.read.table()`) sin operaciones complejas (joins, window functions) son candidatas a actualizacion incremental automatica.

**Recomendacion**: Usar `refresh_policy='auto'` (default) para ambas vistas materializadas. No configurar explicitamente — dejar que LSDP optimice automaticamente. Esto evita depender de una funcionalidad Beta.

#### H9.3 - Confirmacion de Publicacion Cross-Catalog

La documentacion oficial confirma (H1.10 de V1) que para crear vistas en catalogos diferentes al default del pipeline:

```python
@dp.materialized_view(
    name="plata_dev.regional.nombre_vista",
    ...
)
```

Esto es exactamente lo que RF-016 especifica:
```python
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.nombre_vista",
    ...
)
```

Donde `catalogo_plata` proviene de Azure SQL y `esquema_plata` del parametro del pipeline, ambos capturados por closure como variables de modulo.

#### H9.4 - Limitaciones Relevantes para V4 (Confirmadas)

Las siguientes limitaciones de LSDP aplican a las vistas materializadas de plata:

1. **Sin time travel**: Delta Lake time travel NO se soporta con materialized views (solo streaming tables). Esto no afecta a V4 ya que no se requiere time travel en plata.
2. **Sin pivot()**: Confirmado, pero no se requiere en V4 (R1-D4).
3. **Sin Iceberg reads**: No aplica a V4.
4. **Columnas de identidad**: Pueden recalcularse en materialized views — no usar columnas de identidad. V4 usa SHA2_256 en lugar de identity columns, por lo que no hay impacto.
5. **Recalculo completo**: Las materialized views con operaciones complejas (window functions, joins) realizan recalculo completo en cada ejecucion. Esto es aceptable para clientes_saldos_consolidados (dimension tipo 1 — siempre datos mas recientes).

### Decisiones Requeridas - Research 9

> **DECISION R9-D1**: Confirmar que se usa `refresh_policy='auto'` (default, no configurar explicitamente) para ambas vistas materializadas de plata, en lugar de forzar `incremental` (Beta) o `full`.
> - **Recomendacion IA**: Aprobar `auto` (dejar sin configurar). `auto` permite que LSDP optimice automaticamente. Las funcionalidades `incremental` e `incremental_strict` estan en Beta y podrian presentar limitaciones no documentadas. El default `auto` es la opcion mas estable y soportada.
> - **Alternativa considerada**: `refresh_policy='incremental'` para la vista transaccional. Rechazada porque: (1) es Beta, (2) podria tener restricciones con ciertas transformaciones, (3) `auto` ya evalua y aplica incrementalidad cuando es posible.
> - **Decision del usuario**: APROBADO — auto permite que LSDP optimice automaticamente.

---

## Research 10: Estrategia Dimension Tipo 1 con Window Functions

### Fuentes Consultadas

1. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Patrones de transformacion)
2. https://learn.microsoft.com/es-es/azure/databricks/ldp/ (Mejores practicas LSDP)
3. PySpark SQL Functions — Row Number, Window (documentacion oficial Apache Spark)

### Hallazgos Principales

#### H10.1 - Evolucion de R1-D2: De create_auto_cdc_flow a Window Functions

En la Version 1, la decision R1-D2 aprobo `dp.create_auto_cdc_flow()` como estrategia para la consolidacion Maestro+Saldos como Dimension Tipo 1. Sin embargo, la spec de V4 (RF-003) define un enfoque diferente basado en window functions:

| Aspecto | R1-D2 (V1 forward-looking) | RF-003 (V4 spec actual) |
|---------|---------------------------|------------------------|
| Mecanismo | `dp.create_auto_cdc_flow()` + `dp.create_streaming_table()` | `@dp.materialized_view` + window functions |
| Tipo de tabla | Streaming table como target CDC | Vista materializada batch |
| SCD Tipo 1 | Parametro `stored_as_scd_type=1` | `row_number()` con Window particionada por CUSTID |
| Complejidad | Requiere 2 APIs: `create_streaming_table` + `create_auto_cdc_flow` | 1 solo decorador `@dp.materialized_view` |
| Lectura fuente | Streaming desde bronce | Batch desde bronce (`spark.read.table()`) |
| Incrementalidad | Nativa (CDC procesa solo cambios) | Recalculo completo (acceptable para dimension) |

**Razon del cambio**: La spec evoluciono durante las sesiones de clarificacion. El enfoque con window functions es:
1. Mas simple de implementar y mantener
2. Compatible con la consolidacion de 2 tablas (cmstfl LEFT JOIN blncfl)
3. Adecuado para dimension tipo 1 donde siempre se necesitan los datos mas recientes
4. No requiere CDC (las tablas fuente de bronce son append-only via AutoLoader)

#### H10.2 - Patron Window Function para Dimension Tipo 1

El patron para obtener el registro mas reciente por CUSTID en una tabla de bronce append-only:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col

ventana_mas_reciente = Window.partitionBy("CUSTID").orderBy(desc("FechaIngestaDatos"))

df_ultimos_cmstfl = (
    spark.read.table("bronce_dev.regional.cmstfl")
    .withColumn("_rn", row_number().over(ventana_mas_reciente))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
```

**Optimizacion**: La constitucion (Principio VI) establece que "el procesamiento DEBE cumplir con los mas altos niveles de optimizacion". Las window functions con `partitionBy("CUSTID").orderBy(desc("FechaIngestaDatos"))` aprovechan el Liquid Cluster de bronce que tiene exactamente `["FechaIngestaDatos", "CUSTID"]` como claves de clustering, optimizando la lectura y particion.

#### H10.3 - Manejo de Columnas Duplicadas en el JOIN

Al consolidar cmstfl (72 cols) y blncfl (102 cols) via LEFT JOIN, existen columnas con el mismo nombre en ambas tablas. Se identificaron las siguientes duplicaciones:

| Columna AS400 | En cmstfl | En blncfl | Resolucion |
|---------------|-----------|-----------|------------|
| CUSTID | Si (PK) | Si (FK) | Solo de cmstfl (join key) |
| SGMNT | Si | Si | cmstfl: `segmento_cliente`, blncfl: `segmento_cuenta` |
| RISKLV | Si | Si | cmstfl: `nivel_riesgo_cliente`, blncfl: `nivel_riesgo_cuenta` |
| BRNCOD | Si | Si | cmstfl: `codigo_sucursal_cliente`, blncfl: `codigo_sucursal_cuenta` |
| BRNNM | Si | Si | cmstfl: `nombre_sucursal_cliente`, blncfl: `nombre_sucursal_cuenta` |
| ACCTST | Si | Si | cmstfl: `estado_cuenta_cliente`, blncfl: `estado_cuenta_saldo` |
| OPNDT | Si | Si | cmstfl: `fecha_apertura_cuenta`, blncfl: `fecha_apertura_cuenta_saldo` |
| LSTTRX | Si | Si | cmstfl: `fecha_ultima_transaccion`, blncfl: `fecha_ultima_transaccion_saldo` |
| RVWDT | Si | Si | cmstfl: `fecha_revision_kyc`, blncfl: `fecha_revision_cuenta` |
| CNCLDT | Si | Si | cmstfl: `fecha_cancelacion_maestro`, blncfl: `fecha_cancelacion_cuenta` |
| PRMDT | Si | Si | cmstfl: `fecha_promocion_segmento`, blncfl: `fecha_promocion_cuenta` |
| CHGDT | Si | Si | cmstfl: `fecha_cambio_estado`, blncfl: `fecha_cambio_condiciones` |
| FechaIngestaDatos | Si | Si | cmstfl: `fecha_ingesta_maestro`, blncfl: `fecha_ingesta_saldo` |
| _rescued_data | Si | Si | **EXCLUIDO** — no se traslada a plata (decision del usuario) |

**Estrategia**: Usar alias al leer cada tabla (`df_cmstfl.alias("c")`, `df_blncfl.alias("b")`) y en el select final usar `col("c.COLUMNA").alias("nombre_espanol")` para cada columna.

### Decisiones Requeridas - Research 10

> **DECISION R10-D1**: Confirmar que la estrategia de Dimension Tipo 1 para V4 usa `@dp.materialized_view` con window functions (RF-003) en lugar de `dp.create_auto_cdc_flow()` (R1-D2).
> - **Recomendacion IA**: Aprobar window functions. El enfoque es mas simple, esta completamente definido en la spec con 5 clarificaciones resueltas, y es compatible con el LEFT JOIN de 2 tablas. R1-D2 fue una decision forward-looking que la spec V4 evoluciono naturalmente.
> - **Alternativa considerada**: `dp.create_auto_cdc_flow()` con SCD Type 1. Rechazada porque: (1) requiere streaming source y la consolidacion es batch, (2) no soporta LEFT JOIN de 2 tablas en una sola operacion, (3) mayor complejidad sin beneficio adicional dado que la dimension se recalcula completamente.
> - **Decision del usuario**: APROBADO — El enfoque es mas simple, completamente definido en la spec con 5 clarificaciones resueltas, y compatible con el LEFT JOIN de 2 tablas.

> **DECISION R10-D2**: Aprobar la estrategia de resolucion de columnas duplicadas entre cmstfl y blncfl usando calificadores descriptivos en espanol (tabla H10.3).
> - **Recomendacion IA**: Aprobar. Los calificadores `_cliente` y `_cuenta`/`_saldo` son intuitivos, descriptivos y cumplen con RF-010 (nombres en espanol, snake_case, minimo 2 palabras).
> - **Alternativa considerada**: Usar solo las columnas de cmstfl para los campos duplicados y descartar las de blncfl. Rechazada porque la spec requiere TODAS las columnas de bronce.
> - **Decision del usuario**: APROBADO — Los calificadores _cliente y _cuenta/_saldo son intuitivos, descriptivos y cumplen con RF-010 (nombres en espanol, snake_case, minimo 2 palabras).

---

## Research 11: Expectations en Vistas Materializadas

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/ldp/developer/ldp-python-ref-expectations (Referencia Expectations - actualizada Ene 2026)
2. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Referencia Python API)

### Hallazgos Principales

#### H11.1 - Confirmacion de @dp.expect_or_drop con @dp.materialized_view

La documentacion oficial confirma que los decoradores de expectativas (`@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`) se aplican a vistas materializadas, tablas streaming y vistas temporales.

La sintaxis confirmada para RF-018 es:

```python
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados",
    comment="Vista materializada consolidada de clientes y saldos...",
    table_properties={...},
    cluster_by=[...]
)
@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")
def vista_clientes_saldos_consolidados():
    ...
```

**Comportamiento confirmado**:
- Los registros que NO cumplen la condicion son **eliminados** antes de escribir al dataset destino.
- El conteo de registros descartados se registra junto con otras metricas del dataset.
- No detiene la ejecucion del pipeline (diferencia con `expect_or_fail`).

#### H11.2 - Constraint como Expresion SQL

El parametro `constraint` de `@dp.expect_or_drop` es una expresion SQL condicional que se evalua como `true` o `false` para cada registro. La condicion usa los **nombres de columna del DataFrame retornado** (es decir, los nombres en espanol despues del renombramiento).

**Importante**: La constraint debe referenciar el nombre de columna del DataFrame retornado, no el nombre AS400 original. Por lo tanto: `"identificador_cliente IS NOT NULL"` (nombre en espanol post-renombrado), no `"CUSTID IS NOT NULL"`.

#### H11.3 - Multiples Expectations en un Solo Dataset

Es posible apilar multiples decoradores de expectativas:

```python
@dp.materialized_view(...)
@dp.expect_or_drop("custid_no_nulo", "identificador_cliente IS NOT NULL")
@dp.expect("puntaje_crediticio_valido", "puntaje_crediticio BETWEEN 300 AND 850")
def vista_clientes_saldos_consolidados():
    ...
```

**Para V4**: Solo se requiere `@dp.expect_or_drop` para CUSTID nulo segun RF-018. No se agregan expectativas adicionales en esta version.

### Decisiones Requeridas - Research 11

> **DECISION R11-D1**: Confirmar que la constraint del `@dp.expect_or_drop` usa el nombre de columna en espanol post-renombrado (`"identificador_cliente IS NOT NULL"`) y no el nombre AS400 original.
> - **Recomendacion IA**: Aprobar. La documentacion indica que la constraint se evalua sobre el DataFrame retornado. Dado que la funcion retorna un DataFrame con columnas renombradas a espanol, la constraint debe usar el nombre espanol.
> - **Decision del usuario**: APROBADO — La constraint se evalua sobre el DataFrame retornado con columnas renombradas a espanol.

---

## Research 12: Extensiones de Databricks para VS Code (Actualizacion V4)

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/dev-tools/vscode-ext/ (Documentacion oficial Azure - Mar 2026)
2. https://marketplace.visualstudio.com/items?itemName=databricks.databricks (Marketplace VS Code)

### Hallazgos Principales

#### H12.1 - Sin Cambios Relevantes para V4

Las extensiones de Databricks para VS Code documentadas en V1 (R4-D1, R4-D2) y V3 (R8-D1) se mantienen sin cambios funcionales relevantes para V4:

- **Databricks Extension for VS Code**: Soporte para ejecucion de notebooks, sincronizacion con workspace, integracion con Unity Catalog y Computo Serverless.
- **SQLTools Databricks Driver**: Consultas SQL contra Databricks desde VS Code.

La estrategia de testing aprobada en R4-D1 sigue vigente: notebooks de prueba en Databricks para el pipeline LSDP, ya que `pyspark.pipelines` solo existe dentro del contexto de un pipeline.

**Conclusion**: No se requieren decisiones adicionales para extensiones de VS Code en V4.

---

## Registro de Decisiones Version 4

| Decision | Research | Descripcion | Estado |
|----------|----------|-------------|--------|
| R9-D1 | R9 | refresh_policy='auto' (default) para ambas vistas | APROBADO |
| R10-D1 | R10 | Window functions en lugar de create_auto_cdc_flow | APROBADO |
| R10-D2 | R10 | Resolucion columnas duplicadas con calificadores espanol | APROBADO |
| R11-D1 | R11 | Constraint expect_or_drop con nombres espanol post-renombrado | APROBADO |
