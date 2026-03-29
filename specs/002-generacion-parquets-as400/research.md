# Research: Generacion de Parquets Simulando Data AS400 (Version 2)

**Feature Branch**: `002-generacion-parquets-as400`
**Fecha**: 2026-03-28
**Contexto Base**: Research de Version 1 (specs/001-research-inicial-v1/research.md) — 15 decisiones aprobadas

---

## Alcance del Research V2

Conforme al Principio IX de la Constitucion, al iniciar una nueva version SIEMPRE se DEBE investigar:
1. Lakeflow Spark Declarative Pipelines (LSDP)
2. Parquets simuladores de datos AS400
3. Extensiones de Databricks para VS Code

La Version 2 genera notebooks de simulacion de datos (landing zone), NO pipeline LSDP. El research se enfoca en las tecnologias especificas de V2: generacion de parquets a escala, `dbutils.widgets`, escritura en External Locations, y estrategias de mutacion.

---

## Research V2-R1: Lakeflow Spark Declarative Pipelines (Actualizacion)

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/ldp/ (Documentacion oficial Azure - actualizada 14/03/2026)
2. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Python API Reference - actualizada 23/01/2026)

### Hallazgos Principales

#### H-V2-R1.1 - Estado Actual de LSDP

- **Modulo**: `pyspark.pipelines` (importado como `dp`). Sin cambios desde V1.
- **Apache Spark 4.1**: Las canalizaciones declarativas son parte de Apache Spark desde la version 4.1, disponibles via `pyspark.pipelines`.
- **Funcionalidades exclusivas de Databricks** (NO en Apache Spark):
  - `dp.create_auto_cdc_flow`
  - `dp.create_auto_cdc_from_snapshot_flow`
  - `@dp.expect(...)`
  - `@dp.temporary_view`
- **Modulo `dlt`**: Obsoleto (deprecated). Databricks recomienda usar `pipelines`.
- **Restriccion clave**: El modulo `pyspark.pipelines` SOLO esta disponible en el contexto de un pipeline. No es ejecutable fuera de pipelines.

#### H-V2-R1.2 - Relevancia para V2

- Los notebooks de V2 son **independientes de LSDP** (RF-015).
- Los notebooks generan archivos parquet que seran **consumidos** por el pipeline LSDP en V3-V5.
- V2 usa PySpark puro (sin decoradores de pipeline) y `dbutils.widgets` para parametrizacion.
- No hay conflicto ni dependencia directa con LSDP en esta version.

### Conclusion V2-R1

**Sin cambios respecto a V1**. La API `pyspark.pipelines` no afecta V2. Todas las decisiones aprobadas (R1-D1 a R1-D5) siguen vigentes sin modificacion.

---

## Research V2-R2: Parametrizacion con dbutils.widgets

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-utils (Referencia oficial Azure - actualizada 21/03/2026)
2. https://docs.databricks.com/aws/en/dev-tools/databricks-utils (Referencia Databricks AWS - actualizada 21/03/2026)
3. https://learn.microsoft.com/es-es/azure/databricks/notebooks/widgets (Widgets de Databricks)

### Hallazgos Principales

#### H-V2-R2.1 - Tipos de Widgets Disponibles

| Tipo | Metodo | Descripcion | Uso en V2 |
|------|--------|-------------|-----------|
| **text** | `dbutils.widgets.text(name, defaultValue, label)` | Entrada de texto libre | SI - parametros principales (ruta, cantidad, fecha, offsets, rangos) |
| **dropdown** | `dbutils.widgets.dropdown(name, defaultValue, choices, label)` | Lista desplegable con opciones fijas | OPCIONAL - podria usarse para seleccion de formatos |
| **combobox** | `dbutils.widgets.combobox(name, defaultValue, choices, label)` | Lista desplegable con entrada libre | NO aplicable |
| **multiselect** | `dbutils.widgets.multiselect(name, defaultValue, choices, label)` | Seleccion multiple | NO aplicable |

#### H-V2-R2.2 - Patron de Uso Confirmado

```python
# Definicion del widget con valor por defecto
dbutils.widgets.text(
    name='ruta_salida_parquet',
    defaultValue='abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes',
    label='Ruta de Salida del Parquet'
)

# Lectura del valor del widget
ruta_salida = dbutils.widgets.get('ruta_salida_parquet')
```

- **Todos los valores se devuelven como `String`**. Si se requiere un numero, se debe convertir explicitamente: `int(dbutils.widgets.get('cantidad_registros'))`.
- **Compatibilidad**: Funciona en notebooks interactivos, jobs programados y Computo Serverless.
- **Metodo `getAll`**: Disponible desde Databricks Runtime 13.3 LTS. Devuelve un mapa de todos los widgets: `dbutils.widgets.getAll()`. Util para pasar parametros a consultas SQL.
- **Limitacion**: No se puede `remove` y `create` un widget en la misma celda. Los widgets deben definirse en una celda separada.
- **Nota Python**: Los metodos de `dbutils.fs` usan `snake_case` (ej: `extra_configs` en lugar de `extraConfigs`).

#### H-V2-R2.3 - Estrategia de Validacion de Parametros

- `dbutils.widgets.get()` devuelve siempre `str`. Si el widget no tiene valor asignado y no tiene `defaultValue`, retorna cadena vacia.
- La validacion de parametros (RF-016) debe ser explicita en el codigo:
  1. Verificar que el valor no sea vacio o nulo
  2. Verificar formato (ej: fecha en formato `YYYY-MM-DD`)
  3. Verificar rangos numericos validos
  4. Mostrar mensaje descriptivo con `raise ValueError("...")`

### Decision Requerida - Research V2-R2

> **DECISION V2-R2-D1**: ~~Confirmar que~~ `dbutils.widgets.text()` es el tipo principal de widget a utilizar en los 3 notebooks, con conversion explicita a tipos numericos donde sea necesario y validacion programatica de parametros al inicio de cada notebook.
> - **Recomendacion IA**: Aprobar. Es el mecanismo mas flexible: acepta cualquier valor como texto, es compatible con jobs y Serverless, y permite definir valores por defecto claros. La conversion y validacion explicita es una buena practica que previene errores en tiempo de ejecucion.
> - **Alternativas consideradas**: (A) Usar `dropdown` para parametros con opciones fijas - limita la flexibilidad en jobs programados. (B) Usar variables de entorno - no nativo de Databricks. (C) Usar parametros de notebook via API de Jobs - menos interactivo.
> - **Decision del usuario**: APROBADA (2026-03-28). Justificacion: Es el mecanismo mas flexible, compatible con jobs y Serverless, con valores por defecto claros. La conversion y validacion explicita previene errores en tiempo de ejecucion.

---

## Research V2-R3: Escritura de Parquets a Escala en Computo Serverless

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/connect/storage/ (Almacenamiento en la nube con Unity Catalog - actualizada 20/01/2026)
2. https://spark.apache.org/docs/latest/sql-data-sources-parquet.html (Apache Spark - Parquet Data Source)
3. https://docs.databricks.com/aws/en/optimizations/ (Optimizaciones Databricks)

### Hallazgos Principales

#### H-V2-R3.1 - Escritura de Parquets en External Location

- **Patron recomendado**: Usar `df.write.mode("overwrite").parquet(ruta)` donde `ruta` es una ubicacion dentro de un External Location gobernado por Unity Catalog.
- **External Location**: Combina una ruta de almacenamiento en la nube (ADLS Gen2) con una credencial de almacenamiento (Storage Credential). Unity Catalog controla el acceso.
- **Acceso basado en rutas**: Databricks recomienda rutas de tabla o `/Volumes`, pero para archivos parquet crudos (landing zone), el acceso directo a External Location via ruta es valido y apropiado.
- **Compatibilidad Serverless**: `df.write.parquet()` es completamente compatible con Computo Serverless. No requiere configuracion adicional.
- **RESTRICCION SERVERLESS**: `spark.sparkContext.broadcast()` NO es compatible con Serverless. Se debe usar closures Python para distribuir datos a workers en `mapInPandas` (ver V2-R5).

#### H-V2-R3.2 - Optimizacion de Escritura a Escala

Para 5M/15M/5M registros, las mejores practicas son:

1. **Reparticion antes de escribir**: `df.repartition(n)` o `df.coalesce(n)` para controlar el numero de archivos de salida. Para 5M registros, entre 10-50 particiones es razonable. Para 15M registros, entre 30-100 particiones.
2. **Compresion por defecto**: Parquet usa compresion Snappy por defecto en Spark, que ofrece un buen balance entre velocidad y tamaño.
3. **Modo de escritura**: `mode("overwrite")` para reemplazar completamente el parquet existente en cada ejecucion.
4. **Esquema explicito**: Definir el esquema completo con `StructType` + `StructField` para evitar inferencia y garantizar tipos exactos (NUMERIC -> DoubleType/LongType, CHAR -> StringType, DATE -> DateType, TIMESTAMP -> TimestampType).
5. **Generacion distribuida**: Usar funciones de PySpark que operan a nivel de particion (`withColumn`, `lit`, `rand`, `expr`) en lugar de UDFs cuando sea posible, para maximizar paralelismo.

#### H-V2-R3.3 - Patron de Re-ejecucion con Mutacion (Maestro de Clientes)

Para el acenario de re-ejecucion del Maestro de Clientes (RF-004):

1. **Lectura del parquet existente**: `spark.read.parquet(ruta)` para obtener los registros previos.
2. **Identificacion de registros a mutar**: Usar `sample(fraction=0.20)` para seleccionar el 20% de registros que seran modificados.
3. **Aplicacion de mutaciones**: Usar `withColumn()` con funciones como `when`, `rand`, `array`, `element_at` para reemplazar valores en los campos demograficos.
4. **Generacion de nuevos clientes**: Calcular el 0.60% sobre la base existente, generar nuevos registros con CUSTID secuencial continuando desde `max(CUSTID) + 1`.
5. **Union y escritura**: `df_existentes_modificados.union(df_nuevos).write.mode("overwrite").parquet(ruta)`.

#### H-V2-R3.4 - Consideraciones de Rendimiento en Serverless

- Computo Serverless asigna recursos automaticamente; no hay control sobre el tamano del cluster.
- Para 5M de registros con 70 columnas: estimacion ~2-5 GB en parquet comprimido. Generacion estimada en 2-5 minutos en Serverless.
- Para 15M de registros con 60 columnas (numericos pesados): estimacion ~5-15 GB. Generacion estimada en 5-15 minutos.
- Para 5M de registros con 100 columnas: estimacion ~3-7 GB. Generacion estimada en 3-7 minutos.
- **Nota**: Los tiempos son estimaciones conservadoras. Serverless puede ser mas rapido o mas lento dependiendo de la carga y los recursos asignados.

### Decision Requerida - Research V2-R3

> **DECISION V2-R3-D1**: ~~Confirmar la~~ Estrategia de escritura de parquets: `df.write.mode("overwrite").parquet(ruta)` con esquema explicito `StructType`, reparticion controlada, y compresion Snappy por defecto. Para re-ejecucion del Maestro, leer parquet existente, aplicar mutaciones al 20% de registros, y generar nuevos clientes con CUSTID secuencial.
> - **Recomendacion IA**: Aprobar. Es el patron estandar de PySpark para escritura de parquets a escala. El esquema explicito garantiza las 70/60/100 columnas con tipos exactos. La mutacion via `sample()` + `withColumn()` es eficiente y distribuida.
> - **Alternativas consideradas**: (A) Usar Delta format directamente - mas rico en features (time travel, ACID) pero el spec requiere parquet plano para simular landing zone de AS400. (B) Usar pandas para generar datos - no escala a 5M/15M registros. (C) Usar Faker/generador nativo - no disponible en Serverless sin instalacion adicional.
> - **Decision del usuario**: APROBADA (2026-03-28). Justificacion: Es el patron estandar de PySpark a escala. Esquema explicito garantiza columnas con tipos exactos. Mutacion via sample() + withColumn() es eficiente y distribuida.

---

## Research V2-R4: Extensiones de Databricks para VS Code (Actualizacion)

### Fuentes Consultadas

1. https://marketplace.visualstudio.com/items?itemName=databricks.databricks (Marketplace VS Code)
2. https://marketplace.visualstudio.com/items?itemName=databricks.sqltools-databricks-driver (SQLTools Driver)

### Hallazgos

#### H-V2-R4.1 - Estado de las Extensiones

- **Databricks Extension for VS Code** (`databricks.databricks`): Vigente. Permite navegacion por workspace, ejecucion de notebooks remotos (incluido Serverless), sincronizacion de archivos, integracion con Unity Catalog.
- **Databricks Driver for SQLTools** (`databricks.sqltools-databricks-driver`): Vigente. Permite ejecucion de consultas SQL y exploracion de catalogos.

#### H-V2-R4.2 - Relevancia para V2

- Los notebooks .py de V2 pueden sincronizarse y ejecutarse remotamente via la extension de Databricks.
- Los notebooks de prueba TDD (tests/GenerarParquets/) pueden ejecutarse remotamente para validacion.
- No se detectan nuevas extensiones ni actualizaciones relevantes que impacten V2.

### Conclusion V2-R4

**Sin cambios respecto a V1**. Las decisiones R4-D1 (testing mixto) y R4-D2 (autenticacion Azure AD) siguen vigentes. No hay nuevos hallazgos.

---

## Registro de Decisiones V2

| ID | Tema | Opciones | Recomendacion IA | Decision Usuario | Estado |
|----|------|----------|------------------|------------------|--------|
| V2-R2-D1 | Tipo de widget principal | `text()` con conversion explicita | Aprobar `text()` como tipo principal | **APROBADA** (2026-03-28) | CERRADA |
| V2-R3-D1 | Estrategia de escritura parquets | `write.parquet()` + StructType + reparticion | Aprobar patron estandar PySpark | **APROBADA** (2026-03-28) | CERRADA |
| V2-R5-D1 | Prohibir spark.sparkContext en Serverless | Closures Python via cloudpickle | Aprobar — unica alternativa compatible | **APROBADA** (2026-03-28) | CERRADA |
| V2-R5-D2 | Protocolo abfss:// obligatorio | abfss:// en lugar de /mnt/ | Aprobar — estandar Unity Catalog | **APROBADA** (2026-03-28) | CERRADA |

### Decisiones Heredadas de V1 (Vigentes sin Cambios)

| ID | Tema | Estado |
|----|------|--------|
| R1-D1 a R1-D5 | LSDP: pyspark.pipelines, CDC, multi-catalogo, pivot, Delta props | VIGENTES |
| R2-D1 a R2-D4 | Estructuras AS400: Maestro 70, Transaccional 60, 15 tipos, Saldos 100 | VIGENTES |
| R3-D1, R3-D2 | Catalogos de nombres no latinos, nombres ambiguos | VIGENTES |
| R4-D1, R4-D2 | Testing mixto, autenticacion Azure AD | VIGENTES |
| R5-D1, R5-D2 | Conexion Azure SQL, utilidad en utilities/ | VIGENTES |

**Total decisiones nuevas**: 4 (APROBADAS 2026-03-28)
**Total decisiones heredadas**: 15 (todas vigentes)
**Total general**: 19/19 APROBADAS
**Total general**: 19/19 APROBADAS

---

## Research V2-R5: Compatibilidad con Computo Serverless — Restricciones de Acceso JVM (Post-Implementacion)

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/compute/serverless/ (Computo Serverless - Documentacion oficial Azure)
2. https://docs.databricks.com/aws/en/compute/serverless.html (Serverless compute - Databricks)

### Contexto

Durante las pruebas de ejecucion de los notebooks generadores en Computo Serverless, se detecto un error critico: `[JVM_ATTRIBUTE_NOT_SUPPORTED] Directly accessing the underlying Spark driver JVM using the attribute 'sparkContext' is not supported on serverless compute`. Este hallazgo afecto la implementacion original que usaba `spark.sparkContext.broadcast()` para distribuir datos a los workers en funciones `mapInPandas`.

### Hallazgos Principales

#### H-V2-R5.1 - APIs Prohibidas en Computo Serverless

El Computo Serverless de Databricks **bloquea el acceso directo al JVM del driver**. Las siguientes APIs estan **PROHIBIDAS**:

| API Prohibida | Error | Alternativa |
|---------------|-------|-------------|
| `spark.sparkContext.broadcast()` | JVM_ATTRIBUTE_NOT_SUPPORTED | Variables Python capturadas por closure (cloudpickle) |
| `spark.sparkContext.parallelize()` | JVM_ATTRIBUTE_NOT_SUPPORTED | `spark.createDataFrame()` o `spark.range()` |
| `spark.sparkContext.accumulator()` | JVM_ATTRIBUTE_NOT_SUPPORTED | Agregaciones con `groupBy()` o variables Python locales |
| `spark.sparkContext.setJobGroup()` | JVM_ATTRIBUTE_NOT_SUPPORTED | No aplica en Serverless — los jobs se gestionan automaticamente |
| `spark.sparkContext.addFile()` | JVM_ATTRIBUTE_NOT_SUPPORTED | Unity Catalog Volumes o rutas abfss:// |
| `spark.sparkContext.addPyFile()` | JVM_ATTRIBUTE_NOT_SUPPORTED | `%pip install` o cluster libraries |

#### H-V2-R5.2 - Patron de Distribucion de Datos con Closures (Alternativa a Broadcast)

Para funciones `mapInPandas` que necesitan datos de referencia en los workers, el patron correcto en Serverless es:

```python
# CORRECTO — Variables Python capturadas por closure
datos = {
    "catalogos": list(mi_lista),
    "rangos": dict(mis_rangos)
}

def mi_funcion_mapinpandas(iterador):
    # cloudpickle serializa automaticamente 'datos' al worker
    catalogos_local = datos["catalogos"]
    rangos_local = datos["rangos"]
    for pdf in iterador:
        # ... procesamiento con catalogos_local y rangos_local
        yield pdf

# INCORRECTO — spark.sparkContext.broadcast() -> ERROR en Serverless
# bc_datos = spark.sparkContext.broadcast(mi_lista)  # PROHIBIDO
```

**Mecanismo**: Cuando PySpark ejecuta `mapInPandas`, serializa la funcion Python y sus variables capturadas usando `cloudpickle`. Las variables referenciadas dentro de la funcion (closures) son automaticamente empaquetadas y enviadas a cada worker. Este mecanismo es transparente y compatible con Serverless.

**Rendimiento**: Para volumenes de datos de referencia pequenos a medianos (catalogos, listas, diccionarios < 100MB), el rendimiento es equivalente al broadcast. Para datos masivos, se recomienda usar un join distribuido en lugar de enviar datos a cada worker.

#### H-V2-R5.3 - Protocolo abfss:// como Estandar Obligatorio

Las rutas de almacenamiento deben usar el protocolo `abfss://` (Azure Blob File System Secure) en lugar de `/mnt/` (DBFS mounts):

| Aspecto | `/mnt/` (Legacy) | `abfss://` (Recomendado) |
|---------|-------------------|--------------------------|
| Compatibilidad Unity Catalog | Limitada | Completa |
| Gobernanza de acceso | Via DBFS mount point | Via Storage Credential + External Location |
| Soporte Serverless | Parcial (deprecado) | Completo |
| Formato | `/mnt/nombre-mount/ruta/` | `abfss://container@account.dfs.core.windows.net/ruta/` |
| Auditoria | Limitada | Completa via Unity Catalog |

### Decision Requerida - Research V2-R5

> **DECISION V2-R5-D1**: Prohibir el uso de `spark.sparkContext` y cualquier acceso directo al JVM en todos los notebooks del proyecto. Usar variables Python capturadas por closure como alternativa a `broadcast()` para distribuir datos en funciones `mapInPandas`.
> - **Recomendacion IA**: Aprobar. Es la unica alternativa compatible con Serverless. El rendimiento con closures es equivalente para los volumenes de datos de referencia utilizados (catalogos de nombres, rangos de montos, tipos de transaccion).
> - **Evidencia**: Error `JVM_ATTRIBUTE_NOT_SUPPORTED` confirmado en ejecucion real en Databricks Serverless (2026-03-28).
> - **Decision del usuario**: APROBADA (2026-03-28). Hallazgo critico de las pruebas de ejecucion en Computo Serverless. Se incorpora como regla permanente en la Constitucion del proyecto.

> **DECISION V2-R5-D2**: Adoptar el protocolo `abfss://` como estandar obligatorio para todas las rutas de lectura y escritura de parquets, reemplazando cualquier referencia a `/mnt/`.
> - **Recomendacion IA**: Aprobar. El protocolo `abfss://` es el estandar recomendado por Databricks y Microsoft para ADLS Gen2 con Unity Catalog.
> - **Evidencia**: Mejores practicas de Unity Catalog y External Locations de la documentacion oficial.
> - **Decision del usuario**: APROBADA (2026-03-28). Se incorpora como regla permanente en la Constitucion del proyecto.
