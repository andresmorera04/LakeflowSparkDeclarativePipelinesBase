# Research - Version 3: LSDP Medalla de Bronce

**Feature**: 003-lsdp-medalla-bronce
**Fecha**: 2026-03-29
**Estado**: TODAS LAS DECISIONES APROBADAS (2026-03-29)
**Base**: Decisiones aprobadas R1-D1 a R5-D2 de la Version 1

---

## Indice

1. [Research 6: Validacion de LSDP para Version 3](#research-6-validacion-de-lsdp-para-version-3)
2. [Research 7: AutoLoader con Parquets dentro de LSDP](#research-7-autoloader-con-parquets-dentro-de-lsdp)
3. [Research 8: Extensiones de Databricks para VS Code (Actualizacion)](#research-8-extensiones-de-databricks-para-vs-code-actualizacion)
4. [Registro de Decisiones Version 3](#registro-de-decisiones-version-3)

---

## Research 6: Validacion de LSDP para Version 3

### Fuentes Consultadas

1. https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-table (Referencia @dp.table - actualizada Ene 2026)
2. https://learn.microsoft.com/es-es/azure/databricks/ldp/limitations (Limitaciones del pipeline - actualizada Mar 2026)
3. https://docs.databricks.com/aws/en/ldp/developer/python-ref (Referencia Python API)

### Hallazgos Principales

#### H6.1 - Confirmacion de API @dp.table (Sin Cambios Respecto a V1)

La documentacion oficial confirma que la API `@dp.table` de `pyspark.pipelines` no presenta cambios respecto a los hallazgos documentados en la Version 1 (R1-D1 a R1-D5). Los parametros relevantes para la medalla de bronce se mantienen intactos:

- `name`: str - Nombre de la tabla (por defecto el nombre de la funcion)
- `comment`: str - Descripcion de la tabla
- `table_properties`: dict - Propiedades de la tabla Delta
- `cluster_by`: list - Columnas para Liquid Clustering
- `schema`: str/StructType - Definicion de esquema (opcional)

**Conclusion**: Todas las decisiones R1-D1 a R1-D5 siguen vigentes sin modificacion.

#### H6.2 - Nuevo Parametro `private` (Reemplazo de `temporary`)

La documentacion actualizada indica que el parametro `private` reemplaza al antiguo parametro `temporary` en el decorador `@dp.table`:

- **`private`** (bool): Crea una tabla que no se publica en el metastore. La tabla esta disponible dentro del pipeline pero no es accesible externamente. Las tablas privadas persisten durante el tiempo de vida del pipeline.
- **Impact en V3**: No aplica directamente ya que las tablas de bronce (cmstfl, trxpfl, blncfl) deben ser publicas para ser consumidas por las medallas de plata y oro en versiones futuras.

#### H6.3 - Nuevo Parametro `cluster_by_auto`

La documentacion introduce el parametro `cluster_by_auto` (bool):

- **Funcion**: Habilita el liquid clustering automatico en la tabla. Puede combinarse con `cluster_by` para definir columnas iniciales de agrupamiento, seguido de monitoreo y actualizacion automatica de claves.
- **Impact en V3**: La decision R1-D5 ya aprobo el uso de `cluster_by` con columnas explicitas. El `cluster_by_auto` es una alternativa que delega la seleccion de claves a Databricks. Para mantener control explicito sobre las claves de clustering, se recomienda continuar con `cluster_by` explicito.

#### H6.4 - Limitaciones Confirmadas (Sin Cambios)

Las limitaciones documentadas en H1.8 de la Version 1 se mantienen vigentes:

- Limite de 1000 archivos fuente por pipeline (usando carpetas)
- No se soporta `pivot()`
- No se pueden habilitar Iceberg reads en streaming tables
- Delta Lake time travel solo con streaming tables
- El modulo `pyspark.pipelines` solo disponible dentro del contexto de un pipeline
- Un dataset solo puede ser target de una operacion (excepto streaming tables con append flow)

**Nueva limitacion documentada**: 100 archivos de origen individuales por pipeline (o 50 entradas de archivos/carpetas que pueden referenciar hasta 1000 archivos indirectamente). Para V3 con solo 5 archivos fuente (2 utilities + 3 transformations), esta limitacion no aplica.

### Decisiones Requeridas - Research 6

> **DECISION R6-D1**: Confirmar que se continua con `cluster_by` explicito (campos definidos manualmente) en lugar de adoptar `cluster_by_auto` (seleccion automatica por Databricks) para las tablas de bronce.
> - **Recomendacion IA**: Continuar con `cluster_by` explicito. El control manual sobre las claves de clustering asegura que los patrones de consulta esperados para plata y oro sean optimizados desde bronce.
> - **Alternativa considerada**: `cluster_by_auto=True` delega la seleccion a Databricks basada en el workload, pero no hay workload previo en bronce que Databricks pueda analizar.
> - **Decision del usuario**: APROBADA (2026-03-29). Usar cluster_by explicito con las siguientes reglas:
>   - **cmstfl** (Maestro de Clientes): `cluster_by=["FechaIngestaDatos", "CUSTID"]`. FechaIngestaDatos primero (requisito del usuario), CUSTID como clave primaria y columna de join mas frecuente hacia transaccional y saldos.
>   - **trxpfl** (Transaccional): `cluster_by=["TRXDT", "CUSTID", "TRXTYP"]`. TRXDT (fecha de transaccion) primero (requisito del usuario), CUSTID como FK de join con maestro, TRXTYP como filtro frecuente por tipo de transaccion para analisis de ATM y pagos al saldo.
>   - **blncfl** (Saldos): `cluster_by=["FechaIngestaDatos", "CUSTID"]`. FechaIngestaDatos primero (requisito del usuario), CUSTID como FK de join con maestro y clave de dimension tipo 1 en plata.

---

## Research 7: AutoLoader con Parquets dentro de LSDP

### Fuentes Consultadas

1. https://learn.microsoft.com/es-es/azure/databricks/ingestion/cloud-object-storage/auto-loader/options (Opciones Auto Loader - actualizada Mar 2026)
2. https://learn.microsoft.com/es-es/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema (Inferencia y Evolucion de Esquema - actualizada Mar 2026)

### Hallazgos Principales

#### H7.1 - LSDP Gestiona Automaticamente schemaLocation y checkpointLocation

**Hallazgo critico**: La documentacion oficial confirma explicitamente que cuando se usa Auto Loader dentro de Lakeflow Spark Declarative Pipelines, Azure Databricks **administra automaticamente** la ubicacion del esquema (`cloudFiles.schemaLocation`) y demas informaciones de punto de control.

> Cita oficial: "Si usa Lakeflow Spark Declarative Pipelines, Azure Databricks administra automaticamente ubicacion del esquema y otras informaciones de punto de control."

**Implicacion**: En los scripts de bronce, NO se debe configurar `cloudFiles.schemaLocation` ni `checkpointLocation`. LSDP los gestiona internamente. Esto simplifica la configuracion del AutoLoader.

#### H7.2 - Comportamiento de Inferencia de Esquema para Parquet

La documentacion confirma que para el formato **Parquet**, los tipos de datos se codifican directamente en el esquema del propio archivo Parquet (a diferencia de JSON/CSV donde se infieren como cadenas).

**Implicacion**: No es necesario configurar `cloudFiles.inferColumnTypes` para archivos Parquet. Los tipos se preservan automaticamente del esquema embebido en los parquets generados por la Version 2.

#### H7.3 - Comportamiento de schemaEvolutionMode con Parquet

Para parquet sin esquema proporcionado:
- **`addNewColumns`** es el modo predeterminado cuando NO se proporciona un esquema explicitamente
- **`none`** es el modo predeterminado cuando SI se proporciona un esquema
- **Restriccion importantre**: `addNewColumns` NO es permitido cuando se proporciona un esquema explicitamente mediante el parametro `schema` del decorador

**Implicacion para V3**: Para cumplir con la decision de clarificacion (esquema inferido con addNewColumns + _rescued_data), NO se debe proporcionar el parametro `schema` en el decorador `@dp.table`. Esto asegura que:
1. El modo `addNewColumns` sea el predeterminado automatico
2. La columna `_rescued_data` se incluya automaticamente
3. Los tipos de datos se preserven del esquema embebido en los parquets

#### H7.4 - Columna _rescued_data Incluida Automaticamente

La documentacion confirma que cuando Auto Loader infiere el esquema (sin esquema explicitamente proporcionado), agrega automaticamente la columna `_rescued_data` al esquema. Esta columna captura:

- Columnas que faltan en el esquema
- Incompatibilidades de tipo
- Errores de coincidencia de mayusculas/minusculas
- Contiene un blob JSON con las columnas rescatadas y la ruta del archivo fuente

**Implicacion**: No es necesario configurar `rescuedDataColumn` explicitamente. La columna `_rescued_data` se incluira automaticamente.

#### H7.5 - Opciones Clave del AutoLoader para Parquet

Basado en la documentacion oficial, las opciones relevantes para la ingesta de parquets en bronce son:

| Opcion | Valor | Notas |
|--------|-------|-------|
| `cloudFiles.format` | `"parquet"` | Obligatorio. Define el formato de los archivos fuente. |
| `cloudFiles.schemaEvolutionMode` | (no configurar) | Valor predeterminado es `addNewColumns` cuando no se provee esquema. |
| `cloudFiles.schemaLocation` | (no configurar) | Gestionado automaticamente por LSDP. |
| `cloudFiles.includeExistingFiles` | (no configurar) | Valor predeterminado `true`, procesa archivos existentes en la primera ejecucion. |
| `cloudFiles.inferColumnTypes` | (no configurar) | Para Parquet, los tipos se codifican en el archivo. No aplica. |
| `rescuedDataColumn` | (no configurar) | Se incluye automaticamente como `_rescued_data`. |

**Conclusion (actualizada por decision R7-D1)**: La configuracion del AutoLoader dentro de LSDP para parquets en bronce es:

```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaLocation", ruta_abfss_checkpoint) \
    .load(ruta_abfss_parquets)
```

Donde `ruta_abfss_checkpoint` se construye dinamicamente combinando parametros de Azure SQL (contenedor, datalake, directorio raiz) con el parametro del pipeline (rutaCheckpoint por tabla). La columna `_rescued_data` se incluye automaticamente por la inferencia de esquema.

#### H7.6 - Conteo Real de Columnas con _rescued_data y FechaIngestaDatos

Dado que `_rescued_data` se incluye automaticamente y `FechaIngestaDatos` se agrega via `withColumn`, el conteo real de columnas por tabla es:

| Tabla | Cols AS400 | + FechaIngestaDatos | + _rescued_data | Total |
|-------|-----------|---------------------|-----------------|-------|
| cmstfl | 70 | +1 | +1 | 72 |
| trxpfl | 60 | +1 | +1 | 62 |
| blncfl | 100 | +1 | +1 | 102 |

### Decisiones Requeridas - Research 7

> **DECISION R7-D1**: Definir la configuracion del AutoLoader dentro de LSDP para las tablas de bronce.
> - **Recomendacion IA original**: Config minima (solo cloudFiles.format).
> - **Decision del usuario**: APROBADA CON MODIFICACIONES (2026-03-29). La configuracion del AutoLoader DEBE incluir explicitamente:
>   - `cloudFiles.format` = `"parquet"` (formato de los archivos fuente)
>   - `cloudFiles.schemaEvolutionMode` = `"addNewColumns"` (evolucion de esquema explicita)
>   - `cloudFiles.schemaLocation` = ruta abfss:// construida dinamicamente. La ruta especifica del checkpoint se recibe como parametro del pipeline LSDP y se combina con los valores de dbo.Parametros (contenedor, datalake, directorio raiz) para formar la ruta abfss:// completa.
>   - Esto implica 3 parametros adicionales del pipeline: `rutaCheckpointCmstfl`, `rutaCheckpointTrxpfl`, `rutaCheckpointBlncfl` (rutas relativas para el schemaLocation de cada tabla).
>   - La construccion de la ruta abfss:// del checkpoint sigue el mismo patron que las rutas de los parquets: `abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio}/{rutaCheckpoint}`.

> **DECISION R7-D2**: Confirmar que NO se proporcionara el parametro `schema` en el decorador `@dp.table` para las tablas de bronce, permitiendo que el esquema se infiera directamente de los parquets y que `addNewColumns` funcione como modo predeterminado.
> - **Recomendacion IA**: Aprobar. Proveer un esquema explicito deshabilitaria `addNewColumns` (modo predeterminado cambiaria a `none`) y requeriria mantener manualmente la definicion de 70/60/100 columnas AS400. La inferencia automatica es mas robusta y tolerante a cambios.
> - **Alternativa considerada**: Definir el esquema explicito con StructType para cada tabla. Esto daria control total pero es fragil ante cambios en los parquets fuente y contradice la decision de clarificacion de usar esquema inferido.
> - **Decision del usuario**: APROBADA (2026-03-29). Opcion A: NO proveer parametro schema. Se permite la inferencia automatica de los parquets, lo que mantiene addNewColumns como modo predeterminado y _rescued_data automatica.

---

## Research 8: Extensiones de Databricks para VS Code (Actualizacion)

### Fuentes Consultadas

1. https://marketplace.visualstudio.com/items?itemName=databricks.databricks (Marketplace VS Code)
2. https://learn.microsoft.com/es-es/azure/databricks/dev-tools/vscode-ext/ (Documentacion oficial Azure)

### Hallazgos Principales

#### H8.1 - Sin Cambios Relevantes para V3

Las extensiones de Databricks para VS Code documentadas en la Version 1 (R4-D1, R4-D2) se mantienen sin cambios funcionales relevantes para la Version 3:

- **Databricks Extension for VS Code**: Soporte para ejecucion de notebooks, sincronizacion con workspace, integracion con Unity Catalog y Computo Serverless.
- **SQLTools Databricks Driver**: Consultas SQL contra Databricks desde VS Code.

La estrategia de testing aprobada en R4-D1 (notebooks de prueba en Databricks para pipeline LSDP) sigue siendo la unica opcion viable para validar el pipeline, ya que `pyspark.pipelines` solo existe dentro del contexto de un pipeline.

**Conclusion**: No se requieren decisiones adicionales para extensiones de VS Code en V3.

---

## Registro de Decisiones Version 3

| ID | Tema | Opciones | Recomendacion IA | Decision Usuario | Estado |
|----|------|----------|------------------|------------------|--------|
| R6-D1 | cluster_by vs cluster_by_auto | Explicito vs automatico | cluster_by explicito | APROBADA | CERRADA |
| R7-D1 | Config AutoLoader en LSDP | Minima vs explicita | Config explicita (format + schemaEvolution + schemaLocation) | APROBADA CON MODIFICACIONES | CERRADA |
| R7-D2 | Schema en @dp.table bronce | Sin schema vs schema explicito | Sin schema (inferencia automatica) | APROBADA | CERRADA |

### Referencia: Decisiones V1 que se mantienen vigentes

| ID | Tema | Estado |
|----|------|--------|
| R1-D1 | Biblioteca pyspark.pipelines (no dlt) | APROBADA (V1) |
| R1-D2 | dp.create_auto_cdc_flow() para CDC en plata | APROBADA (V1) |
| R1-D3 | Nombres completos para multi-catalogo | APROBADA (V1) |
| R1-D4 | No uso de pivot() | APROBADA (V1) |
| R1-D5 | Bloque estandar de propiedades Delta | APROBADA (V1) |
| R2-D1 | Maestro Clientes 70 campos | APROBADA (V1) |
| R2-D2 | Transaccional 60 campos | APROBADA (V1) |
| R2-D3 | 15 tipos de transaccion | APROBADA (V1) |
| R2-D4 | Saldos 100 campos | APROBADA (V1) |
| R4-D1 | Testing: notebooks Databricks para LSDP | APROBADA (V1) |
| R4-D2 | Autenticacion: Azure AD + PAT | APROBADA (V1) |
| R5-D1 | Conexion Azure SQL: dos secretos + jdbc | APROBADA (V1) |
| R5-D2 | Utilidad Azure SQL en utilities/ | APROBADA (V1) |
