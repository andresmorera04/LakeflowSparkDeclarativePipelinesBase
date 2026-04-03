# Contrato del Pipeline LSDP - Medalla de Bronce

**Feature**: 003-lsdp-medalla-bronce
**Fecha**: 2026-03-29
**Tipo de Interfaz**: Pipeline Lakeflow Spark Declarative Pipelines (LSDP)

---

## Configuracion del Pipeline

### Catalogo y Esquema por Defecto

| Propiedad | Valor |
|-----------|-------|
| Catalogo | bronce_dev |
| Esquema | regional |

### Parametros de Entrada del Pipeline

El pipeline LSDP recibe los siguientes parametros configurables desde la interfaz de Databricks:

| Parametro | Tipo | Obligatorio | Ejemplo | Descripcion |
|-----------|------|-------------|---------|-------------|
| rutaCompletaMaestroCliente | string | Si | LSDP_Base/As400/MaestroCliente/ | Ruta relativa al directorio de parquets del Maestro de Clientes |
| rutaCompletaSaldoCliente | string | Si | LSDP_Base/As400/SaldoCliente/ | Ruta relativa al directorio de parquets de Saldos |
| rutaCompletaTransaccional | string | Si | LSDP_Base/As400/Transaccional/ | Ruta relativa al directorio de parquets Transaccionales |
| rutaCheckpointCmstfl | string | Si | LSDP_Base/Checkpoints/Bronce/cmstfl/ | Ruta relativa para schemaLocation del AutoLoader de cmstfl |
| rutaCheckpointTrxpfl | string | Si | LSDP_Base/Checkpoints/Bronce/trxpfl/ | Ruta relativa para schemaLocation del AutoLoader de trxpfl |
| rutaCheckpointBlncfl | string | Si | LSDP_Base/Checkpoints/Bronce/blncfl/ | Ruta relativa para schemaLocation del AutoLoader de blncfl |
| nombreScopeSecret | string | Si | sc-kv-laboratorio | Nombre del Scope Secret de Databricks para acceso a Azure Key Vault |

---

## Tablas de Salida

### bronce_dev.regional.cmstfl

| Propiedad | Valor |
|-----------|-------|
| Tipo | Streaming Table |
| Catalogo.Esquema | bronce_dev.regional (default) |
| Columnas | 72 iniciales (70 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns |
| Fuente | AutoLoader (parquet) |
| Modo Acumulacion | Append-only (historica) |
| Liquid Cluster | FechaIngestaDatos, CUSTID |
| Change Data Feed | Habilitado |
| autoOptimize | autoCompact + optimizeWrite habilitados |
| Retenciones | 30 dias (deleted files), 60 dias (log) |

### bronce_dev.regional.trxpfl

| Propiedad | Valor |
|-----------|-------|
| Tipo | Streaming Table |
| Catalogo.Esquema | bronce_dev.regional (default) |
| Columnas | 62 iniciales (60 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns |
| Fuente | AutoLoader (parquet) |
| Modo Acumulacion | Append-only (historica) |
| Liquid Cluster | TRXDT, CUSTID, TRXTYP |
| Change Data Feed | Habilitado |
| autoOptimize | autoCompact + optimizeWrite habilitados |
| Retenciones | 30 dias (deleted files), 60 dias (log) |

### bronce_dev.regional.blncfl

| Propiedad | Valor |
|-----------|-------|
| Tipo | Streaming Table |
| Catalogo.Esquema | bronce_dev.regional (default) |
| Columnas | 102 iniciales (100 AS400 + FechaIngestaDatos + _rescued_data) — sujeto a schema evolution via addNewColumns |
| Fuente | AutoLoader (parquet) |
| Modo Acumulacion | Append-only (historica) |
| Liquid Cluster | FechaIngestaDatos, CUSTID |
| Change Data Feed | Habilitado |
| autoOptimize | autoCompact + optimizeWrite habilitados |
| Retenciones | 30 dias (deleted files), 60 dias (log) |

---

## Dependencias Externas

### Conexion Azure SQL

| Propiedad | Valor |
|-----------|-------|
| Metodo | spark.read.format("jdbc") |
| Driver | com.microsoft.sqlserver.jdbc.SQLServerDriver |
| Secreto JDBC | sr-jdbc-asql-asqlmetadatos-adminpd (cadena JDBC sin password) |
| Secreto Password | sr-asql-asqlmetadatos-adminpd (contrasena) |
| Tabla | dbo.Parametros |
| Momento de Ejecucion | Nivel de modulo (una sola vez al inicio del pipeline, closure pattern) |

### Almacenamiento ADLS Gen2

| Propiedad | Valor |
|-----------|-------|
| Protocolo | abfss:// (exclusivamente) |
| Contenedor | Dinamico (leido de dbo.Parametros) |
| Storage Account | Dinamico (leido de dbo.Parametros) |
| Directorio Raiz | Dinamico (leido de dbo.Parametros) |
| External Location | Requerido en Unity Catalog |

---

## Configuracion del AutoLoader por Tabla

Todas las tablas usan la misma configuracion base del AutoLoader:

```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaLocation", ruta_abfss_checkpoint) \
    .load(ruta_abfss_parquets)
```

| Opcion | Valor | Justificacion |
|--------|-------|---------------|
| cloudFiles.format | parquet | Formato de los archivos fuente generados en V2 |
| cloudFiles.schemaEvolutionMode | addNewColumns | Incorpora columnas nuevas automaticamente, bronce tolerante |
| cloudFiles.schemaLocation | abfss:// dinamico | Ruta construida con params Azure SQL + parametro pipeline (R7-D1) |

**No se configura explicitamente**: `schema` (inferido automaticamente de parquets), `rescuedDataColumn` (incluido automaticamente como _rescued_data), `cloudFiles.inferColumnTypes` (tipos embebidos en parquet).

---

## Patron de Codigo de las Funciones de Utilidad

### LsdpConexionAzureSql.py (utilities/)

**Interfaz**:
- **Entrada**: spark (sesion), nombre_scope_secret (string)
- **Salida**: dict con claves {catalogoBronce, contenedorBronce, datalake, DirectorioBronce}
- **Responsabilidad**: Leer parametros de dbo.Parametros via JDBC combinando dos secretos
- **Ejecucion**: A nivel de modulo (closure pattern)

### LsdpConstructorRutasAbfss.py (utilities/)

**Interfaz**:
- **Entrada**: contenedor (string), datalake (string), directorio_raiz (string), ruta_relativa (string)
- **Salida**: string con la ruta abfss:// completa
- **Responsabilidad**: Construir rutas abfss:// conforme al patron `abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}`
- **Ejecucion**: Invocada a nivel de modulo para construir las 6 rutas (3 parquets + 3 checkpoints)

---

## Restricciones de Compatibilidad

| Restriccion | Detalle |
|-------------|---------|
| Computo | Serverless de Databricks exclusivamente |
| Prohibido | spark.sparkContext, import dlt, /mnt/, ZOrder, PartitionBy, pivot() |
| Requerido | Unity Catalog habilitado, plan Premium |
| Protocolo | Solo abfss:// para lectura/escritura |
| Idioma codigo | Espanol, snake_case minuscula |
| Nombres archivos | PascalCase con prefijo Lsdp (pipeline) o Nb (notebooks) |
