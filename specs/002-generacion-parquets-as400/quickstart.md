# Guia Rapida - Version 2: Generacion de Parquets Simulando Data AS400

**Feature**: 002-generacion-parquets-as400
**Fecha**: 2026-03-28
**Estado**: IMPLEMENTADA Y VERIFICADA — 4/4 decisiones APROBADAS. Suite TDD ejecutada exitosamente en Computo Serverless (2026-03-29). 3/3 notebooks generadores operativos, 3/3 suites de pruebas PASADAS al 100%.
**Base**: Research V1 (15/15 decisiones aprobadas) + Research V2 (4/4 decisiones aprobadas, incluyendo V2-R5-D1 y V2-R5-D2)

---

## Objetivo de esta Version

La Version 2 crea 3 notebooks Python (.py) que generan archivos parquet simulando los datos de AS400 para una entidad bancaria. Estos parquets sirven como landing zone para alimentar el pipeline LSDP en las versiones V3-V5. Los notebooks son independientes del Lakeflow Spark Declarative Pipelines (RF-015).

## Artefactos Generados

| Artefacto | Ruta | Descripcion |
|-----------|------|-------------|
| Especificacion | [spec.md](spec.md) | 4 historias de usuario, 18 requerimientos funcionales, 9 criterios de exito |
| Plan de Implementacion | [plan.md](plan.md) | Contexto tecnico, verificacion de constitucion, estructura del proyecto |
| Research V2 | [research.md](research.md) | 4 areas investigadas, 2 decisiones nuevas (pendientes) + 15 heredadas de V1 |
| Modelo de Datos | [data-model.md](data-model.md) | 5 entidades, mapeo AS400→PySpark, parametros por notebook, reglas de validacion |
| Checklist | [checklists/requirements.md](checklists/requirements.md) | Validacion de calidad de la especificacion — 16/16 items PASS |

## Restricciones Criticas de Plataforma

### Computo Serverless — APIs Prohibidas (V2-R5-D1)

| API Prohibida | Alternativa |
|---------------|-------------|
| `spark.sparkContext.broadcast()` | Variables Python capturadas por closure (cloudpickle) |
| `spark.sparkContext.parallelize()` | `spark.createDataFrame()` o `spark.range()` |
| `spark.sparkContext.*` (cualquier metodo) | Equivalente nativo PySpark sin acceso JVM |

> **CRITICO**: El error `[JVM_ATTRIBUTE_NOT_SUPPORTED]` se produce al intentar acceder a `spark.sparkContext` en Computo Serverless. Fue detectado en pruebas de ejecucion reales (2026-03-28). Ver [research.md](research.md) seccion V2-R5 para detalles completos.

### Protocolo de Almacenamiento (V2-R5-D2)

- **Obligatorio**: `abfss://container@storageaccount.dfs.core.windows.net/ruta/`
- **Prohibido**: `/mnt/` (DBFS mounts legacy)

## Prerrequisitos

### Infraestructura Requerida

1. **Azure Databricks** con Unity Catalog habilitado y Tier Premium
2. **Computo Serverless** activo y disponible
3. **External Location** configurado en Unity Catalog, conectado a un contenedor Azure Data Lake Storage Gen2
   - **IMPORTANTE**: Todas las rutas deben usar protocolo `abfss://` (V2-R5-D2). Formato: `abfss://container@storageaccount.dfs.core.windows.net/ruta/`
   - Esta **PROHIBIDO** el uso de rutas `/mnt/` (legacy DBFS mounts)
4. **Extensiones VS Code** instaladas:
   - Databricks extension for Visual Studio Code (`databricks.databricks`)
   - Databricks Driver for SQLTools (`databricks.sqltools-databricks-driver`)
5. **Conexion al workspace** configurada en la extension de VS Code

### Estructura de Directorios Requerida

```text
scripts/
└── GenerarParquets/
    ├── NbGenerarMaestroCliente.py
    ├── NbGenerarTransaccionalCliente.py
    └── NbGenerarSaldosCliente.py

tests/
└── GenerarParquets/
    ├── NbTestMaestroCliente.py
    ├── NbTestTransaccionalCliente.py
    └── NbTestSaldosCliente.py
```

## Ejecucion de los Notebooks

### Orden Obligatorio

```text
1. NbGenerarMaestroCliente.py       (PRIMERO - genera CUSTIDs base)
2. NbGenerarTransaccionalCliente.py (SEGUNDO - depende de CUSTIDs del Maestro)
3. NbGenerarSaldosCliente.py        (SEGUNDO - depende de CUSTIDs del Maestro)
```

> **Nota**: Los notebooks 2 y 3 pueden ejecutarse en cualquier orden despues del notebook 1, pero ambos dependen de que el Maestro de Clientes exista.

### Paso 1: Generar Maestro de Clientes

**Primera ejecucion** (5 millones de registros):
1. Abrir `scripts/GenerarParquets/NbGenerarMaestroCliente.py` en Databricks (o sincronizar via extension VS Code)
2. Configurar los widgets (parametros) en la interfaz del notebook:
   - `ruta_salida_parquet`: ruta en ADLS Gen2 con protocolo abfss:// (ej: `abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes`)
   - `cantidad_registros_base`: `5000000`
   - `offset_custid`: `100000001`
   - `pct_incremento`: `0.006`
   - `pct_mutacion`: `0.20`
   - `ruta_parquet_existente`: (dejar vacio para primera ejecucion)
   - `num_particiones`: `20` (numero de particiones del parquet de salida)
   - Rangos monetarios parametrizables: `rango_credlmt_min/max`, `rango_avlbal_min/max`, `rango_income_min/max`, `rango_loan_min/max`, `rango_ins_min/max` (valores por defecto preconfigurados)
3. Seleccionar **Computo Serverless** como tipo de computo
4. Ejecutar todas las celdas
5. Verificar que el parquet se genero correctamente en la ruta especificada

**Re-ejecuciones** (mutacion + nuevos clientes):
1. Configurar `ruta_parquet_existente` con la ruta del parquet generado previamente
2. Los demas parametros permanecen igual
3. Ejecutar — el notebook lee el parquet existente, muta el 20% de registros, y agrega 0.60% de clientes nuevos

### Paso 2: Generar Transaccional

1. Abrir `scripts/GenerarParquets/NbGenerarTransaccionalCliente.py`
2. Configurar los widgets:
   - `ruta_salida_parquet`: ruta del External Location para transaccional
   - `ruta_maestro_clientes`: ruta del parquet del Maestro generado en el Paso 1
   - `cantidad_registros`: `15000000`
   - `fecha_transaccion`: fecha deseada en formato `YYYY-MM-DD` (ej: `2026-01-15`)
   - `offset_trxid`: `1` (offset inicial del secuencial de TRXID)
   - `pesos_alta`: `60`, `pesos_media`: `30`, `pesos_baja`: `10`
   - Rangos de montos por tipo de transaccion (valores por defecto preconfigurados segun RF-017)
   - `num_particiones`: `50` (numero de particiones del parquet de salida)
3. Ejecutar con **Computo Serverless**
4. Cada ejecucion genera 15M registros nuevos (no acumulativos)

### Paso 3: Generar Saldos

1. Abrir `scripts/GenerarParquets/NbGenerarSaldosCliente.py`
2. Configurar los widgets:
   - `ruta_salida_parquet`: ruta del External Location para saldos
   - `ruta_maestro_clientes`: ruta del parquet del Maestro generado en el Paso 1
   - Rangos de montos por tipo de cuenta (valores por defecto preconfigurados)
   - `num_particiones`: `20` (numero de particiones del parquet de salida)
3. Ejecutar con **Computo Serverless**
4. Genera exactamente 1 registro de saldo por cada cliente del Maestro (relacion 1:1)

## Ejecucion de Pruebas TDD

1. Los notebooks de prueba estan en `tests/GenerarParquets/`
2. Ejecutar cada notebook de prueba en Databricks con **Computo Serverless**:
   - `NbTestMaestroCliente.py` — valida estructura (70 columnas), volumetria (5M), nombres no latinos, IDs unicos
   - `NbTestTransaccionalCliente.py` — valida estructura (60 columnas), volumetria (15M), integridad referencial, tipos de transaccion
   - `NbTestSaldosCliente.py` — valida estructura (100 columnas), relacion 1:1, cobertura 100% de clientes
3. Todas las pruebas deben pasar exitosamente (CE-007)

## Validaciones Clave

| Validacion | Que Verifica | Notebook Afectado |
|------------|-------------|-------------------|
| Estructura 70 columnas | Nombres AS400 aprobados (R2-D1) | Maestro |
| Estructura 60 columnas | Nombres AS400 aprobados (R2-D2) | Transaccional |
| Estructura 100 columnas | Nombres AS400 aprobados (R2-D4) | Saldos |
| Cero nombres latinos | Solo hebreos/egipcios/ingleses (R3-D1) | Maestro |
| 15 tipos de transaccion | Catalogo R2-D3 exclusivamente | Transaccional |
| Integridad referencial | CUSTID existe en Maestro | Transaccional, Saldos |
| Relacion 1:1 | Exactamente 1 saldo por cliente | Saldos |
| Cero hardcoded | Todo parametrizable via widgets | Todos |
| Distribucion TRXTYP | ~60%/~30%/~10% con tolerancia ±5pp | Transaccional |
| Tiempo de ejecucion | Menos de 10 minutos por notebook (CE-001, CE-002, CE-003) | Todos |

## Tiempos Estimados de Ejecucion (Computo Serverless)

| Notebook | Registros | Columnas | Estimacion | Meta (CE) |
|----------|-----------|----------|------------|-----------|
| Maestro de Clientes | 5,000,000 | 70 | 2-5 minutos | < 10 min (CE-001) |
| Transaccional | 15,000,000 | 60 | 5-10 minutos | < 10 min (CE-002) |
| Saldos | 5,000,000 | 100 | 3-7 minutos | < 10 min (CE-003) |

> **Nota**: Los tiempos son estimaciones conservadoras. Los 3 notebooks DEBEN completar en menos de 10 minutos cada uno en Computo Serverless. El rendimiento real depende de los recursos asignados.

## Dependencias con Versiones Futuras

| Version | Que Recibe de V2 | Como lo Usa |
|---------|------------------|-------------|
| V3 | 3 parquets en ADLS Gen2 | AutoLoader los ingesta a tablas Delta en Bronce via `@dp.table` |
| V4 | Tablas Delta de Bronce (via V3) | Vistas materializadas en Plata con campos calculados |
| V5 | Vistas de Plata (via V4) | Agregaciones en Oro por cliente |

## Resultados de Ejecucion TDD (2026-03-29)

> **Plataforma**: Azure Databricks con Computo Serverless
> **Entorno**: Primer ejecucion completa de los 3 generadores + 3 suites TDD

### NbTestMaestroCliente — 11/11 PASADAS ✓

| Prueba | Resultado |
|--------|-----------|
| Estructura 70 columnas (H2.1) | PASADO |
| Tipos de datos PySpark | PASADO |
| Unicidad CUSTID (5,000,000 unicos) | PASADO |
| Volumetria (5,000,000 registros) | PASADO |
| Nombres no latinos (9/9 campos) | PASADO |
| Parametros dinamicos | PASADO |
| Rechazo parametros invalidos (RF-016) | PASADO |
| CUSTNM = FRSTNM + LSTNM | PASADO |
| Auditoria cero hardcodeados (CE-008) | PASADO |
| Compatibilidad Serverless (V2-R5-D1) | PASADO |
| Protocolo abfss:// (V2-R5-D2) | PASADO |

### NbTestTransaccionalCliente — 13/13 PASADAS ✓

| Prueba | Resultado |
|--------|-----------|
| Estructura 60 columnas (H2.2) | PASADO |
| Tipos de datos PySpark | PASADO |
| Integridad referencial CUSTID (0 huerfanos) | PASADO |
| TRXTYP vs catalogo (15/15 tipos) | PASADO |
| Volumetria (15,000,000 registros) | PASADO |
| Prueba negativa sin Maestro (RF-016) | PASADO |
| Prueba negativa fecha invalida (RF-016) | PASADO |
| Distribucion pesos ~60/30/10 | PASADO |
| Rangos de montos por tipo (RF-017) | PASADO |
| Unicidad TRXID | PASADO |
| TRXTM horas/minutos/segundos | PASADO |
| Compatibilidad Serverless (V2-R5-D1) | PASADO |
| Protocolo abfss:// (V2-R5-D2) | PASADO |

### NbTestSaldosCliente — 12/12 PASADAS ✓

| Prueba | Resultado |
|--------|-----------|
| Estructura 100 columnas (H2.4) | PASADO |
| Tipos de datos PySpark | PASADO |
| Relacion 1:1 (N saldos = N clientes) | PASADO |
| Cero CUSTIDs huerfanos | PASADO |
| Cero CUSTIDs faltantes | PASADO |
| Unicidad CUSTID | PASADO |
| Prueba negativa sin Maestro (RF-016) | PASADO |
| Rangos por tipo de cuenta (RF-017) | PASADO |
| Cobertura 100% tipos de cuenta | PASADO |
| Cero nulos campos criticos | PASADO |
| Compatibilidad Serverless (V2-R5-D1) | PASADO |
| Protocolo abfss:// (V2-R5-D2) | PASADO |

### Resumen de Cobertura

| Criterio de Exito | Verificado Por | Estado |
|--------------------|---------------|--------|
| CE-001: Maestro < 10 min | Ejecucion en Serverless | ✓ |
| CE-002: Transaccional < 10 min | Ejecucion en Serverless | ✓ |
| CE-003: Saldos < 10 min | Ejecucion en Serverless | ✓ |
| CE-004: Integridad referencial CUSTID | NbTestTransaccionalCliente, NbTestSaldosCliente | ✓ |
| CE-005: Volumetria 5M / 15M / 5M | Las 3 suites | ✓ |
| CE-006: Estructura AS400 70/60/100 | Las 3 suites | ✓ |
| CE-007: 100% pruebas TDD pasan | 36/36 pruebas PASADAS | ✓ |
| CE-008: Cero valores hardcodeados | NbTestMaestroCliente | ✓ |

## Decisiones Aprobadas

| ID | Tema | Estado |
|----|------|--------|
| V2-R2-D1 | `dbutils.widgets.text()` como tipo principal de widget | APROBADA (2026-03-28) |
| V2-R3-D1 | Estrategia `write.parquet()` + StructType + reparticion | APROBADA (2026-03-28) |
| V2-R5-D1 | Prohibicion de spark.sparkContext en Computo Serverless | APROBADA (2026-03-28) |
| V2-R5-D2 | Protocolo abfss:// obligatorio para parquets | APROBADA (2026-03-28) |

**Todas las decisiones aprobadas (4/4)**. Detalle completo en [research.md](research.md).
