# Quickstart — Version 5: LSDP Medalla de Oro

**Feature**: 005-lsdp-medalla-oro
**Fecha**: 2026-03-31

---

## Prerrequisitos

1. **Version 4 completada**: Las 2 vistas materializadas de plata (`clientes_saldos_consolidados`, `transacciones_enriquecidas`) deben existir y contener datos de al menos una ejecucion.
2. **Catalogo `oro_dev`**: Debe existir en Unity Catalog con los permisos necesarios para crear vistas materializadas.
3. **Esquema `regional`** (o el esquema configurado): Debe existir dentro del catalogo `oro_dev`.
4. **Clave `catalogoOro`**: Debe existir en la tabla `dbo.Parametros` de Azure SQL con el valor `"oro_dev"` (ya registrada — no requiere INSERT).
5. **Extensiones VS Code**: Databricks Extension y SQLTools Driver instaladas y configuradas (Principio II).
6. **Computo Serverless** habilitado en el workspace de Databricks.

---

## Estructura de Archivos V5

```text
src/LSDP_Laboratorio_Basico/
├── utilities/                              # Sin cambios en V5
│   ├── __init__.py
│   ├── LsdpConexionAzureSql.py
│   ├── LsdpConstructorRutasAbfss.py
│   └── LsdpReordenarColumnasLiquidCluster.py
└── transformations/
    ├── LsdpBronceCmstfl.py                 # Sin cambio (V3)
    ├── LsdpBronceTrxpfl.py                 # Sin cambio (V3)
    ├── LsdpBronceBlncfl.py                 # Sin cambio (V3)
    ├── LsdpPlataClientesSaldos.py          # Sin cambio (V4)
    ├── LsdpPlataTransacciones.py           # Sin cambio (V4)
    └── LsdpOroClientes.py                  # NUEVO V5 — 2 vistas materializadas de oro

tests/LSDP_Laboratorio_Basico/
├── NbTestBronceCmstfl.py                   # Sin cambio (V3)
├── NbTestBronceTrxpfl.py                   # Sin cambio (V3)
├── NbTestBronceBlncfl.py                   # Sin cambio (V3)
├── NbTestPlataClientesSaldos.py            # Sin cambio (V4)
├── NbTestPlataTransacciones.py             # Sin cambio (V4)
└── NbTestOroClientes.py                    # NUEVO V5 — Pruebas de oro
```

---

## Pasos de Implementacion

### Paso 1: Verificar clave en Azure SQL

Verificar que la clave `catalogoOro` existe en la tabla `dbo.Parametros` de Azure SQL (ya registrada previamente). Ejecutar en Azure SQL (via SQLTools) para confirmar:

```sql
SELECT Clave, Valor FROM dbo.Parametros WHERE Clave = 'catalogoOro';
```

> **Nota:** La clave `catalogoOro` ya existe en `dbo.Parametros`. NO es necesario hacer INSERT.

### Paso 2: Crear script de oro (LsdpOroClientes.py)

Un unico archivo con ambas vistas materializadas y una sola lectura de parametros compartida:

1. Importar utilities (`leer_parametros_azure_sql`, `reordenar_columnas_liquid_cluster`) y `pyspark.pipelines as dp`
2. Leer parametros a nivel de modulo (closure pattern):
   - `nombre_scope_secret = spark.conf.get("pipelines.parameters.nombreScopeSecret")`
   - `parametros_sql = leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret)`
   - `catalogo_plata = parametros_sql["catalogoPlata"]`
   - `catalogo_oro = parametros_sql["catalogoOro"]`
   - `esquema_plata = spark.conf.get("pipelines.parameters.esquema_plata")`
   - `esquema_oro = spark.conf.get("pipelines.parameters.esquema_oro")`
3. **Vista 1 — `comportamiento_atm_cliente`**:
   - Decorar con `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente", ...)`
   - Decorar con `@dp.expect_or_drop("cliente_valido", "identificador_cliente IS NOT NULL")`
   - `spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")`
   - `groupBy("identificador_cliente").agg(...)` con 5 metricas condicionales
   - `reordenar_columnas_liquid_cluster(df, ["identificador_cliente"])`
   - Retornar DataFrame
4. **Vista 2 — `resumen_integral_cliente`**:
   - Decorar con `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente", ...)`
   - `spark.read.table(f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")` → seleccionar 17 campos
   - `spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")` → 5 metricas
   - LEFT JOIN por `identificador_cliente`
   - `coalesce` a 0 en las 5 metricas para clientes sin transacciones
   - `reordenar_columnas_liquid_cluster(df, ["huella_identificacion_cliente", "identificador_cliente"])`
   - Retornar DataFrame

### Paso 3: Crear pruebas TDD (NbTestOroClientes.py)

Implementar notebook de prueba para validar:
- Estructura (nombres columnas en espanol, tipos, conteo de columnas)
- Metricas agregadas (conteos, promedios, sumas con valores esperados)
- Propiedades Delta (6 propiedades configuradas)
- Dimension Tipo 1 (una fila por cliente en ambas vistas)
- Casos borde (clientes sin transacciones ATM/PGSL → metricas en 0)
- Expectation `@dp.expect_or_drop` (registros con `identificador_cliente` nulo eliminados)
- ~~Lectura de `catalogoOro` desde dbo.Parametros~~ *(CE-012/CE-013: validados via ejecucion del Pipeline LSDP, no desde el notebook)*
- LEFT JOIN con coalesce (clientes sin match → metricas en 0)

### Paso 4: Configurar el pipeline en Databricks

Agregar el nuevo parametro del pipeline al JSON de configuracion existente:

```json
{
  "nombreScopeSecret": "sc-kv-laboratorio",
  "esquema_plata": "regional",
  "esquema_oro": "regional"
}
```

Agregar el nuevo archivo fuente al pipeline existente:
- `transformations/LsdpOroClientes.py`

> **Nota:** Los archivos fuente de bronce (V3) y plata (V4) ya estan configurados. Solo se agrega el nuevo archivo de oro.

### Paso 5: Ejecutar y validar

1. Ejecutar el pipeline completo (bronce + plata + oro)
2. Verificar que las vistas se crean en `oro_dev.regional`
3. Verificar las 6 columnas de `comportamiento_atm_cliente`
4. Verificar las 22 columnas de `resumen_integral_cliente`
5. Ejecutar el notebook de prueba `NbTestOroClientes.py`
6. Validar CE-001 a CE-011 desde el notebook. CE-012 y CE-013 se validan por la ejecucion exitosa del Pipeline LSDP

---

## Parametro Nuevo del Pipeline

| Parametro | Valor | Descripcion |
|-----------|-------|-------------|
| `esquema_oro` | `"regional"` | Esquema destino en Unity Catalog para oro |

**Nota**: El catalogo de oro (`oro_dev`) NO es parametro del pipeline — se lee desde Azure SQL (clave `catalogoOro`).

---

## Archivos Modificados vs Nuevos

| Archivo | Accion | Descripcion |
|---------|--------|-------------|
| `LsdpOroClientes.py` | **NUEVO** | 2 vistas materializadas de oro |
| `NbTestOroClientes.py` | **NUEVO** | Suite de pruebas TDD para oro |
| utilities/ | Sin cambios | Se reutilizan sin modificacion |
| bronce/ | Sin cambios | Scripts V3 intactos |
| plata/ | Sin cambios | Scripts V4 intactos |

> **V5 no modifica ningun archivo existente.** Solo agrega 1 archivo de transformacion y 1 archivo de prueba.
