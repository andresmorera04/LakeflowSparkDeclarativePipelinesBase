# Quickstart — Version 4: LSDP Medalla de Plata

**Feature**: 004-lsdp-medalla-plata
**Fecha**: 2026-03-30

---

## Prerrequisitos

1. **Version 3 completada**: Las 3 tablas streaming de bronce (`cmstfl`, `trxpfl`, `blncfl`) deben existir y contener datos de al menos una ejecucion.
2. **Catalogo `plata_dev`**: Debe existir en Unity Catalog con los permisos necesarios para crear vistas materializadas.
3. **Esquema `regional`** (o el esquema configurado): Debe existir dentro del catalogo `plata_dev`.
4. **Clave `catalogoPlata`**: Debe estar insertada en la tabla `dbo.Parametros` de Azure SQL con el valor `"plata_dev"`.
5. **Extensiones VS Code**: Databricks Extension y SQLTools Driver instaladas y configuradas (Principio II).
6. **Computo Serverless** habilitado en el workspace de Databricks.

---

## Estructura de Archivos V4

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── __init__.py
│   ├── LsdpConexionAzureSql.py        # REFACTORIZAR: retornar todas las claves (RF-019)
│   ├── LsdpConstructorRutasAbfss.py    # Sin cambio
│   └── LsdpReordenarColumnasLiquidCluster.py  # Sin cambio
└── transformations/
    ├── LsdpBronceCmstfl.py             # Sin cambio (V3)
    ├── LsdpBronceTrxpfl.py             # Sin cambio (V3)
    ├── LsdpBronceBlncfl.py             # Sin cambio (V3)
    ├── LsdpPlataClientesSaldos.py      # NUEVO V4 — Vista materializada consolidada
    └── LsdpPlataTransacciones.py       # NUEVO V4 — Vista materializada transaccional

tests/LSDP_Laboratorio_Basico/
├── NbTestPlataClientesSaldos.py        # NUEVO V4 — Pruebas vista consolidada
├── NbTestPlataTransacciones.py         # NUEVO V4 — Pruebas vista transaccional
└── NbTestConexionAzureSqlV4.py         # NUEVO V4 — Pruebas refactorizacion Azure SQL
```

---

## Pasos de Implementacion

### Paso 1: Insertar clave en Azure SQL

Ejecutar en Azure SQL (via SQLTools):

```sql
INSERT INTO dbo.Parametros (Clave, Valor) VALUES ('catalogoPlata', 'plata_dev');
```

### Paso 2: Refactorizar `leer_parametros_azure_sql` (RF-019)

En `utilities/LsdpConexionAzureSql.py`, eliminar el filtro de 4 claves fijas y retornar el diccionario completo de `dbo.Parametros`. Los scripts de bronce existentes siguen funcionando sin cambio ya que acceden a las mismas claves.

### Paso 3: Crear script de vista consolidada (LsdpPlataClientesSaldos.py)

1. Importar utilities y `pyspark.pipelines as dp`
2. Leer parametros a nivel de modulo (closure pattern)
3. Definir funcion con `@dp.materialized_view` y `@dp.expect_or_drop`
4. Implementar window functions para Dimension Tipo 1
5. LEFT JOIN cmstfl + blncfl
6. Renombrar TODAS las columnas a espanol
7. Agregar 4 campos calculados (3 CASE + 1 SHA256)
8. Reordenar columnas para Liquid Cluster
9. Retornar DataFrame

### Paso 4: Crear script de vista transaccional (LsdpPlataTransacciones.py)

1. Importar utilities y `pyspark.pipelines as dp`
2. Leer parametros a nivel de modulo (closure pattern)
3. Definir funcion con `@dp.materialized_view`
4. Leer `bronce_dev.regional.trxpfl` via `spark.read.table()` (sin filtros)
5. Renombrar TODAS las columnas a espanol
6. Agregar 4 campos calculados numericos (con manejo de nulos)
7. Reordenar columnas para Liquid Cluster
8. Retornar DataFrame

### Paso 5: Crear pruebas TDD

Implementar notebooks de prueba para validar:
- Estructura (nombres columnas, tipos, conteo)
- Campos calculados (valores correctos para datos conocidos)
- Propiedades Delta (6 propiedades configuradas)
- Dimension Tipo 1 (una fila por cliente)
- Manejo de nulos (CUSTID eliminados, numericos con coalesce)
- Lectura de catalogoPlata desde dbo.Parametros

### Paso 6: Configurar el pipeline en Databricks

Agregar el nuevo parametro del pipeline:
```json
{
  "esquema_plata": "regional"
}
```

Agregar los nuevos archivos fuente al pipeline existente:
- `transformations/LsdpPlataClientesSaldos.py`
- `transformations/LsdpPlataTransacciones.py`

### Paso 7: Ejecutar y validar

1. Ejecutar el pipeline completo (bronce + plata)
2. Verificar que las vistas se crean en `plata_dev.regional`
3. Ejecutar los notebooks de prueba
4. Validar CE-001 a CE-013

---

## Parametro Nuevo del Pipeline

| Parametro | Valor | Descripcion |
|-----------|-------|-------------|
| `esquema_plata` | `"regional"` | Esquema destino en Unity Catalog para plata |

**Nota**: El catalogo de plata (`plata_dev`) NO es parametro del pipeline — se lee desde Azure SQL (clave `catalogoPlata`).

---

## Comandos de Verificacion Rapida (Databricks SQL)

```sql
-- Verificar que las vistas existen
SHOW TABLES IN plata_dev.regional;

-- Verificar conteo de columnas
DESCRIBE TABLE EXTENDED plata_dev.regional.clientes_saldos_consolidados;
DESCRIBE TABLE EXTENDED plata_dev.regional.transacciones_enriquecidas;

-- Verificar dimension tipo 1 (una fila por cliente)
SELECT identificador_cliente, COUNT(*) as cnt
FROM plata_dev.regional.clientes_saldos_consolidados
GROUP BY identificador_cliente
HAVING cnt > 1;
-- Resultado esperado: 0 filas

-- Verificar propiedades Delta
SHOW TBLPROPERTIES plata_dev.regional.clientes_saldos_consolidados;
SHOW TBLPROPERTIES plata_dev.regional.transacciones_enriquecidas;
```
