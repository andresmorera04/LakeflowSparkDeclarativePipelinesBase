# Research: Documentacion Tecnica y Modelado de Datos (Version 6)

**Branch**: `006-documentacion-tecnica-modelado` | **Fecha**: 2026-04-01
**Decisiones previas vigentes**: R1-D1 a R5-D2 (V1), R6-D1 a R8-D1 (V3), R9-D1 a R12 (V4), R13-D1 a R15-D1 (V5) — 18 decisiones aprobadas

---

## Research 17: Verificacion Rutinaria de Lakeflow Spark Declarative Pipelines (V6)

**Contexto**: La constitucion (Principio IX) requiere un research de LSDP al iniciar cada nueva version. En V6 no se genera codigo LSDP; el entregable es documentacion exclusivamente.

**Fuente**: https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/python-dev, Resultados previos R1-D1 (V1), R6-D1 a R8-D1 (V3), R9-D1 a R11 (V4), R13-D1 a R15-D1 (V5).

### Hallazgo H17.1: Sin Cambios en la API de LSDP desde V5

La API de LSDP sigue usando `from pyspark import pipelines as dp` con los decoradores `@dp.table` (para streaming tables) y `@dp.materialized_view` (para vistas materializadas batch). No se han identificado cambios de breaking change ni nuevas funcionalidades que afecten la documentacion del codigo existente (V2-V5).

Los patrones vigentes a documentar en el ManualTecnico.md permanecen iguales:
- Decoradores `@dp.table` y `@dp.materialized_view`
- Expectativas `@dp.expect_or_drop`
- Lectura intra-pipeline con `spark.read.table()`
- Parametrizacion por closure (alternativa a broadcast para Computo Serverless)
- Configuracion de catalogo/esquema diferente al por defecto via `spark.read.table("catalogo.esquema.tabla")`

**Conclusion**: Sin nuevos hallazgos ni opciones que requieran decision del usuario. La documentacion se basa en la API vigente y las 18 decisiones aprobadas.

---

## Research 18: Verificacion Rutinaria de Parquets Simuladores AS400 (V6)

**Contexto**: La constitucion (Principio IX) requiere un research de los parquets simuladores de datos de AS400 al iniciar cada nueva version. En V6 no se modifican los generadores; el entregable es documentacion de los mismos.

**Fuente**: Codigo fuente de scripts/GenerarParquets/ (NbGenerarMaestroCliente.py, NbGenerarTransaccionalCliente.py, NbGenerarSaldosCliente.py). Resultados previos R2-D1, R3-D1, R5-D1, R5-D2 (V1).

### Hallazgo H18.1: Generadores Estables desde V2

Los 3 notebooks de generacion de parquets implementados en V2 permanecen sin modificaciones:
- **NbGenerarMaestroCliente.py**: 17 widgets, 70 columnas, 5 millones de registros base con incremento de 0.60%.
- **NbGenerarTransaccionalCliente.py**: 33 widgets, 60 columnas, 15 millones de registros por ejecucion.
- **NbGenerarSaldosCliente.py**: 11 widgets, 100 columnas, relacion 1:1 con maestro de clientes.

No se han realizado cambios en la estructura de los parquets ni en los campos AS400 desde V2. Los nombres de campos AS400 (CUSTID, FRSTNM, LSTNM, TRXTYP, TRXDT, TRXTM, etc.) se mantienen vigentes.

**Conclusion**: Sin nuevos hallazgos. La documentacion del ModeladoDatos.md se basa en el estado actual de los generadores.

---

## Research 19: Verificacion Rutinaria de Extensiones Databricks para VS Code (V6)

**Contexto**: La constitucion (Principio IX) requiere un research de las extensiones de Databricks para VS Code al iniciar cada nueva version. En V6, la seccion de Estrategia de Pruebas TDD del ManualTecnico.md (RF-002.11) debe explicar como se ejecutan las pruebas usando estas extensiones.

**Fuente**: Resultados previos R4-D1, R4-D2 (V1), R8-D1 (V3), R12 (V4), R16 (V5).

### Hallazgo H19.1: Sin Cambios Significativos desde V5

Las extensiones requeridas por la constitucion (Principio II) siguen siendo:
- **Databricks extension for Visual Studio Code**: Permite acceder al workspace, enlistar computos, ejecutar notebooks y pruebas.
- **Databricks Driver for SQLTools**: Para consultas SQL directas al catalogo.

La estrategia de testing mixta (R4-D2) se mantiene vigente: pruebas ejecutadas como notebooks en Databricks a traves de la extension de VS Code.

**Conclusion**: Sin nuevos hallazgos. La documentacion de la seccion TDD se basa en las extensiones y la estrategia de testing ya aprobadas.

---

## Research 20: Organizacion de Tablas con Gran Cantidad de Columnas en Markdown

**Contexto**: El ModeladoDatos.md debe documentar entidades con alto numero de columnas: parquet de saldos (100 campos), streaming table blncfl (102 campos), y la vista materializada clientes_saldos_consolidados (175 columnas). El caso borde del spec (item 4) contempla este escenario y sugiere agrupar logicamente.

**Fuente**: Mejores practicas de documentacion tecnica en Markdown, compatibilidad con visores GitHub/VS Code/Azure DevOps.

### Hallazgo H20.1: Tablas Markdown Extensas y Renderizado

Las tablas Markdown con mas de 50 filas son dificiles de navegar en visores estandar (GitHub, VS Code) ya que no tienen controles de paginacion ni filtros. Sin embargo, se renderizan correctamente sin errores.

### Hallazgo H20.2: Alternativas de Organizacion

**Alternativa A — Tabla Unica por Entidad**:
- Todas las columnas en una sola tabla Markdown por entidad.
- Ventaja: Busqueda simple con Ctrl+F en el visor.
- Desventaja: Tablas muy largas (100-175 filas) que son dificiles de navegar linealmente.

**Alternativa B — Tablas Agrupadas por Categoria**:
- Dividir columnas en subtablas por grupo logico (campos identificativos, campos demograficos, campos financieros, campos de fecha, campos calculados).
- Ventaja: Navegacion mas intuitiva con encabezados h4 por grupo.
- Desventaja: Un campo especifico requiere saber a que grupo pertenece.

**Alternativa C — Hibrida**:
- Tabla unica pero con separadores visuales (filas de comentario) entre grupos logicos.
- Compatibilidad limitada con algunos visores.

### Decision R20-D1: Organizacion de Columnas en ModeladoDatos.md

- **Propuesta de la IA**: Alternativa B (tablas agrupadas por categoria con subsecciones h4).
- **Decision del usuario**: Alternativa A (tabla unica por entidad).
- **Razon del usuario**: La busqueda simple con Ctrl+F es prioritaria. Todas las columnas de una entidad en una sola tabla Markdown.
- **Alternativas consideradas**: (A) Tabla unica. (B) Agrupadas por categoria con h4. (C) Hibrida con separadores.
- **Estado**: **APROBADA** (2026-04-01)

---

## Research 21: Representacion del Linaje de Datos en Markdown

**Contexto**: RF-005.2 del spec requiere una seccion de Linaje de Datos al inicio del ModeladoDatos.md. La clarificacion Q1 (sesion 2026-04-01) confirmo que debe incluir descripcion textual del flujo entre capas y las claves de relacion (CUSTID).

**Fuente**: Mejores practicas de documentacion de linaje de datos, compatibilidad con visores Markdown estandar.

### Hallazgo H21.1: Alternativas de Representacion

**Alternativa A — Texto Descriptivo Unicamente**:
- Parrafos explicando el flujo de datos entre capas, las entidades involucradas y las claves de relacion.
- Ventaja: Maxima compatibilidad con cualquier visor Markdown.
- Desventaja: Puede ser menos visual e intuitivo.

**Alternativa B — Diagrama ASCII Art + Texto**:
- Un diagrama simple con caracteres ASCII mostrando el flujo entre capas, complementado con texto descriptivo.
- Ejemplo:
  ```
  Parquets AS400          Bronce               Plata                    Oro
  ┌─────────────┐   ┌──────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
  │MaestroClient│──>│cmstfl        │──┐                       │   │                      │
  │CMSTFL (70)  │   │(72 cols)     │  ├>│clientes_saldos_     │──>│resumen_integral_     │
  │SaldoCliente │──>│blncfl        │──┘ │consolidados (175)   │   │cliente (22 cols)     │
  │BLNCFL (100) │   │(102 cols)    │    └──────────────────────┘   │                      │
  │Transaccional│──>│trxpfl        │   ┌──────────────────────┐   ┌──────────────────────┐
  │TRXPFL (60)  │   │(62 cols)     │──>│transacciones_        │──>│comportamiento_atm_   │
  └─────────────┘   └──────────────┘   │enriquecidas          │   │cliente (6 cols)      │
                                       └──────────────────────┘   └──────────────────────┘
  ```
- Ventaja: Visual e intuitivo, compatible con cualquier visor (es texto plano dentro de un bloque de codigo).
- Desventaja: Dificil de mantener si la estructura cambia.

**Alternativa C — Diagrama Mermaid + Texto**:
- Un diagrama Mermaid renderizable en GitHub y VS Code.
- Ventaja: Visual y facil de mantener.
- Desventaja: No todos los visores Markdown renderizan Mermaid (Azure DevOps no lo soporta nativamente).

### Decision R21-D1: Formato de Linaje de Datos en ModeladoDatos.md

- **Propuesta de la IA**: Alternativa B (diagrama ASCII art dentro de bloque de codigo + texto descriptivo).
- **Decision del usuario**: Alternativa C (diagrama Mermaid visualizable en mermaid.live).
- **Razon del usuario**: El diagrama Mermaid permite visualizacion interactiva en mermaid.live. La compatibilidad con Azure DevOps no es prioritaria.
- **Alternativas consideradas**: (A) Texto unicamente. (B) ASCII art + texto. (C) Mermaid + texto.
- **Estado**: **APROBADA** (2026-04-01)

---

## Resumen de Decisiones

| ID | Research | Propuesta | Estado |
|----|----------|-----------|--------|
| R20-D1 | Organizacion de columnas en ModeladoDatos.md | Tabla unica por entidad | APROBADA |
| R21-D1 | Formato de linaje de datos en ModeladoDatos.md | Diagrama Mermaid (mermaid.live) | APROBADA |

**Decisiones de verificacion rutinaria (R17, R18, R19)**: Sin nuevos hallazgos — no requieren decision del usuario.

**Total decisiones nuevas aprobadas (V6)**: 2
**Total decisiones acumuladas del proyecto**: 18 (V1-V5) + 2 aprobadas (V6) = 20
