# Quickstart: Documentacion Tecnica y Modelado de Datos (Version 6)

**Branch**: `006-documentacion-tecnica-modelado` | **Fecha**: 2026-04-01

## Prerequisitos

- Codigo fuente completo del proyecto (Versiones 2–5 implementadas) en los directorios `src/`, `scripts/` y `tests/`.
- Acceso de lectura a todos los archivos del repositorio para extraer la informacion necesaria.
- Visor Markdown estandar para validar el renderizado (VS Code, GitHub o Azure DevOps).

## Estructura Final Esperada

```text
docs/
    ManualTecnico.md     # ~11 secciones principales, guia tecnica completa
    ModeladoDatos.md     # ~6 secciones principales, diccionario de datos completo
```

## Pasos de Implementacion

### Paso 1: Crear la carpeta docs/

Crear el directorio `docs/` en la raiz del proyecto si no existe (RF-001).

### Paso 2: Generar ManualTecnico.md

Crear el archivo `docs/ManualTecnico.md` con las 11 secciones definidas en RF-002 (ver [data-model.md](data-model.md) Entidad 1 para la estructura detallada):

1. Titulo y descripcion general del pipeline LSDP
2. Dependencias de infraestructura Azure (6 recursos)
3. Parametros (tabla dbo.Parametros + parametros del pipeline)
4. Notebooks de generacion de parquets (3 notebooks)
5. Utilidades LSDP (3 scripts)
6. Transformaciones — Medalla de Bronce (3 scripts)
7. Transformaciones — Medalla de Plata (2 scripts)
8. Transformaciones — Medalla de Oro (1 script, 2 vistas)
9. Paradigma declarativo LSDP
10. Propiedades Delta
11. Estrategia de pruebas TDD

Cada seccion requiere lectura del codigo fuente correspondiente en `src/`, `scripts/` y `tests/` para documentar con precision.

### Paso 3: Generar ModeladoDatos.md

Crear el archivo `docs/ModeladoDatos.md` con las 6 secciones definidas en RF-005 (ver [data-model.md](data-model.md) Entidad 2 para la estructura detallada):

1. Titulo y descripcion general del modelo de datos
2. Linaje de datos (diagrama ASCII + texto descriptivo — sujeto a decision R21-D1)
3. Archivos parquet — Fuente AS400 (3 parquets: 70, 60, 100 campos)
4. Streaming tables de bronce (3 tablas: 72, 62, 102 campos)
5. Vistas materializadas de plata (2 vistas: 175, 65 campos — organizadas por categoria sujeto a decision R20-D1)
6. Vistas materializadas de oro (2 vistas: 6, 22 campos)

### Paso 4: Validar renderizado

Verificar que ambos documentos se renderizan correctamente en un visor Markdown estandar:
- Tablas Markdown con alineacion consistente (pipes y guiones)
- Encabezados jerarquicos h1/h2/h3/h4
- Sin emojis
- Todo en espanol
- Bloques de codigo con sintaxis correcta

## Restricciones

- **No modificar** ningun archivo en `src/` ni `tests/` (RF-009).
- **No crear** archivos adicionales fuera de `docs/` (RF-001).
- Las decisiones pendientes R20-D1 y R21-D1 del [research.md](research.md) deben ser aprobadas por el usuario antes de la implementacion.

## Validacion Rapida

```
1. Verificar docs/ManualTecnico.md existe y se renderiza sin errores
2. Verificar docs/ModeladoDatos.md existe y se renderiza sin errores
3. Verificar que no se modificaron archivos en src/ ni tests/
4. Verificar que docs/ contiene exactamente 2 archivos
5. Verificar que todo esta en espanol y sin emojis
```
