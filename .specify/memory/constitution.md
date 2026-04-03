<!--
====================================================================
INFORME DE SINCRONIZACION (Sync Impact Report)
====================================================================

--- Enmienda 1.0.2 (2026-04-01) ---
- Cambio de version: 1.0.1 -> 1.0.2 (PATCH)
- Principios modificados:
  - Principio III (TDD): Se agrega excepcion explicita para
    versiones cuyo entregable sea exclusivamente documentacion
    Markdown sin codigo ejecutable (motivado por V6).
- Secciones agregadas: ninguna
- Secciones eliminadas: ninguna
- Plantillas verificadas:
  - .specify/templates/plan-template.md: sin cambios requeridos
  - .specify/templates/spec-template.md: sin cambios requeridos
  - .specify/templates/tasks-template.md: sin cambios requeridos
  - .specify/templates/checklist-template.md: sin cambios requeridos
  - .specify/templates/commands/: no existen archivos de comandos
- TODOs pendientes: ninguno

--- Enmienda 1.0.1 (2026-03-30) ---
- Cambio de version: 1.0.0 -> 1.0.1 (PATCH)
- Principios modificados:
  - Principio VI (Arquitectura Medallon): Se alinea el bullet
    de Transaccional en Plata con la decision R9-D1 de V4 —
    refresh_policy='auto' gestionado por LSDP en lugar de
    incrementalidad forzada.
- Secciones agregadas: ninguna
- Secciones eliminadas: ninguna
- Plantillas verificadas:
  - .specify/templates/plan-template.md: sin cambios requeridos
  - .specify/templates/spec-template.md: sin cambios requeridos
  - .specify/templates/tasks-template.md: sin cambios requeridos
  - .specify/templates/checklist-template.md: sin cambios requeridos
  - .specify/templates/commands/: no existen archivos de comandos
- TODOs pendientes: ninguno

--- Version inicial 1.0.0 (2026-03-27) ---
- Cambio de version: N/A -> 1.0.0 (version inicial)
- Principios creados:
  1. Dinamismo Absoluto (Cero Hard-Coded)
  2. Entorno de Desarrollo Databricks
  3. Desarrollo Guiado por Pruebas (TDD)
  4. Simulacion de Datos AS400 (Landing Zone)
  5. Lakeflow Spark Declarative Pipelines Estricto
  6. Arquitectura Medallon (Bronce, Plata, Oro)
  7. Estructura de Proyecto LSDP
  8. Gobernanza del Repositorio
  9. Gobernanza de la IA
  10. Idioma y Comunicacion
- Secciones adicionales:
  - Calidad del Spec-Driven Development
  - Flujo de Trabajo y Research
  - Gobernanza
- Plantillas verificadas:
  - .specify/templates/plan-template.md: verificada, sin conflictos
  - .specify/templates/spec-template.md: verificada, sin conflictos
  - .specify/templates/tasks-template.md: verificada, sin conflictos
  - .specify/templates/checklist-template.md: verificada, sin conflictos
  - .specify/templates/commands/: no existen archivos de comandos
- Secciones eliminadas: ninguna (version inicial)
- TODOs pendientes: ninguno
====================================================================
-->

# Constitucion del Proyecto LakeflowSparkDeclarativePipelinesBase

## Principios Fundamentales

### I. Dinamismo Absoluto (Cero Hard-Coded)

Todo el codigo DEBE manejar dinamismo por medio de parametros
claramente establecidos. El uso de valores constantes hardcodeadas
NO DEBE darse bajo ningun escenario. No existe excepcion alguna
a este principio.

- Cada valor configurable DEBE ser recibido como parametro,
  variable de entorno, secreto o registro de una tabla de
  configuracion.
- Las rutas de almacenamiento, nombres de catalogos, esquemas,
  credenciales y cualquier valor que pueda variar entre entornos
  DEBE ser parametrizado.

**Justificacion**: Garantizar portabilidad, mantenibilidad y
eliminacion de riesgos asociados a despliegues entre ambientes.

### II. Entorno de Desarrollo Databricks

Las siguientes extensiones de Visual Studio Code DEBEN estar
correctamente instaladas y configuradas como prerequisito del
proyecto:

- **Databricks extension for Visual Studio Code**: DEBE permitir
  acceder al workspace de Databricks, enlistar y usar los computos
  disponibles.
- **Databricks Driver for SQLTools**: DEBE estar instalada y
  operativa.

La configuracion de estas extensiones DEBE validarse antes de
iniciar cualquier desarrollo o ejecucion de pruebas.

**Justificacion**: Estas extensiones son la base para ejecutar
notebooks, pruebas y validar la compatibilidad del codigo con
el entorno Databricks desde el IDE local.

### III. Desarrollo Guiado por Pruebas (TDD)

Toda la solucion DEBE tener pruebas correctamente definidas y
ejecutables. Las pruebas DEBEN aprovechar las extensiones de
Databricks para Visual Studio Code mencionadas en el Principio II.

- El ciclo TDD (Red-Green-Refactor) DEBE aplicarse de forma
  estricta.
- Cada version del proyecto (excepto la Version 1 de research
  y versiones cuyo entregable sea exclusivamente documentacion
  Markdown sin codigo ejecutable) DEBE incluir su conjunto de
  pruebas.
- Las pruebas DEBEN verificar funcionalidad, compatibilidad con
  el entorno Serverless y optimizacion del codigo.

**Justificacion**: TDD asegura que cada componente sea verificable
de forma independiente y que los defectos se detecten antes de la
integracion.

### IV. Simulacion de Datos AS400 (Landing Zone)

Se DEBEN generar 3 archivos parquet que simulen la landing zone
del datalake con datos provenientes de AS400 para una entidad
bancaria. Estos scripts de Python DEBEN ejecutarse de forma
aislada como notebooks separados, excluidos del Lakeflow Spark
Declarative Pipelines.

- **Maestro de Clientes**: Nombres de campos similares a AS400
  con la misma cantidad de campos que las tablas originales.
  Primera ejecucion: 5 millones de registros. Ejecuciones
  subsecuentes: mismos 5 millones con datos modificados en
  algunos campos, mas un incremento del 0.60% de nuevos
  clientes. Los nombres y apellidos DEBEN ser exclusivamente
  hebreos, egipcios e ingleses. Bajo NINGUNA circunstancia se
  DEBEN usar nombres o apellidos latinos.
- **Transaccional**: 15 millones de registros nuevos por
  ejecucion. Relacionado con el Maestro de Clientes via
  identificador de cliente. DEBE incluir un campo de tipo de
  transaccion (Creditos por ATM, Debitos por ATM, Transferencias
  Externas, Transferencias Internas, Pagos al Saldo, Adelanto
  Salarial, entre otros). La fecha de transaccion se recibe via
  parametro (formato anno-mes-dia); horas, minutos y segundos
  se simulan.
- **Saldos**: Una linea por cliente (relacion 1:1). Cada
  ejecucion simula nuevamente y por completo los saldos,
  garantizando que todos los clientes del maestro tengan un
  registro de saldo.

**Justificacion**: Simular volumenes y estructuras reales de
AS400 permite validar el pipeline de extremo a extremo sin
depender de datos de produccion.

### V. Lakeflow Spark Declarative Pipelines Estricto

La solucion DEBE usar exclusivamente Lakeflow Spark Declarative
Pipelines. NO DEBE usarse Delta Live Tables bajo ninguna
circunstancia.

- Los scripts DEBEN ser codificados en PySpark/Python.
- Los scripts DEBEN ser ejecutados por Computo Serverless de
  forma estricta, por lo que DEBEN ser totalmente compatibles
  con este tipo de computo.
- El codigo generado DEBE aplicar estrictamente la filosofia
  del paradigma Lakeflow Spark Declarative Pipelines para la
  creacion de ETLs y Pipelines en Databricks.

**Justificacion**: LSDP es el paradigma oficial y vigente de
Databricks para pipelines declarativos. La compatibilidad con
Serverless elimina costos y complejidad de gestion de clusters.

### VI. Arquitectura Medallon (Bronce, Plata, Oro)

El procesamiento y transformacion de datos DEBE aprovechar al
maximo la arquitectura medallon con las siguientes estrategias
de acumulacion:

- **Bronce**: DEBE llevar marca de tiempo (campo
  FechaIngestaDatos) y acumular datos de forma historica,
  aprovechando al maximo el AutoLoader.
- **Plata**: DEBE usar vistas materializadas con dos patrones:
  - Maestro de Clientes + Saldos: consolidados en una sola
    vista materializada con comportamiento de Dimension Tipo 1
    (siempre datos mas recientes). El procesamiento DEBE cumplir
    con los mas altos niveles de optimizacion.
  - Transaccional: vista materializada separada que DEBE
    aprovechar la gestion automatica de actualizacion de LSDP
    (refresh_policy por defecto), permitiendo que el framework
    determine la estrategia optima de procesamiento.
  - Plata DEBE contemplar enriquecimiento de datos y calculos
    fila por fila generados con dos o mas campos de las tablas
    delta de Bronce.
- **Oro**: DEBE manejar vistas materializadas que siempre
  muestren la informacion mas reciente (Dimension Tipo 1).
  Oro DEBE ser una version altamente eficiente para consumo
  y con datos agregados.

**Justificacion**: La arquitectura medallon garantiza separacion
de responsabilidades, trazabilidad y optimizacion progresiva de
los datos para consumo analitico.

### VII. Estructura de Proyecto LSDP

En todo momento se DEBE respetar por completo la estructura de
archivos y carpetas que usa Lakeflow Spark Declarative Pipelines
(por ejemplo: carpetas utilities, transformations, explorations,
entre otras).

- La carpeta utilities DEBE contener funciones y metodos
  transversales que sigan el principio SOLID de responsabilidad
  unica y que sean reutilizables.
- La carpeta transformations DEBE contener los scripts
  principales del pipeline con funciones modulares.

**Justificacion**: Respetar la estructura estandar de LSDP
asegura compatibilidad con el runtime de Databricks y facilita
el mantenimiento del proyecto.

### VIII. Gobernanza del Repositorio

El manejo de branches de Git y GitHub DEBE ser controlado
exclusivamente por el usuario. Bajo NINGUNA circunstancia
speckit podra crear nuevos branches o hacer commits automaticos.

- Toda la gestion del repositorio y la estrategia de branches
  recae en el usuario.
- Speckit solo DEBE crear directorios convencionales dentro del
  proyecto.

**Justificacion**: La gestion del repositorio es responsabilidad
del ingeniero de datos, evitando conflictos y asegurando control
total sobre el historial de cambios.

### IX. Gobernanza de la IA

La IA NO DEBE tomar decisiones. Por cada research que se realice,
DEBE exigir la decision del usuario.

- La IA plantea una propuesta de cual es la mejor decision que
  considera, pero SOLO el usuario toma la decision final.
- La IA NO DEBE alucinar. DEBE mantenerse bajo el alcance
  estipulado por cada punto.
- La IA PUEDE hacer recomendaciones de mejora, pero SOLO las
  PUEDE aplicar bajo autorizacion del usuario.
- Al iniciar una nueva version o incremento del proyecto, SIEMPRE
  se DEBE hacer un research de Lakeflow Spark Declarative
  Pipelines, de los parquets simuladores de datos de AS400 y de
  las extensiones de Databricks para VS Code que permitan ejecutar
  las pruebas necesarias.
- Si el research encuentra nuevos hallazgos u opciones que
  previamente no se han decidido, el usuario DEBE aprobarlas o
  rechazarlas antes de continuar.

**Justificacion**: El usuario es la autoridad final en todas las
decisiones del proyecto. La IA actua como herramienta de soporte,
no como decisor.

### X. Idioma y Comunicacion

Todas las respuestas de la IA DEBEN estar en idioma espanol,
incluyendo las respuestas de los slash commands que se usen,
incluyendo los de spec-kit.

- Para los research se DEBEN usar los links oficiales de Azure
  Databricks, Python y Spark.
- Para el research de Lakeflow Spark Declarative Pipelines, la
  referencia principal DEBE ser la documentacion oficial de
  Azure Databricks en:
  https://learn.microsoft.com/es-es/azure/databricks/ldp/
  complementada con la referencia de Python API:
  https://docs.databricks.com/aws/en/ldp/developer/python-ref
- Para ejemplos, se DEBE basar en primera instancia en links
  oficiales y como segunda opcion en links de Medium y
  StackOverflow.

**Justificacion**: Mantener consistencia idiomatica facilita la
comprension del equipo de ingenieria y asegura que las fuentes
de informacion sean confiables y verificables.

## Calidad del Spec-Driven Development

Cualquier sugerencia que haga la IA para mejorar el Spec-Driven
Development de la verdad del proyecto (archivos constitution.md,
spec.md, plan.md y demas artefactos) DEBE aplicarse de forma
general asegurando consistencia en todos los archivos. La IA
DEBE aplicar los cambios unicamente cuando el usuario los
apruebe.

- Si el SDD esta poco conciso o tiene oportunidad de mejora
  porque le falta especificar mas, la IA DEBE informar
  inmediatamente.
- La IA PUEDE hacer sugerencias que permitan detallar mas y
  generar un SDD mas completo, claro y sin ambiguedad.
- Las validaciones y mejoras DEBEN mantener coherencia entre
  todos los artefactos del SDD.

## Flujo de Trabajo y Research

El flujo de trabajo para cada version o incremento del proyecto
DEBE seguir este proceso:

1. **Research obligatorio**: Al iniciar cada version, realizar
   investigacion actualizada sobre LSDP, simulacion de datos
   AS400 y extensiones de Databricks para VS Code.
2. **Presentacion de hallazgos**: La IA DEBE presentar los
   hallazgos y opciones al usuario de forma clara.
3. **Decision del usuario**: El usuario DEBE tomar las decisiones
   sobre los hallazgos antes de continuar con el proceso.
4. **Ejecucion**: Solo despues de las decisiones del usuario se
   procede con la implementacion.
5. **Validacion**: Cada entrega DEBE validarse contra los
   principios de esta constitucion.

## Gobernanza

Esta constitucion es el documento rector del proyecto
LakeflowSparkDeclarativePipelinesBase. Todos los artefactos
generados por spec-kit (spec.md, plan.md, tasks.md y demas)
DEBEN cumplir con los principios aqui establecidos.

### Procedimiento de Enmienda

- Cualquier modificacion a esta constitucion DEBE ser propuesta
  por la IA y aprobada por el usuario antes de aplicarse.
- Toda enmienda DEBE documentarse en el Informe de Sincronizacion
  al inicio de este archivo.
- Las enmiendas DEBEN propagarse a las plantillas y artefactos
  dependientes para mantener consistencia.

### Politica de Versionamiento

- Se usa versionamiento semantico (MAJOR.MINOR.PATCH):
  - MAJOR: Eliminacion o redefinicion incompatible de principios.
  - MINOR: Adicion de nuevos principios o expansion material de
    los existentes.
  - PATCH: Clarificaciones, correcciones de redaccion y
    refinamientos no semanticos.

### Revision de Cumplimiento

- Antes de cada fase de implementacion (Phase 0 en plan.md), se
  DEBE verificar cumplimiento con esta constitucion (Constitution
  Check en plan-template.md).
- Toda PR o revision de codigo DEBE validar adherencia a los
  principios aqui definidos.

**Version**: 1.0.1 | **Ratificada**: 2026-03-27 | **Ultima Enmienda**: 2026-03-30
