# LakeflowSparkDeclarativePipelinesBase

> **Plataforma exclusiva: Microsoft Azure** — Este laboratorio está diseñado y validado únicamente sobre el ecosistema Azure. No es compatible con otros proveedores cloud ni con entornos on-premise.

[![Platform](https://img.shields.io/badge/Platform-Azure%20Databricks-0078D4?style=flat-square&logo=microsoftazure)](https://azure.microsoft.com/en-us/products/databricks)
[![Runtime](https://img.shields.io/badge/Compute-Serverless-blueviolet?style=flat-square)](https://docs.databricks.com/en/compute/serverless/index.html)
[![Framework](https://img.shields.io/badge/Pipeline-Lakeflow%20LSDP-orange?style=flat-square)](https://docs.databricks.com/en/dlt/index.html)
[![Language](https://img.shields.io/badge/Language-PySpark%20%2F%20Python-3776AB?style=flat-square&logo=python)](https://spark.apache.org/docs/latest/api/python/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-gold?style=flat-square)](https://www.databricks.com/glossary/medallion-architecture)
[![AI Assisted](https://img.shields.io/badge/AI%20Assisted-GitHub%20Copilot-181717?style=flat-square&logo=github)](https://github.com/features/copilot)
[![Methodology](https://img.shields.io/badge/Methodology-Spec--Kit%20%2B%20SDD-success?style=flat-square)](./SYSTEM.md)

---

## ¿Qué es este proyecto?

**LakeflowSparkDeclarativePipelinesBase** es un laboratorio de referencia que implementa un pipeline de datos end-to-end sobre **Azure Databricks** usando **Lakeflow Spark Declarative Pipelines (LSDP)** con arquitectura **Medallion (Bronce / Plata / Oro)**.

El objetivo de negocio es construir un producto de datos que permita al área de clientes de una entidad bancaria analizar el comportamiento de sus clientes con respecto a saldos y uso de cajeros automáticos (ATM): volumen de depósitos y retiros, promedios de montos y pagos al saldo, con granularidad por cliente.

Los datos de origen simulan un sistema **AS400 bancario** (Maestro de Clientes, Transaccional y Saldos) materializados como archivos **Parquet** en **Azure Data Lake Storage Gen2**, los cuales son consumidos de forma incremental mediante **AutoLoader** en la capa Bronce y transformados progresivamente hasta la capa Oro para consumo analítico.

---

## Aviso de Plataforma

> **Este laboratorio es exclusivo de Azure.**
>
> Todos los componentes del stack — almacenamiento, cómputo, configuración de secretos y catálogo de datos — están construidos sobre servicios nativos de Microsoft Azure:
> - **Azure Data Lake Storage Gen2** para el almacenamiento del Data Lake
> - **Azure SQL (Free Tier)** para la tabla de parámetros de configuración `dbo.Parametros`
> - **Azure Key Vault** para la gestión segura de secretos de conexión
> - **Azure Databricks Premium** con Unity Catalog habilitado
>
> El uso del protocolo `abfss://` es obligatorio en todo el proyecto. El uso de rutas `/mnt/` (DBFS mounts legacy) está estrictamente prohibido.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Azure Data Lake Storage Gen2                       │
│                                                                       │
│  [bronce]  ──►  Parquets AS400 (CMSTFL, TRXPFL, BLNCFL)             │
│  [plata]   ──►  Reservado (vistas materializadas)                     │
│  [oro]     ──►  Reservado (vistas materializadas)                     │
│  [control] ──►  Metadatos y checkpoints de AutoLoader                 │
└─────────────────────────────────────────────────────────────────────┘
           │  abfss://  (AutoLoader Streaming)
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│          Lakeflow Spark Declarative Pipelines (LSDP)                 │
│                                                                       │
│  BRONCE ──► Streaming Tables (append-only + FechaIngestaDatos)       │
│     │       bronce_dev.regional.cmstfl                               │
│     │       bronce_dev.regional.trxpfl                               │
│     │       bronce_dev.regional.blncfl                               │
│     │                                                                 │
│  PLATA ──► Vistas Materializadas                                     │
│     │       plata_dev.regional.clientes_saldos_consolidados          │
│     │       plata_dev.regional.transacciones_enriquecidas            │
│     │                                                                 │
│  ORO   ──► Vistas Materializadas (consumo analítico)                 │
│             oro_dev.regional.resumen_integral_cliente                │
│             oro_dev.regional.comportamiento_atm_cliente              │
└─────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Azure SQL (dbo.Parametros)  +  Azure Key Vault (secretos JDBC)      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisitos de Infraestructura Azure

Antes de ejecutar cualquier notebook o pipeline, los siguientes recursos deben estar aprovisionados y configurados en Azure:

| Recurso Azure | Configuración Requerida |
|---|---|
| **Azure Data Lake Storage Gen2** | Storage Account con namespace jerárquico. Contenedores: `bronce`, `plata`, `oro`, `control` |
| **Azure SQL Free Tier** | Base de datos `Configuracion` con tabla `dbo.Parametros` (estructura clave-valor) cargada |
| **Azure Key Vault** | Secretos `sr-jdbc-asql-asqlmetadatos-adminpd` y `sr-asql-asqlmetadatos-adminpd` creados |
| **Azure Databricks Premium** | Unity Catalog habilitado. Scope Secret configurado y apuntando al Key Vault |
| **External Locations** | Una External Location por contenedor de ADLS Gen2 registrada en Unity Catalog |
| **Catálogos Unity Catalog** | `bronce_dev`, `plata_dev`, `oro_dev`, `control_dev` con schema `regional` en cada uno |

> Para una guía completa de configuración de infraestructura, consultar el [Manual Técnico](docs/ManualTecnico.md).

---

## Estructura del Proyecto

```
LakeflowSparkDeclarativePipelinesBase/
│
├── README.md                          # Este archivo
├── SYSTEM.md                          # Spec-Driven Development inicial del proyecto
│
├── docs/
│   ├── ManualTecnico.md               # Manual técnico completo del pipeline
│   └── ModeladoDatos.md               # Diccionario de datos y linaje completo
│
├── scripts/
│   └── GenerarParquets/               # Notebooks independientes: generan Parquets AS400 simulados
│       ├── NbGenerarMaestroCliente.py
│       ├── NbGenerarSaldosCliente.py
│       └── NbGenerarTransaccionalCliente.py
│
├── src/
│   └── LSDP_Laboratorio_Basico/       # Pipeline Lakeflow Spark Declarative Pipelines
│       ├── transformations/           # Scripts LSDP por capa medallion
│       │   ├── LsdpBronceBlncfl.py
│       │   ├── LsdpBronceCmstfl.py
│       │   ├── LsdpBronceTrxpfl.py
│       │   ├── LsdpPlataClientesSaldos.py
│       │   ├── LsdpPlataTransacciones.py
│       │   └── LsdpOroClientes.py
│       └── utilities/                 # Utilidades compartidas del pipeline
│           ├── LsdpConexionAzureSql.py
│           ├── LsdpConstructorRutasAbfss.py
│           └── LsdpReordenarColumnasLiquidCluster.py
│
└── tests/
    ├── GenerarParquets/               # TDD para generadores de Parquets
    └── LSDP_Laboratorio_Basico/       # TDD para el pipeline LSDP
```

---

## Documentación

| Documento | Descripción |
|---|---|
| [Manual Técnico](docs/ManualTecnico.md) | Referencia técnica completa: decoradores `@dp`, paradigma declarativo LSDP, propiedades Delta, API de DataFrames PySpark, tabla `dbo.Parametros` y todos los parámetros del pipeline |
| [Modelado de Datos](docs/ModeladoDatos.md) | Diccionario de datos con todas las entidades (10 en total): 3 Parquets AS400, 3 Streaming Tables Bronce, 2 Vistas Materializadas Plata, 2 Vistas Materializadas Oro. Incluye diagrama de linaje Mermaid |
| [SYSTEM.md](SYSTEM.md) | Spec-Driven Development inicial del proyecto: base de verdad para `constitution`, `specify` y `plan` del framework spec-kit |

---

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Cloud | Microsoft Azure (exclusivo) |
| Motor de Datos | Azure Databricks Premium |
| Pipeline Framework | Lakeflow Spark Declarative Pipelines (LSDP) |
| Cómputo | Databricks Serverless Compute |
| Catálogo | Unity Catalog |
| Almacenamiento | Azure Data Lake Storage Gen2 (`abfss://`) |
| Configuración | Azure SQL Free Tier — `dbo.Parametros` |
| Secretos | Azure Key Vault + Databricks Secret Scope |
| Lenguaje | Python / PySpark |
| Pruebas | TDD con Databricks Extension for VS Code |
| Modelado | Arquitectura Medallion (Bronce / Plata / Oro) |

---

## Metodología de Desarrollo

Este proyecto fue construido usando **Spec-Kit**, un framework de **Spec-Driven Development (SDD)** con **IA Asistida mediante GitHub Copilot**.

### Flujo de trabajo

```
SYSTEM.md  ──►  /speckit.constitution  ──►  constitution.md
               /speckit.specify        ──►  spec.md
               /speckit.plan           ──►  plan.md  +  data-model.md
               /speckit.tasks          ──►  tasks.md
               /speckit.implement      ──►  código fuente
               /speckit.checklist      ──►  validación de requisitos
```

El archivo [SYSTEM.md](SYSTEM.md) centraliza el **Spec-Driven Development inicial** del proyecto. Actúa como base de verdad que alimenta los comandos de spec-kit (`/speckit.constitution`, `/speckit.specify`, `/speckit.plan`), los cuales lo amplían y granulan iterativamente en artefactos de diseño más detallados organizados por versión dentro de la carpeta `specs/`.

> La IA propone, el ingeniero de datos decide. Ningún cambio estructural es aplicado sin aprobación explícita del usuario.

---

## Roadmap

### Versiones completadas

| Versión | Descripción | Estado |
|---|---|---|
| v1 | Research inicial y toma de decisiones arquitecturales | ✅ Completado |
| v2 | Generadores de Parquets AS400 simulados | ✅ Completado |
| v3 | Pipeline LSDP — Medalla Bronce (AutoLoader + Streaming Tables) | ✅ Completado |
| v4 | Pipeline LSDP — Medalla Plata (Vistas Materializadas) | ✅ Completado |
| v5 | Pipeline LSDP — Medalla Oro (Vistas Materializadas analíticas) | ✅ Completado |
| v6 | Documentación técnica: ManualTecnico.md y ModeladoDatos.md | ✅ Completado |

### Próximas versiones

| Versión | Descripción | Estado |
|---|---|---|
| v7 | **Infraestructura como Código (IaC)** — Carpeta `infrastructure/` con scripts **Terraform** para aprovisionar de forma directa y automatizada todos los componentes Azure requeridos: Storage Account (ADLS Gen2), Azure SQL, Azure Key Vault, Azure Databricks workspace, Unity Catalog, External Locations y catálogos. | 🔜 Próximamente |

---

## Extensiones Requeridas en VS Code

Para el desarrollo y ejecución local de pruebas TDD, las siguientes extensiones de Visual Studio Code deben estar instaladas y configuradas:

- **Databricks Extension for Visual Studio Code** — Permite acceder al workspace de Databricks, listar y usar los cómputos disponibles
- **Databricks Driver for SQLTools** — Permite la ejecución de consultas SQL sobre Databricks desde VS Code

---

## Convenciones de Código

- Todo el código está escrito en **español**, nombrado en `snake_case` en minúscula
- Archivos `.py` de notebooks: prefijo `Nb` + `PascalCase` (ej.: `NbGenerarMaestroCliente.py`)
- Archivos `.py` de LSDP: prefijo `Lsdp` + `PascalCase` (ej.: `LsdpBronceCmstfl.py`)
- Protocolo de rutas: exclusivamente `abfss://contenedor@cuenta.dfs.core.windows.net/ruta`
- Sin valores hardcodeados: toda configuración fluye desde `dbo.Parametros` vía Azure SQL

---

*Construido con [spec-kit](https://github.com/features/copilot) · IA Asistida por GitHub Copilot · Plataforma exclusiva Microsoft Azure*

