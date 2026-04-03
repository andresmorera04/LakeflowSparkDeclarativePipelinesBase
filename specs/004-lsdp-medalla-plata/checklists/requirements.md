# Checklist de Calidad de la Especificacion: LSDP Medalla de Plata (Version 4)

**Proposito**: Validar la completitud y calidad de la especificacion antes de proceder a la planificacion
**Creado**: 2026-03-30
**Feature**: [spec.md](../spec.md)

## Calidad del Contenido

- [x] Sin detalles de implementacion (lenguajes, frameworks, APIs)
- [x] Enfocado en valor de usuario y necesidades de negocio
- [x] Escrito para stakeholders no tecnicos
- [x] Todas las secciones obligatorias completadas

## Completitud de Requerimientos

- [x] No quedan marcadores [NEEDS CLARIFICATION]
- [x] Los requerimientos son testeables y no ambiguos
- [x] Los criterios de exito son medibles
- [x] Los criterios de exito son agnosticos a la tecnologia (sin detalles de implementacion)
- [x] Todos los escenarios de aceptacion estan definidos
- [x] Los casos borde estan identificados
- [x] El alcance esta claramente delimitado
- [x] Las dependencias y supuestos estan identificados

## Preparacion del Feature

- [x] Todos los requerimientos funcionales tienen criterios de aceptacion claros
- [x] Los escenarios de usuario cubren los flujos principales
- [x] El feature cumple con los resultados medibles definidos en Criterios de Exito
- [x] No hay detalles de implementacion filtrados en la especificacion

## Notas

- La especificacion aborda correctamente las 4 indicaciones del usuario: (1) se extiende sobre el mismo proyecto LSDP de V3, (2) funciones transversales en utilities/ sin decoradores dp, (3) vistas materializadas con Liquid Cluster usando la funcion existente de reordenamiento, (4) **catalogo de plata leido desde dbo.Parametros de Azure SQL (clave 'catalogoPlata')** y esquema de plata recibido como parametro del pipeline (`esquema_plata`).
- CORRECCION respecto a la version anterior: El catalogo de plata NO se recibe como parametro del pipeline. Se lee desde la tabla dbo.Parametros de Azure SQL usando la clave 'catalogoPlata', reutilizando/extendiendo la funcion `leer_parametros_azure_sql` existente. Solo el esquema se recibe como parametro del pipeline.
- Se realizo un ciclo de clarificacion (5 preguntas) que resolvio: (1) traslado de TODAS las columnas a plata excepto año/mes/dia del lazy evaluation, (2) umbrales especificos para campos calculados CASE definidos en spec, (3) estrategia sin filtro para leer_parametros_azure_sql, (4) expect_or_drop para CUSTID nulo, (5) comment obligatorio en espanol en decoradores.
- RF-019 cubre la refactorizacion de `leer_parametros_azure_sql` sin romper compatibilidad con bronce.
- RF-020 agregado para `comment` obligatorio en decoradores (discoverability Unity Catalog).
- Los campos calculados estan completamente definidos con formulas, umbrales numericos especificos y campos de bronce de origen, cumpliendo las reglas de negocio (minimo 3 campos CASE con 3+ insumos, minimo 4 campos numericos con 2+ insumos numericos, campo SHA2_256).
- La dimension tipo 1 esta correctamente especificada con left join desde cmstfl y filtrado por FechaIngestaDatos mas reciente.
- Los 20 requerimientos funcionales cubren de forma exhaustiva la funcionalidad, compatibilidad, estandares de desarrollo, lectura de parametros desde Azure SQL, observabilidad y pruebas TDD.
