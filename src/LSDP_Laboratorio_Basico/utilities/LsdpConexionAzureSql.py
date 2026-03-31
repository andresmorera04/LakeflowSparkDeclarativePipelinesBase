# LsdpConexionAzureSql — Lectura Dinamica de Parametros desde Azure SQL
#
# Proposito: Leer TODOS los parametros de configuracion desde la tabla dbo.Parametros
# en Azure SQL Server usando JDBC con 2 secretos almacenados en Azure Key Vault via
# Scope Secret de Databricks. Retorna el diccionario completo sin filtro (RF-019).
#
# Patron: Funcion pura reutilizable. Este archivo solo define la funcion; no ejecuta
# codigo a nivel de modulo. Los archivos de transformacion invocan esta funcion a nivel de
# modulo siguiendo el closure pattern (RF-002).
#
# Secretos requeridos (en el Scope Secret configurado como parametro del pipeline):
#   sr-jdbc-asql-asqlmetadatos-adminpd — Cadena JDBC sin contrasena
#   sr-asql-asqlmetadatos-adminpd      — Contrasena de la base de datos Azure SQL
#
# Compatibilidad: Computo Serverless — prohibido spark.sparkContext (RF-011).


def leer_parametros_azure_sql(spark, dbutils, nombre_scope_secret):
    """
    Lee los parametros de configuracion desde dbo.Parametros en Azure SQL Server.

    Combina dos secretos del Scope Secret de Databricks para construir la cadena JDBC
    completa: el primer secreto contiene la cadena de conexion sin contrasena, el segundo
    secreto contiene exclusivamente la contrasena.

    Parametros
    ----------
    spark : SparkSession
        La sesion Spark activa (disponible en el contexto del notebook Databricks).
    dbutils : DBUtils
        Objeto dbutils de Databricks (disponible en el contexto del notebook).
        Se pasa como parametro porque este modulo es Python puro y no tiene
        acceso automatico al contexto del notebook.
    nombre_scope_secret : str
        Nombre del Scope Secret de Databricks vinculado al Azure Key Vault.
        Debe contener los secretos: sr-jdbc-asql-asqlmetadatos-adminpd
        y sr-asql-asqlmetadatos-adminpd.

    Retorna
    -------
    dict
        Diccionario con TODAS las claves de dbo.Parametros (RF-019). Ejemplo:
        {
            "catalogoBronce"   : str,  -- Catalogo de Unity Catalog bronce (ej: "bronce_dev")
            "contenedorBronce" : str,  -- Contenedor ADLS Gen2 (ej: "bronce")
            "datalake"         : str,  -- Storage Account ADLS Gen2 (ej: "adlsg2datalakedev")
            "DirectorioBronce" : str,  -- Directorio raiz en el contenedor (ej: "archivos")
            "catalogoPlata"    : str,  -- Catalogo de Unity Catalog plata (ej: "plata_dev")
            ...                        -- Cualquier otra clave presente en dbo.Parametros
        }
        El codigo consumidor selecciona las claves que necesita del diccionario.
    """

    # -------------------------------------------------------------------------
    # Paso 1: Obtener la cadena JDBC sin contrasena desde Azure Key Vault
    # Secreto: sr-jdbc-asql-asqlmetadatos-adminpd
    # Contiene: cadena JDBC completa excepto el segmento de password
    # -------------------------------------------------------------------------
    cadena_jdbc_sin_password = dbutils.secrets.get(
        scope=nombre_scope_secret,
        key="sr-jdbc-asql-asqlmetadatos-adminpd"
    )

    # -------------------------------------------------------------------------
    # Paso 2: Obtener la contrasena desde Azure Key Vault
    # Secreto: sr-asql-asqlmetadatos-adminpd
    # Contiene: solo la contrasena, sin informacion de conexion adicional
    # -------------------------------------------------------------------------
    password = dbutils.secrets.get(
        scope=nombre_scope_secret,
        key="sr-asql-asqlmetadatos-adminpd"
    )

    # -------------------------------------------------------------------------
    # Paso 3: Construir la cadena JDBC completa
    # La cadena del primer secreto no incluye el segmento de password.
    # Se concatena el password del segundo secreto para formar la URL completa.
    # -------------------------------------------------------------------------
    cadena_jdbc_completa = cadena_jdbc_sin_password + ";password=" + password

    # -------------------------------------------------------------------------
    # Paso 4: Leer la tabla dbo.Parametros desde Azure SQL via JDBC
    # Driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
    # Compatible con Computo Serverless — no usa spark.sparkContext (RF-011)
    # -------------------------------------------------------------------------
    df_parametros = (
        spark.read
        .format("jdbc")
        .option("url", cadena_jdbc_completa)
        .option("dbtable", "dbo.Parametros")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    # -------------------------------------------------------------------------
    # Paso 5: Convertir el DataFrame clave-valor a diccionario Python
    # collect() es seguro para dbo.Parametros: es una tabla de configuracion
    # con pocos registros (no un dataset de millones de filas).
    # -------------------------------------------------------------------------
    filas = df_parametros.collect()
    params_dict = {fila["Clave"]: fila["Valor"] for fila in filas}

    # -------------------------------------------------------------------------
    # Paso 6: Retornar el diccionario completo con TODAS las claves (RF-019)
    # El codigo consumidor selecciona las claves que necesita.
    # -------------------------------------------------------------------------
    return params_dict
