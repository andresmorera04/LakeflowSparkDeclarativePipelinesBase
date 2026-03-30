# LsdpConstructorRutasAbfss — Constructor Dinamico de Rutas abfss://
#
# Proposito: Construir rutas abfss:// completas para Azure Data Lake Storage Gen2
# combinando los parametros leidos de Azure SQL (contenedorBronce, datalake,
# DirectorioBronce) con las rutas relativas configuradas como parametros del pipeline LSDP.
#
# Formato de salida:
#   abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}
#
# Invocaciones en el pipeline de bronce (6 en total):
#   3 rutas de parquets: MaestroCliente, Transaccional, SaldoCliente
#   3 rutas de checkpoints: cmstfl, trxpfl, blncfl
#
# Restricciones:
#   Prohibido /mnt/ — ADLS Gen2 exclusivamente via abfss:// (RF-011)
#   Reutilizable para versiones futuras de plata y oro (RF-017)


def construir_ruta_abfss(contenedor, datalake, directorio_raiz, ruta_relativa):
    """
    Construye una ruta abfss:// completa para Azure Data Lake Storage Gen2.

    Combina los 4 componentes de la ruta siguiendo el formato oficial de ADLS Gen2:
    abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}

    Esta funcion es el unico punto de construccion de rutas abfss:// en el pipeline,
    garantizando consistencia y eliminando hard-coding (Principio I de la Constitucion).

    Parametros
    ----------
    contenedor : str
        Nombre del contenedor ADLS Gen2.
        Se obtiene de dbo.Parametros clave "contenedorBronce" (ej: "bronce").
    datalake : str
        Nombre del storage account ADLS Gen2.
        Se obtiene de dbo.Parametros clave "datalake" (ej: "adlsg2datalakedev").
    directorio_raiz : str
        Directorio raiz dentro del contenedor.
        Se obtiene de dbo.Parametros clave "DirectorioBronce" (ej: "archivos").
    ruta_relativa : str
        Ruta relativa al directorio raiz, incluyendo barra final si es un directorio.
        Se obtiene de los parametros del pipeline LSDP.
        Ejemplo para parquets: "LSDP_Base/As400/MaestroCliente/"
        Ejemplo para checkpoint: "LSDP_Base/Checkpoints/Bronce/cmstfl/"

    Retorna
    -------
    str
        Ruta abfss:// completa. Ejemplo:
        "abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/"

    Notas
    -----
    - Nunca retorna rutas con /mnt/ (protocolo legacy prohibido en Serverless)
    - Reutilizable para plata y oro: solo cambia contenedor/directorio_raiz
    """

    # Construir y retornar la ruta abfss:// completa
    # Formato: abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}
    return f"abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}"
def construir_ruta_abfss(contenedor, datalake, directorio_raiz, ruta_relativa):
    """
    Construye una ruta abfss:// completa para Azure Data Lake Storage Gen2.

    Combina los 4 componentes de la ruta siguiendo el formato oficial de ADLS Gen2:
    abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}

    Esta funcion es el unico punto de construccion de rutas abfss:// en el pipeline,
    garantizando consistencia y eliminando hard-coding (Principio I de la Constitucion).

    Parametros
    ----------
    contenedor : str
        Nombre del contenedor ADLS Gen2.
        Se obtiene de dbo.Parametros clave "contenedorBronce" (ej: "bronce").
    datalake : str
        Nombre del storage account ADLS Gen2.
        Se obtiene de dbo.Parametros clave "datalake" (ej: "adlsg2datalakedev").
    directorio_raiz : str
        Directorio raiz dentro del contenedor.
        Se obtiene de dbo.Parametros clave "DirectorioBronce" (ej: "archivos").
    ruta_relativa : str
        Ruta relativa al directorio raiz, incluyendo barra final si es un directorio.
        Se obtiene de los parametros del pipeline LSDP.
        Ejemplo para parquets: "LSDP_Base/As400/MaestroCliente/"
        Ejemplo para checkpoint: "LSDP_Base/Checkpoints/Bronce/cmstfl/"

    Retorna
    -------
    str
        Ruta abfss:// completa. Ejemplo:
        "abfss://bronce@adlsg2datalakedev.dfs.core.windows.net/archivos/LSDP_Base/As400/MaestroCliente/"

    Notas
    -----
    - Nunca retorna rutas con /mnt/ (protocolo legacy prohibido en Serverless)
    - Reutilizable para plata y oro: solo cambia contenedor/directorio_raiz
    """

    # Construir y retornar la ruta abfss:// completa
    # Formato: abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}
    return f"abfss://{contenedor}@{datalake}.dfs.core.windows.net/{directorio_raiz}/{ruta_relativa}"
