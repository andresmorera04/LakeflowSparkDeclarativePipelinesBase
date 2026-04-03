# LsdpReordenarColumnasLiquidCluster — Reordenamiento de Columnas para Liquid Cluster
#
# Proposito: Reordenar las columnas de un DataFrame de forma que las columnas que
# forman parte del Liquid Cluster queden ubicadas entre las primeras 32 columnas
# de la tabla Delta. Delta Lake recopila estadisticas (min/max) solo para las
# primeras 32 columnas por defecto, y Liquid Clustering requiere que sus columnas
# tengan estadisticas disponibles (DELTA_CLUSTERING_COLUMN_MISSING_STATS).
#
# Patron: Funcion pura reutilizable. Las columnas del Liquid Cluster se colocan
# al inicio del DataFrame y el resto de columnas mantienen su orden original.
#
# Compatibilidad: Computo Serverless — sin spark.sparkContext (RF-011).


def reordenar_columnas_liquid_cluster(df, columnas_liquid_cluster):
    """
    Reordena las columnas de un DataFrame para garantizar que las columnas del
    Liquid Cluster queden ubicadas al inicio (primeras posiciones) de la tabla Delta.

    Delta Lake recopila estadisticas (min/max) solo para las primeras 32 columnas
    de una tabla por defecto. Liquid Clustering requiere que las columnas de
    clustering tengan estadisticas disponibles. Si una columna de clustering esta
    fuera de las primeras 32 columnas, Delta lanza el error
    DELTA_CLUSTERING_COLUMN_MISSING_STATS.

    Esta funcion coloca las columnas del Liquid Cluster al inicio del DataFrame
    y conserva el orden original del resto de columnas despues de ellas.

    Parametros
    ----------
    df : DataFrame
        DataFrame de PySpark (batch o streaming) cuyas columnas se reordenaran.
    columnas_liquid_cluster : list[str]
        Lista de nombres de columnas que forman parte del Liquid Cluster.
        Estas columnas se colocaran al inicio del DataFrame en el orden dado.

    Retorna
    -------
    DataFrame
        El mismo DataFrame con las columnas reordenadas: primero las columnas
        del Liquid Cluster (en el orden proporcionado), luego el resto de
        columnas en su orden original.

    Lanza
    -----
    ValueError
        Si alguna columna de columnas_liquid_cluster no existe en el DataFrame.
        El mensaje incluye las columnas faltantes y las columnas disponibles.

    Ejemplo
    -------
    >>> columnas_lc = ["FechaIngestaDatos", "CUSTID"]
    >>> df_reordenado = reordenar_columnas_liquid_cluster(df_original, columnas_lc)
    # df_reordenado tendra: FechaIngestaDatos, CUSTID, col1, col2, ..., colN
    """

    # -------------------------------------------------------------------------
    # Paso 1: Validar que todas las columnas del Liquid Cluster existen en el DF
    # -------------------------------------------------------------------------
    columnas_df = df.columns
    columnas_faltantes = [col for col in columnas_liquid_cluster if col not in columnas_df]

    if columnas_faltantes:
        raise ValueError(
            f"ERROR: Las siguientes columnas del Liquid Cluster no existen en el DataFrame: "
            f"{columnas_faltantes}. "
            f"Columnas disponibles en el DataFrame: {columnas_df}. "
            f"Verificar que los nombres coincidan exactamente (case-sensitive)."
        )

    # -------------------------------------------------------------------------
    # Paso 2: Construir el orden final de columnas
    # Primero: columnas del Liquid Cluster en el orden proporcionado
    # Despues: resto de columnas en su orden original (excluyendo las del LC)
    # -------------------------------------------------------------------------
    columnas_restantes = [col for col in columnas_df if col not in columnas_liquid_cluster]
    orden_final = list(columnas_liquid_cluster) + columnas_restantes

    # -------------------------------------------------------------------------
    # Paso 3: Aplicar el reordenamiento con select
    # -------------------------------------------------------------------------
    return df.select(orden_final)
