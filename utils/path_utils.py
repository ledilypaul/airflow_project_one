from pathlib import Path

def normalize_spark_path(path_str: str) -> str:
    """
    Normalise un chemin pour Spark :
    - Si c'est un chemin local, ajoute le préfixe file://
    - Si c'est déjà un chemin distribué (hdfs://, s3://, etc.), ne change rien
    - Compatible Mac, Linux et Windows
    """
    # Protocoles déjà distribués
    distributed_protocols = ("hdfs://", "s3://", "gs://", "dbfs:/", "abfss://", "adl://")

    # Si le chemin commence déjà par un protocole distribué → on le garde tel quel
    if path_str.startswith(distributed_protocols):
        return path_str

    # Conversion en Path pour uniformiser les séparateurs
    local_path = Path(path_str).expanduser().resolve()

    # Spark sur Windows nécessite un triple slash après file:
    if local_path.drive:  # Windows (ex: C:\)
        return f"file:///{local_path.as_posix()}"
    else:  # Linux / Mac
        return f"file://{local_path.as_posix()}"
