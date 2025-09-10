"""
Lakeflow pipeline module for ingesting files from a Unity Catalog Volume.

Responsibilities:
- Ensure the target Volume exists
- Stream files from the Volume using Auto Loader
- Compute a content hash
- Detect MIME type and extension with a vectorized Pandas UDF
"""
import os
from typing import Iterator

import dlt
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F, types as T

from common import utils


# ---------- configuration ----------
VOLUME_CATALOG_NAME: str = utils.current_catalog()
VOLUME_SCHEMA_NAME: str = utils.current_schema()
VOLUME_NAME: str = "files"
VOLUME_FULL_NAME: str = f"{VOLUME_CATALOG_NAME}.{VOLUME_SCHEMA_NAME}.{VOLUME_NAME}"
VOLUME_PATH_DBFS: str = f"/Volumes/{VOLUME_CATALOG_NAME}/{VOLUME_SCHEMA_NAME}/{VOLUME_NAME}"


# ---------- setup ----------
def ensure_volume() -> None:
    """
    Ensure the Unity Catalog Volume exists for raw file ingestion.
    Creates the Volume if it is missing.
    """
    wc = WorkspaceClient()
    try:
        wc.volumes.read(VOLUME_FULL_NAME)
        utils.logger().info("volume exists: %s", VOLUME_FULL_NAME)
    except Exception:
        utils.logger().info("creating volume: %s", VOLUME_FULL_NAME)
        wc.volumes.create(
            catalog_name=VOLUME_CATALOG_NAME,
            schema_name=VOLUME_SCHEMA_NAME,
            name=VOLUME_NAME,
            comment="Stores raw files for document hub",
        )


# Run once on import in the pipeline environment
ensure_volume()


# ---------- UDFs ----------
@F.pandas_udf("mime_type string, extension string")
def file_info_udf(it: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    """
    Vectorized detector of MIME type and file extension for file paths.

    Args:
        it: Iterator of pandas Series of file paths

    Yields:
        pandas DataFrame with columns:
            mime_type: detected MIME type
            extension: best guess file extension for the path
    """
    from magika import Magika

    m = Magika()
    for s in it:
        mime_types: list[str] = []
        extensions: list[str | None] = []
        for p in s:
            res = m.identify_path(p).output
            mime_types.append(res.mime_type)
            extensions.append(res.extensions[0] if res.extensions else None)
        yield pd.DataFrame({"mime_type": mime_types, "extension": extensions})


# ---------- DLT tables ----------
@dlt.table(table_properties={"quality": "bronze"})
def file_ingest():
    """
    Stream files from the Volume using Auto Loader and enrich with metadata.

    Returns:
        A streaming DataFrame with:
            path: source file path
            modificationTime, length: file system metadata
            content_hash: SHA-256 hash of file content
            mime_type, extension: detected file metadata
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .load(VOLUME_PATH_DBFS)
        .withColumn("content_hash", F.sha2(F.col("content"), 256))
        .drop("content")
        .withColumn("file_info", file_info_udf(utils.os_path(F.col("path"))))
        .select("*", "file_info.*")
        .drop("file_info")
    )