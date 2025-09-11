"""
Lakeflow pipeline module for ingesting files from a Unity Catalog Volume.

Responsibilities:
- Ensure the target Volume exists
- Stream files from the Volume using Auto Loader
- Compute a content hash
- Detect MIME type and extension with a vectorized Pandas UDF
"""
from typing import Iterator

import dlt
import pandas as pd
from common import utils
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F


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
    catalog_name: str = utils.config_value("catalog_name")
    schema_name: str = utils.config_value("schema_name")
    volume_name: str = utils.config_value("volume_name")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}")
        .withColumn("content_hash", F.sha2(F.col("content"), 256))
        .drop("content")
        .withColumn("file_info", file_info_udf(utils.os_path(F.col("path"))))
        .select("*", "file_info.*")
        .drop("file_info")
    )
