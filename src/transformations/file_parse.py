"""
Lakeflow pipeline module for parsing image files.

Responsibilities:
- Load binary content from image files
- Parse document content using ai_parse_document
- Produce a structured silver-level Delta table
"""

import os
from typing import Iterator

import dlt
import pandas as pd
from pyspark.sql import functions as F, types as T

from common import utils


# ---------- UDFs ----------
@F.pandas_udf(T.BinaryType())
def content_udf(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Vectorized Pandas UDF that reads binary file content from paths.

    Args:
        it: Iterator of pandas Series with file paths

    Yields:
        pandas Series of binary file contents
    """

    def _content(path: str) -> bytes:
        with open(path, "rb") as fh:
            return fh.read()

    for s in it:
        yield s.map(_content)


# ---------- DLT tables ----------
@dlt.table(
    table_properties={
        "quality": "silver",
        "delta.feature.variantType-preview": "supported",
    },
)
def file_parse():
    """
    Stream image files from the bronze table, parse them, and write silver table.

    Returns:
        A streaming DataFrame with:
            content_hash: file content hash
            path: original file path
            parsed: structured content from ai_parse_document
    """
    return (
        spark.readStream.table("file_ingest")
        .filter(F.col("mime_type").like("image/%"))
        .withColumn("content", content_udf(utils.os_path(F.col("path"))))
        .withColumn(
            "parsed", F.expr("ai_parse_document(content, map('version','1.0'))")
        )
        .drop("content")
        .select("content_hash", "path", "parsed")
    )