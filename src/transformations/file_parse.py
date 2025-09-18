"""
Lakeflow pipeline module for parsing image files.

Responsibilities:
• Load binary content from file paths
• Convert SVG images to PNG bytes using resvg_py
• Parse document content with ai_parse_document
• Produce a structured silver level Delta table
"""

from typing import Dict, Callable, NamedTuple, Optional
from typing import Iterator
from common import utils
import pandas as pd
from pyspark.sql import functions as F, types as T
import resvg_py

import dlt

# ---------- TYPES ----------

class MimeType(NamedTuple):
    """
    Parsed MIME type.

    Fields:
        value: Original MIME string such as "image/svg+xml"
        type: Primary type such as "image"
        subtype: Subtype such as "svg+xml" or "" if missing

    Use MimeType.from_str to parse a MIME string into fields.
    """

    value: str
    type: str
    subtype: str

    @classmethod
    def from_str(cls, value: str) -> Optional["MimeType"]:
        if value:
            value = value.strip()
        if not value:
            return None
        value_parts = value.split("/")
        if len(value_parts) > 2:
            return None
        return cls(
            value, value_parts[0].lower(), value_parts[1].lower() if len(value_parts) > 1 else ""
        )


MimeTypePredicate = Callable[[MimeType], bool]
ContentConverter = Callable[[MimeType, bytes], Optional[bytes]]


# ---------- PARSE CONFIGURATIONS ----------

FILE_FILTER = F.col("mime_type").like("image/%")

CONTENT_CONVERTERS: Dict[MimeTypePredicate, ContentConverter] = {}
CONTENT_CONVERTERS[
    lambda mime: mime.type == "image" and mime.subtype.startswith("svg")
] = lambda _, content: resvg_py.svg_to_bytes(svg_string=content.decode("utf-8"))


def convert_content(mime_type: MimeType, content: bytes) -> Optional[bytes]:
    """
    Convert raw file bytes based on MIME type.

    Currently supported:
        image/svg or image/svg+xml → PNG bytes via resvg_py

    Returns:
        Converted bytes if a converter matches, otherwise the original bytes.
        Returns None if conversion or reading fails.
    """
    for predicate, converter in CONTENT_CONVERTERS.items():
        try:
            if predicate(mime_type):
                content = converter(mime_type, content)
        except Exception as e:
            utils.logger().warning(
                f"content coversion failed - mime_type:{mime_type} converter:{converter}",
                e,
            )
            pass
        if not content:
            break
    return content


# ---------- UDFs ----------


@F.pandas_udf(T.BinaryType())
def content_udf(it: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    """
    Vectorized Pandas UDF that reads file bytes from local paths and applies MIME aware conversion.

    Input batches:
        pandas DataFrame with columns:
            path: string path to a local file on the worker
            mime_type: MIME string such as "image/svg+xml" or "image/png"

    Behavior:
        Reads each path into bytes.
        If mime_type indicates SVG, converts to PNG bytes using resvg_py.
        Otherwise returns the original bytes.

    Output:
        pandas Series of bytes suitable for a BinaryType column.
    """

    def _content(path: str, mime: str) -> bytes | None:
        try:
            with open(path, "rb") as fh:
                content = fh.read()
                return convert_content(MimeType.from_str(mime), content)
        except Exception as e:
            utils.logger().warning(
                f"content read failed - path:{path} mime_type:{mime}",
                e,
            )
            return None

    for pdf in it:
        yield pd.Series(
            [_content(path, mime) for path, mime in zip(pdf["path"], pdf["mime_type"])]
        )


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
        .filter(FILE_FILTER)
        .withColumn(
            "content", content_udf(utils.os_path(F.col("path")), F.col("mime_type"))
        )
        .withColumn(
            "parsed", F.expr("ai_parse_document(content, map('version','1.0'))")
        )
        .drop("content")
        .select("content_hash", "path", "parsed")
    )
