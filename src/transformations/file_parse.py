"""
Lakeflow pipeline module for parsing image files.

Responsibilities:
• Load binary content from file paths
• Convert SVG images to PNG bytes using resvg_py
• Parse document content with ai_parse_document
• Produce a structured silver level Delta table
"""


from typing import Dict, Callable, NamedTuple, Optional, Tuple
from common import utils
import pandas as pd
from pyspark.sql import functions as F, types as T
import resvg_py

try:
    import dlt
except ImportError:
    pass

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
            value,
            value_parts[0].lower(),
            value_parts[1].lower() if len(value_parts) > 1 else "",
        )


MimeTypePredicate = Callable[[MimeType], bool]
ContentConverter = Callable[[MimeType, bytes], Optional[bytes]]


# ---------- CONFIGURATIONS ----------
LOG = utils.logger()
FILE_FILTER = F.col("mime_type").like("image/%")

CONTENT_CONVERTERS: Dict[MimeTypePredicate, ContentConverter] = {}

# SVG to PNG
CONTENT_CONVERTERS[
    lambda mime: mime.type == "image" and mime.subtype.startswith("svg")
] = lambda mime_type, content: resvg_py.svg_to_bytes(svg_string=content.decode("utf-8"))


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
            if not predicate(mime_type):
                continue
        except Exception as e:
            LOG.warning(
                f"content predicate failed - mime_type:{mime_type} converter:{converter}",
                e,
            )
            continue
        try:
            content = converter(mime_type, content)
        except Exception as e:
            LOG.warning(
                f"content coversion failed - mime_type:{mime_type} converter:{converter}",
                e,
            )
            pass
        if not content:
            break
    return content


def convert_content_path(mime_type: str, path: str) -> Tuple[bytes, bool]:
    content: Optional[bytes] = None
    try:
        with open(path, "rb") as fh:
            content = fh.read()
    except Exception as e:
        LOG.warning(f"content read failed - path:{path} mime_type:{mime_type}", e)
    if content:
        mt = MimeType.from_str(mime_type)
        if mt:
            converted_bytes = convert_content(mt, content)
            if content != converted_bytes:
                return converted_bytes, True
    return content, False


# ---------- UDFs ----------


convert_content_udf_schema = T.StructType(
    [
        T.StructField("content", T.BinaryType()),
        T.StructField("converted", T.BooleanType()),
    ]
)


@F.pandas_udf(convert_content_udf_schema)
def convert_content_udf(path: pd.Series, mime_type: pd.Series) -> pd.DataFrame:
    """
    UDF that reads file bytes and converts SVG → PNG.
    Returns a struct with content (bytes) and converted (bool).
    """

    rows = [convert_content_path(m, p) for p, m in zip(path, mime_type)]
    return pd.DataFrame(rows, columns=convert_content_udf_schema.fieldNames())


if False:
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
                "convert_content",
                convert_content_udf(utils.os_path(F.col("path")), F.col("mime_type")),
            )
            .withColumn(
                "parsed",
                F.expr(
                    "ai_parse_document(convert_content.content, map('version','1.0'))"
                ),
            )
            .withColumn(
                "converted_content",
                F.when(
                    F.col("convert_content.converted"), F.col("convert_content.content")
                ),
            )
            .select(
                "content_hash",
                "path",
                "parsed",
                "converted_content",
            )
        )


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    # Call your converter
    content_bytes, converted = convert_content_path("image/svg+xml", "dev-local/infographic_3.svg")

    # Write the bytes to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png" if converted else ".bin") as tmp:
        tmp.write(content_bytes)
        tmp_path = Path(tmp.name)

    print(f"Temporary file written to: {tmp_path}")

