"""
Lakeflow pipeline module for indexing parsed documents into a searchable format.

Responsibilities:
- Flatten parsed document elements and pages
- Generate deterministic search IDs
- Prepare clean text content for indexing
- Trigger delta sync index updates after successful events
"""

import dlt
from common import utils, vector_search_config
from dbruntime.databricks_repl_context import get_context
from pyspark.sql import functions as F, types as T


# ---------- DLT tables ----------
@dlt.table(
    name=vector_search_config.SOURCE_TABLE_NAME,
    table_properties={
        "quality": "gold",
        "delta.feature.variantType-preview": "supported",
        "delta.feature.changeDataFeed": "supported",
        "delta.enableChangeDataFeed": "true",
    },
)
def file_index():
    """
    Prepare parsed documents for vector search indexing.

    Steps:
    - Read parsed documents from silver table
    - Extract document elements and pages into a unified schema
    - Flatten arrays into individual items
    - Compute search_id as a stable hash
    - Clean text content for indexing
    """
    parsed_documents = spark.readStream.table("file_parse")

    parsed_document_item_schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("type", T.StringType()),
            T.StructField("page_id", T.LongType()),
            T.StructField("page_number", T.IntegerType()),
            T.StructField("content", T.StringType()),
        ]
    )

    parsed_documents_items = (
        parsed_documents.withColumn(
            "elements",
            F.from_json(
                F.to_json(F.expr("parsed:document:elements")),
                T.ArrayType(parsed_document_item_schema),
            ),
        )
        .withColumn(
            "pages",
            F.from_json(
                F.to_json(F.expr("parsed:document:pages")),
                T.ArrayType(parsed_document_item_schema),
            ),
        )
        .drop("parsed")
    )

    parsed_documents_item = (
        parsed_documents_items.withColumn(
            "items", F.flatten(F.array("elements", "pages"))
        )
        .withColumn("item", F.explode_outer("items"))
        .select(
            *[
                c
                for c in parsed_documents_items.columns
                if c not in {"elements", "pages"}
            ],
            "item.*",
        )
        .drop("items", "item")
    )

    search_id_columns = ["id", "type", "page_id", "page_number"]
    search_id_exprs = [F.col("content_hash")]
    for col_name in search_id_columns:
        search_id_exprs.append(F.coalesce(F.col(col_name).cast("string"), F.lit("_")))

    def _trim_to_none(c):
        # Collapse spaces before newlines, collapse 3+ newlines to 2, trim, then null if empty
        x = F.trim(
            F.regexp_replace(
                F.regexp_replace(c, "\\s+\\n", "\n"),
                "\\n{3,}",
                "\n\n",
            )
        )
        return F.when(F.length(x) == 0, F.lit(None)).otherwise(x)

    return (
        parsed_documents_item.withColumn(
            "search_id", F.sha2(F.concat_ws("|", *search_id_exprs), 256)
        )
        .withColumn("text", _trim_to_none(F.col("content")))
        .drop("content")
        .filter(F.col("text").isNotNull())
    )
