"""
Vector Search endpoint and delta sync index setup for Databricks.

This module:
- Ensures a Vector Search endpoint exists and is ONLINE
- Ensures a delta sync index exists and is synced to the source table
"""

import functools
import time
from typing import Optional

from common import utils
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import SparkSession

# ---------- configuration ----------
spark = SparkSession.builder.getOrCreate()
SOURCE_TABLE_NAME = f"{spark.conf.get('catalog_name')}.{spark.conf.get('schema_name')}.file_index"
ENDPOINT_NAME = utils.snake_case(SOURCE_TABLE_NAME) + "_search"
INDEX_NAME = SOURCE_TABLE_NAME + "_search_index"

utils.logger().info("SOURCE_TABLE_NAME: %s", SOURCE_TABLE_NAME)
utils.logger().info("ENDPOINT_NAME: %s", ENDPOINT_NAME)
utils.logger().info("INDEX_NAME: %s", INDEX_NAME)

# ---------- setup routines ----------
def endpoint_setup(poll_secs: int = 5) -> None:
    """
    Create the Vector Search endpoint if needed and wait until it is ONLINE.

    Args:
        poll_secs: Seconds to wait between status checks
    """
    client = VectorSearchClient()

    def _get_vector_search_endpoint(name: str) -> Optional[dict]:
        try:
            return client.get_endpoint(name=name)
        except Exception:
            return None

    while True:
        ep = _get_vector_search_endpoint(ENDPOINT_NAME)
        if ep:
            state = ep["endpoint_status"]["state"]
            utils.logger().info("%s state: %s", ENDPOINT_NAME, state)

            if state == "ONLINE":
                return
            if state == "PROVISIONING":
                time.sleep(poll_secs)
                continue

            raise ValueError(f"unknown state: {state}")

        utils.logger().info("creating vector search endpoint: %s", ENDPOINT_NAME)
        client.create_endpoint_and_wait(ENDPOINT_NAME, endpoint_type="STANDARD")
        return


def delta_sync_index_setup() -> None:
    """
    Ensure the delta sync index exists on the endpoint and kick off a sync.

    If the index exists, it will be synced. If it does not, it will be created
    and the function returns after creation completes.
    """
    client = VectorSearchClient()
    index = None
    try:
        index = client.get_index(ENDPOINT_NAME, INDEX_NAME)
        utils.logger().info("vector search delta sync index exists: %s", INDEX_NAME)
    except Exception:
        utils.logger().info(
            "creating vector search delta sync index: %s", INDEX_NAME
        )
        client.create_delta_sync_index_and_wait(
            #endpoint_name=ENDPOINT_NAME,
            index_name=INDEX_NAME,
            source_table_name=SOURCE_TABLE_NAME,
            pipeline_type="TRIGGERED",
            primary_key="search_id",
            embedding_source_column="text",
            embedding_model_endpoint_name="databricks-gte-large-en",
            columns_to_sync=["id", "text", "path", "content_hash"],
        )
    if index:
        # TODO: Add conditional sync logic
        utils.logger().info("syncing vector search delta sync index:%s", index.describe())
        index.sync()

if __name__ == "__main__":
    delta_sync_index_setup()