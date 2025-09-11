from common import utils

CATALOG_NAME = utils.config_value("catalog_name")
SCHEMA_NAME = utils.config_value("schema_name")
VOLUME_NAME = utils.config_value("volume_name")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
SOURCE_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.file_index"
INDEX_NAME = SOURCE_TABLE_NAME + "_search_index"
ENDPOINT_NAME = utils.snake_case(SOURCE_TABLE_NAME) + "_search"