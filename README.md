# Document Hub Lakeflow Pipeline

This project defines a Databricks **Lakeflow (Delta Live Tables)** pipeline that ingests, parses, and indexes files for use with a knowledge assistant. It prepares structured content from supported formats (text, PDF, etc.) and leverages **Databricks Vector Search** to handle unsupported formats such as images.

## Pipeline Overview

1. **Bronze – File Ingest (`file_ingest`)**
   - Streams raw files from a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/files`).
   - Computes a content hash.
   - Extracts MIME type and extension using [Magika](https://github.com/google/magika).

2. **Silver – File Parse (`file_parse`)**
   - Loads binary content for image files.
   - Runs `ai_parse_document` on file content.
   - Produces structured JSON fields.

3. **Gold – File Index (`file_index`)**
   - Flattens parsed document elements and pages.
   - Generates stable `search_id` values.
   - Cleans and filters text for indexing.
   - Triggers a Vector Search delta sync index once data is materialized.

## Vector Search

- Ensures a **Vector Search Endpoint** exists (`<catalog>_<schema>_files_search`).
- Maintains a **Delta Sync Index** that maps ingested documents to embeddings.
- Provides semantic search for both supported text-based documents and unsupported formats like images.

## Notes

- Tables are created and updated via the Databricks event lifecycle (`update_progress`, `flow_progress`).
- The pipeline is designed to run in **development or production** modes with Unity Catalog governance.