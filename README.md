```markdown
# Document Hub Lakeflow Pipeline

This project defines a Databricks Lakeflow (Delta Live Tables) pipeline that ingests, parses, and indexes files for use with a knowledge assistant. It prepares structured content from supported formats (text, PDF, etc.) and leverages Databricks Vector Search to handle unsupported formats such as images.

## Pipeline Overview

1. Bronze – File Ingest (`file_ingest`)
   - Streams raw files from a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/files`).
   - Computes a content hash.
   - Extracts MIME type and extension using Magika.

2. Silver – File Parse (`file_parse`)
   - Loads binary content for image files.
   - Runs `ai_parse_document` on file content.
   - Produces structured JSON fields.

3. Gold – File Index (`file_index`)
   - Flattens parsed document elements and pages.
   - Generates stable `search_id` values.
   - Cleans and filters text for indexing.
   - Triggers a Vector Search delta sync index once data is materialized.

## Vector Search

- Ensures a Vector Search Endpoint exists (`<catalog>_<schema>_files_search`).
- Maintains a Delta Sync Index that maps ingested documents to embeddings.
- Provides semantic search for both supported text-based documents and unsupported formats like images.

## What this repo provides

- A Databricks Bundle that declares:
  - A Unity Catalog Volume for file ingestion.
  - A Lakeflow pipeline to ingest → parse → index content.
  - An optional Job that triggers the pipeline when files arrive in the Volume.
- Environment dependencies (e.g., vector search client, Magika) configured for the pipeline runtime.
- Opinionated defaults for dev/prod targets with Unity Catalog governance.

## Prerequisites

- Databricks workspace with Unity Catalog enabled.
- Permissions to create UC schemas, volumes, pipelines, and Vector Search endpoints.
- A profile configured in your local Databricks CLI (v0.205+ recommended).

## Deploy

Deploy the bundle to a target with required variables. At minimum, provide a catalog name.

Example (dev):
```

databricks bundle deploy --target dev --var="catalog_name=YOUR_CATALOG" --profile YOUR_PROFILE
```
- Variables:
  - catalog_name: Required (no default).
  - schema_name: Defaults to document_hub_dev for dev and document_hub for prod.
  - volume_name: Defaults to files.

To deploy to prod:
```

databricks bundle deploy --target prod --var="catalog_name=YOUR_CATALOG" --profile YOUR_PROFILE
```
After deployment, the pipeline and volume will be created in the specified catalog/schema. The pipeline root path and other workspace paths are set by the bundle.

## Run the pipeline

- Start the pipeline from the Databricks UI (Workflows → Delta Live Tables) or trigger it via the associated Job (file arrival).
- Ingest files by placing them in the provisioned Volume:
  - Path: `/Volumes/<catalog_name>/<schema_name>/<volume_name>/`
  - Upload via UI, DBFS CLI, or any supported client.

Once files land in the Volume, the Bronze stream ingests them, the Silver step parses content, and the Gold step prepares and syncs the Vector Search index.

## Notes

- Tables are created and updated via the Databricks event lifecycle (`update_progress`, `flow_progress`).
- The pipeline can run in development or production modes with Unity Catalog governance.
- Vector Search endpoint and index are created on first run and kept in sync by the pipeline logic.
```
