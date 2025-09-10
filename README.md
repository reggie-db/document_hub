# Document Hub Lakeflow Pipeline

This project defines a Databricks Lakeflow (Delta Live Tables) pipeline that ingests, parses, and indexes files for use with a knowledge assistant. It prepares structured content from supported formats (text, PDF, etc.) and leverages Databricks Vector Search to handle unsupported formats such as images.

## Pipeline Overview

```
+---------------+       +-------------------+       +---------------------+
|   File        | ----> | Extract MIME Type | ----> | Is Image?           |
|   Ingested    |       |                   |       |                     |
+---------------+       +-------------------+       +----------+----------+
                                                                |
                                                                v
                                                      +---------------------+
                                                      | Parse Text from     |
                                                      | Image               |
                                                      +---------------------+
                                                                |
                                                                v
                                                      +---------------------+
                                                      | Sync with Vector    |
                                                      | Search Index        |
                                                      +---------------------+
```

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
- First run warm-up: The Vector Search endpoint is created on the first run and may take several minutes to reach the ONLINE state. Index creation and the initial delta sync will not complete until the endpoint is ONLINE, so expect a delay before search becomes available.
// ... existing code ...

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

Note on execution:
- Automatic kick-off: The pipeline is configured to automatically start when new files are added to the ingestion Volume path above. You can still start or refresh it manually from the UI if needed.
- Vector Search setup time: On the first run, the Vector Search endpoint and index are provisioned. This can take several minutes; searches won’t be available until provisioning and the initial sync complete.

Once files land in the Volume, the Bronze stream ingests them, the Silver step parses content, and the Gold step prepares and syncs the Vector Search index.

## Notes

- Tables are created and updated via the Databricks event lifecycle (`update_progress`, `flow_progress`).
- The pipeline can run in development or production modes with Unity Catalog governance.
- Vector Search endpoint and index are created on first run and kept in sync by the pipeline logic.
