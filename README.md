# Document Hub
## Knowledge Assistant and Lakeflow Pipeline Quickstart

This project defines a Databricks Lakeflow (Delta Live Tables) pipeline that ingests, parses, and indexes files for use with a knowledge assistant. It prepares structured content from supported formats (text, PDF, etc.) and leverages Databricks Vector Search to handle unsupported formats such as images.

### TL;DR

- Clone, deploy, and start using it. Copy and paste the below into a terminal
```bash
git clone https://github.com/reggie-db/document_hub.git && cd document_hub && \
printf "Enter your catalog name: " && read CATALOG_NAME && \
printf "Enter your Databricks CLI profile name: " && read PROFILE_NAME && \
databricks bundle deploy --target dev --var "catalog_name=${CATALOG_NAME}" --profile "${PROFILE_NAME}"
```
After deployment:
- Drop files into: /Volumes/<catalog_name>/<schema_name>/<volume_name>/ (defaults to files). The pipeline will automatically start when new files are added.
- On first run, the Vector Search endpoint and index are provisioned; this may take several minutes before search is available.
- The index sync runs after transformations, and a knowledge assistant is set up to use the index.


### Pipeline Overview
```

+---------------------------+       +---------------------------+       +---------------------+       +---------------------------+
| Vector Search Setup       | ----> | File Transformations      | ----> | Vector Search Sync   | ----> | Knowledge Assistant Setup |
| (endpoint + index config) |       | (ingest -> parse -> index)|       | (post-materialization)|      | (assistant + data source) |
+---------------------------+       +---------------------------+       +---------------------+       +---------------------------+
```
1. Vector Search Setup (pre-step)
   - Ensures the Vector Search endpoint exists and is ONLINE.
   - Ensures a Delta Sync Index is created and ready to receive data.

2. Bronze – File Ingest (`file_ingest`)
   - Streams raw files from a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/files`).
   - Computes a content hash.
   - Extracts MIME type and extension using Magika.

3. Silver – File Parse (`file_parse`)
   - Loads binary content for image files.
   - Runs `ai_parse_document` on file content.
   - Produces structured JSON fields.

4. Gold – File Index (`file_index`)
   - Flattens parsed document elements and pages.
   - Generates stable `search_id` values.
   - Cleans and filters text for indexing.

5. Vector Search Sync (post-step)
   - Triggers a Delta Sync of the index after the gold table is materialized.

6. Knowledge Assistant Setup (post-step)
   - Creates a knowledge assistant connected to the Vector Search index.
   - Applies description and instruction text so the assistant can answer questions over ingested files.

### Vector Search

- Endpoint and index are provisioned before any transformation work starts. This ensures the sync can run immediately after gold data is available.
- The sync step runs after indexing to populate and refresh embeddings for search.
- First run warm-up: Endpoint creation can take several minutes to reach the ONLINE state. The initial index sync and search features become available only after the endpoint is ONLINE.


### Deploy

Deploy the bundle to a target with required variables. At minimum, provide a catalog name.

Example (dev):
```

databricks bundle deploy --target dev --var="catalog_name=YOUR_CATALOG" --profile YOUR_PROFILE
```
- Variables:
  - catalog_name: Required (no default).
  - schema_name: Defaults to document_hub_dev for dev and document_hub for prod.
  - volume_name: Defaults to files.
  - knowledge_assistant_sync_interval_ms: Optional. How often to sync the Knowledge Assistant file sources. Defaults to 23.5 hours.
  - knowledge_assistant_description: Optional. Description shown for the assistant.
  - knowledge_assistant_instructions: Optional. System instructions for how the assistant should behave.
  - file_source_description: Optional. Description of the document source feeding the assistant.
  - vector_search_index_description: Optional. Description for the Vector Search index.

To deploy to prod:
```

databricks bundle deploy --target prod --var="catalog_name=YOUR_CATALOG" --profile YOUR_PROFILE
```
After deployment, the endpoint/index, pipeline, and volume will be created in the specified catalog/schema. The pipeline root path and other workspace paths are set by the bundle.

### Run the pipeline

- Automatic trigger: The pipeline is configured to automatically start processing when new files are added to the ingestion Volume path:
  - `/Volumes/<catalog_name>/<schema_name>/<volume_name>/`
- You can also start or refresh it manually from the Databricks UI (Workflows → Delta Live Tables).

Execution flow:
- Pre-step: Vector Search endpoint and index are set up. On first run this may take several minutes; subsequent runs are faster.
- Transform: Files are ingested (bronze), parsed (silver), and indexed (gold).
- Post-steps: The Vector Search index is synced and the knowledge assistant is created/updated to use the latest index.

Upload documents via the UI, DBFS CLI, or any supported client. Once files land in the Volume, ingestion begins automatically; the index is synced after gold tables are written, and the assistant is made ready to answer questions.

### Notes

- Tables are created and updated via the Databricks event lifecycle (`update_progress`, `flow_progress`).
- The pipeline can run in development or production modes with Unity Catalog governance.
- Vector Search endpoint/index are provisioned before transformations, synced after, and then used to configure the knowledge assistant.
