import requests

url = "https://adb-984752964297111.11.azuredatabricks.net/api/2.0/knowledge-assistants"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer XXX",  # replace with your real token
}

payload = {
    "name": "test123",
    "description": "Answers user questions about Databricks.",
    "instructions": "You are a faithful agent who retrieves relevant text from Databricks docs and provides faithful answers to user questions.",
    "knowledge_sources": [
        {
            "file_source": {
                "name": "ygong_papers",
                "type": "files",
                "description": "Databricks Whitepapers.",
                "files": [
                    {
                        "path": "/Volumes/reggie_pierce/document_hub_dev/files/"
                    }
                ]
            },
            "index_source": {
                "name": "ygong_notes",
                "type": "index",
                "description": "PM notes",
                "index": {
                    "name": "reggie_pierce.document_hub_dev.file_index_search_index",
                    "doc_uri_col": "path",
                    "text_col": "text"
                }
            }
        }
    ]
}

response = requests.post(url, headers=headers, json=payload)
response.raise_for_status()


print(response.status_code)
print(response.text)