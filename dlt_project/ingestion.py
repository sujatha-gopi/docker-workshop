import dlt
import dlt
from itertools import islice
from dlt.sources.rest_api import rest_api_source

def openlibrary_source(query: str = "harry potter"):

    return rest_api_source({
        "client": {
            "base_url": "https://openlibrary.org",
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": query,
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": "numFound",
                    },
                },
            },
        ],
    })
    
pipeline = dlt.pipeline(
    pipeline_name="ol_demo",
    destination="duckdb",
    dataset_name="ol_data",
    progress="log" # logs the pipeline run (Optiona)
)

extract_info = pipeline.extract(openlibrary_source())
load_id = extract_info.loads_ids[-1]
m = extract_info.metrics[load_id][0]

print("Resources:", list(m["resource_metrics"].keys()))
print("Tables:", list(m["table_metrics"].keys()))
print("Load ID:", load_id)
print()

for resource, rm in m["resource_metrics"].items():
    print(f"Resource: {resource}")
    print(f"rows extracted: {rm.items_count}")
    print()

normalize_info = pipeline.normalize()
load_id = normalize_info.loads_ids[-1]
m = normalize_info.metrics[load_id][0]

print("Load ID:", load_id)
print()

print("Tables created/updated:")
for table_name, tm in m["table_metrics"].items():
    # skip dlt internal tables to keep it beginner-friendly
    if table_name.startswith("_dlt"):
        continue
    print(f"  - {table_name}: {tm.items_count} rows")

load_info = pipeline.load()