import dlt
import requests
import hashlib
import json

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


def generate_hash(row: dict) -> str:
    # Deterministic hash for idempotent merge
    row_string = json.dumps(row, sort_keys=True)
    return hashlib.md5(row_string.encode()).hexdigest()


@dlt.resource(
    name="taxi_trips",
    write_disposition="merge",
    primary_key="trip_hash"
)
def taxi_source():

    page = 1

    while True:
        response = requests.get(BASE_URL, params={"page": page})
        data = response.json()

        if not data:
            break

        for row in data:
            row["trip_hash"] = generate_hash(row)
            yield row

        page += 1


def run():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data"
    )

    load_info = pipeline.run(taxi_source())
    print(load_info)


if __name__ == "__main__":
    run()