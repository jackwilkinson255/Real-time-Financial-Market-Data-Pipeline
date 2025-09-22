import os
import time
from collections.abc import MutableMapping
from pathlib import Path
import requests
import json

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def wait_for_schema_registry() -> None:
    while True:
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
            if response.status_code == 200:
                break
        except requests.RequestException:
            print("Could not connect to schema registry")
            pass
        time.sleep(2)


def get_schemas_from_files() -> list[MutableMapping]:
    schemas = []
    for file_path in Path.joinpath(Path.cwd(), "schemas").glob("*.avsc"):
        schema = json.loads(file_path.read_text())
        schemas.append(schema)
    assert len(schemas) > 0
    return schemas


def post_schemas(schemas: list[MutableMapping]) -> None:
    for schema in schemas:
        subject = f"{schema["name"]}-value"
        print(f"Posting schema for subject {subject}")
        response = requests.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"schema": json.dumps(schema)}),
        )
        print(response.text)


def main():
    print("Waiting for schema registry")
    wait_for_schema_registry()
    print("Loading schemas...")
    post_schemas(get_schemas_from_files())


if __name__ == "__main__":
    main()
