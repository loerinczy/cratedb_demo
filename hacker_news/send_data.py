import crate
import json
import os
from pathlib import Path

import crate.client

data_dir = Path(__file__).parents[1] / "data"

with open(data_dir / "stories_1500.json") as fp:
    stories = json.load(fp)

for story in stories:
    if "by" in story:
        story["author"] = story["by"]
        del story["by"]


def generate_data_tuples(bulk: list, schema: list[str]) -> list[list]:
    output_bulk = []
    for record in bulk:
        output_record = [
            record[column] if column in record else None for column in schema
        ]
        output_bulk.append(output_record)
    return output_bulk


target_table = "bronze.raw"
schema = (
    "deleted",
    "dead",
    "parent",
    "poll",
    "parts",
    "author",
    "descendants",
    "id",
    "kids",
    "score",
    "time",
    "title",
    "type",
    "url",
)
schema_str = ", ".join(schema)
values = "?, " * (len(schema) - 1) + "?"


with crate.client.connect(
    "https://beige-nute-gunray.aks1.eastus2.azure.cratedb.net:4200",
    username="admin",
    password=os.getenv("CRATEDB_PASSWORD"),
) as conn:
    cursor = conn.cursor()
    tuples = generate_data_tuples(stories, schema)
    print(len(tuples))
    cursor.executemany(
        f"INSERT INTO {target_table} ({schema_str}) VALUES ({values})",
        tuples,
    )
