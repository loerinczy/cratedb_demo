import crate
from crate.client.connection import Connection
import os

import crate.client


class Tables:
    stories = "bronze.raw"
    stories_text = "hackernews.stories_text"
    stories_chunk = "hackernew.stories_chunk"


class Schemas:
    stories = {}
    stories_text = {
        "author": "text",
        "id": "integer",
        "score": "integer",
        "time": "bigint",
        "title": "text",
        "url": "text",
        "text": "text",
    }
    stories_chunk = {}


def get_connection() -> Connection:
    connection = crate.client.connect(
        "https://beige-nute-gunray.aks1.eastus2.azure.cratedb.net:4200",
        username="admin",
        password=os.getenv("CRATEDB_PASSWORD"),
    )
    return connection


def get_create_table_stmt(name: str, schema: dict[str, str]) -> str:
    schema_str = ", ".join(
        [f"{column_name} {data_type}" for column_name, data_type in schema.items()]
    )
    stmt = f"create table {name} ({schema_str})"
    return stmt


def get_insert_stmt(name: str, schema: dict[str, str]) -> str:
    schema_str = ", ".join(schema.keys())
    values = ", ".join(["?"] * len(list(schema.keys())))
    stmt = f"insert into {name} ({schema_str}) values {values}"
    return stmt


def generate_data_tuples(bulk: list, schema: dict[str, str]) -> list[list]:
    output_bulk = []
    for record in bulk:
        output_record = [
            record[column] if column in record else None for column in schema.keys()
        ]
        output_bulk.append(output_record)
    return output_bulk


def map_to_schema(data: list, input_schema: dict, output_schema: dict) -> list:
    output = []
    for idx, input_col in enumerate(input_schema.keys()):
        if input_col in output_schema:
            output.append(data[idx])
    return output


def get_col(data: list, schema: dict, col: str):
    idx = list(schema.keys()).index(col)
    return data[idx]
