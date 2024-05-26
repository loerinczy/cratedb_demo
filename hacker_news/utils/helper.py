from crate.client.connection import Connection
import os
from typing import Union
from pathlib import Path
import json

import crate.client as client

from .catalog import Tables, Table


def get_connection() -> Connection:
    connection = client.connect(
        "https://beige-nute-gunray.aks1.eastus2.azure.cratedb.net:4200",
        username="admin",
        password=os.getenv("CRATEDB_PASSWORD"),
    )
    return connection


def get_create_table_stmt(table: Table) -> str:
    schema_str = ", ".join(
        [
            f"{column_name} {data_type}"
            for column_name, data_type in table.schema.items()
        ]
    )
    stmt = f"create table {table.name} ({schema_str})"
    return stmt


def get_insert_stmt(table: Table) -> str:
    schema_str = ", ".join(table.schema.keys())
    values = ", ".join(["?"] * len(list(table.schema.keys())))
    stmt = f"insert into {table.name} ({schema_str}) values ({values})"
    return stmt


def dict2tuple(data: list[dict], table: Table) -> list[list]:
    output_data = []
    for record in data:
        output_record = [
            record[column] if column in record else None
            for column in table.schema.keys()
        ]
        output_data.append(output_record)
    return output_data


def tuple2dict(data: list[list], schema: dict[str, str]) -> list[dict]:
    output_data = []
    for record in data:
        output_record = {key: value for key, value in zip(schema.keys(), record)}
        output_data.append(output_record)
    return output_data


def map_to_schema(data: list, input_schema: dict, output_schema: dict) -> list:
    output = []
    for idx, input_col in enumerate(input_schema.keys()):
        if input_col in output_schema:
            output.append(data[idx])
    return output


def get_col(data: list, schema: dict, col: str):
    idx = list(schema.keys()).index(col)
    return data[idx]


def by2author(story: dict):
    if "by" in story:
        story["author"] = story["by"]
        del story["by"]


def get_batches(data_tuples: list, batch_size: int = 10):
    generator = (
        data_tuples[i : i + batch_size] for i in range(0, len(data_tuples), batch_size)
    )
    return generator


def commit_snapshot(data: Union[list, dict], snapshot_name: str):
    with open(
        Path(__file__).parents[1] / "snapshots" / (snapshot_name + ".json"), "w"
    ) as fp:
        json.dump(data, fp)

    print(f"Snapshot {snapshot_name} successfully commited.")


def load_snapshot(snapshot_name: str) -> list[dict]:
    with open(
        Path(__file__).parents[1] / "snapshots" / (snapshot_name + ".json")
    ) as fp:
        data = json.load(fp)
    return data


def insert_data(
    data: list[dict], table: Table, create_table: bool = False, batch_size: int = 10
) -> None:
    if create_table:
        create_table_stmt = get_create_table_stmt(table)
    insert_records = dict2tuple(data, table)
    insert_stmt = get_insert_stmt(table)
    batches = get_batches(insert_records, batch_size)
    with get_connection() as conn:
        cursor = conn.cursor()
        if create_table:
            cursor.execute(create_table_stmt)
        for batch in batches:
            cursor.executemany(insert_stmt, batch)
