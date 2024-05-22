from crate.client.connection import Connection
import os
from typing import Union
from pathlib import Path
import json
from langchain_text_splitters import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
import crate.client as client

from .catalog import Tables


def get_connection() -> Connection:
    connection = client.connect(
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
    stmt = f"insert into {name} ({schema_str}) values ({values})"
    return stmt


def dict2tuple(data: list[dict], schema: dict[str, str]) -> list[list]:
    output_data = []
    for record in data:
        output_record = [
            record[column] if column in record else None for column in schema.keys()
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


def chunk_html(
    html: str, chunk_size: int, chunk_overlap: int, headers_to_split_on: dict[str, str]
):
    # html = """<!DOCTYPE html>
    #     <html>
    #     <body>
    #         <div>
    #             <h1>Foo</h1>
    #             <p>Some intro text about Foo.</p>
    #             <div>
    #                 <h2>Bar main section</h2>
    #                 <p>Some intro text about Bar.</p>
    #                 <h3>Bar subsection 1</h3>
    #                 <p>Some text about the first subtopic of Bar.</p>
    #                 <h3>Bar subsection 2</h3>
    #                 <p>Some text about the second subtopic of Bar.</p>
    #             </div>
    #             <div>
    #                 <h2>Baz</h2>
    #                 <p>Some text about Baz</p>
    #             </div>
    #             <br>
    #             <p>Some concluding text about Foo</p>
    #         </div>
    #     </body>
    #     </html>"""
    splitter = HTMLHeaderTextSplitter(headers_to_split_on)
    try:
        html_header_splits = splitter.split_text(html)
    except ValueError:
        return

    chunk_size = 300
    chunk_overlap = 30
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    chunks = text_splitter.split_documents(html_header_splits)
    return chunks


def fetch_top_k_chunks_embedding(embedding: list[float], top_k: int) -> list[str]:
    with get_connection() as conn:
        cursor = conn.cursor()

        # fetch chunks
        cursor.execute(
            f"SELECT url, chunk FROM {Tables.stories_chunk} WHERE knn_match(embedding, {embedding}, {top_k}) ORDER BY _score DESC LIMIT {top_k}"
        )
        urls_chunks = cursor.fetchall()

    urls, chunks = list(zip(*urls_chunks))

    return urls, chunks
