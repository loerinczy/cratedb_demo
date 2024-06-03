from crate.client.connection import Connection
import os
import crate.client as client
import requests

from .catalog import Table
from utils.hackernews_api import HackerNewsAPI
from utils.helper import insert_data, get_create_table_stmt
from utils.catalog import Tables
from utils.ai_client import AIClient
from utils.indexing import chunk_and_embed


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


def get_batches(data_tuples: list, batch_size: int = 10):
    generator = (
        data_tuples[i : i + batch_size] for i in range(0, len(data_tuples), batch_size)
    )
    return generator


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


def search_by_title(title: str):
    hn_api = HackerNewsAPI()
    topstories = hn_api.fetch_topstories()
    for story_id in topstories:
        story = hn_api.fetch_item(story_id)
        if "title" in story:
            if title in story["title"]:
                return story_id


def run_pipeline_by_id(story_id: int):
    # fetch HN story
    hn_api = HackerNewsAPI()
    story = hn_api.fetch_item(story_id)
    if story["type"] != "story":
        print("Error! Item is not story.")
        return
    insert_data([story], Tables.stories)

    # fetch article text
    resp = requests.get(story["url"], timeout=5)
    resp.raise_for_status()
    story["text"] = resp.text
    # insert_data([story], Tables.stories_text)

    # chunk and embed
    client = AIClient(
        gpt_model="gpt-3.5-turbo-0125", embedding_model="text-embedding-3-small"
    )
    story_chunk_embedding = chunk_and_embed(client, story)
    insert_data(story_chunk_embedding, Tables.stories_chunk)
