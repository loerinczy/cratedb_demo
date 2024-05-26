from utils.helper import (
    get_create_table_stmt,
    get_insert_stmt,
    load_snapshot,
    dict2tuple,
    tuple2dict,
    get_connection,
    get_batches,
)
from utils.catalog import Tables, Schemas
from utils.ai_client import AIClient
from utils.indexing import chunk_html, HEADERS_TO_SPLIT_ON, CHUNK_SIZE, CHUNK_OVERLAP


BATCH_SIZE = 20

client = AIClient(
    gpt_model="gpt-3.5-turbo-0125", embedding_model="text-embedding-3-small"
)

stories_text = load_snapshot("stories_text")
stories_text = tuple2dict(stories_text, Schemas.stories_text)

# TODO: experiment

insert_records = []

num_records = len(stories_text)
chunk_idx = 0

for idx, text_row in enumerate(stories_text):
    html = text_row["text"]
    chunks = chunk_html(
        html,
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        headers_to_split_on=HEADERS_TO_SPLIT_ON,
    )
    # in case of chunking does not return sensible data
    if not chunks:
        continue
    chunks = [chunk.page_content for chunk in chunks]
    embeddings = client.embed(chunks)
    for chunk, embedding in zip(chunks, embeddings):
        insert_row = text_row.copy()
        insert_row["chunk"] = chunk
        insert_row["chunk_idx"] = chunk_idx
        insert_row["embedding"] = embedding
        insert_records.append(insert_row)
        chunk_idx += 1
    print(f"Chunked and embedded the {idx + 1}. story out of {num_records}.", end="\r")

print("Finished the chunking and the embedding of the stories.")

insert_records = dict2tuple(insert_records, Schemas.stories_chunk)
create_table_stmt = get_create_table_stmt(Tables.stories_chunk, Schemas.stories_chunk)
insert_stmt = get_insert_stmt(Tables.stories_chunk, Schemas.stories_chunk)
num_batches = -(len(insert_records) // -BATCH_SIZE)

with get_connection() as conn:
    cursor = conn.cursor()

    # create target table
    cursor.execute(create_table_stmt)

    # insert data in batches
    batches = get_batches(insert_records, BATCH_SIZE)
    for idx, batch in enumerate(batches):
        cursor.executemany(insert_stmt, batch)
        print(f"Inserted the {idx+1}. batch out of {num_batches}.", end="\r")
