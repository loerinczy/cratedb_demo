from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)

from utils.ai_client import AIClient

HEADERS_TO_SPLIT_ON = [
    ("h1", "Header 1"),
    ("h2", "Header 2"),
    ("h3", "Header 3"),
    ("h4", "Header 4"),
    ("h5", "Header 5"),
    ("h6", "Header 6"),
]

CHUNK_SIZE = 600
CHUNK_OVERLAP = 60


def chunk_html(
    html: str, chunk_size: int, chunk_overlap: int, headers_to_split_on: dict[str, str]
):
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


def chunk_and_embed(
    client: AIClient,
    story: dict,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
    headers_to_split_on: list = HEADERS_TO_SPLIT_ON,
) -> list[dict]:
    html = story["text"]
    chunks = chunk_html(
        html,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        headers_to_split_on=headers_to_split_on,
    )
    if not chunks:
        print("No chunk returned.")
        return
    chunks = [chunk.page_content for chunk in chunks]
    embeddings = client.embed(chunks)
    insert_records = []
    for chunk, embedding in zip(chunks, embeddings):
        insert_row = story.copy()
        insert_row["chunk"] = chunk
        insert_row["chunk_idx"] = 0
        insert_row["embedding"] = embedding
        insert_records.append(insert_row)
    return insert_records
