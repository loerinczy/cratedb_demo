from .catalog import Tables
from .helper import get_connection


def fetch_top_k_chunks_embedding(embedding: list[float], top_k: int) -> list[str]:
    with get_connection() as conn:
        cursor = conn.cursor()

        # fetch chunks
        cursor.execute(
            f"SELECT url, chunk FROM {Tables.stories_chunk.name} WHERE knn_match(embedding, {embedding}, {top_k}) ORDER BY _score DESC LIMIT {top_k}"
        )
        urls_chunks = cursor.fetchall()

    urls, chunks = list(zip(*urls_chunks))

    return urls, chunks
