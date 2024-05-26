import os
import requests

from openai import OpenAI
import crate.client as client

from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)

llm = OpenAI()

pw = os.getenv("CRATEDB_PASSWORD")
host = os.getenv("CRATEDB_HOST")
user = os.getenv("CRATEDB_USER")
crate_schema = "hackernews_rag"
CONNECTION_STRING = f"crate://{user}:{pw}@{host}?ssl=true&schema=hackernews_rag"
headers_to_split_on = [
    ("h1", "Header 1"),
    ("h2", "Header 2"),
    ("h3", "Header 3"),
    ("h4", "Header 4"),
    ("h5", "Header 5"),
    ("h6", "Header 6"),
]
chunk_size = 300
chunk_overlap = 30

do_indexing = False

if do_indexing:
    story_ids = [40453882]
    hackernews_url = "https://hacker-news.firebaseio.com/v0/item/{}.json"
    stories = [
        requests.get(hackernews_url.format(story_id), timeout=10).json()
        for story_id in story_ids
    ]
    raw_htmls = [requests.get(story["url"], timeout=5).text for story in stories]
    html_splitter = HTMLHeaderTextSplitter(headers_to_split_on)
    html_header_splits_all = [html_splitter.split_text(html) for html in raw_htmls]

    char_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    chunks = []
    for html_header_splits in html_header_splits_all:
        chunks.extend(char_splitter.split_documents(html_header_splits))

    chunks = [chunk.page_content.replace("\n", " ") for chunk in chunks]

    embeddings = [
        data.embedding
        for data in llm.embeddings.create(
            input=chunks, model="text-embedding-3-small"
        ).data
    ]

    # insert chunks & embeddings into the table
    with client.connect(
        f"https://{host}",
        username=user,
        password=pw,
    ) as conn:
        cursor = conn.cursor()
        insert_stmt = """
            insert into hackernews_simple (chunk, embedding)
            values (?, ?)
        """
        cursor.executemany(insert_stmt, list(zip(chunks, embeddings)))

prompt = "Does google have an AI panic?"

# fetch relevant chunks
prompt_embedding = (
    llm.embeddings.create(input=[prompt], model="text-embedding-3-small")
    .data[0]
    .embedding
)

with client.connect(
    f"https://{host}",
    username=user,
    password=pw,
) as conn:
    cursor = conn.cursor()
    cursor.execute(
        f"""
            select chunk 
            from hackernews_simple
            where knn_match(embedding, {prompt_embedding}, 3) 
            order by _score desc 
            limit 4
        """
    )
    chunks = cursor.fetchall()

context = f"Context: " + " ".join(doc[0] for doc in chunks)
system_prompt = "Answer the question with the help of the provided context."
answer = (
    llm.chat.completions.create(
        model="gpt-3.5-turbo-0125",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "assistant", "content": context},
            {"role": "user", "content": prompt},
        ],
        max_tokens=1000,
    )
    .choices[0]
    .message.content
)

print("Answer: ", answer)
