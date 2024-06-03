import os
import requests
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI

# from langchain import hub
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import PromptTemplate
from langchain.vectorstores import CrateDBVectorSearch
from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)


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

story_ids = [40453882]
hackernews_url = "https://hacker-news.firebaseio.com/v0/item/{}"
stories = [
    requests.get(hackernews_url.format(story_id), timeout=10) for story_id in story_ids
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


vectordb = CrateDBVectorSearch.from_documents(
    embedding=OpenAIEmbeddings(),
    documents=chunks,
    collection_name="langchain_rag",
    connection_string=CONNECTION_STRING,
)

retriever = vectordb.as_retriever()
llm = ChatOpenAI(model="gpt-3.5-turbo-0125")
prompt = PromptTemplate.from_template("Question: {question}, Context: {context}.")


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


rag_chain = (
    {"context": retriever | format_docs, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)

print(rag_chain.invoke("Does google have an AI panic?"))
