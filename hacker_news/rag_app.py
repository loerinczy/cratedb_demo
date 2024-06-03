import streamlit as st

from utils.retrieval import fetch_top_k_chunks_embedding
from utils.ai_client import AIClient

if "client" not in st.session_state:
    st.session_state.client = AIClient(
        gpt_model="gpt-3.5-turbo-0125", embedding_model="text-embedding-3-small"
    )

question = st.text_input(label="What is your question?")
use_rag = st.toggle(label="Use RAG", value=False)
if use_rag:
    top_k = st.slider(
        label="How many chunks should I use as context?",
        min_value=1,
        max_value=10,
        value=3,
    )

run = st.button("Submit")

if run:
    if use_rag:
        question_embedding = st.session_state.client.embed(question)
        urls, chunks = fetch_top_k_chunks_embedding(question_embedding, top_k)
        context = f"Context: " + " ".join(doc for doc in chunks)
    else:
        context = "No context for this query."
    system_prompt = "Answer the question with the help of the provided context."
    generated_answer = st.session_state.client.generate_answer(
        system=system_prompt, context=context, prompt=question
    )
    st.write("Generated answer: " + generated_answer)
    if use_rag:
        st.write("Articles used for the generation:")
        urls = set(urls)
        links = ""
        for i in urls:
            links += "- " + (f"[{i}](%s)" % i) + "\n"
        st.markdown(links)
