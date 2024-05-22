from langchain_community.document_loaders import AsyncHtmlLoader
from langchain_community.document_transformers import Html2TextTransformer

urls = ["https://huggingface.co/papers/2404.14619"]
loader = AsyncHtmlLoader(urls)
docs = loader.load()
html2text = Html2TextTransformer()
docs_transformed = html2text.transform_documents(docs)
print(docs_transformed[0].page_content)
