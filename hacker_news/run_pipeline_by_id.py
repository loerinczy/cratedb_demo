import requests

from utils.hackernews_api import HackerNewsAPI
from utils.helper import insert_data, get_create_table_stmt
from utils.catalog import Tables, Schemas
from utils.ai_client import AIClient
from utils.indexing import chunk_and_embed


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


# hn_api = HackerNewsAPI()
# stories = hn_api.fetch_topstories()
# for id in stories:
#     item = hn_api.fetch_item(id)
#     if "title" in item:
#         print(item["title"], item["url"])

title = "copy Scarlett Johansson’s voice for ChatGPT, records show"
story_id = search_by_title(title)
if not story_id:
    print("Story not found.")
    exit()
print(f"Story id: {story_id}")
# run_pipeline_by_id(story_id)
