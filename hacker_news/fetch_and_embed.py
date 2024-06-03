"""Auxiliary script to fetch, chunk, and embed certain articles."""

from utils.hackernews_api import HackerNewsAPI
from utils.helper import run_pipeline_by_id

# search for interesting articles that might be interesting to have question to
# hn_api = HackerNewsAPI()
# stories = hn_api.fetch_topstories()
# for id in stories:
#     item = hn_api.fetch_item(id)
#     if "title" in item:
#         print(id, item["title"], item["url"])

# once an interesting article is found, embed it
# story_id = 40453882
# run_pipeline_by_id(story_id)
