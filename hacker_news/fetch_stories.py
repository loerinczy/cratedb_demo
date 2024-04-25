import json
from hackernews_api import HackerNewsAPI
from pathlib import Path

# from concurrent.futures import ThreadPoolExecutor
#     ids = range(SNAPSHOT_START_ID, SNAPSHOT_END_ID)
#     with ThreadPoolExecutor() as executor:
#         results = list(tqdm(executor.map(client.fetch_item_by_id, ids), total=len(ids)))

data_dir = Path(__file__).parents[1] / "data"

hn_api = HackerNewsAPI()
maxitem = hn_api.get_max_item_id()
ITEM_MIN = 8000
ITEM_MAX = 1500
min_id = maxitem - ITEM_MIN
max_id = maxitem - ITEM_MAX
fetched_items = []
for idx, item_id in enumerate(range(min_id, max_id)):
    item = hn_api.fetch_item(item_id)
    if idx % 100 == 0:
        print("\ritem_id: " + str(idx), end="")
    if item["type"] == "story":
        fetched_items.append(item)

print("number of fetched items: " + str(len(fetched_items)))
with open(data_dir / "stories.json", "w") as fp:
    json.dump(fetched_items, fp)
