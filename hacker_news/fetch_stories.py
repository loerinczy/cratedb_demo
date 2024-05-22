import json
from hackernews_api import HackerNewsAPI
from pathlib import Path
import crate
import json
import os
from utils import (
    generate_data_tuples,
    get_connection,
    get_insert_stmt,
    get_create_table_stmt,
    by2author,
    Tables,
    Schemas,
)


# from concurrent.futures import ThreadPoolExecutor
#     ids = range(SNAPSHOT_START_ID, SNAPSHOT_END_ID)
#     with ThreadPoolExecutor() as executor:
#         results = list(tqdm(executor.map(client.fetch_item_by_id, ids), total=len(ids)))

data_dir = Path(__file__).parents[1] / "data"

hn_api = HackerNewsAPI()
stories = hn_api.fetch_stories(500)

print("Number of fetched stories: " + str(len(stories)))
with open(data_dir / "stories.json", "w") as fp:
    json.dump(stories, fp)

for story in stories:
    by2author(story)


create_table_stmt = get_create_table_stmt(Tables.stories, Schemas.stories)
insert_tuples = generate_data_tuples(stories, Schemas.stories)
insert_stmt = get_insert_stmt(Tables.stories, Schemas.stories)

with get_connection() as conn:
    cursor = conn.cursor()
    # create table
    cursor.execute(create_table_stmt)

    # insert into table
    cursor.executemany(insert_stmt, insert_tuples)
