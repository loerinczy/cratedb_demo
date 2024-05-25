import json
import requests
from utils.helper import (
    get_connection,
    get_create_table_stmt,
    get_insert_stmt,
    get_batches,
    get_col,
    commit_snapshot,
    load_snapshot,
    map_to_schema,
)
from utils.catalog import Tables, Schemas


with get_connection() as conn:
    cursor = conn.cursor()

    # fetch data
    cursor.execute(f"select * from {Tables.stories} where url is not null")
    stories = cursor.fetchall()


insert_records = []
failed_urls = {}
for idx, story in enumerate(stories):
    try:
        url = get_col(story, Schemas.stories, "url")
        print(f"Fetching {idx}. url", end="\r")
        resp = requests.get(url, timeout=5)
    except requests.exceptions.RequestException as e:
        failed_urls[url] = str(e)
        continue
    if resp.status_code == requests.codes.ok:
        record = map_to_schema(story, Schemas.stories, Schemas.stories_text)
        record.append(resp.text)
        insert_records.append(record)
    else:
        failed_urls[url] = f"{resp.status_code} {resp.reason}"

# create snapshot in case of downstream failure
commit_snapshot(insert_records, "stories_text")
print(f"Succesfully fetched {len(insert_records)} out of {len(stories)} pages.")
# print(f"Error codes: \n {json.dumps(failed_urls, indent=4)}")
# create_table_stmt = get_create_table_stmt(Tables.stories_text, Schemas.stories_text)
# insert_stmt = get_insert_stmt(Tables.stories_text, Schemas.stories_text)
# insert_records = load_snapshot("stories_text")

# with get_connection() as conn:
#     cursor = conn.cursor()

#     # create target table
#     # cursor.execute(create_table_stmt)

#     # insert data in batches
#     batches = get_batches(insert_records, 3)
#     for idx, batch in enumerate(batches):
#         cursor.executemany(insert_stmt, batch)
#         print(f"Inserted the {idx+1}. batch.", end="\r")
