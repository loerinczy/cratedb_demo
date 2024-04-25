import json
import requests
from utils import (
    get_connection,
    get_create_table_stmt,
    get_insert_stmt,
    generate_data_tuples,
    map_to_schema,
    get_col,
    Tables,
    Schemas,
)


with get_connection() as conn:
    cursor = conn.cursor()

    # fetch data
    cursor.execute(f"select * from {Tables.stories} where url is not null")
    stories = cursor.fetchall()


insert_records = []
failed_urls = {}
for story in stories:
    try:
        print(story)
        url = story["url"]
        resp = requests.get(url)
    except requests.ConnectionError:
        failed_urls[url] = "connection_error"
        continue
    if resp.status_code == requests.status_codes.ok:
        record = []
        record["text"] = resp.text
        insert_records.append(record)
    else:
        failed_urls[url] = resp.status_code

print(f"Succesfully fetched {len(insert_records)} out of {len(stories)} pages.")
print(f"Error codes: \n {json.dumps(failed_urls, indent=4)}")

create_table_stmt = get_create_table_stmt(Tables.stories_text, Schemas.stories_text)
insert_tuple = generate_data_tuples(insert_records, Schemas.stories_text)
insert_stmt = get_insert_stmt(Tables.stories_text, Schemas.stories_text)

with get_connection() as conn:
    cursor = conn.cursor()

    # create target table
    cursor.execute(create_table_stmt)

    # insert data
    cursor.executemany(insert_stmt, insert_tuple)
