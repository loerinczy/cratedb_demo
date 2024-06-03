from collections import OrderedDict


class Schemas:
    stories_chunk = OrderedDict(
        [
            ("author", "text"),
            ("id", "integer"),
            ("score", "integer"),
            ("time", "bigint"),
            ("title", "text"),
            ("url", "text"),
            ("chunk", "text"),
            ("chunk_idx", "int"),
            ("embedding", "FLOAT_VECTOR(1536)"),
        ]
    )


class Table:
    name: str
    schema: OrderedDict

    def __init__(self, name: str, schema: OrderedDict) -> None:
        self.name = name
        self.schema = schema


class Tables:
    stories_chunk = Table("hackernews.stories_chunk", Schemas.stories_chunk)
