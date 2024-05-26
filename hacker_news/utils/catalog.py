from collections import OrderedDict


class Schemas:
    stories = OrderedDict(
        [
            ("deleted", "boolean"),
            ("dead", "boolean"),
            ("parent", "int"),
            ("poll", "int"),
            ("parts", "array (int)"),
            ("author", "text"),
            ("descendants", "int"),
            ("id", "int"),
            ("kids", "array (int)"),
            ("score", "int"),
            ("time", "bigint"),
            ("title", "text"),
            ("type", "text"),
            ("url", "text"),
        ]
    )
    stories_text = OrderedDict(
        [
            ("author", "text"),
            ("id", "integer"),
            ("score", "integer"),
            ("time", "bigint"),
            ("title", "text"),
            ("url", "text"),
            ("text", "text"),
        ]
    )
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
    stories = Table("hackernews.stories", Schemas.stories)
    stories_text = Table("hackernews.stories_text", Schemas.stories_text)
    stories_chunk = Table("hackernews.stories_chunk", Schemas.stories_chunk)
