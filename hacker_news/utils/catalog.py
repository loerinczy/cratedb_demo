from collections import OrderedDict


class Tables:
    stories = "bronze.raw"
    stories_text = "hackernews.stories_text"
    stories_chunk = "hackernews.stories_chunk"
    test = "demo.test"


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
    test = OrderedDict(
        [
            ("author", "text"),
            ("id", "integer"),
            ("score", "integer"),
            ("time", "bigint"),
            ("title", "text"),
            ("url", "text"),
            ("chunk", "text"),
            ("embedding", "FLOAT_VECTOR(1536)"),
        ]
    )
