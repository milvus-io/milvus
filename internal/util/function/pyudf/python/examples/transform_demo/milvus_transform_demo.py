"""A deterministic wheel to exercise transformation stages without a model API."""

import pyarrow as pa


def vector(text):
    return [float(len(text)), 1.0]


class Ingestion:
    def transform(self, params, columns):
        rows = []
        for value in columns[0]:
            text = value.as_py()
            if text == "__FAIL__":
                raise ValueError("deliberate demo failure")
            if params.get("delay_seconds"):
                import time

                time.sleep(params["delay_seconds"])
            rows.append(
                [{"text": word, "vector": vector(word), "tags": ["demo", "chunk"]} for word in (text or "").split()]
            )
        return [
            pa.array(
                rows,
                type=pa.list_(
                    pa.struct(
                        [
                            ("text", pa.string()),
                            ("vector", pa.list_(pa.float32(), 2)),
                            ("tags", pa.list_(pa.string())),
                        ]
                    )
                ),
            )
        ]


class Query:
    def transform_query(self, params, columns):
        mode = params.get("mode", "embedding")
        if mode == "embedding":
            return [pa.array([vector(value.as_py()) for value in columns[0]], type=pa.list_(pa.float32(), 2))]
        if mode == "translation":
            return [
                pa.array(
                    [{"hello": "你好", "world": "世界"}.get(value.as_py(), value.as_py()) for value in columns[0]],
                    type=pa.string(),
                )
            ]
        return [
            pa.array(
                [{"text": value.as_py(), "start": 0} for value in columns[0]],
                type=pa.struct(
                    [
                        ("text", pa.string()),
                        ("start", pa.int64()),
                    ]
                ),
            )
        ]


def factory(context):
    return Ingestion() if context.stage == "ingestion" else Query()
