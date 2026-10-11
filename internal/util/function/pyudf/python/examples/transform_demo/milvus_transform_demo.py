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
    def transform_query(self, params, columns, *, context):
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
        if mode == "rerank":
            query = context.original_queries[0].values[context.query_index]
            preferred = {"hello": "alpha", "world": "gamma"}.get(query)
            return [pa.array([1.0 if value.as_py() == preferred else 0.1 for value in columns[0]], type=pa.float32())]
        raise ValueError(f"unsupported query mode: {mode}")


class PostProcess:
    def transform_batch(self, params, batches, *, context):
        if params.get("mode") == "summary":
            if not context.table_result or not context.reduction:
                raise ValueError("summary requires a POST TABLE reduce operator")
            output_type = pa.struct(
                [("texts", pa.list_(pa.string())), ("count", pa.int64()), ("query_count", pa.int64())]
            )
            return [
                [
                    pa.array(
                        [
                            {
                                "texts": columns[0].to_pylist(),
                                "count": len(columns[0]),
                                "query_count": context.query_count,
                            }
                        ],
                        type=output_type,
                    )
                ]
                for columns in batches
            ]
        if params.get("mode") == "uppercase":
            return [
                [
                    pa.array(
                        [None if value.as_py() is None else value.as_py().upper() for value in columns[0]],
                        type=pa.string(),
                    )
                ]
                for columns in batches
            ]
        total_hits = sum(len(columns[0]) for columns in batches)
        output_type = pa.struct(
            [
                ("text", pa.string()),
                ("start", pa.int64()),
                ("query", pa.string()),
                ("query_index", pa.int64()),
                ("query_count", pa.int64()),
                ("total_hits", pa.int64()),
            ]
        )
        return [
            [
                pa.array(
                    [
                        {
                            "text": value.as_py(),
                            "start": 0,
                            "query": context.original_queries[0].values[index],
                            "query_index": index,
                            "query_count": context.query_count,
                            "total_hits": total_hits,
                        }
                        for value in columns[0]
                    ],
                    type=output_type,
                )
            ]
            for index, columns in enumerate(batches)
        ]


def factory(context):
    if context.stage == "post_process":
        return PostProcess()
    return Ingestion() if context.stage == "ingestion" else Query()
