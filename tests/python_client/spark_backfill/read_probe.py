"""Small PySpark application used by pytest to verify Connector read behavior."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from typing import Any

try:
    from .contracts import READ_RESULT_PREFIX
except ImportError:  # pragma: no cover - standalone ConfigMap execution
    from contracts import READ_RESULT_PREFIX


VIEW_NAME = "milvus_backfill_read"


def _row_dict(row) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)


def _field_names(schema) -> list[str]:
    names = schema.fieldNames
    return list(names() if callable(names) else names)


def execute_probe(
    *,
    load_dataframe: Callable[[Mapping[str, str]], Any],
    run_sql: Callable[[str], Any],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    options = {str(key): str(value) for key, value in dict(spec.get("options", {})).items()}
    primary_key = str(spec.get("primaryKey", "id"))
    frame = load_dataframe(options)
    count = frame.count()
    primary_keys = sorted(row[primary_key] for row in frame.select(primary_key).collect())
    result: dict[str, Any] = {
        "count": count,
        "primaryKeys": primary_keys,
        "schemaFields": _field_names(frame.schema),
    }

    projection_fields = [str(field) for field in spec.get("projectionFields", [])]
    if projection_fields:
        projection_options = dict(options)
        projection_options.update(
            {str(key): str(value) for key, value in dict(spec.get("projectionOptions", {})).items()}
        )
        projection = (
            load_dataframe(projection_options) if spec.get("projectionOptions") else frame.select(*projection_fields)
        )
        result["projection"] = {
            "fields": _field_names(projection.schema),
            "count": projection.count(),
        }

    sql_query = spec.get("sql")
    if sql_query:
        frame.createOrReplaceTempView(VIEW_NAME)
        result["sqlRows"] = [_row_dict(row) for row in run_sql(str(sql_query)).collect()]

    vector_search = spec.get("vectorSearch")
    if vector_search:
        vector_options = dict(options)
        vector_options.update(
            {
                "vector.search.query": json.dumps(vector_search["query"], separators=(",", ":")),
                "vector.search.topK": str(vector_search["topK"]),
                "vector.search.metric": str(vector_search.get("metric", "L2")),
                "vector.search.column": str(vector_search.get("column", "vector")),
                "vector.search.idColumn": str(vector_search.get("idColumn", primary_key)),
            }
        )
        result["topK"] = [_row_dict(row) for row in load_dataframe(vector_options).collect()]

    return result


def main() -> int:
    from pyspark.sql import SparkSession

    spec = json.loads(os.environ["SPARK_BACKFILL_READ_SPEC_JSON"])
    options = spec.setdefault("options", {})
    token = os.getenv("SPARK_BACKFILL_MILVUS_TOKEN", "")
    access_key = os.getenv("SPARK_BACKFILL_S3_ACCESS_KEY", "")
    secret_key = os.getenv("SPARK_BACKFILL_S3_SECRET_KEY", "")
    if bool(access_key) != bool(secret_key):
        raise ValueError("S3 access key and secret key must both be present or both be absent")
    if token:
        options["milvus.token"] = token
    if access_key:
        options["fs.access_key_id"] = access_key
        options["fs.access_key_value"] = secret_key
    spark = SparkSession.builder.appName("SparkMilvusBackfillReadProbe").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = execute_probe(
            load_dataframe=lambda options: spark.read.format("milvus").options(**options).load(),
            run_sql=spark.sql,
            spec=spec,
        )
        print(READ_RESULT_PREFIX + json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
