from types import SimpleNamespace

from spark_backfill.read_probe import execute_probe


class FakeRow(dict):
    def asDict(self, recursive=False):
        return dict(self)


class FakeDataFrame:
    def __init__(self, rows, fields):
        self.rows = [FakeRow(row) for row in rows]
        self.schema = SimpleNamespace(fieldNames=list(fields))
        self.view_name = None

    def count(self):
        return len(self.rows)

    def select(self, *fields):
        return FakeDataFrame([{field: row.get(field) for field in fields} for row in self.rows], fields)

    def collect(self):
        return self.rows

    def createOrReplaceTempView(self, name):
        self.view_name = name


def test_execute_probe_reports_count_projection_sql_and_topk():
    base = FakeDataFrame(
        [
            {"id": 0, "score": 1.0, "vector": [0.0, 0.0]},
            {"id": 1, "score": 2.0, "vector": [1.0, 1.0]},
        ],
        ["id", "score", "vector"],
    )
    vector = FakeDataFrame(
        [{"id": 1, "distance": 0.0}, {"id": 0, "distance": 2.0}],
        ["id", "distance"],
    )
    loaded_options = []

    def load_dataframe(options):
        loaded_options.append(options)
        if "vector.search.query" in options:
            return vector
        if "fieldIDs" in options:
            return base.select("id", "score")
        return base

    def run_sql(query):
        assert query == "SELECT COUNT(*) AS total FROM milvus_backfill_read"
        return FakeDataFrame([{"total": 2}], ["total"])

    result = execute_probe(
        load_dataframe=load_dataframe,
        run_sql=run_sql,
        spec={
            "options": {"milvus.uri": "http://milvus:19530"},
            "primaryKey": "id",
            "projectionFields": ["id", "score"],
            "projectionOptions": {"fieldIDs": "100,101"},
            "sql": "SELECT COUNT(*) AS total FROM milvus_backfill_read",
            "vectorSearch": {
                "query": [1.0, 1.0],
                "topK": 2,
                "metric": "L2",
                "column": "vector",
                "idColumn": "id",
            },
        },
    )

    assert result["count"] == 2
    assert result["primaryKeys"] == [0, 1]
    assert result["schemaFields"] == ["id", "score", "vector"]
    assert result["projection"] == {"fields": ["id", "score"], "count": 2}
    assert result["sqlRows"] == [{"total": 2}]
    assert result["topK"] == [{"id": 1, "distance": 0.0}, {"id": 0, "distance": 2.0}]
    assert loaded_options[1]["fieldIDs"] == "100,101"
    assert loaded_options[2]["vector.search.topK"] == "2"
