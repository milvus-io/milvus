import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType


class TestJsonScalarShape(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dynamic", [False, True])
    def test_json_flat_preserves_scalar_shape(self, dynamic):
        client = self._client()
        name = cf.gen_unique_str("json_scalar_shape")
        schema = client.create_schema(auto_id=False, enable_dynamic_field=dynamic)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("v", DataType.FLOAT_VECTOR, dim=2)
        if not dynamic:
            schema.add_field("j", DataType.JSON)
        indexes = client.prepare_index_params()
        indexes.add_index("v", index_type="FLAT", metric_type="L2")
        client.create_collection(name, schema=schema, index_params=indexes, consistency_level="Strong", num_shards=1)
        try:
            documents = [
                {"a": 1, "b": True, "s": "one"},
                {"a": [1, 2], "b": [True, False], "s": ["one", "two"]},
                {"a": 2, "b": False, "s": "two"},
                {"a": None, "b": None, "s": None},
                {"a": {"nested": 1}, "b": {}, "s": {}},
                {},
            ]
            rows = [{"id": i, "v": [0.0, 0.0], "j": j} for i, j in enumerate(documents)]
            rows.extend({"id": i, "v": [1.0, 1.0], "j": {}} for i in range(100, 3100))
            client.insert(name, rows)
            expressions = [
                "j['a'] == 1", "j['a'] != 1", "j['a'] > 0",
                "j['a'] in [1, 2]", "j['a'] not in [1, 2]", "not (j['a'] == 1)",
                "j['a'] is null", "j['a'] is not null",
                "j['b'] == true", "j['b'] != true",
                "j['s'] == 'one'", "j['s'] != 'one'", "j['s'] like 'on%'",
            ]

            def evaluate():
                results = {}
                for expression in expressions:
                    predicate = f"id < 10 and ({expression})"
                    query = client.query(name, filter=predicate, output_fields=["id"])
                    search = client.search(name, [[0.0, 0.0]], anns_field="v", filter=predicate, limit=10)
                    ids = sorted(row["id"] for row in query)
                    assert sorted(hit["id"] for hit in search[0]) == ids
                    results[expression] = ids
                return results

            expected = evaluate()
            assert expected["j['a'] == 1"] == [0]
            assert expected["j['b'] == true"] == [0]
            assert expected["j['s'] == 'one'"] == [0]
            client.flush(name)
            client.release_collection(name)
            client.load_collection(name)
            assert evaluate() == expected
            indexes = client.prepare_index_params()
            indexes.add_index("j", index_name="json_flat", index_type="INVERTED",
                              params={"json_cast_type": "JSON", "json_path": "j"})
            client.create_index(name, indexes)
            assert client.describe_index(name, "json_flat")["indexed_rows"] == len(rows)
            client.release_collection(name)
            client.load_collection(name)
            assert evaluate() == expected
        finally:
            client.drop_collection(name)
