"""Check public assignment admission and an exact iterator snapshot."""

import sys
from pathlib import Path

from acceptance import Fixture, literal
from pymilvus import DataType, Function, FunctionType


def run(f):
    c = f.client
    name = f.candidate
    s = c.create_schema(auto_id=False, enable_dynamic_field=True)
    s.add_field("id", DataType.INT64, is_primary=True)
    s.add_field("vec", DataType.FLOAT_VECTOR, dim=2)
    s.add_field("flag", DataType.BOOL)
    s.add_field("arr", DataType.ARRAY, element_type=DataType.INT32, max_capacity=16)
    s.add_field("text", DataType.VARCHAR, max_length=128, enable_analyzer=True)
    s.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
    s.add_function(
        Function(
            name="bm25", function_type=FunctionType.BM25, input_field_names=["text"], output_field_names=["sparse"]
        )
    )
    idx = c.prepare_index_params()
    idx.add_index("vec", index_type="FLAT", metric_type="L2")
    idx.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
    c.create_collection(name, schema=s, index_params=idx, num_shards=1, timeout=90)
    c.insert(name, [{"id": 1, "vec": [1.0, 0.0], "flag": False, "arr": [0], "text": "hello", "extra": 1}])

    def update(flag, arr):
        resp = f.update(
            [1],
            [
                {"fieldName": "flag", "literal": {"scalarValue": {"boolData": flag}}},
                {"fieldName": "arr", "literal": {"arrayValue": {"intData": {"data": arr}}}},
            ],
        )
        f.accepted(resp, 1)
        return int(resp["timestamp"])

    old = update(True, [3, 4])
    update(False, [5])
    for field in ("sparse", "text", "extra", "$meta", "vec", "id"):
        response = f.update([1], [literal(field, 1)])
        assert int(response["status"]["code"]) == 1100, response
    rows = c.query(name, filter="id == 1", output_fields=["id", "flag", "arr"], consistency_level="Strong")
    f.record("current schema row", rows)
    assert rows[0]["flag"] is False and list(rows[0]["arr"]) == [5]
    rows = c.query(
        name,
        filter="id == 1",
        output_fields=["id", "flag", "arr"],
        iterator="True",
        guarantee_timestamp=old,
        consistency_level="Strong",
        timeout=30,
    )
    f.record("iterator exact " + str(old), rows)
    assert rows[0]["flag"] is True and list(rows[0]["arr"]) == [3, 4]


if __name__ == "__main__":
    run(Fixture(Path(sys.argv[1]).resolve()))
