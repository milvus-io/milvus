import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import CollectionSchema, DataType, FieldSchema
from pymilvus.orm.types import CONSISTENCY_STRONG


class TestGrowingCompare(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["query", "count", "search"])
    @pytest.mark.parametrize(
        "rows,first_match,sealed",
        [
            (8192, False, False),
            (8193, False, False),
            (16385, False, False),
            (8193, True, False),
            (8193, False, True),
        ],
    )
    def test_compare_after_short_circuited_batch(self, rows, first_match, sealed, operation):
        fields = [
            FieldSchema("id", DataType.INT64, is_primary=True),
            FieldSchema("gate", DataType.INT64),
            FieldSchema("label", DataType.VARCHAR, max_length=16),
            FieldSchema("left", DataType.INT32),
            FieldSchema("right", DataType.INT32),
            FieldSchema("vector", DataType.FLOAT_VECTOR, dim=8),
        ]
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str("growing_compare"),
            schema=CollectionSchema(fields, enable_dynamic_field=False),
            consistency_level=CONSISTENCY_STRONG,
            shards_num=1,
        )
        collection_w.create_index("vector", {"index_type": "FLAT", "metric_type": "L2"})
        collection_w.load()
        expected_ids = sorted({rows - 1} | ({0} if first_match else set()))
        for start in range(0, rows, 4096):
            collection_w.insert(
                [
                    {
                        "id": i,
                        "gate": int(i in expected_ids),
                        "label": "match" if i in expected_ids else "other",
                        "left": 2,
                        "right": 1,
                        "vector": [0.0] * 8,
                    }
                    for i in range(start, min(start + 4096, rows))
                ]
            )
        if sealed:
            collection_w.flush()
            collection_w.release()
            collection_w.load()

        for expression in [
            'gate == 1 and label == "match"',
            "gate == 1 and left > right",
            'gate == 1 and label == "match" and left > right',
        ]:
            if operation == "query":
                result, _ = collection_w.query(expression, output_fields=["id"], limit=10)
                assert sorted(row["id"] for row in result) == expected_ids
            elif operation == "count":
                result, _ = collection_w.query(expression, output_fields=["count(*)"])
                assert result == [{"count(*)": len(expected_ids)}]
            else:
                result, _ = collection_w.search(
                    [[0.0] * 8],
                    "vector",
                    {"metric_type": "L2", "params": {}},
                    limit=10,
                    expr=expression,
                )
                assert sorted(result[0].ids) == expected_ids
