import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import AnnSearchRequest, DataType, WeightedRanker


class TestMilvusClientEmptyArrayTemplate(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    def test_empty_array_template_across_dml_apis(self):
        """
        target: verify empty array filter templates match inline empty array semantics
        method: exercise scalar, Array, and JSON predicates through query, search, hybrid search, and delete
        expected: every API accepts the template and returns the same result set as the inline expression
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "tags",
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_capacity=8,
            max_length=64,
        )
        schema.add_field("metadata", DataType.JSON)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)

        index_params = client.prepare_index_params()
        index_params.add_index(
            "vector",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 8, "efConstruction": 64},
        )

        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.insert(
            client,
            collection_name,
            [
                {"id": 1, "tags": [], "metadata": {"values": []}, "vector": [0.0, 0.0]},
                {
                    "id": 2,
                    "tags": ["blue"],
                    "metadata": {"values": ["blue"]},
                    "vector": [1.0, 1.0],
                },
            ],
        )
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_cases = [
            ("id in {values}", "id in []", []),
            ("id not in {values}", "id not in []", [1, 2]),
            ("array_contains_any(tags, {values})", "array_contains_any(tags, [])", []),
            ("array_contains_all(tags, {values})", "array_contains_all(tags, [])", [1, 2]),
            (
                'json_contains_any(metadata["values"], {values})',
                'json_contains_any(metadata["values"], [])',
                [],
            ),
            (
                'json_contains_all(metadata["values"], {values})',
                'json_contains_all(metadata["values"], [])',
                [1, 2],
            ),
        ]
        for template, inline, expected_ids in query_cases:
            inline_rows = self.query(
                client,
                collection_name,
                filter=inline,
                output_fields=["id"],
                limit=10,
            )[0]
            template_rows = self.query(
                client,
                collection_name,
                filter=template,
                filter_params={"values": []},
                output_fields=["id"],
                limit=10,
            )[0]
            assert sorted(row["id"] for row in inline_rows) == expected_ids
            assert sorted(row["id"] for row in template_rows) == expected_ids

        unused_param_rows = self.query(
            client,
            collection_name,
            filter="id >= {minimum}",
            filter_params={"minimum": 1, "unused": []},
            output_fields=["id"],
            limit=10,
        )[0]
        assert sorted(row["id"] for row in unused_param_rows) == [1, 2]

        empty_filter = "id in {values}"
        empty_params = {"values": []}
        search_result = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field="vector",
            search_params={"metric_type": "L2", "params": {"ef": 16}},
            filter=empty_filter,
            filter_params=empty_params,
            output_fields=["id"],
            limit=2,
        )[0][0]
        assert search_result == []

        hybrid_request = AnnSearchRequest(
            data=[[0.0, 0.0]],
            anns_field="vector",
            param={"metric_type": "L2", "params": {"ef": 16}},
            limit=2,
            expr=empty_filter,
            expr_params=empty_params,
        )
        hybrid_result = self.hybrid_search(
            client,
            collection_name,
            [hybrid_request],
            WeightedRanker(1.0),
            limit=2,
            output_fields=["id"],
        )[0][0]
        assert hybrid_result == []

        self.delete(
            client,
            collection_name,
            filter=empty_filter,
            filter_params=empty_params,
        )
        remaining_rows = self.query(
            client,
            collection_name,
            filter="id >= 1",
            output_fields=["id"],
            limit=10,
        )[0]
        assert sorted(row["id"] for row in remaining_rows) == [1, 2]
