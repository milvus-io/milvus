import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.function_chain import col, fn

prefix = "function_chain"


class TestFunctionChain(TestMilvusClientV2Base):
    """Test pymilvus FunctionChain SDK integration."""

    dim = 2
    vector_field = "vector"
    scalar_field = "ts"

    def _create_function_chain_collection(self, client):
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(self.scalar_field, DataType.INT64)
        schema.add_field(self.vector_field, DataType.FLOAT_VECTOR, dim=self.dim)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field, index_type="FLAT", metric_type="L2")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )

        rows = [
            {"id": 1, self.scalar_field: 10, self.vector_field: [0.0, 0.0]},
            {"id": 2, self.scalar_field: 20, self.vector_field: [0.01, 0.0]},
            {"id": 3, self.scalar_field: 30, self.vector_field: [0.02, 0.0]},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return collection_name

    def _score_plus_ts_chain(self, stage):
        chain = FunctionChain(stage, name="score_plus_ts").map(
            "$score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )
        if stage == FunctionChainStage.L2_RERANK:
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        return chain

    @staticmethod
    def _hit_field(hit, field):
        if field in hit:
            return hit[field]
        return hit.get("entity", {}).get(field)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_reranks_by_scalar_field(self):
        """
        target: test pymilvus FunctionChain SDK with L0 rerank
        method: map $score = num_combine($score, ts) at L0 stage
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
        assert [hit["distance"] for hit in res[0]] == sorted(
            [hit["distance"] for hit in res[0]], reverse=True
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_uses_hidden_input_field(self):
        """
        target: test L0 FunctionChain SDK can use fields that are not returned
        method: rerank by ts while only requesting primary key output
        expected: search succeeds, result order follows ts, and ts is not returned
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=["id"],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert all(self._hit_field(hit, self.scalar_field) is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_l2_function_chain_sdk_reranks_by_scalar_field(self):
        """
        target: test pymilvus FunctionChain SDK with L2 rerank
        method: map $score = num_combine($score, ts), then sort by $score desc
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
