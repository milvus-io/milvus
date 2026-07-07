import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, Function, FunctionChain, FunctionChainStage, FunctionScore, FunctionType
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

    def _assert_search_error(self, client, collection_name, function_chains, err_msg, **kwargs):
        self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=function_chains,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: err_msg},
            **kwargs,
        )

    @staticmethod
    def _hit_field(hit, field):
        if field in hit:
            return hit[field]
        return hit.get("entity", {}).get(field)

    @staticmethod
    def _expected_l0_score(hit):
        vector = hit.get("entity", {}).get("vector", hit.get("vector"))
        l2_distance = sum(value * value for value in vector)
        return TestFunctionChain._hit_field(hit, "ts") - l2_distance

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
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
            output_fields=[self.scalar_field, self.vector_field],
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
        expected_scores = [self._expected_l0_score(hit) for hit in res[0]]
        assert expected_scores == sorted(expected_scores, reverse=True)
        assert [pytest.approx(abs(hit["distance"]), rel=1e-5) for hit in res[0]] == expected_scores

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
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

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_l0_function_chain_sdk_can_read_id_system_input(self):
        """
        target: test L0 FunctionChain SDK can read public system input $id
        method: map $score = num_combine($score, $id) at L0 stage
        expected: search succeeds and result order follows rewritten score
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="score_plus_id").map(
            "$score",
            fn.num_combine(col("$score"), col("$id"), mode="sum"),
        )

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=chain,
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_sort_op(self):
        """
        target: test L0 FunctionChain SDK rejects non-map operators
        method: use sort op at L0 stage
        expected: request fails because public L0 currently only supports map op
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_sort").sort(
            col("$score"),
            desc=True,
            tie_break_col=col("$id"),
        )

        self._assert_search_error(
            client, collection_name, chain, 'type "sort" is not supported by L0 rerank function chain'
        )

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_write_readonly_system_column(self):
        """
        target: test L0 FunctionChain SDK rejects writes to read-only system columns
        method: write map output to $id
        expected: request fails because only $score is writable in public L0 chains
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_write_id").map(
            "$id",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$id" is not writable')

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_read_internal_system_input(self):
        """
        target: test L0 FunctionChain SDK rejects internal system input columns
        method: read $seg_offset from a map expression
        expected: request fails because public L0 only exposes $id and $score as readable system inputs
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_seg_offset_input").map(
            "$score",
            fn.num_combine(col("$seg_offset"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$seg_offset" is not readable')

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_read_unknown_system_input(self):
        """
        target: test L0 FunctionChain SDK rejects unknown system input columns
        method: read $tmp_score from a map expression before it is produced
        expected: request fails because users cannot invent new $-prefixed system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_unknown_system_input").map(
            "$score",
            fn.num_combine(col("$tmp_score"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$tmp_score" is not readable')

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_reserved_temp_output(self):
        """
        target: test L0 FunctionChain SDK rejects user temporary columns in system namespace
        method: write a map output named $tmp_score
        expected: request fails because $ prefix is reserved for system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L0_RERANK, name="bad_l0_reserved_temp_output").map(
            "$tmp_score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$tmp_score" is not writable')

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_with_function_score(self):
        """
        target: test search rejects ambiguous L0 rerank APIs
        method: send boost FunctionScore and L0 function chain together
        expected: request fails because function_score and function_chains are mutually exclusive
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        function = Function(
            name="boost_ts",
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params={"reranker": "boost", "weight": "1.5"},
        )
        function_score = FunctionScore(functions=[function])

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
            "function_chains and ranker cannot be used together",
            ranker=function_score,
        )

    @pytest.mark.skip(reason="official pymilvus SDK does not support L0 function chain yet")
    @pytest.mark.tags(CaseLabel.L0)
    def test_search_rejects_l0_function_chain_with_order_by(self):
        """
        target: test search rejects order_by with L0 function rerank
        method: send order_by_fields and L0 function chain together
        expected: request fails because they define conflicting sort criteria
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L0_RERANK),
            "order_by and function rerank cannot be used together",
            order_by_fields=[{"field": self.scalar_field, "order": "asc"}],
        )

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_l2_function_chain_sdk_uses_hidden_input_field(self):
        """
        target: test L2 FunctionChain SDK can use fields that are not returned
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
            function_chains=self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert all(self._hit_field(hit, self.scalar_field) is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_l2_function_chain_sdk_temp_column_not_returned(self):
        """
        target: test L2 FunctionChain SDK can use ordinary temporary columns
        method: write tmp_score, write it back to $score, then sort by $score desc
        expected: search succeeds, rerank order is correct, and tmp_score is not returned
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = (
            FunctionChain(FunctionChainStage.L2_RERANK, name="l2_temp_score")
            .map("tmp_score", fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"))
            .map("$score", fn.num_combine(col("tmp_score"), col("$score"), mode="sum"))
            .sort(col("$score"), desc=True, tie_break_col=col("$id"))
        )

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            output_fields=[self.scalar_field],
            function_chains=chain,
        )

        assert [hit["id"] for hit in res[0]] == [3, 2, 1]
        assert [self._hit_field(hit, self.scalar_field) for hit in res[0]] == [30, 20, 10]
        assert all(self._hit_field(hit, "tmp_score") is None for hit in res[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_l2_function_chain_sdk_limit_op(self):
        """
        target: test L2 FunctionChain SDK supports limit operator
        method: request limit=3 and apply function chain limit op with limit=2
        expected: search succeeds and returns only function-chain-limited results
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="l2_limit").limit(2)

        res, _ = self.search(
            client,
            collection_name,
            data=[[0.0, 0.0]],
            anns_field=self.vector_field,
            search_params={"metric_type": "L2"},
            limit=3,
            function_chains=chain,
        )

        assert len(res[0]) == 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_write_readonly_system_column(self):
        """
        target: test L2 FunctionChain SDK rejects writes to read-only system columns
        method: write map output to $id
        expected: request fails because only $score is writable in L2 rerank chains
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_write_id").map(
            "$id",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$id" is not writable')

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_reserved_temp_output(self):
        """
        target: test L2 FunctionChain SDK rejects user temporary columns in system namespace
        method: write a map output named $tmp_score
        expected: request fails because $ prefix is reserved for system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_reserved_temp_output").map(
            "$tmp_score",
            fn.num_combine(col("$score"), col(self.scalar_field), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system output "$tmp_score" is not writable')

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_read_internal_system_input(self):
        """
        target: test L2 FunctionChain SDK rejects internal system input columns
        method: read $seg_offset from a map expression
        expected: request fails because L2 only exposes selected system inputs
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_seg_offset_input").map(
            "$score",
            fn.num_combine(col("$seg_offset"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$seg_offset" is not supported')

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_read_unknown_system_input(self):
        """
        target: test L2 FunctionChain SDK rejects unknown system input columns
        method: read $tmp_score from a map expression before it is produced
        expected: request fails because users cannot invent new $-prefixed system columns
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK, name="bad_l2_unknown_system_input").map(
            "$score",
            fn.num_combine(col("$tmp_score"), col("$score"), mode="sum"),
        )

        self._assert_search_error(client, collection_name, chain, 'system input "$tmp_score" is not supported')

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_with_function_score(self):
        """
        target: test search rejects ambiguous L2 rerank APIs
        method: send boost FunctionScore and L2 function chain together
        expected: request fails because function chains and ranker are mutually exclusive
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)
        function = Function(
            name="boost_ts",
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params={"reranker": "boost", "weight": "1.5"},
        )
        function_score = FunctionScore(functions=[function])

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
            "function_chains and ranker cannot be used together",
            ranker=function_score,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_rejects_l2_function_chain_with_order_by(self):
        """
        target: test search rejects order_by with L2 function rerank
        method: send order_by_fields and L2 function chain together
        expected: request fails because they define conflicting sort criteria
        """
        client = self._client()
        collection_name = self._create_function_chain_collection(client)

        self._assert_search_error(
            client,
            collection_name,
            self._score_plus_ts_chain(FunctionChainStage.L2_RERANK),
            "order_by and function rerank cannot be used together",
            order_by_fields=[{"field": self.scalar_field, "order": "asc"}],
        )
