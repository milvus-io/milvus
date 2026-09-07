import pytest
from base.testbase import TestBase
from pymilvus import Collection
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

prefix = "function_chain_api"


@pytest.mark.tags(CaseLabel.L0)
class TestFunctionChainAPI(TestBase):
    """
    ******************************************************************
      The following cases test Function Chain RESTful API integration.
    ******************************************************************
    """

    def _create_function_chain_collection(self, data=None):
        name = gen_collection_name(prefix)
        self.name = name
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "ts", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                ],
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector", "indexType": "FLAT", "metricType": "L2"},
            ],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, f"create collection failed: {rsp}"

        if data is None:
            data = [
                {"id": 1, "ts": 10, "vector": [0.0, 0.0]},
                {"id": 2, "ts": 20, "vector": [0.01, 0.0]},
                {"id": 3, "ts": 30, "vector": [0.02, 0.0]},
            ]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": data})
        assert rsp["code"] == 0, f"insert failed: {rsp}"
        assert rsp["data"]["insertCount"] == len(data)
        Collection(name).flush()
        self.collection_client.collection_load(collection_name=name)
        self.wait_load_completed(name, timeout=30)
        return name

    def _score_plus_ts_function_chain(self):
        return [
            {
                "name": "l2_score_plus_ts",
                "stage": "FunctionChainStageL2Rerank",
                "ops": [
                    {
                        "op": "map",
                        "outputs": ["$score"],
                        "expr": {
                            "name": "num_combine",
                            "args": [
                                {"column": "$score"},
                                {"column": "ts"},
                            ],
                            "params": {"mode": "sum"},
                        },
                    },
                    {
                        "op": "sort",
                        "inputs": ["$score"],
                        "params": {"column": "$score", "desc": True},
                    },
                ],
            }
        ]

    def _l0_boost_equivalent_function_chain(self):
        return [
            {
                "name": "l0_boost_ts_flag",
                "stage": "FunctionChainStageL0Rerank",
                "ops": [
                    {
                        "op": "map",
                        "outputs": ["$score"],
                        "expr": {
                            "name": "num_combine",
                            "args": [
                                {"column": "$score"},
                                {"column": "ts"},
                            ],
                            "params": {"mode": "weighted", "weights": [1, 10]},
                        },
                    }
                ],
            }
        ]

    def _boost_ts_flag_function_score(self):
        return {
            "functions": [
                {
                    "name": "boost_ts_flag",
                    "type": "Rerank",
                    "inputFieldNames": [],
                    "params": {
                        "reranker": "boost",
                        "filter": "ts > 0",
                        "weight": 10,
                    },
                }
            ],
            "params": {"boost_mode": "sum"},
        }

    def test_search_l0_function_chain_matches_boost_rank(self):
        """
        target: test REST v2 L0 functionChains can cover boost rank semantics
        method: compare L0 chain score + 10 * ts_flag with boost ranker filter ts > 0 and weight 10
        expected: both requests return the same reranked ids
        """
        data = [
            {"id": 1, "ts": 0, "vector": [0.0, 0.0]},
            {"id": 2, "ts": 1, "vector": [0.2, 0.0]},
            {"id": 3, "ts": 0, "vector": [0.1, 0.0]},
        ]
        name = self._create_function_chain_collection(data=data)

        base_payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "outputFields": ["ts"],
        }
        chain_rsp = self.vector_client.vector_search(
            {**base_payload, "functionChains": self._l0_boost_equivalent_function_chain()}
        )
        assert chain_rsp["code"] == 0, f"search with L0 functionChains failed: {chain_rsp}"

        boost_rsp = self.vector_client.vector_search(
            {**base_payload, "functionScore": self._boost_ts_flag_function_score()}
        )
        assert boost_rsp["code"] == 0, f"search with boost rank failed: {boost_rsp}"

        chain_ids = [item["id"] for item in chain_rsp["data"]]
        boost_ids = [item["id"] for item in boost_rsp["data"]]
        assert chain_ids == boost_ids == [2, 1, 3]

    def test_search_with_function_chains_reranks_by_scalar_field(self):
        """
        target: test REST v2 search with functionChains
        method: map $score = num_combine($score, ts), then sort $score desc
        expected: search succeeds and result order follows rewritten score
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "outputFields": ["ts"],
            "functionChains": self._score_plus_ts_function_chain(),
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] == 0, f"search with functionChains failed: {rsp}"
        assert len(rsp["data"]) == 3

        ids = [item["id"] for item in rsp["data"]]
        timestamps = [item["ts"] for item in rsp["data"]]
        distances = [item["distance"] for item in rsp["data"]]

        assert ids == [3, 2, 1]
        assert timestamps == [30, 20, 10]
        assert distances == sorted(distances, reverse=True)
        assert distances[0] > 29
        assert distances[-1] > 9

    def test_search_function_chains_can_use_hidden_input_field(self):
        """
        target: test REST v2 search fetches functionChain input fields even when not returned
        method: rerank by ts but only request id in outputFields
        expected: search succeeds, result order follows ts, and hidden ts is not returned
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "outputFields": ["id"],
            "functionChains": self._score_plus_ts_function_chain(),
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] == 0, f"search with hidden functionChain input failed: {rsp}"
        assert [item["id"] for item in rsp["data"]] == [3, 2, 1]
        assert all("ts" not in item for item in rsp["data"])

    def test_search_function_chains_limit_op(self):
        """
        target: test REST v2 search supports functionChain limit operator
        method: request limit=3 and apply functionChains limit op with limit=2
        expected: search returns only function-chain-limited results
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "l2_limit",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "limit",
                            "params": {"limit": 2},
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] == 0, f"search with functionChains limit failed: {rsp}"
        assert len(rsp["data"]) == 2

    def test_search_function_chains_temp_column_not_returned_and_rerank_correct(self):
        """
        target: test REST v2 search supports normal temporary functionChain columns
        method: write rerank score to tmp_score, then write tmp_score back to $score and sort
        expected: search succeeds, rerank order is correct, and tmp_score is not returned
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "outputFields": ["ts"],
            "functionChains": [
                {
                    "name": "l2_temp_score",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["tmp_score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$score"},
                                    {"column": "ts"},
                                ],
                                "params": {"mode": "sum"},
                            },
                        },
                        {
                            "op": "map",
                            "outputs": ["$score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "tmp_score"},
                                    {"column": "$score"},
                                ],
                                "params": {"mode": "sum"},
                            },
                        },
                        {
                            "op": "sort",
                            "inputs": ["$score"],
                            "params": {"column": "$score", "desc": True},
                        },
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] == 0, f"search with temporary functionChain column failed: {rsp}"
        assert [item["id"] for item in rsp["data"]] == [3, 2, 1]
        assert [item["ts"] for item in rsp["data"]] == [30, 20, 10]
        assert all("tmp_score" not in item for item in rsp["data"])

    def test_search_rejects_function_chains_reserved_temp_output(self):
        """
        target: test REST v2 functionChains reject user temporary columns in system namespace
        method: write a map output named $tmp_score
        expected: request fails because $ prefix is reserved for system columns
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_reserved_output",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$tmp_score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$score"},
                                    {"column": "ts"},
                                ],
                                "params": {"mode": "sum"},
                            },
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "$tmp_score" in rsp["message"]

    def test_search_rejects_function_chains_write_readonly_system_column(self):
        """
        target: test REST v2 functionChains reject writes to read-only system columns
        method: write map output to $id
        expected: request fails because only $score is writable in L2 rerank chains
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_write_id",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$id"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$score"},
                                    {"column": "ts"},
                                ],
                                "params": {"mode": "sum"},
                            },
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "$id" in rsp["message"]

    def test_search_rejects_function_chains_unknown_system_input(self):
        """
        target: test REST v2 functionChains reject unknown system input columns
        method: read $tmp_score from a map expression
        expected: request fails because users cannot invent new $-prefixed system columns
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_system_input",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$tmp_score"},
                                    {"literal": 1},
                                ],
                                "params": {"mode": "sum"},
                            },
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "$tmp_score" in rsp["message"]

    def test_search_rejects_function_chains_unreadable_system_input(self):
        """
        target: test REST v2 functionChains reject internal system input columns
        method: read $seg_offset from a map expression
        expected: request fails because L2 rerank chains only expose selected system inputs
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_seg_offset_input",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$seg_offset"},
                                    {"literal": 1},
                                ],
                                "params": {"mode": "sum"},
                            },
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "$seg_offset" in rsp["message"]

    def test_search_rejects_function_score_with_function_chains(self):
        """
        target: test REST v2 search rejects ambiguous rerank APIs
        method: send functionScore and functionChains together
        expected: request fails with mutual-exclusive error
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionScore": {
                "functions": [
                    {
                        "name": "decay_ts",
                        "type": "Rerank",
                        "inputFieldNames": ["ts"],
                        "params": {
                            "reranker": "decay",
                            "function": "linear",
                            "origin": 30,
                            "scale": 10,
                            "offset": 0,
                            "decay": 0.5,
                        },
                    }
                ]
            },
            "functionChains": self._score_plus_ts_function_chain(),
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "function_score and function_chains cannot be used together" in rsp["message"]

    def test_search_rejects_function_chains_bad_stage(self):
        """
        target: test REST v2 functionChains stage validation
        method: send an ingestion-stage chain in a search request
        expected: request fails because search only supports L2 rerank chains
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_stage",
                    "stage": "FunctionChainStageIngestion",
                    "ops": [
                        {
                            "op": "limit",
                            "params": {"limit": 2},
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "stage FunctionChainStageIngestion is not supported in search request" in rsp["message"]

    def test_search_rejects_function_chains_bad_expr_arg(self):
        """
        target: test REST v2 functionChains expression argument validation
        method: send an expression arg with both column and literal
        expected: request fails with exactly-one-of validation error
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "functionChains": [
                {
                    "name": "bad_arg",
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$score"],
                            "expr": {
                                "name": "num_combine",
                                "args": [
                                    {"column": "$score"},
                                    {"column": "ts", "literal": 1},
                                ],
                            },
                        }
                    ],
                }
            ],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp["code"] != 0
        assert "exactly one of column or literal is required" in rsp["message"]

    def test_hybrid_search_rejects_function_chains(self):
        """
        target: test REST v2 hybrid search rejects functionChains in first version
        method: send top-level functionChains to hybrid_search
        expected: request fails with unsupported error
        """
        name = self._create_function_chain_collection()

        payload = {
            "collectionName": name,
            "search": [
                {
                    "data": [[0.0, 0.0]],
                    "annsField": "vector",
                    "limit": 3,
                }
            ],
            "rerank": {"strategy": "rrf", "params": {}},
            "limit": 3,
            "functionChains": self._score_plus_ts_function_chain(),
        }
        rsp = self.vector_client.vector_hybrid_search(payload)
        assert rsp["code"] != 0
        assert "functionChains is not supported for hybrid search yet" in rsp["message"]
