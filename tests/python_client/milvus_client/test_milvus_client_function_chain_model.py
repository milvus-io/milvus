"""Real Milvus-to-provider HTTP contracts for FunctionChain rerank_model."""

import json
import socket
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.exceptions import ParamError
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr
from utils.util_log import test_log as log


@pytest.fixture
def chain_rerank_provider():
    """Expose a per-case TEI protocol server reachable from Milvus in the E2E vCluster."""
    requests = []
    condition = threading.Condition()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with condition:
                requests.append({"path": self.path, "peer": self.client_address[0], "body": body})
                condition.notify_all()
            query = body["query"]
            if query == "timeout":
                time.sleep(0.5)
            if query == "http400":
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"owned provider rejected request")
                return
            values = {"amber": 0.125, "blue": 0.5, "coral": 0.875, "": 0.0}
            results = [
                {"index": i, "score": values[text] if query != "reverse" else 1.0 - values[text]}
                for i, text in enumerate(body["texts"])
            ]
            if query == "missing":
                results = results[:-1]
            elif query == "duplicate":
                results[-1]["index"] = results[0]["index"]
            elif query == "out_of_range":
                results[-1]["index"] = len(results)
            payload = b"{invalid json" if query == "malformed" else json.dumps(results[::-1]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                # A timed-out Milvus request closes its own provider connection.
                pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = False
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    # The runner Pod IP must be routable from the tested Milvus deployment.
    address = socket.gethostbyname(socket.gethostname())
    try:
        yield f"http://{address}:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
        log.info("FUNCTIONCHAIN_PROVIDER_REQUESTS %s", json.dumps(requests))


class TestMilvusClientFunctionChainModelIndependent(TestMilvusClientV2Base):
    skip_global_role_cleanup = True

    def _collection(self, client):
        name = cf.gen_unique_str("fc_model")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("document", DataType.VARCHAR, max_length=64, nullable=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
        index = self.prepare_index_params(client)[0]
        index.add_index(field_name="vector", index_type="FLAT", metric_type="IP")
        self.create_collection(client, name, schema=schema, index_params=index, consistency_level="Strong")
        rows = [
            {"id": i + 1, "document": text, "vector": [float(i + 1), 1.0]}
            for i, text in enumerate(["amber", "blue", "coral", None])
        ]
        self.insert(client, name, rows)
        self.flush(client, name)
        self.load_collection(client, name)
        return name

    @staticmethod
    def _chain(endpoint, queries, stage=FunctionChainStage.L2_RERANK, **params):
        expr = fn.rerank_model(
            col("document"), queries=queries, provider="tei", endpoint=endpoint, timeout_ms=5000, **params
        )
        chain = FunctionChain(stage).map("$score", expr)
        if stage == FunctionChainStage.L2_RERANK:
            chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
        return chain

    def _search(self, client, name, chain, nq=2, **kwargs):
        return self.search(
            client,
            name,
            data=[[1.0, 0.0], [-1.0, 0.0]][:nq],
            anns_field="vector",
            search_params={"metric_type": "IP"},
            limit=4,
            output_fields=["document"],
            function_chains=chain,
            **kwargs,
        )[0]

    @staticmethod
    def _assert_result(result, queries, rounded=False):
        documents = {1: "amber", 2: "blue", 3: "coral", 4: None}
        expected_forward = {1: 0.125, 2: 0.5, 3: 0.875, 4: 0.0}
        expected_reverse = {1: 0.875, 2: 0.5, 3: 0.125, 4: 1.0}
        if rounded:
            expected_forward = {1: 0.25, 2: 1.0, 3: 1.75, 4: 0.0}
            expected_reverse = {1: 1.75, 2: 1.0, 3: 0.25, 4: 2.0}
        assert len(result) == len(queries)
        for query, hits in zip(queries, result):
            expected = expected_reverse if query == "reverse" else expected_forward
            order = sorted(expected, key=lambda pk: (-expected[pk], pk))
            assert [h["id"] for h in hits] == order
            assert [h["distance"] for h in hits] == pytest.approx([expected[pk] for pk in order], abs=1e-6)
            for hit in hits:
                assert hit["entity"] == {"document": documents[hit["id"]]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("batch", [1, 2, 3, 32])
    def test_function_chain_rerank_model_maps_reordered_provider_scores(self, chain_rerank_provider, nq, batch):
        """target: real provider mapping; method: reversed response indices and batched nq; expected: exact scores/fields."""
        endpoint, requests = chain_rerank_provider
        client = self._client()
        name = self._collection(client)
        queries = ["forward", "reverse"][:nq]
        chain = self._chain(endpoint, queries, max_client_batch_size=batch)
        self._assert_result(self._search(client, name, chain, nq=nq), queries)
        assert len(requests) == nq * ((4 + batch - 1) // batch)
        assert all(r["path"] == "/rerank" and len(r["body"]["texts"]) <= batch for r in requests)
        for query in queries:
            batches = [r["body"]["texts"] for r in requests if r["body"]["query"] == query]
            assert [len(texts) for texts in batches] == [min(batch, 4 - offset) for offset in range(0, 4, batch)]
            sent = [text for texts in batches for text in texts]
            assert Counter(sent) == Counter(["amber", "blue", "coral", ""])

    @pytest.mark.tags(CaseLabel.L1)
    def test_function_chain_rerank_model_composes_with_builtins(self, chain_rerank_provider):
        """target: temporary model output; method: model plus weighted/round/sort; expected: independent scaled scores."""
        endpoint, requests = chain_rerank_provider
        client = self._client()
        name = self._collection(client)
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
            "model_score",
            fn.rerank_model(col("document"), queries=["forward", "reverse"], provider="tei", endpoint=endpoint),
        )
        chain.map("combined", fn.num_combine(col("model_score"), col("model_score"), mode="sum"))
        chain.map("$score", fn.round_decimal(col("combined"), decimal=2)).sort(col("$score"), tie_break_col=col("$id"))
        self._assert_result(self._search(client, name, chain), ["forward", "reverse"], rounded=True)
        assert len(requests) == 2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "fault,code,message",
        [
            ("missing", 2400, "number of docs and scores does not match"),
            ("duplicate", 2400, "invalid or duplicated result index"),
            ("out_of_range", 2400, "invalid or duplicated result index"),
            ("http400", 2400, "400 Bad Request"),
            ("malformed", 65535, "unmarshal response failed"),
            ("timeout", 10001, "context deadline exceeded"),
        ],
    )
    def test_function_chain_rerank_model_provider_failure_and_recovery(
        self, chain_rerank_provider, fault, code, message
    ):
        """target: provider failures; method: dedicated HTTP error/count/index/timeout; expected: explicit error and recovery."""
        endpoint, requests = chain_rerank_provider
        client = self._client()
        name = self._collection(client)
        expr = fn.rerank_model(
            col("document"),
            queries=[fault, fault],
            provider="tei",
            endpoint=endpoint,
            timeout_ms=100 if fault == "timeout" else 5000,
        )
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map("$score", expr)
        error = self._search(
            client, name, chain, check_task=CheckTasks.err_res, check_items={"err_code": code, "err_msg": message}
        )
        assert error.code == code
        assert requests and all(r["body"]["query"] == fault for r in requests)
        before = len(requests)
        self._assert_result(
            self._search(client, name, self._chain(endpoint, ["forward", "reverse"])), ["forward", "reverse"]
        )
        assert len(requests) == before + 2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("stage", [FunctionChainStage.L0_RERANK, FunctionChainStage.L1_RERANK], ids=["l0", "l1"])
    def test_function_chain_rerank_model_rejects_querynode_stage(self, chain_rerank_provider, stage):
        """target: model stage limit; method: valid provider at L0/L1; expected: no HTTP request, then L2 recovery."""
        endpoint, requests = chain_rerank_provider
        client = self._client()
        name = self._collection(client)
        error = self._search(
            client,
            name,
            self._chain(endpoint, ["forward", "reverse"], stage=stage),
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": 'function "rerank_model" does not support stage'},
        )
        assert error.code == 1100 and requests == []
        self._assert_result(
            self._search(client, name, self._chain(endpoint, ["forward", "reverse"])), ["forward", "reverse"]
        )
        assert len(requests) == 2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "override,message",
        [
            ({"queries": ["forward"]}, "queries count (1) != nq count (2)"),
            ({"queries": ["forward", "reverse", "forward"]}, "queries count (3) != nq count (2)"),
            ({"queries": []}, "queries must not be empty"),
            ({"queries": [1]}, "queries"),
            ({"provider": "absent"}, "unknown rerank model provider"),
            ({"endpoint": "invalid"}, "not a valid http/https link"),
            ({"max_client_batch_size": 0}, "must be greater than 0"),
            ({"truncate": "wrong"}, "is not bool type"),
        ],
        ids=[
            "short_queries",
            "long_queries",
            "empty_queries",
            "query_type",
            "provider",
            "endpoint",
            "batch",
            "truncate",
        ],
    )
    def test_function_chain_rerank_model_rejects_invalid_server_params(self, chain_rerank_provider, override, message):
        """target: server model parameters; method: public raw expression; expected: rejection before provider and recovery."""
        endpoint, requests = chain_rerank_provider
        client = self._client()
        name = self._collection(client)
        params = {"queries": ["forward", "reverse"], "provider": "tei", "endpoint": endpoint, **override}
        chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
            "$score", FunctionChainExpr("rerank_model", args=(col("document"),), params=params)
        )
        error = self._search(
            client, name, chain, check_task=CheckTasks.err_res, check_items={"err_code": 1100, "err_msg": message}
        )
        assert error.code == 1100 and requests == []
        self._assert_result(
            self._search(client, name, self._chain(endpoint, ["forward", "reverse"])), ["forward", "reverse"]
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("queries", [[], [""], [1], "forward"], ids=["empty", "blank", "number", "string"])
    def test_function_chain_rerank_model_helper_rejects_invalid_queries(self, queries):
        """target: SDK queries; method: invalid helper input; expected: local ParamError."""
        with pytest.raises(ParamError):
            fn.rerank_model(col("document"), queries=queries, provider="tei")
