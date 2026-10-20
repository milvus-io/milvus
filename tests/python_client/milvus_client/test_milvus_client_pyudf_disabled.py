"""Run only against the dedicated PyUDF-disabled deployment profile."""

from contextlib import ExitStack

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr


class TestMilvusClientPyUDFDisabled(TestMilvusClientV2Base):
    skip_global_role_cleanup = True

    @pytest.fixture(autouse=True)
    def require_disabled_profile(self, request):
        if not request.config.getoption("--run-pyudf-disabled"):
            pytest.skip("requires --run-pyudf-disabled and a dedicated PyUDF-disabled deployment")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("nq", [1, 2])
    def test_pyudf_disabled_rejects_nonempty_search_without_affecting_search(self, nq):
        """
        preconditions: --run-pyudf-disabled and a dedicated deployment with
        function.pyUDF.enabled=false; the regular CI profile enables PyUDF.
        target: check the disabled configuration before resource resolution
        method: compare nonempty ordinary Search with PyUDF Search on the same data
        expected: exact code/message for disabled PyUDF, followed by intact ordinary results
        """
        with ExitStack() as cleanup:
            client = self._client()
            cleanup.callback(client.close)
            name = cf.gen_unique_str("pyudf_disabled")
            schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
            indexes = self.prepare_index_params(client)[0]
            indexes.add_index("vector", index_type="FLAT", metric_type="L2")
            self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
            cleanup.callback(self.drop_collection, client, name)
            index = self.describe_index(client, name, "vector")[0]
            assert index["index_type"] == "FLAT", index
            assert index["metric_type"] == "L2", index
            self.insert(client, name, [{"id": 1, "vector": [0.0, 0.0]}, {"id": 2, "vector": [0.1, 0.0]}])
            self.flush(client, name)
            self.load_collection(client, name)
            queries = [[0.0, 0.0]] * nq
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score",
                FunctionChainExpr("py_udf", args=(col("$score"),), params={"resource_name": "unused_disabled_probe"}),
            )
            for with_udf in (False, True, False):
                if with_udf:
                    error = self.search(
                        client,
                        name,
                        data=queries,
                        anns_field="vector",
                        limit=2,
                        search_params={"metric_type": "L2"},
                        function_chains=chain,
                        check_task=CheckTasks.err_res,
                        check_items={"err_code": 1100, "err_msg": "function.pyUDF.enabled is false"},
                    )[0]
                    assert error.code == 1100, error
                else:
                    result = self.search(
                        client, name, data=queries, anns_field="vector", limit=2, search_params={"metric_type": "L2"}
                    )[0]
                    assert len(result) == nq
                    for hits in result:
                        assert [hit["id"] for hit in hits] == [1, 2]
                        assert [hit["distance"] for hit in hits] == pytest.approx([0.0, 0.01])
