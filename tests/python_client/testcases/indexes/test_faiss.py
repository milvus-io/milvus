import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from idx_faiss import FAISS
from pymilvus import DataType

index_type = "FAISS"
success = "success"
pk_field_name = "id"
vector_field_name = "vector"
dim = ct.default_dim
default_nb = ct.default_nb
default_search_params = {"nprobe": 8}


def _default_search_params_for_faiss_factory(faiss_index_name):
    if faiss_index_name.startswith("IVF"):
        return {"nprobe": 8}
    if faiss_index_name.startswith("HNSW"):
        return {"efSearch": 64}
    return {}


class TestFaissBase(TestMilvusClientV2Base):
    def _create_collection(self, client, collection_name, vector_data_type=DataType.FLOAT_VECTOR):
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        if vector_data_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field(vector_field_name, datatype=vector_data_type)
        else:
            schema.add_field(vector_field_name, datatype=vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        return schema

    def _insert_rows(self, client, collection_name, vector_data_type=DataType.FLOAT_VECTOR):
        vectors = cf.gen_vectors(default_nb, dim=dim, vector_data_type=vector_data_type)
        rows = [{pk_field_name: i, vector_field_name: vectors[i]} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

    def _create_faiss_index(
        self, client, collection_name, metric_type="L2", params=None, check_task=None, check_items=None
    ):
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name, metric_type=metric_type, index_type=index_type, params=params
        )
        return self.create_index(client, collection_name, index_params, check_task=check_task, check_items=check_items)

    def _search_and_check(self, client, collection_name, vector_data_type=DataType.FLOAT_VECTOR, search_params=None):
        nq = ct.default_nq
        search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=vector_data_type)
        self.search(
            client,
            collection_name,
            search_vectors,
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": nq,
                "limit": ct.default_limit,
                "pk_name": pk_field_name,
            },
        )

    def _assert_index_params(self, client, collection_name, params, metric_type):
        idx_info = client.describe_index(collection_name, vector_field_name)
        assert idx_info["index_type"] == index_type
        assert idx_info["metric_type"] == metric_type
        for key, value in params.items():
            assert key in idx_info.keys()
            assert str(value) in [str(v) for v in idx_info.values()]


class TestFaissBuildParams(TestFaissBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", FAISS.build_params)
    def test_faiss_build_params(self, params):
        """
        Test vanilla Faiss factory build parameters.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vector_data_type = params.get("vector_data_type", DataType.FLOAT_VECTOR)
        metric_type = params.get("metric_type", "L2")
        build_params = params.get("params", None)

        self._create_collection(client, collection_name, vector_data_type)
        self._insert_rows(client, collection_name, vector_data_type)

        if params.get("expected", None) != success:
            self._create_faiss_index(
                client,
                collection_name,
                metric_type=metric_type,
                params=build_params,
                check_task=CheckTasks.err_res,
                check_items=params.get("expected"),
            )
        else:
            self._create_faiss_index(client, collection_name, metric_type=metric_type, params=build_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
            self.load_collection(client, collection_name)
            if vector_data_type == DataType.FLOAT_VECTOR and params.get("searchable", True):
                search_params = _default_search_params_for_faiss_factory(build_params["faiss_index_name"])
                self._search_and_check(client, collection_name, vector_data_type, search_params=search_params)
            elif vector_data_type != DataType.FLOAT_VECTOR:
                search_params = {}
                self._search_and_check(client, collection_name, vector_data_type, search_params=search_params)
            self._assert_index_params(client, collection_name, build_params, metric_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", FAISS.supported_metrics)
    @pytest.mark.parametrize("build_params", [{"faiss_index_name": "Flat"}] + FAISS.metric_factories)
    def test_faiss_on_all_float_metrics(self, metric, build_params):
        """
        Test vanilla Faiss float index factories on all supported float metrics.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type=metric, params=build_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)
        search_params = _default_search_params_for_faiss_factory(build_params["faiss_index_name"])
        self._search_and_check(client, collection_name, DataType.FLOAT_VECTOR, search_params=search_params)
        self._assert_index_params(client, collection_name, build_params, metric)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector_data_type", ct.all_vector_types)
    def test_faiss_on_all_vector_types(self, vector_data_type):
        """
        Test vanilla Faiss vector type support.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, vector_data_type)
        self._insert_rows(client, collection_name, vector_data_type)

        if vector_data_type == DataType.BINARY_VECTOR:
            metric_type = "HAMMING"
            build_params = {"faiss_index_name": "BFlat"}
        else:
            metric_type = cf.get_default_metric_for_vector_type(vector_data_type)
            build_params = {"faiss_index_name": "Flat"}

        if vector_data_type not in FAISS.supported_vector_types:
            self._create_faiss_index(
                client,
                collection_name,
                metric_type=metric_type,
                params=build_params,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 999, "err_msg": "invalid parameter"},
            )
        else:
            self._create_faiss_index(client, collection_name, metric_type=metric_type, params=build_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
            self.load_collection(client, collection_name)
            self._search_and_check(client, collection_name, vector_data_type, search_params={})
            self._assert_index_params(client, collection_name, build_params, metric_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "params", [p for p in FAISS.build_params if p.get("expected") == success and p.get("searchable", True)]
    )
    def test_faiss_build_release_load_search(self, params):
        """
        Test vanilla Faiss index survives the full Milvus build -> release -> load -> search flow.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        vector_data_type = params.get("vector_data_type", DataType.FLOAT_VECTOR)
        metric_type = params.get("metric_type", "L2")
        build_params = params["params"]

        self._create_collection(client, collection_name, vector_data_type)
        self._insert_rows(client, collection_name, vector_data_type)
        self._create_faiss_index(client, collection_name, metric_type=metric_type, params=build_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        search_params = {}
        if vector_data_type == DataType.FLOAT_VECTOR:
            search_params = _default_search_params_for_faiss_factory(build_params["faiss_index_name"])
        self._search_and_check(client, collection_name, vector_data_type, search_params=search_params)
        self._assert_index_params(client, collection_name, build_params, metric_type)


class TestFaissSearchParams(TestFaissBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", FAISS.search_params)
    def test_faiss_search_params(self, params):
        """
        Test vanilla Faiss search parameter forwarding.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        build_params = params["build_params"]

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params=build_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)
        if params.get("expected", None) != success:
            nq = ct.default_nq
            search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
            self.search(
                client,
                collection_name,
                search_vectors,
                search_params=params["search_params"],
                limit=ct.default_limit,
                check_task=CheckTasks.err_res,
                check_items=params.get("expected"),
            )
        else:
            self._search_and_check(
                client, collection_name, DataType.FLOAT_VECTOR, search_params=params["search_params"]
            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_faiss_incompatible_search_params(self):
        """
        Test vanilla Faiss rejects search parameters incompatible with the factory index.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        build_params = {"faiss_index_name": "Flat"}

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params=build_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        nq = ct.default_nq
        search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(
            client,
            collection_name,
            search_vectors,
            search_params={"efSearch": 64},
            limit=ct.default_limit,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 999, "err_msg": "not supported"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "build_params",
        [
            {"faiss_index_name": "Flat"},
            {"faiss_index_name": "IVF64,Flat"},
            {"faiss_index_name": "HNSW16,Flat"},
        ],
    )
    def test_faiss_search_with_scalar_filter(self, build_params):
        """
        Test vanilla Faiss search honors Milvus scalar filter bitset.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params=build_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        search_params = _default_search_params_for_faiss_factory(build_params["faiss_index_name"])
        search_vectors = cf.gen_vectors(1, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        results = client.search(
            collection_name,
            search_vectors,
            filter=f"{pk_field_name} >= 100",
            search_params=search_params,
            limit=ct.default_limit,
        )
        assert len(results) == 1
        assert len(results[0]) == ct.default_limit
        assert all(hit["id"] >= 100 for hit in results[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_faiss_flat_range_search(self):
        """
        Test vanilla Faiss float Flat range search. The current adapter implements
        RangeSearch for float FAISS indexes.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params={"faiss_index_name": "Flat"})
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(1, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        range_params = {"radius": 100000.0, "range_filter": 0.0}
        self.search(
            client,
            collection_name,
            search_vectors,
            search_params=range_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": 1,
                "limit": ct.default_limit,
                "pk_name": pk_field_name,
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_faiss_binary_range_search_not_supported(self):
        """
        Test vanilla Faiss binary range search is explicitly not implemented.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.BINARY_VECTOR)
        self._insert_rows(client, collection_name, DataType.BINARY_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="HAMMING", params={"faiss_index_name": "BFlat"})
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(1, dim=dim, vector_data_type=DataType.BINARY_VECTOR)
        self.search(
            client,
            collection_name,
            search_vectors,
            search_params={"radius": 1000, "range_filter": 0},
            limit=ct.default_limit,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 999, "err_msg": "RangeSearch unsupported for binary faiss indexes"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_faiss_pq_search_selector_not_supported(self):
        """
        Test vanilla Faiss IndexPQ rejects Milvus search because Milvus passes
        an ID selector/bitset and native FAISS IndexPQ does not support it.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params={"faiss_index_name": "PQ8x4"})
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(1, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(
            client,
            collection_name,
            search_vectors,
            filter=f"{pk_field_name} >= 100",
            search_params={},
            limit=ct.default_limit,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 999, "err_msg": "selector not supported"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_faiss_search_iterator_not_supported(self):
        """
        Test vanilla Faiss search iterator is rejected because the adapter does
        not expose raw-vector retrieval.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self._create_collection(client, collection_name, DataType.FLOAT_VECTOR)
        self._insert_rows(client, collection_name, DataType.FLOAT_VECTOR)
        self._create_faiss_index(client, collection_name, metric_type="L2", params={"faiss_index_name": "Flat"})
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(1, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search_iterator(
            client,
            collection_name,
            data=search_vectors,
            batch_size=100,
            search_params={},
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "Failed to create iterators from index"},
        )
