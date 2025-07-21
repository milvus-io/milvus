import logging
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base
import pytest
from idx_diskann import DISKANN

index_type = "DISKANN"
success = "success"
pk_field_name = 'id'
vector_field_name = 'vector'
dim = ct.default_dim
default_nb = 2000
default_build_params = {"search_list_size": 100, "beamwidth": 10, "pq_code_budget_gb": 1.0, "num_threads": 8, "max_degree": 64, "indexing_list_size": 100, "build_dram_budget_gb": 2.0, "search_dram_budget_gb": 1.0}
default_search_params = {"search_list_size": 100, "beamwidth": 10, "search_dram_budget_gb": 1.0}


class TestDiskannBuildParams(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("params", DISKANN.build_params)
    def test_diskann_build_params(self, params):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        insert_times = 2
        random_vectors = list(cf.gen_vectors(default_nb * insert_times, dim, vector_data_type=DataType.FLOAT_VECTOR))
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                pk_field_name: i + start_pk,
                vector_field_name: random_vectors[i + start_pk]
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        build_params = params.get("params", None)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type=cf.get_default_metric_for_vector_type(vector_type=DataType.FLOAT_VECTOR),
                               index_type=index_type,
                               params=build_params)
        if params.get("expected", None) != success:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items=params.get("expected"))
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
            self.load_collection(client, collection_name)
            nq = 2
            search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
            self.search(client, collection_name, search_vectors,
                        search_params=default_search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_field_name})
            idx_info = client.describe_index(collection_name, vector_field_name)
            if build_params is not None:
                for key, value in build_params.items():
                    if value is not None:
                        assert key in idx_info.keys()
                        assert str(value) == idx_info[key]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector_data_type", ct.all_vector_types)
    def test_diskann_on_all_vector_types(self, vector_data_type):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        if vector_data_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field(vector_field_name, datatype=vector_data_type)
        else:
            schema.add_field(vector_field_name, datatype=vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        insert_times = 2
        random_vectors = list(cf.gen_vectors(default_nb*insert_times, default_dim, vector_data_type=vector_data_type)) \
            if vector_data_type == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb*insert_times, default_dim, vector_data_type=vector_data_type)
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                pk_field_name: i + start_pk,
                vector_field_name: random_vectors[i + start_pk]
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        metric_type = cf.get_default_metric_for_vector_type(vector_data_type)
        index_params.add_index(field_name=vector_field_name,
                               metric_type=metric_type,
                               index_type=index_type,
                               **default_build_params)
        if vector_data_type not in DISKANN.supported_vector_types:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items={"err_code": 999,
                                           "err_msg": f"can't build with this index DISKANN: invalid parameter"})
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
            self.load_collection(client, collection_name)
            nq = 2
            search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=vector_data_type)
            self.search(client, collection_name, search_vectors,
                        search_params=default_search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", DISKANN.supported_metrics)
    def test_diskann_on_all_metrics(self, metric):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        insert_times = 2
        random_vectors = list(cf.gen_vectors(default_nb*insert_times, default_dim, vector_data_type=DataType.FLOAT_VECTOR))
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                pk_field_name: i + start_pk,
                vector_field_name: random_vectors[i + start_pk]
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type=metric,
                               index_type=index_type,
                               **default_build_params)
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(client, collection_name, search_vectors,
                    search_params=default_search_params,
                    limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_field_name})


@pytest.mark.xdist_group("TestDiskannSearchParams")
class TestDiskannSearchParams(TestMilvusClientV2Base):
    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestDiskannSearchParams" + cf.gen_unique_str("_")
        self.float_vector_field_name = vector_field_name
        self.float_vector_dim = dim
        self.primary_keys = []
        self.enable_dynamic_field = False
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client()
        collection_schema = self.create_schema(client)[0]
        collection_schema.add_field(pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        collection_schema.add_field(self.float_vector_field_name, DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, self.collection_name, schema=collection_schema,
                               enable_dynamic_field=self.enable_dynamic_field, force_teardown=False)
        insert_times = 2
        float_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim,
                                       vector_data_type=DataType.FLOAT_VECTOR)
        for j in range(insert_times):
            rows = []
            for i in range(default_nb):
                pk = i + j * default_nb
                row = {
                    pk_field_name: pk,
                    self.float_vector_field_name: list(float_vectors[pk])
                }
                self.datas.append(row)
                rows.append(row)
            self.insert(client, self.collection_name, data=rows)
            self.primary_keys.extend([i + j * default_nb for i in range(default_nb)])
        self.flush(client, self.collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type="COSINE",
                               index_type=index_type,
                               params=default_build_params)
        self.create_index(client, self.collection_name, index_params=index_params)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.load_collection(client, self.collection_name)
        def teardown():
            self.drop_collection(self._client(), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", DISKANN.search_params)
    def test_diskann_search_params(self, params):
        client = self._client()
        collection_name = self.collection_name
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        search_params = params.get("params", None)
        if params.get("expected", None) != success:
            self.search(client, collection_name, search_vectors,
                        search_params=search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.err_res,
                        check_items=params.get("expected"))
        else:
            self.search(client, collection_name, search_vectors,
                        search_params=search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_field_name}) 