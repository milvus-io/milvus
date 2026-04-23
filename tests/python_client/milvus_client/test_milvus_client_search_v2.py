from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
import math
import random
import threading
import pytest
import numpy as np
from pymilvus import DataType

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = f"{ct.default_int64_field_name} >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name


@pytest.mark.xdist_group("TestMilvusClientSearchBasicV2")
@pytest.mark.tags(CaseLabel.L1)
class TestMilvusClientSearchBasicV2(TestMilvusClientV2Base):
    """Test search functionality with new client API"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestMilvusClientSearchV2" + cf.gen_unique_str("_")
        self.partition_names = ["partition_1", "partition_2"]
        self.pk_field_name = ct.default_primary_field_name
        self.float_vector_field_name = ct.default_float_vec_field_name
        self.bfloat16_vector_field_name = "bfloat16_vector"
        self.sparse_vector_field_name = "sparse_vector"
        self.binary_vector_field_name = "binary_vector"
        self.float_vector_dim = 36
        self.bf16_vector_dim = 35
        self.binary_vector_dim = 32
        self.float_vector_metric = "COSINE"
        self.bf16_vector_metric = "L2"
        self.sparse_vector_metric = "IP"
        self.binary_vector_metric = "JACCARD"
        self.float_vector_index = "IVF_FLAT"
        self.bf16_vector_index = "DISKANN"
        self.sparse_vector_index = "SPARSE_INVERTED_INDEX"
        self.binary_vector_index = "BIN_IVF_FLAT"
        self.primary_keys = []
        self.enable_dynamic_field = True
        self.dyna_field_name1 = "dyna_field_name1"
        self.dyna_field_name2 = "dyna_field_name2"
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()

        # Create collection
        collection_schema = self.create_schema(client, enable_dynamic_field=self.enable_dynamic_field)[0]
        collection_schema.add_field(self.pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        collection_schema.add_field(self.float_vector_field_name, DataType.FLOAT_VECTOR, dim=self.float_vector_dim)
        collection_schema.add_field(self.bfloat16_vector_field_name, DataType.BFLOAT16_VECTOR, dim=self.bf16_vector_dim)
        collection_schema.add_field(self.sparse_vector_field_name, DataType.SPARSE_FLOAT_VECTOR)
        collection_schema.add_field(self.binary_vector_field_name, DataType.BINARY_VECTOR, dim=self.binary_vector_dim)
        collection_schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        collection_schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=256, nullable=True)
        self.create_collection(client, self.collection_name, schema=collection_schema, force_teardown=False)
        for partition_name in self.partition_names:
            self.create_partition(client, self.collection_name, partition_name=partition_name)

        # Define number of insert iterations
        insert_times = 10

        # Generate vectors for each type and store in self
        float_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim,
                                       vector_data_type=DataType.FLOAT_VECTOR)
        bfloat16_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.bf16_vector_dim,
                                          vector_data_type=DataType.BFLOAT16_VECTOR)
        sparse_vectors = cf.gen_sparse_vectors(default_nb * insert_times, empty_percentage=2)
        _, binary_vectors = cf.gen_binary_vectors(default_nb * insert_times, dim=self.binary_vector_dim)

        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            # Group rows by partition based on primary key mod 3
            default_rows = []
            partition1_rows = []
            partition2_rows = []

            for i in range(default_nb):
                pk = i + j * default_nb
                row = {
                    self.pk_field_name: pk,
                    self.float_vector_field_name: list(float_vectors[pk]),
                    self.bfloat16_vector_field_name: bfloat16_vectors[pk],
                    self.sparse_vector_field_name: sparse_vectors[pk],
                    self.binary_vector_field_name: binary_vectors[pk],
                    ct.default_float_field_name: pk * 1.0 if pk % 5 == 0 else None,
                    ct.default_string_field_name: str(pk) if pk % 5 == 0 else None,
                    self.dyna_field_name1: f"dyna_value_{pk}",
                    self.dyna_field_name2: pk * 1.0
                }
                self.datas.append(row)

                # Distribute to partitions based on pk mod 3
                if pk % 3 == 0:
                    default_rows.append(row)
                elif pk % 3 == 1:
                    partition1_rows.append(row)
                else:
                    partition2_rows.append(row)

            # Insert into respective partitions
            if default_rows:
                self.insert(client, self.collection_name, data=default_rows)
            if partition1_rows:
                self.insert(client, self.collection_name, data=partition1_rows, partition_name=self.partition_names[0])
            if partition2_rows:
                self.insert(client, self.collection_name, data=partition2_rows, partition_name=self.partition_names[1])

            # Track all inserted data and primary keys
            self.primary_keys.extend([i + j * default_nb for i in range(default_nb)])
        self.flush(client, self.collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type=self.float_vector_metric,
                               index_type=self.float_vector_index,
                               params={"nlist": 128})
        index_params.add_index(field_name=self.bfloat16_vector_field_name,
                               metric_type=self.bf16_vector_metric,
                               index_type=self.bf16_vector_index,
                               params={})
        index_params.add_index(field_name=self.sparse_vector_field_name,
                               metric_type=self.sparse_vector_metric,
                               index_type=self.sparse_vector_index,
                               params={})
        index_params.add_index(field_name=self.binary_vector_field_name,
                               metric_type=self.binary_vector_metric,
                               index_type=self.binary_vector_index,
                               params={"nlist": 128})
        self.create_index(client, self.collection_name, index_params=index_params)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.bfloat16_vector_field_name)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("search_params", [{"metric_type": "COSINE", "params": {"nprobe": 100}},
                                               {"metric_type": "COSINE", "nprobe": 100},
                                               {"metric_type": "COSINE"},
                                               {"params": {"nprobe": 100}},
                                               {"nprobe": 100},
                                               {}])
    def test_search_float_vectors(self, search_params):
        """
        target: test search float vectors
        method: 1. connect and create a collection
                2. search float vectors
                3. verify search results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with inserted vectors
        vectors_to_search = [self.datas[i][self.float_vector_field_name] for i in range(default_nq)]

        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "pk_name": self.pk_field_name,
                         "metric": self.float_vector_metric
                         }
        )
        # verify the top 1 hit is itself, so the min distance is 0
        for i in range(default_nq):
            assert 1.0 - search_res[i].distances[0] <= epsilon

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_bfloat16_vectors(self):
        """
        target: test search bfloat16 vectors
        method: 1. connect and create a collection
                2. search bfloat16 vectors
                3. verify search results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with bfloat16 vectors
        vectors_to_search = cf.gen_vectors(default_nq, self.bf16_vector_dim,
                                           vector_data_type=DataType.BFLOAT16_VECTOR)
        search_params = {"metric_type": self.bf16_vector_metric, "params": {}}

        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.bfloat16_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "pk_name": self.pk_field_name,
                         "metric": self.bf16_vector_metric
                         }
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_sparse_vectors(self):
        """
        target: test search sparse vectors
        method: 1. connect and create a collection
                2. search sparse vectors
                3. verify search results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with sparse vectors
        vectors_to_search = cf.gen_sparse_vectors(default_nq, empty_percentage=2)
        search_params = {"metric_type": self.sparse_vector_metric, "params": {}}

        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.sparse_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "pk_name": self.pk_field_name,
                         "metric": self.sparse_vector_metric
                         }
        )

        #  search again without specify anns_field
        error = {"err_code": 999, "err_msg": "multiple anns_fields exist, please specify a anns_field in search_params"}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_empty_vectors(self):
        """
        target: test search with empty vectors
        method: 1. connect and create a collection
                2. search with empty vectors
        expected: search successfully with 0 results
        """
        client = self._client()
        collection_name = self.collection_name

        # search with empty vectors
        error = {"err_code": 999, "err_msg": "Unexpected error, message=<list index out of range>"}
        search_res, _ = self.search(
            client,
            collection_name,
            data=[],
            anns_field=self.sparse_vector_field_name,
            search_params={},
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_binary_vectors(self):
        """
        target: test search binary vectors
        method: 1. connect and create a collection
                2. search binary vectors
                3. verify search results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with binary vectors
        _, vectors_to_search = cf.gen_binary_vectors(default_nq, dim=self.binary_vector_dim)
        search_params = {"metric_type": self.binary_vector_metric, "params": {"nprobe": 100}}

        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.binary_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "pk_name": self.pk_field_name,
                         "metric": self.binary_vector_metric
                         }
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit, nq", zip([1, 1000, ct.max_limit], [ct.max_nq, 10, 1]))
    def test_search_with_different_nq_limits(self, limit, nq):
        """
        target: test search with different nq and limit values
        method: 1. connect and create a collection
                2. search with different nq and limit values
                3. verify search results
        expected: search successfully with different nq and limit values
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 128}}

        # search with limit
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": nq,
                         "limit": limit,
                         "pk_name": self.pk_field_name,
                         "metric": self.float_vector_metric
                         }
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("consistency_level", ["Strong", "Session", "Bounded", "Eventually"])
    def test_search_with_output_fields_and_consistency_level(self, consistency_level):
        """
        target: test search with output fields
        method: 1. connect and create a collection
                2. search with output fields
        expected: search successfully with output fields
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            consistency_level=consistency_level,
            output_fields=[ct.default_string_field_name, self.dyna_field_name1, self.dyna_field_name2],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.float_vector_metric,
                         "output_fields": [ct.default_string_field_name, self.dyna_field_name1, self.dyna_field_name2],
                         "original_entities": self.datas,
                         "pk_name": self.pk_field_name
                         }
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_all(self):
        """
        target: test search with output all fields
        method: 1. connect and create a collection
                2. search with output all fields
        expected: search successfully with output all fields
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]
        fields = collection_info.get('fields', None)
        field_names = [field.get('name') for field in fields]

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.float_vector_metric,
                         "output_fields": field_names + [self.dyna_field_name1, self.dyna_field_name2],
                         "original_entities": self.datas,
                         "pk_name": self.pk_field_name
                         }
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", ct.default_primary_field_name],
                                                        ["*", ct.default_float_vec_field_name]])
    def test_search_partition_with_output_fields(self, wildcard_output_fields):
        """
        target: test partition search with output fields
        method: 1. connect to milvus 
                2. partition search on an existing collection with output fields
        expected: search successfully with output fields
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]
        partition_name = self.partition_names[0]

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        expected_outputs = cf.get_wildcard_output_field_names(collection_info, wildcard_output_fields)
        expected_outputs.extend([self.dyna_field_name1, self.dyna_field_name2])
        log.info(f"search with output fields: {wildcard_output_fields}")
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            partition_names=[partition_name],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            output_fields=wildcard_output_fields,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "pk_name": self.pk_field_name,
                         "metric": self.float_vector_metric,
                         "limit": default_limit,
                         "output_fields": expected_outputs})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_invalid_output_fields(self):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {}
        invalid_output_fields = [["%"], [""], ["-"]]
        for field in invalid_output_fields:
            error1 = {ct.err_code: 999, ct.err_msg: f"parse output field name failed: {field[0]}"}
            error2 = {ct.err_code: 999, ct.err_msg: f"`output_fields` value {field} is illegal"}
            error = error2 if field == [""] else error1
            self.search(client, collection_name, vectors_to_search,
                        anns_field=self.float_vector_field_name,
                        search_params=search_params,
                        limit=default_limit,
                        output_fields=field,
                        check_task=CheckTasks.err_res, check_items=error)

        # verify non-exist field as output field is valid as dynamic field enabled
        self.search(client, collection_name, vectors_to_search,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    output_fields=["non_exist_field"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_limit,
                                 "metric": self.float_vector_metric,
                                 "pk_name": self.pk_field_name})    

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_more_than_max_limit(self):
        """
        target: test search with more than max limit
        method: 1. connect and create a collection
                2. search with more than max limit
        expected: search successfully with more than max limit
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        error = {"err_code": 999, "err_msg": f"topk [{ct.max_limit + 1}] is invalid, it should be in range " \
                                             f"[1, {ct.max_limit}], but got {ct.max_limit + 1}"}
        # search with more than max limit
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=ct.max_limit + 1,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_more_than_max_nq(self):
        """
        target: test search with more than max nq
        method: 1. connect and create a collection
                2. search with more than max nq
        expected: search successfully with more than max nq
            """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(ct.max_nq + 1, dim=128, vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
        search_params = {"metric_type": self.sparse_vector_metric}

        error = {"err_code": 999,
                 "err_msg": f"nq [{ct.max_nq + 1}] is invalid, nq (number of search vector per search request) should be in range " \
                            f"[1, {ct.max_nq}], but got {ct.max_nq + 1}"}
        # search with more than max nq
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:ct.max_nq + 1],
            anns_field=self.sparse_vector_field_name,
            search_params=search_params,
            limit=ct.max_nq + 1,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_concurrent_threads(self):
        """
        target: test search with concurrent threads
        method: 1. connect and create a collection
                2. search with concurrent threads
        expected: search successfully with concurrent threads
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with concurrent threads using thread pool
        from concurrent.futures import ThreadPoolExecutor, as_completed
        num_threads = 10
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(num_threads):
                future = executor.submit(
                    self.search,
                    client,
                    collection_name,
                    vectors_to_search,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq,
                                 "limit": default_limit,
                                 "metric": self.float_vector_metric,
                                 "pk_name": self.pk_field_name
                                 }
                )
                futures.append(future)

            # Wait for all searches to complete
            search_results = []
            for future in as_completed(futures):
                search_res, _ = future.result()
                search_results.append(search_res)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_ndarray(self):
        """
        target: test search with ndarray
        method: 1. connect and create a collection
                2. search with ndarray
        expected: search successfully with ndarray
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = np.random.randn(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with ndarray
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.float_vector_metric,
                         "pk_name": self.pk_field_name}
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_client_closed(self):
        """
        target: test search with client closed
        method: 1. connect and create a collection
                2. search with client closed
        expected: search successfully with client closed
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # close client
        client.close()

        # search with client closed
        error = {"err_code": 999, "err_msg": "should create connection first"}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_mismatched_metric_type(self):
        """
        target: test search with mismatched metric type
        method: 1. connect and create a collection
                2. search with mismatched metric type
        expected: search successfully with mismatched metric type
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.sparse_vector_metric, "params": {"nprobe": 100}}

        # search with mismatched metric type
        error = {"err_code": 999, "err_msg": "metric type not match: invalid parameter[expected=COSINE][actual=IP]"}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["*", "non_exist_partition", "par*"])
    def test_search_with_invalid_partition_name(self, partition_name):
        """
        target: test search with invalid partition name
        method: 1. connect and create a collection
                2. search with invalid partition name
        expected: search successfully with invalid partition name
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with invalid partition name
        error = {"err_code": 999, "err_msg": f"partition name {partition_name} not found"}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            partition_names=[partition_name],
            check_task=CheckTasks.err_res,
            check_items=error
        )


class TestSearchV2Independent(TestMilvusClientV2Base):
    """Test search functionality with independent collections"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_on_empty_partition(self):
        """
        target: test search on empty partition
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # create partition
        partition_name = "empty_partition"
        self.create_partition(client, collection_name, partition_name=partition_name)

        # search
        vectors_to_search = cf.gen_vectors(default_nq, ct.default_dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            search_params=search_params,
            limit=default_limit,
            partition_names=[partition_name],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": 0,
                         "pk_name": ct.default_primary_key_field_name,
                         "ids": [],
                         "metric": "COSINE"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_cosine_results_same_as_l2_and_ip(self):
        """
        target: test search cosine results same as l2 and ip
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection with customized schema
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_primary_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=256, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_row_data_by_schema(schema=schema, nb=default_nb)
        self.insert(client, collection_name, data)

        # create index with metric cosine
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name,
                               metric_type='COSINE',
                               index_type='AUTOINDEX')
        self.create_index(client, collection_name, index_params=index_params)

        # load collection
        self.load_collection(client, collection_name)

        # search on the default metric cosine
        vectors_to_search = cf.gen_vectors(default_nq, ct.default_dim)
        search_params = {}
        search_res_cosine, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'COSINE',
                         "pk_name": ct.default_primary_field_name}
        )

        # release and drop index, and rebuild index with metric l2
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, ct.default_float_vec_field_name)

        # rebuild index with metric l2
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name,
                               metric_type='L2',
                               index_type='AUTOINDEX')
        self.create_index(client, collection_name, index_params=index_params)
        # load collection
        self.load_collection(client, collection_name)

        # search on the metric l2
        search_params = {}
        search_res_l2, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'L2',
                         "pk_name": ct.default_primary_field_name}
        )

        # release and drop index, and rebuild index with metric ip
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, ct.default_float_vec_field_name)

        # rebuild index with metric ip
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name,
                               metric_type='IP',
                               index_type='AUTOINDEX')
        self.create_index(client, collection_name, index_params=index_params)

        # load collection   
        self.load_collection(client, collection_name)

        # search on the metric ip
        search_params = {}
        search_res_ip, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'IP',
                         "pk_name": ct.default_primary_field_name}
        )

        # check the 3 metrics cosine, l2 and ip search results ids are the same
        for i in range(default_nq):
            assert search_res_cosine[i].ids == search_res_l2[i].ids == search_res_ip[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_duplicate_primary_key(self):
        """
        target: test search with duplicate primary key
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        dim = 60
        self.create_collection(client, collection_name, dimension=dim)

        # insert data with duplicate primary key
        all_vectors = cf.gen_vectors(default_nb, dim)
        data = [{ct.default_primary_key_field_name: i if i % 2 == 0 else i + 1,
                 ct.default_vector_field_name: all_vectors[i]} for i in range(default_nb)]
        self.insert(client, collection_name, data)
        client.flush(collection_name)

        # search
        vectors_to_search = cf.gen_vectors(default_nq, dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": default_limit}
        )

        # verify the search results are de-duplicated
        for i in range(default_nq):
            assert len(search_res[i].ids) == len(set(search_res[i].ids))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("flush", [True, False])
    def test_search_after_release_load(self, flush):
        """
        target: test search after release and load
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # insert data
        all_vectors = cf.gen_vectors(default_nb, ct.default_dim)
        data = [{ct.default_primary_key_field_name: i,
                 ct.default_vector_field_name: all_vectors[i]} for i in range(default_nb)]
        self.insert(client, collection_name, data)
        if flush:
            self.flush(client, collection_name)
            self.wait_for_index_ready(client, collection_name, index_name=ct.default_vector_field_name)

        # release collection
        self.release_collection(client, collection_name)
        # search after release
        error = {"err_code": 999, "err_msg": "collection not loaded"}
        vectors_to_search = cf.gen_vectors(default_nq, ct.default_dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # load collection
        self.load_collection(client, collection_name)

        # search
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_after_partition_release(self):
        """
        target: test search after partition release     
        method: 1. create connection, collection, insert data and search
                2. release a partition
                3. search again
                4. load the released partition and search again
                5. release the partition again and load the collection
                6. search again
        expected: searched and results are correct
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # create 2 more partitions
        partition_names = ["partition_1", "partition_2"]
        for partition_name in partition_names:
            self.create_partition(client, collection_name, partition_name=partition_name)

        # insert data into all the 3 partitions
        insert_times = 2

        # Generate vectors for each type and store in self
        float_vectors = cf.gen_vectors(ct.default_nb * insert_times, dim=ct.default_dim,
                                       vector_data_type=DataType.FLOAT_VECTOR)

        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            # Group rows by partition based on primary key mod 3
            default_rows = []
            partition1_rows = []
            partition2_rows = []

            for i in range(ct.default_nb):
                pk = i + j * ct.default_nb
                row = {
                    ct.default_primary_key_field_name: pk,
                    ct.default_vector_field_name: float_vectors[pk]
                }

                # Distribute to partitions based on pk mod 3
                if pk % 3 == 0:
                    default_rows.append(row)
                elif pk % 3 == 1:
                    partition1_rows.append(row)
                else:
                    partition2_rows.append(row)

            # Insert into respective partitions
            if default_rows:
                self.insert(client, collection_name, data=default_rows)
            if partition1_rows:
                self.insert(client, collection_name, data=partition1_rows, partition_name=partition_names[0])
            if partition2_rows:
                self.insert(client, collection_name, data=partition2_rows, partition_name=partition_names[1])

        self.flush(client, collection_name)
        self.wait_for_index_ready(client, collection_name, index_name=ct.default_vector_field_name)

        # search in the collection
        vectors_to_search = cf.gen_vectors(1, ct.default_dim)
        limit = 100
        search_params = {}
        search_res1, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": limit})

        # find one result that not in default partition
        to_be_released_partition = None
        the_other_partition = None
        pk_remainder = 0
        for i in range(limit):
            top_id = search_res1[0].ids[i]
            if top_id % 3 == 0:
                pass
            elif top_id % 3 == 1:
                to_be_released_partition = partition_names[0]
                the_other_partition = partition_names[1]
                pk_remainder = 1
                break
            else:
                to_be_released_partition = partition_names[1]
                the_other_partition = partition_names[0]
                pk_remainder = 2
                break

        # release the partition
        if to_be_released_partition is not None:
            self.release_partitions(client, collection_name, [to_be_released_partition])
        else:
            assert False, "expected to find at least one result that not in default partition"

        # search again
        search_res2, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": limit})
        # verify no results are from the released partition
        for i in range(limit):
            not_exist = (search_res2[0].ids[i] not in search_res1[0].ids) or (search_res2[0].ids[i] % 3 != pk_remainder)
            assert not_exist

        # search in the non-released partitions
        search_res3, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            partition_names=[ct.default_partition_name, the_other_partition],
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": limit})
        # verify the results are same as the 2nd search results
        assert search_res3[0].ids == search_res2[0].ids

        # load the released partition and search again
        self.load_partitions(client, collection_name, [to_be_released_partition])
        search_res4, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": limit})
        # verify the results are same as the first search results
        # assert search_res4[0].ids == search_res1[0].ids

        # release the partition again and load the collection
        self.release_partitions(client, collection_name, [to_be_released_partition])
        self.load_collection(client, collection_name)
        self.refresh_load(client, collection_name)  # workaround for #43386, remove this line after it was fixed

        # search again
        search_res5, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field=ct.default_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "pk_name": ct.default_primary_key_field_name,
                         "metric": "COSINE",
                         "limit": limit})
        # verify the results are same as the first search results
        assert search_res5[0].ids == search_res4[0].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.max_dim, ct.min_dim])
    def test_search_max_and_min_dim(self, dim):
        """
        target: test search with max and min dimension collection
        method: create connection, collection, insert and search with max and min dimension
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=dim)

        # insert data
        nb = 200
        all_vectors = cf.gen_vectors(nb, dim)
        data = [{"id": i, "vector": all_vectors[i]} for i in range(nb)]
        self.insert(client, collection_name, data)

        # search
        vectors_to_search = cf.gen_vectors(ct.default_nq, dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
                         "metric": "COSINE",
                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_after_recreate_index(self):
        """
        target: test search after recreate index
        method: create connection, collection, insert and search after recreate index
        expected: search successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # insert data
        all_vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [{"id": i, "vector": all_vectors[i]} for i in range(ct.default_nb)]
        self.insert(client, collection_name, data)

        self.flush(client, collection_name)
        self.wait_for_index_ready(client, collection_name, index_name='vector')

        # search
        vectors_to_search = cf.gen_vectors(ct.default_nq, ct.default_dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
                         "metric": "COSINE",
                         "limit": ct.default_limit})

        # recreate index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, index_name='vector')

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name='vector', index_type='HNSW',
                               params={"M": 8, "efConstruction": 128}, metric_type='L2')
        self.create_index(client, collection_name, index_params=index_params)

        self.wait_for_index_ready(client, collection_name, index_name='vector')
        self.load_collection(client, collection_name)

        # search after recreate with L2
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
                         "metric": "L2",
                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", [i for i in ct.all_dense_float_index_types if i != "DISKANN"])  # DISKANN is disk-based, does not support mmap
    def test_each_index_with_mmap_enabled_search(self, index):
        """
        target: test each index with mmap enabled search
        method: test each index with mmap enabled search
        expected: search success
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        dim = 32
        schema = self.create_schema(client)[0]
        schema.add_field('id', DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field('vector', DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        all_vectors = cf.gen_vectors(ct.default_nb, dim)
        data = [{"id": i, "vector": all_vectors[i]} for i in range(ct.default_nb)]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # create index
        index_params = self.prepare_index_params(client)[0]
        params = cf.get_index_params_params(index)
        index_params.add_index(field_name='vector', index_type=index, params=params, metric_type='L2')
        self.create_index(client, collection_name, index_params=index_params)
        self.wait_for_index_ready(client, collection_name, index_name='vector')

        # alter mmap index
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": True})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'True'
        # search
        self.load_collection(client, collection_name)
        search_params = {}
        vector = cf.gen_vectors(ct.default_nq, dim)
        self.search(client, collection_name, vector, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "L2"})
        # disable mmap
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": False})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'False'
        self.load_collection(client, collection_name)
        self.search(client, collection_name, vector, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "L2"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.binary_supported_index_types)
    def test_enable_mmap_search_for_binary_indexes(self, index):
        """
        Test enabling mmap for binary indexes in Milvus.
        
        This test verifies that:
        1. Binary vector indexes can be successfully created with mmap enabled
        2. Search operations work correctly with mmap enabled
        3. Mmap can be properly disabled and search still works
        
        The test performs following steps:
        - Creates a collection with binary vectors
        - Inserts test data
        - Creates index with mmap enabled
        - Verifies mmap status
        - Performs search with mmap enabled
        - Disables mmap and verifies search still works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        dim = 64
        schema = self.create_schema(client)[0]
        schema.add_field('id', DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field('vector', DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        _, binary_vectors = cf.gen_binary_vectors(ct.default_nb, dim)
        data = [{"id": i, "vector": binary_vectors[i]} for i in range(ct.default_nb)]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # create index
        index_params = self.prepare_index_params(client)[0]
        params = cf.get_index_params_params(index)
        index_params.add_index(field_name='vector', index_type=index, params=params, metric_type='JACCARD')
        self.create_index(client, collection_name, index_params=index_params)
        self.wait_for_index_ready(client, collection_name, index_name='vector')
        # alter mmap index
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": True})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'True'
        # load collection
        self.load_collection(client, collection_name)
        # search
        binary_vectors = cf.gen_binary_vectors(ct.default_nq, dim)[1]
        params = cf.get_search_params_params(index)
        search_params = {"metric_type": "JACCARD", "params": params}
        output_fields = ["*"]
        self.search(client, collection_name, binary_vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "JACCARD"})
        # disable mmap
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": False})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'False'
        self.load_collection(client, collection_name)
        self.search(client, collection_name, binary_vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "JACCARD"})
    
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("num_shards", [-256, 0, ct.max_shards_num // 2, ct.max_shards_num])
    def test_search_with_non_default_shard_nums(self, num_shards):
        """
        Test search functionality with non-default shard numbers.
        
        This test verifies that search operations work correctly when collections are created with:
        - Negative shard numbers (should use default)
        - Zero shards (should use default)
        - Half of max shards
        - Max shards
        
        The test performs the following steps:
        1. Creates a collection with specified shard number
        2. Inserts test data
        3. Builds index
        4. Loads collection
        5. Executes search and verifies results
        
        @param num_shards: Number of shards to test (parameterized)
        @tags: L2
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection
        dim = 32
        schema = self.create_schema(client)[0]
        schema.add_field('id', DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field('vector', DataType.FLOAT_VECTOR, dim=dim)
        # create collection
        self.create_collection(client, collection_name, schema=schema, num_shards=num_shards)
        collection_info = self.describe_collection(client, collection_name)[0]
        expected_num_shards = ct.default_shards_num if num_shards <= 0 else num_shards
        assert collection_info["num_shards"] == expected_num_shards
        # insert
        all_vectors = cf.gen_vectors(ct.default_nb, dim)
        data = [{"id": i, "vector": all_vectors[i]} for i in range(ct.default_nb)]
        self.insert(client, collection_name, data)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name='vector', index_type='HNSW', metric_type='COSINE')
        self.create_index(client, collection_name, index_params=index_params)
        self.wait_for_index_ready(client, collection_name, index_name='vector')
        # load
        self.load_collection(client, collection_name)
        # search
        vectors = cf.gen_vectors(ct.default_nq, dim)
        search_params = {}
        self.search(client, collection_name, vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "COSINE"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('vector_dtype', ct.all_dense_vector_types)
    @pytest.mark.parametrize('index', ct.all_dense_float_index_types)
    def test_search_output_field_vector_with_dense_vector_and_index(self, vector_dtype, index):
        """
        Test search with output vector field after different index types.
        
        Steps:
        1. Create a collection with specified schema and insert test data
        2. Build index (with error handling for unsupported index types)
        3. Load collection and perform search operations with:
           - All output fields ("*")
           - Explicitly specified all fields
           - Subset of fields
        4. Verify search results match expected output fields
        
        Parameters:
        - vector_dtype: Type of vector data (all supported dense vector types)
        - index: Index type (first 8 supported index types)
        
        Expected:
        - Successful search operations with correct output fields returned
        - Proper error when attempting unsupported index combinations
        """

        metrics = 'COSINE'
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        schema = self.create_schema(client)[0]
        schema.add_field('id', DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field('vector', vector_dtype, dim=dim, nullable=True)
        schema.add_field('float_vector2', DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        schema.add_field('float_array', DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=200)
        schema.add_field('json_field', DataType.JSON, max_length=200)
        schema.add_field('string_field', DataType.VARCHAR, max_length=200)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data in 3 batches with unique primary keys using a loop
        insert_times = 3
        random_vectors = list(cf.gen_vectors(ct.default_nb * insert_times, dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(ct.default_nb * insert_times, dim, vector_data_type=vector_dtype)
        for j in range(insert_times):
            start_pk = j * ct.default_nb
            rows = [{
                "id": i + start_pk,
                "vector": None if random.random() < 0.3 else random_vectors[i + start_pk],
                "float_vector2": None if random.random() < 0.9 else cf.gen_vectors(1, dim)[0],
                "float_array": [random.random() for _ in range(10)],
                "json_field": {"name": "abook", "words": i},
                "string_field": "Hello, Milvus!"
            } for i in range(ct.default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name='vector', index_type=index,
                               metric_type=metrics,
                               params=cf.get_index_params_params(index_type=index))
        if vector_dtype == DataType.INT8_VECTOR and index != 'HNSW':
            # INT8_Vector only supports HNSW index for now
            error = {"err_code": 999, "err_msg": f"data type Int8Vector can't build with this index {index}"}
            self.create_index(client, collection_name, index_params=index_params,
                              check_task=CheckTasks.err_res, check_items=error)
        else:
            index_params.add_index(field_name='float_vector2', index_type="HNSW", metric_type="L2")
            self.create_index(client, collection_name, index_params=index_params)

            # load the collection with index
            assert self.wait_for_index_ready(client, collection_name, "vector", timeout=120)
            self.load_collection(client, collection_name)

            # search with output field vector
            search_params = {}
            vectors = random_vectors[:ct.default_nq]
            limit = 100
            # search output all fields
            self.search(client, collection_name, vectors, anns_field="vector",
                        search_params=search_params, limit=limit,
                        output_fields=["*"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": limit,
                                     "metric": metrics,
                                     "output_fields": ["id", "vector", "float_vector2", "float_array", "json_field", "string_field"]})
            # search output specify all fields
            self.search(client, collection_name, vectors, anns_field="vector",
                        search_params=search_params, limit=limit,
                        output_fields=["id", "vector", "float_vector2", "float_array", "json_field", "string_field"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": limit,
                                     "metric": metrics,
                                     "output_fields": ["id", "vector", "float_vector2", "float_array", "json_field", "string_field"]})
            # search output specify some fields
            self.search(client, collection_name, vectors, anns_field="vector",
                        search_params=search_params, limit=limit,
                        output_fields=["id", "vector", "float_vector2", "json_field"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": limit,
                                     "metric": metrics,
                                     "output_fields": ["id", "vector", "float_vector2", "json_field"]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('index', ct.binary_supported_index_types)
    def test_search_with_output_fields_vector_with_binary_vector_and_index(self, index):
        """
        Test search functionality with output fields for binary vector type and specified index.
        
        This test case verifies that:
        1. A collection with binary vector field can be created and data inserted
        2. Index can be built on the binary vector field
        3. Search operation with output fields (including vector field) works correctly
        4. Results contain expected output fields (id and vector)
        
        Parameters:
            index: The index type to test with (parametrized via pytest.mark.parametrize)
        
        The test performs following steps:
        - Creates collection with binary vector field
        - Inserts test data in batches
        - Builds specified index type
        - Performs search with output fields
        - Validates search results contain expected fields
        """
        vector_dtype = DataType.BINARY_VECTOR
        client = self._client()
        dim = 32
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=vector_dtype, dim=dim, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data in 3 batches with unique primary keys using a loop
        insert_times = 3
        random_vectors = list(cf.gen_vectors(ct.default_nb * insert_times, dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(ct.default_nb * insert_times, dim, vector_data_type=vector_dtype)
        for j in range(insert_times):
            start_pk = j * ct.default_nb
            rows = [{
                "id": i + start_pk,
                "vector": None if random.random() < 0.3 else random_vectors[i + start_pk]
            } for i in range(ct.default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name='vector', index_type=index,
                               metric_type='JACCARD',
                               params=cf.get_index_params_params(index_type=index))
        self.create_index(client, collection_name, index_params=index_params)

        # load the collection with index
        assert self.wait_for_index_ready(client, collection_name, 'vector', timeout=120)
        self.load_collection(client, collection_name)

        # search with output field vector
        search_params = {}
        vectors = random_vectors[:ct.default_nq]
        self.search(client, collection_name, vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "metric": "JACCARD",
                                 "output_fields": ["id", "vector"]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_empty(self):
        """
        target: test search with output fields
        method: search with empty output_field
        expected: search success
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        # create collection with fast mode
        self.create_collection(client, collection_name, dimension=dim)
        # insert data
        all_vectors = cf.gen_vectors(ct.default_nb, dim)
        data = [{"id": i, "vector": all_vectors[i]} for i in range(ct.default_nb)]
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # search with empty output fields
        search_params = {}
        vectors = cf.gen_vectors(ct.default_nq, dim)
        self.search(client, collection_name, vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=[],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "metric": "COSINE",
                                 "limit": ct.default_limit})
        self.search(client, collection_name, vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=None,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "metric": "COSINE",
                                 "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_dense_float_index_types[1:])
    def test_search_repeatedly_with_different_index(self, index):
        """
        Test searching repeatedly with different index types to ensure consistent results.

        This test verifies that searching the same collection with different index types
        produces consistent results when performed multiple times. It covers various index
        types (excluding the first one in the list) and checks that the search results
        remain the same across repeated searches.

        Steps:
        1. Create a collection with a custom schema.
        2. Insert data into the collection.
        3. Build an index on the vector field.
        4. Perform searches with different parameters for each index type.
        5. Verify that the search results are consistent across repeated searches.

        Expected:
        - The search results should be the same for repeated searches with the same parameters.
        - The results should match the expected behavior for each index type.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        # create collection with custom schema
        schema, _ = self.create_schema(client)
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # build index
        index_params, _ = self.prepare_index_params(client)
        params = cf.get_index_params_params(index)
        index_params.add_index(field_name='vector', index_type=index, metric_type="COSINE", params=params)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # 3. search with different params for different index
        limit = 100
        search_params = cf.get_search_params_params(index)
        if index == "HNSW":
            search_params.update({"ef": limit * 3})
        if index == "SCANN":
            search_params.update({"reorder_k": limit * 3})
        if index == "DISKANN":
            search_params.update({"search_list": limit * 3})
        vector = cf.gen_vectors(ct.default_nq, dim)
        res1 = self.search(client, collection_name, vector, anns_field="vector",
                           search_params=search_params, limit=limit)[0]
        res2 = self.search(client, collection_name, vector, anns_field="vector",
                           search_params=search_params, limit=limit * 2)[0]
        for i in range(ct.default_nq):
            assert res1[i].ids == res2[i].ids[:limit]
        # search again with the previous limit
        res3 = self.search(client, collection_name, vector, anns_field="vector",
                           search_params=search_params, limit=limit)[0]
        for i in range(ct.default_nq):
            assert res1[i].ids == res3[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="too slow and memory cost in e2e")
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:1])
    @pytest.mark.parametrize("index", ["BIN_IVF_FLAT"])
    @pytest.mark.parametrize("dim", [ct.max_binary_vector_dim])
    def test_binary_indexed_large_dim_vectors_search(self, dim, metrics, index):
        """
        target: binary vector large dim search
        method: binary vector large dim search
        expected: search success
        """
        # 1. create a collection and insert data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", datatype=DataType.BINARY_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        # insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # 2. create index and load
        index_params, _ = self.prepare_index_params(client)
        params = cf.get_index_params_params(index)
        index_params.add_index(field_name='vector', index_type=index, metric_type=metrics, params=params)
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # 3. search with output field vector
        search_params = cf.get_search_params_params(index)
        binary_vectors = cf.gen_vectors(1, dim, vector_data_type=DataType.BINARY_VECTOR)
        res = self.search(client, collection_name, binary_vectors, anns_field="vector",
                          search_params=search_params, limit=ct.default_limit,
                          output_fields=["vector"],
                          check_task=CheckTasks.check_search_results,
                          check_items={"enable_milvus_client_api": True,
                                       "nq": 1,
                                       "limit": ct.default_limit,
                                       "pk_name": "id",
                                       "metric": metrics,
                                       "output_fields": ["vector"]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_verify_expr_cache(self):
        """
        target: test search case to test expr cache
        method: 1. create collection with a double datatype field
                2. search with expr "doubleField == 0"
                3. drop this collection
                4. create collection with same collection name and same field name but modify the type of double field
                   as varchar datatype
                5. search with expr "doubleField == 0" again
        expected: 1. search successfully with limit(topK) for the first collection
                  2. report error for the second collection with the same name
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        schema, _ = self.create_schema(client)
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("double_field", datatype=DataType.DOUBLE, is_nullable=True)
        schema.add_field("expr_field", datatype=DataType.FLOAT, is_nullable=True)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        # create collection with custom schema
        self.create_collection(client, collection_name, schema=schema)
        # insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name='vector', index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        # 2. generate search data
        search_vectors = cf.gen_vectors(ct.default_nq, dim)
        # 3. search with expr "expr_field == 0"
        search_params = {}
        search_exp = "expr_field >= 0"
        output_fields = ["id", "expr_field", "double_field"]
        search_res, _ = self.search(client, collection_name, search_vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "metric": "COSINE",
                                 "output_fields": output_fields})
        # verify returned entities satisfy the filter
        for hits in search_res:
            for hit in hits:
                assert hit.get("expr_field", 0) >= 0
        # 4. drop collection
        self.drop_collection(client, collection_name)
        # 5. create the same collection name with same field name but varchar field type
        schema, _ = self.create_schema(client)
        schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("double_field", datatype=DataType.DOUBLE, is_nullable=True)
        schema.add_field("expr_field", datatype=DataType.VARCHAR, max_length=200, is_nullable=True)
        schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name='vector', index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        # 6. search with expr "expr_field == 0"
        self.search(client, collection_name, search_vectors, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "error: comparisons between VarChar and Int64 are not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_using_all_types_of_default_value(self):
        """
        target: test create collection with default_value
        method: create a schema with all fields using default value and search
        expected: search results are as expected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        schema = self.create_schema(client)[0]
        self.add_field(schema, field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        self.add_field(schema, field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.add_field(schema, field_name=ct.default_int8_field_name, datatype=DataType.INT8, default_value=np.int8(8))
        self.add_field(schema, field_name=ct.default_int16_field_name, datatype=DataType.INT16, default_value=np.int16(16))
        self.add_field(schema, field_name=ct.default_int32_field_name, datatype=DataType.INT32, default_value=np.int32(32))
        self.add_field(schema, field_name=ct.default_int64_field_name, datatype=DataType.INT64, default_value=np.int64(64))
        self.add_field(schema, field_name=ct.default_float_field_name, datatype=DataType.FLOAT, default_value=np.float32(3.14))
        self.add_field(schema, field_name=ct.default_double_field_name, datatype=DataType.DOUBLE, default_value=np.double(3.1415))
        self.add_field(schema, field_name=ct.default_bool_field_name, datatype=DataType.BOOL, default_value=False)
        self.add_field(schema, field_name=ct.default_string_field_name, datatype=DataType.VARCHAR, default_value="abc")

        self.create_collection(client, collection_name, schema=schema)

        skip_field_names=[ct.default_int8_field_name, ct.default_int16_field_name, ct.default_int32_field_name,
                          ct.default_int64_field_name, ct.default_float_field_name, ct.default_double_field_name,
                          ct.default_bool_field_name, ct.default_string_field_name]
        data = cf.gen_row_data_by_schema(schema=schema, skip_field_names=skip_field_names)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        search_vectors = cf.gen_vectors(ct.default_nq, dim=dim)
        res = self.search(client, collection_name, search_vectors,
                          anns_field=ct.default_float_vec_field_name,
                          search_params={},
                          limit=ct.default_limit,
                          output_fields=["*"],
                          check_task=CheckTasks.check_search_results,
                          check_items={"enable_milvus_client_api": True,
                                       "nq": ct.default_nq,
                                       "pk_name": ct.default_primary_field_name,
                                       "metric": "COSINE",
                                       "limit": ct.default_limit})[0]
        for res in res[0]:
            res = res.entity
            assert res.get(ct.default_int8_field_name) == 8
            assert res.get(ct.default_int16_field_name) == 16
            assert res.get(ct.default_int32_field_name) == 32
            assert res.get(ct.default_int64_field_name) == 64
            assert res.get(ct.default_float_field_name) == np.float32(3.14)
            assert res.get(ct.default_double_field_name) == 3.1415
            assert res.get(ct.default_bool_field_name) is False
            assert res.get(ct.default_string_field_name) == "abc"
        
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_ignore_growing(self):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search ignore_growing=True, inside/outside search params
        expected: searched successfully
        """
        # 1. create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        schema = self.create_schema(client)[0]
        self.add_field(schema, field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        self.add_field(schema, field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data again
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)

        # 3. insert data again
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema, start=ct.default_nb * 5)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # 4. search with param ignore_growing=True
        search_params = {}
        search_vectors = cf.gen_vectors(ct.default_nq, dim=dim)
        res1 = self.search(client, collection_name, search_vectors,
                           anns_field=ct.default_float_vec_field_name,
                           search_params=search_params,
                           limit=ct.default_limit,
                           ignore_growing=True,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": ct.default_nq,
                                        "limit": ct.default_limit,
                                        "pk_name": ct.default_primary_field_name,
                                        "metric": "COSINE"})[0]
        search_params = {"ignore_growing": True}
        res2 = self.search(client, collection_name, search_vectors,
                           anns_field=ct.default_float_vec_field_name,
                           search_params=search_params,
                           limit=ct.default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": ct.default_nq,
                                        "limit": ct.default_limit,
                                        "pk_name": ct.default_primary_field_name,
                                        "metric": "COSINE"})[0]
        for i in range(ct.default_nq):
            assert max(res1[i].ids) < ct.default_nb * 5
            assert max(res2[i].ids) < ct.default_nb * 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_json_fields_comparison_in_filter(self):
        """
        Test case for searching with JSON fields comparison in filter.

        Target:
            Verify that searching with JSON fields comparison in filter behaves as expected.

        Method:
            1. Create a collection with JSON fields.
            2. Insert data into the collection.
            3. Build an index and load the collection.
            4. Perform a search with a filter comparing JSON fields.

        Expected:
            The search should return an error indicating that JSON field comparison is not supported.
        """ 
        # 1. create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        schema = self.create_schema(client)[0]
        self.add_field(schema, field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        self.add_field(schema, field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.add_field(schema, field_name='json_field1', datatype=DataType.JSON, is_nullable=True)
        self.add_field(schema, field_name='json_field2', datatype=DataType.JSON, is_nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        
        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        
        # 3. search with json fields comparison in filter
        search_params = {}
        search_vectors = cf.gen_vectors(1, dim=dim)
        search_exp = "json_field1['count'] < json_field2['count']"
        error = {ct.err_code: 999, ct.err_msg: "two column comparison with JSON type is not supported"}
        self.search(client, collection_name, search_vectors,
                    anns_field=ct.default_float_vec_field_name,
                    search_params=search_params,
                    limit=ct.default_limit,
                    filter=search_exp,
                    check_task=CheckTasks.err_res,
                    check_items=error)


@pytest.mark.xdist_group("TestSearchV2Shared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchV2Shared(TestMilvusClientV2Base):
    """Test search with shared enriched collection.
    Schema: int64(PK), int32, int16, int8, bool(nullable), float(nullable), double,
            varchar(65535), json, int32_array(100), float_array(100), string_array(100, nullable),
            float_vector(128), float16_vector(128), bfloat16_vector(128)
    Data: 10000 rows, gen_row_data_by_schema + 20% nulls for nullable fields + dynamic "new_added_field"
    Index: FLAT / COSINE on all 3 vector fields
    Dynamic: True
    """

    shared_alias = "TestSearchV2Shared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchV2Shared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        # Scalar fields (float, bool, string_array are nullable for nullable coverage)
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL, nullable=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        # Array fields (string_array is nullable)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        # Vector fields
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR, dim=default_dim)
        schema.add_field(ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        self.__class__.shared_nb = 10000
        self.__class__.shared_dim = default_dim
        data = cf.gen_row_data_by_schema(nb=self.shared_nb, schema=schema)
        # Inject ~20% null values for nullable fields (float, bool, string_array)
        null_ratio = 0.2
        null_count = int(self.shared_nb * null_ratio)
        for i in range(null_count):
            data[i][ct.default_float_field_name] = None
            data[i][ct.default_bool_field_name] = None
            data[i][ct.default_string_array_field_name] = None
        # Add dynamic field for exists test
        for i in range(self.shared_nb):
            data[i]["new_added_field"] = i
        self.__class__.shared_data = data
        self.__class__.shared_insert_ids = [i for i in range(self.shared_nb)]
        self.__class__.shared_vector_fields = [
            (ct.default_float_vec_field_name, DataType.FLOAT_VECTOR),
            (ct.default_float16_vec_field_name, DataType.FLOAT16_VECTOR),
            (ct.default_bfloat16_vec_field_name, DataType.BFLOAT16_VECTOR),
        ]
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        idx.add_index(field_name=ct.default_float16_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        idx.add_index(field_name=ct.default_bfloat16_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    # ==================== Expression filter tests ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self):
        """
        target: verify filter expressions return only matching results and templates produce equivalent results
        method: 1. search with each expression from gen_normal_expressions_and_templates
                2. compute expected filter_ids locally using eval
                3. verify returned IDs match filter_ids exactly (FLAT index, 100% recall)
                4. repeat with expression template and with iterative_filter hint
        expected: exact ID match for FLAT index, distances in descending order (COSINE)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        # NULL sentinel: all comparisons return False (SQL NULL semantics)
        _null = type('_Null', (), {
            '__eq__': lambda s, o: False, '__ne__': lambda s, o: False,
            '__lt__': lambda s, o: False, '__le__': lambda s, o: False,
            '__gt__': lambda s, o: False, '__ge__': lambda s, o: False,
            '__hash__': lambda s: hash(None)})()

        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_search_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                float_val = data[i][ct.default_float_field_name]
                local_vars = {ct.default_int64_field_name: data[i][ct.default_int64_field_name],
                              ct.default_float_field_name: float_val if float_val is not None else _null}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            expected_limit = min(nb, len(filter_ids))

            # 3. search with expression
            search_vectors = cf.gen_vectors(default_nq, dim)
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set
                assert len(hits) == expected_limit
                # verify distance ordering (COSINE: descending)
                distances = [hit["distance"] for hit in hits]
                assert all(distances[j] >= distances[j+1] for j in range(len(distances)-1)), \
                    "distances not in descending order for COSINE metric"

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq, "limit": expected_limit,
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set

            # 5. search again with expression template and search hints
            search_param = default_search_params.copy()
            search_param.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_param,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq, "limit": expected_limit,
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == filter_ids_set

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, bool_type):
        """
        target: verify search with bool filter returns only rows matching the bool condition
        method: 1. search shared collection with bool == True/False/"true"/"false"
                2. compute expected filter_ids locally
                3. verify returned IDs match exactly (FLAT, 100% recall)
        expected: exact match on filter_ids for bool filter (NULL bools excluded)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i, _id in enumerate(insert_ids):
            # NULL bool values are excluded from == comparison (SQL NULL semantics)
            val = data[i].get(ct.default_bool_field_name)
            if val is not None and val == bool_type_cmp:
                filter_ids.append(_id)

        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with expression: %s" % expression)
        search_vectors = cf.gen_vectors(default_nq, dim)

        expected_limit = min(nb, len(filter_ids))
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=nb,
                                    filter=expression)
        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = [hit[ct.default_int64_field_name] for hit in hits]
            assert set(ids) == filter_ids_set
            assert len(hits) == expected_limit

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", [DataType.INT8, DataType.INT16, DataType.INT32])
    def test_search_expression_different_data_type(self, field):
        """
        target: verify search with out-of-bound integer filter returns empty and normal search returns correct output fields
        method: 1. search with expression where field value exceeds its type range (expect 0 results)
                2. search without filter, verify output_fields contains the requested scalar field
        expected: out-of-bound filter returns 0 results; normal search returns correct output fields
        """
        field_name_str = field.name.lower()
        num = int(field_name_str[3:])
        offset = 2 ** (num - 1)

        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(default_nq, self.shared_dim)

        expression = f"{field_name_str} >= {offset}"
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=expression,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq, "limit": 0,
                                 "metric": "COSINE",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "COSINE",
                                 "output_fields": [field_name_str],
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    # ==================== Round decimal ====================

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: verify search with round_decimal returns distances rounded to specified precision
        method: 1. search without round_decimal to get reference distances
                2. search with round_decimal and compare distances match expected rounding
        expected: rounded distances match Python's round() within abs_tol
        """
        tmp_nq = 1
        tmp_limit = 5
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(tmp_nq, self.shared_dim)

        res, _ = self.search(client, self.collection_name,
                             data=search_vectors[:tmp_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=tmp_limit)

        res_round, _ = self.search(client, self.collection_name,
                                   data=search_vectors[:tmp_nq],
                                   anns_field=default_search_field,
                                   search_params=default_search_params,
                                   limit=tmp_limit,
                                   round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        pk = ct.default_int64_field_name
        dist_map = {res[0][i][pk]: res[0][i]["distance"] for i in range(tmp_limit)}
        matched_count = 0
        for i in range(len(res_round[0])):
            _id = res_round[0][i][pk]
            if _id in dist_map:
                dis_expect = round(dist_map[_id], round_decimal)
                dis_actual = res_round[0][i]["distance"]
                assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)
                matched_count += 1
        assert matched_count > 0, "no matching PKs found between rounded and unrounded results"

    # ==================== Array expression tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_array(self):
        """
        target: verify search with array field expressions returns exact matching results
        method: 1. search with each array expression from gen_array_field_expressions_and_templates
                2. compute expected filter_ids locally using eval on array data
                3. verify exact ID match (FLAT index, 100% recall)
                4. repeat with expression template and iterative_filter hint
        expected: exact match between expected and returned IDs
        """
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data

        for expressions in cf.gen_array_field_expressions_and_templates():
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i in range(nb):
                int32_array = data[i][ct.default_int32_array_field_name]
                float_array = data[i][ct.default_float_array_field_name]
                string_array = data[i][ct.default_string_array_field_name]
                # Skip rows with null string_array when expression references it
                if ct.default_string_array_field_name in expr and string_array is None:
                    continue
                if not expr or eval(expr):
                    filter_ids.append(i)

            search_vectors = cf.gen_vectors(default_nq, dim)
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # search again with expression template and hints
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

    # ==================== Exists expression tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("json_field_name", ["json_field['name']", "json_field['number']",
                                                 "not_exist_field", "new_added_field"])
    def test_search_with_expression_exists(self, json_field_name):
        """
        target: verify 'exists' expression returns correct results for existing and non-existing fields
        method: 1. search with 'exists <field>' for json fields, json subfields, arrays, dynamic fields
                2. compute expected limit based on actual data field/subfield existence
        expected: existing fields/subfields return all rows, non-existing return empty
        """
        exists = "exists"
        nb = self.shared_nb
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        data = self.shared_data

        expression = exists + " " + json_field_name
        # Determine expected limit: check top-level field or JSON subfield existence
        if json_field_name in data[0]:
            limit = nb
        elif "'" in json_field_name:
            # JSON subfield: extract base field and key, e.g. "json_field['name']" → ("json_field", "name")
            base = json_field_name.split("[")[0]
            key = json_field_name.split("'")[1]
            if base in data[0] and isinstance(data[0][base], dict) and key in data[0][base]:
                limit = nb
            else:
                limit = 0
        else:
            limit = 0
        log.info("test_search_with_expression_exists: expression=%s, expected_limit=%d" % (expression, limit))
        search_vectors = cf.gen_vectors(default_nq, dim)
        self.search(client, self.collection_name,
                    data=search_vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=nb,
                    filter=expression,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": limit,
                                 "metric": "COSINE",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    # ==================== Multi-vector type tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_vector_types(self):
        """
        target: verify search works across all vector field types (float, float16, bfloat16) with scalar filter
        method: 1. search each vector field with scalar filter on shared enriched collection
                2. verify nq, limit, metric, and output fields via check_task
        expected: correct results for each vector type with proper output fields
        """
        client = self._client(alias=self.shared_alias)
        nq = 10
        search_exp = (f"{ct.default_int64_field_name} >= 0 && {ct.default_int32_field_name} >= 0 && "
                      f"{ct.default_int16_field_name} >= 0 && {ct.default_int8_field_name} >= 0 && "
                      f"{ct.default_float_field_name} >= 0 && {ct.default_double_field_name} >= 0")

        for vec_field, vec_dtype in self.shared_vector_fields:
            search_vectors = cf.gen_vectors(nq, self.shared_dim, vec_dtype)
            res, _ = self.search(client, self.collection_name,
                                 data=search_vectors[:nq],
                                 anns_field=vec_field,
                                 search_params=default_search_params,
                                 limit=default_limit,
                                 filter=search_exp,
                                 output_fields=[default_int64_field_name,
                                                default_float_field_name,
                                                default_bool_field_name],
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": nq,
                                              "limit": default_limit,
                                              "metric": "COSINE",
                                              "enable_milvus_client_api": True,
                                              "pk_name": ct.default_int64_field_name})
            assert default_int64_field_name in res[0][0]["entity"]
            assert default_float_field_name in res[0][0]["entity"]
            assert default_bool_field_name in res[0][0]["entity"]

    # ==================== Large-scale expression tests ====================

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: verify search with large nq (5000) and range filter works correctly
        method: 1. search shared collection with nq=5000 and range filter "0 < int64 < 5001"
                2. verify via check_task and manual filter assertion
        expected: all returned IDs satisfy the range filter
        """
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        insert_ids = self.shared_insert_ids

        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression_large: searching with expression: %s" % expression)

        nums = 5000
        search_vectors = cf.gen_vectors(nums, dim)
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nums,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "metric": "COSINE",
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name})
        # Manual filter verification
        for hits in search_res:
            for hit in hits:
                pk = hit[ct.default_int64_field_name]
                assert 0 < pk < 5001, f"filter violation: id={pk} not in range (0, 5001)"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: verify search with large 'in' expression (5000 random IDs) works correctly
        method: 1. search shared collection with nq=5000 and 'int64 in [...]' filter
                2. verify via check_task and manual filter assertion
        expected: all returned IDs are within the specified ID list
        """
        dim = self.shared_dim
        client = self._client(alias=self.shared_alias)
        insert_ids = self.shared_insert_ids

        nums = 5000
        search_vectors = cf.gen_vectors(nums, dim)
        vectors_id = [random.randint(0, nums) for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={
                                        "nq": nums,
                                        "ids": insert_ids,
                                        "limit": default_limit,
                                        "metric": "COSINE",
                                        "enable_milvus_client_api": True,
                                        "pk_name": ct.default_int64_field_name,
                                    })
        # Manual filter verification
        vectors_id_set = set(vectors_id)
        for hits in search_res:
            for hit in hits:
                assert hit[ct.default_int64_field_name] in vectors_id_set, \
                    f"filter violation: id={hit[ct.default_int64_field_name]} not in filter list"


class TestSearchV2LegacyIndependent(TestMilvusClientV2Base):
    """ Test cases that require independent collections (auto_id, state modification, custom schema/data) """

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_auto_id(self):
        """
        target: verify filter expressions work correctly with auto_id primary key
        method: 1. create collection with auto_id=True and dynamic fields
                2. search with each float-field expression, compute expected filter_ids
                3. verify returned IDs are subset of filter_ids (IVF_FLAT recall tolerance)
                4. repeat with expression template
        expected: all returned IDs satisfy the filter, recall >= 80% for IVF_FLAT
        """
        nb = ct.default_nb
        dim = 64
        search_limit = nb // 2
        enable_dynamic_field = True
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="IVF_FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(default_nq, dim)
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 64}}
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                local_vars = {default_float_field_name: data[i][default_float_field_name]}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            expected_limit = min(search_limit, len(filter_ids))
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=search_limit,
                                        filter=expr)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=search_limit,
                                        filter=expr, filter_params=expr_params)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expr_json_field(self):
        """
        target: verify search with JSON field expressions works before and after JSON path index creation
        method: 1. create collection with JSON field, insert data with number/float JSON keys
                2. search with JSON expressions, verify via check_task and manual subset assertion
                3. create JSON path indexes (INVERTED on number, AUTOINDEX on float)
                4. release/load and repeat searches to verify JSON index correctness
        expected: all returned IDs satisfy the JSON filter, results consistent before and after JSON indexing
        """
        nb = ct.default_nb
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data - use gen_default_rows_data to get json with {"number": i, "float": i*1.0}
        data = cf.gen_default_rows_data(nb=nb, dim=dim, with_json=True)
        self.insert(client, collection_name, data=data)
        insert_ids = [i for i in range(nb)]
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vectors = cf.gen_vectors(default_nq, dim)
        for expressions in cf.gen_json_field_expressions_and_templates():
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            json_field = {}
            for i, _id in enumerate(insert_ids):
                json_field['number'] = data[i][ct.default_json_field_name]['number']
                json_field['float'] = data[i][ct.default_json_field_name]['float']
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # 3. search expressions
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 6. create json index
            idx2 = self.prepare_index_params(client)[0]
            idx2.add_index(field_name=ct.default_json_field_name,
                           index_type="INVERTED",
                           index_name=f"{ct.default_json_field_name}_0",
                           params={"json_cast_type": "double",
                                   "json_path": f"{ct.default_json_field_name}['number']"})
            self.create_index(client, collection_name, index_params=idx2)

            idx3 = self.prepare_index_params(client)[0]
            idx3.add_index(field_name=ct.default_json_field_name,
                           index_type="AUTOINDEX",
                           index_name=f"{ct.default_json_field_name}_1",
                           params={"json_cast_type": "double",
                                   "json_path": f"{ct.default_json_field_name}['float']"})
            self.create_index(client, collection_name, index_params=idx3)

            # 7. release and load to make sure the new index is loaded
            self.release_collection(client, collection_name)
            self.load_collection(client, collection_name)
            # 8. search expressions after json path index
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 9. search again with expression template after json path index
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 10. search again with expression template and hint after json path index
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "metric": "COSINE",
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self):
        """
        target: verify search with cross-field comparison expression (int64_1 <= int64_2)
        method: 1. create collection with two int64 fields, insert data where int64_1 == int64_2
                2. search with filter "int64_1 <= int64_2", verify all rows match
        expected: all rows returned since int64_1 always equals int64_2
        """
        nb = ct.default_nb
        dim = 2
        nq = 1
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field("int64_1", DataType.INT64, is_primary=True)
        schema.add_field("int64_2", DataType.INT64)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        all_vectors = cf.gen_vectors(nb, dim)
        data = [{"int64_1": i, "int64_2": i,
                 ct.default_float_vec_field_name: all_vectors[i]} for i in range(nb)]
        insert_ids = list(range(nb))
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        filter_ids = []
        for i in range(nb):
            if data[i]["int64_1"] <= data[i]["int64_2"]:
                filter_ids.append(data[i]["int64_1"])

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        expression = "int64_1 <= int64_2"
        search_vectors = cf.gen_vectors(nq, dim)
        res, _ = self.search(client, collection_name,
                             data=search_vectors[:nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": nq,
                                          "ids": insert_ids,
                                          "limit": default_limit,
                                          "metric": "COSINE",
                                          "enable_milvus_client_api": True,
                                          "pk_name": "int64_1"})
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = [hit["int64_1"] for hit in hits]
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: verify search with varchar filter containing escaped double quotes returns exact match
        method: 1. insert data with varchar values containing single and double quotes
                2. search with escaped double-quote filter for a random row
                3. verify exactly 1 result returned with correct PK
        expected: exact match on the escaped string, returns the correct single row
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        string_value = [(f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\""
                         f"{cf.gen_str_by_length(3)}\"") for _ in range(default_nb)]
        for i in range(default_nb):
            data[i][default_string_field_name] = string_value[i]
        insert_ids = [i for i in range(default_nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        _id = random.randint(0, default_nb - 1)
        string_value[_id] = string_value[_id].replace("\"", "\\\"")
        expression = f"{default_string_field_name} == \"{string_value[_id]}\""
        log.debug("test_search_expression_with_double_quotes: searching with expression: %s" % expression)
        search_vectors = cf.gen_vectors(default_nq, default_dim)
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": default_nq,
                                                 "ids": insert_ids,
                                                 "limit": 1,
                                                 "metric": "COSINE",
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name})
        assert search_res[0][0][ct.default_int64_field_name] == _id

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_concurrent_two_collections_nullable(self):
        """
        target: verify concurrent search across two collections with all-null JSON field is thread-safe
        method: 1. create two collections with nullable JSON, insert data with all JSON=None
                2. launch 10 threads searching collection_1 concurrently
                3. verify each thread gets correct results via check_task
        expected: all concurrent searches succeed with correct nq/limit
        """
        nq = 200
        dim = 64
        enable_dynamic_field = False
        threads_num = 10
        threads = []

        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"

        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name_1, schema=schema)
        self.create_collection(client, collection_name_2, schema=schema)

        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        for row in data:
            row[ct.default_json_field_name] = None
        insert_res_1, _ = self.insert(client, collection_name_1, data=data)
        insert_ids = insert_res_1["ids"]
        self.insert(client, collection_name_2, data=data)
        self.flush(client, collection_name_1)
        self.flush(client, collection_name_2)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name_1, index_params=idx)
        self.create_index(client, collection_name_2, index_params=idx)
        self.load_collection(client, collection_name_1)
        self.load_collection(client, collection_name_2)

        def search(coll_name):
            search_vectors = cf.gen_vectors(nq, dim)
            self.search(client, coll_name,
                        data=search_vectors[:nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": nq,
                                     "ids": insert_ids,
                                     "limit": default_limit,
                                     "metric": "COSINE",
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

        log.info("test_search_concurrent_two_collections_nullable: searching with %s threads" % threads_num)
        for _ in range(threads_num):
            t = threading.Thread(target=search, args=(collection_name_1,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["JACCARD", "HAMMING"])
    def test_search_with_invalid_metric_type(self, metric_type):
        """
        target: verify creating index with unsupported metric type for float vector raises error
        method: 1. create collection with float_vector field
                2. attempt to create index with JACCARD/HAMMING metric (unsupported for float vectors)
        expected: index creation raises an error with err_code 1100
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=100, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metric_type,
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx,
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 1100,
                                       "err_msg": f"float vector index does not support metric type: {metric_type}"})
