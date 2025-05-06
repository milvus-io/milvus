import logging
import numpy as np
from common.constants import *
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import pytest
import pandas as pd
from faker import Faker
from pymilvus import ConsistencyLevel

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'


default_vector_field_name = "vector"


@pytest.mark.xdist_group("TestMilvusClientSearchBasicV2")
@pytest.mark.tags(CaseLabel.GPU)
class TestMilvusClientSearchBasicV2(TestMilvusClientV2Base):
    """Test search functionality with new client API"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestMilvusClientSearchV2" + cf.gen_unique_str("_")
        self.partition_names = ["partition_1", "partition_2"]
        self.pk_field_name = ct.default_primary_field_name
        self.float_vector_field_name = "float_vector"
        self.bfloat16_vector_field_name = "bfloat16_vector" 
        self.sparse_vector_field_name = "sparse_vector"
        self.binary_vector_field_name = "binary_vector"
        self.float_vector_dim = 128
        self.bf16_vector_dim = 200
        self.binary_vector_dim = 256  
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
        self.dyna_filed_name1 = "dyna_filed_name1"
        self.dyna_filed_name2 = "dyna_filed_name2"
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
                    self.dyna_filed_name1: f"dyna_value_{pk}",
                    self.dyna_filed_name2: pk * 1.0
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
                         "metric": self.float_vector_metric
                         }
        ) 
        # verfiy the top 1 hit is itself, so the min distance is 0
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
            vectors_to_search[:default_nq],
            anns_field=self.bfloat16_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
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
            vectors_to_search[:default_nq],
            anns_field=self.sparse_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.sparse_vector_metric
                         }
        )

        #  search again without specify anns_field
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.sparse_vector_metric
                         }
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
        search_res, _ = self.search(
            client,
            collection_name,
            data=[],
            anns_field=self.sparse_vector_field_name,
            search_params={},
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 0, "limit": 0,
                         }
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
            vectors_to_search[:default_nq],
            anns_field=self.binary_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
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
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}
        
        # search with limit
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": nq,
                         "limit": limit,
                         "metric": self.float_vector_metric
                         }
            ) 
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self):
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
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            output_fields=[ct.default_string_field_name, self.dyna_filed_name1, self.dyna_filed_name2],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.float_vector_metric,
                         "output_fields": [ct.default_string_field_name, self.dyna_filed_name1, self.dyna_filed_name2],
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
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            output_fields=["*"],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": self.float_vector_metric,
                         "output_fields": field_names.extend([self.dyna_filed_name1, self.dyna_filed_name2]),
                         "original_entities": self.datas,
                         "pk_name": self.pk_field_name
                         }
        )

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
            vectors_to_search[:default_nq],
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
        vectors_to_search = cf.gen_vectors(ct.max_nq + 1, dim=10, vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
        search_params = {"metric_type": self.sparse_vector_metric}
        
        error = {"err_code": 999, "err_msg": f"nq [{ct.max_nq + 1}] is invalid, it should be in range " \
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
        num_threads = 10
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(num_threads):
                future = executor.submit(
                    self.search,
                    client,
                    collection_name, 
                    vectors_to_search[:default_nq],
                    anns_field=self.float_vector_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                "nq": default_nq,
                                "limit": default_limit,
                                "metric": self.float_vector_metric
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
                         "metric": self.float_vector_metric}
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
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_dismatched_metric_type(self):
        """
        target: test search with dismatched metric type
        method: 1. connect and create a collection
                2. search with dismatched metric type
        expected: search successfully with dismatched metric type
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"metric_type": self.sparse_vector_metric, "params": {"nprobe": 100}}
        
        # search with dismatched metric type
        error = {"err_code": 999, "err_msg": "metric type mismatch"}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
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
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,    
            partition_names=[partition_name],
            check_task=CheckTasks.err_res,
            check_items=error
        )


class TestSearchV2Independent(TestMilvusClientV2Base):
    """Test search functionality with independent collections"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_dense_vectors_indices_metrics_growing(self):
        """
        target: test search with different dense vector types, indices and metrics
        method: create connection, collection, insert data and search
        expected: searched successfully
        """
        # basic search on dense vectors, indices and metrics are covered in test_search_pagination_dense_vectors_indices_metrics_growing
        pass

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
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            partition_names=[partition_name],
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": 0,
                         "ids": []})
                
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
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=256)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = []
        for i in range(default_nb):
            data.append({
                ct.default_primary_field_name: i,
                ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                ct.default_float_field_name: i * 1.0,
                ct.default_string_field_name: str(i)
            })
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
            vectors_to_search[:default_nq],
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'COSINE'}
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
            vectors_to_search[:default_nq],
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'L2'}
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
            vectors_to_search[:default_nq],
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit,
                         "metric": 'IP'}
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
        data = []
        for i in range(default_nb):
            data.append({
                "id": i if i % 2 == 0 else i + 1,
                "vector": cf.gen_vectors(1, dim)[0],
            })
        self.insert(client, collection_name, data)
        client.flush(collection_name)

        # search
        vectors_to_search = cf.gen_vectors(default_nq, dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
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
        data = []
        for i in range(default_nb):
            data.append({
                "id": i,
                "vector": cf.gen_vectors(1, ct.default_dim)[0],
            })
        self.insert(client, collection_name, data)  
        if flush:
            self.flush(client, collection_name)
            self.wait_for_index_ready(client, collection_name, index_name='vector')

        # release collection
        self.release_collection(client, collection_name)
        # search after release
        error = {"err_code": 999, "err_msg": "should load collection first"}
        vectors_to_search = cf.gen_vectors(default_nq, ct.default_dim)
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field="vector",
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
            vectors_to_search[:default_nq],
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
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
        float_vectors = cf.gen_vectors(ct.default_nb * insert_times, dim=self.float_vector_dim,
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
                    'id': pk,
                    'vector': float_vectors[pk]
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
                self.insert(client, collection_name, data=default_rows)
            if partition1_rows:
                self.insert(client, collection_name, data=partition1_rows, partition_name=partition_names[0])
            if partition2_rows:
                self.insert(client, collection_name, data=partition2_rows, partition_name=partition_names[1])
                            
        self.flush(client, collection_name)
        
        # search in the collection
        vectors_to_search = cf.gen_vectors(1, ct.default_dim)
        limit = ct.default_nb
        search_params = {}
        search_res1, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:1],
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "limit": limit})

        # find one result that not in default partition
        to_be_released_partition = None
        the_other_partition = None
        for i in range(limit):
            top_id = search_res1[0].ids[i]
            if top_id % 3 == 0:
                pass
            elif top_id % 3 == 1:
                to_be_released_partition = partition_names[0]
                the_other_partition = partition_names[1]
                break   
            else:
                to_be_released_partition = partition_names[1]
                the_other_partition = partition_names[0]
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
            vectors_to_search[:1],
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "limit": limit})
        # verify no results are from the released partition
        for i in range(limit):
            assert search_res2[0].ids[i] not in search_res1[0].ids
        
        # search in the non-released partitions
        search_res3, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:1],
            anns_field="vector",
            partition_names=[ct.default_partition_name, the_other_partition],
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "limit": limit})
        # verify the results are same as the 2nd search results
        assert search_res3 == search_res2
        
        # load the released partition and search again
        self.load_partitions(client, collection_name, [to_be_released_partition])
        search_res4, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:1],
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "limit": limit})   
        # verify the results are same as the first search results
        assert search_res4 == search_res1

        # release the partition again and laod the collection
        self.release_partitions(client, collection_name, [to_be_released_partition])
        self.load_collection(client, collection_name)

        # search again
        search_res5, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:1],
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": 1,
                         "limit": limit})
        # verify the results are same as the first search results
        assert search_res5 == search_res1   

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
    data = []
    for i in range(ct.default_nb):
        data.append({
            "id": i,    
            "vector": cf.gen_vectors(1, dim)[0]
        })
    self.insert(client, collection_name, data)

    # search
    vectors_to_search = cf.gen_vectors(ct.default_nq, dim)
    search_params = {}
    search_res, _ = self.search(
        client,
        collection_name,
        vectors_to_search[:ct.default_nq],
        anns_field="vector",
        search_params=search_params,
        limit=ct.default_limit,
        check_task=CheckTasks.check_search_results,
        check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
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
    data = []
    for i in range(ct.default_nb):
        data.append({
            "id": i,
            "vector": cf.gen_vectors(1, ct.default_dim)[0]
        })
    self.insert(client, collection_name, data)

    self.flush(client, collection_name)
    self.wait_for_index_ready(client, collection_name, index_name='vector')

    # search
    vectors_to_search = cf.gen_vectors(ct.default_nq, ct.default_dim)
    search_params = {}
    search_res, _ = self.search(
        client,
        collection_name,
        vectors_to_search[:ct.default_nq],
        anns_field="vector",
        search_params=search_params,
        limit=ct.default_limit,
        check_task=CheckTasks.check_search_results,
        check_items={"enable_milvus_client_api": True,
                     "nq": ct.default_nq,
                     "limit": ct.default_limit})

    # recreate index
    self.release_collection(client, collection_name)
    self.drop_index(client, collection_name, index_name='vector')

    index_params = {"index_type": "HNSW", "params": {"M": 8, "efConstruction": 128}, "metric_type": "L2"}
    self.create_index(client, collection_name, index_name='vector', index_params=index_params)

    self.wait_for_index_ready(client, collection_name, index_name='vector')
    self.load_collection(client, collection_name)

    # search
    search_res, _ = self.search(
        client,
        collection_name,
        vectors_to_search[:ct.default_nq],
        anns_field="vector",
        search_params=search_params,
        limit=ct.default_limit,
        check_task=CheckTasks.check_search_results,
        check_items={"enable_milvus_client_api": True,
                     "nq": ct.default_nq,
                     "limit": ct.default_limit})
    