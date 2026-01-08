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
import numpy as np

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


@pytest.mark.xdist_group("TestMilvusClientSearchByPk")
@pytest.mark.tags(CaseLabel.GPU)
class TestMilvusClientSearchByPk(TestMilvusClientV2Base):
    """Test search by primary keys functionality"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestMilvusClientSearchByPk" + cf.gen_unique_str("_")
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
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.bfloat16_vector_field_name)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_by_pk_float_vectors(self):
        """
        target: test search by primary keys float vectors
        method: 1. connect and create a collection
                2. search by primary keys float vectors
                3. verify search by primary keys results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with inserted vectors
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
    def test_search_by_pk_bfloat16_vectors(self):
        """
        target: test search by primary keys bfloat16 vectors
        method: 1. connect and create a collection
                2. search by primary keys bfloat16 vectors
                3. verify search by primary keys results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with bfloat16 vectors
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.bf16_vector_metric, "params": {}}

        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
    def test_search_by_pk_sparse_vectors(self):
        """
        target: test search by primary keys sparse vectors
        method: 1. connect and create a collection
                2. search by primary keys sparse vectors
                3. verify search by primary keys results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with sparse vectors
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.sparse_vector_metric, "params": {}}

        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
        error = {"err_code": 999,
                 "err_msg": "multiple vector fields exist, please specify anns_field in search_params"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_nullable_vector_field(self):
        """
        target: test search by pk with nullable vector field where some vectors are null
        method: 1. create a collection with nullable sparse vector field
                2. insert data where some vectors are null
                3. search by IDs including some with null vectors
                4. verify result count equals non-null vector count (effective nq)
        expected: null vectors are filtered out, result count = non-null vector count
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nullable_vec_search_")

        # Create collection with nullable sparse vector field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("sparse_vec", DataType.SPARSE_FLOAT_VECTOR, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data: 10 rows, where rows 2, 5, 8 have null vectors
        nb = 10
        null_indices = {2, 5, 8}
        rows = []
        for i in range(nb):
            if i in null_indices:
                row = {"id": i, "sparse_vec": None}
            else:
                row = {"id": i, "sparse_vec": {i: 1.0, i + 100: 0.5}}
            rows.append(row)

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("sparse_vec", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # Case 1: Search by IDs with mixed null and non-null vectors
        # IDs [0, 2, 3, 5] -> 0, 3 are valid, 2, 5 are null
        ids_to_search = [0, 2, 3, 5]
        expected_nq = 2  # only 2 non-null vectors

        res = self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="sparse_vec",
            search_params={"metric_type": "IP"},
            limit=5
        )[0]

        log.info(f"Search with ids={ids_to_search}, expected_nq={expected_nq}, actual len(res)={len(res)}")
        assert len(res) == expected_nq, f"Expected {expected_nq} results, got {len(res)}"

        # Case 2: Search by IDs with all null vectors should raise error
        all_null_ids = [2, 5, 8]
        error = {"err_code": 65535,
                 "err_msg": "all provided IDs have null vector values"}
        self.search(
            client,
            collection_name,
            ids=all_null_ids,
            anns_field="sparse_vec",
            search_params={"metric_type": "IP"},
            limit=5,
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_by_pk_with_empty_ids(self):
        """
        target: test search by primary keys with empty ids
        method: 1. connect and create a collection
                2. search by primary keys with empty ids
        expected: search successfully with 0 results
        """
        client = self._client()
        collection_name = self.collection_name

        # search with empty vectors
        error = {"err_code": 999, "err_msg": "`ids` value [] is illegal"}
        self.search(
            client,
            collection_name,
            ids=[],
            anns_field=self.sparse_vector_field_name,
            search_params={},
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_by_pk_binary_vectors(self):
        """
        target: test search by primary keys binary vectors
        method: 1. connect and create a collection
                2. search by primary keys binary vectors
                3. verify search by primary keys results
        expected: search successfully and results are correct
        """
        client = self._client()
        collection_name = self.collection_name

        # Search with binary vectors
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.binary_vector_metric, "params": {"nprobe": 100}}

        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
    def test_search_by_pk_with_duplicate_pk_values(self):
        """
        target: test search by primary keys with duplicate primary key values
        method: 1. connect and create a collection
                2. search by primary keys with duplicate primary key values
                3. verify search by primary keys results
        expected: search failed with error
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        ids_to_search = [1, 2, 1]
        search_params = {}

        # search with duplicate primary key values
        # TODO: Update the error msg after #46740 fixed
        error = {"err_code": 999,
                 "err_msg": "duplicate IDs found in search request"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_both_ids_and_vectors(self):
        """
        target: test search by primary keys with both primary key values and vectors
        method: 1. connect and create a collection
                2. search by primary keys with both primary key values and vectors
                3. verify search by primary keys results
        expected: search failed with error
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate ids and vectors to search
        ids_to_search = [1, 2]
        search_params = {}
        vectors_to_search = cf.gen_vectors(nb=2, dim=self.float_vector_dim)

        # search with duplicate primary key values
        error = {"err_code": 999,
                 "err_msg": "Either ids or data must be provided, not both"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            data=vectors_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_mismatched_id_type(self):
        """
        target: test search by primary keys with mismatched primary key type
        method: 1. connect and create a collection
                2. search by primary keys with mismatched primary key type
                3. verify search by primary keys results
        expected: search failed with error
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        ids_to_search = ['1', '2']
        search_params = {}

        # search with duplicate primary key values
        error = {"err_code": 999,
                 "err_msg": "primary key is int64, but IDs type mismatch"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit, nq", zip([1, 1000, ct.max_limit], [ct.max_nq, 10, 1]))
    # @pytest.mark.parametrize("limit, nq", zip([ct.max_limit], [1]))
    def test_search_by_pk_with_different_nq_limits(self, limit, nq):
        """
        target: test search by primary keys with different nq and limit values
        method: 1. connect and create a collection
                2. search by primary keys with different nq and limit values
                3. verify search by primary keys results
        expected: search successfully with different nq and limit values
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 128}}

        # search with limit
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
    def test_search_by_pk_with_output_fields_and_consistency_level(self, consistency_level):
        """
        target: test search by primary keys with output fields
        method: 1. connect and create a collection
                2. search by primary keys with output fields
        expected: search by primary keys successfully with output fields
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            consistency_level=consistency_level,
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
    def test_search_by_pk_with_output_fields_all(self):
        """
        target: test search by primary keys with output all fields
        method: 1. connect and create a collection
                2. search by primary keys with output all fields
        expected: search by primary keys successfully with output all fields
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]
        fields = collection_info.get('fields', None)
        field_names = [field.get('name') for field in fields]

        # Generate vectors to search
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", ct.default_primary_field_name],
                                                        ["*", ct.default_float_vec_field_name]])
    def test_search_by_pk_partition_with_output_fields(self, wildcard_output_fields):
        """
        target: test partition search by primary keys with output fields
        method: 1. connect to milvus 
                2. partition search on an existing collection with output fields
        expected: search by primary keys successfully with output fields
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]
        partition_name = self.partition_names[0]

        # Generate vectors to search
        ids_to_search = [1, 4]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with output fields
        expected_outputs = cf.get_wildcard_output_field_names(collection_info, wildcard_output_fields)
        expected_outputs.extend([self.dyna_filed_name1, self.dyna_filed_name2])
        log.info(f"search with output fields: {wildcard_output_fields}")
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            partition_names=[partition_name],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            output_fields=wildcard_output_fields,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": len(ids_to_search),
                         "pk_name": self.pk_field_name,
                         "limit": default_limit,
                         "output_fields": expected_outputs})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_invalid_output_fields(self):
        """
        target: test search by primary keys with output fields using wildcard
        method: search by primary keys with one output_field (wildcard)
        expected: search by primary keys successfully
        """
        client = self._client()
        collection_name = self.collection_name
        collection_info = self.describe_collection(client, collection_name)[0]

        # Generate vectors to search
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {}
        invalid_output_fields = [["%"], [""], ["-"]]
        for field in invalid_output_fields:
            error1 = {ct.err_code: 999, ct.err_msg: f"parse output field name failed: {field[0]}"}
            error2 = {ct.err_code: 999, ct.err_msg: f"`output_fields` value {field} is illegal"}
            error = error2 if field == [""] else error1
            self.search(client, collection_name, ids=ids_to_search,
                        anns_field=self.float_vector_field_name,
                        search_params=search_params,
                        limit=default_limit,
                        output_fields=field,
                        check_task=CheckTasks.err_res, check_items=error)

        # verify non-exist field as output field is valid as dynamic field enabled
        self.search(client, collection_name, ids=ids_to_search,
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
    def test_search_by_pk_with_more_than_max_limit(self):
        """
        target: test search by primary keys with more than max limit
        method: 1. connect and create a collection
                2. search by primary keys with more than max limit
        expected: search by primary keys successfully with more than max limit
        """
        client = self._client()
        collection_name = self.collection_name

        # Generate vectors to search
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        error = {"err_code": 999, "err_msg": f"topk [{ct.max_limit + 1}] is invalid, it should be in range " \
                                             f"[1, {ct.max_limit}], but got {ct.max_limit + 1}"}
        # search with more than max limit
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=ct.max_limit + 1,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_more_than_max_nq(self):
        """
        target: test search by primary keys with more than max nq
        method: 1. connect and create a collection
                2. search by primary keys with more than max nq
        expected: search by primary keys successfully with more than max nq
            """
        client = self._client()
        collection_name = self.collection_name
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(ct.max_nq + 1)]
        search_params = {"metric_type": self.sparse_vector_metric}

        error = {"err_code": 999,
                 "err_msg": f"nq [{ct.max_nq + 1}] is invalid, nq (number of search vector per search request) should be in range " \
                            f"[1, {ct.max_nq}], but got {ct.max_nq + 1}"}
        # search with more than max nq
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.sparse_vector_field_name,
            search_params=search_params,
            limit=ct.max_nq + 1,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_concurrent_threads(self):
        """
        target: test search by primary keys with concurrent threads
        method: 1. connect and create a collection
                2. search by primary keys with concurrent threads
        expected: search by primary keys successfully with concurrent threads
        """
        client = self._client()
        collection_name = self.collection_name
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
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
                    ids=ids_to_search,
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
    def test_search_by_pk_with_client_closed(self):
        """
        target: test search by primary keys with client closed
        method: 1. connect and create a collection
                2. search by primary keys with client closed
        expected: search by primary keys successfully with client closed
        """
        client = self._client()
        collection_name = self.collection_name
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # close client
        client.close()

        # search with client closed
        error = {"err_code": 999, "err_msg": "should create connection first"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_dismatched_metric_type(self):
        """
        target: test search by primary keys with dismatched metric type
        method: 1. connect and create a collection
                2. search by primary keys with dismatched metric type
        expected: search by primary keys successfully with dismatched metric type
        """
        client = self._client()
        collection_name = self.collection_name
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.sparse_vector_metric, "params": {"nprobe": 100}}

        # search with dismatched metric type
        error = {"err_code": 999,
                 "err_msg": "metric type not match: invalid parameter[expected=COSINE][actual=IP]"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["*", "non_exist_partition", "par*"])
    def test_search_by_pk_with_invalid_partition_name(self, partition_name):
        """
        target: test search by primary keys with invalid partition name
        method: 1. connect and create a collection
                2. search by primary keys with invalid partition name
        expected: search by primary keys successfully with invalid partition name
        """
        client = self._client()
        collection_name = self.collection_name
        ids_to_search = [self.datas[i][self.pk_field_name] for i in range(default_nq)]
        search_params = {"metric_type": self.float_vector_metric, "params": {"nprobe": 100}}

        # search with invalid partition name
        error = {"err_code": 999, "err_msg": f"partition name {partition_name} not found"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            partition_names=[partition_name],
            check_task=CheckTasks.err_res,
            check_items=error
        )


class TestSearchByPkIndependent(TestMilvusClientV2Base):
    """Test search by primary keys functionality with independent collections"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_dense_vectors_indices_metrics_growing(self):
        """
        target: test search by primary keys with different dense vector types, indices and metrics growing
        method: create connection, collection, insert data and search by primary keys
        expected: search by primary keys successfully
        """
        # basic search on dense vectors,
        # TODO: indices and metrics are covered in test_search_pagination_dense_vectors_indices_metrics_growing
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_on_empty_partition(self):
        """
        target: test search by primary keys on empty partition
        method: create connection, collection, insert data and search by primary keys
        expected: search by primary keys successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # create partition
        partition_name = "empty_partition"
        self.create_partition(client, collection_name, partition_name=partition_name)

        # search
        ids_to_search = [0, 1]
        error = {"err_code": 999,
                 "err_msg": f"some of the provided primary key IDs do not exist: missing IDs"}
        search_params = {}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field='vector',
            search_params=search_params,
            limit=default_limit,
            partition_names=[partition_name],
            check_task=CheckTasks.err_res,
            check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_cosine_results_same_as_l2_and_ip(self):
        """
        target: test search by primary keys cosine results same as l2 and ip
        method: create connection, collection, insert data and search by primary keys
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
        ids_to_search = [data[i][ct.default_primary_field_name] for i in range(default_nq)]
        search_params = {}
        search_res_cosine, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
            ids=ids_to_search,
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
            ids=ids_to_search,
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
    def test_search_by_pk_with_duplicate_primary_key(self):
        """
        target: test search by primary keys with duplicate primary key
        method: create connection, collection, insert data and search by primary keys
        expected: search by primary keys successfully
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
        ids_to_search = [data[i]['id'] for i in range(default_nq)]
        search_params = {}
        search_res, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "pk_name": "id",
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
        error = {"err_code": 999, "err_msg": "collection not loaded"}
        ids_to_search = [data[i]['id'] for i in range(default_nq)]
        search_params = {}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
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
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": default_nq,
                         "pk_name": "id",
                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_after_partition_release(self):
        """
        target: test search by primary keys after partition release     
        method: 1. create connection, collection, insert data and search
                2. release a partition
                3. search again
                4. load the released partition and search again
                5. release the partition again and load the collection
                6. search again
        expected: search by primary keys successfully and search results are correct
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
                    'id': pk,
                    'vector': float_vectors[pk]
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
        self.wait_for_index_ready(client, collection_name, index_name='vector')

        # search in the collection
        ids_to_search = [0, 1, 2, 3, 4, 5, 6, 7]
        limit = 100
        search_params = {}
        search_res1, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": len(ids_to_search),
                         "pk_name": "id",
                         "limit": limit})

        # release partition1
        self.release_partitions(client, collection_name, ["partition_1"])

        # search again with error for some ids in partition_1 was released
        error = {ct.err_code: 100,
                 ct.err_msg: f"some of the provided primary key IDs do not exist: missing IDs = [1 4 7]"}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.err_res,
            check_items=error)

        # search in the non-released partitions
        ids_to_search = [0, 2, 3, 5, 6]
        search_res3, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            partition_names=[ct.default_partition_name, "partition_2"],
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": len(ids_to_search),
                         "pk_name": "id",
                         "limit": limit})

        # load the released partition and search again
        self.load_partitions(client, collection_name, ["partition_1"])
        ids_to_search = [0, 1, 2, 3, 4, 5, 6, 7]
        search_res4, _ = self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": len(ids_to_search),
                         "pk_name": "id",
                         "limit": limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.max_dim, ct.min_dim])
    def test_search_by_pk_max_and_min_dim(self, dim):
        """
        target: test search by primary keys with max and min dimension collection
        method: create connection, collection, insert and search by primary keys with max and min dimension
        expected: search by primary keys successfully with limit(topK)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # fast create collection
        self.create_collection(client, collection_name, dimension=dim)

        # insert data
        data = []
        nb = 200
        for i in range(nb):
            data.append({
                "id": i,
                "vector": cf.gen_vectors(1, dim)[0]
            })
        self.insert(client, collection_name, data)

        # search
        ids_to_search = [i for i in range(ct.default_nq)]
        search_params = {}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_after_recreate_index(self):
        """
        target: test search by primary keys after recreate index
        method: create connection, collection, insert and search by primary keys after recreate index
        expected: search by primary keys successfully with limit(topK)
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
        ids_to_search = [i for i in range(ct.default_nq)]
        search_params = {}
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
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

        # search
        self.search(
            client,
            collection_name,
            ids=ids_to_search,
            anns_field="vector",
            search_params=search_params,
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"enable_milvus_client_api": True,
                         "nq": ct.default_nq,
                         "pk_name": "id",
                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:6])
    def test_search_by_pk_each_index_with_mmap_enabled_search(self, index):
        """
        target: test search by primary keys each index with mmap enabled search
        method: test search by primary keys each index with mmap enabled search
        expected: search by primary keys successfully
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
        data = []
        for i in range(ct.default_nb):
            data.append({
                "id": i,
                "vector": cf.gen_vectors(1, dim)[0]
            })
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
        ids_to_search = [i for i in range(ct.default_nq)]
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id"})
        # disable mmap
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": False})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'False'
        self.load_collection(client, collection_name)
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[8:10])
    def test_search_by_pk_enable_mmap_search_for_binary_indexes(self, index):
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
        data = []
        for i in range(ct.default_nb):
            data.append({
                "id": i,
                "vector": cf.gen_binary_vectors(1, dim)[1][0]
            })
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
        ids_to_search = [i for i in range(ct.default_nq)]
        params = cf.get_search_params_params(index)
        search_params = {"metric_type": "JACCARD", "params": params}
        output_fields = ["*"]
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id"})
        # disable mmap
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, index_name='vector', properties={"mmap.enabled": False})
        index_info = self.describe_index(client, collection_name, index_name='vector')
        assert index_info[0]["mmap.enabled"] == 'False'
        self.load_collection(client, collection_name)
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id"})
    
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("num_shards", [-256, 0, ct.max_shards_num // 2, ct.max_shards_num])
    def test_search_by_pk_with_non_default_shard_nums(self, num_shards):
        """
        Test search by primary keys functionality with non-default shard numbers.
        
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
        data = []
        for i in range(ct.default_nb):
            data.append({
                "id": i,
                "vector": cf.gen_vectors(1, dim)[0]
            })
        self.insert(client, collection_name, data)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name='vector', index_type='HNSW', metric_type='COSINE')
        self.create_index(client, collection_name, index_params=index_params)
        self.wait_for_index_ready(client, collection_name, index_name='vector')
        # load
        self.load_collection(client, collection_name)
        # search
        ids_to_search = [i for i in range(ct.default_nq)]
        search_params = {}
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id"})
    
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('vector_dtype', ct.all_dense_vector_types)
    @pytest.mark.parametrize('index', ct.all_index_types[:8])
    def test_search_by_pk_output_field_vector_with_dense_vector_and_index(self, vector_dtype, index):
        """
        Test search by primary keys with output vector field after different index types.
        
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
        schema.add_field('vector', vector_dtype, dim=dim)
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
                "vector": random_vectors[i + start_pk],
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
            self.create_index(client, collection_name, index_params=index_params)

            # load the collection with index
            assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=120)
            self.load_collection(client, collection_name)

            # search with output field vector
            search_params = {}
            ids_to_search = [i for i in range(ct.default_nq)]
            # search output all fields
            self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                        search_params=search_params, limit=ct.default_limit,
                        output_fields=["*"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": ct.default_limit,
                                     "output_fields": ["id", "vector", "float_array", "json_field", "string_field"]})
            # search output specify all fields
            self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                        search_params=search_params, limit=ct.default_limit,
                        output_fields=["id", "vector", "float_array", "json_field", "string_field"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": ct.default_limit,
                                     "output_fields": ["id", "vector", "float_array", "json_field", "string_field"]})
            # search output specify some fields
            self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                        search_params=search_params, limit=ct.default_limit,
                        output_fields=["id", "vector", "json_field"],
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "pk_name": "id",
                                     "nq": ct.default_nq,
                                     "limit": ct.default_limit,
                                     "output_fields": ["id", "vector", "json_field"]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('index', ct.binary_supported_index_types)
    def test_search_by_pk_with_output_fields_vector_with_binary_vector_and_index(self, index):
        """
        Test search by primary keys functionality with output fields for binary vector type and specified index.
        
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
        schema.add_field("vector", datatype=vector_dtype, dim=dim)
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
                "vector": random_vectors[i + start_pk]
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
        ids_to_search = [i for i in range(ct.default_nq)]
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=["*"],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "output_fields": ["id", "vector"]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_with_output_fields_empty(self):
        """
        target: test search by primary keys with output fields
        method: search by primary keys with empty output_field
        expected: search success
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        # create collection with fast mode
        self.create_collection(client, collection_name, dimension=dim)
        # insert data
        data = []
        for i in range(ct.default_nb):
            data.append({
                "id": i,
                "vector": cf.gen_vectors(1, dim)[0]
            })
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)
        # search with empty output fields
        search_params = {}
        ids_to_search = [i for i in range(ct.default_nq)]
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=[],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit})
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    output_fields=None,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "pk_name": "id",
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[1:8])
    def test_search_by_pk_repeatedly_with_different_index(self, index):
        """
        Test searching by primary keys repeatedly with different index types to ensure consistent results.

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
        ids_to_search = [i for i in range(ct.default_nq)]
        res1 = self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                           search_params=search_params, limit=limit)[0]
        res2 = self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                           search_params=search_params, limit=limit * 2)[0]
        for i in range(ct.default_nq):
            assert res1[i].ids == res2[i].ids[:limit]
        # search again with the previous limit
        res3 = self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                           search_params=search_params, limit=limit)[0]
        for i in range(ct.default_nq):
            assert res1[i].ids == res3[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="too slow and memory cost in e2e")
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:1])
    @pytest.mark.parametrize("index", ["BIN_IVF_FLAT"])
    @pytest.mark.parametrize("dim", [ct.max_binary_vector_dim])
    def test_search_by_pk_binary_indexed_large_dim_vectors_search(self, dim, metrics, index):
        """
        target: test search by primary keys binary vector large dim search
        method: binary vector large dim search
        expected: search by primary keys successfully
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
        ids_to_search = [2]
        res = self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
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
    def test_search_by_pk_verify_expr_cache(self):
        """
        target: test search by primary keys case to test expr cache
        method: 1. create collection with a double datatype field and search by primary keys
                2. search by primary keys with expr "doubleField == 0"
                3. drop this collection
                4. create collection with same collection name and same field name but modify the type of double field
                   as varchar datatype and search by primary keys
                5. search by primary keys with expr "doubleField == 0" again
        expected: 1. search by primary keys successfully with limit(topK) for the first collection
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
        ids_to_search = [i for i in range(ct.default_nq)]
        # 3. search with expr "expr_field == 0"
        search_params = {}
        search_exp = "expr_field >= 0"
        output_fields = ["id", "expr_field", "double_field"]
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": "id",
                                 "output_fields": output_fields})
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
        self.search(client, collection_name, ids=ids_to_search, anns_field="vector",
                    search_params=search_params, limit=ct.default_limit,
                    filter=search_exp,
                    output_fields=output_fields,
                    check_task=CheckTasks.err_res,
                    check_items={"err_code": 1100,
                                 "err_msg": "error: comparisons between VarChar and Int64 are not supported"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_by_pk_using_all_types_of_default_value(self):
        """
        target: test search by primary keys create collection with default_value
        method: create a schema with all fields using default value and search by primary keys
        expected: search by primary keys results are as expected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        schema = self.create_schema(client)[0]
        self.add_field(schema, field_name='pk', datatype=DataType.INT64, is_primary=True)
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
        ids_to_search = [i for i in range(ct.default_nq)]
        res = self.search(client, collection_name, ids=ids_to_search,
                          anns_field=ct.default_float_vec_field_name,
                          search_params={},
                          limit=ct.default_limit,
                          output_fields=["*"],
                          check_task=CheckTasks.check_search_results,
                          check_items={"enable_milvus_client_api": True,
                                       "nq": ct.default_nq,
                                       "pk_name": "pk",
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
    def test_search_by_pk_ignore_growing(self):
        """
        target: test search by primary keys ignoring growing segment
        method: 1. create a collection, insert data by primary keys, create index and load
                2. insert data again
                3. search ignore_growing=True, inside/outside search params
        expected: searched by primary keys successfully
        """
        # 1. create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        schema = self.create_schema(client)[0]
        self.add_field(schema, field_name='pk', datatype=DataType.INT64, is_primary=True)
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
        ids_to_search = [i for i in range(ct.default_nq)]
        res1 = self.search(client, collection_name, ids=ids_to_search,
                           anns_field=ct.default_float_vec_field_name,
                           search_params=search_params,
                           limit=ct.default_limit,
                           ignore_growing=True,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": ct.default_nq,
                                        "limit": ct.default_limit,
                                        "pk_name": "pk"})[0]
        search_params = {"ignore_growing": True}
        res2 = self.search(client, collection_name, ids=ids_to_search,
                           anns_field=ct.default_float_vec_field_name,
                           search_params=search_params,
                           limit=ct.default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"enable_milvus_client_api": True,
                                        "nq": ct.default_nq,
                                        "limit": ct.default_limit,
                                        "pk_name": "pk"})[0]
        for i in range(ct.default_nq):
            assert max(res1[i].ids) < ct.default_nb * 5
            assert max(res2[i].ids) < ct.default_nb * 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_by_pk_with_json_fields_comparison_in_filter(self):
        """
        Test case for searching by primary keys with JSON fields comparison in filter.

        Target:
            Verify that searching by primary keys with JSON fields comparison in filter behaves as expected.

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
        self.add_field(schema, field_name='pk', datatype=DataType.INT64, is_primary=True)
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
