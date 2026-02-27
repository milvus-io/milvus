import random
import time
import uuid as uuid_module

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType
from utils.util_log import test_log as log
from pymilvus.orm.types import CONSISTENCY_STRONG
import numpy as np

prefix = "add_field"
default_vector_field_name = "vector"
default_primary_key_field_name = "id"
default_string_field_name = "varchar"
default_float_field_name = "float"
default_new_field_name = "field_new"
default_dynamic_field_name = "field_new"
exp_res = "exp_res"
default_nb = 2000
default_dim = 128
default_limit = 10


class TestMilvusClientAddFieldFeature(TestMilvusClientV2Base):
    """Test cases for add field feature with CaseLabel.L0"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_collection_add_field(self):
        """
        target: test self create collection normal case about add field
        method: create collection with added field
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id_string", DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("title", DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field("nullable_field", DataType.INT64, nullable=True, default_value=10)
        schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=12,
                         max_length=64, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("embeddings", metric_type="COSINE")

        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        check_items = {"collection_name": collection_name,
                       "dim": dim,
                       "consistency_level": 0,
                       "enable_dynamic_field": False,
                       "num_partitions": 16,
                       "id_name": "id_string",
                       "vector_name": "embeddings"}
        self.add_collection_field(client, collection_name, field_name="field_new_int64", data_type=DataType.INT64,
                                  nullable=True, is_clustering_key=True, mmap_enabled=True)
        self.add_collection_field(client, collection_name, field_name="field_new_var", data_type=DataType.VARCHAR,
                                  nullable=True, default_vaule="field_new_var", max_length=64, mmap_enabled=True)
        check_items["add_fields"] = ["field_new_int64", "field_new_var"]
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items=check_items)
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['embeddings']
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index_type", ["HNSW", "IVF_FLAT", "IVF_SQ8", "IVF_RABITQ", "AUTOINDEX", "DISKANN"])
    def test_milvus_client_add_vector_field(self, index_type):
        """
        target: test add vector field
        method: create collection and add vector fields
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 32
        pk_name = default_primary_key_field_name
        vec_field_name = "embeddings"
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field_name, index_type=index_type, metric_type="COSINE")

        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)

        # verify failed to insert null vector for vector field nullable is false by default
        error = {ct.err_code: 999,
                 ct.err_msg: "float vector field 'embeddings' is illegal, "
                             "array type mismatch: invalid parameter[expected=need float vector][actual=got nil]"}
        rows = [{pk_name: i, vec_field_name: None} for i in range(10)]
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res, check_items=error)

        # insert some basic data
        basic_rows = cf.gen_row_data_by_schema(nb=ct.default_nb // 2, schema=schema)
        self.insert(client, collection_name, basic_rows)

        # add a new vector field with nullable=True
        new_vec_field_name = "embeddings_new"
        self.add_collection_field(client, collection_name, field_name=new_vec_field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=True)
        # insert data with null vector
        rows = [{
            pk_name: i + ct.default_nb // 2,
            vec_field_name: cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[0],
            new_vec_field_name: None if i % 2 == 0 else cf.gen_vectors(1, dim, vector_data_type=DataType.FLOAT_VECTOR)[
                0]
        } for i in range(ct.default_nb // 2)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # search on a not nullable vector field
        vectors_to_search = cf.gen_vectors(ct.default_nq, dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(client, collection_name, vectors_to_search,
                    anns_field=vec_field_name, limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_name})

        # query output all fields to check the new added nullable vector field is retrieved correctly
        query_pks = [0, 999, 1000, 1001, 1998, 1999]
        # back fill embeddings_new field value to None for basic rows
        basic_rows_with_null_vector = [row for row in basic_rows if row[pk_name] in query_pks]
        for row in basic_rows_with_null_vector:
            row[new_vec_field_name] = None
        rows_with_null_vector = [row for row in rows if row[pk_name] in query_pks]
        expect_rows = basic_rows_with_null_vector + rows_with_null_vector
        self.query(client, collection_name, limit=10,
                   filter=f"{pk_name} in {query_pks}",
                   output_fields=["*"],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": expect_rows,
                                "with_vec": True})

        # search on the new added null vector field fails for no reloading for it yet
        error = {ct.err_code: 999,
                 ct.err_msg: f"field {new_vec_field_name} is not loaded, please reload the collection"}
        self.search(client, collection_name, vectors_to_search,
                    anns_field=new_vec_field_name, limit=ct.default_limit,
                    check_task=CheckTasks.err_res, check_items=error)

        # release and reload collection
        self.release_collection(client, collection_name)
        # load fails for no index for the nullable vector field
        error = {ct.err_code: 999,
                 ct.err_msg: f"there is no vector index on field: [{new_vec_field_name}], please create index first"}
        self.load_collection(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)
        # create index and reload the collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(new_vec_field_name, index_type=index_type, metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # search on nullable vector field
        self.search(client, collection_name, vectors_to_search,
                    anns_field=new_vec_field_name, limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_name})
        # search by ids on nullable fields
        # PKs 0-999: basic rows, embeddings_new is null (backfilled)
        # PKs 1000-1999: embeddings_new is null if pk%2==0, valid vector if pk%2==1
        res = self.search(client, collection_name, ids=query_pks,
                          anns_field=new_vec_field_name, limit=ct.default_limit)[0]
        assert len(res) == len(query_pks)
        for i in range(len(query_pks)):
            if query_pks[i] >= 1000 and query_pks[i] % 2 == 1:
                assert len(res[i]) == ct.default_limit
            else:
                assert len(res[i]) == 0  # search on null vectors return empty results

        # insert more data and search on nullable vector field again
        rows_without_nullable_vector = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema, start=ct.default_nb)
        self.insert(client, collection_name, rows_without_nullable_vector)
        collection_info = self.describe_collection(client, collection_name)[0]
        rows_with_nullable_vector = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=collection_info,
                                                              start=ct.default_nb * 2)
        self.insert(client, collection_name, rows_with_nullable_vector)

        self.query(client, collection_name, filter="", output_fields=["count(*)"],
                   check_task=CheckTasks.check_query_results,
                   check_items={"count(*)": ct.default_nb * 3})
        self.search(client, collection_name, vectors_to_search,
                    anns_field=new_vec_field_name, limit=ct.default_limit,
                    consistency_level=CONSISTENCY_STRONG,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": ct.default_nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_name})

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("index_type", ["HNSW", "IVF_FLAT", "IVF_SQ8", "IVF_RABITQ", "AUTOINDEX", "DISKANN"])
    def test_milvus_client_add_vector_field_build_index_before_insert(self, index_type):
        """
        target: test add vector field and build index before insert
        method: 
        1. create collection, insert some data and build index
        2. add vector field and build index for new added vector field
        3. insert some data with null vector
        4. add one more vector field and index data
        5. build index for new added vector field
        6. load collection and search on the new added vector field
        expected: build index before and after adding new vector field successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 64
        pk_name = default_primary_key_field_name
        vec_field_name = "embeddings_0"
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(pk_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vec_field_name, DataType.FLOAT_VECTOR, dim=dim, nullable=True)
        self.create_collection(client, collection_name, dimension=dim, schema=schema)
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(vec_field_name, index_type=index_type, metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, vec_field_name)
        # 2. add vector field and build index for new added vector field
        new_vec_field_name = "embeddings_1"
        self.add_collection_field(client, collection_name, field_name=new_vec_field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(new_vec_field_name, index_type=index_type, metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, new_vec_field_name)
        # 3. insert some data with null vector
        new_collection_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=new_collection_info, start=ct.default_nb)
        self.insert(client, collection_name, rows)
        # 4. add one more vector field and index data
        new_vec_field_name_2 = "embeddings_2"
        self.add_collection_field(client, collection_name, field_name=new_vec_field_name_2,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=True)
        # 5. build index for new added vector field
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(new_vec_field_name_2, index_type=index_type, metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, new_vec_field_name_2)
        new_collection_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=new_collection_info, start=ct.default_nb * 2)
        self.insert(client, collection_name, rows)
        # 6. load collection and search on the new added vector field
        self.load_collection(client, collection_name)
        vectors_to_search = cf.gen_vectors(ct.default_nq, dim, vector_data_type=DataType.FLOAT_VECTOR)
        for name in [vec_field_name, new_vec_field_name, new_vec_field_name_2]:
            self.search(client, collection_name, vectors_to_search,
                        anns_field=name, limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": ct.default_nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_name})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_compact_with_added_field(self):
        """
        target: test clustering compaction with added field as cluster key
        method: create connection, collection, insert, add field, insert and compact
        expected: successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim, default_value = 128, 1
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i)} for i in range(10 * default_nb)]
        self.insert(client, collection_name, rows)
        # 3. add collection field
        self.add_collection_field(client, collection_name, field_name=default_new_field_name, data_type=DataType.INT64,
                                  nullable=True, is_clustering_key=True, default_value=default_value)
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        vectors_to_search = [vectors[0]]
        # 4. insert new field after add field
        rows_new = [
            {default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
             default_string_field_name: str(i), default_new_field_name: random.randint(1, 1000)}
            for i in range(10 * default_nb, 11 * default_nb)]
        self.insert(client, collection_name, rows_new)
        # 5. compact
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id, timeout=300)
        self.wait_for_index_ready(client, collection_name, default_vector_field_name)
        self.release_collection(client, collection_name)
        time.sleep(10)
        self.load_collection(client, collection_name)
        insert_ids = [i for i in range(10 * default_nb)]
        # 6. search with default value
        self.search(client, collection_name, vectors_to_search, filter=f'{default_new_field_name} == {default_value}',
                    output_fields=[default_new_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        insert_ids = [i for i in range(10 * default_nb, 11 * default_nb)]
        # 7. search with new data(no default value)
        self.search(client, collection_name, vectors_to_search,
                    filter=f'{default_new_field_name} != {default_value}',
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_insert_with_old_and_added_field(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert, add field, insert old/new field and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert before add field
        vectors = cf.gen_vectors(default_nb * 3, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. add new field
        self.add_collection_field(client, collection_name, field_name=default_new_field_name,
                                  data_type=DataType.VARCHAR,
                                  nullable=True, max_length=64)
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        # 4. check old dynamic data search is not impacted after add new field
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 5. insert data(old field)
        rows_old = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                     default_float_field_name: i * 1.0,
                     default_string_field_name: str(i)} for i in range(default_nb, default_nb * 2)]
        results = self.insert(client, collection_name, rows_old)[0]
        assert results['insert_count'] == default_nb
        insert_ids_with_old_field = [i for i in range(default_nb, default_nb * 2)]
        # 6. insert data(new field)
        rows_new = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                     default_float_field_name: i * 1.0, default_string_field_name: str(i),
                     default_new_field_name: default_new_field_name} for i in range(default_nb * 2, default_nb * 3)]
        results = self.insert(client, collection_name, rows_new)[0]
        assert results['insert_count'] == default_nb
        insert_ids_with_new_field = [i for i in range(default_nb * 2, default_nb * 3)]
        # 7. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter=f'field_new is null',
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids + insert_ids_with_old_field,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter=f"field_new=='field_new'",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids_with_new_field,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_upsert_with_added_field(self):
        """
        target: test upsert (high level api) normal case
        method: create connection, collection, insert, add field, upsert and search
        expected: upsert/search successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": default_dim,
                                              "consistency_level": 0})
        # 2. insert before add field
        vectors = cf.gen_vectors(default_nb * 3, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. add new field
        self.add_collection_field(client, collection_name, field_name=default_new_field_name,
                                  data_type=DataType.VARCHAR,
                                  nullable=True, max_length=64)
        half_default_nb = int(default_nb / 2)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i),
                 default_new_field_name: "default"} for i in range(half_default_nb)]
        results = self.upsert(client, collection_name, rows)[0]
        assert results['upsert_count'] == half_default_nb
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(half_default_nb)]
        insert_ids_with_new_field = [i for i in range(half_default_nb, default_nb)]
        # 4. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter=f'field_new is null',
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids_with_new_field,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter=f"field_new=='default'",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("new_field_name", [default_dynamic_field_name, "new_field"])
    def test_milvus_client_search_query_enable_dynamic_and_add_field(self, new_field_name):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert, add field(same as dynamic and different as dynamic) and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, max_length=64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        schema.add_field(default_float_field_name, DataType.FLOAT, nullable=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)
        # 2. insert
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0, default_string_field_name: str(i),
                 default_dynamic_field_name: 1} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb
        # 3. add new field same as dynamic field name
        default_value = 1
        self.add_collection_field(client, collection_name, field_name=new_field_name, data_type=DataType.INT64,
                                  nullable=True, default_value=default_value)
        vectors_to_search = [vectors[0]]
        insert_ids = [i for i in range(default_nb)]
        # 4. check old dynamic data search is not impacted after add new field
        self.search(client, collection_name, vectors_to_search, limit=default_limit,
                    filter=f'$meta["{default_dynamic_field_name}"] == 1',
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})
        # 5. check old dynamic data query is not impacted after add new field
        for row in rows:
            row[new_field_name] = default_value
        self.query(client, collection_name, filter=f'$meta["{default_dynamic_field_name}"] == 1',
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name,
                                "vector_type": DataType.FLOAT_VECTOR})
        # 6. search filtered with the new field
        self.search(client, collection_name, vectors_to_search,
                    filter=f"{new_field_name} == 1",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        self.search(client, collection_name, vectors_to_search,
                    filter=f"{new_field_name} is null",
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "pk_name": default_primary_key_field_name,
                                 "limit": 0})
        # 7. query filtered with the new field
        self.query(client, collection_name, filter=f"{new_field_name} == 1",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.query(client, collection_name, filter=f"{new_field_name} is null",
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: [],
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_add_field_with_analyzer(self):
        """
        target: test add field with analyzer configuration
        method: create collection, add field with standard analyzer,
                insert data and verify
        expected: successfully add field with analyzer and perform text search
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8

        # 1. create collection with basic schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)

        # 2. insert initial data before adding analyzer field
        schema_info = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb

        # 3. add new field with standard analyzer
        analyzer_params = {
            "type": "standard",
            "stop_words": ["for", "the", "is", "a"]
        }
        self.add_collection_field(client, collection_name, field_name="text_content", data_type=DataType.VARCHAR,
                                  nullable=True, max_length=1000, enable_analyzer=True, analyzer_params=analyzer_params,
                                  enable_match=True)

        # 4. insert data with the new analyzer field
        text_data = [
            "The Milvus vector database is built for scale",
            "This is a test document for analyzer",
            "Vector search with text analysis capabilities",
            "Database performance and scalability features"
        ]
        rows_with_analyzer = []
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        for i in range(default_nb, default_nb + len(text_data)):
            rows_with_analyzer.append({
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i - default_nb],
                default_string_field_name: str(i),
                "text_content": text_data[i - default_nb]
            })
        results = self.insert(client, collection_name, rows_with_analyzer)[0]
        assert results['insert_count'] == len(text_data)

        # 5. verify the analyzer field was added correctly
        collection_info = self.describe_collection(client, collection_name)[0]
        field_names = [field["name"] for field in collection_info["fields"]]
        assert "text_content" in field_names

        # 6. test text search using the analyzer field
        vectors_to_search = [vectors[0]]
        # Simple search without filter to verify basic functionality
        search_results = self.search(
            client, collection_name, vectors_to_search,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "limit": 10,  # Adjust limit to match actual results
                "pk_name": default_primary_key_field_name
            }
        )
        # Verify search returned some results
        assert len(search_results[0]) > 0

        # 7. test query with analyzer field - use simpler condition
        query_results = self.query(
            client, collection_name,
            filter="text_content is not null",
            check_task=CheckTasks.check_query_results,
            check_items={
                "pk_name": default_primary_key_field_name,
                "exp_limit": 4  # We expect 4 documents with text_content
            }
        )
        # Verify that we get results for documents with text_content
        assert len(query_results[0]) > 0

        # 8. test run_analyzer to verify the analyzer configuration
        sample_text = "The Milvus vector database is built for scale"
        analyzer_result = client.run_analyzer(sample_text, analyzer_params)
        # Verify analyzer produces tokens
        # (should remove stop words like "the", "is", "a")
        tokens = analyzer_result.tokens
        assert len(tokens) > 0
        # Handle different token formats - tokens might be strings or dictionaries
        if isinstance(tokens[0], str):
            token_texts = tokens
        else:
            token_texts = [token["token"] for token in tokens]
        # Check that stop words are filtered out
        assert "the" not in token_texts
        assert "is" not in token_texts
        assert "a" not in token_texts

        # 9. cleanup
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_add_field_and_update_existing_data(self):
        """
        target: test that updating existing data after adding a field works correctly in search
        method: create collection, insert data, load collection, add field, update existing data via upsert,
                then test query and search with the new field
        expected: 
            - Scalar query with uuid == "xxx" should work
            - Vector search with filter uuid == "xxx" should work (currently may show uuid as empty - bug)
            - uuid is null should not return all entries (currently returns all - bug)
            - uuid != "xxx" should work (currently may not work - bug)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 128
        nb_entries = 300
        uuid_field_name = "uuid"

        # 1. create collection
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)

        # 2. insert initial data (300 entries)
        vectors = cf.gen_vectors(nb_entries, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: vectors[i],
                 default_string_field_name: str(i)} for i in range(nb_entries)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == nb_entries

        # 3. flush and load collection
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 4. add new field "uuid"
        self.add_collection_field(client, collection_name, field_name=uuid_field_name,
                                  data_type=DataType.VARCHAR, nullable=True, max_length=64)

        # 5. update all existing 300 entries with uuid values using upsert
        # Generate unique uuid values for each entry
        uuid_values = {}
        rows_to_upsert = []
        for i in range(nb_entries):
            uuid_val = f"uuid_{i}_{uuid_module.uuid4().hex[:8]}"
            uuid_values[i] = uuid_val
            rows_to_upsert.append({
                default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_string_field_name: str(i),
                uuid_field_name: uuid_val
            })

        results = self.upsert(client, collection_name, rows_to_upsert)[0]
        assert results['upsert_count'] == nb_entries

        # Flush to ensure data is persisted
        self.flush(client, collection_name)

        # 6. Test scalar query with uuid == "xxx" - should return 1
        test_uuid = uuid_values[0]
        self.query(client, collection_name,
                   filter=f'{uuid_field_name} == "{test_uuid}"',
                   output_fields=["count(*)"],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [{"count(*)": 1}]})

        # 7. Test vector search with filter uuid == "xxx", should return 1 result
        vectors_to_search = [vectors[0]]
        self.search(client, collection_name, vectors_to_search,
                    filter=f'{uuid_field_name} == "{test_uuid}"',
                    output_fields=[default_primary_key_field_name, uuid_field_name],
                    limit=1,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": 1,
                                 "pk_name": default_primary_key_field_name})

        self.query(client, collection_name,
                   filter=f'{uuid_field_name} is null',
                   output_fields=["count(*)"],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [{"count(*)": 0}]})

        # 9. Test uuid != "xxx" - should return all -1
        test_uuid_neq = uuid_values[1]
        self.query(client, collection_name,
                   filter=f'{uuid_field_name} != "{test_uuid_neq}"',
                   output_fields=["count(*)"],
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [{"count(*)": nb_entries - 1}]})

        # 10. Test vector search with uuid != "xxx" filter, should return limit
        self.search(client, collection_name, vectors_to_search,
                    filter=f'{uuid_field_name} != "{test_uuid_neq}"',
                    output_fields=[default_primary_key_field_name, uuid_field_name],
                    limit=10,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": 10,
                                 "pk_name": default_primary_key_field_name})

        # 11. cleanup
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name)


class TestMilvusClientAddFieldFeatureInvalid(TestMilvusClientV2Base):
    """Test invalid cases for add field feature"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_vector_field_nullable_false(self):
        """
        target: test fast create collection with add vector field
        method: add vector field with nullable=False
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 999,
                 ct.err_msg: f"Adding vector field to existing collection requires nullable=True"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=False,
                                  check_task=CheckTasks.err_res, check_items=error)
        # try to add vector field without nullable param
        self.add_collection_field(client, collection_name, field_name=field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  check_task=CheckTasks.err_res, check_items=error)
        # try to add vector field with default value
        error = {ct.err_code: 999,
                 ct.err_msg: f"Default value unsupported data type: 999"}
        self.add_collection_field(client, collection_name, field_name=field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=True,
                                  default_value=cf.gen_vectors(1, dim)[0],
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_varchar_field_without_max_length(self):
        """
        target: test fast create collection with add varchar field without maxlength
        method: create collection name with add varchar field without maxlength
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"type param(max_length) should be specified for "
                                                f"the field({field_name}) of collection {collection_name}"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.VARCHAR,
                                  nullable=True, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_as_auto_id(self):
        """
        target: test fast create collection with add new field as auto id
        method: create collection name with add new field as auto id
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1, ct.err_msg: f"The auto_id can only be specified on the primary key field"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.INT64,
                                  nullable=True, auto_id=True, check_task=CheckTasks.err_res,
                                  check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_with_disable_nullable(self):
        """
        target: test fast create collection with add new field as nullable false
        method: create collection name with add new field as nullable false
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"added field must be nullable, please check it, "
                                                f"field name = {field_name}: invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.INT64,
                                  nullable=False, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_as_partition_ley(self):
        """
        target: test fast create collection with add new field as partition key
        method: create collection name with add new field as partition key
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"not support to add partition key field, "
                                                f"field name  = {field_name}: invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.INT64,
                                  nullable=True, is_partition_key=True,
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_exceed_max_length(self):
        """
        target: test fast create collection with add new field with exceed max length
        method: create collection name with add new field with exceed max length
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"the maximum length specified for the field({field_name}) "
                                                f"should be in (0, 65535], but got 65536 instead: invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.VARCHAR,
                                  nullable=True, max_length=65536, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_as_cluster_key(self):
        """
        target: test fast create collection with add new field as cluster key
        method: create collection with add new field as cluster key(already has cluster key)
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        field_name = default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"already has another clutering key field, "
                                                f"field name: {field_name}: invalid parameter"}
        schema = self.create_schema(client)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_clustering_key=True)

        self.create_collection(client, collection_name, schema=schema)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.INT64,
                                  nullable=True, is_clustering_key=True,
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_same_other_name(self):
        """
        target: test fast create collection with add new field as other same name
        method: create collection with add new field as other same name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        error = {ct.err_code: 1100, ct.err_msg: f"duplicate field name: {default_string_field_name}: invalid parameter"}
        schema = self.create_schema(client)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_clustering_key=True)

        self.create_collection(client, collection_name, schema=schema)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=default_string_field_name,
                                  data_type=DataType.VARCHAR, nullable=True, max_length=64,
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_field_exceed_max_field_number(self):
        """
        target: test fast create collection with add new field with exceed max field number
        method: create collection name with add new field with exceed max field number
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"The number of fields has reached the maximum value 64: "
                                                f"invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        for i in range(62):
            self.add_collection_field(client, collection_name, field_name=f"{field_name}_{i}",
                                      data_type=DataType.VARCHAR, nullable=True, max_length=64)
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.VARCHAR,
                                  nullable=True, max_length=64, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_vector_field_exceed_max_vector_field_number(self):
        """
        target: test fast create collection with add new vector field with exceed max vector field number
        method: create collection name with add new vector field with exceed max vector field number
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 999,
                 ct.err_msg: f"maximum vector field's number should be limited to {ct.max_vector_field_num}"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        for i in range(ct.max_vector_field_num - 1):
            self.add_collection_field(client, collection_name, field_name=f"{field_name}_{i}",
                                      data_type=DataType.FLOAT_VECTOR, dim=dim,
                                      nullable=True)
        self.add_collection_field(client, collection_name, field_name=field_name,
                                  data_type=DataType.FLOAT_VECTOR, dim=dim,
                                  nullable=True,
                                  check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_add_field_with_reranker_unsupported(self):
        """
        target: test that add_collection_field and decay ranker combination is not supported
        method: create collection without reranker field, add nullable reranker field via add_collection_field,
                then try to use it with decay ranker
        expected: raise exception because decay ranker requires non-nullable fields but add_collection_field
                 only supports nullable fields, creating a technical limitation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8

        # 1. create collection WITHOUT reranker field initially
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=64, is_partition_key=True)
        # Note: NO reranker field here - we'll try to add it later via add_collection_field

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, dimension=dim, schema=schema, index_params=index_params)

        # 2. insert initial data WITHOUT reranker field
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: vectors[i],
                 default_string_field_name: str(i)} for i in range(default_nb)]
        results = self.insert(client, collection_name, rows)[0]
        assert results['insert_count'] == default_nb

        # 3. Try to add nullable reranker field via add_collection_field (nullable must be True)
        # This will succeed in adding the field, but then we'll test if it can work with decay reranker
        # The conflict: add_collection_field only supports nullable fields, but decay reranker needs non-nullable fields
        self.add_collection_field(client, collection_name, field_name=ct.default_reranker_field_name,
                                  data_type=DataType.INT64, nullable=True, default_value=0)

        # 4. Insert data with the newly added reranker field
        # Generate new vectors for the second batch of data
        vectors_batch2 = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        rows_with_reranker = [
            {default_primary_key_field_name: i, default_vector_field_name: vectors_batch2[i - default_nb],
             default_string_field_name: str(i), ct.default_reranker_field_name: i}
            for i in range(default_nb, default_nb * 2)]
        results = self.insert(client, collection_name, rows_with_reranker)[0]
        assert results['insert_count'] == default_nb

        # 5. Try to use the nullable reranker field with decay reranker
        # This should fail because decay reranker requires non-nullable fields for proper functionality
        from pymilvus import Function, FunctionType
        my_rerank_fn = Function(
            name="my_reranker",
            input_field_names=[ct.default_reranker_field_name],
            function_type=FunctionType.RERANK,
            params={
                "reranker": "decay",
                "function": "gauss",
                "origin": 0,
                "offset": 0,
                "decay": 0.5,
                "scale": 100
            }
        )

        error = {ct.err_code: 65535, ct.err_msg: "Function input field cannot be nullable: field reranker_field"}
        self.search(client, collection_name, [vectors[0]], ranker=my_rerank_fn,
                    check_task=CheckTasks.err_res, check_items=error)
