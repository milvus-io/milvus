import random
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType
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
                                  nullable=True, is_cluster_key=True, mmap_enabled=True)
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
             default_string_field_name: str(i)} for i in range(10*default_nb)]
        self.insert(client, collection_name, rows)
        # 3. add collection field
        self.add_collection_field(client, collection_name, field_name=default_new_field_name, data_type=DataType.INT64,
                                  nullable=True, is_clustering_key=True, default_value=default_value)
        vectors = cf.gen_vectors(default_nb, dim, vector_data_type=DataType.FLOAT_VECTOR)
        vectors_to_search = [vectors[0]]
        # 4. insert new field after add field
        rows_new = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                     default_string_field_name: str(i), default_new_field_name: random.randint(1, 1000)}
                     for i in range(10*default_nb, 11*default_nb)]
        self.insert(client, collection_name, rows_new)
        # 5. compact
        compact_id = self.compact(client, collection_name)[0]
        self.wait_for_compaction_ready(client, compact_id, timeout=300)
        self.wait_for_index_ready(client, collection_name, default_vector_field_name)
        self.release_collection(client, collection_name)
        time.sleep(10)
        self.load_collection(client, collection_name)
        insert_ids = [i for i in range(10*default_nb)]
        # 6. search with default value
        self.search(client, collection_name, vectors_to_search, filter=f'{default_new_field_name} == {default_value}',
                    output_fields=[default_new_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        insert_ids = [i for i in range(10*default_nb, 11*default_nb)]
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
        self.add_collection_field(client, collection_name, field_name=default_new_field_name, data_type=DataType.VARCHAR,
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
        self.add_collection_field(client, collection_name, field_name=default_new_field_name, data_type=DataType.VARCHAR,
                                  nullable=True, max_length=64)
        half_default_nb = int (default_nb/2)
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


class TestMilvusClientAddFieldFeatureInvalid(TestMilvusClientV2Base):
    """Test invalid cases for add field feature"""
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_collection_add_vector_field(self):
        """
        target: test fast create collection with add vector field
        method: create collection name with add vector field
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        dim, field_name = 8, default_new_field_name
        error = {ct.err_code: 1100, ct.err_msg: f"vector field must have dimension specified, "
                                                f"field name = {field_name}: invalid parameter"}
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.add_collection_field(client, collection_name, field_name=field_name, data_type=DataType.FLOAT_VECTOR,
                                  nullable=True, check_task=CheckTasks.err_res, check_items=error)

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
        rows_with_reranker = [{default_primary_key_field_name: i, default_vector_field_name: vectors_batch2[i - default_nb],
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
