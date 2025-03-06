import pytest
import numpy as np
import random
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_base import TestcaseBase

default_dim = ct.default_dim
default_nb = ct.default_nb
default_nq = ct.default_nq
default_limit = ct.default_limit
default_search_params = ct.default_search_params
default_search_field = ct.default_float_vec_field_name
default_search_exp = ct.default_search_exp
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_index_params = ct.default_index
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
range_search_supported_indexes = ct.all_index_types[:7]
uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num


class TestSearchV2(TestcaseBase):
    """
    Test class for search functionality
    Using similar architecture as test_mix_scenes.py, all test cases share one collection
    """

    def setup_class(self):
        super().setup_class(self)

        # Connect to server
        self._connect(self)

        # Initialize parameters
        self.primary_field, self.nb = "int64_pk", 3000

        # Create collection
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_search_v2"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT_VECTOR.name, DataType.VARCHAR.name,
                        DataType.INT64.name, DataType.FLOAT.name, DataType.BOOL.name,
                        DataType.JSON.name],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT_VECTOR.name: FieldParams(dim=default_dim).to_dict
                },
            )
        )

        # Prepare data
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        # Insert data
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # Flush collection
        self.collection_wrap.flush()

        # Create index
        index_params = {
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            **DefaultScalarIndexParams.INVERTED(DataType.VARCHAR.name)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # Load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_hit_vectors(self):
        """
        Test search using vectors in collection
        Expected: Search successful and top 1 vector is itself (distance is 0)
        """
        # Get vectors inserted into collection
        vectors = []
        for vector in self.insert_data[DataType.FLOAT_VECTOR.name]:
            vectors.append(vector)

        # Search
        search_res, _ = self.collection_wrap.search(
            vectors[:nq], 
            DataType.FLOAT_VECTOR.name,
            default_search_params, 
            default_limit,
            default_search_exp,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "ids": self.insert_data.get(self.primary_field),
                "limit": default_limit
            }
        )

        # Verify top 1 vector is itself
        for hits in search_res:
            assert 1.0 - hits.distances[0] <= epsilon

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self):
        """
        Test search with expression
        Expected: Search successful and results match expression conditions
        """
        # Generate search vectors
        search_vectors = cf.gen_vectors(nq, dim=default_dim)
        
        # Generate expression
        expr = f"{DataType.INT64.name} > 0"
        
        # Search
        search_res, _ = self.collection_wrap.search(
            search_vectors,
            DataType.FLOAT_VECTOR.name,
            default_search_params,
            default_limit,
            expr=expr,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit
            }
        )

        # Verify results
        for hits in search_res:
            for hit in hits:
                assert hit.entity.get(DataType.INT64.name) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_string_expression(self):
        """
        Test search with string expression
        Expected: Search successful and results match string expression conditions
        """
        # Generate search vectors
        search_vectors = cf.gen_vectors(nq, dim=default_dim)
        
        # Generate string expression
        expr = f"{DataType.VARCHAR.name} != ''"
        
        # Search
        search_res, _ = self.collection_wrap.search(
            search_vectors,
            DataType.FLOAT_VECTOR.name,
            default_search_params,
            default_limit,
            expr=expr,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit
            }
        )

        # Verify results
        for hits in search_res:
            for hit in hits:
                assert hit.entity.get(DataType.VARCHAR.name) != ""

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_json_expression(self):
        """
        Test search with JSON expression
        Expected: Search successful and results match JSON expression conditions
        """
        # Generate search vectors
        search_vectors = cf.gen_vectors(nq, dim=default_dim)
        
        # Generate JSON expression
        expr = f"{DataType.JSON.name}['number'] > 0"
        
        # Search
        search_res, _ = self.collection_wrap.search(
            search_vectors,
            DataType.FLOAT_VECTOR.name,
            default_search_params,
            default_limit,
            expr=expr,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": default_limit
            }
        )

        # Verify results
        for hits in search_res:
            for hit in hits:
                assert hit.entity.get(DataType.JSON.name)["number"] > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_pagination(self):
        """
        Test pagination search
        Expected: Search results match pagination requirements
        """
        # Generate search vectors
        search_vectors = cf.gen_vectors(nq, dim=default_dim)
        
        # Set pagination parameters
        limit = 10
        offset = 5
        
        # First search (with pagination)
        search_res1, _ = self.collection_wrap.search(
            search_vectors,
            DataType.FLOAT_VECTOR.name,
            default_search_params,
            limit,
            offset=offset,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": limit
            }
        )
        
        # Second search (without pagination)
        search_res2, _ = self.collection_wrap.search(
            search_vectors,
            DataType.FLOAT_VECTOR.name,
            default_search_params,
            limit + offset,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": nq,
                "limit": limit + offset
            }
        )
        
        # Verify results
        assert set(search_res1[0].ids) == set(search_res2[0].ids[offset:]) 