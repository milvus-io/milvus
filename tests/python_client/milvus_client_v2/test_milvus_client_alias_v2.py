import pytest
import random

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus import DataType

prefix = "alias"
exp_name = "name"
exp_schema = "schema"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


class TestMilvusClientV2AliasInvalid(TestMilvusClientV2Base):
    """ Negative test cases of alias interface parameters"""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("alias_name", ct.invalid_resource_names)
    def test_milvus_client_v2_create_alias_with_invalid_name(self, alias_name):
        """
        target: test alias inserting data
        method: create a collection with invalid alias name
        expected: create alias failed
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias with invalid name
        error = {ct.err_code: 1100, ct.err_msg: "Invalid collection alias"}
        if alias_name is None or alias_name.strip() == "":
            error = {ct.err_code: 1100, ct.err_msg: "collection alias should not be empty"}
        self.create_alias(client, collection_name, alias_name,
                          check_task=CheckTasks.err_res, check_items=error)
        
        # cleanup
        self.drop_collection(client, collection_name)


class TestMilvusClientV2AliasOperation(TestMilvusClientV2Base):
    """ Test cases of alias interface operations"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_v2_alter_alias_operation_default(self):
        """
        target: test collection altering alias
        method:
                1. create collection_1 with index and load, bind alias to collection_1 and insert 2000 entities
                2. verify operations using alias work on collection_1
                3. create collection_2 with index and load with 1500 entities
                4. alter alias to collection_2
                5. verify operations using alias work on collection_2
        expected: 
                1. operations using alias work on collection_1 before alter
                2. operations using alias work on collection_2 after alter
        """
        client = self._client()
        
        # 1. create collection1 with index and load
        collection_name1 = cf.gen_unique_str("collection1")
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, metric_type="L2")
        self.create_collection(client, collection_name1, default_dim, consistency_level="Bounded", index_params=index_params)
        
        # 2. create alias and insert data
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)
        
        # 3. insert data into collection1 using alias
        nb1 = 2000
        vectors = cf.gen_vectors(nb1, default_dim)
        rows = [{default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i)} for i in range(nb1)]
        self.insert(client, alias_name, rows)
        self.flush(client, alias_name)
        
        # 4. verify collection1 data using alias
        res1 = self.query(client, alias_name, filter="", output_fields=["count(*)"])
        assert res1[0][0].get("count(*)") == nb1
        
        # 5. verify search using alias works on collection1
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(client, alias_name, search_vectors, limit=default_limit,
                   check_task=CheckTasks.check_search_results,
                   check_items={"enable_milvus_client_api": True,
                              "nq": len(search_vectors),
                              "limit": default_limit})
        
        # 6. create collection2 with index and load
        collection_name2 = cf.gen_unique_str("collection2")
        self.create_collection(client, collection_name2, default_dim, consistency_level="Bounded", index_params=index_params)
        
        # 7. insert data into collection2
        nb2 = 1500
        vectors = cf.gen_vectors(nb2, default_dim)
        rows = [{default_primary_key_field_name: i,
                default_vector_field_name: vectors[i],
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i)} for i in range(nb2)]
        self.insert(client, collection_name2, rows)
        self.flush(client, collection_name2)
        
        # 8. alter alias to collection2
        self.alter_alias(client, collection_name2, alias_name)
        
        # 9. verify collection2 data using alias
        res2 = self.query(client, alias_name, filter="", output_fields=["count(*)"])
        assert res2[0][0].get("count(*)") == nb2
        
        # 10. verify search using alias works on collection2
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(client, alias_name, search_vectors, limit=default_limit,
                   check_task=CheckTasks.check_search_results,
                   check_items={"enable_milvus_client_api": True,
                              "nq": len(search_vectors),
                              "limit": default_limit})
        
        # 11. verify operations on collection1 still work
        res1 = self.query(client, collection_name1, filter="", output_fields=["count(*)"])
        assert res1[0][0].get("count(*)") == nb1
        
        # cleanup
        self.release_collection(client, collection_name1)
        self.release_collection(client, collection_name2)
        self.drop_collection(client, collection_name1)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_create_drop_alias_operation_default(self):
        """
        target: test collection creating and dropping alias
        method: 
                1. create a collection with 10 partitions
                2. create an alias for the collection
                3. verify alias has same partitions as collection
                4. drop the alias
                5. verify alias is dropped and collection still exists
        expected: 
                1. alias has same partitions as collection
                2. alias can be dropped successfully
                3. collection remains unchanged after alias operations
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create partitions
        partition_names = []
        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            partition_names.append(partition_name)
            self.create_partition(client, collection_name, partition_name)
            
        # 3. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)
        
        # 4. verify partitions in collection and alias
        partitions = self.list_partitions(client, collection_name)
        alias_partitions = self.list_partitions(client, alias_name)
        assert partitions == alias_partitions
        
        # 5. verify collection exists
        assert self.has_collection(client, collection_name)[0]
        assert self.has_collection(client, alias_name)[0]
        
        # 6. drop alias
        self.drop_alias(client, alias_name)
        
        # 7. verify alias is dropped
        error = {ct.err_code: 0,
                 ct.err_msg: f"can't find collection[database=default][collection={alias_name}]"}
        self.describe_collection(client, alias_name,
                               check_task=CheckTasks.err_res,
                               check_items=error)
        
        # 8. verify collection still exists and unchanged
        assert self.has_collection(client, collection_name)[0]
        collection_partitions = self.list_partitions(client, collection_name)
        assert collection_partitions == partitions
        
        # cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_collection_operations_by_alias(self):
        """
        target: test collection operations using alias
        method: 
                1. create collection with alias
                2. verify has_collection works with alias
                3. verify drop_collection fails with alias
        expected: 
                1. has_collection returns True for alias
                2. drop_collection fails with error message
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)
        
        # 3. verify has_collection works with alias
        assert self.has_collection(client, alias_name)[0]
        assert self.has_collection(client, collection_name)[0]
        
        # 4. verify drop_collection fails with alias
        error = {ct.err_code: 1,
                 ct.err_msg: f"cannot drop the collection via alias = {alias_name}"}
        self.drop_collection(client, alias_name,
                           check_task=CheckTasks.err_res,
                           check_items=error)
        
        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)
        assert not self.has_collection(client, collection_name)[0]


class TestMilvusClientV2AliasOperationInvalid(TestMilvusClientV2Base):
    """ Test cases of alias interface invalid operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_create_duplication_alias(self):
        """
        target: test create duplicate alias
        method: create alias twice with same name to different collections
        expected: raise exception
        """
        client = self._client()
        collection_name1 = cf.gen_unique_str("collection1")
        collection_name2 = cf.gen_unique_str("collection2")
        
        # 1. create collection1
        self.create_collection(client, collection_name1, default_dim, consistency_level="Bounded")
        
        # 2. create collection2
        self.create_collection(client, collection_name2, default_dim, consistency_level="Bounded")
        
        # 3. create alias for collection1
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)
        
        # 4. try to create same alias for collection2
        error = {ct.err_code: 1,
                 ct.err_msg: f"{alias_name} is alias to another collection: {collection_name1}"}
        self.create_alias(client, collection_name2, alias_name,
                         check_task=CheckTasks.err_res,
                         check_items=error)
        
        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name1)
        self.drop_collection(client, collection_name2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_alter_not_exist_alias(self):
        """
        target: test alter not exist alias
        method: alter alias that not exists
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        alias_name = cf.gen_unique_str(prefix)
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias and link to the collection
        self.create_alias(client, collection_name, alias_name)
        
        # 3. alter alias, trying to link the collection to a non existing alias
        non_exist_alias = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1600,
                 ct.err_msg: f"alias not found[database=default][alias={non_exist_alias}]"}
        self.alter_alias(client, collection_name, non_exist_alias,
                        check_task=CheckTasks.err_res,
                        check_items=error)
        
        # 4. cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_drop_not_exist_alias(self):
        """
        target: test drop not exist alias
        method: drop alias that not exists
        expected: no exception
        """
        client = self._client()
        alias_name = cf.gen_unique_str(prefix)
        
        # trying to drop a non existing alias
        self.drop_alias(client, alias_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_drop_same_alias_twice(self):
        """
        target: test drop same alias twice
        method: drop alias twice
        expected: no exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)
        
        # 3. drop alias first time
        self.drop_alias(client, alias_name)
        
        # 4. try to drop alias second time
        self.drop_alias(client, alias_name)
        
        # cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_create_dup_name_collection(self):
        """
        target: test create collection with duplicate name
        method: create collection with alias name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("collection")
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)
        
        # 3. try to create collection with alias name
        error = {ct.err_code: 0,
                 ct.err_msg: f"collection name [{alias_name}] conflicts with an existing alias,"
                             " please choose a unique name"}
        self.create_collection(client, alias_name, default_dim, consistency_level="Bounded",
                             check_task=CheckTasks.err_res,
                             check_items=error)
        
        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_v2_reuse_alias_name(self):
        """
        target: test reuse alias name from dropped collection
        method: 
                1.create collection1 with alias
                2.drop collection1
                3.create collection2 with same alias name
        expected: create collection2 successfully
        """
        client = self._client()
        collection_name1 = cf.gen_unique_str("collection1")
        
        # 1. create collection1
        self.create_collection(client, collection_name1, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)
        
        # 3. drop the alias and collection1
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name1)
        
        # 4. create collection2
        collection_name2 = cf.gen_unique_str("collection2")
        self.create_collection(client, collection_name2, default_dim, consistency_level="Bounded")
        
        # 5. create alias with the previous alias name and assign it to collection2
        self.create_alias(client, collection_name2, alias_name)
        
        # 6. verify collection2
        assert self.has_collection(client, collection_name2)[0]
        assert self.has_collection(client, alias_name)[0]
        
        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name2)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_v2_rename_collection_to_alias_name(self):
        """
        target: test rename collection to alias name
        method: 
                1.create collection1 with alias
                2.rename collection2 to alias name
        expected: raise exception
        """
        client = self._client()
        collection_name1 = cf.gen_unique_str("collection1")
        collection_name2 = cf.gen_unique_str("collection2")
        
        # 1. create collection1
        self.create_collection(client, collection_name1, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)
        
        # 3. create collection2
        self.create_collection(client, collection_name2, default_dim, consistency_level="Bounded")
        
        # 4. try to rename collection2 to alias name
        error = {ct.err_code: 999,
                 ct.err_msg: f"cannot rename collection to an existing alias: {alias_name}"}
        self.rename_collection(client, collection_name2, alias_name,
                             check_task=CheckTasks.err_res,
                             check_items=error)
        
        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name1)
        self.drop_collection(client, collection_name2)
