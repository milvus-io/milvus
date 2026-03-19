import pytest

from pymilvus import DataType
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "alias"
default_dim = ct.default_dim
default_limit = ct.default_limit


class TestMilvusClientV2AliasInvalid(TestMilvusClientV2Base):
    """ Negative test cases of alias interface parameters"""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("alias_name", ct.invalid_resource_names)
    def test_milvus_client_v2_create_alias_with_invalid_name(self, alias_name):
        """
        target: test creating alias with invalid name is rejected
        method: create a collection, then create alias with invalid name
        expected: create alias failed with error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias with invalid name
        error = {ct.err_code: 1100, ct.err_msg: "Invalid collection alias"}
        if alias_name is None or alias_name == "":
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
                2. verify count and search using alias work on collection_1
                3. create collection_2 with index and load with 1500 entities (start=10000 to distinguish IDs)
                4. alter alias to collection_2
                5. verify count and search using alias work on collection_2 (IDs in collection_2 range)
                6. verify collection_1 still has its own data
        expected:
                1. operations using alias work on collection_1 before alter
                2. operations using alias work on collection_2 after alter
                3. collection_1 data is unaffected
        """
        client = self._client()

        # 1. create collection1 with schema, index and load
        collection_name1 = cf.gen_collection_name_by_testcase_name()
        schema1 = self.create_schema(client)[0]
        schema1.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema1.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema1.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=256)
        schema1.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, metric_type="L2")
        self.create_collection(client, collection_name1, schema=schema1,
                               index_params=index_params, consistency_level="Bounded")

        # 2. create alias and insert data into collection1 via alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)

        nb1 = 2000
        data1 = cf.gen_row_data_by_schema(nb=nb1, schema=schema1, start=0)
        self.insert(client, alias_name, data1)
        self.flush(client, alias_name)

        # 3. verify collection1 count using alias
        res1 = self.query(client, alias_name, filter=f"{ct.default_int64_field_name} >= 0",
                          output_fields=["count(*)"])
        assert res1[0][0].get("count(*)") == nb1

        # 4. verify search using alias works on collection1
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(client, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": 1,
                                 "pk_name": ct.default_int64_field_name,
                                 "limit": default_limit,
                                 "metric": "L2"})

        # 5. create collection2 with same schema, index and load
        collection_name2 = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name2, schema=schema1,
                               index_params=index_params, consistency_level="Bounded")

        # 6. insert data into collection2 with distinct ID range (start=10000)
        nb2 = 1500
        data2 = cf.gen_row_data_by_schema(nb=nb2, schema=schema1, start=10000)
        self.insert(client, collection_name2, data2)
        self.flush(client, collection_name2)

        # 7. alter alias to collection2
        self.alter_alias(client, collection_name2, alias_name)

        # 8. verify alias now points to collection2 (count = nb2)
        res2 = self.query(client, alias_name, filter=f"{ct.default_int64_field_name} >= 0",
                          output_fields=["count(*)"])
        assert res2[0][0].get("count(*)") == nb2

        # 9. verify search using alias returns collection2 IDs (>= 10000)
        search_res, _ = self.search(client, alias_name, search_vectors, limit=default_limit,
                                    output_fields=[ct.default_int64_field_name],
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"enable_milvus_client_api": True,
                                                 "nq": 1,
                                                 "pk_name": ct.default_int64_field_name,
                                                 "limit": default_limit,
                                                 "metric": "L2"})
        for hit in search_res[0]:
            assert hit[ct.default_int64_field_name] >= 10000, \
                f"After alter, alias should point to collection2 (IDs >= 10000), got {hit[ct.default_int64_field_name]}"

        # 10. verify collection1 data is unaffected
        res1_after = self.query(client, collection_name1,
                                filter=f"{ct.default_int64_field_name} >= 0",
                                output_fields=["count(*)"])
        assert res1_after[0][0].get("count(*)") == nb1

        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name1)
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        
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
        error = {ct.err_code: 100,
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_rename_back_old_alias(self):
        """
        target: test renaming collection to a previously dropped alias name
        method:
                1. create collection with alias
                2. drop the alias
                3. rename collection to the dropped alias name
        expected:
                1. rename collection successfully — dropped alias name is reusable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)

        # 3. drop the alias
        self.drop_alias(client, alias_name)
        assert self.list_aliases(client, collection_name)[0]["aliases"] == []

        # 4. rename collection to the dropped alias name
        self.rename_collection(client, collection_name, alias_name)

        # cleanup
        self.drop_collection(client, alias_name)
        assert not self.has_collection(client, alias_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_v2_rename_back_old_collection(self):
        """
        target: test renaming collection back to original name preserves alias binding
        method:
                1. create collection with alias
                2. rename collection to a new name
                3. rename back to old collection name
        expected:
                1. rename succeeds, alias still bound to the collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection
        self.create_collection(client, collection_name, default_dim)

        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)

        # 3. rename collection
        new_collection_name = cf.gen_collection_name_by_testcase_name()
        self.rename_collection(client, collection_name, new_collection_name)

        # 4. rename back to old collection name
        self.rename_collection(client, new_collection_name, collection_name)
        assert self.has_collection(client, collection_name)[0]
        assert not self.has_collection(client, new_collection_name)[0]
        assert alias_name in self.list_aliases(client, collection_name)[0]["aliases"]

        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)
        assert not self.has_collection(client, collection_name)[0]


class TestMilvusClientV2AliasOperationInvalid(TestMilvusClientV2Base):
    """ Test cases of alias interface invalid operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_create_alias_for_non_exist_collection(self):
        """
        target: test creating alias for a non-existent collection is rejected
        method: create alias pointing to a collection name that does not exist
        expected: raise exception with collection not found error
        """
        client = self._client()
        non_exist_collection = cf.gen_unique_str("non_exist_collection")
        alias_name = cf.gen_unique_str(prefix)

        error = {ct.err_code: 100,
                 ct.err_msg: f"can't find collection[database=default][collection={non_exist_collection}]"}
        self.create_alias(client, non_exist_collection, alias_name,
                          check_task=CheckTasks.err_res,
                          check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_alter_alias_to_non_exist_collection(self):
        """
        target: test altering alias to point to a non-existent collection is rejected
        method: 1. create collection and bind alias
                2. alter alias to point to a non-existent collection
        expected: raise exception with collection not found error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")

        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)

        non_exist_collection = cf.gen_unique_str("non_exist_collection")
        error = {ct.err_code: 100,
                 ct.err_msg: f"can't find collection[database=default][collection={non_exist_collection}]"}
        self.alter_alias(client, non_exist_collection, alias_name,
                         check_task=CheckTasks.err_res,
                         check_items=error)

        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_v2_create_duplicate_alias(self):
        """
        target: test create duplicate alias
        method: create alias twice with same name to different collections
        expected: raise exception
        """
        client = self._client()
        collection_name1 = cf.gen_collection_name_by_testcase_name()
        collection_name2 = cf.gen_collection_name_by_testcase_name()
        
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name, alias_name)
        
        # 3. try to create collection with alias name
        error = {ct.err_code: 1601,
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
        collection_name1 = cf.gen_collection_name_by_testcase_name()
        
        # 1. create collection1
        self.create_collection(client, collection_name1, default_dim, consistency_level="Bounded")
        
        # 2. create alias
        alias_name = cf.gen_unique_str(prefix)
        self.create_alias(client, collection_name1, alias_name)
        
        # 3. drop the alias and collection1
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name1)
        
        # 4. create collection2
        collection_name2 = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name2, default_dim, consistency_level="Bounded")
        
        # 5. create alias with the previous alias name and assign it to collection2
        self.create_alias(client, collection_name2, alias_name)
        
        # 6. verify collection2
        assert self.has_collection(client, collection_name2)[0]
        assert self.has_collection(client, alias_name)[0]

        # 7. verify alias is bound to collection2 via list_aliases
        aliases_res = self.list_aliases(client, collection_name2)[0]
        assert alias_name in aliases_res["aliases"]

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
        collection_name1 = cf.gen_collection_name_by_testcase_name()
        collection_name2 = cf.gen_collection_name_by_testcase_name()
        
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
