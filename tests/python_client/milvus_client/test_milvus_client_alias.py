import pytest

from pymilvus import DataType
from base.client_v2_base import TestMilvusClientV2Base

from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "client_alias"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()


class TestMilvusClientAliasInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_create_alias_invalid_collection_name(self, collection_name):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.create_alias(client, collection_name, alias,
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_collection_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "a".join("a" for i in range(256))
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        self.create_alias(client, collection_name, alias,
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        alias = "a".join("a" for i in range(256))
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection alias must be less than 255 characters"}
        self.create_alias(client, collection_name, alias,
                          check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_same_collection_name(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str('coll')
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1601,
                 ct.err_msg: f"alias and collection name conflict[database=default][alias={collection_name}]"}
        self.create_alias(client, collection_name, collection_name,
                          check_task=CheckTasks.err_res, check_items=error)
        # create a collection with the same alias name
        alias_name = cf.gen_unique_str('alias')
        self.create_alias(client, collection_name, alias_name)
        error = {ct.err_code: 1601,
                 ct.err_msg: f"collection name [{alias_name}] conflicts with an existing alias, "
                             f"please choose a unique name"}
        self.create_collection(client, alias_name, default_dim,
                               check_task=CheckTasks.err_res, check_items=error)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/pull/43064 change drop alias restraint")
    def test_milvus_client_drop_alias_invalid_alias_name(self, alias_name):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection alias: {alias_name}. the first character of a "
                                                f"collection alias must be an underscore or letter"}
        self.drop_alias(client, alias_name,
                        check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/pull/43064 change drop alias restraint")
    def test_milvus_client_drop_alias_over_max_length(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection alias must be less than 255 characters"}
        self.drop_alias(client, alias,
                        check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_alter_alias_invalid_collection_name(self, collection_name):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.alter_alias(client, collection_name, alias,
                         check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_collection_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "a".join("a" for i in range(256))
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters"}
        self.alter_alias(client, collection_name, alias,
                         check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_invalid_alias_name(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        alias_names = ["12-s", "12 s", "(mn)", "中文", "%$#"]
        for alias in alias_names:
            error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection alias: {alias}. the first character of a "
                                                    f"collection alias must be an underscore or letter"}
            self.alter_alias(client, collection_name, alias,
                            check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        alias = "a".join("a" for i in range(256))
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection alias must be less than 255 characters"}
        self.alter_alias(client, collection_name, alias,
                         check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_same_collection_name(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1601, ct.err_msg: f"alias and collection name conflict[database=default]"
                                                f"[alias={collection_name}"}
        self.alter_alias(client, collection_name, collection_name,
                         check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("alias_name", ct.invalid_resource_names)
    def test_milvus_client_create_alias_with_invalid_name(self, alias_name):
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


class TestMilvusClientAliasValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_search_query(self):
        """
        target: test search (high level api) normal case
        method: create connection, collection, insert and search
        expected: search/query successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        alias = "collection_alias"
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        self.drop_alias(client, alias)
        self.create_alias(client, collection_name, alias)
        collection_name = alias
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # self.flush(client, collection_name)
        # assert self.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "limit": default_limit,
                                 "pk_name": default_primary_key_field_name})
        # 4. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        self.release_collection(client, collection_name)
        self.drop_collection(client, collection_name, check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 65535,
                                          ct.err_msg: "cannot drop the collection via alias = collection_alias"})
        self.drop_alias(client, alias)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 1891, 1892")
    def test_milvus_client_alias_default(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        alias = cf.gen_unique_str("collection_alias")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_partition(client, collection_name, partition_name)
        partition_name_list = self.list_partitions(client, collection_name)[0]
        # 2. create alias
        self.create_alias(client, collection_name, alias)
        self.describe_alias(client, alias)
        # 3. list alias
        aliases = self.list_aliases(client)[0]
        # assert alias in aliases
        # 4. assert collection is equal to alias according to partitions
        partition_name_list_alias = self.list_partitions(client, alias)[0]
        assert partition_name_list == partition_name_list_alias
        # 5. drop alias
        self.drop_alias(client, alias)
        aliases = self.list_aliases(client)[0]
        # assert alias not in aliases
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_default(self):
        """
        target: test alter alias (high level api)
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: alter alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        another_collectinon_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        alias = cf.gen_unique_str("collection_alias")
        another_alias = cf.gen_unique_str("collection_alias_another")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_partition(client, collection_name, partition_name)
        partition_name_list = self.list_partitions(client, collection_name)[0]
        self.create_collection(client, another_collectinon_name, default_dim, consistency_level="Strong")
        self.create_alias(client, another_collectinon_name, another_alias)
        # 2. create alias
        self.create_alias(client, collection_name, alias)
        # 3. alter alias
        self.alter_alias(client, collection_name, another_alias)
        self.describe_alias(client, alias)
        # 3. list alias
        aliases = self.list_aliases(client, collection_name)[0]
        # assert alias in aliases
        # assert another_alias in aliases
        # 4. assert collection is equal to alias according to partitions
        partition_name_list_alias = self.list_partitions(client, another_alias)[0]
        assert partition_name_list == partition_name_list_alias
        self.drop_alias(client, alias)
        self.drop_alias(client, another_alias)
        self.drop_collection(client, collection_name)

class TestMilvusClientAliasOperation(TestMilvusClientV2Base):
    """ This is a migration test case for alias operation """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_enable_mmap_by_alias(self):
        """
        target: test utility enable mmap by alias
        method:
                1.create collection with alias
                2.call enable_mmap function with alias as param
        expected: result is True
        """
        # step 1: create collection with alias
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)

        # step 2: enable mmap via alias and verify
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, alias, properties={"mmap.enabled": True})
        res = self.describe_collection(client, collection_name)[0].get("properties")
        assert res["mmap.enabled"] == 'True'

        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_alias_operation_default(self):
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
    def test_milvus_client_create_drop_alias_operation_default(self):
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
    def test_milvus_client_collection_operations_by_alias(self):
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
    def test_milvus_client_rename_back_old_alias(self):
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
    def test_milvus_client_rename_back_old_collection(self):
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


class TestMilvusClientAliasOperationInvalid(TestMilvusClientV2Base):
    """ Test cases of alias interface invalid operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_for_non_exist_collection(self):
        """
        target: test creating alias for a non-existent collection is rejected
        method: create alias pointing to a collection name that does not exist
        expected: raise exception with collection not found error
        """
        client = self._client()
        non_exist_collection = cf.gen_unique_str("non_exist_collection")
        alias_name = cf.gen_unique_str(prefix)

        error = {ct.err_code: 100,
                 ct.err_msg: f"collection not found[database=default][collection={non_exist_collection}]"}
        self.create_alias(client, non_exist_collection, alias_name,
                          check_task=CheckTasks.err_res,
                          check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_to_non_exist_collection(self):
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
                 ct.err_msg: f"collection not found[collection={non_exist_collection}]"}
        self.alter_alias(client, non_exist_collection, alias_name,
                         check_task=CheckTasks.err_res,
                         check_items=error)

        # cleanup
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_duplicate_alias(self):
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
    def test_milvus_client_alter_not_exist_alias(self):
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
    def test_milvus_client_drop_not_exist_alias(self):
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
    def test_milvus_client_drop_same_alias_twice(self):
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


    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_reuse_alias_name(self):
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
    def test_milvus_client_rename_collection_to_alias_name(self):
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