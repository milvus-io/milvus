import pytest

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
    def test_milvus_client_create_alias_not_exist_collection(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "not_exist_collection_alias"
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=1][collection={collection_name}]"}
        self.create_alias(client, collection_name, alias,
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_create_alias_invalid_alias_name(self, alias):
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
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection alias: {alias}. "
                             f"the first character of a collection alias must be an underscore or letter"}
        self.create_alias(client, collection_name, alias,
                          check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

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
    def test_milvus_client_create_same_alias_diff_collections(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        collection_name_1 = cf.gen_unique_str(prefix)
        alias = cf.gen_unique_str("collection_alias")
        # 1. create collection and alias
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_alias(client, collection_name, alias)
        # 2. create another collection and same alias
        self.create_collection(client, collection_name_1, default_dim, consistency_level="Strong")
        error = {ct.err_code: 1602, ct.err_msg: f"{alias} is alias to another collection: "
                                                f"{collection_name}: alias already exist[database=default]"
                                                f"[alias={alias}]"}
        self.create_alias(client, collection_name_1, alias,
                          check_task=CheckTasks.err_res, check_items=error)
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_alias_not_existed(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("not_existed_alias")
        self.drop_alias(client, alias)

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
    def test_milvus_client_alter_alias_not_exist_collection(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._client()
        alias = cf.gen_unique_str("collection_alias")
        collection_name = cf.gen_unique_str("not_exist_collection_alias")
        # 2. create alias
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[collection={collection_name}]"}
        self.alter_alias(client, collection_name, alias,
                         check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_alter_alias_invalid_alias_name(self, alias):
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_non_exists_alias(self):
        """
        target: test alter alias (high level api)
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: alter alias successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("coll")
        alias = cf.gen_unique_str("alias")
        another_alias = cf.gen_unique_str("another_alias")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        self.create_alias(client, collection_name, alias)
        # 3. alter alias
        error = {ct.err_code: 1600, ct.err_msg: f"alias not found[database=default][alias={another_alias}]"}
        self.alter_alias(client, collection_name, another_alias,
                         check_task=CheckTasks.err_res, check_items=error)
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alias_create_duplication_alias(self):
        """
        target: test two collections creating alias with same name
        method: 
                1.create a collection_1 with alias name alias_a
                2.create a collection_2 also with alias name alias_a
        expected: 
                in step 2, creating alias with a duplication name is not allowed
        """
        # step 1: create collection with alias name
        client = self._client()
        collection_1 = cf.gen_collection_name_by_testcase_name(module_index=1)
        alias_a = cf.gen_unique_str("collection_alias")
        self.create_collection(client, collection_1, default_dim, consistency_level="Strong")
        self.create_alias(client, collection_1, alias_a)
        
        # step 2: create a collection_2 also with alias name alias_a
        collection_2 = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_2, default_dim, consistency_level="Strong")
        error = {ct.err_code: 1602, ct.err_msg: f"{alias_a} is alias to another collection: {collection_1}: "
                                                f"alias already exist[database=default][alias={alias_a}]"}
        self.create_alias(client, collection_2, alias_a, 
                          check_task=CheckTasks.err_res, 
                          check_items=error)
        self.drop_alias(client, alias_a)
        self.drop_collection(client, collection_1)
        self.drop_collection(client, collection_2)
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_same_alias_twice(self):
        """
        target: test drop same alias twice
        method: 
                1.create a collection with alias
                2.collection drop alias
                3.collection drop alias again
        expected: drop alias succ
        """
        # step 1: create a collection with alias
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        alias = cf.gen_unique_str("collection_alias")
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_alias(client, collection_name, alias)
        
        # step 2: drop alias
        self.drop_alias(client, alias)
        
        # step 3: drop alias again
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_create_dup_name_collection(self):
        """
        target: test creating a collection with a same name as alias, but a different schema
        method:
                1.create a collection with alias
                2.create a collection with same name as alias, but a different schema
        expected: in step 2, create collection failed
        """
        # step 1: create a collection with alias
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        alias = cf.gen_unique_str("collection_alias")
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_alias(client, collection_name, alias)
        
        # step 2: create a collection with same name as alias, but a different schema
        error = {ct.err_code: 65535, 
                 ct.err_msg: f"collection name [{alias}] conflicts with an existing alias, please choose a unique name"}
        self.create_collection(client, alias, default_dim, 
                               consistency_level="Strong",
                               schema=default_schema,
                               check_task=CheckTasks.err_res, 
                               check_items=error)
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_drop_collection_by_alias(self):
        """
        target: test dropping a collection by alias
        method:
                1.create a collection with alias
                2.drop a collection by alias
        expected: in step 2, drop collection by alias failed by design
        """
        # step 1: create a collection with alias
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        alias = cf.gen_unique_str("collection_alias")
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_alias(client, collection_name, alias)
        
        # step 2: drop collection by alias
        error = {ct.err_code: 1600, ct.err_msg: f"cannot drop the collection via alias = {alias}"}
        self.drop_collection(client, alias, check_task=CheckTasks.err_res, check_items=error)
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_reuse_alias_name_from_dropped_collection(self):
        """
        target: test dropping a collection which has a alias
        method:
                1.create a collection
                2.create an alias for the collection
                3.drop the collection
                4.create a new collection
                5.create an alias with the same alias name for the new collection
            expected: in step 5, create alias with the same name for the new collection succ
        """
        # step 1: create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # step 2: create an alias for the collection
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)
        res, _ = self.list_aliases(client, collection_name)
        assert len(res['aliases']) == 1

        # step 3: drop the collection
        self.drop_alias(client, alias)
        res, _ = self.list_aliases(client, collection_name)
        assert len(res['aliases']) == 0
        self.drop_collection(client, collection_name)
        
        # step 4: create a new collection
        collection_name_2 = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name_2, default_dim, consistency_level="Strong")
        
        # step 5: create an alias with the same alias name for the new collection
        self.create_alias(client, collection_name_2, alias)
        res, _ = self.list_aliases(client, collection_name_2)
        assert len(res['aliases']) == 1
        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name_2)
    
    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_rename_collection_to_alias_name(self):
        """
        target: test renaming a collection to a alias name
        method:
                1.create a collection
                2.create an alias for the collection
                3.rename the collection to the alias name
        expected: in step 3, rename collection to alias name failed
        """
        # step 1: create a collection
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        # step 2: create an alias for the collection
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)
        error = {ct.err_code: 65535, 
                 ct.err_msg: f"cannot rename collection to an existing alias: {alias}"}
        self.rename_collection(client, collection_name, alias, 
                               check_task=CheckTasks.err_res, check_items=error)

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

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alias_alter_operation_default(self):
        """
        target: test collection altering alias
        method:
                1. create collection_1, bind alias to collection_1 and insert 2000 entities
                2. search on alias verify num_entities=2000
                3. create collection_2 with 1500 entities
                4. alter alias to collection_2 and search on alias
                verify num_entities=1500
        expected:
                after step 2, collection_1 num_entities == alias num_entities == 2000
                after step 3, collection_2 num_entities == alias num_entities == 1500
        """
        # step 1: create collection and create alias
        client = self._client()
        c_name1 = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, c_name1, default_dim, consistency_level="Strong")
        
        alias = cf.gen_unique_str("collection_alias")
        # create a collection alias and bind to collection_1
        self.create_alias(client, c_name1, alias)

        # step 2: insert 2000 entities and search on alias
        # default nb is 2000
        c_info = self.describe_collection(client, c_name1)[0]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=c_info)
        self.insert(client, c_name1, rows)

        # pymilvus does not support num_entities yet, so we use query to check the number of entities
        # assert self.num_entities(client, alias) == default_nb == self.num_entities(client, c_name1)
        res1 = self.query(client, c_name1, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        res1_alias = self.query(client, alias, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(res1) == len(res1_alias) == default_nb

        # step 3: create collection 2 with 1500 entities
        nb2 = 1500
        c_name2 = cf.gen_collection_name_by_testcase_name(module_index=2)
        self.create_collection(client, c_name2, default_dim, consistency_level="Strong")
        c_info2 = self.describe_collection(client, c_name2)[0]
        rows = cf.gen_row_data_by_schema(nb=nb2, schema=c_info2)
        self.insert(client, c_name2, rows)

        # step 4: alter the collection alias to collection2
        self.alter_alias(client, c_name2, alias)
        #assert self.num_entities(client, alias) == nb2 == self.num_entities(client, c_name2)
        res2 = self.query(client, c_name2, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        res2_alias = self.query(client, alias, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})[0]
        assert len(res2) == len(res2_alias) == nb2

        self.drop_alias(client, alias)
        self.drop_collection(client, c_name1)
        self.drop_collection(client, c_name2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alias_create_operation_default(self):
        """
        target: test collection creating alias
        method: 
                1.create a collection and create 10 partitions for it
                2.collection create an alias, then init a collection with this alias but not create partitions
        expected: collection is equal to alias
        """
        # step 1: create partitions and check the partition exists
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            # create partition with different names and check the partition exists
            self.create_partition(client, collection_name, partition_name)

        # step 2: create alias from collection, then init a collection with this alias but not create partitions
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)
        # assert collection is equal to alias according to partitions
        partition_name_list = self.list_partitions(client, collection_name)[0]
        partition_name_list_alias = self.list_partitions(client, alias)[0]
        assert partition_name_list == partition_name_list_alias

        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alias_drop_operation_default(self):
        """
        target: test collection dropping alias
        method:
                1.create a collection with 10 partitions
                2.collection create an alias
                3.collection drop the alias
        expected: 
                after step 2, collection is equal to alias
                after step 3, collection with alias name is not exist
        """
        # step 1: create collection with 10 partitions
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            self.create_partition(client, collection_name, partition_name)
            assert self.has_partition(client, collection_name, partition_name)[0]

        # step 2: create alias and drop the alias
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)
        collection_partitions = self.list_partitions(client, collection_name)[0]
        alias_partitions = self.list_partitions(client, alias)[0]
        assert collection_partitions == alias_partitions

        # step 3: check the alias does not exist
        self.drop_alias(client, alias)
        error = {ct.err_code: 1600, ct.err_msg: f"alias not found[database=default][alias={alias}]"}
        # check if error is raised, if raised, function is True and test passed, vise versa
        self.describe_alias(client, alias, check_task=CheckTasks.err_res, check_items=error)
        
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_has_collection_partition(self):
        """
        target: test utility has collection by alias
        method:
                1.create collection with alias and partition
                2.call has_collection function with alias as param 
                3.call has_partition function with alias as param
        expected: result is True
        """
        # step 1: create collection with alias and partition
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)
        partition_name = cf.gen_unique_str("partition")
        self.create_partition(client, collection_name, partition_name)

        # step 2: call has_collection function with alias as param and assert True
        exists_a, _ = self.has_collection(client, alias)
        assert exists_a
        exists_p, _ = self.has_partition(client, alias, partition_name)
        assert exists_p

        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_alias_drop_collection(self):
        """
        target: test utility drop collection by alias
        method:
                1.create collection with alias
                2.call drop_collection function with alias as param
        expected: Got error: collection cannot be dropped via alias.
        """
        # step 1: create collection with alias
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        alias = cf.gen_unique_str("collection_alias")
        self.create_alias(client, collection_name, alias)

        # step 2: call drop_collection function with alias as param and assert error
        error = {ct.err_code: 1600, ct.err_msg: f"cannot drop the collection via alias = {alias}"}
        # check if error is raised, if raised, function is True and test passed, vise versa
        self.drop_collection(client, alias, check_task=CheckTasks.err_res, check_items=error)

        self.drop_alias(client, alias)
        self.drop_collection(client, collection_name)
    
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