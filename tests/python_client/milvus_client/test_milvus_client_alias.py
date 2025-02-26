import multiprocessing
import numbers
import random
import numpy
import threading
import pytest
import pandas as pd
import decimal
from decimal import Decimal, getcontext
from time import sleep
import heapq

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from base.high_level_api_wrapper import HighLevelApiWrapper
client_w = HighLevelApiWrapper()

prefix = "milvus_client_api_alias"
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


class TestMilvusClientAliasInvalid(TestcaseBase):
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
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.create_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_collection_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "a".join("a" for i in range(256))
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.create_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_not_exist_collection(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "not_exist_collection_alias"
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection collection not "
                                               f"found[database=default][collection={collection_name}]"}
        client_w.create_alias(client, collection_name, alias,
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
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.create_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        alias = "a".join("a" for i in range(256))
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.create_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_alias_same_collection_name(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1601, ct.err_msg: f"alias and collection name conflict[database=default]"
                                                f"[alias={collection_name}]"}
        client_w.create_alias(client, collection_name, collection_name,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_same_alias_diff_collections(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        collection_name_1 = cf.gen_unique_str(prefix)
        alias = cf.gen_unique_str("collection_alias")
        # 1. create collection and alias
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        client_w.create_alias(client, collection_name, alias)
        # 2. create another collection and same alias
        client_w.create_collection(client, collection_name_1, default_dim, consistency_level="Strong")
        error = {ct.err_code: 1602, ct.err_msg: f"{alias} is alias to another collection: "
                                                f"{collection_name}: alias already exist[database=default]"
                                                f"[alias={alias}]"}
        client_w.create_alias(client, collection_name_1, alias,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_alias(client, alias)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_alias_not_existed(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("not_existed_alias")
        client_w.drop_alias(client, alias)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_drop_alias_invalid_alias_name(self, alias_name):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {alias_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.drop_alias(client, alias_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_alias_over_max_length(self):
        """
        target: test create same alias to different collections
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = "a".join("a" for i in range(256))
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {alias}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.drop_alias(client, alias,
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
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.alter_alias(client, collection_name, alias,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_collection_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        collection_name = "a".join("a" for i in range(256))
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                             f"the length of a collection name must be less than 255 characters: "
                                             f"invalid parameter"}
        client_w.alter_alias(client, collection_name, alias,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_not_exist_collection(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        alias = cf.gen_unique_str("collection_alias")
        collection_name = cf.gen_unique_str("not_exist_collection_alias")
        # 2. create alias
        error = {ct.err_code: 100, ct.err_msg: f"can't find collection collection not "
                                               f"found[database=default][collection={collection_name}]"}
        client_w.alter_alias(client, collection_name, alias,
                             check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_alter_alias_invalid_alias_name(self, alias):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        client_w.alter_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_name_over_max_length(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        alias = "a".join("a" for i in range(256))
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        client_w.alter_alias(client, collection_name, alias,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_same_collection_name(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        error = {ct.err_code: 1601, ct.err_msg: f"alias and collection name conflict[database=default]"
                                                f"[alias={collection_name}"}
        client_w.alter_alias(client, collection_name, collection_name,
                              check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_non_exists_alias(self):
        """
        target: test alter alias (high level api)
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: alter alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        alias = cf.gen_unique_str("collection_alias")
        another_alias = cf.gen_unique_str("collection_alias_another")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        client_w.create_alias(client, collection_name, alias)
        # 3. alter alias
        error = {ct.err_code: 1600, ct.err_msg: f"alias not found[database=default][alias={collection_name}]"}
        client_w.alter_alias(client, collection_name, another_alias,
                             check_task=CheckTasks.err_res, check_items=error)
        client_w.drop_alias(client, alias)
        client_w.drop_collection(client, collection_name)


class TestMilvusClientAliasValid(TestcaseBase):
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
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        alias = "collection_alias"
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create alias
        client_w.drop_alias(client, alias)
        client_w.create_alias(client, collection_name, alias)
        collection_name = alias
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        client_w.insert(client, collection_name, rows)
        # client_w.flush(client, collection_name)
        # assert client_w.num_entities(client, collection_name)[0] == default_nb
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        client_w.search(client, collection_name, vectors_to_search,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": len(vectors_to_search),
                                     "ids": insert_ids,
                                     "limit": default_limit})
        # 4. query
        client_w.query(client, collection_name, filter=default_search_exp,
                       check_task=CheckTasks.check_query_results,
                       check_items={exp_res: rows,
                                    "with_vec": True,
                                    "primary_field": default_primary_key_field_name})
        client_w.release_collection(client, collection_name)
        client_w.drop_collection(client, collection_name, check_task=CheckTasks.err_res,
                                 check_items={ct.err_code: 65535,
                                              ct.err_msg: "cannot drop the collection via alias = collection_alias"})
        client_w.drop_alias(client, alias)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue 1891, 1892")
    def test_milvus_client_alias_default(self):
        """
        target: test alias (high level api) normal case
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: create alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        alias = cf.gen_unique_str("collection_alias")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        client_w.create_partition(client, collection_name, partition_name)
        partition_name_list = client_w.list_partitions(client, collection_name)[0]
        # 2. create alias
        client_w.create_alias(client, collection_name, alias)
        client_w.describe_alias(client, alias)
        # 3. list alias
        aliases = client_w.list_aliases(client)[0]
        # assert alias in aliases
        # 4. assert collection is equal to alias according to partitions
        partition_name_list_alias = client_w.list_partitions(client, alias)[0]
        assert partition_name_list == partition_name_list_alias
        # 5. drop alias
        client_w.drop_alias(client, alias)
        aliases = client_w.list_aliases(client)[0]
        # assert alias not in aliases
        client_w.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_alias_default(self):
        """
        target: test alter alias (high level api)
        method: create connection, collection, partition, alias, and assert collection
                is equal to alias according to partitions
        expected: alter alias successfully
        """
        client = self._connect(enable_milvus_client_api=True)
        collection_name = cf.gen_unique_str(prefix)
        another_collectinon_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition")
        alias = cf.gen_unique_str("collection_alias")
        another_alias = cf.gen_unique_str("collection_alias_another")
        # 1. create collection
        client_w.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        client_w.create_partition(client, collection_name, partition_name)
        partition_name_list = client_w.list_partitions(client, collection_name)[0]
        client_w.create_collection(client, another_collectinon_name, default_dim, consistency_level="Strong")
        client_w.create_alias(client, another_collectinon_name, another_alias)
        # 2. create alias
        client_w.create_alias(client, collection_name, alias)
        # 3. alter alias
        client_w.alter_alias(client, collection_name, another_alias)
        client_w.describe_alias(client, alias)
        # 3. list alias
        aliases = client_w.list_aliases(client, collection_name)[0]
        # assert alias in aliases
        # assert another_alias in aliases
        # 4. assert collection is equal to alias according to partitions
        partition_name_list_alias = client_w.list_partitions(client, another_alias)[0]
        assert partition_name_list == partition_name_list_alias
        client_w.drop_alias(client, alias)
        client_w.drop_alias(client, another_alias)
        client_w.drop_collection(client, collection_name)
