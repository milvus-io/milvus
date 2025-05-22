import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "milvus_client_api_partition"
partition_prefix = "milvus_client_api_partition"
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


class TestMilvusClientPartitionInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_partition_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.create_partition(client, collection_name, partition_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partition_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        self.create_partition(client, collection_name, partition_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        self.create_partition(client, collection_name, partition_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        self.create_partition(client, collection_name, partition_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.create_partition(client, collection_name, partition_names,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Take much running time")
    def test_milvus_client_create_over_max_partition_num(self):
        """
        target: test create more than maximum partitions
        method: create 4097 partitions
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_nums = 4095
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        for i in range(partition_nums):
            partition_name = cf.gen_unique_str(partition_prefix)
            # 2. create partition
            self.create_partition(client, collection_name, partition_name)
        results = self.list_partitions(client, collection_name)[0]
        assert len(results) == partition_nums + 1
        partition_name = cf.gen_unique_str(partition_prefix)
        error = {ct.err_code: 65535, ct.err_msg: f"partition number (4096) exceeds max configuration (4096), "
                                                 f"collection: {collection_name}"}
        self.create_partition(client, collection_name, partition_name,
                              check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientPartitionValid(TestMilvusClientV2Base):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["COSINE", "L2", "IP"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["int", "string"])
    def id_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="pymilvus issue 1880")
    def test_milvus_client_partition_default(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        index = self.list_indexes(client, collection_name)[0]
        assert index == ['vector']
        # load_state = self.get_load_state(collection_name)[0]
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(client, collection_name, vectors_to_search,
                    partition_names=partitions,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "ids": insert_ids,
                                 "pk_name": default_primary_key_field_name,
                                 "limit": default_limit})
        # 4. query
        res = self.query(client, collection_name, filter=default_search_exp,
                         output_fields=["vector"], partition_names=partitions,
                         check_task=CheckTasks.check_query_results,
                         check_items={exp_res: rows,
                                      "with_vec": True,
                                      "pk_name": default_primary_key_field_name})[0]

        assert set(res[0].keys()) == {"ids", "vector"}
        partition_number = self.get_partition_stats(client, collection_name, "_default")[0]
        assert partition_number == default_nb
        partition_number = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_number[0]['value'] == 0
        if self.has_partition(client, collection_name, partition_name)[0]:
            self.release_partitions(client, collection_name, partition_name)
            self.drop_partition(client, collection_name, partition_name)
        if self.has_collection(client, collection_name)[0]:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_create_partition_name_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create partition successfully with only one partition created
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, "_default")
        results = self.list_partitions(client, collection_name)[0]
        assert len(results) == 1
        self.create_partition(client, collection_name, partition_name)
        results = self.list_partitions(client, collection_name)[0]
        assert len(results) == 2
        self.create_partition(client, collection_name, partition_name)
        results = self.list_partitions(client, collection_name)[0]
        assert len(results) == 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_partition_not_exist_partition(self):
        """
        target: test drop not exist partition
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.drop_partition(client, collection_name, partition_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_partition_collection_partition_not_match(self):
        """
        target: test drop partition in another collection
        method: drop partition in another collection
        expected: drop successfully without any operations
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        another_collection_name = cf.gen_unique_str("another")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        self.create_collection(client, another_collection_name, default_dim)
        self.drop_partition(client, another_collection_name, partition_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_partition_collection_partition_not_match(self):
        """
        target: test drop partition in another collection
        method: drop partition in another collection
        expected: drop successfully without any operations
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        another_collection_name = cf.gen_unique_str("another")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        self.create_collection(client, another_collection_name, default_dim)
        result = self.has_partition(client, another_collection_name, partition_name)[0]
        assert result is False


class TestMilvusClientDropPartitionInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_drop_partition_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.drop_partition(client, collection_name, partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_partition_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        self.drop_partition(client, collection_name, partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        self.drop_partition(client, collection_name, partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_drop_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}."}
        self.drop_partition(client, collection_name, partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.drop_partition(client, collection_name, partition_names,
                            check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleasePartitionInvalid(TestMilvusClientV2Base):
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("collection_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_release_partition_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 999, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                               f"collection name must be an underscore or letter: invalid parameter"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 999,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_not_exist_collection_name(self):
        """
        target: test release partition -- not exist collection name
        method: release partition with not exist collection name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 999, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 1896")
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_release_partition_invalid_partition_name(self, partition_name):
        """
        target: test release partition -- invalid partition name value
        method: release partition with invalid partition name value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
                                                 f"partition name must be an underscore or letter.]"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 1896")
    def test_milvus_client_release_partition_invalid_partition_name_list(self):
        """
        target: test release partition -- invalid partition name value
        method: release partition with invalid partition name value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        partition_name = ["12-s"]
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
                                                 f"partition name must be an underscore or letter.]"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_name_lists_empty(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_names = []
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"invalid parameter[expected=any partition][actual=empty partition list"}
        self.release_partitions(client, collection_name, partition_names,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_name_lists_not_all_exists(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        not_exist_partition = cf.gen_unique_str("partition_not_exist")
        partition_names = ["_default", not_exist_partition]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"partition not found[partition={not_exist_partition}]"}
        self.release_partitions(client, collection_name, partition_names,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/38223")
    def test_milvus_client_release_not_exist_partition_name(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 2. create partition
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.create_collection(client, collection_name, default_dim)
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)
        partition_name = ""
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.release_partitions(client, collection_name, partition_name,
                                check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleasePartitionValid(TestMilvusClientV2Base):
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
    def test_milvus_client_partition_release_multiple_partitions(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        partition_names = ["_default", partition_name]
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.release_partitions(client, collection_name, partition_names)
        self.release_partitions(client, collection_name, partition_names)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_partition_release_unloaded_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.release_partitions(client, collection_name, partition_name)
        self.release_partitions(client, collection_name, partition_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_partition_release_unloaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_partition_release_loaded_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.load_partitions(client, collection_name, partition_name)
        self.release_partitions(client, collection_name, partition_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_partition_release_loaded_collection(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_name)
        self.drop_collection(client, collection_name)


class TestMilvusClientListPartitionInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_list_partitions_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        # 2. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.list_partitions(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_list_partitions_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        # 2. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        self.list_partitions(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_list_partitions_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("partition_not_exist")
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        self.list_partitions(client, collection_name,
                             check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientHasPartitionInvalid(TestMilvusClientV2Base):
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
    def test_milvus_client_has_partition_invalid_collection_name(self, collection_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
                                                f"collection name must be an underscore or letter: invalid parameter"}
        self.has_partition(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
                             f"must be less than 255 characters: invalid parameter"}
        self.has_partition(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("partition_not_exist")
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default]"
                                               f"[collection={collection_name}]"}
        self.has_partition(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_has_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        self.has_partition(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = "a".join("a" for i in range(256))
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. "
                                                 f"The length of a partition name must be less than 255 characters"}
        self.has_partition(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.has_partition(client, collection_name, partition_names,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_not_exist_partition_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. create partition
        result = self.has_partition(client, collection_name, partition_name)[0]
        assert result == False


class TestMilvusClientLoadPartitionInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_load_partitions_invalid_collection_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        self.load_partitions(client, name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str("nonexisted")
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default]"
                                                f"[collection={collection_name}]"}
        self.load_partitions(client, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_collection_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = "a".join("a" for i in range(256))
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        self.load_partitions(client, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_load_partitions_invalid_partition_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid partition name: {name}. collection name can only "
                                                f"contain numbers, letters and underscores: invalid parameter"}
        self.load_partitions(client, collection_name, name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_partition_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("nonexisted")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[database=default]"
                                                f"[collection={collection_name}]"}
        self.load_partitions(client, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_partition_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = "a".join("a" for i in range(256))
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"invalid dimension: {collection_name}. "
                                                f"the length of a collection name must be less than 255 characters: "
                                                f"invalid parameter"}
        self.load_partitions(client, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_without_index(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. drop index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. load partition
        error = {ct.err_code: 700, ct.err_msg: f"index not found[collection={collection_name}]"}
        self.load_partitions(client, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientLoadPartitionInvalid(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_multiple_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        partition_names = ["_default", partition_name]
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        # 2. load partition
        self.load_partitions(client, collection_name, partition_names)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_unloaded_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        # 2. load partition
        self.load_partitions(client, collection_name, partition_name)
        self.load_partitions(client, collection_name, "_default")

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_unloaded_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self.create_partition(client, collection_name, partition_name)
        self.release_collection(client, collection_name)
        # 2. load partition
        self.load_partitions(client, collection_name, partition_name)
        self.load_partitions(client, collection_name, partition_name)
        self.load_collection(client, collection_name)
        self.load_partitions(client, collection_name, partition_name)
