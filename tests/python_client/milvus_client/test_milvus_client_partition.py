import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus.client.types import LoadState


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

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_after_disconnect(self):
        """
        target: test release partition without connection
        method: release partition with correct params, with a disconnected instance
        expected: release raise exception
        """
        client_temp = self._client(alias="client_release_partition")
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # Create collection and partition
        self.create_collection(client_temp, collection_name, default_dim)
        self.create_partition(client_temp, collection_name, partition_name)
        # Load partition first
        self.load_partitions(client_temp, collection_name, [partition_name])
        # Close the client connection
        self.close(client_temp)
        # Try to release partition after disconnect - should raise exception
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.release_partitions(client_temp, collection_name, partition_name,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_load_release_partition_after_collection_drop(self):
        """
        target: test load and release partition after collection drop
        method: load and release the partition after the collection has been dropped
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        description = cf.gen_unique_str("desc_")
        # 1. Create collection and partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name, description=description)
        # 2. Drop the collection
        self.drop_collection(client, collection_name)
        # 3. Try to load partition after collection drop - should raise exception
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.load_partitions(client, collection_name, partition_name,
                           check_task=CheckTasks.err_res, check_items=error)
        # 4. Try to release partition after collection drop - should raise exception
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
        target: test releasing a partition that has not been loaded
        method: create a collection and a partition, do not load the partition, then release the partition twice
        expected: releasing an unloaded partition should succeed and be idempotent
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
        target: Test releasing partitions after the collection has been released (unloaded)
        method: 1. Create a collection and a partition
                2. Release the entire collection (unload)
                3. Attempt to release the partition after the collection is unloaded
        expected: Releasing a partition after the collection is unloaded should succeed and be idempotent
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
        error = {ct.err_code: 1100, ct.err_msg: f"Invalid collection name: {name}. the first character "
                                                f"of a collection name must be an underscore or letter"}
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
        error = {ct.err_code: 1100, ct.err_msg: f"the length of a collection name must be less than 255 characters: "
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
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={name}]"}
        self.load_partitions(client, collection_name, name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_partition_not_existed(self):
        """
        target: test loading a non-existent partition
        method: create a collection, then attempt to load a partition that does not exist
        expected: returns an error indicating the partition is not found
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        partition_name = cf.gen_unique_str("nonexisted")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={partition_name}]"}
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
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={partition_name}]"}
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partition_after_disconnect(self):
        """
        target: test load partition without connection
        method: load partition with correct params, with a disconnected instance
        expected: load raises exception
        """
        client_temp = self._client(alias="client_load_partition")
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition")
        # Create collection and partition
        self.create_collection(client_temp, collection_name, default_dim)
        self.create_partition(client_temp, collection_name, partition_name)
        # Load partition first
        self.load_partitions(client_temp, collection_name, [partition_name])
        # Close the client connection
        self.close(client_temp)
        # Try to release partition after disconnect - should raise exception
        error = {ct.err_code: 1, ct.err_msg: 'should create connection first'}
        self.load_partitions(client_temp, collection_name, partition_name,
                             check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientLoadPartitionValid(TestMilvusClientV2Base):
    """ Test case of load partition valid cases """

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
    def test_milvus_client_load_unloaded_default_partition(self):
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

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize('binary_index_type', ct.binary_supported_index_types)
    @pytest.mark.parametrize('metric_type', ct.binary_metrics)
    def test_milvus_client_load_partition_after_index_binary(self, binary_index_type, metric_type):
        """
        target: test load binary collection partition after index created
        method: insert and create index, load binary collection partition with correct params
        expected: no error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. Create collection with binary vector
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)
        # 2. Create partition
        self.create_partition(client, collection_name, partition_name)
        # 3. Insert some data
        schema_info = self.describe_collection(client, collection_name)[0]
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema_info)
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        # 4. Create binary index
        index_params = self.prepare_index_params(client)[0]
        binary_index = {"index_type": binary_index_type, "metric_type": metric_type, "params": {"nlist": 128}}
        # Handle special case for BIN_IVF_FLAT with structure metrics
        if binary_index_type == "BIN_IVF_FLAT" and metric_type in ct.structure_metrics:
            # This combination should raise an error, so create with default instead
            error = {ct.err_code: 65535,
                     ct.err_msg: f"metric type {metric_type} not found or not supported, supported: [HAMMING JACCARD]"}
            index_params.add_index(field_name=ct.default_binary_vec_field_name, 
                                 index_type=binary_index_type, metric_type=metric_type, params={"nlist": 128})
            self.create_index(client, collection_name, index_params,
                            check_task=CheckTasks.err_res, check_items=error)
            # Create with default binary index instead
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(field_name=ct.default_binary_vec_field_name, **ct.default_bin_flat_index)
            self.create_index(client, collection_name, index_params)
        else:
            index_params.add_index(field_name=ct.default_binary_vec_field_name, **binary_index)
            self.create_index(client, collection_name, index_params)
        # 5. Load the partition
        self.release_collection(client, collection_name)
        self.load_partitions(client, collection_name, [partition_name])
        # 6. Verify partition is loaded by checking load state
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.Loaded
        self.drop_collection(client, collection_name)
