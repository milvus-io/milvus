import threading
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
default_search_string_exp = 'varchar >= "0"'
default_search_mix_exp = 'int64 >= 0 && varchar >= "0"'
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = 'json_field["number"] >= 0'
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
    """Test case of search interface"""

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
            f"collection name must be an underscore or letter: invalid parameter",
        }
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
            f"must be less than 255 characters: invalid parameter",
        }
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.create_partition(
            client, collection_name, partition_names, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Take much running time")
    def test_milvus_client_create_over_max_partition_num(self):
        """
        target: test create more than maximum partitions
        method: create 4097 partitions
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        error = {
            ct.err_code: 65535,
            ct.err_msg: f"partition number (4096) exceeds max configuration (4096), collection: {collection_name}",
        }
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientPartitionValid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        index = self.list_indexes(client, collection_name)[0]
        assert index == ["vector"]
        # load_state = self.get_load_state(collection_name)[0]
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, default_dim))[0]),
                default_float_field_name: i * 1.0,
                default_string_field_name: str(i),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        # 3. search
        vectors_to_search = rng.random((1, default_dim))
        insert_ids = [i for i in range(default_nb)]
        self.search(
            client,
            collection_name,
            vectors_to_search,
            partition_names=partitions,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": len(vectors_to_search),
                "ids": insert_ids,
                "pk_name": default_primary_key_field_name,
                "limit": default_limit,
            },
        )
        # 4. query
        res = self.query(
            client,
            collection_name,
            filter=default_search_exp,
            output_fields=["vector"],
            partition_names=partitions,
            check_task=CheckTasks.check_query_results,
            check_items={exp_res: rows, "with_vec": True, "pk_name": default_primary_key_field_name},
        )[0]

        assert set(res[0].keys()) == {"ids", "vector"}
        partition_number = self.get_partition_stats(client, collection_name, "_default")[0]
        assert partition_number == default_nb
        partition_number = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert partition_number[0]["value"] == 0
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, "_default")
        results = self.list_partitions(client, collection_name)[0]
        assert len(results) == 1
        self.create_partition(client, collection_name, partition_name)
        results1 = self.list_partitions(client, collection_name)[0]
        assert len(results1) == 2
        assert partition_name in results1
        self.create_partition(client, collection_name, partition_name)
        results2 = self.list_partitions(client, collection_name)[0]
        assert len(results2) == 2
        assert partition_name in results2
        # verify partition list is identical after duplicate creation
        assert results1 == results2

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_partition_not_exist_partition(self):
        """
        target: test drop not exist partition
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        another_collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        another_collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        self.create_collection(client, another_collection_name, default_dim)
        result = self.has_partition(client, another_collection_name, partition_name)[0]
        assert result is False


class TestMilvusClientDropPartitionInvalid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
            f"collection name must be an underscore or letter: invalid parameter",
        }
        self.drop_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
            f"must be less than 255 characters: invalid parameter",
        }
        self.drop_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.drop_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_drop_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}."}
        self.drop_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_drop_partition_none_partition_name(self):
        """
        target: test drop partition with None partition name
        method: call drop_partition with partition_name=None
        expected: raise ParamError
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "`partition_name` value None is illegal"}
        self.drop_partition(client, collection_name, None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.drop_partition(client, collection_name, partition_names, check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientReleasePartitionInvalid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        error = {
            ct.err_code: 999,
            ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
            f"collection name must be an underscore or letter: invalid parameter",
        }
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

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
        error = {
            ct.err_code: 999,
            ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
            f"must be less than 255 characters: invalid parameter",
        }
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_not_exist_collection_name(self):
        """
        target: test release partition -- not exist collection name
        method: release partition with not exist collection name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 999, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

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
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {
            ct.err_code: 65535,
            ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
            f"partition name must be an underscore or letter.]",
        }
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 1896")
    def test_milvus_client_release_partition_invalid_partition_name_list(self):
        """
        target: test release partition -- invalid partition name value
        method: release partition with invalid partition name value
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        partition_name = ["12-s"]
        error = {
            ct.err_code: 65535,
            ct.err_msg: f"Invalid partition name: {partition_name}. The first character of a "
            f"partition name must be an underscore or letter.]",
        }
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_name_lists_empty(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_names = []
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"invalid parameter[expected=any partition][actual=empty partition list"}
        self.release_partitions(
            client, collection_name, partition_names, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_release_partition_name_lists_not_all_exists(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        not_exist_partition = cf.gen_unique_str("partition_not_exist")
        partition_names = ["_default", not_exist_partition]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 999, ct.err_msg: f"partition not found[partition={not_exist_partition}]"}
        self.release_partitions(
            client, collection_name, partition_names, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/38223")
    def test_milvus_client_release_not_exist_partition_name(self):
        """
        target: test fast release partition -- invalid partition name type
        method: release partition with invalid partition name type
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 2. create partition
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.create_collection(client, collection_name, default_dim)
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )
        partition_name = ""
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

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
        error = {ct.err_code: 1, ct.err_msg: "should create connection first"}
        self.release_partitions(
            client_temp, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )

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
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)
        # 4. Try to release partition after collection drop - should raise exception
        self.release_partitions(
            client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )


class TestMilvusClientReleasePartitionValid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. create partition
        self.create_partition(client, collection_name, partition_name)
        self.load_collection(client, collection_name)
        self.release_partitions(client, collection_name, partition_name)
        self.drop_collection(client, collection_name)


class TestMilvusClientListPartitionInvalid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
            f"collection name must be an underscore or letter: invalid parameter",
        }
        self.list_partitions(client, collection_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
            f"must be less than 255 characters: invalid parameter",
        }
        self.list_partitions(client, collection_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_list_partitions_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.list_partitions(client, collection_name, check_task=CheckTasks.err_res, check_items=error)


class TestMilvusClientHasPartitionInvalid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the first character of a "
            f"collection name must be an underscore or letter: invalid parameter",
        }
        self.has_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {collection_name}. the length of a collection name "
            f"must be less than 255 characters: invalid parameter",
        }
        self.has_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_not_exist_collection_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 2. create partition
        error = {ct.err_code: 100, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.has_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_has_partition_invalid_partition_name(self, partition_name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}"}
        self.has_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_has_partition_none_partition_name(self):
        """
        target: test has_partition with None partition name
        method: call has_partition with partition_name=None
        expected: raise ParamError
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "`partition_name` value None is illegal"}
        self.has_partition(client, collection_name, None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = "a".join("a" for i in range(256))
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {
            ct.err_code: 65535,
            ct.err_msg: f"Invalid partition name: {partition_name}. "
            f"The length of a partition name must be less than 255 characters",
        }
        self.has_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_name_lists(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_names = [cf.gen_unique_str(partition_prefix), cf.gen_unique_str(partition_prefix)]
        # 2. create partition
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_names} is illegal"}
        self.has_partition(client, collection_name, partition_names, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_has_partition_not_exist_partition_name(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("partition_not_exist")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. create partition
        result = self.has_partition(client, collection_name, partition_name)[0]
        assert result == False


class TestMilvusClientLoadPartitionInvalid(TestMilvusClientV2Base):
    """Test case of search interface"""

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"Invalid collection name: {name}. the first character "
            f"of a collection name must be an underscore or letter",
        }
        self.load_partitions(client, name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_not_existed(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1100, ct.err_msg: f"collection not found[database=default][collection={collection_name}]"}
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"the length of a collection name must be less than 255 characters: invalid parameter",
        }
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["12 s", "(mn)", "中文", "%$#"])
    def test_milvus_client_load_partitions_invalid_partition_name(self, name):
        """
        target: test fast create collection normal case
        method: create collection
        expected: create collection with default schema, index, and load successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={name}]"}
        self.load_partitions(client, collection_name, name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_partition_not_existed(self):
        """
        target: test loading a non-existent partition
        method: create a collection, then attempt to load a partition that does not exist
        expected: returns an error indicating the partition is not found
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str("nonexisted")
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_partition_name_over_max_length(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = "a".join("a" for i in range(256))
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. load partition
        error = {ct.err_code: 1100, ct.err_msg: f"partition not found[partition={partition_name}]"}
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_load_partitions_without_index(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        # 2. drop index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")
        # 2. load partition
        error = {ct.err_code: 700, ct.err_msg: f"index not found[collection={collection_name}]"}
        self.load_partitions(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {ct.err_code: 1, ct.err_msg: "should create connection first"}
        self.load_partitions(
            client_temp, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error
        )


class TestMilvusClientLoadPartitionValid(TestMilvusClientV2Base):
    """Test case of load partition valid cases"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_load_multiple_partition(self):
        """
        target: test fast create collection normal case
        method: create collection
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        collection_name = cf.gen_collection_name_by_testcase_name()
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
    @pytest.mark.parametrize("binary_index_type", ct.binary_supported_index_types)
    @pytest.mark.parametrize("metric_type", ct.binary_metrics)
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
            error = {
                ct.err_code: 65535,
                ct.err_msg: f"metric type {metric_type} not found or not supported, supported: [HAMMING JACCARD]",
            }
            index_params.add_index(
                field_name=ct.default_binary_vec_field_name,
                index_type=binary_index_type,
                metric_type=metric_type,
                params={"nlist": 128},
            )
            self.create_index(client, collection_name, index_params, check_task=CheckTasks.err_res, check_items=error)
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


class TestPartitionParams(TestMilvusClientV2Base):
    """Test case of partition interface in parameters"""

    @pytest.fixture(scope="function")
    def default_schema(self):
        def _create(client):
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(ct.default_float_field_name, DataType.FLOAT)
            schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
            schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
            return schema

        yield _create

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default(self, default_schema):
        """
        target: verify create a partition
        method: create a partition
        expected: create successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        assert self.has_partition(client, collection_name, partition_name)[0]
        # verify partition is empty with 0 entities
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", [""])
    def test_partition_empty_name(self, partition_name):
        """
        target: verify create a partition with empty name
        method: create a partition with empty name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "Partition name should not be empty"}
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default_name(self):
        """
        target: verify create a partition with default name
        method: 1. get the _default partition
                2. create a partition with _default name
        expected: the same partition returned
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        # check that the default partition exists
        assert self.has_partition(client, collection_name, ct.default_partition_name)[0] is True
        # check that list partitions contains _default
        partitions = self.list_partitions(client, collection_name)[0]
        assert ct.default_partition_name in partitions

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_max_length_name(self):
        """
        target: verify create a partition with max length(256) name
        method: create a partition with max length name
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        partition_name = cf.gen_str_by_length(256)
        error = {
            ct.err_code: 65535,
            ct.err_msg: f"Invalid partition name: {partition_name}. "
            f"The length of a partition name must be less "
            f"than 255 characters.",
        }
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["_Partiti0n", "pArt1_ti0n"])
    def test_partition_naming_rules(self, partition_name):
        """
        target: test partition naming rules
        method: 1. Create a collection
                2. Create a partition with a name which uses all the supported elements in the naming rules
        expected: Partition create successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0] is True

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ct.invalid_resource_names)
    def test_partition_invalid_name(self, partition_name):
        """
        target: verify create a partition with invalid name
        method: create a partition with invalid names
        expected: raise exception
        """
        if partition_name == "12name":
            pytest.skip(reason="won't fix issue #32998")
        if partition_name == "n-ame":
            pytest.skip(reason="https://github.com/milvus-io/milvus/issues/39432")
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        if partition_name is not None:
            error = {ct.err_code: 999, ct.err_msg: f"Invalid partition name: {partition_name.strip()}"}
        else:
            error = {ct.err_code: 999, ct.err_msg: f"`partition_name` value {partition_name} is illegal"}
        self.create_partition(client, collection_name, partition_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop(self, default_schema):
        """
        target: verify drop a partition in one collection
        method: 1. create a partition in one collection
                2. drop the partition
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0] is True
        self.drop_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0] is False

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_respectively(self, default_schema):
        """
        target: test release the partition after load partition
        method: load partition1 and load another partition
        expected: raise no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str(partition_prefix)
        partition_name2 = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name1)
        self.insert(client, collection_name, data, partition_name=partition_name2)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load partitions respectively
        self.load_partitions(client, collection_name, [partition_name1])
        self.load_partitions(client, collection_name, [partition_name2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_release_partitions_after_load(self, default_schema):
        """
        target: test release the partition after load partition
        method: load partitions and release partitions
        expected: no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = "partition_w1"
        partition_name2 = "partition_w2"
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name1)
        self.insert(client, collection_name, data, partition_name=partition_name2)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load partitions
        partition_names = [partition_name1, partition_name2]
        self.load_partitions(client, collection_name, partition_names)
        # release partitions
        self.release_partitions(client, collection_name, partition_names)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_after_load_partition(self, default_schema):
        """
        target: test release the partition after load partition
        method: load partition1 and release the partition1, then load partition2
        expected: no exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str(partition_prefix)
        partition_name2 = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name1)
        self.insert(client, collection_name, data, partition_name=partition_name2)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load partition1
        self.load_partitions(client, collection_name, [partition_name1])
        # release partition1
        self.release_partitions(client, collection_name, [partition_name1])
        # load partition2
        self.load_partitions(client, collection_name, [partition_name2])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [1.2, "not-int"])
    def test_load_partition_replica_non_number(self, default_schema, replicas):
        """
        target: test load partition with non-number replicas
        method: load with non-number replicas
        expected: raise exceptions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        # insert data
        data = cf.gen_default_rows_data(nb=100, with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load with non-number replicas
        error = {ct.err_code: 0, ct.err_msg: f"`replica_number` value {replicas} is illegal"}
        self.load_partitions(
            client,
            collection_name,
            [partition_name],
            replica_number=replicas,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [0, -1])
    def test_load_replica_invalid_number(self, default_schema, replicas):
        """
        target: test load partition with 0 and negative number
        method: load with 0 or -1
        expected: load successful
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        assert self.get_partition_stats(client, collection_name, partition_name)[0]["row_count"] == ct.default_nb
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load with invalid replica number
        self.load_partitions(client, collection_name, [partition_name], replica_number=replicas)
        # verify replica count
        replicas_info = self.describe_replica(client, collection_name)[0]
        assert len(replicas_info) == 1
        # verify query works
        query_res = self.query(
            client, collection_name, filter=f"{ct.default_int64_field_name} in [0]", partition_names=[partition_name]
        )[0]
        assert len(query_res) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_replica_greater_than_querynodes(self, default_schema):
        """
        target: test load with replicas that greater than querynodes
        method: load with 3 replicas (2 querynode)
        expected: Verify load fails with resource insufficient error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        assert self.get_partition_stats(client, collection_name, partition_name)[0]["row_count"] == ct.default_nb
        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # load with 3 replicas
        error = {
            ct.err_code: 65535,
            ct.err_msg: "when load 3 replica count: service resource insufficient"
            "[currentStreamingNode=2][expectedStreamingNode=3]",
        }
        self.load_partitions(
            client,
            collection_name,
            [partition_name],
            replica_number=3,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_change(self, default_schema):
        """
        target: test load replica change
        method: 1.load with replica 1
                2.load with a new replica number
                3.release partition
                4.load with a new replica
                5.create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
        expected: The second time successfully loaded with a new replica number
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        assert self.get_partition_stats(client, collection_name, partition_name)[0]["row_count"] == ct.default_nb
        # create index (IVF_SQ8, same as v1 ct.default_index)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="COSINE",
            params={"nlist": 128},
        )
        self.create_index(client, collection_name, index_params)
        # load with replica 1
        self.load_partitions(client, collection_name, [partition_name], replica_number=1)
        self.query(
            client,
            collection_name,
            filter=f"{ct.default_int64_field_name} in [0]",
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": [data[0]], "pk_name": ct.default_int64_field_name},
        )
        # load with replica 2 without release — should fail
        error = {
            ct.err_code: 1100,
            ct.err_msg: "can't change the replica number for loaded partitions: "
            "invalid parameter[expected=1][actual=2]",
        }
        self.load_partitions(
            client,
            collection_name,
            [partition_name],
            replica_number=2,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        # release then load with replica 2
        self.release_partitions(client, collection_name, [partition_name])
        self.load_partitions(client, collection_name, [partition_name], replica_number=2)
        self.query(
            client,
            collection_name,
            filter=f"{ct.default_int64_field_name} in [0]",
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": [data[0]], "pk_name": ct.default_int64_field_name},
        )
        # verify replica count
        two_replicas = self.describe_replica(client, collection_name)[0]
        assert len(two_replicas) == 2
        # verify loaded segments included 2 replicas and twice num entities
        seg_info = client._get_connection().get_query_segment_info(collection_name)
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release(self, default_schema):
        """
        target: verify release partition
        method: 1. create a collection and two partitions
                2. insert data into each partition
                3. flush and load the partition1
                4. release partition1
                5. release partition2
        expected: 1. the 1st partition is released
                  2. the 2nd partition is released
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str(partition_prefix)
        partition_name2 = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        # create index (FLAT, same as v1 ct.default_flat_index)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        # create two partitions
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        # insert data
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name1)
        self.insert(client, collection_name, data, partition_name=partition_name2)
        # load partition1
        self.load_partitions(client, collection_name, [partition_name1])
        # search partition1
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res = self.search(
            client,
            collection_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params={"nprobe": 32},
            limit=1,
            partition_names=[partition_name1],
        )[0]
        assert len(res) == 1
        # release both partitions
        self.release_partitions(client, collection_name, [partition_name1])
        self.release_partitions(client, collection_name, [partition_name2])
        # search after release — should fail
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        self.search(
            client,
            collection_name,
            data=search_vectors,
            anns_field=ct.default_float_vec_field_name,
            search_params={"nprobe": 32},
            limit=1,
            partition_names=[partition_name1],
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert(self, default_schema):
        """
        target: verify insert entities multiple times
        method: 1. create a collection and a partition
                2. partition.insert(data)
                3. insert data again
        expected: insert data successfully
        """
        nums = 10
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0] is True
        # insert data
        data = cf.gen_default_rows_data(nb=nums, with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        assert self.get_partition_stats(client, collection_name, partition_name)[0]["row_count"] == nums
        # insert data again
        self.insert(client, collection_name, data, partition_name=partition_name)
        self.flush(client, collection_name)
        assert self.get_partition_stats(client, collection_name, partition_name)[0]["row_count"] == nums + nums

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_partition_replicas_change_cross_partitions(self, default_schema):
        """
        target: test load with different replicas between partitions
        method: 1.Create two partitions and insert data
                2.Create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
                3.Load two partitions with different replicas
        expected: Raise an exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_name1 = cf.gen_unique_str(partition_prefix)
        partition_name2 = cf.gen_unique_str(partition_prefix)
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        self.create_partition(client, collection_name, partition_name1)
        self.create_partition(client, collection_name, partition_name2)
        # insert data
        data1 = cf.gen_default_rows_data(with_json=False)
        data2 = cf.gen_default_rows_data(start=ct.default_nb, with_json=False)
        self.insert(client, collection_name, data1, partition_name=partition_name1)
        self.insert(client, collection_name, data2, partition_name=partition_name2)
        self.flush(client, collection_name)
        stats1 = self.get_partition_stats(client, collection_name, partition_name1)[0]["row_count"]
        stats2 = self.get_partition_stats(client, collection_name, partition_name2)[0]["row_count"]
        assert stats1 + stats2 == ct.default_nb * 2
        # create index (IVF_SQ8, same as v1 ct.default_index)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="COSINE",
            params={"nlist": 128},
        )
        self.create_index(client, collection_name, index_params)
        # load with different replicas
        self.load_partitions(client, collection_name, [partition_name1], replica_number=1)
        self.release_partitions(client, collection_name, [partition_name1])
        self.load_partitions(client, collection_name, [partition_name2], replica_number=2)
        # verify replica count — v2 describe_replica is collection-level
        # v1 had per-partition get_replicas(), but both return the same replica groups
        replicas_info = self.describe_replica(client, collection_name)[0]
        assert len(replicas_info) == 2
        # verify loaded segments included 2 replicas and 1 partition
        seg_info = client._get_connection().get_query_segment_info(collection_name)
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb


class TestPartitionOperations(TestMilvusClientV2Base):
    """Test case of partition interface in operations"""

    @pytest.fixture(scope="function")
    def default_schema(self):
        def _create(client):
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(ct.default_float_field_name, DataType.FLOAT)
            schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
            schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
            return schema

        yield _create

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_dropped_collection(self, default_schema):
        """
        target: verify create partition against a dropped collection
        method: 1. create a collection
                2. drop collection
                3. create partition in collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        # drop collection
        self.drop_collection(client, collection_name)
        # create partition failed
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(
            client,
            collection_name,
            partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 4, ct.err_msg: "collection not found"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_same_name_in_diff_collections(self, default_schema):
        """
        target: verify create partitions with same name in diff collections
        method: 1. create a partition in collection1
                2. create a partition in collection2
        expected: create successfully
        """
        client = self._client()
        collection_name1 = cf.gen_collection_name_by_testcase_name()
        collection_name2 = cf.gen_collection_name_by_testcase_name()
        schema1 = default_schema(client)
        self.create_collection(client, collection_name1, schema=schema1)
        schema2 = default_schema(client)
        self.create_collection(client, collection_name2, schema=schema2)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name1, partition_name)
        self.create_partition(client, collection_name2, partition_name)

        assert self.has_partition(client, collection_name1, partition_name)[0]
        assert self.has_partition(client, collection_name2, partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_multi_partitions_in_collection(self, default_schema):
        """
        target: verify create multiple partitions in one collection
        method: create multiple partitions in one collection
        expected: create successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        for _ in range(10):
            partition_name = cf.gen_unique_str(prefix)
            self.create_partition(client, collection_name, partition_name)
            assert self.has_partition(client, collection_name, partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_maximum_partitions(self, default_schema):
        """
        target: verify create maximum partitions
        method: 1. create maximum partitions
                2. create one more partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        threads_num = 8
        threads = []

        def create_partition(threads_n):
            for _ in range(ct.max_partition_num // threads_n):
                name = cf.gen_unique_str(prefix)
                self.create_partition(client, collection_name, name, check_task=CheckTasks.check_nothing)

        for _ in range(threads_num):
            t = threading.Thread(target=create_partition, args=(threads_num,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        partitions = self.list_partitions(client, collection_name)[0]
        log.info(f"partitions: {len(partitions)}")
        p_name = cf.gen_unique_str()
        err_msg = f"partition number ({ct.max_partition_num}) exceeds max configuration ({ct.max_partition_num})"
        self.create_partition(
            client,
            collection_name,
            p_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: err_msg},
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_drop_default_partition(self, default_schema):
        """
        target: verify drop the _default partition
        method: drop the _default partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # verify _default partition exists
        assert self.has_partition(client, collection_name, ct.default_partition_name)[0]

        # drop _default partition should fail
        self.drop_partition(
            client,
            collection_name,
            ct.default_partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "default partition cannot be deleted"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop_partition_twice(self, default_schema):
        """
        target: verify drop the same partition twice
        method: 1.create a partition with default schema
                2. drop the partition
                3. drop the same partition again
        expected: drop successfully both times (idempotent)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # drop partition
        self.drop_partition(client, collection_name, partition_name)
        assert not self.has_partition(client, collection_name, partition_name)[0]

        # drop the partition again (idempotent)
        self.drop_partition(client, collection_name, partition_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_create_and_drop_multi_times(self, default_schema):
        """
        target: verify create and drop for times
        method: 1. create a partition with default schema
                2. drop the partition
                3. loop #1 and #2 for times
        expected: create and drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        for _ in range(5):
            self.create_partition(client, collection_name, partition_name)
            assert self.has_partition(client, collection_name, partition_name)[0]

            self.drop_partition(client, collection_name, partition_name)
            assert not self.has_partition(client, collection_name, partition_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop_non_empty_partition(self, default_schema):
        """
        target: verify drop a partition which has data inserted
        method: 1. create a partition with default schema
                2. insert some data
                3. search and verify data exists
                4. drop the partition
                5. search and verify data is gone
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # insert data to partition
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)

        # create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.load_partitions(client, collection_name, [partition_name])

        # search and verify data exists
        vectors = cf.gen_vectors(1, ct.default_dim)
        res = self.search(client, collection_name, vectors, limit=1, partition_names=[partition_name])[0]
        assert len(res) == 1
        assert len(res[0]) == 1

        # release before drop
        self.release_partitions(client, collection_name, [partition_name])

        # drop partition
        self.drop_partition(client, collection_name, partition_name)
        assert not self.has_partition(client, collection_name, partition_name)[0]

        # search and verify data is gone
        self.load_collection(client, collection_name)
        res = self.search(client, collection_name, vectors, limit=1)[0]
        assert len(res) == 1
        assert len(res[0]) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_drop_indexed_partition(self, default_schema):
        """
        target: verify drop an indexed partition
        method: 1. create a partition
                2. insert same data
                3. create an index
                4. drop the partition
        expected: drop successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # insert data to partition
        data = cf.gen_default_rows_data(nb=3000, with_json=False)
        ins_res = self.insert(client, collection_name, data, partition_name=partition_name)[0]
        assert ins_res["insert_count"] == 3000

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="L2",
            params=cf.get_index_params_params("IVF_SQ8"),
        )
        self.create_index(client, collection_name, index_params)

        # drop partition
        self.drop_partition(client, collection_name, partition_name)
        assert not self.has_partition(client, collection_name, partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_empty_partition(self, default_schema):
        """
        target: verify release an empty partition
        method: 1. create a partition
                2. release the partition
        expected: release successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # release partition
        self.release_partitions(client, collection_name, [partition_name])

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_dropped_partition(self, default_schema):
        """
        target: verify release a dropped partition
        method: 1. create a partition
                2. drop the partition
                3. release the partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # drop partition
        self.drop_partition(client, collection_name, partition_name)

        # release the dropped partition and check err response
        self.release_partitions(
            client,
            collection_name,
            [partition_name],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 200, ct.err_msg: "partition not found[partition=%s]" % partition_name},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_dropped_collection(self, default_schema):
        """
        target: verify release a dropped collection
        method: 1. create a collection and partition
                2. drop the collection
                3. release the partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # drop collection
        self.drop_collection(client, collection_name)

        # release the partition and check err response
        self.release_partitions(
            client,
            collection_name,
            [partition_name],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 4, ct.err_msg: "collection not found"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_after_collection_released(self, default_schema):
        """
        target: verify release a partition after the collection released
        method: 1. create a collection and partition
                2. insert some data
                3. release the collection
                4. release the partition
        expected: partition released successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # insert data to partition
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)

        # verify num_entities
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == ct.default_nb
        c_stats = self.get_collection_stats(client, collection_name)[0]
        assert c_stats["row_count"] == ct.default_nb

        # load partition
        self.load_partitions(client, collection_name, [partition_name])

        # search of partition
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res = self.search(client, collection_name, search_vectors, limit=1, partition_names=[partition_name])[0]
        assert len(res) == 1
        assert len(res[0]) == 1

        # release collection
        self.release_collection(client, collection_name)

        # search of partition after collection released
        self.search(
            client,
            collection_name,
            search_vectors,
            limit=1,
            partition_names=[partition_name],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 0, ct.err_msg: "not loaded"},
        )

        # release partition
        self.release_partitions(client, collection_name, [partition_name])

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_default_partition(self, default_schema):
        """
        target: verify insert data into _default partition
        method: 1. create a collection
                2. insert some data into _default partition
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # get the default partition
        partition_name = ct.default_partition_name
        assert self.has_partition(client, collection_name, partition_name)[0]

        # insert data to partition
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(client, collection_name, data, partition_name=partition_name)

        # verify num_entities
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == len(data)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_partition(self, default_schema):
        """
        target: verify insert data into a dropped partition
        method: 1. create a collection
                2. insert some data into a dropped partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # drop partition
        self.drop_partition(client, collection_name, partition_name)

        # insert data to dropped partition
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(
            client,
            collection_name,
            data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 200, ct.err_msg: "partition not found"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_collection(self, default_schema):
        """
        target: verify insert data into a dropped collection
        method: 1. create a collection
                2. insert some data into a dropped collection
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # drop collection
        self.drop_collection(client, collection_name)

        # insert data to partition of dropped collection
        data = cf.gen_default_rows_data(with_json=False)
        self.insert(
            client,
            collection_name,
            data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 100,
                ct.err_msg: f"can't find collection[database=default][collection={collection_name}]",
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_insert_maximum_size_data(self, default_schema):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1. create a partition
                2. insert maximum size data
        expected: insert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # insert data to partition
        max_size = 100000
        data = cf.gen_default_rows_data(nb=max_size, with_json=False)
        ins_res = self.insert(client, collection_name, data, partition_name=partition_name, timeout=40)[0]
        assert ins_res["insert_count"] == max_size

        # verify num_entities
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == max_size

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.default_dim - 1, ct.default_dim + 1])
    def test_partition_insert_mismatched_dimensions(self, default_schema, dim):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1. create a collection with default dim
                2. insert dismatch dim data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # insert data with mismatched dim
        data = cf.gen_default_rows_data(nb=10, dim=dim, with_json=False)
        self.insert(
            client,
            collection_name,
            data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: f"float data should divide the dim({ct.default_dim})"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_delete_indexed_data(self, default_schema):
        """
        target: verify delete entities with an expression condition from an indexed partition
        method: 1. create collection
                2. create an index
                3. create a partition
                4. insert same data
                5. delete entities with an expression condition
        expected: delete successfully
        issue #15456
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=ct.default_float_vec_field_name,
            index_type="IVF_SQ8",
            metric_type="L2",
            params=cf.get_index_params_params("IVF_SQ8"),
        )
        self.create_index(client, collection_name, index_params)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # insert data to partition
        data = cf.gen_default_rows_data(nb=3000, with_json=False)
        ins_res = self.insert(client, collection_name, data, partition_name=partition_name)[0]
        assert ins_res["insert_count"] == 3000

        # delete entities with an expression condition
        expr = "int64 in [0,1]"
        res = self.delete(client, collection_name, filter=expr, partition_name=partition_name)[0]
        assert res["delete_count"] == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_upsert_empty_partition(self, default_schema):
        """
        target: verify upsert data in empty partition
        method: 1. create a collection
                2. upsert some data in empty partition
        expected: upsert successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # verify _default partition is empty
        partition_name = ct.default_partition_name
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == 0

        # upsert data to the empty partition
        data = cf.gen_row_data_by_schema(schema=schema)
        self.upsert(client, collection_name, data, partition_name=partition_name)

        # verify num_entities
        self.flush(client, collection_name)
        p_stats = self.get_partition_stats(client, collection_name, partition_name)[0]
        assert p_stats["row_count"] == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_upsert_dropped_partition(self, default_schema):
        """
        target: verify upsert data in a dropped partition
        method: 1. create a partition and drop
                2. upsert some data into the dropped partition
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # drop partition
        self.drop_partition(client, collection_name, partition_name)

        # upsert data to dropped partition
        data = cf.gen_row_data_by_schema(schema=schema)
        self.upsert(
            client,
            collection_name,
            data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 200, ct.err_msg: "partition not found"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_upsert_mismatched_data(self, default_schema):
        """
        target: test upsert mismatched data in partition
        method: 1. create a partition
                2. insert some data
                3. upsert with mismatched data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # insert data
        data = cf.gen_row_data_by_schema(schema=schema)
        self.insert(client, collection_name, data, partition_name=partition_name)

        # upsert mismatched data
        upsert_data = cf.gen_row_data_by_schema(schema=schema)
        for row in upsert_data:
            row[ct.default_float_vec_field_name] = [float(i) for i in range(ct.default_dim - 1)]
        self.upsert(
            client,
            collection_name,
            upsert_data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: f"float data should divide the dim({ct.default_dim})"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="smellthemoon: behavior changed")
    def test_partition_upsert_with_auto_id(self):
        """
        target: test upsert data in partition when auto_id=True
        method: 1. create a partition
                2. insert some data
                3. upsert data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # create collection with auto_id=True
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        # insert data (without pk since auto_id=True)
        data = cf.gen_row_data_by_schema(schema=schema)
        self.insert(client, collection_name, data, partition_name=partition_name)

        # upsert data (without pk)
        upsert_data = cf.gen_row_data_by_schema(schema=schema)
        self.upsert(
            client,
            collection_name,
            upsert_data,
            partition_name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "Upsert don't support autoid == true"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("is_flush", [True, False])
    def test_partition_upsert_same_pk_in_diff_partitions(self, default_schema, is_flush):
        """
        target: test upsert same pk in different partitions
        method: 1. create 2 partitions
                2. insert some data
                3. upsert data
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        # create 2 partitions
        partition_1 = cf.gen_unique_str(prefix)
        partition_2 = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_1)
        self.create_partition(client, collection_name, partition_2)

        # insert data
        nb = 1000
        data1 = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data1, partition_name=partition_1)
        data2 = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=nb)
        self.insert(client, collection_name, data2, partition_name=partition_2)

        # upsert same pk=0 data in both partitions
        upsert_data = cf.gen_row_data_by_schema(nb=1, schema=schema)
        self.upsert(client, collection_name, upsert_data, partition_name=partition_1)
        self.upsert(client, collection_name, upsert_data, partition_name=partition_2)

        # flush if needed
        if is_flush:
            self.flush(client, collection_name)

        # create index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

        # query and check the results
        expr = "int64 == 0"
        res1 = self.query(
            client,
            collection_name,
            filter=expr,
            output_fields=[ct.default_float_field_name],
            partition_names=[partition_1],
        )[0]
        res2 = self.query(
            client,
            collection_name,
            filter=expr,
            output_fields=[ct.default_float_field_name],
            partition_names=[partition_2],
        )[0]
        assert res1 == res2

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_partition_name_none(self, default_schema):
        """
        target: test create partition with partition_name=None
        method: call function: create_partition with None
        expected: raise ParamError (v2 validates partition_name eagerly)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)
        partition_name = None
        self.create_partition(
            client,
            collection_name,
            partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "`partition_name` value None is illegal"},
        )


class TestShowBase(TestMilvusClientV2Base):
    """Test case of list partition interface"""

    @pytest.fixture(scope="function")
    def default_schema(self):
        def _create(client):
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(ct.default_float_field_name, DataType.FLOAT)
            schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
            schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
            return schema

        yield _create

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_partitions(self, default_schema):
        """
        target: test list partitions, check partitions returned
        method: create partition first, then call list_partitions
        expected: partition correct
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        assert "_default" in partitions

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_multi_partitions(self, default_schema):
        """
        target: test list partitions with duplicate partition name creation
        method: create partition with same name twice, then call list_partitions
        expected: partitions correct, only one partition with that name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        # creating again with same name is idempotent
        self.create_partition(client, collection_name, partition_name)

        partitions = self.list_partitions(client, collection_name)[0]
        assert partition_name in partitions
        assert "_default" in partitions
        assert len(partitions) == 2  # _default + partition_name


class TestHasBase(TestMilvusClientV2Base):
    """Test case of has_partition interface"""

    @pytest.fixture(scope="function")
    def default_schema(self):
        def _create(client):
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(ct.default_float_field_name, DataType.FLOAT)
            schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
            schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
            return schema

        yield _create

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition(self, default_schema):
        """
        target: test has_partition, check result
        method: create partition first, then call has_partition
        expected: result true
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)

        assert self.has_partition(client, collection_name, partition_name)[0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_multi_partitions(self, default_schema):
        """
        target: test has_partition with duplicate partition name creation
        method: create partition with same name twice, then call has_partition
        expected: result true
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        # creating again with same name is idempotent
        self.create_partition(client, collection_name, partition_name)

        assert self.has_partition(client, collection_name, partition_name)[0]


class TestDropBase(TestMilvusClientV2Base):
    """Test case of drop_partition interface"""

    @pytest.fixture(scope="function")
    def default_schema(self):
        def _create(client):
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(ct.default_float_field_name, DataType.FLOAT)
            schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_length)
            schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
            return schema

        yield _create

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_create(self, default_schema):
        """
        target: test drop partition, and create again
        method: create partition, drop it, create again
        expected: partition re-created successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = default_schema(client)
        self.create_collection(client, collection_name, schema=schema)

        partition_name = cf.gen_unique_str(prefix)
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]

        # drop partition
        self.drop_partition(client, collection_name, partition_name)
        assert not self.has_partition(client, collection_name, partition_name)[0]

        # re-create partition
        self.create_partition(client, collection_name, partition_name)
        assert self.has_partition(client, collection_name, partition_name)[0]
