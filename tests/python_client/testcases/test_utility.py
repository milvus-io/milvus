import threading
import time

import pytest
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen.common_pb2 import SegmentState

from base.client_base import TestcaseBase
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from common import common_func as cf
from common import common_type as ct
from common.common_params import DefaultVectorIndexParams, DefaultVectorSearchParams, FieldParams
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log

prefix = "utility"
default_schema = cf.gen_default_collection_schema()
default_int64_field_name = ct.default_int64_field_name
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_dim = ct.default_dim
default_nb = ct.default_nb
num_loaded_entities = "num_loaded_entities"
num_total_entities = "num_total_entities"
loading_progress = "loading_progress"
num_loaded_partitions = "num_loaded_partitions"
not_loaded_partitions = "not_loaded_partitions"
exp_name = "name"
exp_schema = "schema"


class TestUtilityParams(TestcaseBase):
    """Test case of index interface"""

    @pytest.fixture(scope="function", params=["JACCARD", "Superstructure", "Substructure"])
    def get_not_support_metric(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["metric_type", "metric"])
    def get_support_metric_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_not_string)
    def get_invalid_type_collection_name(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.invalid_resource_names)
    def get_invalid_value_collection_name(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_name_type_invalid(self, get_invalid_type_collection_name):
        """
        target: test has_collection with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_type_collection_name
        self.utility_wrap.has_collection(
            c_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_name_value_invalid(self, get_invalid_value_collection_name):
        """
        target: test has_collection with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_value_collection_name
        error = {ct.err_code: 999, ct.err_msg: f"Invalid collection name: {c_name}"}
        if c_name in [None, ""]:
            error = {ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"}
        self.utility_wrap.has_collection(c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_collection_name_type_invalid(self, get_invalid_type_collection_name):
        """
        target: test has_partition with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_type_collection_name
        p_name = cf.gen_unique_str(prefix)
        self.utility_wrap.has_partition(
            c_name,
            p_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_collection_name_value_invalid(self, get_invalid_value_collection_name):
        """
        target: test has_partition with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_value_collection_name
        p_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 999, ct.err_msg: f"Invalid collection name: {c_name}"}
        if c_name in [None, ""]:
            error = {ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"}
        self.utility_wrap.has_partition(c_name, p_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_name_type_invalid(self, get_invalid_type_collection_name):
        """
        target: test has_partition with error partition name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtilityWrapper()
        c_name = cf.gen_unique_str(prefix)
        p_name = get_invalid_type_collection_name
        ut.has_partition(
            c_name,
            p_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: f"`partition_name` value {p_name} is illegal"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_name_value_invalid(self, get_invalid_value_collection_name):
        """
        target: test has_partition with error partition name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtilityWrapper()
        c_name = cf.gen_unique_str(prefix)
        p_name = get_invalid_value_collection_name
        if p_name == "12name":
            pytest.skip("partition name 12name is legal")
        if p_name == "n-ame":
            pytest.skip("https://github.com/milvus-io/milvus/issues/39432")
        error = {ct.err_code: 999, ct.err_msg: f"Invalid partition name: {p_name}"}
        if p_name in [None]:
            error = {ct.err_code: 999, ct.err_msg: f"`partition_name` value {p_name} is illegal"}
        elif p_name in [" ", ""]:
            error = {ct.err_code: 999, ct.err_msg: "Invalid partition name: . Partition name should not be empty."}
        ut.has_partition(c_name, p_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_name_type_invalid(self, get_invalid_type_collection_name):
        self._connect()
        c_name = get_invalid_type_collection_name
        self.utility_wrap.drop_collection(
            c_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_name_value_invalid(self, get_invalid_value_collection_name):
        self._connect()
        c_name = get_invalid_value_collection_name
        if c_name in [None, ""]:
            error = {ct.err_code: 999, ct.err_msg: f"`collection_name` value {c_name} is illegal"}
            self.utility_wrap.drop_collection(c_name, check_task=CheckTasks.err_res, check_items=error)
        else:
            self.utility_wrap.drop_collection(c_name)

    # TODO: enable
    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_using_invalid(self):
        """
        target: test list_collections with invalid using
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        using = "empty"
        ut = ApiUtilityWrapper()
        ex, _ = ut.list_collections(
            using=using,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 0, ct.err_msg: "should create connect"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_name", ct.invalid_resource_names)
    def test_index_process_invalid_name(self, invalid_name):
        """
        target: test building_process
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        error = {ct.err_code: 999, ct.err_msg: f"Invalid collection name: {invalid_name}"}
        if invalid_name in [None, ""]:
            error = {ct.err_code: 999, ct.err_msg: "collection name should not be empty"}
        self.utility_wrap.index_building_progress(
            collection_name=invalid_name, check_task=CheckTasks.err_res, check_items=error
        )

    # TODO: not support index name
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_index_name", ct.invalid_resource_names)
    def test_index_process_invalid_index_name(self, invalid_index_name):
        """
        target: test building_process
        method: input invalid index name
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        error = {ct.err_code: 999, ct.err_msg: "index not found"}
        self.utility_wrap.index_building_progress(
            collection_name=collection_w.name,
            index_name=invalid_index_name,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not ready")
    def test_wait_index_invalid_name(self, get_invalid_type_collection_name):
        """
        target: test wait_index
        method: input invalid name
        expected: raise exception
        """
        pass
        # self._connect()
        # c_name = get_invalid_collection_name
        # ut = ApiUtilityWrapper()
        # if isinstance(c_name, str) and c_name:
        #     ex, _ = ut.wait_for_index_building_complete(c_name,
        #                                                 check_items={ct.err_code: 1,
        #                                                              ct.err_msg: "Invalid collection name"})

    @pytest.mark.tags(CaseLabel.L1)
    def _test_wait_index_invalid_index_name(self, get_invalid_index_name):
        """
        target: test wait_index
        method: input invalid index name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        ut = ApiUtilityWrapper()
        ex, _ = ut.wait_for_index_building_complete(c_name, index_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_c_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_loading_progress_invalid_collection_name(self, invalid_c_name):
        """
        target: test loading progress with invalid collection name
        method: input invalid collection name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data()
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name)
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()
        error = {ct.err_code: 1, ct.err_msg: f"Invalid collection name: {invalid_c_name}"}
        self.utility_wrap.loading_progress(invalid_c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_not_existed_collection_name(self):
        """
        target: test loading progress with invalid collection name
        method: input invalid collection name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        df = cf.gen_default_dataframe_data()
        self.collection_wrap.construct_from_dataframe(c_name, df, primary_field=ct.default_int64_field_name)
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()
        error = {ct.err_code: 100, ct.err_msg: "collection not found[database=default][collection=not_existed_name]"}
        self.utility_wrap.loading_progress("not_existed_name", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ct.invalid_resource_names)
    def test_loading_progress_invalid_partition_names(self, partition_name):
        """
        target: test loading progress with invalid partition names
        method: input invalid partition names
        expected: raise an exception
        """
        if partition_name == "":
            pytest.skip("https://github.com/milvus-io/milvus/issues/38223")
        collection_w = self.init_collection_general(prefix, nb=10)[0]
        partition_names = [partition_name]
        collection_w.load()
        err_msg = {ct.err_code: 999, ct.err_msg: "partition not found"}
        if partition_name is None:
            err_msg = {ct.err_code: 999, ct.err_msg: "is illegal"}
        self.utility_wrap.loading_progress(
            collection_w.name, partition_names, check_task=CheckTasks.err_res, check_items=err_msg
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_names", [[ct.default_tag], [ct.default_partition_name, ct.default_tag]])
    def test_loading_progress_not_existed_partitions(self, partition_names):
        """
        target: test loading progress with not existed partitions
        method: input all or part not existed partition names
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, nb=10)[0]
        collection_w.load()
        err_msg = {ct.err_code: 15, ct.err_msg: "partition not found"}
        self.utility_wrap.loading_progress(
            collection_w.name, partition_names, check_task=CheckTasks.err_res, check_items=err_msg
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_wait_for_loading_collection_not_existed(self):
        """
        target: test wait for loading
        method: input collection not created before
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.utility_wrap.wait_for_loading_complete(
            c_name,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 100,
                ct.err_msg: f"collection not found[database=default][collection={c_name}]",
            },
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_wait_for_loading_partition_not_existed(self):
        """
        target: test wait for loading
        method: input partition not created before
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        self.utility_wrap.wait_for_loading_complete(
            collection_w.name,
            partition_names=[ct.default_tag],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 200, ct.err_msg: f"partition not found[partition={ct.default_tag}]"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_not_existed(self):
        """
        target: test drop an not existed collection
        method: drop a not created collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)

        # error = {ct.err_code: 1, ct.err_msg: f"DescribeCollection failed: can't find collection: {c_name}"}
        # self.utility_wrap.drop_collection(c_name, check_task=CheckTasks.err_res, check_items=error)

        # @longjiquan: dropping collection should be idempotent.
        self.utility_wrap.drop_collection(c_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_left_vector_invalid_type(self, get_invalid_vector_dict):
        """
        target: test calculated distance with invalid vectors
        method: input invalid vectors type
        expected: raise exception
        """
        self._connect()
        invalid_vector = get_invalid_vector_dict
        if not isinstance(invalid_vector, dict):
            self.utility_wrap.calc_distance(
                invalid_vector,
                invalid_vector,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1, "err_msg": f"vectors_left value {invalid_vector} is illegal"},
            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_left_vector_invalid_value(self, get_invalid_vector_dict):
        """
        target: test calculated distance with invalid vectors
        method: input invalid vectors value
        expected: raise exception
        """
        self._connect()
        invalid_vector = get_invalid_vector_dict
        if isinstance(invalid_vector, dict):
            self.utility_wrap.calc_distance(
                invalid_vector,
                invalid_vector,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1, "err_msg": f"vectors_left value {invalid_vector} is illegal"},
            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_right_vector_invalid_type(self, get_invalid_vector_dict):
        """
        target: test calculated distance with invalid vectors
        method: input invalid vectors type
        expected: raise exception
        """
        self._connect()
        invalid_vector = get_invalid_vector_dict
        vector = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vector}
        if not isinstance(invalid_vector, dict):
            self.utility_wrap.calc_distance(
                op_l,
                invalid_vector,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1, "err_msg": f"vectors_right value {invalid_vector} is illegal"},
            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_right_vector_invalid_value(self, get_invalid_vector_dict):
        """
        target: test calculated distance with invalid vectors
        method: input invalid vectors value
        expected: raise exception
        """
        self._connect()
        invalid_vector = get_invalid_vector_dict
        vector = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vector}
        if isinstance(invalid_vector, dict):
            self.utility_wrap.calc_distance(
                op_l,
                invalid_vector,
                check_task=CheckTasks.err_res,
                check_items={"err_code": 1, "err_msg": f"vectors_right value {invalid_vector} is illegal"},
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection_old_invalid_type(self, get_invalid_type_collection_name):
        """
        target: test rename_collection when the type of old collection name is not valid
        method: input not invalid collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = get_invalid_type_collection_name
        new_collection_name = cf.gen_unique_str(prefix)
        self.utility_wrap.rename_collection(
            old_collection_name,
            new_collection_name,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 999,
                "err_msg": f"`collection_name` value {old_collection_name} is illegal",
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection_old_invalid_value(self, get_invalid_value_collection_name):
        """
        target: test rename_collection when the value of old collection name is not valid
        method: input not invalid collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = get_invalid_value_collection_name
        new_collection_name = cf.gen_unique_str(prefix)
        error = {"err_code": 4, "err_msg": "collection not found"}
        if old_collection_name in [None, ""]:
            error = {"err_code": 999, "err_msg": "is illegal"}
        self.utility_wrap.rename_collection(
            old_collection_name, new_collection_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_rename_collection_new_invalid_type(self, get_invalid_type_collection_name):
        """
        target: test rename_collection when the type of new collection name is not valid
        method: input not invalid collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        new_collection_name = get_invalid_type_collection_name
        self.utility_wrap.rename_collection(
            old_collection_name,
            new_collection_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1, "err_msg": f"`collection_name` value {new_collection_name} is illegal"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_rename_collection_new_invalid_value(self, get_invalid_value_collection_name):
        """
        target: test rename_collection when the value of new collection name is not valid
        method: input not invalid collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        new_collection_name = get_invalid_value_collection_name
        error = {"err_code": 1100, "err_msg": "Invalid collection name"}
        if new_collection_name in [None, ""]:
            error = {"err_code": 999, "err_msg": f"`collection_name` value {new_collection_name} is illegal"}
        self.utility_wrap.rename_collection(
            old_collection_name, new_collection_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_rename_collection_not_existed_collection(self):
        """
        target: test rename_collection when the collection name does not exist
        method: input not existing collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = "test_collection_non_exist"
        new_collection_name = cf.gen_unique_str(prefix)
        self.utility_wrap.rename_collection(
            old_collection_name,
            new_collection_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 100, "err_msg": "collection not found"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection_existed_collection_name(self):
        """
        target: test rename_collection when the collection name is existed
        method: input existing collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        self.utility_wrap.rename_collection(
            old_collection_name,
            old_collection_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "collection name or database name should be different"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection_existed_collection_alias(self):
        """
        target: test rename_collection when the collection alias is existed
            method: input existing collection alias
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        alias = "test_alias"
        self.utility_wrap.create_alias(old_collection_name, alias)
        self.utility_wrap.rename_collection(
            old_collection_name,
            alias,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": f"cannot rename collection to an existing alias: {alias}"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection_using_alias(self):
        """
        target: test rename_collection using alias
        method: rename collection using alias name
        expected: raise exception
        """
        self._connect()
        collection_w = self.init_collection_general(prefix)[0]
        old_collection_name = collection_w.name
        alias = cf.gen_unique_str(prefix + "alias")
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.create_alias(old_collection_name, alias)
        self.utility_wrap.rename_collection(
            alias,
            new_collection_name,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 1,
                "err_msg": f"unsupported use an alias to rename collection, alias:{alias}",
            },
        )


class TestUtilityBase(TestcaseBase):
    """Test case of index interface"""

    @pytest.fixture(scope="function", params=["metric_type", "metric"])
    def metric_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def sqrt(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["L2", "IP"])
    def metric(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["HAMMING", "JACCARD"])
    def metric_binary(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection(self):
        """
        target: test has_collection with collection name
        method: input collection name created before
        expected: True
        """
        cw = self.init_collection_wrap()
        res, _ = self.utility_wrap.has_collection(cw.name)
        assert res is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_not_created(self):
        """
        target: test has_collection with collection name which is not created
        method: input random collection name
        expected: False
        """
        c_name = cf.gen_unique_str(prefix)
        _ = self.init_collection_wrap()
        res, _ = self.utility_wrap.has_collection(c_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection_after_drop(self):
        """
        target: test has_collection with collection name droped before
        method: input random collection name
        expected: False
        """
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.has_collection(c_name)
        assert res is True
        cw.drop()
        res, _ = self.utility_wrap.has_collection(c_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition(self):
        """
        target: test has_partition with partition name
        method: input collection name and partition name created before
        expected: True
        """
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        self.init_partition_wrap(cw, p_name)
        res, _ = self.utility_wrap.has_partition(c_name, p_name)
        assert res is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_not_created(self):
        """
        target: test has_partition with partition name
        method: input collection name, and partition name not created before
        expected: True
        """
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str()
        self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.has_partition(c_name, p_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition_after_drop(self):
        """
        target: test has_partition with partition name
        method: input collection name, and partition name dropped
        expected: True
        """
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str()
        cw = self.init_collection_wrap(name=c_name)
        pw = self.init_partition_wrap(cw, p_name)
        res, _ = self.utility_wrap.has_partition(c_name, p_name)
        assert res is True
        pw.drop()
        res, _ = self.utility_wrap.has_partition(c_name, p_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_default_partition(self):
        """
        target: test has_partition with '_default' partition
        method: input collection name and partition name created before
        expected: True
        """
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.has_partition(c_name, ct.default_partition_name)
        assert res is True

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_collections(self):
        """
        target: test list_collections
        method: create collection, list_collections
        expected: in the result
        """
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.list_collections()
        assert c_name in res

    # TODO: make sure all collections deleted
    @pytest.mark.tags(CaseLabel.L1)
    def _test_list_collections_no_collection(self):
        """
        target: test list_collections
        method: no collection created, list_collections
        expected: length of the result equals to 0
        """
        self._connect()
        res, _ = self.utility_wrap.list_collections()
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_index_process_collection_not_existed(self):
        """
        target: test building_process
        method: input collection not created before
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.utility_wrap.index_building_progress(
            c_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 4, ct.err_msg: "collection not found"}
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_empty(self):
        """
        target: test building_process
        method: input empty collection
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(cw.collection, default_field_name, default_index_params)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        exp_res = {"total_rows": 0, "indexed_rows": 0, "pending_index_rows": 0, "state": "Finished"}
        assert res == exp_res

    @pytest.mark.tags(CaseLabel.L2)
    def test_index_process_collection_insert_no_index(self):
        """
        target: test building_process
        method: insert 1 entity, no index created
        expected: no exception raised
        """
        nb = 1
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        error = {ct.err_code: 999, ct.err_msg: f"index not found[collection={c_name}]"}
        self.utility_wrap.index_building_progress(c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_index(self):
        """
        target: test building_process
        method: 1.insert 1024 (because minSegmentSizeToEnableIndex=1024)
                2.build(server does create index) and call building_process
        expected: indexed_rows=nb
        """
        nb = 1024
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        cw.create_index(default_field_name, default_index_params)
        cw.flush()
        res, _ = self.utility_wrap.index_building_progress(c_name)
        # The indexed_rows may be 0 due to compaction,
        # remove this assertion for now
        # assert res['indexed_rows'] == nb
        assert res["total_rows"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="wait to modify")
    def test_index_process_collection_indexing(self):
        """
        target: test building_process
        method: 1.insert 2048 entities to ensure that server will build
                2.call building_process during building
        expected: 2048 or less entities indexed
        """
        nb = 2048
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        cw.create_index(default_field_name, default_index_params)
        cw.flush()
        start = time.time()
        while True:
            time.sleep(1)
            res, _ = self.utility_wrap.index_building_progress(c_name)
            if 0 < res["indexed_rows"] <= nb:
                break
            if time.time() - start > 5:
                raise MilvusException(1, "Index build completed in more than 5s")

    @pytest.mark.tags(CaseLabel.L2)
    def test_wait_index_collection_not_existed(self):
        """
        target: test wait_index
        method: input collection not created before
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self.utility_wrap.wait_for_index_building_complete(
            c_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 4, ct.err_msg: "collection not found"}
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_collection_empty(self):
        """
        target: test wait_index
        method: input empty collection
        expected: no exception raised
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        cw.create_index(default_field_name, default_index_params)
        assert self.utility_wrap.wait_for_index_building_complete(c_name)[0]
        res, _ = self.utility_wrap.index_building_progress(c_name)
        exp_res = {"total_rows": 0, "indexed_rows": 0, "pending_index_rows": 0, "state": "Finished"}
        assert res == exp_res

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_collection_index(self):
        """
        target: test wait_index
        method: insert 5000 entities, build and call wait_index
        expected: 5000 entity indexed
        """
        nb = 5000
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        cw.flush()
        cw.create_index(default_field_name, default_index_params)
        res, _ = self.utility_wrap.wait_for_index_building_complete(c_name)
        assert res is True
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert res["indexed_rows"] == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_without_loading(self):
        """
        target: test loading progress without loading
        method: insert and flush data, call loading_progress without loading
        expected: raise exception
        """
        collection_w = self.init_collection_wrap()
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        self.utility_wrap.loading_progress(
            collection_w.name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 101, ct.err_msg: "collection not loaded"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nb", [ct.default_nb, 5000])
    def test_loading_progress_collection(self, nb):
        """
        target: test loading progress
        method: 1.insert flush and load 2.call loading_progress
        expected: all entities is loafed, because load is synchronous
        """
        # create, insert default_nb, flush and load
        collection_w = self.init_collection_general(prefix, insert_data=True, nb=nb)[0]
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res[loading_progress] == "100%"

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_with_async_load(self):
        """
        target: test loading progress with async collection load
        method: 1.load collection with async=True 2.loading_progress
        expected: loading part entities
        """
        collection_w = self.init_collection_wrap()
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(_async=True)
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        loading_int = cf.percent_to_int(res[loading_progress])
        if loading_int != -1:
            assert 0 <= loading_int <= 100
        else:
            log.info("The output of loading progress is not a string or a percentage")

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_empty_collection(self):
        """
        target: test loading_progress on an empty collection
        method: 1.create collection and no insert 2.loading_progress
        expected: 0 entities is loaded
        """
        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        exp_res = {loading_progress: "100%"}

        assert exp_res == res

    @pytest.mark.tags(CaseLabel.L1)
    def test_loading_progress_after_release(self):
        """
        target: test loading progress after release
        method: insert and flush data, call loading_progress after release
        expected: return successfully with 0%
        """
        collection_w = self.init_collection_general(prefix, insert_data=True, nb=100)[0]
        collection_w.release()
        error = {ct.err_code: 999, ct.err_msg: "collection not loaded"}
        self.utility_wrap.loading_progress(collection_w.name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_with_release_partition(self):
        """
        target: test loading progress after release part partitions
        method: 1.insert data into two partitions and flush
                2.load one partition and release one partition
        expected: loaded one partition entities
        """
        half = ct.default_nb
        # insert entities into two partitions, collection flush and load
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        partition_w.release()
        res = self.utility_wrap.loading_progress(collection_w.name)[0]
        assert res[loading_progress] == "100%"

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_with_load_partition(self):
        """
        target: test loading progress after load partition
        method: 1.insert data into two partitions and flush
                2.load one partition and loading progress
        expected: loaded one partition entities
        """
        half = ct.default_nb
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        collection_w.release()
        partition_w.load()
        res = self.utility_wrap.loading_progress(collection_w.name)[0]
        assert res[loading_progress] == "100%"

    @pytest.mark.tags(CaseLabel.L1)
    def test_loading_progress_with_partition(self):
        """
        target: test loading progress with partition
        method: 1.insert data into two partitions and flush, and load
                2.loading progress with one partition
        expected: loaded one partition entities
        """
        half = ct.default_nb
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        res = self.utility_wrap.loading_progress(collection_w.name, partition_names=[partition_w.name])[0]
        assert res[loading_progress] == "100%"

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_loading_progress_multi_replicas(self):
        """
        target: test loading progress with multi replicas
        method: 1.Create collection and insert data
                2.Load replicas and get loading progress
                3.Create partitions and insert data
                4.Get loading progress
                5.Release and re-load replicas, get loading progress
        expected: Verify loading progress result
        """
        collection_w = self.init_collection_wrap()
        collection_w.insert(cf.gen_default_dataframe_data())
        assert collection_w.num_entities == ct.default_nb
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load(partition_names=[ct.default_partition_name], replica_number=2)
        res_collection, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_collection == {loading_progress: "100%"}

        # create partition and insert
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        partition_w.insert(cf.gen_default_dataframe_data(start=ct.default_nb))
        assert partition_w.num_entities == ct.default_nb
        res_part_partition, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_part_partition == {"loading_progress": "100%"}

        res_part_partition, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_part_partition == {"loading_progress": "100%"}

        collection_w.release()
        collection_w.load(replica_number=2)
        res_all_partitions, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_all_partitions == {"loading_progress": "100%"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_loading_collection_empty(self):
        """
        target: test wait_for_loading
        method: input empty collection
        expected: no exception raised
        """
        self._connect()
        cw = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        cw.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        cw.load()
        self.utility_wrap.wait_for_loading_complete(cw.name)
        res, _ = self.utility_wrap.loading_progress(cw.name)
        exp_res = {loading_progress: "100%"}
        assert res == exp_res

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_for_loading_complete(self):
        """
        target: test wait for loading collection
        method: insert 10000 entities and wait for loading complete
        expected: after loading complete, loaded entities is 10000
        """
        nb = 6000
        collection_w = self.init_collection_wrap()
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df, timeout=60)
        assert collection_w.num_entities == nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(_async=True)
        self.utility_wrap.wait_for_loading_complete(collection_w.name, timeout=45)
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res[loading_progress] == "100%"

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_collection(self):
        """
        target: test utility drop collection by name
        method: input collection name and drop collection
        expected: collection is dropped
        """
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(c_name)
        assert self.utility_wrap.has_collection(c_name)[0]
        self.utility_wrap.drop_collection(c_name)
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_collection_repeatedly(self):
        """
        target: test drop collection repeatedly
        method: 1.collection.drop 2.utility.drop_collection
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(c_name)
        assert self.utility_wrap.has_collection(c_name)[0]
        collection_w.drop()
        assert not self.utility_wrap.has_collection(c_name)[0]

        # error = {ct.err_code: 1, ct.err_msg: {"describe collection failed: can't find collection:"}}
        # self.utility_wrap.drop_collection(c_name, check_task=CheckTasks.err_res, check_items=error)
        # @longjiquan: dropping collection should be idempotent.
        self.utility_wrap.drop_collection(c_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_create_repeatedly(self):
        """
        target: test repeatedly create and drop same name collection
        method: repeatedly create and drop collection
        expected: no exception
        """
        from time import sleep

        loops = 3
        c_name = cf.gen_unique_str(prefix)
        for _ in range(loops):
            self.init_collection_wrap(c_name)
            assert self.utility_wrap.has_collection(c_name)[0]
            self.utility_wrap.drop_collection(c_name)
            assert not self.utility_wrap.has_collection(c_name)[0]
            sleep(1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection(self):
        """
        target: test rename collection function to single collection
        method: call rename_collection API to rename collection
        expected: collection renamed successfully without any change on aliases
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        new_collection_name = cf.gen_unique_str(prefix + "new")
        alias = cf.gen_unique_str(prefix + "alias")
        self.utility_wrap.create_alias(old_collection_name, alias)
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name)
        collection_w = self.init_collection_wrap(
            name=new_collection_name,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: new_collection_name, exp_schema: default_schema},
        )
        collections = self.utility_wrap.list_collections()[0]
        assert new_collection_name in collections
        assert old_collection_name not in collections
        assert [alias] == collection_w.aliases

    @pytest.mark.tags(CaseLabel.L2)
    def test_rename_collections(self):
        """
        target: test rename collection function to multiple collections
        method: call rename_collection API to rename collections
        expected: collections renamed successfully without any change on aliases
        """
        self._connect()
        # create two collections
        collection_w_1 = self.init_collection_general(prefix)[0]
        old_collection_name_1 = collection_w_1.name
        collection_w_2 = self.init_collection_general(prefix)[0]
        old_collection_name_2 = collection_w_2.name
        tmp_collection_name = cf.gen_unique_str(prefix + "tmp")
        # create alias to each collection
        alias_1 = cf.gen_unique_str(prefix + "alias1")
        alias_2 = cf.gen_unique_str(prefix + "alias2")
        self.utility_wrap.create_alias(old_collection_name_1, alias_1)
        self.utility_wrap.create_alias(old_collection_name_2, alias_2)
        # switch name of the existing collections
        self.utility_wrap.rename_collection(old_collection_name_1, tmp_collection_name)
        self.utility_wrap.rename_collection(old_collection_name_2, old_collection_name_1)
        self.utility_wrap.rename_collection(tmp_collection_name, old_collection_name_2)
        # check collection renamed successfully
        collection_w_1 = self.init_collection_wrap(
            name=old_collection_name_1,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: old_collection_name_1, exp_schema: default_schema},
        )
        collection_w_2 = self.init_collection_wrap(
            name=old_collection_name_2,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: old_collection_name_2, exp_schema: default_schema},
        )
        collections = self.utility_wrap.list_collections()[0]
        assert old_collection_name_1 in collections
        assert old_collection_name_2 in collections
        assert tmp_collection_name not in collections
        # check alias not changed
        assert collection_w_1.aliases[0] == alias_2
        assert collection_w_2.aliases[0] == alias_1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("move to test_milvus_client_v2_rename_back_old_collection")
    def test_rename_back_old_collection(self):
        """
        target: test rename collection function to single collection
        method: rename back to old collection name
        expected: collection renamed successfully without any change on aliases
        """
        # 1. connect
        self._connect()
        # 2. create a collection
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        new_collection_name = cf.gen_unique_str(prefix + "new")
        alias = cf.gen_unique_str(prefix + "alias")
        # 3. create an alias
        self.utility_wrap.create_alias(old_collection_name, alias)
        collection_alias = collection_w.aliases
        # 4. rename collection
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name)
        # 5. rename back to old collection name
        self.utility_wrap.rename_collection(new_collection_name, old_collection_name)
        collection_w = self.init_collection_wrap(
            name=old_collection_name,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: old_collection_name, exp_schema: default_schema},
        )
        collections = self.utility_wrap.list_collections()[0]
        assert old_collection_name in collections
        assert new_collection_name not in collections
        assert collection_alias == collection_w.aliases

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("move to test_milvus_client_v2_rename_back_old_alias")
    def test_rename_back_old_alias(self):
        """
        target: test rename collection function to single collection
        method: rename back to old collection alias
        expected: collection renamed successfully without any change on aliases
        """
        # 1. connect
        self._connect()
        # 2. create a collection
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = collection_w.name
        alias = cf.gen_unique_str(prefix + "alias")
        # 3. create an alias
        self.utility_wrap.create_alias(old_collection_name, alias)
        collection_alias = collection_w.aliases
        log.info(collection_alias)
        # 4. drop the alias
        self.utility_wrap.drop_alias(collection_alias[0])
        # 5. rename collection to the dropped alias name
        self.utility_wrap.rename_collection(old_collection_name, collection_alias[0])
        self.init_collection_wrap(
            name=collection_alias[0],
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: collection_alias[0], exp_schema: default_schema},
        )
        collections = self.utility_wrap.list_collections()[0]
        assert collection_alias[0] in collections
        assert old_collection_name not in collections

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_alias_using_dropped_collection_name(self):
        """
        target: test create alias using a dropped collection name
        method: create 2 collections and drop one collection
        expected: raise no exception
        """
        # 1. create 2 collections
        a_name = cf.gen_unique_str("aa")
        b_name = cf.gen_unique_str("bb")
        self.init_collection_wrap(
            name=a_name,
            schema=default_schema,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: a_name, exp_schema: default_schema},
        )
        self.init_collection_wrap(
            name=b_name,
            schema=default_schema,
            check_task=CheckTasks.check_collection_property,
            check_items={exp_name: b_name, exp_schema: default_schema},
        )

        # 2. drop collection a
        self.utility_wrap.drop_collection(a_name)
        assert self.utility_wrap.has_collection(a_name)[0] is False
        assert len(self.utility_wrap.list_aliases(b_name)[0]) == 0

        # 3. create alias with the name of collection a
        self.utility_wrap.create_alias(b_name, a_name)
        b_alias, _ = self.utility_wrap.list_aliases(b_name)
        assert a_name in b_alias

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_indexes(self):
        """
        target: test utility.list_indexes
        method: create 2 collections and list indexes
        expected: raise no exception
        """
        # 1. create 2 collections
        string_field = ct.default_string_field_name
        collection_w1 = self.init_collection_general(prefix, True)[0]
        collection_w2 = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w2.create_index(string_field)

        # 2. list indexes
        res1, _ = self.utility_wrap.list_indexes(collection_w1.name)
        assert res1 == [ct.default_float_vec_field_name]
        res2, _ = self.utility_wrap.list_indexes(collection_w2.name)
        assert res2 == [string_field]

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_server_type(self):
        """
        target: test utility.get_server_type
        method: get_server_type
        expected: raise no exception
        """
        self._connect()
        res, _ = self.utility_wrap.get_server_type()
        assert res == "milvus"

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_state(self):
        """
        target: test utility.load_state
        method: load_state
        expected: raise no exception
        """
        collection_w = self.init_collection_general(prefix, True, partition_num=1)[0]
        res1, _ = self.utility_wrap.load_state(collection_w.name)
        assert str(res1) == "Loaded"
        collection_w.release()
        res2, _ = self.utility_wrap.load_state(collection_w.name)
        assert str(res2) == "NotLoad"
        collection_w.load(partition_names=[ct.default_partition_name])
        res3, _ = self.utility_wrap.load_state(collection_w.name)
        assert str(res3) == "Loaded"


class TestUtilityAdvanced(TestcaseBase):
    """Test case of index interface"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_multi_collections(self):
        """
        target: test has_collection with collection name
        method: input collection name created before
        expected: True
        """
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        self.init_collection_wrap(name=c_name_2)
        for name in [c_name, c_name_2]:
            res, _ = self.utility_wrap.has_collection(name)
            assert res is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_multi_collection(self):
        """
        target: test list_collections
        method: create collection, list_collections
        expected: in the result
        """
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        self.init_collection_wrap(name=c_name_2)
        res, _ = self.utility_wrap.list_collections()
        for name in [c_name, c_name_2]:
            assert name in res

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_multi_collection_concurrent(self):
        """
        target: test concurrent drop collection
        method: multi thread drop one collection
        expected: drop successfully
        """
        thread_num = 3
        threads = []
        c_names = []
        num = 5

        for i in range(thread_num * num):
            c_name = cf.gen_unique_str(prefix)
            self.init_collection_wrap(c_name)
            c_names.append(c_name)

        def create_and_drop_collection(names):
            for name in names:
                assert self.utility_wrap.has_collection(name)[0]
                self.utility_wrap.drop_collection(name)
                assert not self.utility_wrap.has_collection(name)[0]

        for i in range(thread_num):
            x = threading.Thread(target=create_and_drop_collection, args=(c_names[i * num : (i + 1) * num],))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        log.debug(self.utility_wrap.list_collections()[0])

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_query_segment_info_empty_collection(self):
        """
        target: test getting query segment info of empty collection
        method: init a collection and get query segment info
        expected: length of segment is 0
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", ["int64_pk", "varchar_pk"])
    def test_get_sealed_query_segment_info(self, primary_field):
        """
        target: test getting sealed query segment info of collection with data
        method: init a collection, insert data, flush, index, load, and get query segment info
        expected:
            1. length of segment is greater than 0
            2. the sum num_rows of each segment is equal to num of entities
            3. all segment is_sorted true
        """
        nb = 3000
        segment_num = 2
        collection_name = cf.gen_unique_str(prefix)

        # connect -> create collection
        self._connect()
        self.collection_wrap.init_collection(
            name=collection_name,
            schema=cf.set_collection_schema(
                fields=[primary_field, ct.default_float_vec_field_name],
                field_params={
                    primary_field: FieldParams(is_primary=True, max_length=128).to_dict,
                    ct.default_float_vec_field_name: FieldParams(dim=ct.default_dim).to_dict,
                },
            ),
        )

        for _ in range(segment_num):
            # insert random pks
            data = cf.gen_values(self.collection_wrap.schema, nb=nb, random_pk=True)
            self.collection_wrap.insert(data)
            self.collection_wrap.flush()

        # flush -> index -> load -> sealed segments is sorted
        self.build_multi_index(index_params=DefaultVectorIndexParams.IVF_SQ8(ct.default_float_vec_field_name))
        self.collection_wrap.load()

        # get_query_segment_info and verify results
        res_sealed, _ = self.utility_wrap.get_query_segment_info(collection_name)
        assert len(res_sealed) > 0  # maybe mix compaction to 1 segment
        cnt = 0
        for r in res_sealed:
            log.info(f"segmentID {r.segmentID}: state: {r.state}; num_rows: {r.num_rows}; is_sorted: {r.is_sorted} ")
            cnt += r.num_rows
            assert r.is_sorted is True
        assert cnt == nb * segment_num

        # verify search
        self.collection_wrap.search(
            data=cf.gen_vectors(ct.default_nq, ct.default_dim),
            anns_field=ct.default_float_vec_field_name,
            param=DefaultVectorSearchParams.IVF_SQ8(),
            limit=ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": ct.default_nq, "limit": ct.default_limit},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_growing_query_segment_info(self):
        """
        target: test getting growing query segment info of collection with data
        method: init a collection, index, load, insert data, and get query segment info
        expected:
            1. length of segment is 0, growing segment is not visible for get_query_segment_info
        """
        nb = 3000
        primary_field = "int64"
        collection_name = cf.gen_unique_str(prefix)

        # connect -> create collection
        self._connect()
        self.collection_wrap.init_collection(
            name=collection_name,
            schema=cf.set_collection_schema(
                fields=[primary_field, ct.default_float_vec_field_name],
                field_params={
                    primary_field: FieldParams(is_primary=True, max_length=128).to_dict,
                    ct.default_float_vec_field_name: FieldParams(dim=ct.default_dim).to_dict,
                },
            ),
        )

        # index -> load
        self.build_multi_index(index_params=DefaultVectorIndexParams.IVF_SQ8(ct.default_float_vec_field_name))
        self.collection_wrap.load()

        # insert random pks ***
        data = cf.gen_values(self.collection_wrap.schema, nb=nb, random_pk=True)
        self.collection_wrap.insert(data)

        # get_query_segment_info and verify results
        res_sealed, _ = self.utility_wrap.get_query_segment_info(collection_name)
        assert len(res_sealed) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_sealed_query_segment_info_after_create_index(self):
        """
        target: test getting sealed query segment info of collection with data
        method: init a collection, insert data, flush, create index, load, and get query segment info
        expected:
            1. length of segment is greater than 0
            2. the sum num_rows of each segment is equal to num of entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        collection_w.num_entities
        collection_w.create_index(default_field_name, default_index_params)
        collection_w.load()
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        assert len(res) > 0
        segment_ids = []
        cnt = 0
        for r in res:
            log.info(f"segmentID {r.segmentID}: state: {r.state}; num_rows: {r.num_rows} ")
            if r.segmentID not in segment_ids:
                segment_ids.append(r.segmentID)
                cnt += r.num_rows
        assert cnt == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_balance_normal(self):
        """
        target: test load balance of collection
        method: init a collection and load balance
        expected: sealed_segment_ids is subset of des_sealed_segment_ids
        """
        # init a collection
        self._connect()
        querynode_num = len(MilvusSys().query_nodes)
        if querynode_num < 2:
            pytest.skip("skip load balance testcase when querynode number less than 2")
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(default_field_name, default_index_params)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        # get growing segments
        collection_w.insert(df)
        collection_w.load()
        # prepare load balance params
        time.sleep(0.5)
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        assert len(all_querynodes) > 1
        all_querynodes = sorted(
            all_querynodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        src_node_id = all_querynodes[0]
        des_node_ids = all_querynodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, des_node_ids, sealed_segment_ids)
        # get segments distribution after load balance
        time.sleep(0.5)
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        sealed_segment_ids_after_load_banalce = segment_distribution[src_node_id]["sealed"]
        # assert src node has no sealed segments
        assert sealed_segment_ids_after_load_banalce == []
        des_sealed_segment_ids = []
        for des_node_id in des_node_ids:
            des_sealed_segment_ids += segment_distribution[des_node_id]["sealed"]
        # assert sealed_segment_ids is subset of des_sealed_segment_ids
        assert set(sealed_segment_ids).issubset(des_sealed_segment_ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_balance_with_src_node_not_exist(self):
        """
        target: test load balance of collection
        method: init a collection and load balance with src_node not exist
        expected: raise exception
        """
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        # get growing segments
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # prepare load balance params
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        all_querynodes = sorted(
            all_querynodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        # use a node id greater than all nodes (not just querynodes) to ensure it truly does not exist
        all_node_ids = [node["identifier"] for node in ms.nodes]
        invalid_src_node_id = max(all_node_ids) + 1
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(
            collection_w.name,
            invalid_src_node_id,
            dst_node_ids,
            sealed_segment_ids,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "source node not found in any replica"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_balance_with_all_dst_node_not_exist(self):
        """
        target: test load balance of collection
        method: init a collection and load balance with all dst_node not exist
        expected: raise exception
        """
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        # get growing segments
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # prepare load balance params
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        all_querynodes = sorted(
            all_querynodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        src_node_id = all_querynodes[0]
        # use node ids greater than all nodes (not just querynodes) to ensure they truly do not exist
        all_node_ids = [node["identifier"] for node in ms.nodes]
        max_node_id = max(all_node_ids)
        dst_node_ids = [max_node_id + 1, max_node_id + 2]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(
            collection_w.name,
            src_node_id,
            dst_node_ids,
            sealed_segment_ids,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "destination node not found in the same replica"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_balance_with_one_sealed_segment_id_not_exist(self):
        """
        target: test load balance of collection
        method: init a collection and load balance with one of sealed segment ids not exist
        expected: raise exception
        """
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(default_field_name, default_index_params)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.flush()
        # get growing segments
        collection_w.insert(df)
        collection_w.load()
        # prepare load balance params
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        all_querynodes = sorted(
            all_querynodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        if len(segment_distribution[src_node_id]["sealed"]) == 0:
            sealed_segment_ids = [0]  # add a segment id which is not exist
        else:
            # add a segment id which is not exist
            sealed_segment_ids.append(max(segment_distribution[src_node_id]["sealed"]) + 1)
        # load balance
        self.utility_wrap.load_balance(
            collection_w.name,
            src_node_id,
            dst_node_ids,
            sealed_segment_ids,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: "not found in source node"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_balance_with_all_sealed_segment_id_not_exist(self):
        """
        target: test load balance of collection
        method: init a collection and load balance with one of sealed segment ids not exist
        expected: raise exception
        """
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        collection_w.create_index(default_field_name, default_index_params)
        # get growing segments
        collection_w.insert(df)
        collection_w.load()
        # prepare load balance params
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        all_querynodes = sorted(
            all_querynodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        # add segment ids which are not exist
        sealed_segment_ids = [
            sealed_segment_id
            for sealed_segment_id in range(
                max(segment_distribution[src_node_id]["sealed"]) + 100,
                max(segment_distribution[src_node_id]["sealed"]) + 103,
            )
        ]
        # load balance
        self.utility_wrap.load_balance(
            collection_w.name,
            src_node_id,
            dst_node_ids,
            sealed_segment_ids,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "not found in source node"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_balance_in_one_group(self):
        """
        target: test load balance of collection in one group
        method: init a collection, load with multi replicas and load balance among the querynodes in one group
        expected: load balance successfully
        """
        self._connect()
        querynode_num = len(MilvusSys().query_nodes)
        if querynode_num < 3:
            pytest.skip("skip load balance for multi replicas testcase when querynode number less than 3")
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(replica_number=2)
        # get growing segments
        collection_w.insert(df)
        # get replicas information
        res, _ = collection_w.get_replicas()
        # prepare load balance params
        # find a group which has multi nodes
        group_nodes = []
        for g in res.groups:
            if len(g.group_nodes) >= 2:
                group_nodes = list(g.group_nodes)
                break
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        group_nodes = sorted(
            group_nodes,
            key=lambda x: len(segment_distribution[x]["sealed"]) if x in segment_distribution else 0,
            reverse=True,
        )
        src_node_id = group_nodes[0]
        dst_node_ids = group_nodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids)
        # get segments distribution after load balance
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        sealed_segment_ids_after_load_balance = segment_distribution[src_node_id]["sealed"]
        # assert src node has no sealed segments
        # assert sealed_segment_ids_after_load_balance == []
        des_sealed_segment_ids = []
        for des_node_id in dst_node_ids:
            des_sealed_segment_ids += segment_distribution[des_node_id]["sealed"]
        # assert sealed_segment_ids is subset of des_sealed_segment_ids
        assert set(sealed_segment_ids).issubset(des_sealed_segment_ids)

    @pytest.mark.tags(CaseLabel.L3)
    def test_load_balance_not_in_one_group(self):
        """
        target: test load balance of collection in one group
        method: init a collection, load with multi replicas and load balance among the querynodes in different group
        expected: load balance failed
        """
        # init a collection
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        ms = MilvusSys()
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        # get sealed segments
        collection_w.num_entities
        collection_w.load(replica_number=2)
        # get growing segments
        collection_w.insert(df)
        # get replicas information
        res, _ = collection_w.get_replicas()
        # prepare load balance params
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        # find a group which has multi nodes
        group_nodes = []
        for g in res.groups:
            if len(g.group_nodes) >= 2:
                group_nodes = list(g.group_nodes)
                break
        src_node_id = group_nodes[0]
        dst_node_ids = list(set(all_querynodes) - set(group_nodes))
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(
            collection_w.name,
            src_node_id,
            dst_node_ids,
            sealed_segment_ids,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "must be in the same replica group"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="querycoordv2")
    def test_handoff_query_search(self):
        """
        target: test query search after handoff
        method: 1.load collection
                2.insert, query and search
                3.flush collection and triggere handoff
                4. search with handoff indexed segments
        expected: Search ids before and after handoff are different, because search from growing and search from index
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        collection_w.create_index(default_field_name, default_index_params)
        collection_w.load()

        # handoff: insert and flush one segment
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        term_expr = f"{ct.default_int64_field_name} in {insert_res.primary_keys[:10]}"
        res = df.iloc[:10, :1].to_dict("records")
        collection_w.query(
            term_expr,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": res, "pk_name": collection_w.primary_field.name},
        )
        search_res_before, _ = collection_w.search(
            df[ct.default_float_vec_field_name][:1].to_list(),
            ct.default_float_vec_field_name,
            ct.default_search_params,
            ct.default_limit,
        )
        log.debug(collection_w.num_entities)

        start = time.time()
        while True:
            time.sleep(2)
            segment_infos, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
            # handoff done
            if len(segment_infos) == 1 and segment_infos[0].state == SegmentState.Sealed:
                break
            if time.time() - start > 20:
                raise MilvusException(1, "Get query segment info after handoff cost more than 20s")

        # query and search from handoff segments
        collection_w.query(
            term_expr,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_res": res, "pk_name": collection_w.primary_field.name},
        )
        search_res_after, _ = collection_w.search(
            df[ct.default_float_vec_field_name][:1].to_list(),
            ct.default_float_vec_field_name,
            ct.default_search_params,
            ct.default_limit,
        )
        # the ids between twice search is different because of index building
        # log.debug(search_res_before[0].ids)
        # log.debug(search_res_after[0].ids)
        assert search_res_before[0].ids != search_res_after[0].ids

        # assert search result includes the nq-vector before or after handoff
        assert search_res_after[0].ids[0] == 0
        assert search_res_before[0].ids[0] == search_res_after[0].ids[0]


@pytest.mark.tags(CaseLabel.L3)
class TestUtilityFlushAll(TestcaseBase):
    @pytest.mark.parametrize("with_db", [True, False])
    def test_flush_all_multi_collections(self, with_db):
        """
        target: test flush multi collections
        method: 1.create multi collections
                2. insert data into each collections without flushing
                3. delete some data
                4. flush all collections
                5. verify num entities
                6. create index -> load -> query deleted ids -> query no-deleted ids
        expected: the insert and delete data of all collections are flushed
        """
        collection_num = 5
        collection_names = []
        delete_num = 100

        self._connect()

        if with_db:
            db_name = cf.gen_unique_str("db")
            self.database_wrap.create_database(db_name)
            self.database_wrap.using_database(db_name)

        for i in range(collection_num):
            collection_w, _, _, insert_ids, _ = self.init_collection_general(
                prefix, insert_data=True, is_flush=False, is_index=True
            )
            collection_w.delete(f"{ct.default_int64_field_name} in {insert_ids[:delete_num]}")
            collection_names.append(collection_w.name)

        self.utility_wrap.flush_all(timeout=300)
        cw = ApiCollectionWrapper()
        for c in collection_names:
            cw.init_collection(name=c)
            assert cw.num_entities_without_flush == ct.default_nb

            cw.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
            cw.load()
            cw.query(f"{ct.default_int64_field_name} in {insert_ids[:100]}", check_task=CheckTasks.check_query_empty)

            res, _ = cw.query(f"{ct.default_int64_field_name} not in {insert_ids[:delete_num]}")
            assert len(res) == ct.default_nb - delete_num

    def test_flush_all_db_collections(self):
        """
        target: test flush all db's collections
        method: 1. create collections and insert data in multi dbs
                2. flush all
                3. verify num entities
        expected: the insert and delete data of all db's collections are flushed
        """
        tmp_nb = 100
        db_num = 3
        collection_num = 2
        collection_names = {}
        delete_num = 10

        self._connect()
        for d in range(db_num):
            db_name = cf.gen_unique_str("db")
            self.database_wrap.create_database(db_name)
            self.database_wrap.using_database(db_name)
            db_collection_names = []
            for i in range(collection_num):
                # create collection
                collection_w = self.init_collection_wrap()
                # insert
                insert_res, _ = collection_w.insert(cf.gen_default_dataframe_data(tmp_nb))
                # create index
                collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
                # delete
                collection_w.delete(f"{ct.default_int64_field_name} in {insert_res.primary_keys[:delete_num]}")
                db_collection_names.append(collection_w.name)
            collection_names[db_name] = db_collection_names
            self.utility_wrap.list_collections()

        # flush all db collections
        self.utility_wrap.flush_all(timeout=600)

        for _db, _db_collection_names in collection_names.items():
            self.database_wrap.using_database(_db)
            self.utility_wrap.list_collections()
            for c in _db_collection_names:
                cw = ApiCollectionWrapper()
                cw.init_collection(name=c)
                assert cw.num_entities_without_flush == tmp_nb

                cw.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
                cw.load()
                cw.query(
                    f"{ct.default_int64_field_name} in {insert_res.primary_keys[:delete_num]}",
                    check_task=CheckTasks.check_query_empty,
                )

                res, _ = cw.query(f"{ct.default_int64_field_name} not in {insert_res.primary_keys[:delete_num]}")
                assert len(res) == tmp_nb - delete_num

    def test_flush_empty_db_collections(self):
        """
        target: test flush empty db collections
        method: 1. create collection and insert in the default db
                2. create a db and using the db
                3. flush all
        expected: verify default db's collection flushed
        """
        tmp_nb = 10
        self._connect()

        # create collection
        self.database_wrap.using_database(ct.default_db)
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        # insert
        insert_res, _ = collection_w.insert(cf.gen_default_dataframe_data(tmp_nb))

        # create a db and using db
        db_name = cf.gen_unique_str("db")
        self.database_wrap.create_database(db_name)
        self.database_wrap.using_database(db_name)

        # flush all in the db and no exception
        self.utility_wrap.flush_all(timeout=120)

        # verify default db's collection not flushed
        self.database_wrap.using_database(ct.default_db)
        assert collection_w.num_entities_without_flush == tmp_nb

    def test_flush_all_multi_shards_timeout(self):
        """
        target: test flush_all collection with max shards_num
        method: 1.create collection with max shards_num
                2.insert data
                3.flush all with a small timeout and gets exception
                4.flush and verify num entities
        expected:
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=ct.max_shards_num)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        error = {ct.err_code: 1, ct.err_msg: "wait for flush all timeout"}
        self.utility_wrap.flush_all(timeout=5, check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.flush_all(timeout=120)
        assert collection_w.num_entities_without_flush == ct.default_nb

    def test_flush_all_no_collections(self):
        """
        target: test flush all without any collections
        method: connect and flush all
        expected: no exception
        """
        self._connect()
        self.utility_wrap.flush_all(check_task=None)

    def test_flush_all_async(self):
        """
        target: test flush all collections with _async
        method: flush all collections and _async=True
        expected: finish flush all collection within a period of time
        """
        collection_num = 5
        collection_names = []
        delete_num = 100

        for i in range(collection_num):
            collection_w, _, _, insert_ids, _ = self.init_collection_general(
                prefix, insert_data=True, is_flush=False, is_index=True
            )
            collection_w.delete(f"{ct.default_int64_field_name} in {insert_ids[:delete_num]}")
            collection_names.append(collection_w.name)

        self.utility_wrap.flush_all(_async=True)
        _async_timeout = 60
        cw = ApiCollectionWrapper()
        start = time.time()
        while time.time() - start < _async_timeout:
            time.sleep(2.0)
            flush_flag = False
            for c in collection_names:
                cw.init_collection(name=c)
                if cw.num_entities_without_flush == ct.default_nb:
                    flush_flag = True
                else:
                    log.debug(f"collection num entities: {cw.num_entities_without_flush} of collection {c}")
                    flush_flag = False
            if flush_flag:
                break
            if time.time() - start > _async_timeout:
                raise MilvusException(1, f"Waiting more than {_async_timeout}s for the flush all")

    def test_flush_all_while_insert_delete(self):
        """
        target: test flush all while insert and delete
        method: 1. prepare multi collections
                2. flush_all while inserting and deleting
        expected:
        """
        from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

        collection_num = 5
        collection_names = []
        delete_num = 100
        delete_ids = [_i for _i in range(delete_num)]

        for i in range(collection_num):
            collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=4)
            df = cf.gen_default_dataframe_data(nb=ct.default_nb, start=0)
            collection_w.insert(df)
            collection_names.append(collection_w.name)

        def do_insert():
            cw = ApiCollectionWrapper()
            df = cf.gen_default_dataframe_data(nb=ct.default_nb, start=ct.default_nb)
            for c_name in collection_names:
                cw.init_collection(c_name)
                insert_res, _ = cw.insert(df)
                assert insert_res.insert_count == ct.default_nb

        def do_delete():
            cw = ApiCollectionWrapper()
            for c_name in collection_names:
                cw.init_collection(c_name)
                del_res, _ = cw.delete(f"{ct.default_int64_field_name} in {delete_ids}")
                assert del_res.delete_count == delete_num

        def do_flush_all():
            time.sleep(2)
            self.utility_wrap.flush_all(timeout=600)

        executor = ThreadPoolExecutor(max_workers=3)
        insert_task = executor.submit(do_insert)
        delete_task = executor.submit(do_delete)
        flush_task = executor.submit(do_flush_all)

        # wait all tasks completed
        wait([insert_task, delete_task, flush_task], return_when=ALL_COMPLETED)

        # verify
        for c in collection_names:
            cw = ApiCollectionWrapper()
            cw.init_collection(name=c)
            log.debug(f"num entities: {cw.num_entities_without_flush}")
            assert cw.num_entities_without_flush >= ct.default_nb
            assert cw.num_entities_without_flush <= ct.default_nb * 2

            cw.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
            cw.load()
            cw.query(f"{ct.default_int64_field_name} in {delete_ids}", check_task=CheckTasks.check_query_empty)

            res, _ = cw.query(f"{ct.default_int64_field_name} not in {delete_ids}")
            assert len(res) == ct.default_nb * 2 - delete_num
