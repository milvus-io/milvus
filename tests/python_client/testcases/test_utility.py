import threading
import time

import pytest
from pymilvus import DefaultConfig
from pymilvus.exceptions import MilvusException
from base.client_base import TestcaseBase
from base.collection_wrapper import ApiCollectionWrapper
from base.utility_wrapper import ApiUtilityWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.milvus_sys import MilvusSys
from pymilvus.grpc_gen.common_pb2 import SegmentState
import random

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
    """ Test case of index interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_metric_type(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("metric empty is valid for distance calculation")
        if isinstance(request.param, str):
            pytest.skip("string is valid type for metric")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_metric_value(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("metric empty is valid for distance calculation")
        if not isinstance(request.param, str):
            pytest.skip("Skip invalid type for metric")
        yield request.param

    @pytest.fixture(scope="function", params=["JACCARD", "Superstructure", "Substructure"])
    def get_not_support_metric(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["metric_type", "metric"])
    def get_support_metric_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_partition_names(self, request):
        if isinstance(request.param, list):
            if len(request.param) == 0:
                pytest.skip("empty is valid for partition")
        if request.param is None:
            pytest.skip("None is valid for partition")
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_not_string)
    def get_invalid_type_collection_name(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.get_not_string_value)
    def get_invalid_value_collection_name(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_name_invalid(self, get_invalid_collection_name):
        """
        target: test has_collection with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_collection_name
        if isinstance(c_name, str) and c_name:
            self.utility_wrap.has_collection(
                c_name,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "Invalid collection name"})
        # elif not isinstance(c_name, str): self.utility_wrap.has_collection(c_name, check_task=CheckTasks.err_res,
        # check_items={ct.err_code: 1, ct.err_msg: "illegal"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_collection_name_invalid(self, get_invalid_collection_name):
        """
        target: test has_partition with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_collection_name
        p_name = cf.gen_unique_str(prefix)
        if isinstance(c_name, str) and c_name:
            self.utility_wrap.has_partition(
                c_name, p_name,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "Invalid"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_name_invalid(self, get_invalid_partition_name):
        """
        target: test has_partition with error partition name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtilityWrapper()
        c_name = cf.gen_unique_str(prefix)
        p_name = get_invalid_partition_name
        if isinstance(p_name, str) and p_name:
            ex, _ = ut.has_partition(
                c_name, p_name,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "Invalid"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_collection_name_invalid(self, get_invalid_collection_name):
        self._connect()
        error = f'`collection_name` value {get_invalid_collection_name} is illegal'
        self.utility_wrap.drop_collection(get_invalid_collection_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: error})

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
        ex, _ = ut.list_collections(using=using, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 0, ct.err_msg: "should create connect"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_invalid_name(self, get_invalid_collection_name):
        """
        target: test building_process
        method: input invalid name
        expected: raise exception
        """
        pass
        # self._connect() c_name = get_invalid_collection_name ut = ApiUtilityWrapper() if isinstance(c_name,
        # str) and c_name: ex, _ = ut.index_building_progress(c_name, check_items={ct.err_code: 1, ct.err_msg:
        # "Invalid collection name"})

    # TODO: not support index name
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_process_invalid_index_name(self, get_invalid_index_name):
        """
        target: test building_process
        method: input invalid index name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        ut = ApiUtilityWrapper()
        ex, _ = ut.index_building_progress(c_name, index_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L2)
    def test_wait_index_invalid_name(self, get_invalid_collection_name):
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
        error = {ct.err_code: 1, ct.err_msg: "Invalid collection name: {}".format(invalid_c_name)}
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
        error = {ct.err_code: 1, ct.err_msg: "describe collection failed: can't find collection"}
        self.utility_wrap.loading_progress("not_existed_name", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_loading_progress_invalid_partition_names(self, get_invalid_partition_names):
        """
        target: test loading progress with invalid partition names
        method: input invalid partition names
        expected: raise an exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        partition_names = get_invalid_partition_names
        err_msg = {ct.err_code: 0, ct.err_msg: "`partition_name_array` value {} is illegal".format(partition_names)}
        collection_w.load()
        self.utility_wrap.loading_progress(collection_w.name, partition_names,
                                           check_task=CheckTasks.err_res, check_items=err_msg)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_names", [[ct.default_tag], [ct.default_partition_name, ct.default_tag]])
    def test_loading_progress_not_existed_partitions(self, partition_names):
        """
        target: test loading progress with not existed partitions
        method: input all or part not existed partition names
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix)[0]
        log.debug(collection_w.num_entities)
        collection_w.load()
        err_msg = {ct.err_code: 15, ct.err_msg: f"partition not found"}
        self.utility_wrap.loading_progress(collection_w.name, partition_names,
                                           check_task=CheckTasks.err_res, check_items=err_msg)

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
            check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})

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
            collection_w.name, partition_names=[ct.default_tag],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: f'partitionID of partitionName:{ct.default_tag} can not be find'})

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
            self.utility_wrap.calc_distance(invalid_vector, invalid_vector,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "vectors_left value {} "
                                                                    "is illegal".format(invalid_vector)})

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
            self.utility_wrap.calc_distance(invalid_vector, invalid_vector,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "vectors_left value {} "
                                                                    "is illegal".format(invalid_vector)})

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
            self.utility_wrap.calc_distance(op_l, invalid_vector,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "vectors_right value {} "
                                                                    "is illegal".format(invalid_vector)})

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
            self.utility_wrap.calc_distance(op_l, invalid_vector,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "vectors_right value {} "
                                                                    "is illegal".format(invalid_vector)})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_invalid_metric_type(self, get_support_metric_field, get_invalid_metric_type):
        """
        target: test calculated distance with invalid metric
        method: input invalid metric
        expected: raise exception
        """
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        metric_field = get_support_metric_field
        metric = get_invalid_metric_type
        params = {metric_field: metric}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "params value {{'metric': {}}} "
                                                                "is illegal".format(metric)})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_invalid_metric_value(self, get_support_metric_field, get_invalid_metric_value):
        """
        target: test calculated distance with invalid metric
        method: input invalid metric
        expected: raise exception
        """
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        metric_field = get_support_metric_field
        metric = get_invalid_metric_value
        params = {metric_field: metric}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "{} metric type is invalid for "
                                                                "float vector".format(metric)})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_not_support_metric(self, get_support_metric_field, get_not_support_metric):
        """
        target: test calculated distance with invalid metric
        method: input invalid metric
        expected: raise exception
        """
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        metric_field = get_support_metric_field
        metric = get_not_support_metric
        params = {metric_field: metric}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "{} metric type is invalid for "
                                                                "float vector".format(metric)})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_invalid_using(self, get_support_metric_field):
        """
        target: test calculated distance with invalid using
        method: input invalid using
        expected: raise exception
        """
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        metric_field = get_support_metric_field
        params = {metric_field: "L2", "sqrt": True}
        using = "empty"
        self.utility_wrap.calc_distance(op_l, op_r, params, using=using,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "should create connect"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_not_match_dim(self):
        """
        target: test calculated distance with invalid vectors
        method: input invalid vectors type and value
        expected: raise exception
        """
        self._connect()
        dim = 129
        vector_l = cf.gen_vectors(default_nb, default_dim)
        vector_r = cf.gen_vectors(default_nb, dim)
        op_l = {"float_vectors": vector_l}
        op_r = {"float_vectors": vector_r}
        self.utility_wrap.calc_distance(op_l, op_r,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "Cannot calculate distance between "
                                                                "vectors with different dimension"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_collection_before_load(self, get_support_metric_field):
        """
        target: test calculated distance when entities is not ready
        method: calculate distance before load
        expected: raise exception
        """
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb,
                                                                               is_index=True)
        middle = len(insert_ids) // 2
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "field": default_field_name}
        metric_field = get_support_metric_field
        params = {metric_field: "L2", "sqrt": True}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1,
                                                     "err_msg": "collection {} was not "
                                                                "loaded into memory)".format(collection_w.name)})

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
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "`collection_name` value {} is illegal".format(
                                                             old_collection_name)})

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
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "collection {} was not "
                                                                    "loaded into memory)".format(collection_w.name)})

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
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "`collection_name` value {} is "
                                                                    "illegal".format(new_collection_name)})

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
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 9,
                                                         "err_msg": "collection {} was not "
                                                                    "loaded into memory)".format(collection_w.name)})

    @pytest.mark.tags(CaseLabel.L2)
    def test_rename_collection_not_existed_collection(self):
        """
        target: test rename_collection when the collection name is not existed
        method: input not existing collection name
        expected: raise exception
        """
        self._connect()
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix)
        old_collection_name = "test_collection_non_exist"
        new_collection_name = cf.gen_unique_str(prefix)
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "can't find collection: {}".format(
                                                             collection_w.name)})

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
        self.utility_wrap.rename_collection(old_collection_name, old_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "duplicated new collection name :{} with other "
                                                                    "collection name or alias".format(
                                                             collection_w.name)})

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
        self.utility_wrap.rename_collection(old_collection_name, alias,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "duplicated new collection name :{} with "
                                                                    "other collection name or alias".format(alias)})

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
        self.utility_wrap.rename_collection(alias, new_collection_name,
                                            check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1,
                                                         "err_msg": "unsupported use an alias to "
                                                                    "rename collection, alias:{}".format(alias)})


class TestUtilityBase(TestcaseBase):
    """ Test case of index interface """

    @pytest.fixture(scope="function", params=["metric_type", "metric"])
    def metric_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def sqrt(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["L2", "IP"])
    def metric(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["HAMMING", "TANIMOTO"])
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
            c_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})

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
        exp_res = {'total_rows': 0, 'indexed_rows': 0, 'pending_index_rows': 0}
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
        error = {ct.err_code: 25, ct.err_msg: "there is no index on collection"}
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
        assert res['total_rows'] == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason='wait to modify')
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
            if 0 < res['indexed_rows'] <= nb:
                break
            if time.time() - start > 5:
                raise MilvusException(1, f"Index build completed in more than 5s")

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
            c_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})

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
        exp_res = {'total_rows': 0, 'indexed_rows': 0, 'pending_index_rows': 0}
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
        self.utility_wrap.loading_progress(collection_w.name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1,
                                                        ct.err_msg: 'fail to show collections from '
                                                                    'the querycoord, no data'})

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
        assert res[loading_progress] == '100%'

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
        if -1 != loading_int:
            assert (0 <= loading_int <= 100)
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
        exp_res = {loading_progress: '100%'}

        assert exp_res == res

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue 19754")
    def test_loading_progress_after_release(self):
        """
        target: test loading progress after release
        method: insert and flush data, call loading_progress after release
        expected: return successfully with 0%
        """
        collection_w = self.init_collection_general(prefix, insert_data=True)[0]
        collection_w.release()
        res = self.utility_wrap.loading_progress(collection_w.name)[0]
        exp_res = {loading_progress: '0%', num_loaded_partitions: 0, not_loaded_partitions: ['_default']}

        assert exp_res == res

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
        assert res[loading_progress] == '100%'

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
        assert res[loading_progress] == '100%'

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
        assert res[loading_progress] == '100%'

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
        assert res_collection == {loading_progress: '100%'}

        # create partition and insert
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        partition_w.insert(cf.gen_default_dataframe_data(start=ct.default_nb))
        assert partition_w.num_entities == ct.default_nb
        res_part_partition, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_part_partition == {'loading_progress': '100%'}

        res_part_partition, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_part_partition == {'loading_progress': '100%'}

        collection_w.release()
        collection_w.load(replica_number=2)
        res_all_partitions, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res_all_partitions == {'loading_progress': '100%'}

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
        assert res[loading_progress] == '100%'

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
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_default(self):
        """
        target: test calculated distance with default params
        method: calculated distance between two random vectors
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        log.info("Creating vectors for distance calculation")
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        log.info("Calculating distance for generated vectors")
        self.utility_wrap.calc_distance(op_l, op_r,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_default_sqrt(self, metric_field, metric):
        """
        target: test calculated distance with default param
        method: calculated distance with default sqrt
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        log.info("Creating vectors for distance calculation")
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        log.info("Calculating distance for generated vectors within default sqrt")
        params = {metric_field: metric}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_default_metric(self, sqrt):
        """
        target: test calculated distance with default param
        method: calculated distance with default metric
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        log.info("Creating vectors for distance calculation")
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
        log.info("Calculating distance for generated vectors within default metric")
        params = {"sqrt": sqrt}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_binary_metric(self, metric_field, metric_binary):
        """
        target: test calculate distance with binary vectors
        method: calculate distance between binary vectors
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        log.info("Creating vectors for distance calculation")
        nb = 10
        raw_vectors_l, vectors_l = cf.gen_binary_vectors(nb, default_dim)
        raw_vectors_r, vectors_r = cf.gen_binary_vectors(nb, default_dim)
        op_l = {"bin_vectors": vectors_l}
        op_r = {"bin_vectors": vectors_r}
        log.info("Calculating distance for binary vectors")
        params = {metric_field: metric_binary}
        vectors_l = raw_vectors_l
        vectors_r = raw_vectors_r
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric_binary})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_from_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: both left and right vectors are from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb)
        middle = len(insert_ids) // 2
        vectors = vectors[0].loc[:, default_field_name]
        vectors_l = vectors[:middle]
        vectors_r = []
        for i in range(middle):
            vectors_r.append(vectors[middle + i])
        log.info("Creating vectors from collections for distance calculation")
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "field": default_field_name}
        log.info("Creating vectors for entities")
        params = {metric_field: metric, "sqrt": sqrt}
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_from_collections(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between entities from collections
        method: calculated distance between entities from two collections
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        prefix_1 = "utility_distance"
        log.info("Creating two collections")
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb)
        collection_w_1, vectors_1, _, insert_ids_1, _ = self.init_collection_general(prefix_1, True, nb)
        vectors_l = vectors[0].loc[:, default_field_name]
        vectors_r = vectors_1[0].loc[:, default_field_name]
        log.info("Extracting entities from collections for distance calculating")
        op_l = {"ids": insert_ids, "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"ids": insert_ids_1, "collection": collection_w_1.name,
                "field": default_field_name}
        params = {metric_field: metric, "sqrt": sqrt}
        log.info("Calculating distance for entities from two collections")
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_left_vector_and_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: set left vectors as random vectors, right vectors from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb)
        middle = len(insert_ids) // 2
        vectors = vectors[0].loc[:, default_field_name]
        vectors_l = cf.gen_vectors(nb, default_dim)
        vectors_r = []
        for i in range(middle):
            vectors_r.append(vectors[middle + i])
        op_l = {"float_vectors": vectors_l}
        log.info("Extracting entities from collections for distance calculating")
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "field": default_field_name}
        params = {metric_field: metric, "sqrt": sqrt}
        log.info("Calculating distance between vectors and entities")
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_right_vector_and_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: set right vectors as random vectors, left vectors from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb)
        middle = len(insert_ids) // 2
        vectors = vectors[0].loc[:, default_field_name]
        vectors_l = vectors[:middle]
        vectors_r = cf.gen_vectors(nb, default_dim)
        log.info("Extracting entities from collections for distance calculating")
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"float_vectors": vectors_r}
        params = {metric_field: metric, "sqrt": sqrt}
        log.info("Calculating distance between right vector and entities")
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_from_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from one partition entities
        method: both left and right vectors are from partition
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb, partition_num=1)
        partitions = collection_w.partitions
        middle = len(insert_ids) // 2
        params = {metric_field: metric, "sqrt": sqrt}
        start = 0
        end = middle
        for i in range(len(partitions)):
            log.info("Extracting entities from partitions for distance calculating")
            vectors_l = vectors[i].loc[:, default_field_name]
            vectors_r = vectors[i].loc[:, default_field_name]
            op_l = {"ids": insert_ids[start:end], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            op_r = {"ids": insert_ids[start:end], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            start += middle
            end += middle
            log.info("Calculating distance between entities from one partition")
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_from_partitions(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between entities from partitions
        method: calculate distance between entities from two partitions
        expected: distance calculated successfully
        """
        log.info("Create connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb, partition_num=1)
        partitions = collection_w.partitions
        middle = len(insert_ids) // 2
        params = {metric_field: metric, "sqrt": sqrt}
        vectors_l = vectors[0].loc[:, default_field_name]
        vectors_r = vectors[1].loc[:, default_field_name]
        log.info("Extract entities from two partitions for distance calculating")
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "partition": partitions[0].name, "field": default_field_name}
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "partition": partitions[1].name, "field": default_field_name}
        log.info("Calculate distance between entities from two partitions")
        self.utility_wrap.calc_distance(op_l, op_r, params,
                                        check_task=CheckTasks.check_distance,
                                        check_items={"vectors_l": vectors_l,
                                                     "vectors_r": vectors_r,
                                                     "metric": metric,
                                                     "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_left_vectors_and_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between vectors and partition entities
        method: set left vectors as random vectors, right vectors are entities
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb, partition_num=1)
        middle = len(insert_ids) // 2
        partitions = collection_w.partitions
        vectors_l = cf.gen_vectors(nb // 2, default_dim)
        log.info("Extract entities from collection as right vectors")
        op_l = {"float_vectors": vectors_l}
        params = {metric_field: metric, "sqrt": sqrt}
        start = 0
        end = middle
        log.info("Calculate distance between vector and entities")
        for i in range(len(partitions)):
            vectors_r = vectors[i].loc[:, default_field_name]
            op_r = {"ids": insert_ids[start:end], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            start += middle
            end += middle
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="calc_distance interface is no longer supported")
    def test_calc_distance_right_vectors_and_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between vectors and partition entities
        method: set right vectors as random vectors, left vectors are entities
        expected: distance calculated successfully
        """
        log.info("Create connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids, _ = self.init_collection_general(prefix, True, nb, partition_num=1)
        middle = len(insert_ids) // 2
        partitions = collection_w.partitions
        vectors_r = cf.gen_vectors(nb // 2, default_dim)
        op_r = {"float_vectors": vectors_r}
        params = {metric_field: metric, "sqrt": sqrt}
        start = 0
        end = middle
        for i in range(len(partitions)):
            vectors_l = vectors[i].loc[:, default_field_name]
            log.info("Extract entities from partition %d as left vector" % i)
            op_l = {"ids": insert_ids[start:end], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            start += middle
            end += middle
            log.info("Calculate distance between vector and entities from partition %d" % i)
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})

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
        collection_alias = collection_w.aliases
        self.utility_wrap.rename_collection(old_collection_name, new_collection_name)
        collection_w = self.init_collection_wrap(name=new_collection_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: new_collection_name,
                                                              exp_schema: default_schema})
        collections = self.utility_wrap.list_collections()[0]
        assert new_collection_name in collections
        assert old_collection_name not in collections
        assert collection_alias == collection_w.aliases

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
        collection_w_1 = self.init_collection_wrap(name=old_collection_name_1,
                                                   check_task=CheckTasks.check_collection_property,
                                                   check_items={exp_name: old_collection_name_1,
                                                                exp_schema: default_schema})
        collection_w_2 = self.init_collection_wrap(name=old_collection_name_2,
                                                   check_task=CheckTasks.check_collection_property,
                                                   check_items={exp_name: old_collection_name_2,
                                                                exp_schema: default_schema})
        collections = self.utility_wrap.list_collections()[0]
        assert old_collection_name_1 in collections
        assert old_collection_name_2 in collections
        assert tmp_collection_name not in collections
        # check alias not changed
        assert collection_w_1.aliases[0] == alias_2
        assert collection_w_2.aliases[0] == alias_1

    @pytest.mark.tags(CaseLabel.L2)
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
        collection_w = self.init_collection_wrap(name=old_collection_name,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: old_collection_name,
                                                              exp_schema: default_schema})
        collections = self.utility_wrap.list_collections()[0]
        assert old_collection_name in collections
        assert new_collection_name not in collections
        assert collection_alias == collection_w.aliases

    @pytest.mark.tags(CaseLabel.L2)
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
        # 4. drop the alias
        self.utility_wrap.drop_alias(collection_alias[0])
        # 5. rename collection to the dropped alias name
        self.utility_wrap.rename_collection(old_collection_name, collection_alias[0])
        self.init_collection_wrap(name=collection_alias[0],
                                  check_task=CheckTasks.check_collection_property,
                                  check_items={exp_name: collection_alias[0],
                                               exp_schema: default_schema})
        collections = self.utility_wrap.list_collections()[0]
        assert collection_alias[0] in collections
        assert old_collection_name not in collections


class TestUtilityAdvanced(TestcaseBase):
    """ Test case of index interface """

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
            x = threading.Thread(target=create_and_drop_collection, args=(c_names[i * num:(i + 1) * num],))
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
    @pytest.mark.skip("index must created before load, but create_index will trigger flush")
    def test_get_sealed_query_segment_info(self):
        """
        target: test getting sealed query segment info of collection without index
        method: init a collection, insert data, flush, load, and get query segment info
        expected:
            1. length of segment is equal to 0
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 3000
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        collection_w.num_entities
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        assert len(res) == 0

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
        all_querynodes = sorted(all_querynodes,
                                key=lambda x: len(segment_distribution[x]["sealed"])
                                if x in segment_distribution else 0, reverse=True)
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
        all_querynodes = sorted(all_querynodes,
                                key=lambda x: len(segment_distribution[x]["sealed"])
                                if x in segment_distribution else 0, reverse=True)
        # add node id greater than all querynodes, which is not exist for querynode, to src_node_ids
        max_query_node_id = max(all_querynodes)
        invalid_src_node_id = max_query_node_id + 1
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, invalid_src_node_id, dst_node_ids, sealed_segment_ids,
                                       check_task=CheckTasks.err_res,
                                       check_items={ct.err_code: 1, ct.err_msg: "source node not found in any replica"})

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
        all_querynodes = sorted(all_querynodes,
                                key=lambda x: len(segment_distribution[x]["sealed"])
                                if x in segment_distribution else 0, reverse=True)
        src_node_id = all_querynodes[0]
        # add node id greater than all querynodes, which is not exist for querynode, to dst_node_ids
        max_query_node_id = max(all_querynodes)
        dst_node_ids = [id for id in range(max_query_node_id + 1, max_query_node_id + 3)]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids,
                                       check_task=CheckTasks.err_res,
                                       check_items={ct.err_code: 1, ct.err_msg: "no available queryNode to allocate"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/19441")
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
        collection_w.num_entities
        # get growing segments
        collection_w.insert(df)
        collection_w.load()
        # prepare load balance params
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        all_querynodes = [node["identifier"] for node in ms.query_nodes]
        all_querynodes = sorted(all_querynodes,
                                key=lambda x: len(segment_distribution[x]["sealed"])
                                if x in segment_distribution else 0, reverse=True)
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # add a segment id which is not exist
        sealed_segment_ids.append(max(segment_distribution[src_node_id]["sealed"]) + 1)
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids,
                                       check_task=CheckTasks.err_res,
                                       check_items={ct.err_code: 1, ct.err_msg: "not found in source node"})

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
        all_querynodes = sorted(all_querynodes,
                                key=lambda x: len(segment_distribution[x]["sealed"])
                                if x in segment_distribution else 0, reverse=True)
        src_node_id = all_querynodes[0]
        dst_node_ids = all_querynodes[1:]
        # add segment ids which are not exist
        sealed_segment_ids = [sealed_segment_id
                              for sealed_segment_id in range(max(segment_distribution[src_node_id]["sealed"]) + 100,
                                                             max(segment_distribution[src_node_id]["sealed"]) + 103)]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids,
                                       check_task=CheckTasks.err_res,
                                       check_items={ct.err_code: 1, ct.err_msg: "not found in source node"})

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
        group_nodes = sorted(group_nodes,
                             key=lambda x: len(
                                 segment_distribution[x]["sealed"])
                             if x in segment_distribution else 0, reverse=True)
        src_node_id = group_nodes[0]
        dst_node_ids = group_nodes[1:]
        sealed_segment_ids = segment_distribution[src_node_id]["sealed"]
        # load balance
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids)
        # get segments distribution after load balance
        res, _ = self.utility_wrap.get_query_segment_info(c_name)
        segment_distribution = cf.get_segment_distribution(res)
        sealed_segment_ids_after_load_banalce = segment_distribution[src_node_id]["sealed"]
        # assert src node has no sealed segments
        assert sealed_segment_ids_after_load_banalce == []
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
        self.utility_wrap.load_balance(collection_w.name, src_node_id, dst_node_ids, sealed_segment_ids,
                                       check_task=CheckTasks.err_res,
                                       check_items={ct.err_code: 1, ct.err_msg: "must be in the same replica group"})

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
        term_expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:10]}'
        res = df.iloc[:10, :1].to_dict('records')
        collection_w.query(term_expr, check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': res})
        search_res_before, _ = collection_w.search(df[ct.default_float_vec_field_name][:1].to_list(),
                                                   ct.default_float_vec_field_name,
                                                   ct.default_search_params, ct.default_limit)
        log.debug(collection_w.num_entities)

        start = time.time()
        while True:
            time.sleep(2)
            segment_infos, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
            # handoff done
            if len(segment_infos) == 1 and segment_infos[0].state == SegmentState.Sealed:
                break
            if time.time() - start > 20:
                raise MilvusException(1, f"Get query segment info after handoff cost more than 20s")

        # query and search from handoff segments
        collection_w.query(term_expr, check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': res})
        search_res_after, _ = collection_w.search(df[ct.default_float_vec_field_name][:1].to_list(),
                                                  ct.default_float_vec_field_name,
                                                  ct.default_search_params, ct.default_limit)
        # the ids between twice search is different because of index building
        # log.debug(search_res_before[0].ids)
        # log.debug(search_res_after[0].ids)
        assert search_res_before[0].ids != search_res_after[0].ids

        # assert search result includes the nq-vector before or after handoff
        assert search_res_after[0].ids[0] == 0
        assert search_res_before[0].ids[0] == search_res_after[0].ids[0]


class TestUtilityUserPassword(TestcaseBase):
    """ Test case of user interface """

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    def test_create_user_with_user_password(self, host, port):
        """
        target: test the user creation with user and password
        method: create user with the default user and password parameter
        expected: connected is True
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = "nico"
        password = "wertyu567"
        self.utility_wrap.create_user(user=user, password=password)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user, password=password,
                                     check_task=ct.CheckTasks.ccr)
        self.utility_wrap.list_collections()

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("old_password", ["abc1234"])
    @pytest.mark.parametrize("new_password", ["abc12345"])
    def test_reset_password_with_user_and_old_password(self, host, port, old_password, new_password):
        """
        target: test the password reset with old password
        method: get a connection with user and corresponding old password
        expected: connected is True
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = "robot2048"
        self.utility_wrap.create_user(user=user, password=old_password)
        self.utility_wrap.reset_password(user=user, old_password=old_password, new_password=new_password)
        self.utility_wrap.list_collections()

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("old_password", ["abc1234"])
    @pytest.mark.parametrize("new_password", ["abc12345"])
    def test_update_password_with_user_and_old_password(self, host, port, old_password, new_password):
        """
        target: test the password update with old password
        method: get a connection with user and corresponding old password
        expected: connected is True
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = "robot2060"
        self.utility_wrap.create_user(user=user, password=old_password)
        self.utility_wrap.update_password(user=user, old_password=old_password, new_password=new_password)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=new_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.list_collections()

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    def test_list_usernames(self, host, port):
        """
        target: test the user list created successfully
        method: get a list of users
        expected: list all users
        """
        # 1. default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2. create 2 users
        self.utility_wrap.create_user(user="user1", password="abc123")
        self.utility_wrap.create_user(user="user2", password="abc123")

        # 3. list all users
        res = self.utility_wrap.list_usernames()[0]
        assert "user1" and "user2" in res

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("connect_name", [DefaultConfig.DEFAULT_USING])
    def test_delete_user_with_username(self, host, port, connect_name):
        """
        target: test deleting user with username
        method: delete user with username and connect with the wrong user then list collections
        expected: deleted successfully
        """
        user = "xiaoai"
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.create_user(user=user, password="abc123")
        self.utility_wrap.delete_user(user=user)
        self.connection_wrap.disconnect(alias=connect_name)
        self.connection_wrap.connect(host=host, port=port, user=user, password="abc123",
                                     check_task=ct.CheckTasks.err_res,
                                     check_items={ct.err_code: 2,
                                                  ct.err_msg: "Fail connecting to server"})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    def test_delete_user_with_invalid_username(self, host, port):
        """
        target: test the nonexistant user when deleting credential
        method: delete a credential with user wrong
        excepted: delete is true
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.delete_user(user="asdfghj")

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    def test_delete_all_users(self, host, port):
        """
        target: delete the users that created for test
        method: delete the users in list_usernames except root
        excepted: delete is true
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        res = self.utility_wrap.list_usernames()[0]
        for user in res:
            if user != "root":
                self.utility_wrap.delete_user(user=user)
        res = self.utility_wrap.list_usernames()[0]
        assert len(res) == 1


class TestUtilityInvalidUserPassword(TestcaseBase):
    """ Test invalid case of user interface """

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["qwertyuiopasdfghjklzxcvbnmqwertyui", "@*-.-*", "alisd/"])
    def test_create_user_with_invalid_username(self, host, port, user):
        """
        target: test the user when create user
        method: make the length of user beyond standard
        excepted: the creation is false
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.create_user(user=user, password=ct.default_password,
                                      check_task=ct.CheckTasks.err_res,
                                      check_items={ct.err_code: 5})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["alice123w"])
    def test_create_user_with_existed_username(self, host, port, user):
        """
        target: test the user when create user
        method: create a user, and then create a user with the same username
        excepted: the creation is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create the first user successfully
        self.utility_wrap.create_user(user=user, password=ct.default_password)

        # 3.create the second user with the same username
        self.utility_wrap.create_user(user=user, password=ct.default_password,
                                      check_task=ct.CheckTasks.err_res, check_items={ct.err_code: 29})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("password", ["12345"])
    def test_create_user_with_invalid_password(self, host, port, password):
        """
        target: test the password when create user
        method: make the length of user exceed the limitation [6, 256]
        excepted: the creation is false
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = "alice"
        self.utility_wrap.create_user(user=user, password=password,
                                      check_task=ct.CheckTasks.err_res, check_items={ct.err_code: 5})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["hobo89"])
    @pytest.mark.parametrize("old_password", ["qwaszx0"])
    def test_reset_password_with_invalid_username(self, host, port, user, old_password):
        """
        target: test the wrong user when resetting password
        method: create a user, and then reset the password with wrong username
        excepted: reset is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create a user
        self.utility_wrap.create_user(user=user, password=old_password)

        # 3.reset password with the wrong username
        self.utility_wrap.reset_password(user="hobo", old_password=old_password, new_password="qwaszx1",
                                         check_task=ct.CheckTasks.err_res,
                                         check_items={ct.err_code: 30})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["demo"])
    @pytest.mark.parametrize("old_password", ["qwaszx0"])
    @pytest.mark.parametrize("new_password", ["12345"])
    def test_reset_password_with_invalid_new_password(self, host, port, user, old_password, new_password):
        """
        target: test the new password when resetting password
        method: create a user, and then set a wrong new password
        excepted: reset is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create a user
        self.utility_wrap.create_user(user=user, password=old_password)

        # 3.reset password with the wrong new password
        self.utility_wrap.reset_password(user=user, old_password=old_password, new_password=new_password,
                                         check_task=ct.CheckTasks.err_res,
                                         check_items={ct.err_code: 5})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["genny001"])
    def test_reset_password_with_invalid_old_password(self, host, port, user):
        """
        target: test the old password when resetting password
        method: create a credential, and then reset with a wrong old password
        excepted: reset is false
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.create_user(user=user, password="qwaszx0")
        self.utility_wrap.reset_password(user=user, old_password="waszx0", new_password="123456",
                                         check_task=ct.CheckTasks.err_res,
                                         check_items={ct.err_code: 30})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["hobo233"])
    @pytest.mark.parametrize("old_password", ["qwaszx0"])
    def test_update_password_with_invalid_username(self, host, port, user, old_password):
        """
        target: test the wrong user when resetting password
        method: create a user, and then reset the password with wrong username
        excepted: reset is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create a user
        self.utility_wrap.create_user(user=user, password=old_password)

        # 3.reset password with the wrong username
        self.utility_wrap.update_password(user="hobo", old_password=old_password, new_password="qwaszx1",
                                          check_task=ct.CheckTasks.err_res,
                                          check_items={ct.err_code: 30})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["demo001"])
    @pytest.mark.parametrize("old_password", ["qwaszx0"])
    @pytest.mark.parametrize("new_password", ["12345"])
    def test_update_password_with_invalid_new_password(self, host, port, user, old_password, new_password):
        """
        target: test the new password when resetting password
        method: create a user, and then set a wrong new password
        excepted: reset is false
        """
        # 1.default user login
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # 2.create a user
        self.utility_wrap.create_user(user=user, password=old_password)

        # 3.reset password with the wrong new password
        self.utility_wrap.update_password(user=user, old_password=old_password, new_password=new_password,
                                          check_task=ct.CheckTasks.err_res,
                                          check_items={ct.err_code: 5})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    @pytest.mark.parametrize("user", ["genny"])
    def test_update_password_with_invalid_old_password(self, host, port, user):
        """
        target: test the old password when resetting password
        method: create a credential, and then reset with a wrong old password
        excepted: reset is false
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.create_user(user=user, password="qwaszx0")
        self.utility_wrap.update_password(user=user, old_password="waszx0", new_password="123456",
                                          check_task=ct.CheckTasks.err_res,
                                          check_items={ct.err_code: 30})

    @pytest.mark.tags(ct.CaseLabel.RBAC)
    def test_delete_user_root(self, host, port):
        """
        target: test deleting user root when deleting credential
        method: connect and then delete the user root
        excepted: delete is false
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.delete_user(user=ct.default_user, check_task=ct.CheckTasks.err_res,
                                      check_items={ct.err_code: 31})


class TestUtilityRBAC(TestcaseBase):

    def teardown_method(self, method):
        """
        teardown method: drop role and user
        """
        log.info("[utility_teardown_method] Start teardown utility test cases ...")

        self.connection_wrap.connect(host=cf.param_info.param_host, port=cf.param_info.param_port, user=ct.default_user,
                                     password=ct.default_password, secure=cf.param_info.param_secure)

        # drop users
        users, _ = self.utility_wrap.list_users(False)
        for u in users.groups:
            if u.username != ct.default_user:
                self.utility_wrap.delete_user(u.username)
        user_groups, _ = self.utility_wrap.list_users(False)
        assert len(user_groups.groups) == 1

        role_groups, _ = self.utility_wrap.list_roles(False)

        # drop roles
        for role_group in role_groups.groups:
            if role_group.role_name not in ['admin', 'public']:
                self.utility_wrap.init_role(role_group.role_name)
                g_list, _ = self.utility_wrap.role_list_grants()
                for g in g_list.groups:
                    self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
                self.utility_wrap.role_drop()
        role_groups, _ = self.utility_wrap.list_roles(False)
        assert len(role_groups.groups) == 2

        super().teardown_method(method)

    def init_db_kwargs(self, with_db):
        """
        init db name kwargs
        """
        db_kwargs = {}
        if with_db:
            db_name = cf.gen_unique_str("db")
            self.database_wrap.create_database(db_name)
            db_kwargs = {"db_name": db_name}
        return db_kwargs

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_clear_roles(self, host, port):
        """
        target: check get roles list and clear them
        method: remove all roles except admin and public
        expected: assert clear success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # add user and bind to role
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)

        usernames, _ = self.utility_wrap.list_usernames()
        for username in usernames:
            if username != "root":
                self.utility_wrap.delete_user(username)

        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        # get roles
        role_groups, _ = self.utility_wrap.list_roles(False)

        # drop roles
        for role_group in role_groups.groups:
            if role_group.role_name not in ['admin', 'public']:
                self.utility_wrap.init_role(role_group.role_name)
                g_list, _ = self.utility_wrap.role_list_grants()
                for g in g_list.groups:
                    self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
                self.utility_wrap.role_drop()
        role_groups, _ = self.utility_wrap.list_roles(False)
        assert len(role_groups.groups) == 2

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_role_list_user_with_root_user(self, host, port):
        """
        target: check list user
        method: check list user with root
        expected: assert list user success, and root has no roles
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        user_info, _ = self.utility_wrap.list_user("root", True)
        user_item = user_info.groups[0]
        assert user_item.roles == ()
        assert user_item.username == "root"

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_role_list_users(self, host, port):
        """
        target: check list users
        method: check list users con
        expected: assert list users success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        # add user and bind to role
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        # get users
        user_info, _ = self.utility_wrap.list_users(True)

        # check root user and new user
        root_exist = False
        new_user_exist = False
        for user_item in user_info.groups:
            if user_item.username == "root" and len(user_item.roles) == 0:
                root_exist = True
            if user_item.username == user and user_item.roles[0] == r_name:
                new_user_exist = True
        assert root_exist
        assert new_user_exist

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_create_role(self, host, port):
        """
        target: test create role
        method: test create role with random name
        expected: assert role create success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name, check_task=CheckTasks.check_role_property,
                                    check_items={exp_name: r_name})
        assert not self.utility_wrap.role_is_exist()[0]
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_drop_role(self, host, port):
        """
        target: test drop role
        method: create a role, drop this role
        expected: assert role drop success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_drop()
        assert not self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_add_user_to_role(self, host, port):
        """
        target: test add user to role
        method: create a new user,add user to role
        expected: assert add user success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)
        users, _ = self.utility_wrap.role_get_users()
        user_info, _ = self.utility_wrap.list_user(user, True)
        user_item = user_info.groups[0]
        assert r_name in user_item.roles
        assert user in users

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_remove_user_from_role(self, host, port):
        """
        target: test remove user from role
        method: create a new user,add user to role, remove user from role
        expected: assert remove user from role success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)
        self.utility_wrap.role_remove_user(user)
        users, _ = self.utility_wrap.role_get_users()
        assert len(users) == 0

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_role_is_exist(self, host, port):
        """
        target: test role is existed
        method: check not exist role and exist role
        expected: assert is_exist interface is correct
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        r_not_exist = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]
        self.utility_wrap.init_role(r_not_exist)
        assert not self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_role_grant_collection_insert(self, host, port):
        """
        target: test grant role collection insert privilege
        method: create one role and tow collections, grant one collection insert privilege
        expected: assert grant privilege success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name, check_task=CheckTasks.check_role_property,
                                    check_items={exp_name: r_name})
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        self.init_collection_wrap(name=c_name)
        self.init_collection_wrap(name=c_name_2)

        # verify user default privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data, check_task=CheckTasks.check_permission_deny)
        collection_w2 = self.init_collection_wrap(name=c_name_2)
        collection_w2.insert(data=data, check_task=CheckTasks.check_permission_deny)

        # grant user collection insert privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.role_grant("Collection", c_name, "Insert")

        # verify user specific collection insert privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.insert(data=data)

        # verify grant scope
        index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params,
                                  check_task=CheckTasks.check_permission_deny)
        collection_w2 = self.init_collection_wrap(name=c_name_2)
        collection_w2.insert(data=data, check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_revoke_public_role_privilege(self, host, port):
        """
        target: revoke public role privilege
        method: revoke public role privilege
        expected: success to revoke
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role("public")
        self.utility_wrap.role_add_user(user)
        self.utility_wrap.role_revoke("Collection", c_name, "Insert")
        data = cf.gen_default_list_data(ct.default_nb)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.insert(data=data, check_task=CheckTasks.check_permission_deny)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role("public")
        self.utility_wrap.role_grant("Collection", c_name, "Insert")

    @pytest.mark.tags(CaseLabel.L2)
    def test_revoke_user_after_delete_user(self, host, port):
        """
        target: test revoke user with deleted user
        method: 1. create user -> create a role -> role add user
                2. delete user
                3. revoke the deleted user
        expected: revoke successfully
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        # create user
        username = cf.gen_unique_str("user")
        u, _ = self.utility_wrap.create_user(user=username, password=ct.default_password)

        # create a role and bind user
        role_name = cf.gen_unique_str("role")
        self.utility_wrap.init_role(role_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(username)

        # delete user
        self.utility_wrap.delete_user(username)

        # get role users
        users, _ = self.utility_wrap.role_get_users()
        log.debug(users)

        # re-create the user with different password
        self.utility_wrap.create_user(user=username, password=ct.default_password + "aaa")
        self.connection_wrap.connect(alias="re-user", host=host, port=port, user=username,
                                     password=ct.default_password + "aaa", check_task=ct.CheckTasks.ccr)
        self.utility_wrap.list_collections(using="re-user")

        # delete user and remove user successfully
        self.utility_wrap.delete_user(username)
        self.utility_wrap.role_remove_user(username)

        # get role users and verify user removed
        role_users, _ = self.utility_wrap.role_get_users()
        assert username not in role_users

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [True, False])
    def test_role_revoke_collection_privilege(self, host, port, with_db):
        """
        target: test revoke role collection privilege,
        method: create role and collection, grant role insert privilege, revoke privilege
        expected: assert revoke privilege success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        # self.init_collection_wrap(name=c_name)
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)

        # grant user collection insert privilege
        self.utility_wrap.role_grant("Collection", c_name, "Insert", **db_kwargs)
        self.utility_wrap.role_list_grants(**db_kwargs)

        # verify user specific collection insert privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)

        # revoke privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.role_revoke("Collection", c_name, "Insert", **db_kwargs)

        # verify revoke is success
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.insert(data=data, check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_role_revoke_global_privilege(self, host, port, with_db):
        """
        target: test revoke role global privilege,
        method: create role, grant role global createcollection privilege, revoke privilege
        expected: assert revoke privilege success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        # grant user Global CreateCollection privilege
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "CreateCollection", **db_kwargs)

        # verify user specific Global CreateCollection privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w = self.init_collection_wrap(name=c_name)

        # revoke privilege
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.role_revoke("Global", "*", "CreateCollection", **db_kwargs)

        # verify revoke is success
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w = self.init_collection_wrap(name=c_name_2,
                                                 check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_role_revoke_user_privilege(self, host, port, with_db):
        """
        target: test revoke role user privilege,
        method: create role, grant role user updateuser privilege, revoke privilege
        expected: assert revoke privilege success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        # grant user User UpdateUser privilege
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("User", "*", "UpdateUser", **db_kwargs)
        self.utility_wrap.role_revoke("User", "*", "UpdateUser", **db_kwargs)

        # verify revoke is success
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        self.utility_wrap.reset_password(user=user_test, old_password=password_test, new_password=password,
                                         check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_role_list_grants(self, host, port, with_db):
        """
        target: test grant role privileges and list them
        method: grant role privileges and list them
        expected: assert list granted privileges success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        self.init_collection_wrap(name=c_name)

        # grant user privilege
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.init_role(r_name)
        grant_list = cf.gen_grant_list(c_name)
        for grant_item in grant_list:
            self.utility_wrap.role_grant(grant_item["object"], grant_item["object_name"], grant_item["privilege"],
                                         **db_kwargs)

        # list grants
        g_list, _ = self.utility_wrap.role_list_grants(**db_kwargs)
        assert len(g_list.groups) == len(grant_list)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_drop_role_which_bind_user(self, host, port):
        """
        target: drop role which bind user
        method: create a role, bind user to the role, drop the role
        expected: drop success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user)

        self.utility_wrap.role_drop()
        assert not self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("name", ["admin", "public"])
    def test_add_user_to_default_role(self, name, host, port):
        """
        target: add user to admin role or public role
        method: create a user,add user to admin role or public role
        expected: add success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(name)
        self.utility_wrap.role_add_user(user)
        self.utility_wrap.role_add_user(user)
        users, _ = self.utility_wrap.role_get_users()
        user_info, _ = self.utility_wrap.list_user(user, True)
        user_item = user_info.groups[0]
        assert name in user_item.roles
        assert user in users

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_add_root_to_new_role(self, host, port):
        """
        target: add root to new role
        method: add root to new role
        expected: add success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user("root")
        users, _ = self.utility_wrap.role_get_users()
        user_info, _ = self.utility_wrap.list_user("root", True)
        user_item = user_info.groups[0]
        assert r_name in user_item.roles
        assert "root" in users
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_collection_grands_by_role_and_object(self, host, port):
        """
        target: list grants by role and object
        method: create a new role,grant role collection privilege,list grants by role and object
        expected: list success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_grant("Collection", c_name, "Search")
        self.utility_wrap.role_grant("Collection", c_name, "Insert")

        g_list, _ = self.utility_wrap.role_list_grant("Collection", c_name)
        assert len(g_list.groups) == 2
        for g in g_list.groups:
            assert g.object == "Collection"
            assert g.object_name == c_name
            assert g.privilege in ["Search", "Insert"]
            self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_global_grants_by_role_and_object(self, host, port):
        """
        target: list grants by role and object
        method: create a new role,grant role global privilege,list grants by role and object
        expected: list success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_grant("Global", "*", "CreateCollection")
        self.utility_wrap.role_grant("Global", "*", "All")

        g_list, _ = self.utility_wrap.role_list_grant("Global", "*")
        assert len(g_list.groups) == 2
        for g in g_list.groups:
            assert g.object == "Global"
            assert g.object_name == "*"
            assert g.privilege in ["CreateCollection", "All"]
            self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_verify_admin_role_privilege(self, host, port):
        """
        target: verify admin role privilege
        method: create a new user, bind to admin role, crud collection
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role("admin")
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.role_add_user(user)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        assert collection_w.num_entities == ct.default_nb
        collection_w.release()
        collection_w.drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_collection_load_privilege(self, host, port, with_db):
        """
        target: verify grant collection load privilege
        method: verify grant collection load privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)

        self.utility_wrap.role_grant("Collection", c_name, "Load", **db_kwargs)
        self.utility_wrap.role_grant("Collection", c_name, "GetLoadingProgress", **db_kwargs)
        log.debug(self.utility_wrap.role_list_grants(**db_kwargs))

        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(100)
        mutation_res, _ = collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        log.debug(collection_w.name)
        collection_w.load()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_collection_release_privilege(self, host, port, with_db):
        """
        target: verify grant collection release privilege
        method: verify grant collection release privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)

        self.utility_wrap.role_grant("Collection", c_name, "Release", **db_kwargs)

        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(100)
        mutation_res, _ = collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        self.utility_wrap.role_list_grants(**db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        collection_w.release()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/23945 not supported compaction privilege")
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_collection_compaction_privilege(self, host, port, with_db):
        """
        target: verify grant collection compaction privilege
        method: verify grant collection compaction privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str("user")
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str("coll")
        r_name = cf.gen_unique_str("role")
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)

        self.utility_wrap.role_grant("Collection", c_name, "Compaction", **db_kwargs)
        self.utility_wrap.role_list_grant("Collection", c_name, **db_kwargs)
        self.utility_wrap.role_get_users()

        # create collection in the db
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w.compact()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_collection_insert_privilege(self, host, port, with_db):
        """
        target: verify grant collection insert privilege
        method: verify grant collection insert privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)

        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)

        # with db
        self.utility_wrap.role_grant("Collection", c_name, "Insert", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_collection_delete_privilege(self, host, port, with_db):
        """
        target: verify grant collection delete privilege
        method: verify grant collection delete privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)

        # with db
        self.utility_wrap.role_grant("Collection", c_name, "Delete", **db_kwargs)

        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(tmp_expr)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_create_index_privilege(self, host, port, with_db):
        """
        target: verify grant create index privilege
        method: verify grant create index privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)

        self.utility_wrap.role_grant("Collection", c_name, "CreateIndex", **db_kwargs)
        self.utility_wrap.role_grant("Collection", c_name, "Flush", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        self.index_wrap.init_index(collection_w.collection, ct.default_int64_field_name,
                                   default_index_params)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_drop_index_privilege(self, host, port, with_db):
        """
        target: verify grant drop index privilege
        method: verify grant drop index privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, ct.default_int64_field_name,
                                   default_index_params)

        self.utility_wrap.role_grant("Collection", c_name, "DropIndex", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        self.index_wrap.drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_collection_search_privilege(self, host, port, with_db):
        """
        target: verify grant collection search privilege
        method: verify grant collection search privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        self.utility_wrap.role_grant("Collection", c_name, "Search", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:ct.default_nq], ct.default_float_vec_field_name,
                            ct.default_search_params, ct.default_limit,
                            "int64 >= 0", check_task=CheckTasks.check_search_results,
                            check_items={"nq": ct.default_nq,
                                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_collection_flush_privilege(self, host, port, with_db):
        """
        target: verify grant collection flush privilege
        method: verify grant collection flush privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        self.utility_wrap.role_grant("Collection", c_name, "Flush", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w.flush()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_collection_query_privilege(self, host, port, with_db):
        """
        target: verify grant collection query privilege
        method: verify grant collection query privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(100)
        mutation_res, _ = collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()

        self.utility_wrap.role_grant("Collection", c_name, "Query", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
        res, _ = collection_w.query(default_term_expr)
        assert len(res) == 2

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_all_privilege(self, host, port, with_db):
        """
        target: verify grant global all privilege
        method: verify grant global all privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "All", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.drop()
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()
        self.utility_wrap.role_add_user(user_test)
        self.utility_wrap.role_grant("Collection", c_name, "Insert")
        self.utility_wrap.role_revoke("Collection", c_name, "Insert")
        self.utility_wrap.role_remove_user(user_test)

        self.utility_wrap.delete_user(user=user_test)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_create_collection_privilege(self, host, port, with_db):
        """
        target: verify grant global create collection privilege
        method: verify grant global create collection privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "CreateCollection", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        self.init_collection_wrap(name=c_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_drop_collection_privilege(self, host, port, with_db):
        """
        target: verify grant global drop collection privilege
        method: verify grant global drop collection privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "DropCollection", **db_kwargs)
        collection_w = self.init_collection_wrap(name=c_name)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        collection_w.drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_create_ownership_privilege(self, host, port, with_db):
        """
        target: verify grant global create ownership privilege
        method: verify grant global create ownership privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "CreateOwnership", **db_kwargs)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_drop_ownership_privilege(self, host, port, with_db):
        """
        target: verify grant global drop ownership privilege
        method: verify grant global drop ownership privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "DropOwnership", **db_kwargs)

        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        self.utility_wrap.role_drop()
        self.utility_wrap.delete_user(user=user_test)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_select_ownership_privilege(self, host, port, with_db):
        """
        target: verify grant global select ownership privilege
        method: verify grant global select ownership privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "SelectOwnership", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        self.utility_wrap.list_usernames()
        self.utility_wrap.role_list_grants()
        self.utility_wrap.list_roles(False)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_global_manage_ownership_privilege(self, host, port, with_db):
        """
        target: verify grant global manage ownership privilege
        method: verify grant global manage ownership privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()

        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("Global", "*", "ManageOwnership", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        self.utility_wrap.role_add_user(user_test)
        self.utility_wrap.role_remove_user(user_test)
        self.utility_wrap.role_grant("Collection", c_name, "Search")
        self.utility_wrap.role_revoke("Collection", c_name, "Search")

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_user_update_privilege(self, host, port, with_db):
        """
        target: verify grant user update privilege
        method: verify grant user update privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()

        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("User", "*", "UpdateUser", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)
        self.utility_wrap.reset_password(user=user_test, old_password=password_test, new_password=password)
        self.utility_wrap.update_password(user=user_test, old_password=password, new_password=password_test)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_select_user_privilege(self, host, port, with_db):
        """
        target: verify grant select user privilege
        method: verify grant select user privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        r_test = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_test)
        self.utility_wrap.create_role()

        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)
        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        self.utility_wrap.role_grant("User", "*", "SelectUser", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        self.utility_wrap.list_user(username=user_test, include_role_info=False)
        self.utility_wrap.list_users(include_role_info=False)

    def test_admin_user_after_db_deleted(self, host, port):
        """
        target: test admin role can opearte after db deleted
        method: 1.root connect -> create collection in
                2.create co
        expected:
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        collection_w_default = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # create db and create collection
        self.database_wrap.create_database("db")
        self.database_wrap.create_database("db2")
        # self.database_wrap.using_database("db")
        # collection_w_db = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # create a user and bind to admin role
        self.utility_wrap.create_user("u1", "Milvus")
        self.utility_wrap.init_role("admin")
        self.utility_wrap.role_add_user("u1")

        # drop db
        self.database_wrap.drop_database("db")
        self.database_wrap.list_database()
        self.utility_wrap.list_collections()
        self.database_wrap.using_database("db2")
        self.database_wrap.list_database()
        self.utility_wrap.list_collections()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_privilege_with_wildcard_object_name(self, host, port, with_db):
        """
        target: verify grant privilege with wildcard instead of object name
        method: verify grant privilege with wildcard instead of object name
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)

        collection_w = self.init_collection_wrap(name=c_name)
        collection_w2 = self.init_collection_wrap(name=c_name_2)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w2.create_index(ct.default_float_vec_field_name, default_index_params)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        self.utility_wrap.role_grant("Collection", "*", "Load", **db_kwargs)
        self.utility_wrap.role_grant("Collection", "*", "GetLoadingProgress", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        collection_w.load()
        collection_w2.load()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("with_db", [False, True])
    def test_verify_grant_privilege_with_wildcard_privilege(self, host, port, with_db):
        """
        target: verify grant privilege with wildcard instead of privilege
        method: verify grant privilege with wildcard instead of privilege
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)

        # with db
        db_kwargs = self.init_db_kwargs(with_db)
        db_name = db_kwargs.get("db_name", ct.default_db)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=c_name)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        self.utility_wrap.role_add_user(user)

        self.utility_wrap.role_grant("Collection", "*", "*", **db_kwargs)

        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr, **db_kwargs)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        collection_w.release()
        collection_w.compact()
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data)
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(tmp_expr)
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.load()
        collection_w.search(vectors[:ct.default_nq], ct.default_float_vec_field_name,
                            ct.default_search_params, ct.default_limit,
                            "int64 >= 0")
        collection_w.flush()
        default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
        collection_w.query(default_term_expr)
        collection_w.release()
        collection_w.drop_index()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_new_user_default_owns_public_role_permission(self, host, port):
        """
        target: new user owns public role privilege
        method: create a role,verify its permission
        expected: verify success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user_test = cf.gen_unique_str(prefix)
        password_test = cf.gen_unique_str(prefix)
        self.utility_wrap.create_user(user=user_test, password=password_test)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        collection_w = self.init_collection_wrap(name=c_name)
        _, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        self.connection_wrap.disconnect(alias=DefaultConfig.DEFAULT_USING)
        self.connection_wrap.connect(host=host, port=port, user=user,
                                     password=password, check_task=ct.CheckTasks.ccr)

        # Collection permission deny
        collection_w.load(check_task=CheckTasks.check_permission_deny)
        collection_w.release(check_task=CheckTasks.check_permission_deny)
        collection_w.compact(check_task=CheckTasks.check_permission_deny)
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data, check_task=CheckTasks.check_permission_deny)
        tmp_expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(tmp_expr, check_task=CheckTasks.check_permission_deny)
        self.index_wrap.drop(ct.default_int64_field_name, check_task=CheckTasks.check_permission_deny)
        self.index_wrap.init_index(collection_w.collection, ct.default_int64_field_name,
                                   default_index_params, check_task=CheckTasks.check_permission_deny)
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:ct.default_nq], ct.default_float_vec_field_name,
                            ct.default_search_params, ct.default_limit,
                            "int64 >= 0", check_task=CheckTasks.check_permission_deny)
        collection_w.flush(check_task=CheckTasks.check_permission_deny)
        default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
        collection_w.query(default_term_expr, check_task=CheckTasks.check_permission_deny)
        # self.utility_wrap.bulk_insert(c_name, check_task=CheckTasks.check_permission_deny)

        # Global permission deny
        self.init_collection_wrap(name=c_name_2, check_task=CheckTasks.check_permission_deny)
        collection_w.drop(check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.create_user(user=c_name, password=password, check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role(check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.delete_user(user=user, check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_drop(check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.list_usernames(check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_list_grants(check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.list_roles(False, check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_add_user(user, check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_remove_user(user, check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_grant("Collection", c_name, "Insert", check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.role_revoke("Collection", c_name, "Insert", check_task=CheckTasks.check_permission_deny)

        # User permission deny
        self.utility_wrap.reset_password(user=user_test, old_password=password_test, new_password=password,
                                         check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.update_password(user=user_test, old_password=password, new_password=password_test,
                                          check_task=CheckTasks.check_permission_deny)
        self.utility_wrap.list_user(user_test, False, check_task=CheckTasks.check_permission_deny)

        # public role access
        collection_w.index()
        self.utility_wrap.list_collections()
        self.utility_wrap.has_collection(c_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("name", ["admin", "public"])
    def test_remove_user_from_default_role(self, name, host, port):
        """
        target: remove user from admin role or public role
        method: create a user,add user to admin role or public role,remove user from role
        expected: remove success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(name)
        self.utility_wrap.role_add_user(user)
        users, _ = self.utility_wrap.role_get_users()
        user_info, _ = self.utility_wrap.list_user(user, True)
        user_item = user_info.groups[0]
        assert name in user_item.roles
        assert user in users

        self.utility_wrap.role_remove_user(user)
        users, _ = self.utility_wrap.role_get_users()
        assert user not in users

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_remove_root_from_new_role(self, host, port):
        """
        target: remove root from new role
        method: create a new role, bind root to role,remove root from role
        expected: remove success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]
        self.utility_wrap.role_add_user("root")
        users, _ = self.utility_wrap.role_get_users()
        user_info, _ = self.utility_wrap.list_user("root", True)
        user_item = user_info.groups[0]
        assert r_name in user_item.roles
        assert "root" in users

        self.utility_wrap.role_remove_user("root")
        users, _ = self.utility_wrap.role_get_users()
        assert "root" not in users
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_db_collections(self, host, port):
        """
        target: test grant collection privilege with db
        method: 1.root user create db and collection
                2.create a role and grant collection privilege with db
                3.bind role to a user
                4.user connect and verify privilege with different db
        expected: privilege valid only with the granted db
        """
        tmp_nb = 100
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.insert(cf.gen_default_dataframe_data(tmp_nb))

        # grant role collection flush privilege
        user, pwd, role = self.init_user_with_privilege("Collection", collection_w.name, "Flush", db_name)
        self.utility_wrap.role_grant("Collection", collection_w.name, "GetStatistics", db_name)

        # re-connect with new user and default db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd,
                                     db_name=ct.default_db, secure=cf.param_info.param_secure,
                                     check_task=ct.CheckTasks.ccr)

        # verify user flush collection with different db
        collection_w.flush(check_task=CheckTasks.check_permission_deny)

        # set using db to db_name and flush
        self.database_wrap.using_database(db_name)
        assert collection_w.num_entities == tmp_nb

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_db_global(self, host, port):
        """
        target: test grant global privilege with db
        method: 1.root user create db
                2.create a role and grant all global privilege with db
                3.bind role to a user
                4.user connect and verify privilege with different db
        expected: privilege valid only with the granted db
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # grant role collection flush privilege
        user, pwd, role = self.init_user_with_privilege("Global", "*", "*", db_name)

        # re-connect with new user and default db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd,
                                     db_name=ct.default_db, secure=cf.param_info.param_secure,
                                     check_task=ct.CheckTasks.ccr)

        # verify user list grants with different db
        self.utility_wrap.role_list_grants(check_task=CheckTasks.check_permission_deny)

        # set using db to db_name and verify grants
        self.database_wrap.using_database(db_name)
        self.utility_wrap.role_list_grants()
        self.utility_wrap.describe_resource_group(ct.default_resource_group_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_db_users(self, host, port):
        """
        target: test grant user privilege with db
        method: 1.root user create db
                2.create a role and grant user privilege with db
                3.bind role to a user
                4.user connect and verify privilege with different db
        expected: privilege valid only with the granted db
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # grant role collection flush privilege
        user, pwd, role = self.init_user_with_privilege("User", "*", "SelectUser", db_name)

        # re-connect with new user and default db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd, db_name=ct.default_db,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # verify user list grants with different db
        self.utility_wrap.list_users(False, check_task=CheckTasks.check_permission_deny)

        # set using db to db_name and verify grants
        self.database_wrap.using_database(db_name)
        self.utility_wrap.list_users(False)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_revoke_db_collection(self, host, port):
        """
        target: test revoke db collection
        method: test revoke privilege with normal db or not existed db
        expected: verify privilege
        """
        tmp_nb = 100
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.insert(cf.gen_default_dataframe_data(tmp_nb))

        # grant role collection flush privilege
        user, pwd, role = self.init_user_with_privilege("Collection", collection_w.name, "Flush", db_name)

        # revoke privilege with default db
        self.utility_wrap.role_revoke("Collection", collection_w.name, "Flush", ct.default_db)
        self.utility_wrap.role_revoke("Collection", collection_w.name, "Flush", db_name)

        # re-connect with new user and db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd,
                                     db_name=ct.default_db, secure=cf.param_info.param_secure,
                                     check_task=ct.CheckTasks.ccr)

        # verify user flush collection with db
        collection_w.flush(check_task=CheckTasks.check_permission_deny)

        # set using db to default db and verify privilege
        self.database_wrap.using_database(db_name)
        collection_w.flush(check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_grant_db(self, host, port):
        """
        target: test list grant with db
        method: 1.list grant with granted db and default db
        expected: verify role grants with different db
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str("db")
        self.database_wrap.create_database(db_name)

        # grant role collection * All privilege
        _, _, role_name = self.init_user_with_privilege("Global", "*", "All", db_name)
        log.debug(f"role name: {role_name}")

        # list grant with db and verify
        grants, _ = self.utility_wrap.role_list_grants(db_name=db_name)
        for g in grants.groups:
            assert g.object == "Global"
            assert g.object_name == "*"
            assert g.privilege == "All"
            assert g.db_name == db_name

        log.info(self.utility_wrap.role.name)
        grant_db, _ = self.utility_wrap.role_list_grant(object="Global", object_name="*", db_name=db_name)
        for g in grant_db.groups:
            assert g.object == "Global"
            assert g.object_name == "*"
            assert g.privilege == "All"
            assert g.db_name == db_name

        grant_default, _ = self.utility_wrap.role_list_grant("Global", "*", db_name=ct.default_db)
        assert len(grant_default.groups) == 0

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_grants_db(self, host, port):
        """
        target: test list grants with db
        method: 1.list grants with granted db and default db
        expected: verify role grants with different db
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # grant role collection flush privilege
        self.init_user_with_privilege("Global", "*", "All", db_name)
        self.utility_wrap.role_grant("User", "*", "UpdateUser", db_name)

        # list grants with db and verify
        grants, _ = self.utility_wrap.role_list_grants(db_name=db_name)
        assert len(grants.groups) == 2

        for g in grants.groups:
            if g.object == "Global":
                assert g.object_name == "*"
                assert g.privilege == "All"
            elif g.object == "User":
                assert g.object_name == "*"
                assert g.privilege == "UpdateUser"
            else:
                raise Exception(f"{db_name} should't have the privilege {g.object_name}, Please check ")

        # verify the role no grant in default db
        grants_default, _ = self.utility_wrap.role_list_grants(db_name=ct.default_db)
        assert len(grants_default.groups) == 0

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_connect(self, host, port):
        """
        target: test grant user with db and connect with db
        method: 1. grand role privilege in the default db
                2. user connect with default db
                3. verify db privilege
        expected: succ
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure)

        # grant global privilege to default db
        tmp_user, tmp_pwd, tmp_role = self.init_user_with_privilege("User", "*", "SelectUser", ct.default_db)

        # re-connect
        self.connection_wrap.disconnect(ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=tmp_user, password=tmp_pwd,
                                     secure=cf.param_info.param_secure)

        # list collections
        self.utility_wrap.list_users(False)
        self.utility_wrap.list_collections()
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_permission_deny)


class TestUtilityNegativeRbac(TestcaseBase):

    def teardown_method(self, method):
        """
        teardown method: drop role and user
        """
        log.info("[utility_teardown_method] Start teardown utility test cases ...")

        self.connection_wrap.connect(host=cf.param_info.param_host, port=cf.param_info.param_port, user=ct.default_user,
                                     password=ct.default_password, secure=cf.param_info.param_secure)

        # drop users
        users, _ = self.utility_wrap.list_users(False)
        for u in users.groups:
            if u.username != ct.default_user:
                self.utility_wrap.delete_user(u.username)
        user_groups, _ = self.utility_wrap.list_users(False)
        assert len(user_groups.groups) == 1

        role_groups, _ = self.utility_wrap.list_roles(False)

        # drop roles
        for role_group in role_groups.groups:
            if role_group.role_name not in ['admin', 'public']:
                self.utility_wrap.init_role(role_group.role_name)
                g_list, _ = self.utility_wrap.role_list_grants()
                for g in g_list.groups:
                    self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
                self.utility_wrap.role_drop()
        role_groups, _ = self.utility_wrap.list_roles(False)
        assert len(role_groups.groups) == 2

        super().teardown_method(method)

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_non_string(self, request):
        """
        get invalid string without None
        """
        if isinstance(request.param, str):
            pytest.skip("skip string")
        yield request.param

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("name", ["longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglong",
                                      "n%$#@!", "123n", " ", "''", "test-role", "ff ff", "中文"])
    def test_create_role_with_invalid_name(self, name, host, port):
        """
        target: create role with invalid name
        method: create role with invalid name
        expected: create fail
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        self.utility_wrap.init_role(name)

        error = {"err_code": 5}
        self.utility_wrap.create_role(check_task=CheckTasks.err_res, check_items=error)
        # get roles
        role_groups, _ = self.utility_wrap.list_roles(False)

        # drop roles
        for role_group in role_groups.groups:
            if role_group.role_name not in ['admin', 'public']:
                self.utility_wrap.init_role(role_group.role_name)
                g_list, _ = self.utility_wrap.role_list_grants()
                for g in g_list.groups:
                    self.utility_wrap.role_revoke(g.object, g.object_name, g.privilege)
                self.utility_wrap.role_drop()
        role_groups, _ = self.utility_wrap.list_roles(False)
        assert len(role_groups.groups) == 2

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_create_exist_role(self, host, port):
        """
        target: check create an exist role fail
        method: double create a role with same name
        expected: fail to create
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]
        error = {"err_code": 35,
                 "err_msg": "fail to create role"}
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role(check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.role_drop()
        assert not self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("name", ["admin", "public"])
    def test_drop_admin_and_public_role(self, name, host, port):
        """
        target: drop admin and public role fail
        method: drop admin and public role fail
        expected: fail to drop
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(name)
        assert self.utility_wrap.role_is_exist()[0]
        error = {"err_code": 5,
                 "err_msg": "the role[%s] is a default role, which can\'t be dropped" % name}
        self.utility_wrap.role_drop(check_task=CheckTasks.err_res, check_items=error)
        assert self.utility_wrap.role_is_exist()[0]

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_drop_role_which_not_exist(self, host, port):
        """
        target: drop role which not exist fail
        method: drop role which not exist
        expected: fail to drop
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        assert not self.utility_wrap.role_is_exist()[0]
        error = {"err_code": 36,
                 "err_msg": "the role isn\'t existed"}
        self.utility_wrap.role_drop(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_add_user_not_exist_role(self, host, port):
        """
        target: add user to not exist role
        method: create a user,add user to not exist role
        expected: fail to add
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)

        self.utility_wrap.init_role(r_name)
        assert not self.utility_wrap.role_is_exist()[0]

        error = {"err_code": 37,
                 "err_msg": "fail to check the role name"}
        self.utility_wrap.role_add_user(user, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_add_not_exist_user_to_role(self, host, port):
        """
        target: add not exist user to role
        method: create a role,add not exist user to role
        expected: fail to add
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        user = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]

        error = {"err_code": 37,
                 "err_msg": "fail to check the username"}
        self.utility_wrap.role_remove_user(user, check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.role_add_user(user, check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.parametrize("name", ["admin", "public"])
    def test_remove_root_from_default_role(self, name, host, port):
        """
        target: remove root from admin role or public role
        method: remove root from admin role or public role
        expected: remove success
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        self.utility_wrap.init_role(name)
        error = {"err_code": 37,
                 "err_msg": "fail to operate user to role"}
        self.utility_wrap.role_remove_user("root", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_remove_user_from_unbind_role(self, host, port):
        """
        target: remove user from unbind role
        method: create new role and new user, remove user from unbind role
        expected: fail to  remove
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]
        self.utility_wrap.role_grant("Global", "*", "All")
        self.utility_wrap.role_add_user(user)
        self.utility_wrap.role_list_grants()

        error = {"err_code": 37,
                 "err_msg": "fail to operate user to role"}
        self.utility_wrap.role_remove_user(user, check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_remove_user_from_empty_role(self, host, port):
        """
        target: remove not exist user from role
        method: create new role, remove not exist user from unbind role
        expected: fail to remove
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        user = cf.gen_unique_str(prefix)
        password = cf.gen_unique_str(prefix)
        u, _ = self.utility_wrap.create_user(user=user, password=password)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        assert not self.utility_wrap.role_is_exist()[0]

        error = {"err_code": 37,
                 "err_msg": "fail to check the role name"}
        self.utility_wrap.role_remove_user(user, check_task=CheckTasks.err_res, check_items=error)
        users, _ = self.utility_wrap.role_get_users()
        assert user not in users

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_remove_not_exist_user_from_role(self, host, port):
        """
        target: remove not exist user from role
        method: create new role, remove not exist user from unbind role
        expected: fail to remove
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)

        user = cf.gen_unique_str(prefix)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        assert self.utility_wrap.role_is_exist()[0]

        error = {"err_code": 37,
                 "err_msg": "fail to check the username"}
        self.utility_wrap.role_remove_user(user, check_task=CheckTasks.err_res, check_items=error)
        users, _ = self.utility_wrap.role_get_users()
        assert user not in users
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_drop_role_with_bind_privilege(self, host, port):
        """
        target: drop role with bind privilege
        method: create a new role,grant role privilege,drop it
        expected: fail to drop
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)

        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        self.utility_wrap.role_grant("Collection", "*", "*")

        error = {"err_code": 36,
                 "err_msg": "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"}
        self.utility_wrap.role_drop(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_grant_by_not_exist_role(self, host, port):
        """
        target: list grants by not exist role
        method: list grants by not exist role
        expected: fail to list
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        error = {"err_code": 42,
                 "err_msg": "there is no value on key = by-dev/meta/root-coord/credential/roles/%s" % r_name}
        self.utility_wrap.role_list_grants(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_grant_by_role_and_not_exist_object(self, host, port):
        """
        target: list grants by role and not exist object
        method: list grants by role and not exist object
        expected: fail to list
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        o_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        error = {"err_code": 42,
                 "err_msg": f"not found the object type[name: {o_name}], supported the object types: [Global User "
                            f"Collection]"}
        self.utility_wrap.role_list_grant(o_name, "*", check_task=CheckTasks.err_res, check_items=error)
        self.utility_wrap.role_drop()

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_privilege_with_object_not_exist(self, host, port):
        """
        target: grant privilege with not exist object
        method: grant privilege with not exist object
        expected: fail to grant
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        o_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        error = {"err_code": 41,
                 "err_msg": "the object type in the object entity[name: %s] is invalid" % o_name}
        self.utility_wrap.role_grant(o_name, "*", "*", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_privilege_with_privilege_not_exist(self, host, port):
        """
        target: grant privilege with not exist privilege
        method: grant privilege with not exist privilege
        expected: fail to grant
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        error = {"err_code": 41, "err_msg": "the privilege name[%s] in the privilege entity is invalid" % p_name}
        self.utility_wrap.role_grant("Global", "*", p_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_revoke_privilege_with_object_not_exist(self, host, port):
        """
        target: revoke privilege with not exist object
        method: revoke privilege with not exist object
        expected: fail to revoke
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        o_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        error = {"err_code": 41,
                 "err_msg": "the object type in the object entity[name: %s] is invalid" % o_name}
        self.utility_wrap.role_revoke(o_name, "*", "*", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_revoke_privilege_with_privilege_not_exist(self, host, port):
        """
        target: revoke privilege with not exist privilege
        method: revoke privilege with not exist privilege
        expected: fail to revoke
        """
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user,
                                     password=ct.default_password, check_task=ct.CheckTasks.ccr)
        r_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str(prefix)
        self.utility_wrap.init_role(r_name)
        self.utility_wrap.create_role()
        error = {"err_code": 41, "err_msg": "the privilege name[%s] in the privilege entity is invalid" % p_name}
        self.utility_wrap.role_revoke("Global", "*", p_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_privilege_with_db_not_exist(self, host, port):
        """
        target: test grant with an not existed db
        method: 1.root user create role -> grant privilege -> bind user
                2.new user connent with db
                3.list collection
        expected: database not exist
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)
        db_name = cf.gen_unique_str("db")
        user, pwd, _ = self.init_user_with_privilege("Global", "*", "All", db_name)

        # using the granted and not existing db
        self.database_wrap.using_database(db_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 2, ct.err_msg: "database not found"})

        # re-connect with new user and db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd, db_name=db_name,
                                     secure=cf.param_info.param_secure, check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 2, ct.err_msg: "database not found"})

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_privilege_with_collection_not_exist(self, host, port):
        """
        target: test grant collection object privilege with not existed collection or other's db collection
        method: 1.root connect and create db_a and create collection in db_a
                2.create db_b and create a user and role, grant not existed collection privilege to role
                3.grant db_a's collection with db_b privilege to role
        expected: 1.grant with invalid db or collection no error
                  2.use db and operator collection gets permission deny
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create collection in db_a_xxx
        db_a = cf.gen_unique_str("db_a")
        self.database_wrap.create_database(db_a)

        # create collection in db_a
        self.database_wrap.using_database(db_a)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # create db
        db_b = cf.gen_unique_str("db_b")
        self.database_wrap.create_database(db_b)

        # grant role privilege: collection not existed -> no error
        user, pwd, role = self.init_user_with_privilege("Collection", "rbac", "Flush", db_name=db_b)

        # grant role privilege: db_a collection provilege with database db_b
        self.utility_wrap.role_grant("Collection", collection_w.name, "Flush", db_name=db_b)

        # list grants and verify collection privilege has a not existed collection
        grants, _ = self.utility_wrap.role_list_grants(db_name=db_b)
        for g in grants.groups:
            assert g.object == "Collection"
            assert g.object_name in [collection_w.name, "rbac"]
            assert g.privilege == "Flush"

        # re-connect with new user and db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # collection flush with db_b permission
        self.database_wrap.using_database(db_b)
        collection_w.flush(check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})
        self.database_wrap.using_database(db_a)
        collection_w.flush(check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_revoke_db_not_existed(self, host, port):
        """
        target: test revoke with not existed db
        method: 1. init a user and role by a root user
                2. revoke role privilege with a not existed db
        expected: revoke failed
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # grant role privilege: collection not existed -> no error
        db_name = cf.gen_unique_str("db")
        user, pwd, role = self.init_user_with_privilege("Global", "*", "All", ct.default_db)
        self.utility_wrap.role_revoke("Global", "*", "All", db_name)

        self.database_wrap.using_database(db_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: "database not exist"})
        self.database_wrap.create_database(db_name)
        self.utility_wrap.role_grant("Global", "*", "All", db_name)
        self.database_wrap.using_database(db_name)
        self.utility_wrap.list_users(True)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_list_grant_db_non_existed(self, host, port):
        """
        target: test list grant with not existed db
        method: list grant with an random db name
        expected: empty grant
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # grant role privilege: collection not existed -> no error
        self.init_user_with_privilege("Global", "*", "All", ct.default_db)

        # list grant with a not-existed db
        grant, _ = self.utility_wrap.role_list_grant("Global", "*", "All", cf.gen_unique_str())
        assert len(grant.groups) == 0

        # list grants with a not-existed db
        grants, _ = self.utility_wrap.role_list_grants(cf.gen_unique_str())
        assert len(grants.groups) == 0

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_admin_public_role_privilege_all_dbs(self, host, port):
        """
        target: test admin role have privileges in all dbs
        method: 1. create a user and bind admin role
                2. create different dbs and create collection in db
                3. user connect and list collections using different db
        expected: verify list succ
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create a user and bind user to admin role
        username = cf.gen_unique_str("user")
        pwd = cf.gen_unique_str()
        self.utility_wrap.create_user(username, pwd)
        self.utility_wrap.init_role("admin")
        self.utility_wrap.role_add_user(username)

        # create db_a and create collection in db_a
        db_a = cf.gen_unique_str("a")
        self.database_wrap.create_database(db_a)
        self.database_wrap.using_database(db_a)

        # collection in db_a
        coll_a = cf.gen_unique_str("a")
        collection_w_a = self.init_collection_wrap(name=coll_a)
        collection_w_a.insert(cf.gen_default_dataframe_data(nb=100))

        # create db_b and create collection in db_b
        db_b = cf.gen_unique_str("b")
        self.database_wrap.create_database(db_b)
        self.database_wrap.using_database(db_b)

        # collection in db_b
        coll_b = cf.gen_unique_str("b")
        collection_w_b = self.init_collection_wrap(name=coll_b)
        collection_w_b.insert(cf.gen_default_dataframe_data(nb=200))

        # re-connect with new user and db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=username, password=pwd,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # using db_a
        self.database_wrap.using_database(db_a)
        assert collection_w_a.num_entities == 100
        collections_a, _ = self.utility_wrap.list_collections()
        assert coll_a in collections_a
        self.utility_wrap.list_users(True)

        # using db_b
        self.database_wrap.using_database(db_b)
        assert collection_w_b.num_entities == 200
        collections_b, _ = self.utility_wrap.list_collections()
        assert coll_b in collections_b
        self.utility_wrap.list_users(True)

        # create a user and bind user to public role
        p_username = cf.gen_unique_str("public")
        p_pwd = cf.gen_unique_str()
        self.utility_wrap.create_user(p_username, p_pwd)
        self.utility_wrap.init_role("public")
        self.utility_wrap.role_add_user(p_username)

        # re-connect with new user and db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=p_username, password=p_pwd,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # using default db
        self.database_wrap.using_database(ct.default_db)
        self.utility_wrap.list_collections()

        # using db a
        self.database_wrap.using_database(db_a)
        collections_a, _ = self.utility_wrap.list_collections()
        assert coll_a in collections_a

        # using db b
        self.database_wrap.using_database(db_b)
        collections_b, _ = self.utility_wrap.list_collections()
        assert coll_b in collections_b

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_grant_not_existed_collection_privilege(self, host, port):
        """
        target: test grant privilege with inconsistent collection and db
        method: 1. create a collection in the default db
                2. create a db
                3. create a user and grant all privileges with the collection and db (collection not in this db)
        expected: collection not exist
        """
        # root connect
        self.connection_wrap.connect(host=host, port=port, user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure, check_task=ct.CheckTasks.ccr)

        # create a collection in the default db
        coll_name = cf.gen_unique_str("coll")
        collection_w = self.init_collection_wrap(name=coll_name)

        # create a db
        db_name = cf.gen_unique_str("db")
        self.database_wrap.create_database(db_name)

        # grant role privilege: collection not existed in the db -> no error
        user, pwd, role = self.init_user_with_privilege("Collection", coll_name, "Flush", db_name)

        # re-connect with new user and granted db
        self.connection_wrap.disconnect(alias=ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, user=user, password=pwd, secure=cf.param_info.param_secure,
                                     db_name=db_name, check_task=ct.CheckTasks.ccr)

        # operate collection in the granted db
        collection_w.flush(check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "CollectionNotExists"})

        # operate collection in the default db
        self.database_wrap.using_database(ct.default_db)
        collection_w.flush(check_task=CheckTasks.check_permission_deny)


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
            collection_w, _, _, insert_ids, _ = self.init_collection_general(prefix, insert_data=True, is_flush=False,
                                                                             is_index=True)
            collection_w.delete(f'{ct.default_int64_field_name} in {insert_ids[:delete_num]}')
            collection_names.append(collection_w.name)

        self.utility_wrap.flush_all(timeout=300)
        cw = ApiCollectionWrapper()
        for c in collection_names:
            cw.init_collection(name=c)
            assert cw.num_entities_without_flush == ct.default_nb

            cw.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
            cw.load()
            cw.query(f'{ct.default_int64_field_name} in {insert_ids[:100]}', check_task=CheckTasks.check_query_empty)

            res, _ = cw.query(f'{ct.default_int64_field_name} not in {insert_ids[:delete_num]}')
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
                collection_w.delete(f'{ct.default_int64_field_name} in {insert_res.primary_keys[:delete_num]}')
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
                cw.query(f'{ct.default_int64_field_name} in {insert_res.primary_keys[:delete_num]}',
                         check_task=CheckTasks.check_query_empty)

                res, _ = cw.query(f'{ct.default_int64_field_name} not in {insert_res.primary_keys[:delete_num]}')
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
            collection_w, _, _, insert_ids, _ = self.init_collection_general(prefix, insert_data=True, is_flush=False,
                                                                             is_index=True)
            collection_w.delete(f'{ct.default_int64_field_name} in {insert_ids[:delete_num]}')
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
        from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
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
            cw.query(f'{ct.default_int64_field_name} in {delete_ids}', check_task=CheckTasks.check_query_empty)

            res, _ = cw.query(f'{ct.default_int64_field_name} not in {delete_ids}')
            assert len(res) == ct.default_nb * 2 - delete_num
