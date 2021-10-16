import threading

import pytest
from base.client_base import TestcaseBase
from base.utility_wrapper import ApiUtilityWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "utility"
default_schema = cf.gen_default_collection_schema()
default_int64_field_name = ct.default_int64_field_name
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_dim = ct.default_dim
default_nb = ct.default_nb
num_loaded_entities = "num_loaded_entities"
num_total_entities = "num_total_entities"


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

    """
    ******************************************************************
    #  The followings are invalid cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
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
        # elif not isinstance(c_name, str):
        #     self.utility_wrap.has_collection(c_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 1, ct.err_msg: "illegal"})

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_collection_name_invalid(self, get_invalid_collection_name):
        self._connect()
        error = f'`collection_name` value {get_invalid_collection_name} is illegal'
        self.utility_wrap.drop_collection(get_invalid_collection_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: error})

    # TODO: enable
    @pytest.mark.tags(CaseLabel.L1)
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
        # self._connect()
        # c_name = get_invalid_collection_name
        # ut = ApiUtilityWrapper()
        # if isinstance(c_name, str) and c_name:
        #     ex, _ = ut.index_building_progress(c_name, check_items={ct.err_code: 1, ct.err_msg: "Invalid collection name"})

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

    @pytest.mark.tags(CaseLabel.L1)
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
        #     ex, _ = ut.wait_for_index_building_complete(c_name, check_items={ct.err_code: 1, ct.err_msg: "Invalid collection name"})

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

    @pytest.mark.tags(CaseLabel.L1)
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
        self.collection_wrap.load()
        error = {ct.err_code: 1, ct.err_msg: "Invalid collection name: {}".format(invalid_c_name)}
        self.utility_wrap.loading_progress(invalid_c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
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
        self.collection_wrap.load()
        error = {ct.err_code: 1, ct.err_msg: "describe collection failed: can't find collection"}
        self.utility_wrap.loading_progress("not_existed_name", check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tag(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue #677")
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

    @pytest.mark.tag(CaseLabel.L1)
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
        err_msg = {ct.err_code: 1, ct.err_msg: f"partitionID of partitionName:{ct.default_tag} can not be found"}
        self.utility_wrap.loading_progress(collection_w.name, partition_names,
                                           check_task=CheckTasks.err_res, check_items=err_msg)

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    def test_drop_collection_not_existed(self):
        """
        target: test drop an not existed collection
        method: drop a not created collection
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "describe collection failed: can't find collection:"}
        self.utility_wrap.drop_collection(c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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
    def test_calc_distance_collection_before_load(self, get_support_metric_field):
        """
        target: test calculated distance when entities is not ready
        method: calculate distance before load
        expected: raise exception
        """
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb,
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
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
        exp_res = {'total_rows': 0, 'indexed_rows': 0}
        assert res == exp_res

    @pytest.mark.tags(CaseLabel.L1)
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
        error = {ct.err_code: 1, ct.err_msg: "no index is created"}
        self.utility_wrap.index_building_progress(c_name, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_index(self):
        """
        target: test building_process
        method: 1.insert 1024 (because minSegmentSizeToEnableIndex=1024)
                2.build(server does create index) and call building_process
        expected: indexed_rows=0
        """
        nb = 1024
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        cw.create_index(default_field_name, default_index_params)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert res['indexed_rows'] == 0
        assert res['total_rows'] == nb

    @pytest.mark.tags(CaseLabel.L1)
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
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert (0 < res['indexed_rows'] <= nb)
        assert res['total_rows'] == nb
        # for _ in range(2):
        #     assert "indexed_rows" in res
        #     assert res["indexed_rows"] <= nb
        #     assert res["indexed_rows"] >= 0
        #     assert "total_rows" in res
        #     assert res["total_rows"] == nb

    @pytest.mark.tags(CaseLabel.L1)
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
        exp_res = {'total_rows': 0, 'indexed_rows': 0}
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
        cw.create_index(default_field_name, default_index_params)
        res, _ = self.utility_wrap.wait_for_index_building_complete(c_name)
        assert res is True
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert res["indexed_rows"] == nb

    @pytest.mark.tag(CaseLabel.L1)
    def test_loading_progress_without_loading(self):
        """
        target: test loading progress without loading
        method: insert and flush data, call loading_progress without loading
        expected: loaded entities is 0
        """
        collection_w = self.init_collection_wrap()
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        exp_res = {num_loaded_entities: 0, num_total_entities: ct.default_nb}
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res == exp_res

    @pytest.mark.tag(CaseLabel.L1)
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
        assert res[num_total_entities] == nb
        assert res[num_loaded_entities] == nb

    @pytest.mark.tag(CaseLabel.L1)
    @pytest.mark.xfail(reason="pymilvus issue #702")
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
        collection_w.load(_async=True)
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert (0 < res[num_loaded_entities] <= ct.default_nb)

    @pytest.mark.tag(CaseLabel.L1)
    def test_loading_progress_empty_collection(self):
        """
        target: test loading_progress on a empty collection
        method: 1.create collection and no insert 2.loading_progress
        expected: 0 entities is loaded
        """
        collection_w = self.init_collection_wrap()
        collection_w.load()
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        exp_res = {num_loaded_entities: 0, num_total_entities: 0}
        assert exp_res == res

    @pytest.mark.tag(CaseLabel.L1)
    def test_loading_progress_after_release(self):
        """
        target: test loading progress without loading
        method: insert and flush data, call loading_progress without loading
        expected: loaded entities is 0
        """
        collection_w = self.init_collection_general(prefix, insert_data=True)[0]
        collection_w.release()
        exp_res = {num_loaded_entities: 0, num_total_entities: ct.default_nb}
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res == exp_res

    @pytest.mark.tag(CaseLabel.L2)
    def test_loading_progress_with_release_partition(self):
        """
        target: test loading progress after release part partitions
        method: 1.insert data into two partitions and flush
                2.load collection and release onr partition
        expected: loaded one partition entities
        """
        half = ct.default_nb
        # insert entities into two partitions, collection flush and load
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)
        partition_w.release()
        res = self.utility_wrap.loading_progress(collection_w.name)[0]
        assert res[num_total_entities] == half * 2
        assert res[num_loaded_entities] == half

    @pytest.mark.tag(CaseLabel.L2)
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
        assert res[num_total_entities] == half * 2
        assert res[num_loaded_entities] == half

    @pytest.mark.tag(CaseLabel.L1)
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
        assert res[num_total_entities] == half
        assert res[num_loaded_entities] == half

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_loading_collection_empty(self):
        """
        target: test wait_for_loading
        method: input empty collection
        expected: no exception raised
        """
        self._connect()
        cw = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        cw.load()
        self.utility_wrap.wait_for_loading_complete(cw.name)
        res, _ = self.utility_wrap.loading_progress(cw.name)
        exp_res = {num_total_entities: 0, num_loaded_entities: 0}
        assert res == exp_res

    @pytest.mark.xfail(reason="pymilvus issue #702")
    @pytest.mark.tag(CaseLabel.L1)
    def test_wait_for_loading_complete(self):
        """
        target: test wait for loading collection
        method: insert 10000 entities and wait for loading complete
        expected: after loading complete, loaded entities is 10000
        """
        nb = 6000
        collection_w = self.init_collection_wrap()
        df = cf.gen_default_dataframe_data(nb)
        collection_w.insert(df)
        assert collection_w.num_entities == nb
        collection_w.load(_async=True)
        self.utility_wrap.wait_for_loading_complete(collection_w.name)
        res, _ = self.utility_wrap.loading_progress(collection_w.name)
        assert res[num_loaded_entities] == nb

    @pytest.mark.tag(CaseLabel.L0)
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
        error = {ct.err_code: 1, ct.err_msg: {"describe collection failed: can't find collection:"}}
        self.utility_wrap.drop_collection(c_name, check_task=CheckTasks.err_res, check_items=error)

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
    def test_calc_distance_from_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: both left and right vectors are from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
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
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
        collection_w_1, vectors_1, _, insert_ids_1 = self.init_collection_general(prefix_1, True, nb)
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
    def test_calc_distance_left_vector_and_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: set left vectors as random vectors, right vectors from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
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
    def test_calc_distance_right_vector_and_collection_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from collection entities
        method: set right vectors as random vectors, left vectors from collection
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
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
    def test_calc_distance_from_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance from one partition entities
        method: both left and right vectors are from partition
        expected: distance calculated successfully
        """
        log.info("Creating connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
        partitions = collection_w.partitions
        middle = len(insert_ids) // 2
        params = {metric_field: metric, "sqrt": sqrt}
        for i in range(len(partitions)):
            log.info("Extracting entities from partitions for distance calculating")
            vectors_l = vectors[i].loc[:, default_field_name]
            vectors_r = vectors[i].loc[:, default_field_name]
            op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            log.info("Calculating distance between entities from one partition")
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    def test_calc_distance_from_partitions(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between entities from partitions
        method: calculate distance between entities from two partitions
        expected: distance calculated successfully
        """
        log.info("Create connection")
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
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
    def test_calc_distance_left_vectors_and_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between vectors and partition entities
        method: set left vectors as random vectors, right vectors are entities
        expected: distance calculated successfully
        """
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
        middle = len(insert_ids) // 2
        partitions = collection_w.partitions
        vectors_l = cf.gen_vectors(nb // 2, default_dim)
        op_l = {"float_vectors": vectors_l}
        params = {metric_field: metric, "sqrt": sqrt}
        for i in range(len(partitions)):
            vectors_r = vectors[i].loc[:, default_field_name]
            op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})

    @pytest.mark.tags(CaseLabel.L2)
    def test_calc_distance_right_vectors_and_partition_ids(self, metric_field, metric, sqrt):
        """
        target: test calculated distance between vectors and partition entities
        method: set right vectors as random vectors, left vectors are entities
        expected: distance calculated successfully
        """
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
        middle = len(insert_ids) // 2
        partitions = collection_w.partitions
        vectors_r = cf.gen_vectors(nb // 2, default_dim)
        op_r = {"float_vectors": vectors_r}
        params = {metric_field: metric, "sqrt": sqrt}
        for i in range(len(partitions)):
            vectors_l = vectors[i].loc[:, default_field_name]
            op_l = {"ids": insert_ids[middle:], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            self.utility_wrap.calc_distance(op_l, op_r, params,
                                            check_task=CheckTasks.check_distance,
                                            check_items={"vectors_l": vectors_l,
                                                         "vectors_r": vectors_r,
                                                         "metric": metric,
                                                         "sqrt": sqrt})


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

        for i in range(thread_num*num):
            c_name = cf.gen_unique_str(prefix)
            self.init_collection_wrap(c_name)
            c_names.append(c_name)

        def create_and_drop_collection(names):
            for name in names:
                assert self.utility_wrap.has_collection(name)[0]
                self.utility_wrap.drop_collection(name)
                assert not self.utility_wrap.has_collection(name)[0]

        for i in range(thread_num):
            x = threading.Thread(target=create_and_drop_collection, args=(c_names[i*num:(i+1)*num],))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        log.debug(self.utility_wrap.list_collections()[0])

