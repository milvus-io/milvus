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

    # TODO: enable
    @pytest.mark.tags(CaseLabel.L1)
    def _test_list_collections_using_invalid(self):
        """
        target: test list_collections with invalid using
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        using = "empty"
        ut = ApiUtilityWrapper()
        ex, _ = ut.list_collections(using=using,
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

    #@pytest.mark.xfail(reason="issue #5667")
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

    @pytest.mark.xfail(reason="issue #5673")
    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_empty(self):
        """
        target: test building_process
        method: input empty collection
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert "indexed_rows" in res
        assert res["indexed_rows"] == 0
        assert "total_rows" in res
        assert res["total_rows"] == 0

    @pytest.mark.xfail(reason="issue #5674")
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
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert "indexed_rows" in res
        assert res["indexed_rows"] == 0
        assert "total_rows" in res
        assert res["total_rows"] == nb

    @pytest.mark.xfail(reason="issue #5674")
    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_index(self):
        """
        target: test building_process
        method: insert 1200 entities, build and call building_process
        expected: 1200 entity indexed
        """
        nb = 1200
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        assert "indexed_rows" in res
        assert res["indexed_rows"] == nb
        assert "total_rows" in res
        assert res["total_rows"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_indexing(self):
        """
        target: test building_process
        method: call building_process during building
        expected: 1200 entity indexed
        """
        nb = 1200
        c_name = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb)
        cw.insert(data=data)
        cw.create_index(default_field_name, default_index_params)
        res, _ = self.utility_wrap.index_building_progress(c_name)
        for _ in range(2):
            assert "indexed_rows" in res
            assert res["indexed_rows"] <= nb
            assert res["indexed_rows"] >= 0
            assert "total_rows" in res
            assert res["total_rows"] == nb

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
        self.init_collection_wrap(name=c_name)
        res, _ = self.utility_wrap.wait_for_index_building_complete(
                        c_name,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 1, ct.err_msg: "no index is created"})

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

    @pytest.mark.tags(CaseLabel.L1)
    def test_calc_distance_default(self):
        """
        target: test calculated distance with default params
        method: calculated distance between two random vectors
        expected: distance calculated successfully
        """
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
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
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
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
        self._connect()
        vectors_l = cf.gen_vectors(default_nb, default_dim)
        vectors_r = cf.gen_vectors(default_nb, default_dim)
        op_l = {"float_vectors": vectors_l}
        op_r = {"float_vectors": vectors_r}
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
        self._connect()
        nb = 10
        raw_vectors_l, vectors_l = cf.gen_binary_vectors(nb, default_dim)
        raw_vectors_r, vectors_r = cf.gen_binary_vectors(nb, default_dim)
        op_l = {"bin_vectors": vectors_l}
        op_r = {"bin_vectors": vectors_r}
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
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
        middle = len(insert_ids) // 2
        vectors = vectors[0].loc[:, default_field_name]
        vectors_l = vectors[:middle]
        vectors_r = []
        for i in range(middle):
            vectors_r.append(vectors[middle+i])
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "field": default_field_name}
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
        self._connect()
        nb = 10
        prefix_1 = "utility_distance"
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
        collection_w_1, vectors_1, _, insert_ids_1 = self.init_collection_general(prefix_1, True, nb)
        vectors_l = vectors[0].loc[:, default_field_name]
        vectors_r = vectors_1[0].loc[:, default_field_name]
        op_l = {"ids": insert_ids, "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"ids": insert_ids_1, "collection": collection_w_1.name,
                "field": default_field_name}
        params = {metric_field: metric, "sqrt": sqrt}
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
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "field": default_field_name}
        params = {metric_field: metric, "sqrt": sqrt}
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
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)
        middle = len(insert_ids) // 2
        vectors = vectors[0].loc[:, default_field_name]
        vectors_l = vectors[:middle]
        vectors_r = cf.gen_vectors(nb, default_dim)
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "field": default_field_name}
        op_r = {"float_vectors": vectors_r}
        params = {metric_field: metric, "sqrt": sqrt}
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
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
        partitions = collection_w.partitions
        middle = len(insert_ids) // 2
        params = {metric_field: metric, "sqrt": sqrt}
        for i in range(len(partitions)):
            vectors_l = vectors[i].loc[:, default_field_name]
            vectors_r = vectors[i].loc[:, default_field_name]
            op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
            op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                    "partition": partitions[i].name, "field": default_field_name}
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
        self._connect()
        nb = 10
        collection_w, vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1)
        partitions = collection_w.partitions
        middle = len(insert_ids) // 2
        params = {metric_field: metric, "sqrt": sqrt}
        vectors_l = vectors[0].loc[:, default_field_name]
        vectors_r = vectors[1].loc[:, default_field_name]
        op_l = {"ids": insert_ids[:middle], "collection": collection_w.name,
                "partition": partitions[0].name, "field": default_field_name}
        op_r = {"ids": insert_ids[middle:], "collection": collection_w.name,
                "partition": partitions[1].name, "field": default_field_name}
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
