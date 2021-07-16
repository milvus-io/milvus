import pytest
from base.client_base import TestcaseBase
from base.utility_wrapper import ApiUtilityWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "utility"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestUtilityParams(TestcaseBase):
    """ Test case of index interface """


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


class TestUtilityBase(TestcaseBase):
    """ Test case of index interface """

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

    @pytest.mark.xfail(reason="issue #5667")
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
