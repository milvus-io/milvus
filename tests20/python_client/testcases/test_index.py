import copy
import pdb
import pytest
from pymilvus_orm import FieldSchema

from base.client_base import TestcaseBase
from base.index_wrapper import ApiIndexWrapper
from base.collection_wrapper import ApiCollectionWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "index"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestIndexParams(TestcaseBase):
    """ Test case of index interface """

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_field_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_index_type(self, request):
        yield request.param

    # TODO: construct invalid index params for all index types
    @pytest.fixture(
        scope="function",
        params=[
            {"metric_type": "L3", "index_type": "IVF_FLAT"},
            {"metric_type": "L2", "index_type": "IVF_FLAT", "err_params": {"nlist": 10}},
            {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": -1}},
        ]
    )
    def get_invalid_index_params(self, request):
        yield request.param

    # @pytest.mark.xfail(reason="issue #5646")
    @pytest.mark.tags(CaseLabel.L1)
    def test_index_collection_None(self):
        """
        target: test index with None collection
        method: input none collection object
        expected: raise exception
        """
        self._connect()
        self.index_wrap.init_index(None, default_field_name, default_index_params, check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1, "err_msg": "Collection"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_field_name_invalid(self, get_invalid_field_name):
        """
        target: test index with error field name
        method: input field name
        expected: raise exception
        """
        f_name = get_invalid_field_name
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, f_name, default_index_params, check_task=CheckTasks.err_res,
                                            check_items={"err_code": 1, "err_msg": "but expected one of"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_field_name_not_existed(self):
        """
        target: test index with error field name
        method: input field name not created
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        f_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, f_name, default_index_params, check_task=CheckTasks.err_res,
                                           check_items={"err_code": 1, "err_msg": "CreateIndex failed"})

    # TODO: update eror msg
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_type_invalid(self, get_invalid_index_type):
        """
        target: test index with error index type
        method: input invalid index type
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = copy.deepcopy(default_index_params)
        index_params["index_type"] = get_invalid_index_type
        if not isinstance(index_params["index_type"], str):
            msg = "must be str"
        else:
            msg = "Invalid index_type"
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params, check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_type_not_supported(self):
        """
        target: test index with error index type
        method: input unsupported index type
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = copy.deepcopy(default_index_params)
        index_params["index_type"] = "IVFFFFFFF"
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params, check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": ""})

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_params_invalid(self, get_invalid_index_params):
        """
        target: test index with error index params
        method: input invalid index params
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index_params = get_invalid_index_params
        self.index_wrap.init_index(collection_w.collection, default_field_name, index_params, check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": ""})

    # TODO: not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_name_invalid(self, get_invalid_index_name):
        """
        target: test index with error index name
        method: input invalid index name
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        collection_w = self.init_collection_wrap(name=c_name)
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params, check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": ""})


class TestIndexBase(TestcaseBase):
    """ Test case of index interface """
    # TODO: construct valid index params for all index types
    @pytest.fixture(
        scope="function",
        params=[
            default_index_params,
        ]
    )
    def get_valid_index_params(self, request):
        yield request.param


    @pytest.mark.tags(CaseLabel.L1)
    def test_index_collection_empty(self):
        """
        target: test index with empty collection
        method: Index on empty collection
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        # TODO: assert index
        cf.assert_equal_index(index, collection_w.collection.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_params(self, get_valid_index_params):
        """
        target: test index with all index type/params
        method: input valid params
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        index_params = get_valid_index_params
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, index_params)
        # TODO: assert index
        cf.assert_equal_index(index, collection_w.collection.indexes[0])

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_params_flush(self):
        """
        target: test index with all index type/params
        method: input valid params
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        self._connect().flush([collection_w.name])
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        # TODO: assert index
        cf.assert_equal_index(index, collection_w.collection.indexes[0])
        assert collection_w.num_entities == ct.default_nb

    # TODO: not support
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_name_dup(self):
        """
        target: test index with duplicate index name
        method: create index with existed index name create by `collection.create_index`
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        index_name = ct.default_index_name
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.collection.create_index(default_field_name, default_index_params, index_name=index_name)
        self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params, check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": ""})

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_field_names(self):
        """
        target: test index on one field, with two indexes
        method: create index with two different indexes
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_fields(self):
        """
        target: test index on two fields, with the same name
        method: create the same index name with two different fields
        expected: exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_fields_B(self):
        """
        target: test index on two fields, with the different name
        method: create the different index with two different fields
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_field_names_eq_maximum(self):
        """
        target: test index on one field, with the different names, num of the names equal to the maximum num supported
        method: create the different indexes 
        expected: no exception raised
        """
        pass

    # TODO: server not supported
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_field_names_more_maximum(self):
        """
        target: test index on one field, with the different names, num of the names more than the maximum num supported
        method: create the different indexes 
        expected: exception raised
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_drop(self):
        """
        target: test index.drop
        method: create index by `index`, and then drop it
        expected: no exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        index, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        cf.assert_equal_index(index, collection_w.collection.indexes[0])
        self.index_wrap.drop()
        assert len(collection_w.collection.indexes) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_drop_repeatly(self):
        """
        target: test index.drop
        method: create index by `index`, and then drop it twice
        expected: exception raised
        """
        c_name = cf.gen_unique_str(prefix)       
        collection_w = self.init_collection_wrap(name=c_name)
        _, _ = self.index_wrap.init_index(collection_w.collection, default_field_name, default_index_params)
        self.index_wrap.drop()
        self.index_wrap.drop(check_task=CheckTasks.err_res,
                                        check_items={"err_code": 1, "err_msg": "Index doesn't exist"})


class TestIndexAdvanced(TestcaseBase):
    """ Test case of index interface """

    # @pytest.mark.xfail(reason="issue #5660")
    @pytest.mark.tags(CaseLabel.L2)
    def test_index_drop_multi_collections(self):
        """
        target: test index.drop
        method: create indexes by `index`, and then drop it, assert there is one index left
        expected: exception raised
        """
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        cw = self.init_collection_wrap(name=c_name)
        cw2 = self.init_collection_wrap(name=c_name_2)
        iw_2 = ApiIndexWrapper()
        self.index_wrap.init_index(cw.collection, default_field_name, default_index_params)
        index_2, _ = iw_2.init_index(cw2.collection, default_field_name, default_index_params)
        self.index_wrap.drop()
        assert cf.assert_equal_index(index_2, cw2.collection.indexes[0])
        assert len(cw.collection.indexes) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def _test_index_drop_during_inserting(self):
        """
        target: test index.drop during inserting
        method: create indexes by `index`, and then drop it during inserting entities, make sure async insert 
        expected: no exception raised, insert success
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def _test_index_drop_during_searching(self):
        """
        target: test index.drop during searching
        method: create indexes by `index`, and then drop it during searching, make sure async search 
        expected: no exception raised, search success
        """
        pass

    @pytest.mark.tags(CaseLabel.L3)
    def _test_index_recovery_after_restart(self):
        """
        target: test index still existed after server restart
        method: create indexe by `index`, and then restart server, assert index existed
        expected: index in collection.indexes
        """
        pass

    @pytest.mark.tags(CaseLabel.L3)
    def _test_index_building_after_restart(self):
        """
        target: index can still build if not finished before server restart
        method: create index by `index`, and then restart server, assert server is indexing
        expected: index build finished after server resstart
        """
        pass
