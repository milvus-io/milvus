import time

import numpy as np
import pandas as pd
import pytest

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "insert"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()


class TestInsertParams(TestcaseBase):
    """ Test case of Insert interface """

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_non_data_type(self, request):
        if isinstance(request.param, list) or request.param is None:
            pytest.skip("list and None type is valid data type")
        yield request.param

    @pytest.fixture(scope="module", params=ct.get_invalid_strs)
    def get_invalid_field_name(self, request):
        if isinstance(request.param, (list, dict)):
            pytest.skip()
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_dataframe_data(self):
        """
        target: test insert DataFrame data
        method: 1.create 2.insert dataframe data
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_list_data(self):
        """
        target: test insert list-like data
        method: 1.create 2.insert list data
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(ct.default_nb)
        collection_w.insert(data=data)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_non_data_type(self, get_non_data_type):
        """
        target: test insert with non-dataframe, non-list data
        method: insert with data (non-dataframe and non-list type)
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 1, ct.err_msg: "Datas must be list"}
        collection_w.insert(data=get_non_data_type, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("data", [[], pd.DataFrame()])
    def test_insert_empty_data(self, data):
        """
        target: test insert empty data
        method: insert empty
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 1, ct.err_msg: "Column cnt not match with schema"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_dataframe_only_columns(self):
        """
        target: test insert with dataframe just columns
        method: dataframe just have columns
        expected: num entities is zero
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        columns = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        df = pd.DataFrame(columns=columns)
        error = {ct.err_code: 1, ct.err_msg: "Cannot infer schema from empty dataframe"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_empty_field_name_dataframe(self):
        """
        target: test insert empty field name df
        method: dataframe with empty column
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(10)
        df.rename(columns={ct.default_int64_field_name: ' '}, inplace=True)
        error = {ct.err_code: 1, ct.err_msg: "The types of schema and data do not match"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_invalid_field_name_dataframe(self, get_invalid_field_name):
        """
        target: test insert with invalid dataframe data
        method: insert with invalid field name dataframe
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(10)
        df.rename(columns={ct.default_int64_field_name: get_invalid_field_name}, inplace=True)
        error = {ct.err_code: 1, ct.err_msg: "The types of schema and data do not match"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    def test_insert_dataframe_nan_value(self):
        """
        target: test insert dataframe with nan value
        method: insert dataframe with nan value
        expected: todo
        """
        pass

    def test_insert_dataframe_index(self):
        """
        target: test insert dataframe with index
        method: insert dataframe with index
        expected: todo
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_none(self):
        """
        target: test insert None
        method: data is None
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        res, _ = collection_w.insert(data=None)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert len(res) == 0
        assert collection_w.is_empty
        assert collection_w.num_entities == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_numpy_data(self):
        """
        target: test insert numpy.ndarray data
        method: 1.create by schema 2.insert data
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_numpy_data(nb=10)
        error = {ct.err_code: 1, ct.err_msg: "Data type not support numpy.ndarray"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_dataframe(self):
        """
        target: test insert binary dataframe
        method: 1. create by schema 2. insert dataframe
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_binary_data(self):
        """
        target: test insert list-like binary data
        method: 1. create by schema 2. insert data
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        data, _ = cf.gen_default_binary_list_data(ct.default_nb)
        collection_w.insert(data=data)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_single(self):
        """
        target: test insert single
        method: insert one entity
        expected: verify num
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb=1)
        collection_w.insert(data=data)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_dim_not_match(self):
        """
        target: test insert with not match dim
        method: insert data dim not equal to schema dim
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        dim = 129
        df = cf.gen_default_dataframe_data(ct.default_nb, dim=dim)
        error = {ct.err_code: 1, ct.err_msg: f'Collection field dim is {ct.default_dim}, but entities field dim is {dim}'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_dim_not_match(self):
        """
        target: test insert binary with dim not match
        method: insert binary data dim not equal to schema
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        dim = 120
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb, dim=dim)
        error = {ct.err_code: 1, ct.err_msg: f'Collection field dim is {ct.default_dim}, but entities field dim is {dim}'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_field_name_not_match(self):
        """
        target: test insert field name not match
        method: data field name not match schema
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(10)
        df.rename(columns={ct.default_float_field_name: "int"}, inplace=True)
        error = {ct.err_code: 1, ct.err_msg: 'The types of schema and data do not match'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_field_value_not_match(self):
        """
        target: test insert data value not match
        method: insert data value type not match schema
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        df = cf.gen_default_dataframe_data(nb)
        new_float_value = pd.Series(data=[float(i) for i in range(nb)], dtype="float64")
        df.iloc[:, 1] = new_float_value
        error = {ct.err_code: 1, ct.err_msg: 'The types of schema and data do not match'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_value_less(self):
        """
        target: test insert value less than other
        method: int field value less than vec-field value
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        int_values = [i for i in range(nb - 1)]
        float_values = [np.float32(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb, ct.default_dim)
        data = [int_values, float_values, float_vec_values]
        error = {ct.err_code: 1, ct.err_msg: 'arrays must all be same length'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_vector_value_less(self):
        """
        target: test insert vector value less than other
        method: vec field value less than int field
        expected: todo
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        int_values = [i for i in range(nb)]
        float_values = [np.float32(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb - 1, ct.default_dim)
        data = [int_values, float_values, float_vec_values]
        error = {ct.err_code: 1, ct.err_msg: 'arrays must all be same length'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_fields_more(self):
        """
        target: test insert with fields more
        method: field more than schema fields
        expected: todo
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        new_values = [i for i in range(ct.default_nb)]
        df.insert(3, 'new', new_values)
        error = {ct.err_code: 1, ct.err_msg: 'Column cnt not match with schema'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_fields_less(self):
        """
        target: test insert with fields less
        method: fields less than schema fields
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        df.drop(ct.default_float_vec_field_name, axis=1, inplace=True)
        error = {ct.err_code: 1, ct.err_msg: 'Column cnt not match with schema'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_list_order_inconsistent_schema(self):
        """
        target: test insert data fields order inconsistent with schema
        method: insert list data, data fields order inconsistent with schema
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        int_values = [i for i in range(nb)]
        float_values = [np.float32(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb, ct.default_dim)
        data = [float_values, int_values, float_vec_values]
        error = {ct.err_code: 1, ct.err_msg: 'The types of schema and data do not match'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_dataframe_order_inconsistent_schema(self):
        """
        target: test insert with dataframe fields inconsistent with schema
        method: insert dataframe, and fields order inconsistent with schema
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        int_values = pd.Series(data=[i for i in range(nb)])
        float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
        float_vec_values = cf.gen_vectors(nb, ct.default_dim)
        df = pd.DataFrame({
            ct.default_float_field_name: float_values,
            ct.default_float_vec_field_name: float_vec_values,
            ct.default_int64_field_name: int_values
        })
        error = {ct.err_code: 1, ct.err_msg: 'The types of schema and data do not match'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)


class TestInsertOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert interface operations
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_without_connection(self):
        """
        target: test insert without connection
        method: insert after remove connection
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        data = cf.gen_default_list_data(10)
        error = {ct.err_code: 1, ct.err_msg: 'should create connect first'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    def test_insert_drop_collection(self):
        """
        target: test insert and drop
        method: insert data and drop collection
        expected: verify collection if exist
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_list, _ = self.utility_wrap.list_collections()
        assert collection_w.name in collection_list
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        collection_w.drop()
        collection_list, _ = self.utility_wrap.list_collections()
        assert collection_w.name not in collection_list

    def test_insert_create_index(self):
        """
        target: test insert and create index
        method: 1. insert 2. create index
        expected: verify num entities and index
        """
        pass

    def test_insert_after_create_index(self):
        """
        target: test insert after create index
        method: 1. create index 2. insert data
        expected: verify index and num entities
        """
        pass

    def test_insert_binary_after_index(self):
        """
        target: test insert binary after index
        method: 1.create index 2.insert binary data
        expected: 1.index ok 2.num entities correct
        """
        pass

    def test_insert_search(self):
        """
        target: test insert and search
        method: 1.insert data 2.search
        expected: verify search result
        """
        pass

    def test_insert_binary_search(self):
        """
        target: test insert and search
        method: 1.insert binary data 2.search
        expected: search result correct
        """
        pass

    def _test_insert_ids(self):
        """
        target: test insert with ids
        method: insert with ids field value
        expected: 1.verify num entities 2.verify ids
        """
        schema = cf.gen_default_collection_schema(primary_field=ct.default_int64_field_name)
        collection = self._collection(schema=schema)
        assert not collection.auto_id
        assert collection.primary_field.name == ct.default_int64_field_name
        data = cf.gen_default_list_data(ct.default_nb)
        self.collection_wrap.insert(data=data)
        time.sleep(1)
        assert collection.num_entities == ct.default_nb
        # TODO assert ids

    def test_insert_ids_without_value(self):
        """
        target: test insert ids value not match
        method: insert without ids field value
        expected: raise exception
        """
        pass

    def test_insert_same_ids(self):
        """
        target: test insert ids field
        method: insert with same ids
        expected: num entities equal to nb
        """
        pass

    def test_insert_invalid_type_ids(self):
        """
        target: test insert with non-int64 ids
        method: insert ids field with non-int64 value
        expected: raise exception
        """
        pass

    def test_insert_multi_threading(self):
        """
        target: test concurrent insert
        method: multi threads insert
        expected: verify num entities
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_multi_times(self):
        """
        target: test insert multi times
        method: insert data multi times
        expected: verify num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        step = 120
        for _ in range(ct.default_nb//step):
            df = cf.gen_default_dataframe_data(step)
            collection_w.insert(data=df)
        conn, _ = self.connection_wrap.get_connection()
        conn.flush([c_name])
        assert collection_w.num_entities == ct.default_nb


class TestInsertAsync(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert async
    ******************************************************************
    """

    def test_insert_sync(self):
        """
        target: test async insert
        method: insert with async=True
        expected: verify num entities
        """
        pass

    def test_insert_async_false(self):
        """
        target: test insert with false async
        method: async = false
        expected: verify num entities
        """
        pass

    def test_insert_async_callback(self):
        """
        target: test insert with callback func
        method: insert with callback func
        expected: verify num entities
        """
        pass

    def test_insert_async_long(self):
        """
        target: test insert with async
        method: insert 5w entities with callback func
        expected: verify num entities
        """
        pass

    def test_insert_async_callback_timeout(self):
        """
        target: test insert async with callback
        method: insert 10w entities with timeout=1
        expected: raise exception
        """
        pass

    def test_insert_async_invalid_data(self):
        """
        target: test insert async with invalid data
        method: insert async with invalid data
        expected: raise exception
        """
        pass

    def test_insert_async_invalid_partition(self):
        """
        target: test insert async with invalid partition
        method: insert async with invalid partition
        expected: raise exception
        """
        pass
