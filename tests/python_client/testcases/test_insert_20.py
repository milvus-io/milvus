import threading

import numpy as np
import pandas as pd
import pytest
from pymilvus import Index

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
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}


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
        mutation_res, _ = collection_w.insert(data=df)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
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
        mutation_res, _ = collection_w.insert(data=data)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == data[0]
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
        error = {ct.err_code: 0, ct.err_msg: "Data type is not support"}
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
        error = {ct.err_code: 0, ct.err_msg: "The data fields number is not match with schema"}
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
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe"}
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
        error = {ct.err_code: 0, ct.err_msg: "The types of schema and data do not match"}
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
        error = {ct.err_code: 0, ct.err_msg: "The types of schema and data do not match"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    def test_insert_dataframe_index(self):
        """
        target: test insert dataframe with index
        method: insert dataframe with index
        expected: todo
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_none(self):
        """
        target: test insert None
        method: data is None
        expected: return successfully with zero results
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        mutation_res, _ = collection_w.insert(data=None)
        assert mutation_res.insert_count == 0
        assert len(mutation_res.primary_keys) == 0
        assert collection_w.is_empty
        assert collection_w.num_entities == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_numpy_data(self):
        """
        target: test insert numpy.ndarray data
        method: 1.create by schema 2.insert data
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_numpy_data(nb=10)
        error = {ct.err_code: 0, ct.err_msg: "Data type not support numpy.ndarray"}
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
        mutation_res, _ = collection_w.insert(data=df)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
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
        mutation_res, _ = collection_w.insert(data=data)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == data[0]
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
        mutation_res, _ = collection_w.insert(data=data)
        assert mutation_res.insert_count == 1
        assert mutation_res.primary_keys == data[0]
        assert collection_w.num_entities == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not Milvus Exception")
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
        error = {ct.err_code: 1,
                 ct.err_msg: f'Collection field dim is {ct.default_dim}, but entities field dim is {dim}'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="exception not Milvus Exception")
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
        error = {ct.err_code: 1,
                 ct.err_msg: f'Collection field dim is {ct.default_dim}, but entities field dim is {dim}'}
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
        error = {ct.err_code: 0, ct.err_msg: 'The types of schema and data do not match'}
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
        error = {ct.err_code: 0, ct.err_msg: 'The types of schema and data do not match'}
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
        error = {ct.err_code: 0, ct.err_msg: 'Arrays must all be same length.'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_vector_value_less(self):
        """
        target: test insert vector value less than other
        method: vec field value less than int field
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        int_values = [i for i in range(nb)]
        float_values = [np.float32(i) for i in range(nb)]
        float_vec_values = cf.gen_vectors(nb - 1, ct.default_dim)
        data = [int_values, float_values, float_vec_values]
        error = {ct.err_code: 0, ct.err_msg: 'Arrays must all be same length.'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_fields_more(self):
        """
        target: test insert with fields more
        method: field more than schema fields
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        new_values = [i for i in range(ct.default_nb)]
        df.insert(3, 'new', new_values)
        error = {ct.err_code: 0, ct.err_msg: 'The data fields number is not match with schema.'}
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
        error = {ct.err_code: 0, ct.err_msg: 'The data fields number is not match with schema.'}
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
        error = {ct.err_code: 0, ct.err_msg: 'The types of schema and data do not match'}
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
        error = {ct.err_code: 0, ct.err_msg: 'The types of schema and data do not match'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_inconsistent_data(self):
        """
        target: test insert with inconsistent data
        method: insert with data that same field has different type data
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_list_data(nb=100)
        data[0][1] = 1.0
        error = {ct.err_code: 0, ct.err_msg: "The data in the same column must be of the same type"}
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items=error)


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
        error = {ct.err_code: 0, ct.err_msg: 'should create connect first'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.parametrize("vec_fields", [[cf.gen_float_vec_field(name="float_vector1")],
                                            [cf.gen_binary_vec_field()],
                                            [cf.gen_binary_vec_field(), cf.gen_binary_vec_field("binary_vec")]])
    def test_insert_multi_float_vec_fields(self, vec_fields):
        """
        target: test insert into multi float vec fields collection
        method: create collection and insert
        expected: verify num entities
        """
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_create_index(self):
        """
        target: test insert and create index
        method: 1. insert 2. create index
        expected: verify num entities and index
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(collection_w.collection, ct.default_float_vec_field_name, default_index_params)
        assert collection_w.indexes[0] == index

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_after_create_index(self):
        """
        target: test insert after create index
        method: 1. create index 2. insert data
        expected: verify index and num entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(collection_w.collection, ct.default_float_vec_field_name, default_index_params)
        assert collection_w.indexes[0] == index
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_binary_after_index(self):
        """
        target: test insert binary after index
        method: 1.create index 2.insert binary data
        expected: 1.index ok 2.num entities correct
        """
        schema = cf.gen_default_binary_collection_schema()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        collection_w.create_index(ct.default_binary_vec_field_name, default_binary_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(collection_w.collection, ct.default_binary_vec_field_name, default_binary_index_params)
        assert collection_w.indexes[0] == index
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_create_index(self):
        """
        target: test create index in auto_id=True collection
        method: 1.create auto_id=True collection and insert 2.create index
        expected: index correct
        """
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb
        # create index
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(collection_w.collection, ct.default_float_vec_field_name, default_index_params)
        assert collection_w.indexes[0] == index

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true(self):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert without ids
        expected: verify primary_keys and num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_twice_auto_id_true(self):
        """
        target: test insert ids fields twice when auto_id=True
        method: 1.create collection with auto_id=True 2.insert twice
        expected: verify primary_keys unique
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        nb = 10
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data(nb)
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        primary_keys = mutation_res.primary_keys
        assert cf._check_primary_keys(primary_keys, nb)
        mutation_res_1, _ = collection_w.insert(data=df)
        primary_keys.extend(mutation_res_1.primary_keys)
        assert cf._check_primary_keys(primary_keys, nb * 2)
        assert collection_w.num_entities == nb * 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true_list_data(self):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert list data with ids field values
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data(nb=ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data[1:])
        assert mutation_res.insert_count == ct.default_nb
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true_with_dataframe_values(self):
        """
        target: test insert with auto_id=True
        method: create collection with auto_id=True
        expected: 1.verify num entities 2.verify ids
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: 'Auto_id is True, primary field should not have data'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.is_empty

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true_with_list_values(self):
        """
        target: test insert with auto_id=True
        method: create collection with auto_id=True
        expected: 1.verify num entities 2.verify ids
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data(nb=100)
        error = {ct.err_code: 0, ct.err_msg: 'The data fields number is not match with schema'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.is_empty

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_false_same_values(self):
        """
        target: test insert same ids with auto_id false
        method: 1.create collection with auto_id=False 2.insert same int64 field values
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 100
        data = cf.gen_default_list_data(nb=nb)
        data[0] = [1 for i in range(nb)]
        mutation_res, _ = collection_w.insert(data)
        assert mutation_res.insert_count == nb
        assert mutation_res.primary_keys == data[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_false_negative_values(self):
        """
        target: test insert negative ids with auto_id false
        method: auto_id=False, primary field values is negative
        expected: verify num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 100
        data = cf.gen_default_list_data(nb)
        data[0] = [i for i in range(0, -nb, -1)]
        mutation_res, _ = collection_w.insert(data)
        assert mutation_res.primary_keys == data[0]
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_multi_threading(self):
        """
        target: test concurrent insert
        method: multi threads insert
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(ct.default_nb)
        thread_num = 4
        threads = []
        primary_keys = df[ct.default_int64_field_name].values.tolist()

        def insert(thread_i):
            log.debug(f'In thread-{thread_i}')
            mutation_res, _ = collection_w.insert(df)
            assert mutation_res.insert_count == ct.default_nb
            assert mutation_res.primary_keys == primary_keys

        for i in range(thread_num):
            x = threading.Thread(target=insert, args=(i,))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        assert collection_w.num_entities == ct.default_nb * thread_num

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Currently primary keys are not unique")
    def test_insert_multi_threading_auto_id(self):
        """
        target: test concurrent insert auto_id=True collection
        method: 1.create auto_id=True collection 2.concurrent insert
        expected: verify primary keys unique
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_multi_times(self):
        """
        target: test insert multi times
        method: insert data multi times
        expected: verify num entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        step = 120
        for _ in range(ct.default_nb // step):
            df = cf.gen_default_dataframe_data(step)
            mutation_res, _ = collection_w.insert(data=df)
            assert mutation_res.insert_count == step
            assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()

        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_all_datatype_collection(self):
        """
        target: test insert into collection that contains all datatype fields
        method: 1.create all datatype collection 2.insert data
        expected: verify num entities
        """
        self._connect()
        nb = 100
        df = cf.gen_dataframe_all_data_type(nb=nb)
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == nb


class TestInsertAsync(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert async
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_sync(self):
        """
        target: test async insert
        method: insert with async=True
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        future, _ = collection_w.insert(data=df, _async=True)
        future.done()
        mutation_res = future.result()
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_async_false(self):
        """
        target: test insert with false async
        method: async = false
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        mutation_res, _ = collection_w.insert(data=df, _async=False)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_async_callback(self):
        """
        target: test insert with callback func
        method: insert with callback func
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        future, _ = collection_w.insert(data=df, _async=True, _callback=assert_mutation_result)
        future.done()
        mutation_res = future.result()
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_long(self):
        """
        target: test insert with async
        method: insert 5w entities with callback func
        expected: verify num entities
        """
        nb = 50000
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb)
        future, _ = collection_w.insert(data=df, _async=True)
        future.done()
        mutation_res = future.result()
        assert mutation_res.insert_count == nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_callback_timeout(self):
        """
        target: test insert async with callback
        method: insert 10w entities with timeout=1
        expected: raise exception
        """
        nb = 100000
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb)
        future, _ = collection_w.insert(data=df, _async=True, _callback=assert_mutation_result, timeout=1)
        with pytest.raises(Exception):
            future.result()

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_invalid_data(self):
        """
        target: test insert async with invalid data
        method: insert async with invalid data
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        columns = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        df = pd.DataFrame(columns=columns)
        error = {ct.err_code: 0, ct.err_msg: "Cannot infer schema from empty dataframe"}
        collection_w.insert(data=df, _async=True, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_invalid_partition(self):
        """
        target: test insert async with invalid partition
        method: insert async with invalid partition
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        err_msg = "partitionID of partitionName:p can not be find"
        future, _ = collection_w.insert(data=df, partition_name="p", _async=True)
        future.done()
        with pytest.raises(Exception, match=err_msg):
            future.result()


def assert_mutation_result(mutation_res):
    assert mutation_res.insert_count == ct.default_nb
