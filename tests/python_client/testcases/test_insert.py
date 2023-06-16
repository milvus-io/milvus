from ssl import ALERT_DESCRIPTION_UNKNOWN_PSK_IDENTITY
import threading

import numpy as np
import pandas as pd
import random
import pytest
from pymilvus import Index, DataType
from pymilvus.exceptions import MilvusException

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "insert"
pre_upsert = "upsert"
exp_name = "name"
exp_schema = "schema"
exp_num = "num_entities"
exp_primary = "primary"
default_float_name = ct.default_float_field_name
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
default_binary_index_params = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": {"nlist": 64}}
default_search_exp = "int64 >= 0"


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
        method: 1.create collection
                2.insert dataframe data
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_non_data_type(self, get_non_data_type):
        """
        target: test insert with non-dataframe, non-list data
        method: insert with data (non-dataframe and non-list type)
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 1, ct.err_msg: "The type of data should be list or pandas.DataFrame"}
        collection_w.insert(data=get_non_data_type, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("data", [[], pd.DataFrame()])
    def test_insert_empty_data(self, data):
        """
        target: test insert empty data
        method: insert empty
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['int64', 'float', 'varchar', 'float_vector'], got %s" % data}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The name of field don't match, expected: int64, got "}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The name of field don't match, expected: int64, got %s" % get_invalid_field_name}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    def test_insert_dataframe_index(self):
        """
        target: test insert dataframe with index
        method: insert dataframe with index
        expected: todo
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
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
        collection_w.insert(data=data)

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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The name of field don't match, expected: float, got int"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        df[df.columns[1]] = new_float_value
        error = {ct.err_code: 1, ct.err_msg: 'The data fields number is not match with schema.'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: 'Arrays must all be same length.'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: 'Arrays must all be same length.'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['int64', 'float', 'varchar', 'float_vector'], "
                                             "got ['int64', 'float', 'varchar', 'new', 'float_vector']"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['int64', 'float', 'varchar', 'float_vector'], "
                                             "got ['int64', 'float', 'varchar']"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: 'The data fields number is not match with schema.'}
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
        error = {ct.err_code: 1, ct.err_msg: 'The data fields number is not match with schema.'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.fixture(scope="function", params=[8, 4096])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_default_partition(self):
        """
        target: test insert entities into default partition
        method: create partition and insert info collection
        expected: the collection insert count equals to nb
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w1 = self.init_partition_wrap(collection_w)
        data = cf.gen_default_list_data(nb=ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

    def test_insert_partition_not_existed(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the not existed partition_name param
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=ct.default_nb)
        error = {ct.err_code: 1, ct.err_msg: "partitionID of partitionName:p can not be existed"}
        mutation_res, _ = collection_w.insert(data=df, partition_name="p", check_task=CheckTasks.err_res,
                                              check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_partition_repeatedly(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it repeatedly, with the partition_name param
        expected: the collection row count equals to nq
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)
        df = cf.gen_default_dataframe_data(nb=ct.default_nb)
        mutation_res, _ = collection_w.insert(data=df, partition_name=partition_w1.name)
        new_res, _ = collection_w.insert(data=df, partition_name=partition_w2.name)
        assert mutation_res.insert_count == ct.default_nb
        assert new_res.insert_count == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_partition_with_ids(self):
        """
        target: test insert entities in collection created before, insert with ids
        method: create collection and insert entities in it, with the partition_name param
        expected: the collection insert count equals to nq
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_name = cf.gen_unique_str(prefix)
        partition_w1 = self.init_partition_wrap(collection_w, partition_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=df, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_field_type_not_match(self):
        """
        target: test insert entities, with the entity field type updated
        method: update entity field type
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_collection_schema_all_datatype
        error = {ct.err_code: 1, ct.err_msg: "The type of data should be list or pandas.DataFrame"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_exceed_varchar_limit(self):
        """
        target: test insert exceed varchar limit
        method: create a collection with varchar limit=2 and insert invalid data
        expected: error raised
        """
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_string_field(name='small_limit', max_length=2),
            cf.gen_string_field(name='big_limit', max_length=65530)
        ]
        schema = cf.gen_collection_schema(fields, auto_id=True)
        name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name, schema)
        vectors = cf.gen_vectors(2, ct.default_dim)
        data = [vectors, ["limit_1___________", "limit_2___________"], ['1', '2']]
        error = {ct.err_code: 1, ct.err_msg: "invalid input, length of string exceeds max length"}
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_lack_vector_field(self):
        """
        target: test insert entities, with no vector field
        method: remove entity values of vector field
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_collection_schema([cf.gen_int64_field(is_primary=True)])
        error = {ct.err_code: 1, ct.err_msg: "Data type is not support."}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_no_vector_field_dtype(self):
        """
        target: test insert entities, with vector field type is error
        method: vector field dtype is not existed
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        vec_field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_int64_field_name, dtype=DataType.NONE)
        field_one = cf.gen_int64_field(is_primary=True)
        field_two = cf.gen_int64_field()
        df = [field_one, field_two, vec_field]
        error = {ct.err_code: 1, ct.err_msg: "Field dtype must be of DataType."}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_no_vector_field_name(self):
        """
        target: test insert entities, with no vector field name
        method: vector field name is error
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        vec_field = cf.gen_float_vec_field(name=ct.get_invalid_strs)
        field_one = cf.gen_int64_field(is_primary=True)
        field_two = cf.gen_int64_field()
        df = [field_one, field_two, vec_field]
        error = {ct.err_code: 1, ct.err_msg: "data should be a list of list"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

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

    @pytest.mark.tags(CaseLabel.L1)
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
        method: 1.create auto_id=True collection and insert
                2.create index
        expected: index correct
        """
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data()
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true(self):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert without ids
        expected: verify primary_keys and num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data()
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true_list_data(self):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert list data with ids field values
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data()
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
        error = {ct.err_code: 1, ct.err_msg: "Please don't provide data for auto_id primary field: int64"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.is_empty

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['float', 'varchar', 'float_vector'], got ['', '', '', '']"}
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

    @pytest.mark.tags(CaseLabel.L1)
    # @pytest.mark.xfail(reason="issue 15416")
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_multi_times(self, dim):
        """
        target: test insert multi times
        method: insert data multi times
        expected: verify num entities
        """
        step = 120
        nb = 12000
        collection_w = self.init_collection_general(prefix, dim=dim)[0]
        for _ in range(nb // step):
            df = cf.gen_default_dataframe_data(step, dim)
            mutation_res, _ = collection_w.insert(data=df)
            assert mutation_res.insert_count == step
            assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()

        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_equal_to_resource_limit(self):
        """
        target: test insert data equal to RPC limitation 64MB (67108864)
        method: calculated critical value and insert equivalent data
        expected: raise exception
        """
        # nb = 127583 without json field
        nb = 108993
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        data = cf.gen_default_dataframe_data(nb)
        collection_w.insert(data=data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_one_field_using_default_value(self, default_value, auto_id):
        """
        target: test insert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using []/None to replace the field value
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [
            [i for i in range(ct.default_nb)],
            [np.float32(i) for i in range(ct.default_nb)],
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        if auto_id:
            del data[0]
        collection_w.insert(data)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_multi_fields_using_default_value(self, default_value, auto_id):
        """
        target: test insert with multi fields using default value
        method: 1. default value fields before vector, insert [], None, fail
                2. default value fields all after vector field, insert empty, succeed
        expected: report error and insert successfully
        """
        # 1. default value fields before vector, insert [], None, fail
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(default_value=np.float32(1.0)),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [[i for i in range(ct.default_nb)], default_value,
                # if multi default_value fields before vector field, every field must use []/None
                cf.gen_vectors(ct.default_nb, ct.default_dim)]
        if auto_id:
            del data[0]
        collection_w.insert(data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "The data type of field varchar doesn't match"})
        # 2. default value fields all after vector field, insert empty, succeed
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(),
                  cf.gen_float_field(default_value=np.float32(1.0)),
                  cf.gen_string_field(default_value="abc")]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [[i for i in range(ct.default_nb)], cf.gen_vectors(ct.default_nb, ct.default_dim)]
        data1 = [[i for i in range(ct.default_nb)], cf.gen_vectors(ct.default_nb, ct.default_dim),
                 [np.float32(i) for i in range(ct.default_nb)]]
        if auto_id:
            del data[0], data1[0]
        collection_w.insert(data)
        assert collection_w.num_entities == ct.default_nb
        collection_w.insert(data1)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_dataframe_using_default_value(self):
        """
        target: test insert with dataframe
        method: insert with invalid dataframe
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        # None/[] is not allowed when using dataframe
        # To use default value, delete the whole item
        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(0, ct.default_nb)]),
            "float_vector": vectors,
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32")
        })
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb


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
        future, _ = collection_w.insert(data=df, _async=True, _callback=None, timeout=0.2)
        with pytest.raises(MilvusException):
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
        err_msg = "partition=p: partition not found"
        future, _ = collection_w.insert(data=df, partition_name="p", _async=True)
        future.done()
        with pytest.raises(MilvusException, match=err_msg):
            future.result()

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_no_vectors_raise_exception(self):
        """
        target: test insert vectors with no vectors
        method: set only vector field and insert into collection
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_collection_schema([cf.gen_int64_field(is_primary=True)])
        error = {ct.err_code: 1, ct.err_msg: "fleldSchema lack of vector field."}
        future, _ = collection_w.insert(data=df, _async=True, check_task=CheckTasks.err_res, check_items=error)


def assert_mutation_result(mutation_res):
    assert mutation_res.insert_count == ct.default_nb


class TestInsertBinary(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_binary_partition(self):
        """
        target: test insert entities and create partition 
        method: create collection and insert binary entities in it, with the partition_name param
        expected: the collection row count equals to nb
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        partition_name = cf.gen_unique_str(prefix)
        partition_w1 = self.init_partition_wrap(collection_w, partition_name)
        mutation_res, _ = collection_w.insert(data=df, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_multi_times(self):
        """
        target: test insert entities multi times and final flush
        method: create collection and insert binary entity multi 
        expected: the collection row count equals to nb
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        nums = 2
        for i in range(nums):
            mutation_res, _ = collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb * nums

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_binary_create_index(self):
        """
        target: test build index insert after vector
        method: insert vector and build index
        expected: no error raised
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=df)
        assert mutation_res.insert_count == ct.default_nb
        default_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)


class TestInsertInvalid(TestcaseBase):
    """
      ******************************************************************
      The following cases are used to test insert invalid params
      ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_ids_invalid(self):
        """
        target: test insert, with using auto id is invalid, which are not int64
        method: create collection and insert entities in it
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        int_field = cf.gen_float_field(is_primary=True)
        vec_field = cf.gen_float_vec_field(name='vec')
        df = [int_field, vec_field]
        error = {ct.err_code: 1, ct.err_msg: "Primary key type must be DataType.INT64."}
        mutation_res, _ = collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_invalid_partition_name(self):
        """
        target: test insert with invalid scenario
        method: insert with invalid partition name
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        df = cf.gen_default_list_data(ct.default_nb)
        error = {ct.err_code: 1, 'err_msg': "partition name is illegal"}
        mutation_res, _ = collection_w.insert(data=df, partition_name="p", check_task=CheckTasks.err_res,
                                              check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_with_invalid_field_value(self):
        """
        target: test insert with invalid field
        method: insert with invalid field value
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        field_one = cf.gen_int64_field(is_primary=True)
        field_two = cf.gen_int64_field()
        vec_field = ct.get_invalid_vectors
        df = [field_one, field_two, vec_field]
        error = {ct.err_code: 1, ct.err_msg: "Data type is not support."}
        mutation_res, _ = collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_over_resource_limit(self):
        """
        target: test insert over RPC limitation 64MB (67108864)
        method: insert excessive data
        expected: raise exception
        """
        nb = 150000
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        data = cf.gen_default_dataframe_data(nb)
        error = {ct.err_code: 1, ct.err_msg: "<_MultiThreadedRendezvous of RPC that terminated with:"
                                             "status = StatusCode.RESOURCE_EXHAUSTED"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_array_using_default_value(self, default_value):
        """
        target: test insert with array
        method: insert with invalid array
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [{"int64": 1, "float_vector": vectors[1], "varchar": default_value, "float": np.float32(1.0)}]
        collection_w.insert(data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "Field varchar don't match in entities[0]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_tuple_using_default_value(self, default_value):
        """
        target: test insert with tuple
        method: insert with invalid tuple
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(),
                  cf.gen_string_field(), cf.gen_float_field(default_value=np.float32(3.14))]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, vectors, string_values, default_value)
        collection_w.insert(data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "Field varchar don't match in entities[0]"})


class TestInsertInvalidBinary(TestcaseBase):
    """
      ******************************************************************
      The following cases are used to test insert invalid params of binary
      ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_ids_binary_invalid(self):
        """
        target: test insert, with using customize ids, which are not int64
        method: create collection and insert entities in it
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        field_one = cf.gen_float_field(is_primary=True)
        field_two = cf.gen_float_field()
        vec_field, _ = self.field_schema_wrap.init_field_schema(name=ct.default_binary_vec_field_name,
                                                                dtype=DataType.BINARY_VECTOR)
        df = [field_one, field_two, vec_field]
        error = {ct.err_code: 1, ct.err_msg: "data should be a list of list"}
        mutation_res, _ = collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_invalid_binary_partition_name(self):
        """
        target: test insert with invalid scenario
        method: insert with invalid partition name
        expected: raise exception
        """
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        partition_name = ct.get_invalid_strs
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        error = {ct.err_code: 1, 'err_msg': "The types of schema and data do not match."}
        mutation_res, _ = collection_w.insert(data=df, partition_name=partition_name, check_task=CheckTasks.err_res,
                                              check_items=error)


class TestInsertString(TestcaseBase):
    """
      ******************************************************************
      The following cases are used to test insert string
      ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_string_field_is_primary(self):
        """
        target: test insert string is primary
        method: 1.create a collection and string field is primary
                2.insert string field data
        expected: Insert Successfully
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=data)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == data[2]

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("string_fields", [[cf.gen_string_field(name="string_field1")],
                                               [cf.gen_string_field(name="string_field2")],
                                               [cf.gen_string_field(name="string_field3")]])
    def test_insert_multi_string_fields(self, string_fields):
        """
        target: test insert multi string fields
        method: 1.create a collection
                2.Insert multi string fields
        expected: Insert Successfully
        """

        schema = cf.gen_schema_multi_string_fields(string_fields)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_string_fields(string_fields=string_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_string_field_invalid_data(self):
        """
        target: test insert string field data is not match
        method: 1.create a collection
                2.Insert string field data is not match
        expected: Raise exceptions
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        df = cf.gen_default_dataframe_data(nb)
        new_float_value = pd.Series(data=[float(i) for i in range(nb)], dtype="float64")
        df[df.columns[2]] = new_float_value
        error = {ct.err_code: 1, ct.err_msg: "The data type of field varchar doesn't match, expected: VARCHAR, got DOUBLE"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_string_field_name_invalid(self):
        """
        target: test insert string field name is invaild
        method: 1.create a collection  
                2.Insert string field name is invalid
        expected: Raise exceptions
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = [cf.gen_int64_field(), cf.gen_string_field(name=ct.get_invalid_strs), cf.gen_float_vec_field()]
        error = {ct.err_code: 1, ct.err_msg: 'data should be a list of list'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_string_field_length_exceed(self):
        """
        target: test insert string field exceed the maximum length
        method: 1.create a collection  
                2.Insert string field length is exceeded maximum value of 65535
        expected: Raise exceptions
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nums = 70000
        field_one = cf.gen_int64_field()
        field_two = cf.gen_float_field()
        field_three = cf.gen_string_field(max_length=nums)
        vec_field = cf.gen_float_vec_field()
        df = [field_one, field_two, field_three, vec_field]
        error = {ct.err_code: 1, ct.err_msg: 'data should be a list of list'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_string_field_dtype_invalid(self):
        """
        target: test insert string field with invaild dtype
        method: 1.create a collection  
                2.Insert string field dtype is invalid
        expected: Raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        string_field = self.field_schema_wrap.init_field_schema(name="string", dtype=DataType.STRING)[0]
        int_field = cf.gen_int64_field(is_primary=True)
        vec_field = cf.gen_float_vec_field()
        df = [string_field, int_field, vec_field]
        error = {ct.err_code: 1, ct.err_msg: 'data should be a list of list'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_string_field_auto_id_is_true(self):
        """
        target: test create collection with string field 
        method: 1.create a collection  
                2.Insert string field with auto id is true
        expected: Raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        int_field = cf.gen_int64_field()
        vec_field = cf.gen_float_vec_field()
        string_field = cf.gen_string_field(is_primary=True, auto_id=True)
        df = [int_field, string_field, vec_field]
        error = {ct.err_code: 1, ct.err_msg: 'data should be a list of list'}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_string_field_space(self):
        """
        target: test create collection with string field 
        method: 1.create a collection  
                2.Insert string field  with space
        expected: Insert successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 1000
        data = cf.gen_default_list_data(nb)
        data[2] = [" "for _ in range(nb)] 
        collection_w.insert(data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_string_field_empty(self):
        """
        target: test create collection with string field 
        method: 1.create a collection  
                2.Insert string field with empty
        expected: Insert successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 1000
        data = cf.gen_default_list_data(nb)
        data[2] = [""for _ in range(nb)] 
        collection_w.insert(data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_string_field_is_pk_and_empty(self):
        """
        target: test create collection with string field is primary
        method: 1.create a collection  
                2.Insert string field with empty, string field is pk
        expected: Insert successfully
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        nb = 1000
        data = cf.gen_default_list_data(nb)
        data[2] = [""for _ in range(nb)] 
        collection_w.insert(data)
        assert collection_w.num_entities == nb


class TestUpsertValid(TestcaseBase):
    """ Valid test case of Upsert interface """

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_data_pk_not_exist(self):
        """
        target: test upsert with collection has no data
        method: 1. create a collection with no initialized data
                2. upsert data
        expected: upsert run normally as inert
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_dataframe_data()
        collection_w.upsert(data=data)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("start", [0, 1500, 2500, 3500])
    def test_upsert_data_pk_exist(self, start):
        """
        target: test upsert data and collection pk exists
        method: 1. create a collection and insert data
                2. upsert data whose pk exists
        expected: upsert succeed
        """
        upsert_nb = 1000
        collection_w = self.init_collection_general(pre_upsert, True)[0]
        upsert_data, float_values = cf.gen_default_data_for_upsert(upsert_nb, start=start)
        collection_w.upsert(data=upsert_data)
        exp = f"int64 >= {start} && int64 <= {upsert_nb + start}"
        res = collection_w.query(exp, output_fields=[default_float_name])[0]
        assert [res[i][default_float_name] for i in range(upsert_nb)] == float_values.to_list()

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_with_primary_key_string(self):
        """
        target: test upsert with string primary key
        method: 1. create a collection with pk string
                2. insert data
                3. upsert data with ' ' before or after string
        expected: raise no exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        fields = [cf.gen_string_field(), cf.gen_float_vec_field(dim=ct.default_dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field=ct.default_string_field_name)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(2)]
        collection_w.insert([["a", "b"], vectors])
        collection_w.upsert([[" a", "b  "], vectors])
        assert collection_w.num_entities == 4

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_binary_data(self):
        """
        target: test upsert binary data
        method: 1. create a collection and insert data
                2. upsert data
                3. check the results
        expected: raise no exception
        """
        nb = 500
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_general(c_name, True, is_binary=True)[0]
        binary_vectors = cf.gen_binary_vectors(nb, ct.default_dim)[1]
        data = [[i for i in range(nb)], [np.float32(i) for i in range(nb)],
                [str(i) for i in range(nb)], binary_vectors]
        collection_w.upsert(data)
        res = collection_w.query("int64 >= 0", [ct.default_binary_vec_field_name])[0]
        assert binary_vectors[0] == res[0][ct. default_binary_vec_field_name][0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_same_with_inserted_data(self):
        """
        target: test upsert with data same with collection inserted data
        method: 1. create a collection and insert data
                2. upsert data same with inserted
                3. check the update data number
        expected: upsert successfully
        """
        upsert_nb = 1000
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_dataframe_data()
        collection_w.insert(data=data)
        upsert_data = data[:upsert_nb]
        res = collection_w.upsert(data=upsert_data)[0]
        assert res.insert_count == upsert_nb, res.delete_count == upsert_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_data_is_none(self):
        """
        target: test upsert with data=None
        method: 1. create a collection
                2. insert data
                3. upsert data=None
        expected: raise no exception
        """
        collection_w = self.init_collection_general(pre_upsert, insert_data=True, is_index=False)[0]
        assert collection_w.num_entities == ct.default_nb
        collection_w.upsert(data=None)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_in_specific_partition(self):
        """
        target: test upsert in specific partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. upsert in the given partition
        expected: raise no exception
        """
        # create a collection and 2 partitions
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_partition("partition_new")
        cf.insert_data(collection_w)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # check the ids which will be upserted is in partition _default
        upsert_nb = 10
        expr = f"int64 >= 0 && int64 < {upsert_nb}"
        res0 = collection_w.query(expr, [default_float_name], ["_default"])[0]
        assert len(res0) == upsert_nb
        collection_w.flush()
        res1 = collection_w.query(expr, [default_float_name], ["partition_new"])[0]
        assert collection_w.partition('partition_new')[0].num_entities == ct.default_nb // 2

        # upsert ids in partition _default
        data, float_values = cf.gen_default_data_for_upsert(upsert_nb)
        collection_w.upsert(data=data, partition_name="_default")

        # check the result in partition _default(upsert successfully) and others(no missing, nothing new)
        collection_w.flush()
        res0 = collection_w.query(expr, [default_float_name], ["_default"])[0]
        res2 = collection_w.query(expr, [default_float_name], ["partition_new"])[0]
        assert res1 == res2
        assert [res0[i][default_float_name] for i in range(upsert_nb)] == float_values.to_list()
        assert collection_w.partition('partition_new')[0].num_entities == ct.default_nb // 2

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.skip(reason="issue #22592")
    def test_upsert_in_mismatched_partitions(self):
        """
        target: test upsert in unmatched partition
        method: 1. create a collection and 2 partitions
                2. insert data and load
                3. upsert in unmatched partitions
        expected: upsert successfully
        """
        # create a collection and 2 partitions
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_partition("partition_1")
        collection_w.create_partition("partition_2")

        # insert data and load collection
        cf.insert_data(collection_w)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # check the ids which will be upserted is not in partition 'partition_1'
        upsert_nb = 100
        expr = f"int64 >= 0 && int64 <= {upsert_nb}"
        res = collection_w.query(expr, [default_float_name], ["partition_1"])[0]
        assert len(res) == 0

        # upsert in partition 'partition_1'
        data, float_values = cf.gen_default_data_for_upsert(upsert_nb)
        collection_w.upsert(data, "partition_1")

        # check the upserted data in 'partition_1'
        res1 = collection_w.query(expr, [default_float_name], ["partition_1"])[0]
        assert [res1[i][default_float_name] for i in range(upsert_nb)] == float_values.to_list()

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_same_pk_concurrently(self):
        """
        target: test upsert the same pk concurrently
        method: 1. create a collection and insert data
                2. load collection
                3. upsert the same pk
        expected: not raise exception
        """
        # initialize a collection
        upsert_nb = 1000
        collection_w = self.init_collection_general(pre_upsert, True)[0]
        data1, float_values1 = cf.gen_default_data_for_upsert(upsert_nb, size=1000)
        data2, float_values2 = cf.gen_default_data_for_upsert(upsert_nb)

        # upsert at the same time
        def do_upsert1():
            collection_w.upsert(data=data1)

        def do_upsert2():
            collection_w.upsert(data=data2)

        t1 = threading.Thread(target=do_upsert1, args=())
        t2 = threading.Thread(target=do_upsert2, args=())

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # check the result
        exp = f"int64 >= 0 && int64 <= {upsert_nb}"
        res = collection_w.query(exp, [default_float_name], consistency_level="Strong")[0]
        res = [res[i][default_float_name] for i in range(upsert_nb)]
        if not (res == float_values1.to_list() or res == float_values2.to_list()):
            assert False

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_multiple_times(self):
        """
        target: test upsert multiple times
        method: 1. create a collection and insert data
                2. upsert repeatedly
        expected: not raise exception
        """
        # initialize a collection
        upsert_nb = 1000
        collection_w = self.init_collection_general(pre_upsert, True)[0]
        # upsert
        step = 500
        for i in range(10):
            data = cf.gen_default_data_for_upsert(upsert_nb, start=i*step)[0]
            collection_w.upsert(data)
        # check the result
        res = collection_w.query(expr="", output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == upsert_nb * 10 - step * 9

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_pk_string_multiple_times(self):
        """
        target: test upsert multiple times
        method: 1. create a collection and insert data
                2. upsert repeatedly
        expected: not raise exception
        """
        # initialize a collection
        upsert_nb = 1000
        schema = cf.gen_string_pk_default_collection_schema()
        name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name, schema)
        collection_w.insert(cf.gen_default_list_data())
        # upsert
        step = 500
        for i in range(10):
            data = cf.gen_default_list_data(upsert_nb, start=i * step)
            collection_w.upsert(data)
        # load
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        # check the result
        res = collection_w.query(expr="", output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == upsert_nb * 10 - step * 9

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_one_field_using_default_value(self, default_value):
        """
        target: test insert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using []/None to replace the field value
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        cf.insert_data(collection_w, with_json=False)
        data = [
            [i for i in range(ct.default_nb)],
            [np.float32(i) for i in range(ct.default_nb)],
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.upsert(data)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_multi_fields_using_default_value(self, default_value):
        """
        target: test insert with multi fields using default value
        method: 1. default value fields before vector, insert [], None, fail
                2. default value fields all after vector field, insert empty, succeed
        expected: report error and insert successfully
        """
        # 1. default value fields before vector, insert [], None, fail
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(default_value=np.float32(1.0)),
            cf.gen_string_field(default_value="abc"),
            cf.gen_float_vec_field()
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        cf.insert_data(collection_w, with_json=False)
        data = [
            [i for i in range(ct.default_nb)],
            default_value,
            # if multi default_value fields before vector field, every field must use []/None
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.upsert(data,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1,
                                         ct.err_msg: "The data type of field varchar doesn't match"})

        # 2. default value fields all after vector field, insert empty, succeed
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_float_field(default_value=np.float32(1.0)),
            cf.gen_string_field(default_value="abc")
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [
            [i for i in range(ct.default_nb)],
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        data1 = [
            [i for i in range(ct.default_nb)],
            cf.gen_vectors(ct.default_nb, ct.default_dim),
            [np.float32(i) for i in range(ct.default_nb)]
        ]
        collection_w.upsert(data)
        assert collection_w.num_entities == ct.default_nb
        collection_w.upsert(data1)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_dataframe_using_default_value(self):
        """
        target: test upsert with dataframe
        method: upsert with invalid dataframe
        expected: upsert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        # None/[] is not allowed when using dataframe
        # To use default value, delete the whole item
        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(0, ct.default_nb)]),
            "float_vector": vectors,
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32")
        })
        collection_w.upsert(df)
        assert collection_w.num_entities == ct.default_nb


class TestUpsertInvalid(TestcaseBase):
    """ Invalid test case of Upsert interface """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("data", ct.get_invalid_strs[:12])
    def test_upsert_non_data_type(self, data):
        """
        target: test upsert with invalid data type
        method: upsert data type string, set, number, float...
        expected: raise exception
        """
        if data is None:
            pytest.skip("data=None is valid")
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, expected: "
                                             "['int64', 'float', 'varchar', 'float_vector']"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_pk_type_invalid(self):
        """
        target: test upsert with invalid pk type
        method: upsert data type string, float...
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        data = [['a', 1.5], [np.float32(i) for i in range(2)], [str(i) for i in range(2)],
                cf.gen_vectors(2, ct.default_dim)]
        error = {ct.err_code: 1, ct.err_msg: "The data type of field int64 doesn't match, "
                                             "expected: INT64, got VARCHAR"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_data_unmatch(self):
        """
        target: test upsert with unmatched data type
        method: 1. create a collection with default schema [int, float, string, vector]
                2. upsert with data [int, string, float, vector]
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        vector = [random.random() for _ in range(ct.default_dim)]
        data = [1, "a", 2.0, vector]
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['int64', 'float', 'varchar', 'float_vector']"}
        collection_w.upsert(data=[data], check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector", [[], [1.0, 2.0], "a", 1.0, None])
    def test_upsert_vector_unmatch(self, vector):
        """
        target: test upsert with unmatched data vector
        method: 1. create a collection with dim=128
                2. upsert with vector dim unmatch
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        data = [2.0, "a", vector]
        error = {ct.err_code: 1, ct.err_msg: "The fields don't match with schema fields, "
                                             "expected: ['int64', 'float', 'varchar', 'float_vector']"}
        collection_w.upsert(data=[data], check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [120, 129, 200])
    def test_upsert_binary_dim_unmatch(self, dim):
        """
        target: test upsert with unmatched vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim
        expected: raise exception
        """
        collection_w = self.init_collection_general(pre_upsert, True, is_binary=True)[0]
        data = cf.gen_default_binary_dataframe_data(dim=dim)[0]
        error = {ct.err_code: 1, ct.err_msg: f"Collection field dim is 128, but entities field dim is {dim}"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [127, 129, 200])
    def test_upsert_dim_unmatch(self, dim):
        """
        target: test upsert with unmatched vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim
        expected: raise exception
        """
        collection_w = self.init_collection_general(pre_upsert, True)[0]
        data = cf.gen_default_data_for_upsert(dim=dim)[0]
        error = {ct.err_code: 1, ct.err_msg: f"Collection field dim is 128, but entities field dim is {dim}"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ct.get_invalid_strs[:13])
    def test_upsert_partition_name_invalid(self, partition_name):
        """
        target: test upsert partition name invalid
        method: 1. create a collection with partitions
                2. upsert with invalid partition name
        expected: raise exception
        """
        if partition_name is None or partition_name == [] or partition_name == "":
            pytest.skip("valid")
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        p_name = cf.gen_unique_str('partition_')
        collection_w.create_partition(p_name)
        cf.insert_data(collection_w)
        data = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 1, ct.err_msg: "Invalid partition name"}
        collection_w.upsert(data=data, partition_name=partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_partition_name_nonexistent(self):
        """
        target: test upsert partition name nonexistent
        method: 1. create a collection
                2. upsert with nonexistent partition name
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_dataframe_data(nb=2)
        partition_name = "partition1"
        error = {ct.err_code: 1, ct.err_msg: "partition is not exist: partition1"}
        collection_w.upsert(data=data, partition_name=partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_multi_partitions(self):
        """
        target: test upsert two partitions
        method: 1. create a collection and two partitions
                2. upsert two partitions
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_partition("partition_1")
        collection_w.create_partition("partition_2")
        cf.insert_data(collection_w)
        data = cf.gen_default_dataframe_data(nb=1000)
        error = {ct.err_code: 1, ct.err_msg: "['partition_1', 'partition_2'] has type <class 'list'>, "
                                             "but expected one of: (<class 'bytes'>, <class 'str'>)"}
        collection_w.upsert(data=data, partition_name=["partition_1", "partition_2"],
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_with_auto_id(self):
        """
        target: test upsert with auto id
        method: 1. create a collection with autoID=true
                2. upsert data no pk
        expected: raise exception
        """
        collection_w = self.init_collection_general(pre_upsert, auto_id=True, is_index=False)[0]
        error = {ct.err_code: 1, ct.err_msg: "Upsert don't support autoid == true"}
        float_vec_values = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [[np.float32(i) for i in range(ct.default_nb)], [str(i) for i in range(ct.default_nb)],
                float_vec_values]
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_array_using_default_value(self, default_value):
        """
        target: test upsert with array
        method: upsert with invalid array
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [{"int64": 1, "float_vector": vectors[1], "varchar": default_value, "float": np.float32(1.0)}]
        collection_w.upsert(data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "Field varchar don't match in entities[0]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_tuple_using_default_value(self, default_value):
        """
        target: test upsert with tuple
        method: upsert with invalid tuple
        expected: upsert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(default_value=np.float32(3.14)),
                  cf.gen_string_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, default_value, string_values, vectors)
        collection_w.upsert(data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "Field varchar don't match in entities[0]"})
