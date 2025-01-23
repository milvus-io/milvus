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
default_index_params = {"index_type": "IVF_SQ8",
                        "metric_type": "L2", "params": {"nlist": 64}}
default_binary_index_params = ct.default_binary_index
default_search_exp = "int64 >= 0"


class TestInsertParams(TestcaseBase):
    """ Test case of Insert interface """

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
        assert mutation_res.primary_keys == data[0].tolist()
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_non_data_type(self):
        """
        target: test insert with non-dataframe, non-list data
        method: insert with data (non-dataframe and non-list type)
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 999,
                 ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"}
        collection_w.insert(data=None,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("data", [pd.DataFrame()])
    def test_insert_empty_dataframe(self, data):
        """
        target: test insert empty dataFrame()
        method: insert empty
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 999, ct.err_msg: "The fields don't match with schema fields"}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("data", [[[]]])
    def test_insert_empty_data(self, data):
        """
        target: test insert empty array
        method: insert empty
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        error = {ct.err_code: 999, ct.err_msg: "The data doesn't match with schema fields"}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_dataframe_only_columns(self):
        """
        target: test insert with dataframe just columns
        method: dataframe just have columns
        expected: num entities is zero
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        columns = [ct.default_int64_field_name,
                   ct.default_float_vec_field_name]
        df = pd.DataFrame(columns=columns)
        error = {ct.err_code: 999,
                 ct.err_msg: "The fields don't match with schema fields"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_empty_field_name_dataframe(self):
        """
        target: test insert empty field name df
        method: dataframe with empty column
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, dim=32)
        df = cf.gen_default_dataframe_data(10)
        df.rename(columns={ct.default_int64_field_name: ' '}, inplace=True)
        error = {ct.err_code: 999,
                 ct.err_msg: "The name of field doesn't match, expected: int64"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_invalid_field_name_dataframe(self):
        """
        target: test insert with invalid dataframe data
        method: insert with invalid field name dataframe
        expected: raise exception
        """
        invalid_field_name = "non_existing"
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data(10)
        df.rename(
            columns={ct.default_int64_field_name: invalid_field_name}, inplace=True)
        error = {ct.err_code: 999,
                 ct.err_msg: f"The name of field doesn't match, expected: int64, got {invalid_field_name}"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_numpy_data(self):
        """
        target: test insert numpy.ndarray data
        method: 1.create by schema 2.insert data
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        data = cf.gen_numpy_data(nb=nb)
        collection_w.insert(data=data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_dataframe(self):
        """
        target: test insert binary dataframe
        method: 1. create by schema 2. insert dataframe
        expected: assert num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
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
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
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
        assert mutation_res.primary_keys == data[0].tolist()
        assert collection_w.num_entities == 1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #37543")
    def test_insert_dim_not_match(self):
        """
        target: test insert with not match dim
        method: insert data dim not equal to schema dim
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        dim = 129
        df = cf.gen_default_dataframe_data(nb=20, dim=dim)
        error = {ct.err_code: 999,
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
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
        dim = 120
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb, dim=dim)
        error = {ct.err_code: 1100,
                 ct.err_msg: f'the dim ({dim}) of field data(binary_vector) is not equal to schema dim '
                             f'({ct.default_dim}): invalid parameter[expected={ct.default_dim}][actual={dim}]'}
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
        error = {ct.err_code: 999, ct.err_msg: "The name of field doesn't match, expected: float, got int"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Currently not check in pymilvus")
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
        error = {ct.err_code: 999,
                 ct.err_msg: "The data type of field float doesn't match, expected: FLOAT, got DOUBLE"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_value_less(self):
        """
        target: test insert value less than other
        method: string field value less than vec-field value
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        data = []
        for fields in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(fields, nb=nb)
            if fields.dtype == DataType.VARCHAR:
                field_data = field_data[:-1]
            data.append(field_data)
        error = {ct.err_code: 999, ct.err_msg: "Field data size misaligned for field [varchar] "}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

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
        data = []
        for fields in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(fields, nb=nb)
            if fields.dtype == DataType.FLOAT_VECTOR:
                field_data = field_data[:-1]
            data.append(field_data)
        error = {ct.err_code: 999, ct.err_msg: 'Field data size misaligned for field [float_vector] '}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_fields_more(self):
        """
        target: test insert with fields more
        method: field more than schema fields
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 10
        data = []
        for fields in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(fields, nb=nb)
            data.append(field_data)
        data.append([1 for _ in range(nb)])
        error = {ct.err_code: 999, ct.err_msg: "The data doesn't match with schema fields"}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

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
        error = {ct.err_code: 999, ct.err_msg: "The fields don't match with schema fields"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)

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
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            data.append(field_data)
        tmp = data[0]
        data[0] = data[1]
        data[1] = tmp
        error = {ct.err_code: 999,
                 ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_inconsistent_data(self):
        """
        target: test insert with inconsistent data
        method: insert with data that same field has different type data
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        data = cf.gen_default_rows_data(nb=100)
        data[0][ct.default_int64_field_name] = 1.0
        error = {ct.err_code: 999,
                 ct.err_msg: "The Input data type is inconsistent with defined schema, {%s} field should be a int64, "
                             "but got a {<class 'float'>} instead." % ct.default_int64_field_name}
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

    @pytest.fixture(scope="function", params=[ct.default_int64_field_name, ct.default_string_field_name])
    def pk_field(self, request):
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
        error = {ct.err_code: 999, ct.err_msg: 'should create connection first'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_default_partition(self):
        """
        target: test insert entities into default partition
        method: create partition and insert info collection
        expected: the collection insert count equals to nb
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        partition_w1 = self.init_partition_wrap(collection_w)
        data = cf.gen_default_list_data(nb=ct.default_nb)
        mutation_res, _ = collection_w.insert(
            data=data, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

    def test_insert_partition_not_existed(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it, with the not existed partition_name param
        expected: error raised
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=10)
        error = {ct.err_code: 999,
                 ct.err_msg: "partition not found[partition=p]"}
        mutation_res, _ = collection_w.insert(data=df, partition_name="p", check_task=CheckTasks.err_res,
                                              check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_partition_repeatedly(self):
        """
        target: test insert entities in collection created before
        method: create collection and insert entities in it repeatedly, with the partition_name param
        expected: the collection row count equals to nq
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)
        df = cf.gen_default_dataframe_data(nb=ct.default_nb)
        mutation_res, _ = collection_w.insert(
            data=df, partition_name=partition_w1.name)
        new_res, _ = collection_w.insert(
            data=df, partition_name=partition_w2.name)
        assert mutation_res.insert_count == ct.default_nb
        assert new_res.insert_count == ct.default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_partition_with_ids(self):
        """
        target: test insert entities in collection created before, insert with ids
        method: create collection and insert entities in it, with the partition_name param
        expected: the collection insert count equals to nq
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        partition_name = cf.gen_unique_str(prefix)
        partition_w1 = self.init_partition_wrap(collection_w, partition_name)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(
            data=df, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

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
        data = [vectors, ["limit_1___________",
                          "limit_2___________"], ['1', '2']]
        error = {ct.err_code: 999,
                 ct.err_msg: "length of string exceeds max length"}
        collection_w.insert(
            data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_no_vector_field_dtype(self):
        """
        target: test insert entities, with no vector field
        method: vector field is missing in data
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        nb = 1
        data = []
        fields = collection_w.schema.fields
        for field in fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.dtype != DataType.FLOAT_VECTOR:
                data.append(field_data)
        error = {ct.err_code: 999, ct.err_msg: f"The data doesn't match with schema fields, "
                                               f"expect {len(fields)} list, got {len(data)}"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_vector_field_dismatch_dtype(self):
        """
        target: test insert entities, with no vector field
        method: vector field is missing in data
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        nb = 1
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.dtype == DataType.FLOAT_VECTOR:
                field_data = [random.randint(-1000, 1000) * 0.0001 for _ in range(nb)]
            data.append(field_data)
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

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
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(
            collection_w.collection, ct.default_float_vec_field_name, default_index_params)
        assert collection_w.indexes[0] == index

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_after_create_index(self):
        """
        target: test insert after create index
        method: 1. create index 2. insert data
        expected: verify index and num entities
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(
            collection_w.collection, ct.default_float_vec_field_name, default_index_params)
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
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema)
        collection_w.create_index(
            ct.default_binary_vec_field_name, default_binary_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(
            collection_w.collection, ct.default_binary_vec_field_name, default_binary_index_params)
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
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data()
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb
        # create index
        collection_w.create_index(
            ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index, _ = collection_w.index()
        assert index == Index(
            collection_w.collection, ct.default_float_vec_field_name, default_index_params)
        assert collection_w.indexes[0] == index

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true(self, pk_field):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert without ids
        expected: verify primary_keys and num_entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(
            primary_field=pk_field, auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data()
        df.drop(pk_field, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_twice_auto_id_true(self, pk_field):
        """
        target: test insert ids fields twice when auto_id=True
        method: 1.create collection with auto_id=True 2.insert twice
        expected: verify primary_keys unique
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(
            primary_field=pk_field, auto_id=True)
        nb = 10
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data(nb)
        df.drop(pk_field, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        primary_keys = mutation_res.primary_keys
        assert cf._check_primary_keys(primary_keys, nb)
        mutation_res_1, _ = collection_w.insert(data=df)
        primary_keys.extend(mutation_res_1.primary_keys)
        assert cf._check_primary_keys(primary_keys, nb * 2)
        assert collection_w.num_entities == nb * 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true_list_data(self, pk_field):
        """
        target: test insert ids fields values when auto_id=True
        method: 1.create collection with auto_id=True 2.insert list data with ids field values
        expected: assert num entities
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(
            primary_field=pk_field, auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_data()
        if pk_field == ct.default_int64_field_name:
            mutation_res, _ = collection_w.insert(data=data[1:])
        else:
            del data[2]
            mutation_res, _ = collection_w.insert(data=data)
        assert mutation_res.insert_count == ct.default_nb
        assert cf._check_primary_keys(mutation_res.primary_keys, ct.default_nb)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_auto_id_true_with_dataframe_values(self, pk_field):
        """
        target: test insert with auto_id=True
        method: create collection with auto_id=True
        expected: 1.verify num entities 2.verify ids
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(
            primary_field=pk_field, auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        df = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 999,
                 ct.err_msg: f"Expect no data for auto_id primary field: {pk_field}"}
        collection_w.insert(
            data=df, check_task=CheckTasks.err_res, check_items=error)
        assert collection_w.is_empty

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_auto_id_true_with_list_values(self, pk_field):
        """
        target: test insert with auto_id=True
        method: create collection with auto_id=True
        expected: 1.verify num entities 2.verify ids
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=pk_field, auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = []
        nb = 100
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.name != pk_field:
                data.append(field_data)
        collection_w.insert(data=data)
        assert collection_w.num_entities == nb

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
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
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
            assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist(
            )

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
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for i in range(ct.default_nb)]])
    def test_insert_one_field_using_default_value(self, default_value, nullable, auto_id):
        """
        target: test insert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using default value to replace the field value []/[None]
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc", nullable=nullable), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        # default value fields, [] or [None]
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
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for _ in range(ct.default_nb)]])
    def test_insert_multi_fields_using_none_data(self, enable_partition_key, default_value, auto_id):
        """
        target: test insert with multi fields include array using none value
        method: 1. create a collection with multi fields using default value
                2. insert using none value to replace the field value
        expected: insert successfully
        """
        json_embedded_object = "json_embedded_object"
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_int32_field(default_value=np.int32(1), nullable=True),
            cf.gen_float_field(default_value=np.float32(1.0), nullable=True),
            cf.gen_string_field(default_value="abc", enable_partition_key=enable_partition_key, nullable=True),
            cf.gen_array_field(name=ct.default_int32_array_field_name, element_type=DataType.INT32, nullable=True),
            cf.gen_array_field(name=ct.default_float_array_field_name, element_type=DataType.FLOAT, nullable=True),
            cf.gen_array_field(name=ct.default_string_array_field_name, element_type=DataType.VARCHAR, max_length=100, nullable=True),
            cf.gen_json_field(name=json_embedded_object, nullable=True),
            cf.gen_float_vec_field()
        ]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        # default value fields, [] or [None]
        data = [
            [i for i in range(ct.default_nb)],
            default_value,
            default_value,
            default_value,
            [[np.int32(j) for j in range(10)] for _ in range(ct.default_nb)],
            [[np.float32(j) for j in range(10)] for _ in range(ct.default_nb)],
            default_value,
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        if auto_id:
            del data[0]
        collection_w.insert(data=data)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_insert_multi_fields_by_rows_using_default(self, enable_partition_key, nullable):
        """
        target: test insert multi fields by rows with default value
        method: 1. create a collection with one field using default value
                2. insert using default value to replace the field value
        expected: insert successfully
        """
        # 1. initialize with data
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(default_value=np.float32(3.14), nullable=nullable),
                  cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
                  cf.gen_json_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # 2. insert data
        array = cf.gen_default_rows_data()
        for i in range(0, ct.default_nb, 2):
            array[i][ct.default_string_field_name] = None
        collection_w.insert(array)

        exp = f"{ct.default_string_field_name} == 'abc'"
        res = collection_w.query(exp, output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb/2

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_multi_fields_by_rows_using_none(self):
        """
        target: test insert multi fields by rows with none value
        method: 1. create a collection with one field using none value
                2. insert using none to replace the field value
        expected: insert successfully
        """
        # 1. initialize with data
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(nullable=True),
                  cf.gen_string_field(default_value="abc", nullable=True), cf.gen_json_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # 2. insert data
        array = cf.gen_default_rows_data()
        for i in range(0, ct.default_nb, 2):
            array[i][ct.default_float_field_name] = None
            array[i][ct.default_string_field_name] = None
        collection_w.insert(array)

        exp = f"{ct.default_string_field_name} == 'abc'"
        res = collection_w.query(exp, output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb/2
        assert res[0][ct.default_float_field_name] is None

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_insert_dataframe_using_default_data(self, enable_partition_key, nullable):
        """
        target: test insert with dataframe
        method: insert with valid dataframe using default data
        expected: insert successfully
        """
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
                  cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
            "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
            "float_vector": vectors
        })
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_dataframe_using_none_data(self):
        """
        target: test insert with dataframe
        method: insert with valid dataframe using none data
        expected: insert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value=None, nullable=True), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
            "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
            "float_vector": vectors
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
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        future, _ = collection_w.insert(data=df, _async=True)
        future.done()
        mutation_res = future.result()
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist(
        )
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_async_false(self):
        """
        target: test insert with false async
        method: async = false
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        mutation_res, _ = collection_w.insert(data=df, _async=False)
        assert mutation_res.insert_count == ct.default_nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist(
        )
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_async_callback(self):
        """
        target: test insert with callback func
        method: insert with callback func
        expected: verify num entities
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        future, _ = collection_w.insert(
            data=df, _async=True, _callback=assert_mutation_result)
        future.done()
        mutation_res = future.result()
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist(
        )
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_long(self):
        """
        target: test insert with async
        method: insert 5w entities with callback func
        expected: verify num entities
        """
        nb = 50000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb)
        future, _ = collection_w.insert(data=df, _async=True)
        future.done()
        mutation_res = future.result()
        assert mutation_res.insert_count == nb
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist(
        )
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_callback_timeout(self):
        """
        target: test insert async with callback
        method: insert 10w entities with timeout=1
        expected: raise exception
        """
        nb = 100000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb)
        future, _ = collection_w.insert(
            data=df, _async=True, _callback=None, timeout=0.2)
        with pytest.raises(MilvusException):
            future.result()

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_invalid_data(self):
        """
        target: test insert async with invalid data
        method: insert async with invalid data
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix))
        columns = [ct.default_int64_field_name,
                   ct.default_float_vec_field_name]
        df = pd.DataFrame(columns=columns)
        error = {ct.err_code: 0,
                 ct.err_msg: "The fields don't match with schema fields"}
        collection_w.insert(data=df, _async=True,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_async_invalid_partition(self):
        """
        target: test insert async with invalid partition
        method: insert async with invalid partition
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        err_msg = "partition not found"
        future, _ = collection_w.insert(data=df, partition_name="p", _async=True)
        future.done()
        with pytest.raises(MilvusException, match=err_msg):
            future.result()


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
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        partition_name = cf.gen_unique_str(prefix)
        partition_w1 = self.init_partition_wrap(collection_w, partition_name)
        mutation_res, _ = collection_w.insert(
            data=df, partition_name=partition_w1.name)
        assert mutation_res.insert_count == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_binary_multi_times(self):
        """
        target: test insert entities multi times and final flush
        method: create collection and insert binary entity multi 
        expected: the collection row count equals to nb
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
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
        collection_w = self.init_collection_wrap(
            name=c_name, schema=default_binary_schema)
        df, _ = cf.gen_default_binary_dataframe_data(ct.default_nb)
        mutation_res, _ = collection_w.insert(data=df)
        assert mutation_res.insert_count == ct.default_nb
        default_index = {"index_type": "BIN_IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)


class TestInsertInvalid(TestcaseBase):
    """
      ******************************************************************
      The following cases are used to test insert invalid params
      ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_insert_with_invalid_field_value(self, primary_field):
        """
        target: verify error msg when inserting with invalid field value
        method: insert with invalid field value
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, auto_id=False, insert_data=False,
                                                    primary_field=primary_field, is_index=False,
                                                    is_all_data_type=True, with_json=True)[0]
        nb = 100
        data = cf.gen_data_by_collection_schema(collection_w.schema, nb=nb)
        for dirty_i in [0, nb // 2, nb - 1]:      # check the dirty data at first, middle and last
            log.debug(f"dirty_i: {dirty_i}")
            for i in range(len(data)):
                if data[i][dirty_i].__class__ is int:
                    tmp = data[i][0]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is str:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = random.randint(0, 1000)
                    error = {ct.err_code: 999, ct.err_msg: "field (varchar) expects string input, got: <class 'int'>"}
                    collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is bool:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is float:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                else:
                    continue
        res = collection_w.insert(data)[0]
        assert res.insert_count == nb

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
        error = {ct.err_code: 15, 'err_msg': "partition not found"}
        mutation_res, _ = collection_w.insert(data=df, partition_name="p", check_task=CheckTasks.err_res,
                                              check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_pk_varchar_auto_id_true(self):
        """
        target: test insert invalid with pk varchar and auto id true
        method: set pk varchar max length < 18, insert data
        expected: varchar pk supports auto_id=true
        """
        string_field = cf.gen_string_field(is_primary=True, max_length=6)
        embedding_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(
            [string_field, embedding_field], auto_id=True)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [[[random.random() for _ in range(ct.default_dim)]
                 for _ in range(2)]]
        res = collection_w.insert(data=data)[0]
        assert res.insert_count == 2

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_int8", [-129, 128])
    def test_insert_int8_overflow(self, invalid_int8):
        """
        target: test insert int8 out of range
        method: insert int8 out of range
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_all_data_type=True)[0]
        data = cf.gen_dataframe_all_data_type(nb=1)
        data[ct.default_int8_field_name] = [invalid_int8]
        error = {ct.err_code: 1100, ct.err_msg: f"the 0th element ({invalid_int8}) out of range: [-128, 127]"}
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_int16", [-32769, 32768])
    def test_insert_int16_overflow(self, invalid_int16):
        """
        target: test insert int16 out of range
        method: insert int16 out of range
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_all_data_type=True)[0]
        data = cf.gen_dataframe_all_data_type(nb=1)
        data[ct.default_int16_field_name] = [invalid_int16]
        error = {ct.err_code: 1100, ct.err_msg: f"the 0th element ({invalid_int16}) out of range: [-32768, 32767]"}
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_int32", [-2147483649, 2147483648])
    def test_insert_int32_overflow(self, invalid_int32):
        """
        target: test insert int32 out of range
        method: insert int32 out of range
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, is_all_data_type=True)[0]
        data = cf.gen_dataframe_all_data_type(nb=1)
        data[ct.default_int32_field_name] = [invalid_int32]
        error = {ct.err_code: 999, 'err_msg': "The Input data type is inconsistent with defined schema"}
        collection_w.insert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
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
        error = {ct.err_code: 999, ct.err_msg: "message larger than max"}
        collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], 123])
    def test_insert_rows_using_default_value(self, default_value):
        """
        target: test insert with rows
        method: insert with invalid rows
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [{"int64": 1, "float_vector": vectors[1],
                 "varchar": default_value, "float": np.float32(1.0)}]
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_tuple_using_default_value(self, default_value):
        """
        target: test insert with tuple
        method: insert with invalid tuple
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_vec_field(),
                  cf.gen_string_field(), cf.gen_float_field(default_value=np.float32(3.14))]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, vectors, string_values, default_value)
        error = {ct.err_code: 999, ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_nan_value(self):
        """
        target: test insert with nan value
        method: insert with nan value: None, float('nan'), np.NAN/np.nan, float('inf')
        expected: raise exception
        """
        vector_field = ct.default_float_vec_field_name
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)
        data = cf.gen_default_dataframe_data()
        data[vector_field][0][0] = None
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
        data[vector_field][0][0] = float('nan')
        error = {ct.err_code: 999, ct.err_msg: "value 'NaN' is not a number or infinity"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
        data[vector_field][0][0] = np.NAN
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)
        data[vector_field][0][0] = float('inf')
        error = {ct.err_code: 65535, ct.err_msg: "value '+Inf' is not a number or infinity"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index ", ct.all_index_types[9:11])
    @pytest.mark.parametrize("invalid_vector_type ", ["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def test_invalid_sparse_vector_data(self, index, invalid_vector_type):
        """
        target: insert illegal data type
        method: insert illegal data type
        expected: raise exception
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        nb = 100
        data = cf.gen_default_list_sparse_data(nb=nb)[:-1]
        invalid_vec = cf.gen_vectors(nb, dim=128, vector_data_type=invalid_vector_type)
        data.append(invalid_vec)
        error = {ct.err_code: 1, ct.err_msg: 'input must be a sparse matrix in supported format'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)


class TestInsertInvalidBinary(TestcaseBase):
    """
      ******************************************************************
      The following cases are used to test insert invalid params of binary
      ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_ids_binary_invalid(self):
        """
        target: test insert float vector into a collection with binary vector schema
        method: create collection and insert entities in it
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, auto_id=False, insert_data=False, is_binary=True,
                                                    is_index=False, with_json=False)[0]
        data = cf.gen_default_list_data(nb=100, with_json=False)
        error = {ct.err_code: 999, ct.err_msg: "Invalid binary vector data exists"}
        mutation_res, _ = collection_w.insert(
            data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_with_invalid_binary_partition_name(self):
        """
        target: test insert with invalid scenario
        method: insert with invalid partition name
        expected: raise exception
        """
        collection_w = self.init_collection_general(prefix, auto_id=False, insert_data=False, is_binary=True,
                                                    is_index=False, with_json=False)[0]
        partition_name = "non_existent_partition"
        df, _ = cf.gen_default_binary_dataframe_data(nb=100)
        error = {ct.err_code: 999, 'err_msg': f"partition not found[partition={partition_name}]"}
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
        assert mutation_res.primary_keys == data[2].tolist()

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("string_fields", [[cf.gen_string_field(name="string_field1")],
                                               [cf.gen_string_field(
                                                   name="string_field2")],
                                               [cf.gen_string_field(name="string_field3")]])
    def test_insert_multi_string_fields(self, string_fields):
        """
        target: test insert multi string fields
        method: 1.create a collection
                2.Insert multi string fields
        expected: Insert Successfully
        """
        schema = cf.gen_schema_multi_string_fields(string_fields)
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_string_fields(string_fields=string_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

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
        max = 65535
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=1)
            if field.dtype == DataType.VARCHAR:
                field_data = [cf.gen_str_by_length(length=max + 1)]
            data.append(field_data)

        error = {ct.err_code: 999, ct.err_msg: 'length of string exceeds max length'}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("str_field_value", ["", "    "])
    def test_insert_string_field_space_empty(self, str_field_value):
        """
        target: test create collection with string field 
        method: 1.create a collection  
                2.Insert string field  with space
        expected: Insert successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        nb = 100
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.dtype == DataType.VARCHAR:
                field_data = [str_field_value for _ in range(nb)]
            data.append(field_data)

        collection_w.insert(data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("str_field_value", ["", "    "])
    def test_insert_string_field_is_pk_and_empty(self, str_field_value):
        """
        target: test create collection with string field is primary
        method: 1.create a collection  
                2.Insert string field with empty, string field is pk
        expected: Insert successfully
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        nb = 100
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.dtype == DataType.VARCHAR:
                field_data = [str_field_value for _ in range(nb)]
            data.append(field_data)
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
    @pytest.mark.parametrize("start", [0, 1500, 3500])
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

    @pytest.mark.tags(CaseLabel.L0)
    def test_upsert_with_auto_id(self):
        """
        target: test upsert with auto id
        method: 1. create a collection with autoID=true
                2. upsert 10 entities with non-existing pks
                verify: success, and the pks are auto-generated
                3. query 10 entities to get the existing pks
                4. upsert 10 entities with existing pks
                verify: success, and the pks are re-generated, and the new pks are visibly
        """
        dim = 32
        collection_w, _, _, insert_ids, _ = self.init_collection_general(pre_upsert, auto_id=True,
                                                                         dim=dim, insert_data=True, with_json=False)
        nb = 10
        start = ct.default_nb * 10
        data = cf.gen_default_list_data(dim=dim, nb=nb, start=start, with_json=False)
        res_upsert1 = collection_w.upsert(data=data)[0]
        collection_w.flush()
        # assert the pks are auto-generated, and num_entities increased for upsert with non_existing pks
        assert res_upsert1.primary_keys[0] > insert_ids[-1]
        assert collection_w.num_entities == ct.default_nb + nb

        # query 10 entities to get the existing pks
        res_q = collection_w.query(expr='', limit=nb)[0]
        print(f"res_q: {res_q}")
        existing_pks = [res_q[i][ct.default_int64_field_name] for i in range(nb)]
        existing_count = collection_w.query(expr=f"{ct.default_int64_field_name} in {existing_pks}",
                                            output_fields=[ct.default_count_output])[0]
        assert nb == existing_count[0].get(ct.default_count_output)
        # upsert 10 entities with the existing pks
        start = ct.default_nb * 20
        data = cf.gen_default_list_data(dim=dim, nb=nb, start=start, with_json=False)
        data[0] = existing_pks
        res_upsert2 = collection_w.upsert(data=data)[0]
        collection_w.flush()
        # assert the new pks are auto-generated again
        assert res_upsert2.primary_keys[0] > res_upsert1.primary_keys[-1]
        existing_count = collection_w.query(expr=f"{ct.default_int64_field_name} in {existing_pks}",
                                            output_fields=[ct.default_count_output])[0]
        assert 0 == existing_count[0].get(ct.default_count_output)
        res_q = collection_w.query(expr=f"{ct.default_int64_field_name} in {res_upsert2.primary_keys}",
                                   output_fields=["*"])[0]
        assert nb == len(res_q)
        current_count = collection_w.query(expr='', output_fields=[ct.default_count_output])[0]
        assert current_count[0].get(ct.default_count_output) == ct.default_nb + nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_upsert_with_primary_key_string(self, auto_id):
        """
        target: test upsert with string primary key
        method: 1. create a collection with pk string
                2. insert data
                3. upsert data with ' ' before or after string
        expected: raise no exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        fields = [cf.gen_string_field(), cf.gen_float_vec_field(dim=ct.default_dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field=ct.default_string_field_name,
                                          auto_id=auto_id)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(2)]
        if not auto_id:
            collection_w.insert([["a", "b"], vectors])
            res_upsert = collection_w.upsert([[" a", "b  "], vectors])[0]
            assert res_upsert.primary_keys[0] == " a" and res_upsert.primary_keys[1] == "b  "
        else:
            collection_w.insert([vectors])
            res_upsert = collection_w.upsert([[" a", "b  "], vectors])[0]
            assert res_upsert.primary_keys[0] != " a" and res_upsert.primary_keys[1] != "b  "
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
        collection_w.upsert(data=None, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999,
                                         ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"})

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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_upsert_in_row_with_enable_dynamic_field(self, auto_id):
        """
        target: test upsert in rows when enable dynamic field is True
        method: 1. create a collection and insert data
                2. upsert in rows
        expected: upsert successfully
        """
        upsert_nb = ct.default_nb
        start = ct.default_nb // 2
        collection_w = self.init_collection_general(pre_upsert, insert_data=True, auto_id=auto_id,
                                                    enable_dynamic_field=True)[0]
        upsert_data = cf.gen_default_rows_data(start=start)
        for i in range(start, start + upsert_nb):
            upsert_data[i - start]["new"] = [i, i + 1]
        collection_w.upsert(data=upsert_data)
        expr = f"float >= {start} && float <= {upsert_nb + start}"
        extra_num = start if auto_id is True else 0  # upsert equals insert in this case if auto_id is True
        res = collection_w.query(expr=expr, output_fields=['count(*)'])[0]
        assert res[0].get('count(*)') == upsert_nb + extra_num
        res = collection_w.query(expr, output_fields=["new"])[0]
        assert len(res[upsert_nb + extra_num - 1]["new"]) == 2
        res = collection_w.query(expr="", output_fields=['count(*)'])[0]
        assert res[0].get('count(*)') == start + upsert_nb + extra_num

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for i in range(ct.default_nb)]])
    def test_upsert_one_field_using_default_value(self, default_value, nullable):
        """
        target: test insert/upsert with one field using default value
        method: 1. create a collection with one field using default value
                2. insert using default value to replace the field value []/[None]
        expected: insert/upsert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc", nullable=nullable), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        cf.insert_data(collection_w, with_json=False)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        # default value fields, [] or [None]
        data = [
            [i for i in range(ct.default_nb)],
            [np.float32(i) for i in range(ct.default_nb)],
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.upsert(data)
        exp = f"{ct.default_string_field_name} == 'abc'"
        res = collection_w.query(exp, output_fields=[default_float_name])[0]
        assert len(res) == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("default_value", [[], [None for _ in range(ct.default_nb)]])
    def test_upsert_multi_fields_using_none_data(self, enable_partition_key, default_value):
        """
        target: test insert/upsert with multi fields include array using none value
        method: 1. create a collection with multi fields include array using default value
                2. insert using none value to replace the field value
        expected: insert/upsert successfully
        """
        json_embedded_object = "json_embedded_object"
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_int32_field(default_value=np.int32(1), nullable=True),
            cf.gen_float_field(default_value=np.float32(1.0), nullable=True),
            cf.gen_string_field(default_value="abc", enable_partition_key=enable_partition_key, nullable=True),
            cf.gen_array_field(name=ct.default_int32_array_field_name, element_type=DataType.INT32, nullable=True),
            cf.gen_array_field(name=ct.default_float_array_field_name, element_type=DataType.FLOAT, nullable=True),
            cf.gen_array_field(name=ct.default_string_array_field_name, element_type=DataType.VARCHAR,
                               max_length=100, nullable=True),
            cf.gen_json_field(name=json_embedded_object, nullable=True),
            cf.gen_float_vec_field()
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        # insert data and load collection
        data = [
            [i for i in range(ct.default_nb)],
            default_value,
            [np.float32(2.0) for _ in range(ct.default_nb)],
            [str(i) for i in range(ct.default_nb)],
            [[np.int32(j) for j in range(10)] for _ in range(ct.default_nb)],
            [[np.float32(j) for j in range(10)] for _ in range(ct.default_nb)],
            [[str(j) for j in range(10)] for _ in range(ct.default_nb)],
            cf.gen_json_data_for_diff_json_types(nb=ct.default_nb, start=0, json_type=json_embedded_object),
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.insert(data=data)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        # default value fields, [] or [None]
        data = [
            [i for i in range(ct.default_nb)],
            default_value,
            default_value,
            default_value,
            [[np.int32(j) for j in range(10)] for _ in range(ct.default_nb)],
            [[np.float32(j) for j in range(10)] for _ in range(ct.default_nb)],
            default_value,
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.upsert(data=data)
        exp = f"{ct.default_float_field_name} == {np.float32(1.0)}"
        res = collection_w.query(exp, output_fields=[default_float_name, json_embedded_object,
                                                     ct.default_string_array_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][json_embedded_object] is None
        assert res[0][ct.default_string_array_field_name] is None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_upsert_multi_fields_by_rows_using_default(self, enable_partition_key, nullable):
        """
        target: test upsert multi fields by rows with default value
        method: 1. create a collection with one field using default value
                2. upsert using default value to replace the field value
        expected: upsert successfully
        """
        # 1. initialize with data
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(default_value=np.float32(3.14), nullable=nullable),
                  cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
                  cf.gen_json_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # 2. upsert data
        array = cf.gen_default_rows_data()
        for i in range(0, ct.default_nb, 2):
            array[i][ct.default_float_field_name] = None
            array[i][ct.default_string_field_name] = None
        collection_w.upsert(array)

        exp = f"{ct.default_float_field_name} == {np.float32(3.14)} and {ct.default_string_field_name} == 'abc'"
        res = collection_w.query(exp, output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb/2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    def test_upsert_multi_fields_by_rows_using_none(self,enable_partition_key):
        """
        target: test insert/upsert multi fields by rows with none value
        method: 1. create a collection with one field using none value
                2. insert/upsert using none to replace the field value
        expected: insert/upsert successfully
        """
        # 1. initialize with data
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(nullable=True),
                  cf.gen_string_field(default_value="abc", nullable=True, enable_partition_key=enable_partition_key),
                  cf.gen_json_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()

        # 2. insert data
        array = cf.gen_default_rows_data()
        for i in range(1, ct.default_nb, 2):
            array[i][ct.default_float_field_name] = None
            array[i][ct.default_string_field_name] = None
        collection_w.insert(array)

        for i in range(0, ct.default_nb, 2):
            array[i][ct.default_float_field_name] = None
            array[i][ct.default_string_field_name] = None
        collection_w.upsert(array)

        exp = f"{ct.default_int64_field_name} >= 0"
        res = collection_w.query(exp, output_fields=[ct.default_float_field_name, ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][ct.default_float_field_name] is None

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_upsert_dataframe_using_default_data(self, enable_partition_key, nullable):
        """
        target: test upsert with dataframe
        method: upsert with valid dataframe using default data
        expected: upsert successfully
        """
        if enable_partition_key is True and nullable is True:
            pytest.skip("partition key field not support nullable")
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
                  cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
            "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
            "float_vector": vectors
        })
        collection_w.upsert(df)
        exp = f"{ct.default_string_field_name} == 'abc'"
        res = collection_w.query(exp, output_fields=[ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_dataframe_using_none_data(self):
        """
        target: test upsert with dataframe
        method: upsert with valid dataframe using none data
        expected: upsert successfully
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value=None, nullable=True),
                  cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame({
            "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
            "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
            "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
            "float_vector": vectors
        })
        collection_w.upsert(df)
        exp = f"{ct.default_int64_field_name} >= 0"
        res = collection_w.query(exp, output_fields=[ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][ct.default_string_field_name] is None
        exp = f"{ct.default_string_field_name} == ''"
        res = collection_w.query(exp, output_fields=[ct.default_string_field_name])[0]
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index ", ct.all_index_types[9:11])
    def test_upsert_sparse_data(self, index):
        """
        target: multiple upserts and counts(*)
        method: multiple upserts and counts(*)
        expected: number of data entries normal
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        data = cf.gen_default_list_sparse_data(nb=ct.default_nb)
        collection_w.upsert(data=data)
        assert collection_w.num_entities == ct.default_nb
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)
        collection_w.load()
        for i in range(5):
            collection_w.upsert(data=data)
            collection_w.query(expr=f'{ct.default_int64_field_name} >= 0', output_fields=[ct.default_count_output]
                                        , check_task=CheckTasks.check_query_results,
                                         check_items={"exp_res": [{"count(*)": ct.default_nb}]})


class TestUpsertInvalid(TestcaseBase):
    """ Invalid test case of Upsert interface """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_upsert_data_type_dismatch(self, primary_field):
        """
        target: test upsert with invalid data type
        method: upsert data type string, set, number, float...
        expected: raise exception
        """
        collection_w = self.init_collection_general(pre_upsert, auto_id=False, insert_data=False,
                                                    primary_field=primary_field, is_index=False,
                                                    is_all_data_type=True, with_json=True)[0]
        nb = 100
        data = cf.gen_data_by_collection_schema(collection_w.schema, nb=nb)
        for dirty_i in [0, nb // 2, nb - 1]:  # check the dirty data at first, middle and last
            log.debug(f"dirty_i: {dirty_i}")
            for i in range(len(data)):
                if data[i][dirty_i].__class__ is int:
                    tmp = data[i][0]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is str:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = random.randint(0, 1000)
                    error = {ct.err_code: 999, ct.err_msg: "field (varchar) expects string input, got: <class 'int'>"}
                    collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is bool:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                elif data[i][dirty_i].__class__ is float:
                    tmp = data[i][dirty_i]
                    data[i][dirty_i] = "iamstring"
                    error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
                    collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)
                    data[i][dirty_i] = tmp
                else:
                    continue
        res = collection_w.upsert(data)[0]
        assert res.insert_count == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_vector_unmatch(self):
        """
        target: test upsert with unmatched data vector
        method: 1. create a collection with dim=128
                2. upsert with vector dim unmatch
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name, with_json=False)
        data = cf.gen_default_binary_dataframe_data()[0]
        error = {ct.err_code: 999,
                 ct.err_msg: "The name of field doesn't match, expected: float_vector, got binary_vector"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [128-8, 128+8])
    def test_upsert_binary_dim_unmatch(self, dim):
        """
        target: test upsert with unmatched vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim
        expected: raise exception
        """
        collection_w = self.init_collection_general(pre_upsert, True, is_binary=True)[0]
        data = cf.gen_default_binary_dataframe_data(dim=dim)[0]
        error = {ct.err_code: 1100,
                 ct.err_msg: f"the dim ({dim}) of field data(binary_vector) is not equal to schema dim ({ct.default_dim})"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [256])
    def test_upsert_dim_unmatch(self, dim):
        """
        target: test upsert with unmatched vector dim
        method: 1. create a collection with default dim 128
                2. upsert with mismatched dim
        expected: raise exception
        """
        nb = 10
        collection_w = self.init_collection_general(pre_upsert, True, with_json=False)[0]
        data = cf.gen_default_list_data(nb=nb, dim=dim, with_json=False)
        error = {ct.err_code: 1100,
                 ct.err_msg: f"the dim ({dim}) of field data(float_vector) is not equal to schema dim ({ct.default_dim})"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ct.invalid_resource_names[4:])
    def test_upsert_partition_name_non_existing(self, partition_name):
        """
        target: test upsert partition name invalid
        method: 1. create a collection with partitions
                2. upsert with invalid partition name
        expected: raise exception
        """
        c_name = cf.gen_unique_str(pre_upsert)
        collection_w = self.init_collection_wrap(name=c_name)
        p_name = cf.gen_unique_str('partition_')
        collection_w.create_partition(p_name)
        cf.insert_data(collection_w)
        data = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 999, ct.err_msg: "Invalid partition name"}
        if partition_name == "n-ame":
            error = {ct.err_code: 999, ct.err_msg: f"partition not found[partition={partition_name}]"}
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
        error = {ct.err_code: 200, ct.err_msg: f"partition not found[partition={partition_name}]"}
        collection_w.upsert(data=data, partition_name=partition_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("insert and upsert have removed the [] error check")
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
        error = {ct.err_code: 999, ct.err_msg: "['partition_1', 'partition_2'] has type <class 'list'>, "
                                               "but expected one of: (<class 'bytes'>, <class 'str'>)"}
        collection_w.upsert(data=data, partition_name=["partition_1", "partition_2"],
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_upsert_with_auto_id_pk_type_dismacth(self):
        """
        target: test upsert with auto_id and pk type dismatch
        method: 1. create a collection with pk int64 and auto_id=True
                2. upsert with pk string type dismatch
        expected: raise exception
        """
        dim = 16
        collection_w = self.init_collection_general(pre_upsert, auto_id=False,
                                                    dim=dim, insert_data=True, with_json=False)[0]
        nb = 10
        data = cf.gen_default_list_data(dim=dim, nb=nb, with_json=False)
        data[0] = [str(i) for i in range(nb)]
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.upsert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], 123])
    def test_upsert_rows_using_default_value(self, default_value):
        """
        target: test upsert with rows
        method: upsert with invalid rows
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(),
                  cf.gen_string_field(default_value="abc"), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        data = [{"int64": 1, "float_vector": vectors[1],
                 "varchar": default_value, "float": np.float32(1.0)}]
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_tuple_using_default_value(self, default_value):
        """
        target: test upsert with tuple
        method: upsert with invalid tuple
        expected: raise exception
        """
        fields = [cf.gen_int64_field(is_primary=True), cf.gen_float_field(default_value=np.float32(3.14)),
                  cf.gen_string_field(), cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, default_value, string_values, vectors)
        error = {ct.err_code: 999, ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)


class TestInsertArray(TestcaseBase):
    """ Test case of Insert array """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_insert_array_dataframe(self, auto_id):
        """
        target: test insert DataFrame data
        method: Insert data in the form of dataframe
        expected: assert num entities
        """
        schema = cf.gen_array_collection_schema(auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = cf.gen_array_dataframe_data()
        if auto_id:
            data = data.drop(ct.default_int64_field_name, axis=1)
        collection_w.insert(data=data)
        collection_w.flush()
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_insert_array_list(self, auto_id):
        """
        target: test insert list data
        method: Insert data in the form of a list
        expected: assert num entities
        """
        schema = cf.gen_array_collection_schema(auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)

        nb = ct.default_nb
        arr_len = ct.default_max_capacity
        pk_values = [i for i in range(nb)]
        float_vec = cf.gen_vectors(nb, ct.default_dim)
        int32_values = [[np.int32(j) for j in range(i, i+arr_len)] for i in range(nb)]
        float_values = [[np.float32(j) for j in range(i, i+arr_len)] for i in range(nb)]
        string_values = [[str(j) for j in range(i, i+arr_len)] for i in range(nb)]

        data = [pk_values, float_vec, int32_values, float_values, string_values]
        if auto_id:
            del data[0]
        # log.info(data[0][1])
        collection_w.insert(data=data)
        assert collection_w.num_entities == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_array_rows(self):
        """
        target: test insert row data
        method: Insert data in the form of rows
        expected: assert num entities
        """
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        data = cf.gen_row_data_by_schema(schema=schema)
        collection_w.insert(data=data)
        assert collection_w.num_entities == ct.default_nb

        collection_w.upsert(data[:2])

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_array_empty_list(self):
        """
        target: test insert DataFrame data
        method: Insert data with the length of array = 0
        expected: assert num entities
        """
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        data = cf.gen_array_dataframe_data()
        data[ct.default_int32_array_field_name] = [[] for _ in range(nb)]
        collection_w.insert(data=data)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_array_length_differ(self):
        """
        target: test insert row data
        method: Insert data with every row's array length differ
        expected: assert num entities
        """
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)
        array = []
        for i in range(nb):
            arr_len1 = random.randint(0, ct.default_max_capacity)
            arr_len2 = random.randint(0, ct.default_max_capacity)
            arr = {
                ct.default_int64_field_name: i,
                ct.default_float_vec_field_name: [random.random() for _ in range(ct.default_dim)],
                ct.default_int32_array_field_name: [np.int32(j) for j in range(arr_len1)],
                ct.default_float_array_field_name: [np.float32(j) for j in range(arr_len2)],
                ct.default_string_array_field_name: [str(j) for j in range(ct.default_max_capacity)],
            }
            array.append(arr)

        collection_w.insert(array)
        assert collection_w.num_entities == nb

        data = cf.gen_row_data_by_schema(nb=2, schema=schema)
        collection_w.upsert(data)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_array_length_invalid(self):
        """
        target: Insert actual array length > max_capacity
        method: Insert actual array length > max_capacity
        expected: raise error
        """
        # init collection
        schema = cf.gen_array_collection_schema(dim=32)
        collection_w = self.init_collection_wrap(schema=schema)
        # Insert actual array length > max_capacity
        arr_len = ct.default_max_capacity + 1
        data = cf.gen_row_data_by_schema(schema=schema, nb=11)
        data[1][ct.default_float_array_field_name] = [np.float32(i) for i in range(arr_len)]
        err_msg = (f"the length ({arr_len}) of 1th array exceeds max capacity ({ct.default_max_capacity})")
        collection_w.insert(data=data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1100, ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_array_type_invalid(self):
        """
        target: Insert array type invalid
        method: 1. Insert string values to an int array
                2. upsert float values to a string array
        expected: raise error
        """
        # init collection
        arr_len = 5
        nb = 10
        dim = 8
        schema = cf.gen_array_collection_schema(dim=dim)
        collection_w = self.init_collection_wrap(schema=schema)
        data = cf.gen_row_data_by_schema(schema=schema, nb=nb)
        # 1. Insert string values to an int array
        data[1][ct.default_int32_array_field_name] = [str(i) for i in range(arr_len)]
        err_msg = "The Input data type is inconsistent with defined schema"
        collection_w.insert(data=data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999, ct.err_msg: err_msg})

        # 2. upsert float values to a string array
        data = cf.gen_row_data_by_schema(schema=schema)
        data[1][ct.default_string_array_field_name] = [np.float32(i) for i in range(arr_len)]
        collection_w.upsert(data=data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999, ct.err_msg: err_msg})

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_array_mixed_value(self):
        """
        target: Insert array consisting of mixed values
        method: Insert array consisting of mixed values
        expected: raise error
        """
        # init collection
        schema = cf.gen_array_collection_schema(dim=32)
        collection_w = self.init_collection_wrap(schema=schema)
        # Insert array consisting of mixed values
        data = cf.gen_row_data_by_schema(schema=schema, nb=10)
        data[1][ct.default_string_array_field_name] = ["a", 1, [2.0, 3.0], False]
        collection_w.insert(data=data, check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999,
                                         ct.err_msg: "The Input data type is inconsistent with defined schema"})
