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
default_binary_index_params = ct.default_binary_index
default_search_exp = "int64 >= 0"


class TestInsertParams(TestcaseBase):
    """Test case of Insert interface"""

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
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

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
        error = {ct.err_code: 999, ct.err_msg: "The fields don't match with schema fields"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

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
        df.rename(columns={ct.default_int64_field_name: " "}, inplace=True)
        error = {ct.err_code: 999, ct.err_msg: "The name of field doesn't match, expected: int64"}
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

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
        df.rename(columns={ct.default_int64_field_name: invalid_field_name}, inplace=True)
        error = {
            ct.err_code: 999,
            ct.err_msg: f"The name of field doesn't match, expected: int64, got {invalid_field_name}",
        }
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 999,
            ct.err_msg: f"Collection field dim is {ct.default_dim}, but entities field dim is {dim}",
        }
        collection_w.insert(data=df, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 999,
            ct.err_msg: "The data type of field float doesn't match, expected: FLOAT, got DOUBLE",
        }
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
        data = []
        for fields in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(fields, nb=nb)
            if fields.dtype == DataType.FLOAT_VECTOR:
                field_data = field_data[:-1]
            data.append(field_data)
        error = {ct.err_code: 999, ct.err_msg: "Field data size misaligned for field [float_vector] "}
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
        nb = 10
        data = []
        for fields in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(fields, nb=nb)
            data.append(field_data)
        data.append([1 for _ in range(nb)])
        error = {ct.err_code: 999, ct.err_msg: "The data doesn't match with schema fields"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

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
        data = []
        for field in collection_w.schema.fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            data.append(field_data)
        tmp = data[0]
        data[0] = data[1]
        data[1] = tmp
        error = {ct.err_code: 999, ct.err_msg: "The Input data type is inconsistent with defined schema"}
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)


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
    def test_insert_with_no_vector_field_dtype(self):
        """
        target: test insert entities, with no vector field
        method: vector field is missing in data
        expected: error raised
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        nb = 10
        data = []
        fields = collection_w.schema.fields
        for field in fields:
            field_data = cf.gen_data_by_collection_field(field, nb=nb)
            if field.dtype != DataType.FLOAT_VECTOR:
                data.append(field_data)
        error = {
            ct.err_code: 999,
            ct.err_msg: f"The data doesn't match with schema fields, expect {len(fields)} list, got {len(data)}",
        }
        collection_w.insert(data=data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_twice_auto_id_true(self, pk_field):
        """
        target: test insert ids fields twice when auto_id=True
        method: 1.create collection with auto_id=True 2.insert twice
        expected: verify primary_keys unique
        """
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_collection_schema(primary_field=pk_field, auto_id=True)
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
        schema = cf.gen_default_collection_schema(primary_field=pk_field, auto_id=True)
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
        nb = 100
        data = cf.gen_column_data_by_schema(nb=nb, schema=collection_w.schema)

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
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(ct.default_nb)
        thread_num = 4
        threads = []
        primary_keys = df[ct.default_int64_field_name].values.tolist()

        def insert(thread_i):
            log.debug(f"In thread-{thread_i}")
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
            assert mutation_res.primary_keys == df[ct.default_int64_field_name].values.tolist()

        assert collection_w.num_entities == nb

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
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(),
            cf.gen_string_field(default_value="abc", nullable=nullable),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        # default value fields, [] or [None]
        data = [
            [i for i in range(ct.default_nb)],
            [np.float32(i) for i in range(ct.default_nb)],
            default_value,
            cf.gen_vectors(ct.default_nb, ct.default_dim),
        ]
        if auto_id:
            del data[0]
        collection_w.insert(data)
        assert collection_w.num_entities == ct.default_nb

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
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(),
            cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame(
            {
                "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
                "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
                "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
                "float_vector": vectors,
            }
        )
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_dataframe_using_none_data(self):
        """
        target: test insert with dataframe
        method: insert with valid dataframe using none data
        expected: insert successfully
        """
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(),
            cf.gen_string_field(default_value=None, nullable=True),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame(
            {
                "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
                "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
                "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
                "float_vector": vectors,
            }
        )
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb


class TestInsertAsync(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert async
    ******************************************************************
    """

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


def assert_mutation_result(mutation_res):
    assert mutation_res.insert_count == ct.default_nb


class TestInsertInvalid(TestcaseBase):
    """
    ******************************************************************
    The following cases are used to test insert invalid params
    ******************************************************************
    """

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
        error = {ct.err_code: 15, "err_msg": "partition not found"}
        mutation_res, _ = collection_w.insert(
            data=df, partition_name="p", check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_insert_tuple_using_default_value(self, default_value):
        """
        target: test insert with tuple
        method: insert with invalid tuple
        expected: raise exception
        """
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_string_field(),
            cf.gen_float_field(default_value=np.float32(3.14)),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, vectors, string_values, default_value)
        error = {ct.err_code: 999, ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)


class TestUpsertValid(TestcaseBase):
    """Valid test case of Upsert interface"""

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
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(),
            cf.gen_string_field(default_value="abc", is_partition_key=enable_partition_key, nullable=nullable),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame(
            {
                "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
                "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
                "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
                "float_vector": vectors,
            }
        )
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
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(),
            cf.gen_string_field(default_value=None, nullable=True),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        collection_w.load()
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)

        df = pd.DataFrame(
            {
                "int64": pd.Series(data=[i for i in range(ct.default_nb)]),
                "float": pd.Series(data=[float(i) for i in range(ct.default_nb)], dtype="float32"),
                "varchar": pd.Series(data=[None for _ in range(ct.default_nb)]),
                "float_vector": vectors,
            }
        )
        collection_w.upsert(df)
        exp = f"{ct.default_int64_field_name} >= 0"
        res = collection_w.query(exp, output_fields=[ct.default_string_field_name])[0]
        assert len(res) == ct.default_nb
        assert res[0][ct.default_string_field_name] is None
        exp = f"{ct.default_string_field_name} == ''"
        res = collection_w.query(exp, output_fields=[ct.default_string_field_name])[0]
        assert len(res) == 0


class TestUpsertInvalid(TestcaseBase):
    """Invalid test case of Upsert interface"""

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
        p_name = cf.gen_unique_str("partition_")
        collection_w.create_partition(p_name)
        cf.insert_data(collection_w)
        data = cf.gen_default_dataframe_data(nb=100)
        error = {ct.err_code: 999, ct.err_msg: "Invalid partition name"}
        if partition_name == "n-ame":
            error = {ct.err_code: 999, ct.err_msg: f"partition not found[partition={partition_name}]"}
        collection_w.upsert(data=data, partition_name=partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        collection_w.upsert(data=data, partition_name=partition_name, check_task=CheckTasks.err_res, check_items=error)

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
        error = {
            ct.err_code: 999,
            ct.err_msg: "['partition_1', 'partition_2'] has type <class 'list'>, "
            "but expected one of: (<class 'bytes'>, <class 'str'>)",
        }
        collection_w.upsert(
            data=data, partition_name=["partition_1", "partition_2"], check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("default_value", [[], None])
    def test_upsert_tuple_using_default_value(self, default_value):
        """
        target: test upsert with tuple
        method: upsert with invalid tuple
        expected: raise exception
        """
        fields = [
            cf.gen_int64_field(is_primary=True),
            cf.gen_float_field(default_value=np.float32(3.14)),
            cf.gen_string_field(),
            cf.gen_float_vec_field(),
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        vectors = cf.gen_vectors(ct.default_nb, ct.default_dim)
        int_values = [i for i in range(0, ct.default_nb)]
        string_values = ["abc" for i in range(ct.default_nb)]
        data = (int_values, default_value, string_values, vectors)
        error = {ct.err_code: 999, ct.err_msg: "The type of data should be List, pd.DataFrame or Dict"}
        collection_w.upsert(data, check_task=CheckTasks.err_res, check_items=error)


class TestInsertArray(TestcaseBase):
    """Test case of Insert array"""

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
        int32_values = [[np.int32(j) for j in range(i, i + arr_len)] for i in range(nb)]
        float_values = [[np.float32(j) for j in range(i, i + arr_len)] for i in range(nb)]
        string_values = [[str(j) for j in range(i, i + arr_len)] for i in range(nb)]

        data = [pk_values, float_vec, int32_values, float_values, string_values]
        if auto_id:
            del data[0]
        # log.info(data[0][1])
        collection_w.insert(data=data)
        assert collection_w.num_entities == nb
