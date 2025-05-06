import utils.util_pymilvus as ut
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common.text_generator import KoreanTextGenerator, ICUTextGenerator
from common.code_mapping import ConnectionErrorMessage as cem
from base.client_base import TestcaseBase
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_EVENTUALLY
from pymilvus import (
    FieldSchema,
    CollectionSchema,
    DataType,
)
import threading
from pymilvus import DefaultConfig
import time
import pytest
import random
import numpy as np
import pandas as pd
from collections import Counter
from faker import Faker

Faker.seed(19530)

fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")
fake_de = Faker("de_DE")
fake_jp = Faker("ja_JP")
fake_ko = Faker("ko_KR")



# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "query"
exp_res = "exp_res"
count = "count(*)"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'
default_mix_expr = "int64 >= 0 && varchar >= \"0\""
default_expr = f'{ct.default_int64_field_name} >= 0'
default_invalid_expr = "varchar >= 0"
default_string_term_expr = f'{ct.default_string_field_name} in [\"0\", \"1\"]'
default_index_params = ct.default_index
binary_index_params = ct.default_binary_index

default_entities = ut.gen_entities(ut.default_nb, is_normal=True)
default_pos = 5
json_field = ct.default_json_field_name
default_int_field_name = ct.default_int64_field_name
default_float_field_name = "float"
default_string_field_name = "varchar"


class TestQueryParams(TestcaseBase):
    """
    test Query interface
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_invalid(self):
        """
        target: test query with invalid term expression
        method: query with invalid term expr
        expected: raise exception
        """
        collection_w, entities = self.init_collection_general(prefix, insert_data=True, nb=10)[0:2]
        term_expr = f'{default_int_field_name} in {entities[:default_pos]}'
        error = {ct.err_code: 999, ct.err_msg: "cannot parse expression: int64 in"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

        # check missing the template variable
        expr = "int64 in {value_0}"
        expr_params = {"value_1": [0, 1]}
        error = {ct.err_code: 999, ct.err_msg: "the value of expression template variable name {value_0} is not found"}
        collection_w.query(expr=expr, expr_params=expr_params,
                           check_task=CheckTasks.err_res, check_items=error)

        # check the template variable type dismatch
        expr = "int64 in {value_0}"
        expr_params = {"value_0": 1}
        error = {ct.err_code: 999, ct.err_msg: "the value of term expression template variable {value_0} is not array"}
        collection_w.query(expr=expr, expr_params=expr_params,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query(self, enable_dynamic_field):
        """
        target: test query
        method: query with term expr
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field)[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values = []
            for vector in vectors[0]:
                vector = vector[ct.default_int64_field_name]
                int_values.append(vector)
            res = [{ct.default_int64_field_name: int_values[i]} for i in range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :1].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_no_collection(self):
        """
        target: test the scenario which query the non-exist collection
        method: 1. create collection
                2. drop collection
                3. query the dropped collection
        expected: raise exception and report the error
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(prefix)[0]
        # 2. Drop collection
        log.info("test_query_no_collection: drop collection %s" %
                 collection_w.name)
        collection_w.drop()
        # 3. Search without collection
        log.info("test_query_no_collection: query without collection ")
        collection_w.query(default_term_expr,
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 1,
                                        "err_msg": "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_empty_collection(self):
        """
        target: test query empty collection
        method: query on an empty collection
        expected: empty result
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res, _ = collection_w.query(default_term_expr)
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_auto_id_collection(self):
        """
        target: test query with auto_id=True collection
        method: test query with auto id
        expected: query result is correct
        """
        self._connect()
        df = cf.gen_default_dataframe_data()
        df[ct.default_int64_field_name] = None
        insert_res, _, = self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                                       primary_field=ct.default_int64_field_name,
                                                                       auto_id=True)
        assert self.collection_wrap.num_entities == ct.default_nb
        ids = insert_res[1].primary_keys
        pos = 5
        res = df.iloc[:pos, :1].to_dict('records')
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # query with all primary keys
        term_expr_1 = f'{ct.default_int64_field_name} in {ids[:pos]}'
        for i in range(5):
            res[i][ct.default_int64_field_name] = ids[i]
        self.collection_wrap.query(term_expr_1,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

        # query with part primary keys
        term_expr_2 = f'{ct.default_int64_field_name} in {[ids[0], 0]}'
        self.collection_wrap.query(term_expr_2, check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res[:1], "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2, 3])
    @pytest.mark.parametrize("dim", [8, 128])
    def test_query_with_dup_primary_key(self, dim, dup_times):
        """
        target: test query with duplicate primary key
        method: 1.insert same data twice
                2.search
        expected: query results are de-duplicated
        """
        nb = ct.default_nb
        collection_w, insert_data, _, _ = self.init_collection_general(prefix, True, nb, dim=dim)[0:4]
        # insert dup data multi times
        for i in range(dup_times):
            collection_w.insert(insert_data[0])
        # query
        res, _ = collection_w.query(default_term_expr)
        # assert that query results are de-duplicated
        res = [m["int64"] for m in res]
        assert sorted(list(set(res))) == sorted(res)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_auto_id_not_existed_primary_values(self):
        """
        target: test query on auto_id true collection
        method: 1.create auto_id true collection
                2.query with not existed primary keys
        expected: query result is empty
        """
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_default_dataframe_data(ct.default_nb)
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        mutation_res, _ = collection_w.insert(data=df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in [0, 1, 2]'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_none(self):
        """
        target: test query with none expr
        method: query with expr None
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        error = {ct.err_code: 0, ct.err_msg: "The type of expr must be string"}
        collection_w.query(None, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_non_string_expr(self):
        """
        target: test query with non-string expr
        method: query with non-string expr, eg 1, [] ..
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        exprs = [1, 2., [], {}, ()]
        error = {ct.err_code: 0, ct.err_msg: "The type of expr must be string"}
        for expr in exprs:
            collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_invalid_string(self):
        """
        target: test query with invalid expr
        method: query with invalid string expr
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        error = {ct.err_code: 1100, ct.err_msg: "cannot parse expression"}
        exprs = ["12-s", "中文", "a"]
        for expr in exprs:
            collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="repeat with test_query, waiting for other expr")
    def test_query_expr_term(self):
        """
        target: test query with TermExpr
        method: query with TermExpr
        expected: query result is correct
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res = vectors[0].iloc[:2, :1].to_dict('records')
        collection_w.query(default_term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_not_existed_field(self):
        """
        target: test query with not existed field
        method: query by term expr with fake field
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        term_expr = 'field in [1, 2]'
        error = {ct.err_code: 65535,
                 ct.err_msg: "cannot parse expression: field in [1, 2], error: field field not exist"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_non_primary_fields(self):
        """
        target: test query on non-primary non-vector fields
        method: query on non-primary non-vector fields
        expected: verify query result
        """
        self._connect()
        # construct dataframe and inert data
        df = pd.DataFrame({
            ct.default_int64_field_name: pd.Series(data=[i for i in range(ct.default_nb)]),
            ct.default_int32_field_name: pd.Series(data=[np.int32(i) for i in range(ct.default_nb)], dtype="int32"),
            ct.default_int16_field_name: pd.Series(data=[np.int16(i) for i in range(ct.default_nb)], dtype="int16"),
            ct.default_float_field_name: pd.Series(data=[np.float32(i) for i in range(ct.default_nb)], dtype="float32"),
            ct.default_double_field_name: pd.Series(data=[np.double(i) for i in range(ct.default_nb)], dtype="double"),
            ct.default_string_field_name: pd.Series(data=[str(i) for i in range(ct.default_nb)], dtype="string"),
            ct.default_float_vec_field_name: cf.gen_vectors(ct.default_nb, ct.default_dim)
        })
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # query by non_primary non_vector scalar field
        non_primary_field = [ct.default_int32_field_name, ct.default_int16_field_name,
                             ct.default_float_field_name, ct.default_double_field_name, ct.default_string_field_name]

        # exp res: first two rows and all fields expect last vec field
        res = df.iloc[:2, :].to_dict('records')
        for field in non_primary_field:
            filter_values = df[field].tolist()[:2]
            if field is not ct.default_string_field_name:
                term_expr = f'{field} in {filter_values}'
            else:
                term_expr = f'{field} in {filter_values}'
                term_expr = term_expr.replace("'", "\"")
            log.info(res)
            self.collection_wrap.query(term_expr, output_fields=["*"],
                                       check_task=CheckTasks.check_query_results,
                                       check_items={exp_res: res, "with_vec": True,
                                                    "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_by_bool_field(self):
        """
        target: test query by bool field and output bool field
        method: 1.create and insert with [int64, float, bool, float_vec] fields
                2.query by bool field, and output all int64, bool fields
        expected: verify query result and output fields
        """
        self._connect()
        df = cf.gen_default_dataframe_data()
        bool_values = pd.Series(data=[True if i % 2 == 0 else False for i in range(ct.default_nb)], dtype="bool")
        df.insert(2, ct.default_bool_field_name, bool_values)
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # output bool field

        res, _ = self.collection_wrap.query(default_term_expr, output_fields=[ct.default_bool_field_name])
        assert set(res[0].keys()) == {ct.default_int64_field_name, ct.default_bool_field_name}

        # not support filter bool field with expr 'bool in [0/ 1]'
        not_support_expr = f'{ct.default_bool_field_name} in [0]'
        error = {ct.err_code: 65535,
                 ct.err_msg: "cannot parse expression: bool in [0], error: "
                             "value 'int64_val:0' in list cannot be casted to Bool"}
        self.collection_wrap.query(not_support_expr, output_fields=[ct.default_bool_field_name],
                                   check_task=CheckTasks.err_res, check_items=error)

        # filter bool field by bool term expr
        for bool_value in [True, False]:
            exprs = [f'{ct.default_bool_field_name} in [{bool_value}]',
                     f'{ct.default_bool_field_name} == {bool_value}']
            for expr in exprs:
                res, _ = self.collection_wrap.query(expr, output_fields=[ct.default_bool_field_name])
                assert len(res) == ct.default_nb / 2
                for _r in res:
                    assert _r[ct.default_bool_field_name] == bool_value

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_by_int64(self):
        """
        target: test query through int64 field and output int64 field
        method: use int64 as query expr parameter
        expected: verify query output number
        """
        self._connect()
        df = cf.gen_default_dataframe_data(nb=ct.default_nb * 10)
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb * 10
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # filter on int64 fields
        expr_list = [f'{ct.default_int64_field_name} > 8192 && {ct.default_int64_field_name} < 8194',
                     f'{ct.default_int64_field_name} > 16384 && {ct.default_int64_field_name} < 16386']
        for expr in expr_list:
            res, _ = self.collection_wrap.query(expr, output_fields=[ct.default_int64_field_name])
            assert len(res) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_by_int8_field(self):
        """
        target: test query by int8 field
        method: 1.create and insert with [int64, float, int8, float_vec] fields
                2.query by int8 field, and output all scalar fields
        expected: verify query result
        """
        self._connect()
        # construct collection from dataFrame according to [int64, float, int8, float_vec]
        df = cf.gen_default_dataframe_data()
        int8_values = pd.Series(data=[np.int8(i) for i in range(ct.default_nb)], dtype="int8")
        df.insert(2, ct.default_int8_field_name, int8_values)
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb
        # query expression
        term_expr = f'{ct.default_int8_field_name} in {[0]}'
        # expected query result
        res = []
        # int8 range [-128, 127] so when nb=1200, there are many repeated int8 values equal to 0
        for i in range(0, ct.default_nb, 256):
            res.extend(df.iloc[i:i + 1, :-2].to_dict('records'))
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()
        self.collection_wrap.query(term_expr, output_fields=["float", "int64", "int8", "varchar"],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_expression(self, enable_dynamic_field):
        """
        target: test query with different expr
        method: query with different boolean expr
        expected: verify query result
        """
        # 1. initialize with data
        nb = 2000
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_query_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                if enable_dynamic_field:
                    int64 = _vectors[i][ct.default_int64_field_name]
                    float = _vectors[i][ct.default_float_field_name]
                else:
                    int64 = _vectors.int64[i]
                    float = _vectors.float[i]
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # query and verify result
            res = collection_w.query(expr=expr, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

            # query again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = collection_w.query(expr=expr, expr_params=expr_params, limit=nb)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_wrong_term_keyword(self):
        """
        target: test query with wrong term expr keyword
        method: query with wrong keyword term expr
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        expr_1 = f'{ct.default_int64_field_name} inn [1, 2]'
        error_1 = {ct.err_code: 65535, ct.err_msg: "cannot parse expression: int64 inn [1, 2], "
                                                   "error: invalid expression: int64 inn [1, 2]"}
        collection_w.query(expr_1, check_task=CheckTasks.err_res, check_items=error_1)

        expr_3 = f'{ct.default_int64_field_name} in not [1, 2]'
        error_3 = {ct.err_code: 65535, ct.err_msg: "cannot parse expression: int64 in not [1, 2], "
                                                   "error: value 'not[1,2]' in list cannot be a non-const expression"}
        collection_w.query(expr_3, check_task=CheckTasks.err_res, check_items=error_3)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", [ct.default_int64_field_name, ct.default_float_field_name])
    def test_query_expr_not_in_term(self, field):
        """
        target: test query with `not in` expr
        method: query with not in expr
        expected: verify query result
        """
        self._connect()
        df = cf.gen_default_dataframe_data()
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()
        values = df[field].tolist()
        pos = 100
        term_expr = f'{field} not in {values[pos:]}'
        res = df.iloc[:pos, :3].to_dict('records')
        self.collection_wrap.query(term_expr, output_fields=["float", "int64", "varchar"],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("pos", [0, ct.default_nb])
    def test_query_expr_not_in_empty_and_all(self, pos):
        """
        target: test query with `not in` expr
        method: query with `not in` expr for (non)empty collection
        expected: verify query result
        """
        self._connect()
        df = cf.gen_default_dataframe_data()
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == ct.default_nb
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()
        int64_values = df[ct.default_int64_field_name].tolist()
        term_expr = f'{ct.default_int64_field_name} not in {int64_values[pos:]}'
        res = df.iloc[:pos, :1].to_dict('records')
        self.collection_wrap.query(term_expr,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_random_values(self):
        """
        target: test query with random filter values
        method: query with random filter values, like [0, 2, 4, 3]
        expected: correct query result
        """
        self._connect()
        df = cf.gen_default_dataframe_data(nb=100)
        log.debug(df.head(5))
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == 100
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        # random_values = [random.randint(0, ct.default_nb) for _ in range(4)]
        random_values = [0, 2, 4, 3]
        term_expr = f'{ct.default_int64_field_name} in {random_values}'
        res = df.iloc[random_values, :1].to_dict('records')
        self.collection_wrap.query(term_expr,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_not_in_random(self):
        """
        target: test query with fixed filter values
        method: query with fixed filter values
        expected: correct query result
        """
        self._connect()
        df = cf.gen_default_dataframe_data(nb=50)
        log.debug(df.head(5))
        self.collection_wrap.construct_from_dataframe(cf.gen_unique_str(prefix), df,
                                                      primary_field=ct.default_int64_field_name)
        assert self.collection_wrap.num_entities == 50
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        self.collection_wrap.load()

        random_values = [i for i in range(10, 50)]
        log.debug(f'random values: {random_values}')
        random.shuffle(random_values)
        term_expr = f'{ct.default_int64_field_name} not in {random_values}'
        res = df.iloc[:10, :1].to_dict('records')
        self.collection_wrap.query(term_expr,
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: res, "pk_name": self.collection_wrap.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_non_array_term(self):
        """
        target: test query with non-array term expr
        method: query with non-array term expr
        expected: raise exception
        """
        exprs = [f'{ct.default_int64_field_name} in 1',
                 f'{ct.default_int64_field_name} in "in"']
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        for expr in exprs:
            error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression: {expr}, "
                                                    "error: the right-hand side of 'in' must be a list"}
            collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)
        expr = f'{ct.default_int64_field_name} in (mn)'
        error = {ct.err_code: 1100, ct.err_msg: f"cannot parse expression: {expr}, "
                                                "error: field mn not exist"}
        collection_w.query(expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_empty_term_array(self):
        """
        target: test query with empty array term expr
        method: query with empty term expr
        expected: empty result
        """
        term_expr = f'{ct.default_int64_field_name} in []'
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res, _ = collection_w.query(term_expr)
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_inconsistent_mix_term_array(self):
        """
        target: test query with term expr that field and array are inconsistent or mix type
        method: 1.query with int field and float values
                2.query with term expr that has int and float type value
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        values = [1., 2.]
        term_expr = f'{ct.default_int64_field_name} in {values}'
        error = {ct.err_code: 1100,
                 ct.err_msg: f"cannot parse expression: int64 in {values}, "
                             "error: value 'float_val:1' in list cannot be casted to Int64"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

        values = [1, 2.]
        term_expr = f'{ct.default_int64_field_name} in {values}'
        error = {ct.err_code: 1100,
                 ct.err_msg: f"cannot parse expression: int64 in {values}, "
                             "error: value 'float_val:2' in list cannot be casted to Int64"}
        collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_non_constant_array_term(self):
        """
        target: test query with non-constant array term expr
        method: query with non-constant array expr
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        constants = [[1], (), {}]
        for constant in constants:
            error = {ct.err_code: 1100,
                     ct.err_msg: f"cannot parse expression: int64 in [{constant}]"}
            term_expr = f'{ct.default_int64_field_name} in [{constant}]'
            collection_w.query(term_expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS",
                                             "array_contains", "ARRAY_CONTAINS"])
    def test_query_expr_json_contains(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        limit = 99
        for i in range(ct.default_nb):
            array[i][json_field] = {"number": i,
                                    "list": [m for m in range(i, i + limit)]}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        expression = f"{expr_prefix}({json_field}['list'], 1000)"
        res = collection_w.query(expression)[0]
        assert len(res) == limit

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_query_expr_list_json_contains(self, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=True)[0]

        # 2. insert data
        limit = ct.default_nb // 4
        array = []
        for i in range(ct.default_nb):
            data = {
                ct.default_int64_field_name: i,
                ct.default_json_field_name: [str(m) for m in range(i, i + limit)],
                ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0]
            }
            array.append(data)
        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        expression = f"{expr_prefix}({json_field}, '1000')"
        res = collection_w.query(expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == limit

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_query_expr_json_contains_combined_with_normal(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        limit = ct.default_nb // 3
        for i in range(ct.default_nb):
            array[i][ct.default_json_field_name] = {"number": i, "list": [m for m in range(i, i + limit)]}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        tar = 1000
        expression = f"{expr_prefix}({json_field}['list'], {tar}) && float > {tar - limit // 2}"
        res = collection_w.query(expression)[0]
        assert len(res) == limit // 2

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_all", "JSON_CONTAINS_ALL",
                                             "array_contains_all", "ARRAY_CONTAINS_ALL"])
    def test_query_expr_all_datatype_json_contains_all(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        limit = 10
        for i in range(ct.default_nb):
            content = {
                # test for int
                "listInt": [m for m in range(i, i + limit)],
                # test for string
                "listStr": [str(m) for m in range(i, i + limit)],
                # test for float
                "listFlt": [m * 1.0 for m in range(i, i + limit)],
                # test for bool
                "listBool": [bool(i % 2)],
                # test for list
                "listList": [[i, str(i + 1)], [i * 1.0, i + 1]],
                # test for mixed data
                "listMix": [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]]
            }
            array[i][ct.default_json_field_name] = content

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        # test for int
        _id = random.randint(limit, ct.default_nb - limit)
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({json_field}['listInt'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({json_field}['listStr'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == limit - len(ids) + 1

        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({json_field}['listFlt'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == limit

        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({json_field}['listBool'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == ct.default_nb // 2

        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({json_field}['listList'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        # test for mixed data
        ids = [[_id, str(_id)], bool(_id % 2)]
        expression = f"{expr_prefix}({json_field}['listMix'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_all", "JSON_CONTAINS_ALL"])
    def test_query_expr_list_all_datatype_json_contains_all(self, expr_prefix):
        """
        target: test query with expression using json_contains_all
        method: query with expression using json_contains_all
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=True)[0]

        # 2. insert data
        array = cf.gen_default_rows_data(with_json=False)
        limit = 10
        for i in range(ct.default_nb):
            array[i]["listInt"] = [m for m in range(i, i + limit)]  # test for int
            array[i]["listStr"] = [str(m) for m in range(i, i + limit)]  # test for string
            array[i]["listFlt"] = [m * 1.0 for m in range(i, i + limit)]  # test for float
            array[i]["listBool"] = [bool(i % 2)]  # test for bool
            array[i]["listList"] = [[i, str(i + 1)], [i * 1.0, i + 1]]  # test for list
            array[i]["listMix"] = [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]]  # test for mixed data

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()

        # test for int
        _id = random.randint(limit, ct.default_nb - limit)
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}(listInt, {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}(listStr, {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == limit - len(ids) + 1

        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}(listFlt, {ids})"
        res = collection_w.query(expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == limit

        # test for bool
        ids = [True]
        expression = f"{expr_prefix}(listBool, {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == ct.default_nb // 2

        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}(listList, {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        # test for mixed data
        ids = [[_id, str(_id)], bool(_id % 2)]
        expression = f"{expr_prefix}(listMix, {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY"])
    def test_query_expr_all_datatype_json_contains_any(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        limit = 10
        for i in range(ct.default_nb):
            content = {
                # test for int
                "listInt": [m for m in range(i, i + limit)],
                # test for string
                "listStr": [str(m) for m in range(i, i + limit)],
                # test for float
                "listFlt": [m * 1.0 for m in range(i, i + limit)],
                # test for bool
                "listBool": [bool(i % 2)],
                # test for list
                "listList": [[i, str(i + 1)], [i * 1.0, i + 1]],
                # test for mixed data
                "listMix": [i, i * 1.1, str(i), bool(i % 2), [i, str(i)]]
            }
            array[i][ct.default_json_field_name] = content

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()

        # test for int
        _id = random.randint(limit, ct.default_nb - limit)
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}({json_field}['listInt'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 2 * limit - 1

        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}({json_field}['listStr'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == limit + len(ids) - 1

        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}({json_field}['listFlt'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == limit

        # test for bool
        ids = [True]
        expression = f"{expr_prefix}({json_field}['listBool'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == ct.default_nb // 2

        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}({json_field}['listList'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        # test for mixed data
        ids = [_id, bool(_id % 2)]
        expression = f"{expr_prefix}({json_field}['listMix'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == ct.default_nb // 2

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY",
                                             "array_contains_any", "ARRAY_CONTAINS_ANY"])
    def test_query_expr_list_all_datatype_json_contains_any(self, expr_prefix):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        # 1. initialize with data
        nb = ct.default_nb
        pk_field = ct.default_int64_field_name
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=True)[0]

        # 2. insert data
        array = cf.gen_default_rows_data(with_json=False)
        limit = random.randint(10, 20)
        int_data = [[m for m in range(i, i + limit)] for i in range(nb)]
        str_data = [[str(m) for m in range(i, i + limit)] for i in range(nb)]
        flt_data = [[m * 1.0 for m in range(i, i + limit)] for i in range(nb)]
        bool_data = [[bool(i % 2)] for i in range(nb)]
        list_data = [[[i, str(i + 1)], [i * 1.0, i + 1]] for i in range(nb)]
        mix_data = [[i, i * 1.1, str(i), bool(i % 2), [i, str(i)]] for i in range(nb)]
        for i in range(nb):
            array[i]["listInt"] = int_data[i]  # test for int
            array[i]["listStr"] = str_data[i]  # test for string
            array[i]["listFlt"] = flt_data[i]  # test for float
            array[i]["listBool"] = bool_data[i]  # test for bool
            array[i]["listList"] = list_data[i]  # test for list
            array[i]["listMix"] = mix_data[i]  # test for mixed data

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()

        _id = random.randint(limit, nb - limit)
        # test for int
        ids = [i for i in range(_id, _id + limit)]
        expression = f"{expr_prefix}(listInt, {ids})"
        res = collection_w.query(expression)[0]
        assert [entity[pk_field] for entity in res] == cf.assert_json_contains(expression, int_data)

        # test for string
        ids = [str(_id), str(_id + 1), str(_id + 2)]
        expression = f"{expr_prefix}(listStr, {ids})"
        res = collection_w.query(expression)[0]
        assert [entity[pk_field] for entity in res] == cf.assert_json_contains(expression, str_data)

        # test for float
        ids = [_id * 1.0]
        expression = f"{expr_prefix}(listFlt, {ids})"
        res = collection_w.query(expression)[0]
        assert [entity[pk_field] for entity in res] == cf.assert_json_contains(expression, flt_data)

        # test for bool
        ids = [True]
        expression = f"{expr_prefix}(listBool, {ids})"
        res = collection_w.query(expression)[0]
        assert [entity[pk_field] for entity in res] == cf.assert_json_contains(expression, bool_data)

        # test for list
        ids = [[_id, str(_id + 1)]]
        expression = f"{expr_prefix}(listList, {ids})"
        res = collection_w.query(expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == 1

        # test for mixed data
        ids = [str(_id)]
        expression = f"{expr_prefix}(listMix, {ids})"
        res = collection_w.query(expression, output_fields=["count(*)"])[0]
        assert res[0]["count(*)"] == 1

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "json_contains_all"])
    def test_query_expr_json_contains_list_in_list(self, expr_prefix, enable_dynamic_field):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        for i in range(ct.default_nb):
            array[i][json_field] = {"list": [[i, i + 1], [i, i + 2], [i, i + 3]]}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        _id = random.randint(3, ct.default_nb - 3)
        ids = [[_id, _id + 1]]
        expression = f"{expr_prefix}({json_field}['list'], {ids})"
        res = collection_w.query(expression)[0]
        assert len(res) == 1

        ids = [[_id + 4, _id], [_id]]
        expression = f"{expr_prefix}({json_field}['list'], {ids})"
        collection_w.query(expression, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains_any", "JSON_CONTAINS_ANY",
                                             "json_contains_all", "JSON_CONTAINS_ALL"])
    @pytest.mark.parametrize("not_list", ["str", {1, 2, 3}, (1, 2, 3), 10])
    def test_query_expr_json_contains_invalid_type(self, expr_prefix, enable_dynamic_field, not_list):
        """
        target: test query with expression using json_contains_any
        method: query with expression using json_contains_any
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        nb = 10
        array = cf.gen_default_rows_data(nb=nb)
        for i in range(nb):
            array[i][json_field] = {"number": i,
                                    "list": [m for m in range(i, i + 10)]}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        expression = f"{expr_prefix}({json_field}['list'], {not_list})"
        error = {ct.err_code: 1100, ct.err_msg: f"failed to create query plan: cannot parse expression: {expression}"}
        collection_w.query(expression, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr_prefix", ["json_contains", "JSON_CONTAINS"])
    def test_query_expr_json_contains_pagination(self, enable_dynamic_field, expr_prefix):
        """
        target: test query with expression using json_contains
        method: query with expression using json_contains
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=enable_dynamic_field)[0]

        # 2. insert data
        array = cf.gen_default_rows_data()
        limit = ct.default_nb // 3
        for i in range(ct.default_nb):
            array[i][json_field] = {"number": i,
                                    "list": [m for m in range(i, i + limit)]}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        expression = f"{expr_prefix}({json_field}['list'], 1000)"
        offset = random.randint(1, limit)
        res = collection_w.query(expression, limit=limit, offset=offset)[0]
        assert len(res) == limit - offset

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("array_length", ["ARRAY_LENGTH", "array_length"])
    @pytest.mark.parametrize("op", ["==", "!="])
    def test_query_expr_array_length(self, array_length, op, enable_dynamic_field):
        """
        target: test query with expression using array_length
        method: query with expression using array_length
                array_length only support == , !=
        expected: succeed
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)

        # 2. insert data
        data = cf.gen_array_dataframe_data()
        length = []
        for i in range(ct.default_nb):
            ran_int = random.randint(50, 53)
            length.append(ran_int)

        data[ct.default_float_array_field_name] = \
            [[np.float32(j) for j in range(length[i])] for i in range(ct.default_nb)]
        collection_w.insert(data)

        # 3. load and query
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        expression = f"{array_length}({ct.default_float_array_field_name}) {op} 51"
        res = collection_w.query(expression)[0]

        # 4. check
        expression = expression.replace(f"{array_length}(float_array)", "array_length")
        filter_ids = []
        for i in range(ct.default_nb):
            array_length = length[i]
            if not expression or eval(expression):
                filter_ids.append(i)
        assert len(res) == len(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("op", [">", "<=", "==", "!="])
    def test_query_expr_invalid_array_length(self, op):
        """
        target: test query with expression using array_length
        method: query with expression using array_length
                array_length only support == , !=
        expected: raise error
        """
        # 1. create a collection
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        data = cf.gen_array_dataframe_data()
        collection_w.insert(data)

        # 3. load and query
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        expression = f"array_length({ct.default_float_array_field_name}) {op} 51"
        res = collection_w.query(expression)[0]
        assert len(res) >= 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_empty_without_limit(self):
        """
        target: test query with empty expression and no limit
        method: query empty expression without setting limit
        expected: raise error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. query with no limit and no offset
        error = {ct.err_code: 1, ct.err_msg: "empty expression should be used with limit"}
        collection_w.query("", check_task=CheckTasks.err_res, check_items=error)

        # 3. query with offset but no limit
        collection_w.query("", offset=1, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_empty(self):
        """
        target: test query  empty
        method: query empty
        expected: return error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. query
        try:
            collection_w.query()
        except TypeError as e:
            assert "missing 1 required positional argument: 'expr'" in str(e)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("limit", [10, 100, 1000])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_query_expr_empty(self, auto_id, limit):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id)[0:4]
        exp_ids, res = insert_ids[:limit], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids})

        # 2. query with limit
        collection_w.query("", limit=limit,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_empty_pk_string(self):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, primary_field=ct.default_string_field_name)[0:4]
        # string field is sorted by lexicographical order
        exp_ids, res = ['0', '1', '10', '100', '1000', '1001', '1002', '1003', '1004', '1005'], []
        for ids in exp_ids:
            res.append({ct.default_string_field_name: ids})

        # 2. query with limit
        collection_w.query("", limit=ct.default_limit,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

        # 2. query with limit + offset
        res = res[5:]
        collection_w.query("", limit=5, offset=5,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [100, 1000])
    @pytest.mark.parametrize("limit", [100, 1000])
    @pytest.mark.parametrize("auto_id", [True, False])
    def test_query_expr_empty_with_pagination(self, auto_id, limit, offset):
        """
        target: test query with empty expression
        method: query empty expression with a limit
        expected: return topK results by order
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id)[0:4]
        exp_ids, res = insert_ids[:limit + offset][offset:], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids})

        # 2. query with limit and offset
        collection_w.query("", limit=limit, offset=offset,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [100, 1000])
    @pytest.mark.parametrize("limit", [100, 1000])
    def test_query_expr_empty_with_random_pk(self, limit, offset):
        """
        target: test query with empty expression
        method: create a collection using random pk, query empty expression with a limit
        expected: return topK results by order
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, with_json=False)[0]

        # 2. generate unordered pk array and insert
        unordered_ids = [i for i in range(ct.default_nb)]
        random.shuffle(unordered_ids)
        float_value = [np.float32(i) for i in unordered_ids]
        string_value = [str(i) for i in unordered_ids]
        vector_value = cf.gen_vectors(nb=ct.default_nb, dim=ct.default_dim)
        collection_w.insert([unordered_ids, float_value, string_value, vector_value])
        collection_w.load()

        # 3. query with empty expr and check the result
        exp_ids, res = sorted(unordered_ids)[:limit], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids, ct.default_string_field_name: str(ids)})

        collection_w.query("", limit=limit, output_fields=[ct.default_string_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

        # 4. query with pagination
        exp_ids, res = sorted(unordered_ids)[:limit + offset][offset:], []
        for ids in exp_ids:
            res.append({ct.default_int64_field_name: ids, ct.default_string_field_name: str(ids)})

        collection_w.query("", limit=limit, offset=offset, output_fields=[ct.default_string_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_expr_with_limit_offset_out_of_range(self):
        """
        target: test query with empty expression
        method: query empty expression with limit and offset out of range
        expected: raise error
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. query with limit > 16384
        error = {ct.err_code: 1,
                 ct.err_msg: "invalid max query result window, (offset+limit) should be in range [1, 16384]"}
        collection_w.query("", limit=16385, check_task=CheckTasks.err_res, check_items=error)

        # 3. query with offset + limit > 16384
        collection_w.query("", limit=1, offset=16384, check_task=CheckTasks.err_res, check_items=error)
        collection_w.query("", limit=16384, offset=1, check_task=CheckTasks.err_res, check_items=error)

        # 4. query with limit < 0
        error = {ct.err_code: 1,
                 ct.err_msg: "invalid max query result window, offset [-1] is invalid, should be gte than 0"}
        collection_w.query("", limit=2, offset=-1,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expression", cf.gen_integer_overflow_expressions())
    def test_query_expr_out_of_range(self, expression):
        """
        target: test query expression out of range
        method: query empty expression with limit and offset out of range
        expected:
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, is_all_data_type=True)[0]
        start = ct.default_nb // 2
        _vectors = cf.gen_dataframe_all_data_type(start=start)

        # increase the value to cover the int range
        _vectors["int16"] = \
            pd.Series(data=[np.int16(i * 40) for i in range(start, start + ct.default_nb)], dtype="int16")
        _vectors["int32"] = \
            pd.Series(data=[np.int32(i * 2200000) for i in range(start, start + ct.default_nb)], dtype="int32")
        insert_ids = collection_w.insert(_vectors)[0].primary_keys

        # filter result with expression in collection
        expression = expression.replace("&&", "and").replace("||", "or")
        filter_ids = []
        for i, _id in enumerate(insert_ids):
            int8 = _vectors.int8[i]
            int16 = _vectors.int16[i]
            int32 = _vectors.int32[i]
            if not expression or eval(expression):
                filter_ids.append(_id)

        # query
        collection_w.load()
        res = collection_w.query(expression, output_fields=["int8"])[0]
        assert len(res) == len(filter_ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_output_field_none_or_empty(self, enable_dynamic_field):
        """
        target: test query with none and empty output field
        method: query with output field=None, field=[]
        expected: return primary field
        """
        collection_w = self.init_collection_general(prefix, insert_data=True,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        for fields in [None, []]:
            res, _ = collection_w.query(default_term_expr, output_fields=fields)
            assert res[0].keys() == {ct.default_int64_field_name}

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_output_one_field(self, enable_dynamic_field):
        """
        target: test query with output one field
        method: query with output one field
        expected: return one field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field)[0:2]
        res, _ = collection_w.query(default_term_expr, output_fields=[ct.default_float_field_name])
        assert set(res[0].keys()) == {ct.default_int64_field_name, ct.default_float_field_name}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 30437")
    def test_query_output_all_fields(self, enable_dynamic_field, random_primary_key):
        """
        target: test query with none output field
        method: query with output field=None
        expected: return all fields
        """
        enable_dynamic_field = False
        # 1. initialize with data
        collection_w, df, _, insert_ids = \
            self.init_collection_general(prefix, True, nb=10, is_all_data_type=True,
                                         enable_dynamic_field=enable_dynamic_field,
                                         random_primary_key=random_primary_key)[0:4]
        all_fields = [ct.default_int64_field_name, ct.default_int32_field_name, ct.default_int16_field_name,
                      ct.default_int8_field_name, ct.default_bool_field_name, ct.default_float_field_name,
                      ct.default_double_field_name, ct.default_string_field_name, ct.default_json_field_name,
                      ct.default_float_vec_field_name, ct.default_float16_vec_field_name,
                      ct.default_bfloat16_vec_field_name]
        if enable_dynamic_field:
            res = df[0][:2]
        else:
            res = []
            for id in range(2):
                num = df[0][df[0][ct.default_int64_field_name] == id].index.to_list()[0]
                res.append(df[0].iloc[num].to_dict())
        log.info(res)
        collection_w.load()
        actual_res, _ = collection_w.query(default_term_expr, output_fields=all_fields,
                                           check_task=CheckTasks.check_query_results,
                                           check_items={exp_res: res, "with_vec": True,
                                                        "pk_name": collection_w.primary_field.name})
        assert set(actual_res[0].keys()) == set(all_fields)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_output_float_vec_field(self):
        """
        target: test query with vec output field
        method: specify vec field as output field
        expected: return primary field and vec field
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        fields = [[ct.default_float_vec_field_name],
                  [ct.default_int64_field_name, ct.default_float_vec_field_name]]
        res = df.loc[:1, [ct.default_int64_field_name, ct.default_float_vec_field_name]].to_dict('records')
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        for output_fields in fields:
            collection_w.query(default_term_expr, output_fields=output_fields,
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: res, "with_vec": True,
                                            "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", default_float_field_name],
                                                        ["*", default_int_field_name]])
    def test_query_output_field_wildcard(self, wildcard_output_fields):
        """
        target: test query with output fields using wildcard
        method: query with one output_field (wildcard)
        expected: query success
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        output_fields = cf.get_wildcard_output_field_names(collection_w, wildcard_output_fields)
        output_fields.append(default_int_field_name)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        with_vec = True if ct.default_float_vec_field_name in output_fields else False
        actual_res = collection_w.query(default_term_expr, output_fields=wildcard_output_fields)[0]
        assert set(actual_res[0].keys()) == set(output_fields)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/12680")
    @pytest.mark.parametrize("vec_fields", [[cf.gen_float_vec_field(name="float_vector1")]])
    def test_query_output_multi_float_vec_field(self, vec_fields):
        """
        target: test query and output multi float vec fields
        method: a.specify multi vec field as output
                b.specify output_fields with wildcard %
        expected: verify query result
        """
        # init collection with two float vector fields
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

        # query with two vec output_fields
        output_fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        for vec_field in vec_fields:
            output_fields.append(vec_field.name)
        res = df.loc[:1, output_fields].to_dict('records')
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "with_vec": True, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/12680")
    @pytest.mark.parametrize("vec_fields", [[cf.gen_binary_vec_field()],
                                            [cf.gen_binary_vec_field(), cf.gen_binary_vec_field("binary_vec1")]])
    def test_query_output_mix_float_binary_field(self, vec_fields):
        """
        target:  test query and output mix float and binary vec fields
        method: a.specify mix vec field as output
                b.specify output_fields with wildcard %
        expected: output binary vector and float vec
        """
        # init collection with two float vector fields
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb

        # query with two vec output_fields
        output_fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        for vec_field in vec_fields:
            output_fields.append(vec_field.name)
        res = df.loc[:1, output_fields].to_dict('records')
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "with_vec": True, "pk_name": collection_w.primary_field.name})

        # query with wildcard %
        collection_w.query(default_term_expr, output_fields=["*"],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "with_vec": True, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_output_binary_vec_field(self):
        """
        target: test query with binary vec output field
        method: specify binary vec field as output field
        expected: return primary field and binary vec field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_binary=True)[0:2]
        fields = [[ct.default_binary_vec_field_name],
                  [ct.default_int64_field_name, ct.default_binary_vec_field_name]]
        for output_fields in fields:
            res, _ = collection_w.query(default_term_expr, output_fields=output_fields)
            assert res[0].keys() == set(fields[-1])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_output_primary_field(self):
        """
        target: test query with output field only primary field
        method: specify int64 primary field as output field
        expected: return int64 field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res, _ = collection_w.query(default_term_expr, output_fields=[ct.default_int64_field_name])
        assert res[0].keys() == {ct.default_int64_field_name}

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_output_not_existed_field(self):
        """
        target: test query output not existed field
        method: query with not existed output field
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        error = {ct.err_code: 65535, ct.err_msg: 'field int not exist'}
        output_fields = [["int"], [ct.default_int64_field_name, "int"]]
        for fields in output_fields:
            collection_w.query(default_term_expr, output_fields=fields,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="exception not MilvusException")
    def test_query_invalid_output_fields(self):
        """
        target: test query with invalid output fields
        method: query with invalid field fields
        expected: raise exception
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        output_fields = ["12-s", 1, [1, "2", 3], (1,), {1: 1}]
        error = {ct.err_code: 0, ct.err_msg: f'Invalid query format. \'output_fields\' must be a list'}
        for fields in output_fields:
            collection_w.query(default_term_expr, output_fields=fields, check_task=CheckTasks.err_res,
                               check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(reason="issue 24637")
    def test_query_output_fields_simple_wildcard(self):
        """
        target: test query output_fields with simple wildcard (* and %)
        method: specify output_fields as "*"
        expected: output all scale field; output all fields
        """
        # init collection with fields: int64, float, float_vec, float_vector1
        # collection_w, df = self.init_multi_fields_collection_wrap(cf.gen_unique_str(prefix))
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        df = vectors[0]

        # query with wildcard all fields
        res3 = df.iloc[:2].to_dict('records')
        collection_w.query(default_term_expr, output_fields=["*"],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res3, "with_vec": True, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 24637")
    def test_query_output_fields_part_scale_wildcard(self):
        """
        target: test query output_fields with part wildcard
        method: specify output_fields as wildcard and part field
        expected: verify query result
        """
        # init collection with fields: int64, float, float_vec
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False)[0:2]
        df = vectors[0]

        # query with output_fields=["*", float_vector)
        res = df.iloc[:2].to_dict('records')
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(default_term_expr, output_fields=["*", ct.default_float_vec_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "with_vec": True, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields", [["*%"], ["**"], ["*", "@"]])
    def test_query_invalid_wildcard(self, output_fields):
        """
        target: test query with invalid output wildcard
        method: output_fields is invalid output wildcard
        expected: raise exception
        """
        # init collection with fields: int64, float, float_vec
        collection_w = self.init_collection_general(prefix, insert_data=True, nb=100)[0]
        collection_w.load()

        # query with invalid output_fields
        error = {ct.err_code: 65535, ct.err_msg: f"field {output_fields[-1]} not exist"}
        collection_w.query(default_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_partition(self):
        """
        target: test query on partition
        method: create a partition and query
        expected: verify query result
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data()
        partition_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        res = df.iloc[:2, :1].to_dict('records')
        collection_w.query(default_term_expr, partition_names=[partition_w.name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_partition_without_loading(self):
        """
        target: test query on partition without loading
        method: query on partition and no loading
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data()
        partition_w.insert(df)
        assert partition_w.num_entities == ct.default_nb
        error = {ct.err_code: 65535, ct.err_msg: "collection not loaded"}
        collection_w.query(default_term_expr, partition_names=[partition_w.name],
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_default_partition(self):
        """
        target: test query on default partition
        method: query on default partition
        expected: verify query result
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res = vectors[0].iloc[:2, :1].to_dict('records')
        collection_w.query(default_term_expr, partition_names=[ct.default_partition_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_empty_partition_names(self):
        """
        target: test query with empty partition_names
        method: query with partition_names=[]
        expected: query from all partitions
        """
        # insert [0, half) into partition_w, [half, nb) into _default
        half = ct.default_nb // 2
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)

        # query from empty partition_names
        term_expr = f'{ct.default_int64_field_name} in [0, {half}, {ct.default_nb}-1]'
        res = [{'int64': 0}, {'int64': half}, {'int64': ct.default_nb - 1}]
        collection_w.query(term_expr, partition_names=[],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_empty_partition(self):
        """
        target: test query on empty partition
        method: query on an empty collection
        expected: empty query result
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        assert partition_w.is_empty
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        res, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_not_existed_partition(self):
        """
        target: test query on a not existed partition
        method: query on not existed partition
        expected: raise exception
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        partition_names = cf.gen_unique_str()
        error = {ct.err_code: 65535, ct.err_msg: f'partition name {partition_names} not found'}
        collection_w.query(default_term_expr, partition_names=[partition_names],
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_ignore_growing(self):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. query with param ignore_growing=True
        expected: query successfully
        """
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(start=10000)
        collection_w.insert(data)

        # 3. query with param ignore_growing=True
        res = collection_w.query('int64 >= 0', ignore_growing=True)[0]
        assert len(res) == ct.default_nb
        for ids in [res[i][default_int_field_name] for i in range(ct.default_nb)]:
            assert ids < 10000

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_ignore_growing_after_upsert(self):
        """
        target: test query ignoring growing segment after upsert
        method: 1. create a collection, insert data, create index and load
                2. upsert the inserted data
                3. query with param ignore_growing=True
        expected: query successfully
        """
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. insert data again
        data = cf.gen_default_data_for_upsert()[0]
        collection_w.upsert(data)

        # 3. query with param ignore_growing=True
        res1 = collection_w.query('int64 >= 0', ignore_growing=True)[0]
        res2 = collection_w.query('int64 >= 0')[0]
        assert len(res1) == 0
        assert len(res2) == ct.default_nb

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ignore_growing", [2.3, "str"])
    def test_query_invalid_ignore_growing_param(self, ignore_growing):
        """
        target: test query ignoring growing segment param invalid
        method: 1. create a collection, insert data and load
                2. insert data again
                3. query with ignore_growing type invalid
        expected: raise exception
        """
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(start=100)
        collection_w.insert(data)

        # 3. query with param ignore_growing invalid
        error = {ct.err_code: 999, ct.err_msg: "parse ignore growing field failed"}
        collection_w.query('int64 >= 0', ignore_growing=ignore_growing,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.fixture(scope="function", params=[0, 10, 100])
    def offset(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_pagination(self, offset):
        """
        target: test query pagination
        method: create collection and query with pagination params,
                verify if the result is ordered by primary key
        expected: query successfully and verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[offset: pos + offset]}'
        res = vectors[0].iloc[offset:pos + offset, :1].to_dict('records')
        query_params = {"offset": offset, "limit": 10}
        query_res = collection_w.query(term_expr, params=query_params,
                                       check_task=CheckTasks.check_query_results,
                                       check_items={exp_res: res, "pk_name": collection_w.primary_field.name})[0]
        key_res = [item[key] for item in query_res for key in item]
        assert key_res == int_values[offset: pos + offset]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_binary_pagination(self, offset):
        """
        target: test query binary pagination
        method: create collection and query with pagination params,
                verify if the result is ordered by primary key
        expected: query successfully and verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             is_binary=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[offset: pos + offset]}'
        res = vectors[0].iloc[offset:pos + offset, :1].to_dict('records')
        query_params = {"offset": offset, "limit": 10}
        query_res = collection_w.query(term_expr, params=query_params,
                                       check_task=CheckTasks.check_query_results,
                                       check_items={exp_res: res, "pk_name": collection_w.primary_field.name})[0]
        key_res = [item[key] for item in query_res for key in item]
        assert key_res == int_values[offset: pos + offset]

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_pagination_with_expression(self, offset):
        """
        target: test query pagination with different expression
        method: query with different expression and verify the result
        expected: query successfully
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates()[1:]:
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # query and verify result
            query_params = {"offset": offset, "limit": 10}
            res = collection_w.query(expr=expr, params=query_params)[0]
            key_res = [item[key] for item in res for key in item]
            assert key_res == filter_ids

            # query again with expression tempalte
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = collection_w.query(expr=expr, expr_params=expr_params, params=query_params)[0]
            key_res = [item[key] for item in res for key in item]
            assert key_res == filter_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_pagination_with_partition(self, offset):
        """
        target: test query pagination on partition
        method: create a partition and query with different offset
        expected: verify query result
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        df = cf.gen_default_dataframe_data()
        partition_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()
        res = df.iloc[:2, :1].to_dict('records')
        query_params = {"offset": offset, "limit": 10}
        collection_w.query(default_term_expr, params=query_params, partition_names=[partition_w.name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_pagination_with_insert_data(self, offset):
        """
        target: test query pagination on partition
        method: create a partition and query with pagination
        expected: verify query result
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        res = df.iloc[:2, :1].to_dict('records')
        query_params = {"offset": offset, "limit": 10}
        collection_w.query(default_term_expr, params=query_params,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_pagination_without_limit(self, offset):
        """
        target: test query pagination without limit
        method: create collection and query with pagination params(only offset),
                compare the result with query without pagination params
        expected: query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[offset: pos + offset]}'
        res = vectors[0].iloc[offset:pos + offset, :1].to_dict('records')
        query_params = {"offset": offset}
        query_res = collection_w.query(term_expr, params=query_params,
                                       check_task=CheckTasks.check_query_results,
                                       check_items={exp_res: res, "pk_name": collection_w.primary_field.name})[0]
        res = collection_w.query(term_expr,
                                 check_task=CheckTasks.check_query_results,
                                 check_items={exp_res: res, "pk_name": collection_w.primary_field.name})[0]
        assert query_res == res

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [3000, 5000])
    def test_query_pagination_with_offset_over_num_entities(self, offset):
        """
        target: test query pagination with offset over num_entities
        method: query with offset over num_entities
        expected: return an empty list
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[10: pos + 10]}'
        res = collection_w.query(term_expr, offset=offset, limit=10)[0]
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", ["12 s", " ", [0, 1], {2}])
    def test_query_pagination_with_invalid_limit_type(self, limit):
        """
        target: test query pagination with invalid limit type
        method: query with invalid limit tyype
        expected: raise exception
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[10: pos + 10]}'
        collection_w.query(term_expr, offset=10, limit=limit,
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1,
                                        ct.err_msg: "limit [%s] is invalid" % limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [-1, 67890])
    def test_query_pagination_with_invalid_limit_value(self, limit):
        """
        target: test query pagination with invalid limit value
        method: query with invalid limit value
        expected: raise exception
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[10: pos + 10]}'
        error = {ct.err_code: 65535,
                 ct.err_msg: f"invalid max query result window, (offset+limit) should be in range [1, 16384], but got 67900"}
        if limit == -1:
            error = {ct.err_code: 65535,
                     ct.err_msg: f"invalid max query result window, limit [{limit}] is invalid, should be greater than 0"}
        collection_w.query(term_expr, offset=10, limit=limit,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", ["12 s", " ", [0, 1], {2}])
    def test_query_pagination_with_invalid_offset_type(self, offset):
        """
        target: test query pagination with invalid offset type
        method: query with invalid offset type
        expected: raise exception
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[10: pos + 10]}'
        collection_w.query(term_expr, offset=offset, limit=10,
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1,
                                        ct.err_msg: "offset [%s] is invalid" % offset})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [-1, 67890])
    def test_query_pagination_with_invalid_offset_value(self, offset):
        """
        target: test query pagination with invalid offset value
        method: query with invalid offset value
        expected: raise exception
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        pos = 10
        term_expr = f'{ct.default_int64_field_name} in {int_values[10: pos + 10]}'
        error = {ct.err_code: 65535,
                 ct.err_msg: f"invalid max query result window, (offset+limit) should be in range [1, 16384], but got 67900"}
        if offset == -1:
            error = {ct.err_code: 65535,
                     ct.err_msg: f"invalid max query result window, offset [{offset}] is invalid, should be gte than 0"}
        collection_w.query(term_expr, offset=offset, limit=10,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not stable")
    def test_query_during_upsert(self):
        """
        target: test query during upsert
        method: 1. create a collection and query
                2. query during upsert
                3. compare two query results
        expected: the two query results is the same
        """
        upsert_nb = 1000
        expr = f"int64 >= 0 && int64 <= {upsert_nb}"
        collection_w = self.init_collection_general(prefix, True)[0]
        res1 = collection_w.query(expr, output_fields=[default_float_field_name])[0]

        def do_upsert():
            data = cf.gen_default_data_for_upsert(upsert_nb)[0]
            collection_w.upsert(data=data)

        t = threading.Thread(target=do_upsert, args=())
        t.start()
        res2 = collection_w.query(expr, output_fields=[default_float_field_name])[0]
        t.join()
        assert [res1[i][default_float_field_name] for i in range(upsert_nb)] == \
               [res2[i][default_float_field_name] for i in range(upsert_nb)]

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_query_expr_empty_pk_string(self):
        """
        target: turn on mmap to test queries using empty expression
        method: enable mmap to query for empty expressions with restrictions.
        expected: return the first K results in order
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, is_index=False, primary_field=ct.default_string_field_name)[0:4]

        collection_w.set_properties({'mmap.enabled': True})

        # string field is sorted by lexicographical order
        exp_ids, res = ['0', '1', '10', '100', '1000', '1001', '1002', '1003', '1004', '1005'], []
        for ids in exp_ids:
            res.append({ct.default_string_field_name: ids})

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="query_index")
        collection_w.load()
        # 2. query with limit
        collection_w.query("", limit=ct.default_limit,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

        # 3. query with limit + offset
        res = res[5:]
        collection_w.query("", limit=5, offset=5,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_enable_mmap_query_with_expression(self, enable_dynamic_field):
        """
        target: turn on mmap use different expr queries
        method: turn on mmap and query with different expr
        expected: verify query result
        """
        # 1. initialize with data
        nb = 1000
        collection_w, _vectors, _, insert_ids = self.init_collection_general(prefix, True, nb, is_index=False,
                                                                             enable_dynamic_field=enable_dynamic_field)[
                                                0:4]
        # enable mmap
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="query_expr_index")
        collection_w.alter_index("query_expr_index", {'mmap.enabled': True})
        collection_w.load()
        # filter result with expression in collection
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates()[1:]:
            log.debug(f"expr: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                if enable_dynamic_field:
                    int64 = _vectors[i][ct.default_int64_field_name]
                    float = _vectors[i][ct.default_float_field_name]
                else:
                    int64 = _vectors.int64[i]
                    float = _vectors.float[i]
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # query and verify result
            res = collection_w.query(expr=expr)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

            # query again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            res = collection_w.query(expr=expr, expr_params=expr_params)[0]
            query_ids = set(map(lambda x: x[ct.default_int64_field_name], res))
            assert query_ids == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_query_string_field_not_primary_is_empty(self):
        """
        target: enable mmap, use string expr to test query, string field is not the main field
        method: create collection , string field is primary
                enable mmap
                collection load and insert empty data with string field
                collection query uses string expr in string field
        expected: query successfully
        """
        # 1.  create a collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=False, is_index=False)[0:2]

        nb = 3000
        df = cf.gen_default_list_data(nb)
        df[2] = ["" for _ in range(nb)]

        collection_w.insert(df)
        assert collection_w.num_entities == nb

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="index_query")
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.alter_index("index_query", {'mmap.enabled': True})

        collection_w.load()

        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]

        expr = "varchar == \"\""
        res, _ = collection_w.query(expr, output_fields=output_fields)

        assert len(res) == nb

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([default_string_field_name]))
    def test_mmap_query_string_is_primary(self, expression):
        """
        target: test query with output field only primary field
        method: specify string primary field as output field
        expected: return string primary field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=ct.default_string_field_name)[0:2]
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="query_expr_index")
        collection_w.load()
        res, _ = collection_w.query(expression, output_fields=[ct.default_string_field_name])
        assert res[0].keys() == {ct.default_string_field_name}

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_query_string_expr_with_prefixes(self):
        """
        target: test query with prefix string expression
        method: specify string is primary field, use prefix string expr
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=ct.default_string_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name="query_expr_pre_index")
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.alter_index("query_expr_pre_index", {'mmap.enabled': True})

        collection_w.load()
        res = vectors[0].iloc[:1, :3].to_dict('records')
        expression = 'varchar like "0%"'
        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]
        collection_w.query(expression, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})


class TestQueryOperation(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query interface operations
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_without_connection(self):
        """
        target: test query without connection
        method: close connect and query
        expected: raise exception
        """

        # init a collection with default connection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # remove default connection
        self.connection_wrap.remove_connection(alias=DefaultConfig.DEFAULT_USING)

        # list connection to check
        self.connection_wrap.list_connections(check_task=ct.CheckTasks.ccr, check_items={ct.list_content: []})

        # query after remove default connection
        collection_w.query(default_term_expr, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 0, ct.err_msg: cem.ConnectFirst})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_without_loading(self):
        """
        target: test query without loading
        method: no loading before query
        expected: raise exception
        """

        # init a collection with default connection
        collection_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=collection_name)

        # insert data to collection
        collection_w.insert(data=cf.gen_default_list_data())

        # check number of entities and that method calls the flush interface
        assert collection_w.num_entities == ct.default_nb

        # query without load
        collection_w.query(default_term_expr, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535,
                                        ct.err_msg: "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("term_expr", [f'{ct.default_int64_field_name} in [0]'])
    def test_query_expr_single_term_array(self, term_expr):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """

        # init a collection and insert data
        collection_w, vectors, binary_raw_vectors = self.init_collection_general(prefix, insert_data=True)[0:3]

        # query the first row of data
        check_vec = vectors[0].iloc[:, [0]][0:1].to_dict('records')
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("term_expr", [f'{ct.default_int64_field_name} in [0]'])
    def test_query_binary_expr_single_term_array(self, term_expr, check_content):
        """
        target: test query with single array term expr
        method: query with single array value
        expected: query result is one entity
        """

        # init a collection and insert data
        collection_w, vectors, binary_raw_vectors = self.init_collection_general(prefix, insert_data=True,
                                                                                 is_binary=True)[0:3]

        # query the first row of data
        check_vec = vectors[0].iloc[:, [0]][0:1].to_dict('records')
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_expr_all_term_array(self):
        """
        target: test query with all array term expr
        method: query with all array value
        expected: verify query result
        """

        # init a collection and insert data
        collection_w, vectors, binary_raw_vectors = \
            self.init_collection_general(prefix, insert_data=True)[0:3]

        # data preparation
        int_values = vectors[0][ct.default_int64_field_name].values.tolist()
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        check_vec = vectors[0].iloc[:, [0]][0:len(int_values)].to_dict('records')

        # query all array value
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_half_term_array(self):
        """
        target: test query with half array term expr
        method: query with half array value
        expected: verify query result
        """

        half = ct.default_nb // 2
        collection_w, partition_w, df_partition, df_default = \
            self.insert_entities_into_two_partitions_in_half(half)

        int_values = df_default[ct.default_int64_field_name].values.tolist()
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == len(int_values)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_expr_repeated_term_array(self):
        """
        target: test query with repeated term array on primary field with unique value
        method: query with repeated array value
        expected: return hit entities, no repeated
        """
        collection_w, vectors, binary_raw_vectors = self.init_collection_general(prefix, insert_data=True)[0:3]
        int_values = [0, 0, 0, 0]
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        res, _ = collection_w.query(term_expr)
        assert len(res) == 1
        assert res[0][ct.default_int64_field_name] == int_values[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_dup_ids_dup_term_array(self):
        """
        target: test query on duplicate primary keys with dup term array
        method: 1.create collection and insert dup primary keys
                2.query with dup term array
        expected: todo
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=100)
        df[ct.default_int64_field_name] = 0
        mutation_res, _ = collection_w.insert(df)
        assert mutation_res.primary_keys == df[ct.default_int64_field_name].tolist()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        term_expr = f'{ct.default_int64_field_name} in {[0, 0, 0]}'
        res = df.iloc[:, :2].to_dict('records')
        collection_w.query(term_expr, output_fields=["*"], check_items=CheckTasks.check_query_results,
                           check_task={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("with_growing", [True])
    def test_query_to_get_latest_entity_with_dup_ids(self, with_growing):
        """
        target: test query to get latest entity with duplicate primary keys
        method: 1.create collection and insert dup primary key = 0
                2.query with expr=dup_id
        expected: return the latest entity; verify the result is same as dedup entities
        """
        collection_w = self.init_collection_general(prefix, dim=16, is_flush=False, insert_data=False, is_index=False,
                                                    vector_data_type=DataType.FLOAT_VECTOR, with_json=False)[0]
        nb = 50
        rounds = 10
        for i in range(rounds):
            df = cf.gen_default_dataframe_data(dim=16, nb=nb, start=i * nb, with_json=False)
            df[ct.default_int64_field_name] = i
            collection_w.insert(df)
            # re-insert the last piece of data in df to refresh the timestamp
            last_piece = df.iloc[-1:]
            collection_w.insert(last_piece)

        if not with_growing:
            collection_w.flush()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_index)
        collection_w.load()
        # verify the result returns the latest entity if there are duplicate primary keys
        expr = f'{ct.default_int64_field_name} == 0'
        res = collection_w.query(expr=expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name])[0]
        assert len(res) == 1 and res[0][ct.default_float_field_name] == (nb - 1) * 1.0

        # verify the result is same as dedup entities
        expr = f'{ct.default_int64_field_name} >= 0'
        res = collection_w.query(expr=expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name])[0]
        assert len(res) == rounds

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_after_index(self):
        """
        target: test query after creating index
        method: 1. indexing
                2. load
                3. query
        expected: query result is correct
        """
        collection_w, vectors, binary_raw_vectors = self.init_collection_general(prefix, insert_data=True,
                                                                                 is_index=False)[0:3]

        default_field_name = ct.default_float_vec_field_name
        collection_w.create_index(default_field_name, default_index_params)

        collection_w.load()

        int_values = [0]
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        check_vec = vectors[0].iloc[:, [0]][0:len(int_values)].to_dict('records')
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_after_search(self):
        """
        target: test query after search
        method: 1. search
                2. query without load again
        expected: query result is correct
        """

        limit = 1000
        nb_old = 500
        collection_w, vectors, binary_raw_vectors, insert_ids = \
            self.init_collection_general(prefix, True, nb_old)[0:4]

        # 2. search for original data after load
        vectors_s = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors_s[:ct.default_nq], ct.default_float_vec_field_name,
                            ct.default_search_params, limit, "int64 >= 0",
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": ct.default_nq, "limit": nb_old, "ids": insert_ids})

        # check number of entities and that method calls the flush interface
        assert collection_w.num_entities == nb_old

        term_expr = f'{ct.default_int64_field_name} in [0, 1]'
        check_vec = vectors[0].iloc[:, [0]][0:2].to_dict('records')
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_output_vec_field_after_index(self):
        """
        target: test query output vec field after index
        method: create index and specify vec field as output field
        expected: return primary field and vec field
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=5000)
        collection_w.insert(df)
        assert collection_w.num_entities == 5000
        fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        res = df.loc[:1, [ct.default_int64_field_name, ct.default_float_vec_field_name]].to_dict('records')
        collection_w.load()
        collection_w.query(default_term_expr, output_fields=fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "with_vec": True, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_output_binary_vec_field_after_index(self):
        """
        target: test query output vec field after index
        method: create index and specify vec field as output field
        expected: return primary field and vec field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             is_binary=True, is_index=False)[0:2]
        fields = [ct.default_int64_field_name, ct.default_binary_vec_field_name]
        collection_w.create_index(ct.default_binary_vec_field_name, binary_index_params)
        assert collection_w.has_index()[0]
        collection_w.load()
        res, _ = collection_w.query(default_term_expr, output_fields=[ct.default_binary_vec_field_name])
        assert res[0].keys() == set(fields)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_query_output_all_vector_type(self, vector_data_type):
        """
        target: test query output different vector type
        method: create index and specify vec field as output field
        expected: return primary field and vec field
        """
        collection_w, vectors = self.init_collection_general(prefix, True,
                                                             vector_data_type=vector_data_type)[0:2]
        fields = [ct.default_int64_field_name, ct.default_float_vec_field_name]
        res, _ = collection_w.query(default_term_expr, output_fields=[ct.default_float_vec_field_name])
        assert res[0].keys() == set(fields)

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_partition_repeatedly(self):
        """
        target: test query repeatedly on partition
        method: query on partition twice
        expected: verify query result
        """

        # create connection
        self._connect()

        # init collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # init partition
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert data to partition
        df = cf.gen_default_dataframe_data()
        partition_w.insert(df)

        # check number of entities and that method calls the flush interface
        assert collection_w.num_entities == ct.default_nb

        # load partition
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load()

        # query twice
        res_one, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        res_two, _ = collection_w.query(default_term_expr, partition_names=[partition_w.name])
        assert res_one == res_two

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_another_partition(self):
        """
        target: test query another partition
        method: 1. insert entities into two partitions
                2.query on one partition and query result empty
        expected: query result is empty
        """
        half = ct.default_nb // 2
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)

        term_expr = f'{ct.default_int64_field_name} in [{half}]'
        # half entity in _default partition rather than partition_w
        collection_w.query(term_expr, partition_names=[partition_w.name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_multi_partitions_multi_results(self):
        """
        target: test query on multi partitions and get multi results
        method: 1.insert entities into two partitions
                2.query on two partitions and query multi result
        expected: query results from two partitions
        """
        half = ct.default_nb // 2
        collection_w, partition_w, _, _ = self.insert_entities_into_two_partitions_in_half(half)

        term_expr = f'{ct.default_int64_field_name} in [{half - 1}, {half}]'
        # half entity in _default, half-1 entity in partition_w
        res, _ = collection_w.query(term_expr,
                                    partition_names=[ct.default_partition_name, partition_w.name])
        assert len(res) == 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_multi_partitions_single_result(self):
        """
        target: test query on multi partitions and get single result
        method: 1.insert into two partitions
                2.query on two partitions and query single result
        expected: query from two partitions and get single result
        """
        half = ct.default_nb // 2
        collection_w, partition_w, df_partition, df_default = \
            self.insert_entities_into_two_partitions_in_half(half)

        term_expr = f'{ct.default_int64_field_name} in [{half}]'
        # half entity in _default
        res, _ = collection_w.query(term_expr,
                                    partition_names=[ct.default_partition_name, partition_w.name])
        assert len(res) == 1
        assert res[0][ct.default_int64_field_name] == half

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_growing_segment_data(self):
        """
        target: test query data in the growing segment
        method: 1. create collection
                2.load collection
                3.insert without flush
                4.query
        expected: Data can be queried
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # load collection
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        tmp_nb = 100
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        res = df.iloc[1:2, :1].to_dict('records')
        time.sleep(1)
        collection_w.query(f'{ct.default_int64_field_name} in [1]',
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not support default_value now")
    def test_query_using_all_types_of_default_value(self):
        """
        target: test create collection with default_value
        method: create a schema with all fields using default value and query
        expected: query results are as expected
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_int8_field(default_value=np.int8(8)),
            cf.gen_int16_field(default_value=np.int16(16)),
            cf.gen_int32_field(default_value=np.int32(32)),
            cf.gen_int64_field(default_value=np.int64(64)),
            cf.gen_float_field(default_value=np.float32(3.14)),
            cf.gen_double_field(default_value=np.double(3.1415)),
            cf.gen_bool_field(default_value=False),
            cf.gen_string_field(default_value="abc")
        ]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [
            [i for i in range(ct.default_nb)],
            cf.gen_vectors(ct.default_nb, ct.default_dim)
        ]
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name)
        collection_w.load()
        expr = "pk in [0, 1]"
        res = collection_w.query(expr, output_fields=["*"])[0][0]
        log.info(res)
        assert res[ct.default_int8_field_name] == 8
        assert res[ct.default_int16_field_name] == 16
        assert res[ct.default_int32_field_name] == 32
        assert res[ct.default_int64_field_name] == 64
        assert res[ct.default_float_field_name] == np.float32(3.14)
        assert res[ct.default_double_field_name] == 3.1415
        assert res[ct.default_bool_field_name] is False
        assert res[ct.default_string_field_name] == "abc"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_multi_logical_exprs(self):
        """
        target: test the scenario which query with many logical expressions
        method: 1. create collection
                3. query the expr that like: int64 == 0 || int64 == 1 ........
        expected: run successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        multi_exprs = " || ".join(f'{default_int_field_name} == {i}' for i in range(60))
        _, check_res = collection_w.query(multi_exprs, output_fields=[f'{default_int_field_name}'])
        assert (check_res == True)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_multi_logical_exprs(self):
        """
        target: test the scenario which search with many logical expressions
        method: 1. create collection
                3. search with the expr that like: int64 == 0 || int64 == 1 ........
        expected: run successfully
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        multi_exprs = " || ".join(f'{default_int_field_name} == {i}' for i in range(60))

        collection_w.load()
        vectors_s = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        limit = 1000
        _, check_res = collection_w.search(vectors_s[:ct.default_nq], ct.default_float_vec_field_name,
                                           ct.default_search_params, limit, multi_exprs)
        assert (check_res == True)


class TestQueryString(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query with string
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_is_not_primary(self):
        """
        target: test query data with string field is not primary
        method: create collection and insert data
                collection.load()
                query with string expr in string field is not primary
        expected: query successfully
        """

        collection_w, vectors = self.init_collection_general(prefix, insert_data=True)[0:2]
        res = vectors[0].iloc[:2, :3].to_dict('records')
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.query(default_string_term_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_normal_string_expressions([default_string_field_name]))
    def test_query_string_is_primary(self, expression):
        """
        target: test query with output field only primary field
        method: specify string primary field as output field
        expected: return string primary field
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name)[0:2]
        res, _ = collection_w.query(expression, output_fields=[ct.default_string_field_name])
        assert res[0].keys() == {ct.default_string_field_name}

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_with_mix_expr(self):
        """
        target: test query data
        method: create collection and insert data
                query with mix expr in string field and int field
        expected: query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name)[0:2]
        res = vectors[0].iloc[:, 1:3].to_dict('records')
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.query(default_mix_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expression", cf.gen_invalid_string_expressions())
    def test_query_with_invalid_string_expr(self, expression):
        """
        target: test query data
        method: create collection and insert data
                query with invalid expr
        expected: Raise exception
        """
        collection_w = self.init_collection_general(prefix, insert_data=True)[0]
        collection_w.query(expression, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1100,
                                        ct.err_msg: f"failed to create query plan: cannot parse expression: {expression}"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_binary(self):
        """
        target: test query string expr with binary
        method: query string expr with binary
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             is_binary=True, is_index=False)[0:2]
        collection_w.create_index(ct.default_binary_vec_field_name, binary_index_params)
        collection_w.load()
        assert collection_w.has_index()[0]
        res, _ = collection_w.query(default_string_term_expr, output_fields=[ct.default_binary_vec_field_name])
        assert len(res) == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_prefixes(self):
        """
        target: test query with prefix string expression
        method: specify string is primary field, use prefix string expr
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name)[0:2]
        res = vectors[0].iloc[:1, :3].to_dict('records')
        expression = 'varchar like "0%"'
        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]
        collection_w.query(expression, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_alter_offset_cache_param(self):
        """
        target: test bitmap index with enable offset cache.
        expected: verify create index and load successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=default_int_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params, index_name="test_vec")
        collection_w.create_index("varchar", index_name="bitmap_offset_cache", index_params={"index_type": "BITMAP"})
        time.sleep(1)
        collection_w.load()
        expression = 'varchar like "0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len = len(result)
        collection_w.release()
        collection_w.alter_index("bitmap_offset_cache", {'indexoffsetcache.enabled': True})
        collection_w.create_index("varchar", index_name="bitmap_offset_cache", index_params={"index_type": "BITMAP"})
        collection_w.load()
        expression = 'varchar like "0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_new = len(result)
        assert res_len_new == res_len
        collection_w.release()
        collection_w.alter_index("bitmap_offset_cache", {'indexoffsetcache.enabled': False})
        collection_w.create_index("varchar", index_name="bitmap_offset_cache", index_params={"index_type": "BITMAP"})
        collection_w.load()
        expression = 'varchar like "0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_new = len(result)
        assert res_len_new == res_len
        collection_w.release()

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_prefixes_auto_index(self):
        """
        target: test query with prefix string expression and indexed with auto index
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=default_int_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name="query_expr_pre_index")
        collection_w.create_index("varchar", index_name="varchar_auto_index")
        time.sleep(1)
        collection_w.load()
        expression = 'varchar like "0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len = len(result)
        collection_w.release()
        collection_w.drop_index(index_name="varchar_auto_index")
        collection_w.load()
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_1 = len(result)
        assert res_len_1 == res_len

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_prefixes_bitmap(self):
        """
        target: test query with prefix string expression and indexed with bitmap
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=default_int_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name="query_expr_pre_index")
        collection_w.create_index("varchar", index_name="bitmap_auto_index", index_params={"index_type": "BITMAP"})
        time.sleep(1)
        collection_w.load()
        expression = 'varchar like "0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len = len(result)
        collection_w.release()
        collection_w.drop_index(index_name="varchar_bitmap_index")
        collection_w.load()
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_1 = len(result)
        assert res_len_1 == res_len

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_match_auto_index(self):
        """
        target: test query with match string expression and indexed with auto index
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=default_int_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name="query_expr_pre_index")
        collection_w.create_index("varchar", index_name="varchar_auto_index")
        time.sleep(1)
        collection_w.load()
        expression = 'varchar like "%0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len = len(result)
        collection_w.release()
        collection_w.drop_index(index_name="varchar_auto_index")
        collection_w.load()
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_1 = len(result)
        assert res_len_1 == res_len

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_expr_with_match_bitmap(self):
        """
        target: test query with match string expression and indexed with bitmap
        expected: verify query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False,
                                                             primary_field=default_int_field_name)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params,
                                  index_name="query_expr_pre_index")
        collection_w.create_index("varchar", index_name="bitmap_auto_index", index_params={"index_type": "BITMAP"})
        time.sleep(1)
        collection_w.load()
        expression = 'varchar like "%0%"'
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len = len(result)
        collection_w.release()
        collection_w.drop_index(index_name="varchar_bitmap_index")
        collection_w.load()
        result, _ = collection_w.query(expression, output_fields=['varchar'])
        res_len_1 = len(result)
        assert res_len_1 == res_len

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_string_with_invalid_prefix_expr(self):
        """
        target: test query with invalid prefix string expression
        method: specify string primary field, use invalid prefix string expr
        expected: raise error
        """
        collection_w = self.init_collection_general(prefix, insert_data=True)[0]
        expression = 'float like "0%"'
        collection_w.query(expression,
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535,
                                        ct.err_msg: f"cannot parse expression: {expression}, error: like "
                                                    f"operation on non-string or no-json field is unsupported"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_compare_two_fields(self):
        """
        target: test query with bool expression comparing two fields
        method: specify string primary field, compare two fields
        expected: verify query successfully
        """
        collection_w = self.init_collection_general(prefix, insert_data=True,
                                                    primary_field=ct.default_string_field_name)[0]
        res = []
        expression = 'float > int64'
        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]
        collection_w.query(expression, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_compare_invalid_fields(self):
        """
        target: test query with
        method: specify string primary field, compare string and int field
        expected: raise error
        """
        collection_w = self.init_collection_general(prefix, insert_data=True,
                                                    primary_field=ct.default_string_field_name)[0]
        expression = 'varchar == int64'
        collection_w.query(expression, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1100, ct.err_msg:
                               f"failed to create query plan: cannot parse expression: {expression}, "
                               f"error: comparisons between VarChar and Int64 are not supported: invalid parameter"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 24637")
    def test_query_after_insert_multi_threading(self):
        """
        target: test data consistency after multi threading insert
        method: multi threads insert, and query, compare queried data with original
        expected: verify data consistency
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        thread_num = 4
        threads = []
        primary_keys = []
        df_list = []

        # prepare original data for parallel insert
        for i in range(thread_num):
            df = cf.gen_default_dataframe_data(ct.default_nb, start=i * ct.default_nb)
            df_list.append(df)
            primary_key = df[ct.default_int64_field_name].values.tolist()
            primary_keys.append(primary_key)

        def insert(thread_i):
            log.debug(f'In thread-{thread_i}')
            mutation_res, _ = collection_w.insert(df_list[thread_i])
            assert mutation_res.insert_count == ct.default_nb
            assert mutation_res.primary_keys == primary_keys[thread_i]

        for i in range(thread_num):
            x = threading.Thread(target=insert, args=(i,))
            threads.append(x)
            x.start()
        for t in threads:
            t.join()
        assert collection_w.num_entities == ct.default_nb * thread_num

        # Check data consistency after parallel insert
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        df_dict_list = []
        for df in df_list:
            df_dict_list += df.to_dict('records')
        output_fields = ["*"]
        expression = "int64 >= 0"
        collection_w.query(expression, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: df_dict_list,
                                        "pk_name": collection_w.primary_field.name,
                                        "with_vec": True})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_string_field_pk_is_empty(self):
        """
        target: test query with string expr and string field is primary
        method: create collection , string field is primary
                collection load and insert empty data with string field
                collection query uses string expr in string field
        expected: query successfully
        """
        # 1. create a collection
        schema = cf.gen_string_pk_default_collection_schema()
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), schema=schema)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        nb = 3000
        df = cf.gen_default_list_data(nb)
        df[2] = ["" for _ in range(nb)]

        collection_w.insert(df)
        assert collection_w.num_entities == nb

        string_exp = "varchar >= \"\""
        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]
        res, _ = collection_w.query(string_exp, output_fields=output_fields)

        assert len(res) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_string_field_not_primary_is_empty(self):
        """
        target: test query with string expr and string field is not primary
        method: create collection , string field is primary
                collection load and insert empty data with string field
                collection query uses string expr in string field
        expected: query successfully
        """
        # 1.  create a collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=False, is_index=False)[0:2]

        nb = 3000
        df = cf.gen_default_list_data(nb)
        df[2] = ["" for _ in range(nb)]

        collection_w.insert(df)
        assert collection_w.num_entities == nb

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        collection_w.load()

        output_fields = [default_int_field_name, default_float_field_name, default_string_field_name]

        expr = "varchar == \"\""
        res, _ = collection_w.query(expr, output_fields=output_fields)

        assert len(res) == nb

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_with_create_diskann_index(self):
        """
        target: test query after create diskann index
        method: create a collection and build diskann index
        expected: verify query result
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, is_index=False)[0:2]

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_diskann_index)
        assert collection_w.has_index()[0]

        collection_w.load()

        int_values = [0]
        term_expr = f'{ct.default_int64_field_name} in {int_values}'
        check_vec = vectors[0].iloc[:, [0]][0:len(int_values)].to_dict('records')
        collection_w.query(term_expr,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: check_vec, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_with_create_diskann_with_string_pk(self):
        """
        target: test query after create diskann index
        method: create a collection with string pk and build diskann index
        expected: verify query result
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name,
                                                             is_index=False)[0:2]
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_diskann_index)
        assert collection_w.has_index()[0]
        collection_w.load()
        res = vectors[0].iloc[:, 1:3].to_dict('records')
        output_fields = [default_float_field_name, default_string_field_name]
        collection_w.query(default_mix_expr, output_fields=output_fields,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_scalar_field(self):
        """
        target: test query with Scalar field
        method: create collection , string field is primary
                collection load and insert empty data with string field
                collection query uses string expr in string field
        expected: query successfully
        """
        # 1.  create a collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=False, is_index=False)[0:2]

        nb = 3000
        df = cf.gen_default_list_data(nb)
        df[2] = ["" for _ in range(nb)]

        collection_w.insert(df)
        assert collection_w.num_entities == nb

        collection_w.create_index(ct.default_float_vec_field_name, default_index_params)
        assert collection_w.has_index()[0]
        index_params = {}
        collection_w.create_index(ct.default_int64_field_name, index_params=index_params)

        collection_w.load()

        output_fields = [default_int_field_name, default_float_field_name]

        expr = "int64 in [2,4,6,8]"
        res, _ = collection_w.query(expr, output_fields=output_fields)

        assert len(res) == 4


class TestQueryArray(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("array_element_data_type", [DataType.INT64])
    def test_query_array_with_inverted_index(self, array_element_data_type):
        # create collection
        additional_params = {"max_length": 1000} if array_element_data_type == DataType.VARCHAR else {}
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="contains", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000,
                        **additional_params),
            FieldSchema(name="contains_any", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="contains_all", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="equals", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000,
                        **additional_params),
            FieldSchema(name="array_length_field", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="array_access", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # insert data
        train_data, query_expr = cf.prepare_array_test_data(3000, hit_rate=0.05)
        collection_w.insert(train_data)
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}
        collection_w.create_index("emb", index_params=index_params)
        for f in ["contains", "contains_any", "contains_all", "equals", "array_length_field", "array_access"]:
            collection_w.create_index(f, {"index_type": "INVERTED"})
        collection_w.load()

        for item in query_expr:
            expr = item["expr"]
            ground_truth = item["ground_truth"]
            res, _ = collection_w.query(
                expr=expr,
                output_fields=["*"],
            )
            assert len(res) == len(ground_truth)
            for i in range(len(res)):
                assert res[i]["id"] == ground_truth[i]


class TestQueryCount(TestcaseBase):
    """
    test query count(*)
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("consistency_level", [CONSISTENCY_BOUNDED, CONSISTENCY_STRONG, CONSISTENCY_EVENTUALLY])
    def test_count_consistency_level(self, consistency_level):
        """
        target: test count(*) with bounded level
        method: 1. create collection with different consistency level
                2. load collection
                3. insert and count
                4. verify count
        expected: expected count
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix),
                                                 consistency_level=consistency_level)
        # load collection
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)

        if consistency_level == CONSISTENCY_BOUNDED:
            time.sleep(ct.default_graceful_time)
        elif consistency_level == CONSISTENCY_STRONG:
            pass
        elif consistency_level == CONSISTENCY_EVENTUALLY:
            time.sleep(ct.default_graceful_time)

        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_output_field", ["count", "count(int64)", "count(**)"])
    def test_count_invalid_output_field(self, invalid_output_field):
        """
        target: test count with invalid
        method:
        expected:
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        # load collection
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # insert
        df = cf.gen_default_dataframe_data(nb=2)
        insert_res, _ = collection_w.insert(df)

        collection_w.query(expr=default_term_expr, output_fields=[invalid_output_field],
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 1,
                                        "err_msg": f"field {invalid_output_field} not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_without_loading(self):
        """
        target: test count without loading
        method: count without loading
        expected: exception
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.query(expr=default_term_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 65535,
                                        "err_msg": "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_duplicate_ids(self):
        """
        target: test count duplicate ids
        method: 1. insert duplicate ids
                2. count
                3. delete duplicate ids
                4. count
        expected: verify count
        """
        # create
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # insert duplicate ids
        tmp_nb = 100
        df = cf.gen_default_dataframe_data(tmp_nb)
        df[ct.default_int64_field_name] = 0
        collection_w.insert(df)

        # query count
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb}],"pk_name": collection_w.primary_field.name})

        # delete and verify count
        collection_w.delete(default_term_expr)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 0}], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_multi_partitions(self):
        """
        target: test count multi partitions
        method: 1. init partitions: p1, _default
                2. count p1, _default, [p1, _default]
                3. delete _default entities and count _default, [p1, _default]
                4. drop p1 and count p1, [p1, _default]
        expected: verify count
        """
        half = ct.default_nb // 2
        # insert [0, half) into partition_w, [half, nb) into _default
        collection_w, p1, _, _ = self.insert_entities_into_two_partitions_in_half(half=half)

        # query count p1, [p1, _default]
        for p_name in [p1.name, ct.default_partition_name]:
            collection_w.query(expr=default_expr, output_fields=[ct.default_count_output], partition_names=[p_name],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: half}], "pk_name": collection_w.primary_field.name})

        # delete entities from _default
        delete_expr = f"{ct.default_int64_field_name} in {[i for i in range(half, ct.default_nb)]} "
        collection_w.delete(expr=delete_expr)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           partition_names=[ct.default_partition_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 0}], "pk_name": collection_w.primary_field.name})
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           partition_names=[p1.name, ct.default_partition_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: half}], "pk_name": collection_w.primary_field.name})

        # drop p1 partition
        p1.release()
        p1.drop()
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           partition_names=[p1.name],
                           check_task=CheckTasks.err_res,
                           check_items={"err_code": 65535,
                                        "err_msg": f'partition name {p1.name} not found'})
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           partition_names=[ct.default_partition_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 0}], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_partition_duplicate(self):
        """
        target: test count from partitions which have duplicate ids
        method: 1. insert same ids into 2 partitions
                2. count
                3. delete some ids and count
        expected: verify count
        """
        # init partitions: _default and p1
        p1 = "p1"
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_partition(p1)

        df = cf.gen_default_dataframe_data()
        collection_w.insert(df, partition_name=ct.default_partition_name)
        collection_w.insert(df, partition_name=p1)

        # index and load
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # count
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb * 2}],
                                        "pk_name": collection_w.primary_field.name}
                           )

        # delete some duplicate ids
        delete_res, _ = collection_w.delete(default_term_expr)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           partition_names=[p1],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb - delete_res.delete_count}],
                                        "pk_name": collection_w.primary_field.name}
                           )

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_growing_sealed_segment(self):
        """
        target: test count growing and sealed segment
        method: 1. insert -> index -> load
                2. count
                3. new insert
                4. count
        expected: verify count
        """
        tmp_nb = 100
        # create -> insert -> index -> load -> count sealed
        collection_w = self.init_collection_general(insert_data=True, nb=tmp_nb)[0]
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb}], "pk_name": collection_w.primary_field.name})

        # new insert and growing count
        df = cf.gen_default_dataframe_data(nb=tmp_nb, start=tmp_nb)
        collection_w.insert(df)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb * 2}], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_during_handoff(self):
        """
        target: test count during handoff
        method: 1. index -> load
                2. insert
                3. flush while count
        expected: verify count
        """
        # create -> index -> load
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # flush while count
        df = cf.gen_default_dataframe_data()
        collection_w.insert(df)

        t_flush = threading.Thread(target=collection_w.flush, args=())
        t_count = threading.Thread(target=collection_w.query, args=(default_expr,),
                                   kwargs={
                                       "output_fields": [ct.default_count_output],
                                       "check_task": CheckTasks.check_query_results,
                                       "check_items": {exp_res: [{count: ct.default_nb}],
                                                       "pk_name": collection_w.primary_field.name}})

        t_flush.start()
        t_count.start()
        t_flush.join()
        t_count.join()

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_delete_insert_duplicate_ids(self):
        """
        target: test count after delete and re-insert same entities
        method: 1. insert and delete
                2. count
                3. re-insert deleted ids with different vectors
                4. count
        expected: verify count
        """
        tmp_nb = 100
        # create -> insert ids [0, default_nb + tmp) -> index -> load
        collection_w = self.init_collection_general(insert_data=True)[0]
        df = cf.gen_default_dataframe_data(nb=tmp_nb, start=ct.default_nb)
        insert_res, _ = collection_w.insert(df)

        # delete growing and sealed ids -> count
        collection_w.delete(f"{ct.default_int64_field_name} in {[i for i in range(ct.default_nb)]}")
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb}],
                                        "pk_name": collection_w.primary_field.name}
                           )

        # re-insert deleted ids [0, default_nb) with different vectors
        df_same = cf.gen_default_dataframe_data()
        collection_w.insert(df_same)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb + tmp_nb}],
                                        "pk_name": collection_w.primary_field.name}
                           )

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_compact_merge(self):
        """
        target: test count after compact merge segments
        method: 1. init 2 segments with same channel
                2. compact
                3. count
        expected: verify count
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)

        # init two segments
        tmp_nb = 100
        segment_num = 2
        for i in range(segment_num):
            df = cf.gen_default_dataframe_data(nb=tmp_nb, start=i * tmp_nb)
            collection_w.insert(df)
            collection_w.flush()

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()

        collection_w.load()
        segment_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        assert len(segment_info) == 1

        # count after compact
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb * segment_num}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_compact_delete(self):
        """
        target: test count after delete-compact
        method: 1. init segments
                2. delete half ids and compact
                3. count
        expected: verify count
        """
        # create -> index -> insert
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        # delete half entities, flush
        half_expr = f'{ct.default_int64_field_name} in {[i for i in range(ct.default_nb // 2)]}'
        collection_w.delete(half_expr)
        assert collection_w.num_entities == ct.default_nb

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()

        # load and count
        collection_w.load()
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb // 2}],
                                        "pk_name": collection_w.primary_field.name}
                           )

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_during_compact(self):
        """
        target: test count during compact merge many small segments
        method: 1. init many small segments
                2. compact while count
        expected: verify count
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)

        # init 2 segments
        tmp_nb = 100
        for i in range(10):
            df = cf.gen_default_dataframe_data(tmp_nb, start=i * tmp_nb)
            collection_w.insert(df)
            collection_w.flush()

        # compact while count
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()

        t_compact = threading.Thread(target=collection_w.compact, args=())
        t_count = threading.Thread(target=collection_w.query, args=(default_expr,),
                                   kwargs={
                                       "output_fields": [ct.default_count_output],
                                       "check_task": CheckTasks.check_query_results,
                                       "check_items": {exp_res: [{count: tmp_nb * 10}],
                                                       "pk_name": collection_w.primary_field.name}
                                   })

        t_compact.start()
        t_count.start()
        t_count.join()
        t_count.join()

    @pytest.mark.tags(CaseLabel.L0)
    def test_count_with_expr(self):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w = self.init_collection_general(insert_data=True)[0]

        # count with expr
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})

        collection_w.query(expr=default_term_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 2}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_count_expr_json(self):
        """
        target: test query with part json key value
        method: 1. insert data and some entities doesn't have number key
                2. query count with number expr filet
        expected: succeed
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, enable_dynamic_field=True, with_json=True)[0]

        # 2. insert data
        array = cf.gen_default_rows_data(with_json=False)
        for i in range(ct.default_nb):
            if i % 2 == 0:
                array[i][json_field] = {"string": str(i), "bool": bool(i)}
            else:
                array[i][json_field] = {"string": str(i), "bool": bool(i), "number": i}

        collection_w.insert(array)
        time.sleep(0.4)
        # 3. query
        collection_w.load()
        expression = f'{ct.default_json_field_name}["number"] < 100'
        collection_w.query(expression, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 50}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_expr_on_search_n_query(self):
        """
        target: verify more expressions of json object, json array and json texts are supported in search and query
        method: 1. insert data with vectors and different json format
                2. verify insert successfully
                3. build index and load
                4. search and query with different expressions
                5. verify search and query successfully
        expected: succeed
        """
        # 1. initialize with data
        c_name = cf.gen_unique_str()
        json_int = "json_int"
        json_float = "json_float"
        json_string = "json_string"
        json_bool = "json_bool"
        json_array = "json_array"
        json_embedded_object = "json_embedded_object"
        json_objects_array = "json_objects_array"
        dim = 16
        fields = [cf.gen_int64_field(), cf.gen_float_vec_field(dim=dim),
                  cf.gen_json_field(json_int), cf.gen_json_field(json_float), cf.gen_json_field(json_string),
                  cf.gen_json_field(json_bool), cf.gen_json_field(json_array),
                  cf.gen_json_field(json_embedded_object), cf.gen_json_field(json_objects_array)]
        schema = cf.gen_collection_schema(fields=fields, primary_field=ct.default_int64_field_name, auto_id=True)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # 2. insert data
        nb = 1000
        for i in range(10):
            data = [
                cf.gen_vectors(nb, dim),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_int),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_float),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_string),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_bool),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_array),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_embedded_object),
                cf.gen_json_data_for_diff_json_types(nb=nb, start=i * nb, json_type=json_objects_array)
            ]
            collection_w.insert(data)
        time.sleep(0.4)
        # 3. build index and load
        collection_w.create_index(ct.default_float_vec_field_name, index_params=default_index_params)
        collection_w.load()

        # 4. search and query with different expressions. All the expressions will return 10 results
        query_exprs = [
            f'json_contains_any({json_embedded_object}["{json_embedded_object}"]["level2"]["level2_array"], [1,3,5,7,9])',
            f'json_contains_any({json_embedded_object}["array"], [1,3,5,7,9])',
            f'{json_int} < 10',
            f'{json_float} <= 200.0 and {json_float} > 190.0',
            f'{json_string} in ["1","2","3","4","5","6","7","8","9","10"]',
            f'{json_bool} == true and {json_float} <= 10',
            f'{json_array} == [4001,4002,4003,4004,4005,4006,4007,4008,4009,4010] or {json_int} < 9',
            f'{json_embedded_object}["{json_embedded_object}"]["number"] < 10',
            f'{json_objects_array}[0]["level2"]["level2_str"] like "199%" and {json_objects_array}[1]["float"] >= 1990'
        ]
        search_data = cf.gen_vectors(2, dim)
        search_param = {}
        for expr in query_exprs:
            log.debug(f"query_expr: {expr}")
            collection_w.query(expr=expr, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: 10}],
                                            "pk_name": collection_w.primary_field.name})
            collection_w.search(data=search_data, anns_field=ct.default_float_vec_field_name,
                                param=search_param, limit=10, expr=expr,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 2, "limit": 10})

        # verify for issue #36718
        for expr in [f'{json_embedded_object}["{json_embedded_object}"]["number"] in []',
                     f'{json_embedded_object}["{json_embedded_object}"] in []']:
            log.debug(f"query_expr: {expr}")
            collection_w.query(expr=expr, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: 0}],
                                            "pk_name": collection_w.primary_field.name})
            collection_w.search(data=search_data, anns_field=ct.default_float_vec_field_name,
                                param=search_param, limit=10, expr=expr,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 2, "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_with_pagination_param(self):
        """
        target: test count with pagination params
        method: count with pagination params: offset, limit
        expected: exception
        """
        # create -> insert -> index -> load
        collection_w = self.init_collection_general(insert_data=True)[0]

        # only params offset is not considered pagination
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output], offset=10,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})
        # count with limit
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output], limit=10,
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "count entities with pagination is not allowed"}
                           )
        # count with pagination params
        collection_w.query(default_expr, output_fields=[ct.default_count_output], offset=10, limit=10,
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "count entities with pagination is not allowed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_alias_insert_delete_drop(self):
        """
        target: test count after alias insert and load
        method: 1. init collection
                2. alias insert more entities
                3. count and alias count
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w = self.init_collection_general(insert_data=True)[0]

        # create alias

        alias = cf.gen_unique_str("alias")
        self.utility_wrap.create_alias(collection_w.name, alias)
        collection_w_alias = self.init_collection_wrap(name=alias)

        # new insert partitions and count
        p_name = cf.gen_unique_str("p_alias")
        collection_w_alias.create_partition(p_name)
        collection_w_alias.insert(cf.gen_default_dataframe_data(start=ct.default_nb), partition_name=p_name)
        collection_w_alias.query(expr=default_expr, output_fields=[ct.default_count_output],
                                 check_task=CheckTasks.check_query_results,
                                 check_items={exp_res: [{count: ct.default_nb * 2}],
                                              "pk_name": collection_w.primary_field.name})

        # release collection and alias drop partition
        collection_w_alias.drop_partition(p_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 65535,
                                                       ct.err_msg: "partition cannot be dropped, "
                                                                   "partition is loaded, please release it first"})
        self.partition_wrap.init_partition(collection_w_alias.collection, p_name)
        self.partition_wrap.release()

        collection_w_alias.drop_partition(p_name)
        res, _ = collection_w_alias.has_partition(p_name)
        assert res is False
        collection_w_alias.query(expr=default_expr, output_fields=[ct.default_count_output],
                                 check_task=CheckTasks.check_query_results,
                                 check_items={exp_res: [{count: ct.default_nb}],
                                              "pk_name": collection_w.primary_field.name})

        # alias delete and count
        collection_w_alias.delete(f"{ct.default_int64_field_name} in {[i for i in range(ct.default_nb)]}")
        collection_w_alias.query(expr=default_expr, output_fields=[ct.default_count_output],
                                 check_task=CheckTasks.check_query_results,
                                 check_items={exp_res: [{count: 0}],
                                              "pk_name": collection_w.primary_field.name})

        collection_w_alias.drop(check_task=CheckTasks.err_res,
                                check_items={ct.err_code: 1,
                                             ct.err_msg: "cannot drop the collection via alias"})
        self.utility_wrap.drop_alias(alias)
        collection_w.drop()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("is_growing", [True, False])
    def test_count_upsert_growing_sealed(self, is_growing):
        """
        target: test count after upsert growing
        method: 1. create -> index -> load -> insert -> delete
                2. upsert deleted id and count (+1)
                3. upsert new id and count (+1)
                4. upsert existed id and count (+0)
        expected: verify count
        """
        if is_growing:
            # create -> index -> load -> insert -> delete
            collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
            collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
            collection_w.load()
            collection_w.insert(cf.gen_default_dataframe_data())

            # delete one entity
            single_expr = f'{ct.default_int64_field_name} in [0]'
            collection_w.delete(single_expr)
        else:
            # create -> insert -> delete -> index -> load
            collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
            collection_w.insert(cf.gen_default_dataframe_data())

            # delete one entity
            single_expr = f'{ct.default_int64_field_name} in [0]'
            collection_w.delete(single_expr)

            collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
            collection_w.load()

        # upsert deleted id
        df_zero = cf.gen_default_dataframe_data(nb=1)
        collection_w.upsert(df_zero)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})

        # upsert new id and count
        df_new = cf.gen_default_dataframe_data(nb=1, start=ct.default_nb)
        collection_w.upsert(df_new)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb + 1}],
                                        "pk_name": collection_w.primary_field.name})

        # upsert existed id and count
        df_existed = cf.gen_default_dataframe_data(nb=1, start=10)
        collection_w.upsert(df_existed)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb + 1}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_upsert_duplicate(self):
        """
        target: test count after upsert duplicate
        method: 1. insert many duplicate ids
                2. upsert id and count
                3. delete id and count
                4. upsert deleted id and count
        expected: verify count
        """
        # init collection and insert same ids
        tmp_nb = 100
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=tmp_nb)
        df[ct.default_int64_field_name] = 0
        collection_w.insert(df)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # upsert id and count
        df_existed = cf.gen_default_dataframe_data(nb=tmp_nb, start=0)
        collection_w.upsert(df_existed)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb}],
                                        "pk_name": collection_w.primary_field.name}
                           )

        # delete id and count
        delete_res, _ = collection_w.delete(default_term_expr)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb - delete_res.delete_count}],
                                        "pk_name": collection_w.primary_field.name})

        # upsert deleted id and count
        df_deleted = cf.gen_default_dataframe_data(nb=delete_res.delete_count, start=0)
        collection_w.upsert(df_deleted)
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: tmp_nb}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_rename_collection(self):
        """
        target: test count after rename collection
        method: 1. create -> insert -> index -> load
                2. rename collection
                3. count
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w = self.init_collection_general(insert_data=True)[0]
        new_name = cf.gen_unique_str("new_name")
        self.utility_wrap.rename_collection(collection_w.name, new_name)
        self.collection_wrap.init_collection(new_name)
        self.collection_wrap.query(expr=default_expr, output_fields=[ct.default_count_output],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={exp_res: [{count: ct.default_nb}],
                                                "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_disable_growing_segments(self):
        """
        target: test count when disable growing segments
        method: 1. create -> index -> load -> insert
                2. query count with ignore_growing
        expected: verify count 0
        """
        # create -> index -> load
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # insert
        collection_w.insert(cf.gen_default_dataframe_data(nb=100))
        collection_w.query(expr=default_expr, output_fields=[ct.default_count_output], ignore_growing=True,
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 0}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_expressions(self):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w, _vectors, _, insert_ids = self.init_collection_general(insert_data=True)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"query with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                int64 = _vectors.int64[i]
                float = _vectors.float[i]
                if not expr or eval(expr):
                    filter_ids.append(_id)
            res = len(filter_ids)

            # count with expr
            collection_w.query(expr=expr, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: res}],
                                            "pk_name": collection_w.primary_field.name})

            # count agian with expr template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            collection_w.query(expr=expr, expr_params=expr_params, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: res}],
                                            "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_count_bool_expressions(self, bool_type):
        """
        target: test count with binary expr
        method: count with binary expr
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(insert_data=True, is_all_data_type=True)[0:4]

        # filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i in range(len(_vectors[0])):
            if _vectors[0][i].dtypes == bool:
                num = i
                break

        for i, _id in enumerate(insert_ids):
            if _vectors[0][num][i] == bool_type_cmp:
                filter_ids.append(_id)
        res = len(filter_ids)

        # count with expr
        expression = f"{ct.default_bool_field_name} == {bool_type}"
        collection_w.query(expr=expression, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: res}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_expression_auto_field(self):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w, _vectors, _, insert_ids = self.init_collection_general(insert_data=True)[0:4]

        # filter result with expression in collection
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"query with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                float = _vectors.float[i]
                if not expr or eval(expr):
                    filter_ids.append(_id)
            res = len(filter_ids)

            # count with expr
            collection_w.query(expr=expr, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: res}],
                                            "pk_name": collection_w.primary_field.name})
            # count with expr and expr_params
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            collection_w.query(expr=expr, expr_params=expr_params, output_fields=[count],
                               check_task=CheckTasks.check_query_results,
                               check_items={exp_res: [{count: res}],
                                            "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_count_expression_all_datatype(self):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        # create -> insert -> index -> load
        collection_w = self.init_collection_general(insert_data=True, is_all_data_type=True)[0]

        # count with expr
        expr = "int64 >= 0 && int32 >= 1999 && int16 >= 0 && int8 <= 0 && float <= 1999.0 && double >= 0"
        collection_w.query(expr=expr, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 1}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_count_expression_comparative(self):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        # create -> insert -> index -> load
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_float_vec_field()]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(schema=schema)

        nb, res = 10, 0
        int_values = [random.randint(0, nb) for _ in range(nb)]
        data = [[i for i in range(nb)], int_values, cf.gen_vectors(nb, ct.default_dim)]
        collection_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name)
        collection_w.load()

        for i in range(nb):
            res = res + 1 if i >= int_values[i] else res

        # count with expr
        expression = "int64_1 >= int64_2"
        collection_w.query(expr=expression, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: res}],
                                        "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_counts_expression_sparse_vectors(self, index):
        """
        target: test count with expr
        method: count with expr
        expected: verify count
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        schema = cf.gen_default_sparse_schema()
        collection_w = self.init_collection_wrap(c_name, schema=schema)
        data = cf.gen_default_list_sparse_data()
        collection_w.insert(data)
        params = cf.get_index_params_params(index)
        index_params = {"index_type": index, "metric_type": "IP", "params": params}
        collection_w.create_index(ct.default_sparse_vec_field_name, index_params, index_name=index)
        collection_w.load()
        collection_w.query(expr=default_expr, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: ct.default_nb}],
                                        "pk_name": collection_w.primary_field.name})
        expr = "int64 > 50 && int64 < 100 && float < 75"
        collection_w.query(expr=expr, output_fields=[count],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [{count: 24}],
                                        "pk_name": collection_w.primary_field.name})
        batch_size = 100
        collection_w.query_iterator(batch_size=batch_size, expr=default_expr,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "batch_size": batch_size,
                                                 "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.repeat(3)
    @pytest.mark.skip(reason="issue #36538")
    def test_count_query_search_after_release_partition_load(self):
        """
        target: test query count(*) after release collection and load partition
        method: 1. create a collection and 2 partitions with nullable and default value fields
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=True)[0]
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 200}],
                                        "pk_name": collection_w.primary_field.name})
        collection_w.release()
        partition_w1, partition_w2 = collection_w.partitions
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        # partition_w1.load()
        collection_w.load(partition_names=[partition_w1.name])
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:1], ct.default_float_vec_field_name, ct.default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})


class TestQueryNoneAndDefaultData(TestcaseBase):
    """
    test Query interface with none and default data
    query(collection_name, expr, output_fields=None, partition_names=None, timeout=None)
    """

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def numeric_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["TRIE", "INVERTED", "BITMAP"])
    def varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_normal_with_none_data(self, enable_dynamic_field, null_data_percent):
        """
        target: test query with none data
        method: query with term expr with nullable fields, insert data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 default_float_field_name: null_data_percent})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_expr_none_with_none_data(self, enable_dynamic_field, null_data_percent):
        """
        target: test query by none expr with nullable fields, insert data including none
        method: query by expr None after inserting data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 default_float_field_name: null_data_percent})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f''
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           limit=pos, check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_by_nullable_field_with_none_data(self):
        """
        target: test query with nullable fields expr, insert data including none into nullable Fields
        method: query by nullable field expr after inserting data including none
        expected: verify query result
        """
        # create collection, insert default_nb, load collection
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True, enable_dynamic_field=True,
                                                             nullable_fields={default_float_field_name: 0.5})[0:2]
        pos = 5
        int_values, float_values = [], []
        for vector in vectors[0]:
            int_values.append(vector[ct.default_int64_field_name])
            float_values.append(vector[default_float_field_name])
        res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'{default_float_field_name} < {pos}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_after_none_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index,
                                                      null_data_percent):
        """
        target: test query after different index on scalar fields
        method: query after different index on nullable fields
        expected: verify query result
        """
        # 1. initialize with data
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        # 2. create collection, insert default_nb
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, is_all_data_type=True, is_index=False,
                                                             nullable_fields=nullable_fields)[0:2]
        # 3. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 4. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 5. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        if numeric_scalar_index != "STL_SORT":
            collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'0 <= {ct.default_int64_field_name} < {pos}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_default_value_with_insert(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        # 2. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_default_value_without_insert(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, no insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, False, enable_dynamic_field=enable_dynamic_field,
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]

        term_expr = f'{ct.default_int64_field_name} > 0'
        # 2. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: [], "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_after_default_data_all_field_datatype(self, varchar_scalar_index, numeric_scalar_index):
        """
        target: test query after different index on default value data
        method: test query after different index on default value and corresponding search params
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        default_value_fields = {ct.default_int32_field_name: np.int32(1),
                                ct.default_int16_field_name: np.int32(2),
                                ct.default_int8_field_name: np.int32(3),
                                ct.default_bool_field_name: True,
                                ct.default_float_field_name: np.float32(10.0),
                                ct.default_double_field_name: 10.0,
                                ct.default_string_field_name: "1"}
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, partition_num=1, is_all_data_type=True,
                                                             is_index=False, default_value_fields=default_value_fields)[
                                0:2]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int32_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int16_field_name, scalar_index_params)
        collection_w.create_index(ct.default_int8_field_name, scalar_index_params)
        if numeric_scalar_index != "STL_SORT":
            collection_w.create_index(ct.default_bool_field_name, scalar_index_params)
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'0 <= {ct.default_int64_field_name} < {pos}'
        # 5. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36003")
    def test_query_both_default_value_non_data(self, enable_dynamic_field):
        """
        target: test query normal case with default value set
        method: create connection, collection with default value set, insert and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={ct.default_float_field_name: 1},
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_float_field_name} in [10.0]'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           limit=pos, check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    def test_query_after_different_index_with_params_none_default_data(self, varchar_scalar_index, numeric_scalar_index,
                                                                       null_data_percent):
        """
        target: test query after different index
        method: test query after different index on none default data
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, 1000, partition_num=1,
                                                             is_all_data_type=True, is_index=False,
                                                             nullable_fields={
                                                                 ct.default_string_field_name: null_data_percent},
                                                             default_value_fields={
                                                                 ct.default_float_field_name: np.float32(10.0)})[0:2]
        # 2. create index on vector field and load
        index = "HNSW"
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field with None data
        scalar_index_params = {"index_type": varchar_scalar_index, "params": {}}
        collection_w.create_index(ct.default_string_field_name, scalar_index_params)
        # 4. create index on scalar field with default data
        scalar_index_params = {"index_type": numeric_scalar_index, "params": {}}
        collection_w.create_index(ct.default_float_field_name, scalar_index_params)
        collection_w.load()
        pos = 5
        int64_values, float_values = [], []
        scalar_fields = vectors[0]
        for i in range(pos):
            int64_values.append(scalar_fields[0][i])
            float_values.append(scalar_fields[5][i])
        res = [{ct.default_int64_field_name: int64_values[i], default_float_field_name: float_values[i]} for i in
               range(pos)]

        term_expr = f'{ct.default_int64_field_name} in {int64_values[:pos]}'
        # 5. query
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_iterator_with_none_data(self, null_data_percent):
        """
        target: test query iterator normal with none data
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        batch_size = 100
        collection_w = self.init_collection_general(prefix, True, is_index=False,
                                                    nullable_fields={ct.default_string_field_name: null_data_percent})[
            0]
        collection_w.create_index(ct.default_float_vec_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        expr = "int64 >= 0"
        collection_w.query_iterator(batch_size, expr=expr,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "pk_name": collection_w.primary_field.name,
                                                 "batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36123")
    def test_query_normal_none_data_partition_key(self, enable_dynamic_field, null_data_percent):
        """
        target: test query normal case with none data inserted
        method: create connection, collection with nullable fields, insert data including none, and query
        expected: query successfully and verify query result
        """
        # 1. initialize with data
        collection_w, vectors = self.init_collection_general(prefix, True, enable_dynamic_field=enable_dynamic_field,
                                                             nullable_fields={
                                                                 ct.default_float_field_name: null_data_percent},
                                                             is_partition_key=ct.default_float_field_name)[0:2]
        pos = 5
        if enable_dynamic_field:
            int_values, float_values = [], []
            for vector in vectors[0]:
                int_values.append(vector[ct.default_int64_field_name])
                float_values.append(vector[default_float_field_name])
            res = [{ct.default_int64_field_name: int_values[i], default_float_field_name: float_values[i]} for i in
                   range(pos)]
        else:
            int_values = vectors[0][ct.default_int64_field_name].values.tolist()
            res = vectors[0].iloc[0:pos, :2].to_dict('records')

        term_expr = f'{ct.default_int64_field_name} in {int_values[:pos]}'
        collection_w.query(term_expr, output_fields=[ct.default_int64_field_name, default_float_field_name],
                           check_task=CheckTasks.check_query_results,
                           check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #36538")
    def test_query_none_count(self, null_data_percent):
        """
        target: test query count(*) with None and default data
        method: 1. create a collection and 2 partitions with nullable and default value fields
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=True,
                                                    nullable_fields={ct.default_float_field_name: null_data_percent},
                                                    default_value_fields={ct.default_string_field_name: "data"})[0]
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 200}],
                                        "pk_name": collection_w.primary_field.name})
        collection_w.release()
        partition_w1, partition_w2 = collection_w.partitions
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        # partition_w1.load()
        collection_w.load(partition_names=[partition_w1.name])
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}],
                                        "pk_name": collection_w.primary_field.name})
        vectors = [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]
        collection_w.search(vectors[:1], ct.default_float_vec_field_name, ct.default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})


class TestQueryTextMatch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query text match
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["standard"])
    def test_query_text_match_en_normal(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any([token in r[field] for token in top_10_tokens])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("lang_type", ["chinese"])
    def test_query_text_match_zh_normal(
            self, lang_type, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "type": lang_type,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if lang_type == "chinese":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        # query with blank space and punctuation marks
        for field in text_fields:
            expr = f"text_match({field}, ' ') or text_match({field}, ',') or text_match({field}, '.')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) == 0

        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["icu"])
    def test_query_text_match_with_icu_tokenizer(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match with icu tokenizer
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = ICUTextGenerator()
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents_with_analyzer_params(df[field].tolist(), analyzer_params)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            for r in res:
                assert any([token in r[field] for token in top_10_tokens])


    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("tokenizer", ["jieba", "standard"])
    def test_query_text_match_with_growing_segment(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # generate growing segment
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        time.sleep(3)
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0

        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0

        # flush and then query again
        collection_w.flush()
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0


    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("lang_type", ["chinese"])
    def test_query_text_match_zh_en_mix(
            self, lang_type, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "type": lang_type,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 3000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if lang_type == "chinese":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() + " " + fake_en.word().lower(),
                "sentence": fake.sentence().lower() + " " + fake_en.sentence().lower(),
                "paragraph": fake.paragraph().lower() + " " + fake_en.paragraph().lower(),
                "text": fake.text().lower() + " " + fake_en.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one token
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]

            # verify inverted index
            if enable_inverted_index:
                if field == "word":
                    expr = f"{field} == '{token}'"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    log.info(f"res len {len(res)}")
                    for r in res:
                        assert r[field] == token
        # query single field for multi-word
        for field in text_fields:
            # match top 10 most common words
            top_10_tokens = []
            for word, count in wf_map[field].most_common(10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

        # query single field for multi-word
        for field in text_fields:
            # match latest 10 most common  english words
            top_10_tokens = []
            for word, count in cf.get_top_english_tokens(wf_map[field], 10):
                top_10_tokens.append(word)
            string_of_top_10_words = " ".join(top_10_tokens)
            expr = f"text_match({field}, '{string_of_top_10_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any(
                    [token in r[field] for token in top_10_tokens]), f"top 10 tokens {top_10_tokens} not in {r[field]}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_stop_words(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        stops_words = ["in", "of"]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                       {
                           "type": "stop",
                           "stop_words": stops_words,
                       }],
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence().lower() + " ".join(stops_words),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            for token in stops_words:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(res)}")
                assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_lowercase(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["lowercase"],
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            tokens =[item[0] for item in wf_map[field].most_common(1)]
            for token in tokens:
                # search with Capital case
                token = token.capitalize()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                capital_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(capital_case_res)}")
                # search with lower case
                token = token.lower()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                lower_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(lower_case_res)}")

                # search with upper case
                token = token.upper()
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                upper_case_res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(upper_case_res)}")
                assert len(capital_case_res) == len(lower_case_res)  and len(capital_case_res) == len(upper_case_res)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_length_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "length",  # Specifies the filter type as length
                    "max": 10,  # Sets the maximum token length to 10 characters
                }
            ],
        }

        long_word = "a" * 11
        max_length_word = "a" * 10
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + long_word + " " + max_length_word,
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with long word
        for field in text_fields:
            tokens =[long_word]
            for token in tokens:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                assert len(res) == 0
        # query sentence field with max length word
        for field in text_fields:
            tokens =[max_length_word]
            for token in tokens:
                expr = f"text_match({field}, '{token}')"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                assert len(res) == data_size


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_stemmer_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
            "filter": [{
                "type": "stemmer",  # Specifies the filter type as stemmer
                "language": "english",  # Sets the language for stemming to English
            }]
        }
        word_pairs = {
            "play": ['play', 'plays', 'played', 'playing'],
            "book": ['book', 'books', 'booked', 'booking'],
            "study": ['study', 'studies', 'studied', 'studying'],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(word_pairs.keys()),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with variant word
        for field in text_fields:
            for stem in word_pairs.keys():
                tokens = word_pairs[stem]
                for token in tokens:
                    expr = f"text_match({field}, '{token}')"
                    log.info(f"expr: {expr}")
                    res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                    pytest.assume(len(res) == data_size, f"stem {stem} token {token} not found in {res}")


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_ascii_folding_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        from unidecode import unidecode
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["asciifolding"],
        }

        origin_texts = [
            "Café Möller serves crème brûlée",
            "José works at Škoda in São Paulo",
            "The œuvre of Łukasz includes æsthetic pieces",
            "München's König Street has günstig prices",
            "El niño está jugando en el jardín",
            "Le système éducatif français"
        ]

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(origin_texts),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query sentence field with variant word
        for field in text_fields:
            for text in origin_texts:
                ascii_folding_text = unidecode(text)
                expr = f"""text_match({field}, "{ascii_folding_text}")"""
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                pytest.assume(len(res) == data_size, f"origin {text} ascii_folding text {ascii_folding_text} not found in {res}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_decompounder_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        word_list = ["dampf", "schiff", "fahrt", "brot", "backen", "automat"]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["lowercase",
                {
                "type": "decompounder",  # Specifies the filter type as decompounder
                "word_list": word_list,  # Sets the word list for decompounding
            }],
        }

        origin_texts = [
            "Die tägliche Dampfschifffahrt von Hamburg nach Oslo startet um sechs Uhr morgens.",
            "Unser altes Dampfschiff macht eine dreistündige Rundfahrt durch den Hafen.",
            "Der erfahrene Dampfschifffahrtskapitän kennt jede Route auf dem Fluss.",
            "Die internationale Dampfschifffahrtsgesellschaft erweitert ihre Flotte.",
            "Während der Dampfschifffahrt können Sie die Küstenlandschaft bewundern.",
            "Der neue Brotbackautomat produziert stündlich frische Brötchen.",
            "Im Maschinenraum des Dampfschiffs steht ein moderner Brotbackautomat.",
            "Die Brotbackautomatentechnologie wird ständig verbessert.",
            "Unser Brotbackautomat arbeitet mit traditionellen Rezepten.",
            "Der programmierbare Brotbackautomat bietet zwanzig verschiedene Programme.",
        ]

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(origin_texts),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(word_list)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == data_size, f"res len {len(res)}, data size {data_size}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_alphanumonly_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        common_non_ascii = [
            'é',  # common in words like café, résumé
            '©',  # copyright
            '™',  # trademark
            '®',  # registered trademark
            '°',  # degrees, e.g. 20°C
            '€',  # euro currency
            '£',  # pound sterling
            '±',  # plus-minus sign
            '→',  # right arrow
            '•'  # bullet point
        ]
        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["alphanumonly"],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(common_non_ascii),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(common_non_ascii)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == 0, f"res len {len(res)}, data size {data_size}")


    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_custom_analyzer_with_cncharonly_filter(self):
        """
        target: test text match with custom analyzer
        method: 1. enable text match, use custom analyzer and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        non_zh_char_word_list = ["hello", "milvus", "vector", "database", "19530"]

        analyzer_params = {
            "tokenizer": "standard",
            "filter": ["cncharonly"],
        }

        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "sentence": fake.sentence() + " " + " ".join(non_zh_char_word_list),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = " ".join(non_zh_char_word_list)
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pytest.assume(len(res) == 0, f"res len {len(res)}, data size {data_size}")

    @pytest.mark.parametrize("dict_kind", ["ipadic", "ko-dic", "cc-cedict"])
    def test_query_text_match_with_Lindera_tokenizer(self, dict_kind):
        """
        target: test text match with lindera tokenizer
        method: 1. enable text match, use lindera tokenizer and insert data with varchar in different lang
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": {
            "type": "lindera",
            "dict_kind": dict_kind
            }
        }
        if dict_kind == "ipadic":
            fake = fake_jp
        elif dict_kind == "ko-dic":
            fake = KoreanTextGenerator()
        elif dict_kind == "cc-cedict":
            fake = fake_zh
        else:
            fake = fake_en
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        data = [
            {
                "id": i,
                "sentence": fake.sentence(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup
        text_fields = ["sentence"]
        # query sentence field with word list
        for field in text_fields:
            match_text = df["sentence"].iloc[0]
            expr = f"text_match({field}, '{match_text}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            assert len(res) > 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_combined_expression_for_single_field(self):
        """
        target: test query text match with combined expression for single field
        method: 1. enable text match, and insert data with varchar
                2. get the most common words and form the combined expression with and operator
                3. verify the result
        expected: query successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"df \n{df}")
        log.info(f"new df \n{df_new}")
        for field in text_fields:
            expr_list = []
            wf_counter = Counter(wf_map[field])
            pd_tmp_res_list = []
            for word, count in wf_counter.most_common(2):
                tmp = f"text_match({field}, '{word}')"
                log.info(f"tmp expr {tmp}")
                expr_list.append(tmp)
                tmp_res = cf.manual_check_text_match(df_new, word, field)
                log.info(f"manual check result for  {tmp} {len(tmp_res)}")
                pd_tmp_res_list.append(tmp_res)
            log.info(f"manual res {len(pd_tmp_res_list)}, {pd_tmp_res_list}")
            final_res = set(pd_tmp_res_list[0])
            for i in range(1, len(pd_tmp_res_list)):
                final_res = final_res.intersection(set(pd_tmp_res_list[i]))
            log.info(f"intersection res {len(final_res)}")
            log.info(f"final res {final_res}")
            and_expr = " and ".join(expr_list)
            log.info(f"expr: {and_expr}")
            res, _ = collection_w.query(expr=and_expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, final res {len(final_res)}")
            assert len(res) == len(final_res)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_combined_expression_for_multi_field(self):
        """
        target: test query text match with combined expression for multi field
        method: 1. enable text match, and insert data with varchar
                2. create the combined expression with `and`, `or` and `not` operator for multi field
                3. verify the result
        expected: query successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        for i in range(2):
            query, text_match_expr, pandas_expr = (
                cf.generate_random_query_from_freq_dict(
                    wf_map, min_freq=3, max_terms=5, p_not=0.2
                )
            )
            log.info(f"expr: {text_match_expr}")
            res, _ = collection_w.query(expr=text_match_expr, output_fields=text_fields)
            onetime_res = res
            log.info(f"res len {len(res)}")
            step_by_step_results = []
            for expr in query:
                if isinstance(expr, dict):
                    if "not" in expr:
                        key = expr["not"]["field"]
                    else:
                        key = expr["field"]

                    tmp_expr = cf.generate_text_match_expr(expr)
                    res, _ = collection_w.query(
                        expr=tmp_expr, output_fields=text_fields
                    )
                    text_match_df = pd.DataFrame(res)
                    log.info(
                        f"text match res {len(text_match_df)}\n{text_match_df[key]}"
                    )
                    log.info(f"tmp expr {tmp_expr} {len(res)}")
                    tmp_idx = [r["id"] for r in res]
                    step_by_step_results.append(tmp_idx)
                    pandas_filter_res = cf.generate_pandas_text_match_result(
                        expr, df_new
                    )
                    tmp_pd_idx = pandas_filter_res["id"].tolist()
                    diff_id = set(tmp_pd_idx).union(set(tmp_idx)) - set(
                        tmp_pd_idx
                    ).intersection(set(tmp_idx))
                    log.info(f"diff between text match and manual check {diff_id}")
                    assert len(diff_id) == 0
                    for idx in diff_id:
                        log.info(df[df["id"] == idx][key].values)
                    log.info(
                        f"pandas_filter_res {len(pandas_filter_res)} \n {pandas_filter_res}"
                    )
                if isinstance(expr, str):
                    step_by_step_results.append(expr)
            final_res = cf.evaluate_expression(step_by_step_results)
            log.info(f"one time res {len(onetime_res)}, final res {len(final_res)}")
            if len(onetime_res) != len(final_res):
                log.info("res is not same")
                assert False

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_text_match_with_multi_lang(self):
        """
        target: test text match with multi-language text data
        method: 1. enable text match, and insert data with varchar in different language
                2. get the most common words and query with text match
                3. verify the result
        expected: get the correct token, text match successfully and result is correct
        """

        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_en = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        fake = fake_de
        data_de = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2, data_size)
        ]
        data = data_en + data_de
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]

        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_addition_inverted_index(self):
        """
        target: test text match with addition inverted index
        method: 1. enable text match, and insert data with varchar
                2. create inverted index
                3. get the most common words and query with text match
                4. query with inverted index and verify the result
        expected: get the correct token, text match successfully and result is correct
        """
        # 1. initialize with data
        fake_en = Faker("en_US")
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=default_schema
        )
        data = []
        data_size = 10000
        for i in range(data_size):
            d = {
                "id": i,
                "word": fake_en.word().lower(),
                "sentence": fake_en.sentence().lower(),
                "paragraph": fake_en.paragraph().lower(),
                "text": fake_en.text().lower(),
                "emb": cf.gen_vectors(1, dim)[0],
            }
            data.append(d)
        batch_size = 5000
        for i in range(0, data_size, batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < data_size
                else data[i:data_size]
            )
        # only if the collection is flushed, the inverted index ca be applied.
        # growing segment may be not applied, although in strong consistency.
        collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        df = pd.DataFrame(data)
        df_split = cf.split_dataframes(df, fields=["word", "sentence", "paragraph", "text"])
        log.info(f"dataframe\n{df}")
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language="en")
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            pandas_res = df_split[df_split.apply(lambda row: token in row[field], axis=1)]
            log.info(f"res len {len(res)}, pandas res len {len(pandas_res)}")
            log.info(f"pandas res\n{pandas_res}")
            assert len(res) == len(pandas_res)
            log.info(f"res len {len(res)}")
            for r in res:
                assert token in r[field]
            if field == "word":
                assert len(res) == wf_map[field].most_common()[-1][1]
                expr = f"{field} == '{token}'"
                log.info(f"expr: {expr}")
                res, _ = collection_w.query(expr=expr, output_fields=["id", field])
                log.info(f"res len {len(res)}")
                assert len(res) == wf_map[field].most_common()[-1][1]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("combine_op", ["and", "or"])
    def test_query_text_match_with_non_varchar_fields_expr(self, combine_op):
        """
        target: test text match with non-varchar fields expr
        method: 1. enable text match for varchar field and add some non varchar fields
                2. insert data, create index and load
                3. query with text match expr and non-varchar fields expr
                4. verify the result
        expected: query result is correct
        """
        # 1. initialize with data
        fake_en = Faker("en_US")
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="age",
                dtype=DataType.INT64,
            ),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=default_schema
        )
        data = []
        data_size = 10000
        for i in range(data_size):
            d = {
                "id": i,
                "age": random.randint(1, 100),
                "word": fake_en.word().lower(),
                "sentence": fake_en.sentence().lower(),
                "paragraph": fake_en.paragraph().lower(),
                "text": fake_en.text().lower(),
                "emb": cf.gen_vectors(1, dim)[0],
            }
            data.append(d)
        batch_size = 5000
        for i in range(0, data_size, batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < data_size
                else data[i:data_size]
            )
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language="en")
        # query single field for one word
        for field in text_fields:
            most_common_tokens = wf_map[field].most_common(10)
            mid = len(most_common_tokens) // 2
            idx = random.randint(0, max(0, mid - 1))
            token = most_common_tokens[idx][0]
            tm_expr = f"text_match({field}, '{token}')"
            int_expr = "age > 10"
            combined_expr = f"{tm_expr} {combine_op} {int_expr}"
            log.info(f"expr: {combined_expr}")
            res, _ = collection_w.query(expr=combined_expr, output_fields=["id", field, "age"])
            log.info(f"res len {len(res)}")
            for r in res:
                if combine_op == "and":
                    assert token in r[field] and r["age"] > 10
                if combine_op == "or":
                    assert token in r[field] or r["age"] > 10

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_some_empty_string(self):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar with some empty string
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_en = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        data_empty = [
            {
                "id": i,
                "word": "",
                "sentence": " ",
                "paragraph": "",
                "text": " ",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2, data_size)
        ]
        data = data_en + data_empty
        df = pd.DataFrame(data)
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # analyze the croup and get the tf-idf, then base on it to crate expr and ground truth
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)

        df_new = cf.split_dataframes(df, fields=text_fields)
        log.info(f"new df \n{df_new}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]
        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=["id", field])
            log.info(f"res len {len(res)}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_with_nullable(self):
        """
        target: test text match with nullable
        method: 1. enable text match and nullable, and insert data with varchar with some None value
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        # 1. initialize with data
        analyzer_params = {
            "tokenizer": "standard",
        }
        # 1. initialize with data
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                nullable=True,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        data_null = [
            {
                "id": i,
                "word": None if random.random() < 0.9 else fake.word().lower(),
                "sentence": None if random.random() < 0.9 else fake.sentence().lower(),
                "paragraph": None if random.random() < 0.9 else fake.paragraph().lower(),
                "text": None if random.random() < 0.9 else fake.paragraph().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(0, data_size)
        ]
        data = data_null
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i:i + batch_size]
                if i + batch_size < len(df)
                else data[i:len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection_w.load()
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # query single field for one word
        for field in text_fields:
            token = wf_map[field].most_common()[-1][0]
            expr = f"text_match({field}, '{token}')"
            log.info(f"expr: {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, \n{res}")
            assert len(res) > 0
            for r in res:
                assert token in r[field]
        # query single field for multi-word
        for field in text_fields:
            # match top 3 most common words
            multi_words = []
            for word, count in wf_map[field].most_common(3):
                multi_words.append(word)
            string_of_multi_words = " ".join(multi_words)
            expr = f"text_match({field}, '{string_of_multi_words}')"
            log.info(f"expr {expr}")
            res, _ = collection_w.query(expr=expr, output_fields=text_fields)
            log.info(f"res len {len(res)}, {res}")
            assert len(res) > 0
            for r in res:
                assert any([token in r[field] for token in multi_words])


class TestQueryTextMatchNegative(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_query_text_match_with_unsupported_tokenizer(self):
        """
        target: test query text match with unsupported tokenizer
        method: 1. enable text match, and use unsupported tokenizer
                2. create collection
        expected: create collection failed and return error
        """
        analyzer_params = {
            "tokenizer": "Unsupported",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="title",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="overview",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="genres",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="producer",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="cast",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )
        error = {ct.err_code: 2000, ct.err_msg: "unsupported tokenizer"}
        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=default_schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestQueryFunction(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_query_function_calls(self):
        """
        target: test query data
        method: create collection and insert data
                query with mix call expr in string field and int field
        expected: query successfully
        """
        collection_w, vectors = self.init_collection_general(prefix, insert_data=True,
                                                             primary_field=ct.default_string_field_name)[0:2]
        res = vectors[0].iloc[:, 1:3].to_dict('records')
        output_fields = [default_float_field_name, default_string_field_name]
        for mixed_call_expr in [
            "not empty(varchar) && int64 >= 0",
            # function call is case-insensitive
            "not EmPty(varchar) && int64 >= 0",
            "not EMPTY(varchar) && int64 >= 0",
            "starts_with(varchar, varchar) && int64 >= 0",
        ]:
            collection_w.query(
                mixed_call_expr,
                output_fields=output_fields,
                check_task=CheckTasks.check_query_results,
                check_items={exp_res: res, "pk_name": collection_w.primary_field.name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_invalid(self):
        """
        target: test query with invalid call expression
        method: query with invalid call expr
        expected: raise exception
        """
        collection_w, entities = self.init_collection_general(
            prefix, insert_data=True, nb=10)[0:2]
        test_cases = [
            (
                "A_FUNCTION_THAT_DOES_NOT_EXIST()".lower(),
                "function A_FUNCTION_THAT_DOES_NOT_EXIST() not found".lower(),
            ),
            # empty
            ("empty()", "function empty() not found"),
            (f"empty({default_int_field_name})", "function empty(int64_t) not found"),
            # starts_with
            (f"starts_with({default_int_field_name})", "function starts_with(int64_t) not found"),
            (f"starts_with({default_int_field_name}, {default_int_field_name})",
             "function starts_with(int64_t, int64_t) not found"),
        ]
        for call_expr, err_msg in test_cases:
            error = {ct.err_code: 65535, ct.err_msg: err_msg}
            collection_w.query(
                call_expr, check_task=CheckTasks.err_res, check_items=error
            )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue 36685")
    def test_query_text_match_with_unsupported_fields(self):
        """
        target: test enable text match with unsupported field
        method: 1. enable text match in unsupported field
                2. create collection
        expected: create collection failed and return error
        """
        analyzer_params = {
            "tokenizer": "standard",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="title",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="overview",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="age",
                dtype=DataType.INT64,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        default_schema = CollectionSchema(
            fields=default_fields, description="test collection"
        )
        error = {ct.err_code: 2000, ct.err_msg: "field type is not supported"}
        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=default_schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
