import random
import re
import math  # do not remove `math`
import pytest
import random
import numpy as np
import pandas as pd
from pymilvus import DataType, AnnSearchRequest, RRFRanker, WeightedRanker

from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common import common_params as cp
from common.code_mapping import QueryErrorMessage as qem
from common.common_params import (
    FieldParams, MetricType, DefaultVectorIndexParams, DefaultScalarIndexParams, Expr, AlterIndexParams
)
from base.client_base import TestcaseBase, TestCaseClassBase
from utils.util_log import test_log as log


@pytest.mark.xdist_group("TestNoIndexDQLExpr")
class TestNoIndexDQLExpr(TestCaseClassBase):
    """
    Scalar fields are not indexed, and verify DQL requests

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 10000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_no_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name,
                        'VARCHAR_1', *self().all_scalar_fields],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            'VARCHAR_1': cf.gen_varchar_data(1, self.nb)
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        for d in cf.iter_insert_list_data(list(self.insert_data.values()), batch=3000, total_len=self.nb):
            self.collection_wrap.insert(data=d, check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build vectors index
        index_params = {
            **DefaultVectorIndexParams.IVF_SQ8(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.IVF_FLAT(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_IVF_FLAT(DataType.BINARY_VECTOR.name)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestNoIndexDQLExpr] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, output_fields", [
        (Expr.In(Expr.MOD('INT8', 13).subset, [0, 1, 2]).value, ['INT8']),
        (Expr.Nin(Expr.MOD('INT16', 100).subset, [10, 20, 30, 40]).value, ['INT16']),
    ])
    def test_no_index_query_with_invalid_expr(self, expr, output_fields):
        """
        target:
            1. check invalid expr
        method:
            1. prepare some data
            2. query with the invalid expr
        expected:
            1. raises expected error
        """
        # query
        self.collection_wrap.query(expr=expr, check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1100, ct.err_msg: qem.ParseExpressionFailed})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_modulo_expression(['int64_pk', 'INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_no_index_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_expression(['VARCHAR']) + cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_no_index_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_l, expr_field_l, rex_l", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("expr_r, expr_field_r, rex_r", cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("expr_obj, op", [(Expr.AND, 'and'), (Expr.OR, 'or')])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_no_index_query_with_mix_string(
            self, expr_l, expr_field_l, rex_l, expr_r, expr_field_r, rex_r, expr_obj, op, limit):
        """
        target:
            1. check mix string fields expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len(cf.count_match_expr(self.insert_data.get(expr_field_l, []), rex_l, op,
                                             self.insert_data.get(expr_field_r, []), rex_r))

        # query
        res, _ = self.collection_wrap.query(expr=expr_obj(f"({expr_l})", f"({expr_r})").value, limit=limit,
                                            output_fields=[expr_field_l, expr_field_r])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field_l) == []
        assert self.check_query_res(res=res, expr_field=expr_field_r) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_no_index_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_no_index_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})


@pytest.mark.xdist_group("TestHybridIndexDQLExpr")
class TestHybridIndexDQLExpr(TestCaseClassBase):
    """
    Scalar fields build Hybrid index, and verify DQL requests

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 10000
        self.all_fields = [self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                           DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name,
                           'VARCHAR_1', *self().all_scalar_fields]

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_hybrid_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=self.all_fields,
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            'VARCHAR': cf.gen_varchar_data(3, self.nb),
            'VARCHAR_1': cf.gen_varchar_data(1, self.nb),
            'ARRAY_VARCHAR': [cf.gen_varchar_data(length=2, nb=random.randint(0, 10)) for _ in range(self.nb)]
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        for d in cf.iter_insert_list_data(list(self.insert_data.values()), batch=3000, total_len=self.nb):
            self.collection_wrap.insert(data=d, check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `Hybrid index`
        index_params = {
            **DefaultVectorIndexParams.DISKANN(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.IVF_SQ8(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_INVERTED_INDEX(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_IVF_FLAT(DataType.BINARY_VECTOR.name),
            # build Hybrid index
            **DefaultScalarIndexParams.list_default([self.primary_field, 'VARCHAR_1'] + self.all_index_scalar_fields)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestHybridIndexDQLExpr] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_modulo_expression(['int64_pk', 'INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_hybrid_index_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_expression(['VARCHAR']) + cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_hybrid_index_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_l, expr_field_l, rex_l", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("expr_r, expr_field_r, rex_r", cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("expr_obj, op", [(Expr.AND, 'and'), (Expr.OR, 'or')])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_hybrid_index_query_with_mix_string(
            self, expr_l, expr_field_l, rex_l, expr_r, expr_field_r, rex_r, expr_obj, op, limit):
        """
        target:
            1. check mix string fields expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len(cf.count_match_expr(self.insert_data.get(expr_field_l, []), rex_l, op,
                                             self.insert_data.get(expr_field_r, []), rex_r))

        # query
        res, _ = self.collection_wrap.query(expr=expr_obj(f"({expr_l})", f"({expr_r})").value, limit=limit,
                                            output_fields=[expr_field_l, expr_field_r])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field_l) == []
        assert self.check_query_res(res=res, expr_field=expr_field_r) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_hybrid_index_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_hybrid_index_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `Hybrid index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([1, 3], 50), ([2, 5], 50), ([3, 3], 100)])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    @pytest.mark.parametrize("expr_field", ['VARCHAR'])
    def test_hybrid_index_query_with_varchar_in(self, range_num, counts, limit, expr_field):
        """
        target:
            1. check varchar operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `Hybrid index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [cf.gen_varchar_data(random.randint(*range_num), 1)[0] for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("length", [0, 5, 11])
    @pytest.mark.parametrize("expr_obj", [Expr.array_length, Expr.ARRAY_LENGTH])
    @pytest.mark.parametrize("expr_field", ['ARRAY_VARCHAR'])
    def test_hybrid_index_query_array_length_count(self, length, expr_obj, expr_field):
        """
        target:
            1. check query with count(*) via expr `array length`
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. query with count(*) via expr
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        expr = Expr.EQ(expr_obj(expr_field).value, length).value

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if len(i) == length])

        # query count(*)
        self.collection_wrap.query(expr=expr, output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_index_query_count(self):
        """
        target:
            1. check query with count(*)
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. query with count(*)
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        # query count(*)
        self.collection_wrap.query(expr='', output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": self.nb}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_index_search_output_fields(self):
        """
        target:
            1. check search output fields with Hybrid index built on scalar fields
        method:
            1. prepare some data and build `Hybrid index` on scalar fields
            2. search output fields and check result
        expected:
            1. search output fields with Hybrid index
        """
        search_params, vector_field, limit, nq = {"metric_type": "L2", "ef": 32}, DataType.FLOAT16_VECTOR, 3, 1

        self.collection_wrap.search(
            cf.gen_vectors(nb=nq, dim=3, vector_data_type=vector_field), vector_field.name, search_params, limit,
            output_fields=['*'], check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field),
                         "limit": limit, "output_fields": self.all_fields})


@pytest.mark.xdist_group("TestInvertedIndexDQLExpr")
class TestInvertedIndexDQLExpr(TestCaseClassBase):
    """
    Scalar fields build INVERTED index, and verify DQL requests

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 10000
        self.all_fields = [self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                           DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name,
                           'VARCHAR_1', *self().all_scalar_fields]

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_inverted_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=self.all_fields,
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            'VARCHAR': cf.gen_varchar_data(3, self.nb),
            'VARCHAR_1': cf.gen_varchar_data(1, self.nb),
            'ARRAY_VARCHAR': [cf.gen_varchar_data(length=2, nb=random.randint(0, 10)) for _ in range(self.nb)]
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        for d in cf.iter_insert_list_data(list(self.insert_data.values()), batch=3000, total_len=self.nb):
            self.collection_wrap.insert(data=d, check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `INVERTED index`
        index_params = {
            **DefaultVectorIndexParams.IVF_FLAT(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.HNSW(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_FLAT(DataType.BINARY_VECTOR.name),
            # build INVERTED index
            **DefaultScalarIndexParams.list_inverted(
                [self.primary_field, 'VARCHAR_1'] + self.inverted_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestInvertedIndexDQLExpr] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_modulo_expression(['int64_pk', 'INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_inverted_index_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data and build `INVERTED index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_expression(['VARCHAR']) + cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_inverted_index_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `INVERTED index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_l, expr_field_l, rex_l", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("expr_r, expr_field_r, rex_r", cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("expr_obj, op", [(Expr.AND, 'and'), (Expr.OR, 'or')])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_inverted_index_query_with_mix_string(
            self, expr_l, expr_field_l, rex_l, expr_r, expr_field_r, rex_r, expr_obj, op, limit):
        """
        target:
            1. check mix string fields expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len(cf.count_match_expr(self.insert_data.get(expr_field_l, []), rex_l, op,
                                             self.insert_data.get(expr_field_r, []), rex_r))

        # query
        res, _ = self.collection_wrap.query(expr=expr_obj(f"({expr_l})", f"({expr_r})").value, limit=limit,
                                            output_fields=[expr_field_l, expr_field_r])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field_l) == []
        assert self.check_query_res(res=res, expr_field=expr_field_r) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_inverted_index_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data and build `INVERTED index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_inverted_index_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `INVERTED index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([1, 3], 50), ([2, 5], 50), ([3, 3], 100)])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    @pytest.mark.parametrize("expr_field", ['VARCHAR'])
    def test_inverted_index_query_with_varchar_in(self, range_num, counts, limit, expr_field):
        """
        target:
            1. check varchar operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `INVERTED index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [cf.gen_varchar_data(random.randint(*range_num), 1)[0] for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("length", [0, 5, 11])
    @pytest.mark.parametrize("expr_obj", [Expr.array_length, Expr.ARRAY_LENGTH])
    @pytest.mark.parametrize("expr_field", ['ARRAY_VARCHAR'])
    def test_inverted_index_query_array_length_count(self, length, expr_obj, expr_field):
        """
        target:
            1. check query with count(*) via expr `array length`
        method:
            1. prepare some data and build `INVERTED index` on scalar fields
            2. query with count(*) via expr
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        expr = Expr.EQ(expr_obj(expr_field).value, length).value

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if len(i) == length])

        # query count(*)
        self.collection_wrap.query(expr=expr, output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})


@pytest.mark.xdist_group("TestBitmapIndexDQLExpr")
class TestBitmapIndexDQLExpr(TestCaseClassBase):
    """
    Scalar fields build BITMAP index, and verify DQL requests

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 10000
        self.all_fields = [self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                           DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name,
                           "VARCHAR_1", *self().all_scalar_fields]

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=self.all_fields,
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            'VARCHAR': cf.gen_varchar_data(3, self.nb),
            'VARCHAR_1': cf.gen_varchar_data(1, self.nb),
            'ARRAY_VARCHAR': [cf.gen_varchar_data(length=2, nb=random.randint(0, 10)) for _ in range(self.nb)]
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        for d in cf.iter_insert_list_data(list(self.insert_data.values()), batch=3000, total_len=self.nb):
            self.collection_wrap.insert(data=d, check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `BITMAP index`
        index_params = {
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.DISKANN(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_IVF_FLAT(DataType.BINARY_VECTOR.name),
            # build BITMAP index
            **DefaultScalarIndexParams.list_bitmap(["VARCHAR_1"] + self.bitmap_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestBitmapIndexDQLExpr] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_index_query_with_invalid_array_params(self):
        """
        target:
            1. check query with invalid array params
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different wrong expr
            3. check query result error
        expected:
            1. query response check error
        """
        # query
        self.collection_wrap.query(
            expr=Expr.array_contains_any('ARRAY_VARCHAR', [['a', 'b']]).value, limit=1, check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: qem.ParseExpressionFailed})

        self.collection_wrap.query(
            expr=Expr.array_contains_all('ARRAY_VARCHAR', [['a', 'b']]).value, limit=1, check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: qem.ParseExpressionFailed})

        self.collection_wrap.query(
            expr=Expr.array_contains('ARRAY_VARCHAR', [['a', 'b']]).value, limit=1, check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: qem.ParseExpressionFailed})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field", cf.gen_modulo_expression(['INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_index_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_expression(['VARCHAR']) + cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_index_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_l, expr_field_l, rex_l", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("expr_r, expr_field_r, rex_r", cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("expr_obj, op", [(Expr.AND, 'and'), (Expr.OR, 'or')])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_index_query_with_mix_string(
            self, expr_l, expr_field_l, rex_l, expr_r, expr_field_r, rex_r, expr_obj, op, limit):
        """
        target:
            1. check mix string fields expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len(cf.count_match_expr(self.insert_data.get(expr_field_l, []), rex_l, op,
                                             self.insert_data.get(expr_field_r, []), rex_r))

        # query
        res, _ = self.collection_wrap.query(expr=expr_obj(f"({expr_l})", f"({expr_r})").value, limit=limit,
                                            output_fields=[expr_field_l, expr_field_r])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field_l) == []
        assert self.check_query_res(res=res, expr_field=expr_field_r) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_index_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_index_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([1, 3], 50), ([2, 5], 50), ([3, 3], 100)])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    @pytest.mark.parametrize("expr_field", ['VARCHAR'])
    def test_bitmap_index_query_with_varchar_in(self, range_num, counts, limit, expr_field):
        """
        target:
            1. check varchar operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [cf.gen_varchar_data(random.randint(*range_num), 1)[0] for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("length", [0, 5, 11])
    @pytest.mark.parametrize("expr_obj", [Expr.array_length, Expr.ARRAY_LENGTH])
    @pytest.mark.parametrize("expr_field", ['ARRAY_VARCHAR'])
    def test_bitmap_index_query_array_length_count(self, length, expr_obj, expr_field):
        """
        target:
            1. check query with count(*) via expr `array length`
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with count(*) via expr
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        expr = Expr.EQ(expr_obj(expr_field).value, length).value

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if len(i) == length])

        # query count(*)
        self.collection_wrap.query(expr=expr, output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_index_query_count(self):
        """
        target:
            1. check query with count(*)
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with count(*)
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        # query count(*)
        self.collection_wrap.query(expr='', output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": self.nb}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [10, 1000])
    @pytest.mark.parametrize("group_by_field", ['INT8', 'INT16', 'INT32', 'INT64', 'BOOL', 'VARCHAR'])
    @pytest.mark.parametrize(
        "dim, search_params, vector_field",
        [(3, {"metric_type": MetricType.L2, "ef": 32}, DataType.FLOAT16_VECTOR),
         (1000, {"metric_type": MetricType.IP, "drop_ratio_search": 0.2}, DataType.SPARSE_FLOAT_VECTOR)])
    def test_bitmap_index_search_group_by(self, limit, group_by_field, dim, search_params, vector_field):
        """
        target:
            1. check search iterator with BITMAP index built on scalar fields
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. search group by scalar fields and check result
        expected:
            1. search group by with BITMAP index
        """
        res, _ = self.collection_wrap.search(cf.gen_vectors(nb=1, dim=dim, vector_data_type=vector_field), vector_field.name,
                                             search_params, limit, group_by_field=group_by_field,
                                             output_fields=[group_by_field])
        output_values = [i.fields for r in res for i in r]

        # check output field
        assert len([True for i in output_values if set(i.keys()) != {group_by_field}]) == 0, f"res: {output_values}"

        # check `group_by_field` field values are unique
        values = [v for i in output_values for k, v in i.items()]

        assert len(values) == len(set(values)), f"values: {values}, output_values:{output_values}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("batch_size", [10, 1000])
    def test_bitmap_index_search_iterator(self, batch_size):
        """
        target:
            1. check search iterator with BITMAP index built on scalar fields
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. search iterator and check result
        expected:
            1. search iterator with BITMAP index
        """
        ef = 32 if batch_size <= 32 else batch_size  # ef must be larger than or equal to batch size
        search_params, vector_field = {"metric_type": "L2", "ef": ef}, DataType.FLOAT16_VECTOR
        self.collection_wrap.search_iterator(
            cf.gen_vectors(nb=1, dim=3, vector_data_type=vector_field), vector_field.name, search_params, batch_size,
            expr='INT16 > 15', check_task=CheckTasks.check_search_iterator, check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_index_search_output_fields(self):
        """
        target:
            1. check search output fields with BITMAP index built on scalar fields
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. search output fields and check result
        expected:
            1. search output fields with BITMAP index
        """
        search_params, vector_field, limit, nq = {"metric_type": "L2", "ef": 32}, DataType.FLOAT16_VECTOR, 3, 1

        self.collection_wrap.search(
            cf.gen_vectors(nb=nq, dim=3, vector_data_type=vector_field), vector_field.name, search_params, limit,
            output_fields=['*'], check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field),
                         "limit": limit, "output_fields": self.all_fields})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_index_hybrid_search(self):
        """
        target:
            1. check hybrid search with expr
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. hybrid search with expr
        expected:
            1. hybrid search with expr
        """
        nq, limit = 10, 10
        vectors = cf.gen_field_values(self.collection_wrap.schema, nb=nq)

        req_list = [
            AnnSearchRequest(
                data=vectors.get(DataType.FLOAT16_VECTOR.name), anns_field=DataType.FLOAT16_VECTOR.name,
                param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
                expr=Expr.In('INT64', [i for i in range(10, 30)]).value
            ),
            AnnSearchRequest(
                data=vectors.get(DataType.BFLOAT16_VECTOR.name), anns_field=DataType.BFLOAT16_VECTOR.name,
                param={"metric_type": MetricType.L2, "search_list": 30}, limit=limit,
                expr=Expr.OR(Expr.GT(Expr.SUB('INT8', 30).subset, 10), Expr.LIKE('VARCHAR', 'a%')).value
            ),
            AnnSearchRequest(
                data=vectors.get(DataType.SPARSE_FLOAT_VECTOR.name), anns_field=DataType.SPARSE_FLOAT_VECTOR.name,
                param={"metric_type": MetricType.IP, "drop_ratio_search": 0.2}, limit=limit),
            AnnSearchRequest(
                data=vectors.get(DataType.BINARY_VECTOR.name), anns_field=DataType.BINARY_VECTOR.name,
                param={"metric_type": MetricType.JACCARD, "nprobe": 128}, limit=limit)
        ]
        self.collection_wrap.hybrid_search(
            req_list, RRFRanker(), limit, check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field), "limit": limit})


@pytest.mark.xdist_group("TestBitmapIndexOffsetCacheDQL")
class TestBitmapIndexOffsetCache(TestCaseClassBase):
    """
    Scalar fields build BITMAP index, and altering index indexoffsetcache

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 3000
        self.all_fields = [self.primary_field, DataType.FLOAT_VECTOR.name, 'VARCHAR_1', *self().all_scalar_fields]

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_offset_cache"),
            schema=cf.set_collection_schema(
                fields=self.all_fields,
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            'VARCHAR': cf.gen_varchar_data(3, self.nb),
            'VARCHAR_1': cf.gen_varchar_data(1, self.nb),
            'ARRAY_VARCHAR': [cf.gen_varchar_data(length=2, nb=random.randint(0, 10)) for _ in range(self.nb)]
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `BITMAP index`
        index_params = {
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            # build BITMAP index
            **DefaultScalarIndexParams.list_bitmap(['VARCHAR_1'] + self.bitmap_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # enable offset cache
        for index_name in ['VARCHAR_1'] + self.bitmap_support_dtype_names:
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_offset_cache())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestBitmapIndexOffsetCache] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field", cf.gen_modulo_expression(['INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_offset_cache_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=['*'])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_expression(['VARCHAR']) + cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_offset_cache_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=['*'])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr_l, expr_field_l, rex_l", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("expr_r, expr_field_r, rex_r", cf.gen_varchar_operation(['VARCHAR_1']))
    @pytest.mark.parametrize("expr_obj, op", [(Expr.AND, 'and'), (Expr.OR, 'or')])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_offset_cache_query_with_mix_string(
            self, expr_l, expr_field_l, rex_l, expr_r, expr_field_r, rex_r, expr_obj, op, limit):
        """
        target:
            1. check mix string fields expression
        method:
            1. prepare some data
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len(cf.count_match_expr(self.insert_data.get(expr_field_l, []), rex_l, op,
                                             self.insert_data.get(expr_field_r, []), rex_r))

        # query
        res, _ = self.collection_wrap.query(expr=expr_obj(f"({expr_l})", f"({expr_r})").value, limit=limit,
                                            output_fields=[expr_field_l, expr_field_r])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field_l) == []
        assert self.check_query_res(res=res, expr_field=expr_field_r) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_offset_cache_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=['*'])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_offset_cache_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([1, 3], 50), ([2, 5], 50), ([3, 3], 100)])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    @pytest.mark.parametrize("expr_field", ['VARCHAR'])
    def test_bitmap_offset_cache_query_with_varchar_in(self, range_num, counts, limit, expr_field):
        """
        target:
            1. check varchar operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [cf.gen_varchar_data(random.randint(*range_num), 1)[0] for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("length", [0, 5, 11])
    @pytest.mark.parametrize("expr_obj", [Expr.array_length, Expr.ARRAY_LENGTH])
    @pytest.mark.parametrize("expr_field", ['ARRAY_VARCHAR'])
    def test_bitmap_offset_cache_query_array_length_count(self, length, expr_obj, expr_field):
        """
        target:
            1. check query with count(*) via expr `array length`
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with count(*) via expr
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        expr = Expr.EQ(expr_obj(expr_field).value, length).value

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if len(i) == length])

        # query count(*)
        self.collection_wrap.query(expr=expr, output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_offset_cache_query_count(self):
        """
        target:
            1. check query with count(*)
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with count(*)
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        # query count(*)
        self.collection_wrap.query(expr='', output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": self.nb}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_offset_cache_search_output_fields(self):
        """
        target:
            1. check search output fields with BITMAP index built on scalar fields
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. search output fields and check result
        expected:
            1. search output fields with BITMAP index
        """
        search_params, vector_field, limit, nq = {"metric_type": "L2", "ef": 32}, DataType.FLOAT_VECTOR, 3, 1

        self.collection_wrap.search(
            cf.gen_vectors(nb=nq, dim=ct.default_dim, vector_data_type=vector_field),
            vector_field.name, search_params, limit, output_fields=['*'], check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field),
                         "limit": limit, "output_fields": self.all_fields})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_offset_cache_hybrid_search(self):
        """
        target:
            1. check hybrid search with expr
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. hybrid search with expr
        expected:
            1. hybrid search with expr
        """
        nq, limit = 10, 10
        vectors = cf.gen_field_values(self.collection_wrap.schema, nb=nq)

        req_list = [
            AnnSearchRequest(
                data=vectors.get(DataType.FLOAT_VECTOR.name), anns_field=DataType.FLOAT_VECTOR.name,
                param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
                expr=Expr.In('INT64', [i for i in range(10, 30)]).value
            ),
            AnnSearchRequest(
                data=vectors.get(DataType.FLOAT_VECTOR.name), anns_field=DataType.FLOAT_VECTOR.name,
                param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
                expr=Expr.OR(Expr.GT(Expr.SUB('INT8', 30).subset, 10), Expr.LIKE('VARCHAR', 'a%')).value
            )
        ]
        self.collection_wrap.hybrid_search(
            req_list, RRFRanker(), limit, check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field), "limit": limit})


@pytest.mark.xdist_group("TestBitmapIndexOffsetCacheDQL")
class TestBitmapIndexMmap(TestCaseClassBase):
    """
    Scalar fields build BITMAP index, and altering index Mmap

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 3000
        self.all_fields = [self.primary_field, DataType.FLOAT_VECTOR.name, *self().all_scalar_fields]

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_bitmap"),
            schema=cf.set_collection_schema(
                fields=self.all_fields,
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `BITMAP index`
        index_params = {
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            # build BITMAP index
            **DefaultScalarIndexParams.list_bitmap(self.bitmap_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # enable mmap
        for index_name in self.bitmap_support_dtype_names:
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_mmap())

        # load collection
        self.collection_wrap.load()

    def check_query_res(self, res, expr_field: str) -> list:
        """ Ensure that primary key field values are unique """
        real_data = {x[0]: x[1] for x in zip(self.insert_data.get(self.primary_field),
                                             self.insert_data.get(expr_field))}

        if len(real_data) != len(self.insert_data.get(self.primary_field)):
            log.warning("[TestBitmapIndexMmap] The primary key values are not unique, " +
                        "only check whether the res value is within the inserted data")
            return [(r.get(self.primary_field), r.get(expr_field)) for r in res if
                    r.get(expr_field) not in self.insert_data.get(expr_field)]

        return [(r[self.primary_field], r[expr_field], real_data[r[self.primary_field]]) for r in res if
                r[expr_field] != real_data[r[self.primary_field]]]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field", cf.gen_modulo_expression(['INT8', 'INT16', 'INT32', 'INT64']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_mmap_query_with_modulo(self, expr, expr_field, limit):
        """
        target:
            1. check modulo expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if
                          eval('math.fmod' + expr.replace(expr_field, str(i)).replace('%', ','))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_mmap_query_with_string(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "expr, expr_field", cf.gen_number_operation(['INT8', 'INT16', 'INT32', 'INT64', 'FLOAT', 'DOUBLE']))
    @pytest.mark.parametrize("limit", [1, 10])
    def test_bitmap_mmap_query_with_operation(self, expr, expr_field, limit):
        """
        target:
            1. check number operation
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("range_num, counts", [([-100, 200], 10), ([2000, 5000], 10), ([3000, 4000], 5)])
    @pytest.mark.parametrize("expr_field", ['INT8', 'INT16', 'INT32', 'INT64'])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_bitmap_mmap_query_with_int_in(self, range_num, counts, expr_field, limit):
        """
        target:
            1. check number operation `in` and `not in`, calculate total number via expr
        method:
            1. prepare some data and  build `BITMAP index` on scalar fields
            2. query with the different expr(in, not in) and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # random set expr list
        range_numbers = [random.randint(*range_num) for _ in range(counts)]

        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if i in range_numbers])

        # query `in`
        res, _ = self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `in`
        self.collection_wrap.query(expr=Expr.In(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": expr_count}]})

        # query `not in`
        not_in_count = self.nb - expr_count
        res, _ = self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, limit=limit,
                                            output_fields=[expr_field])
        assert len(res) == min(not_in_count, limit), f"actual: {len(res)} == expect: {min(not_in_count, limit)}"
        # check query response data
        assert self.check_query_res(res=res, expr_field=expr_field) == []

        # count `not in`
        self.collection_wrap.query(expr=Expr.Nin(expr_field, range_numbers).value, output_fields=['count(*)'],
                                   check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": not_in_count}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_mmap_query_count(self):
        """
        target:
            1. check query with count(*)
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with count(*)
            3. check query result
        expected:
            1. query response equal to insert nb
        """
        # query count(*)
        self.collection_wrap.query(expr='', output_fields=['count(*)'], check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": [{"count(*)": self.nb}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_mmap_search_output_fields(self):
        """
        target:
            1. check search output fields with BITMAP index built on scalar fields
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. search output fields and check result
        expected:
            1. search output fields with BITMAP index
        """
        search_params, vector_field, limit, nq = {"metric_type": "L2", "ef": 32}, DataType.FLOAT_VECTOR, 3, 1

        self.collection_wrap.search(
            cf.gen_vectors(nb=nq, dim=ct.default_dim, vector_data_type=vector_field),
            vector_field.name, search_params, limit, output_fields=['*'], check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field),
                         "limit": limit, "output_fields": self.all_fields})

    @pytest.mark.tags(CaseLabel.L1)
    def test_bitmap_mmap_hybrid_search(self):
        """
        target:
            1. check hybrid search with expr
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. hybrid search with expr
        expected:
            1. hybrid search with expr
        """
        nq, limit = 10, 10
        vectors = cf.gen_field_values(self.collection_wrap.schema, nb=nq)

        req_list = [
            AnnSearchRequest(
                data=vectors.get(DataType.FLOAT_VECTOR.name), anns_field=DataType.FLOAT_VECTOR.name,
                param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
                expr=Expr.In('INT64', [i for i in range(10, 30)]).value
            ),
            AnnSearchRequest(
                data=vectors.get(DataType.FLOAT_VECTOR.name), anns_field=DataType.FLOAT_VECTOR.name,
                param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
                expr=Expr.OR(Expr.GT(Expr.SUB('INT8', 30).subset, 10), Expr.LIKE('VARCHAR', 'a%')).value
            )
        ]
        self.collection_wrap.hybrid_search(
            req_list, RRFRanker(), limit, check_task=CheckTasks.check_search_results,
            check_items={"nq": nq, "ids": self.insert_data.get(self.primary_field), "limit": limit})


@pytest.mark.xdist_group("TestIndexUnicodeString")
class TestIndexUnicodeString(TestCaseClassBase):
    """
    Scalar fields build BITMAP index, and verify Unicode string

    Author: Ting.Wang
    """

    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field, self.nb = "int64_pk", 3000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_index_unicode_string"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT_VECTOR.name,
                        f"{DataType.VARCHAR.name}_BITMAP", f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_BITMAP",
                        f"{DataType.VARCHAR.name}_INVERTED", f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_INVERTED",
                        f"{DataType.VARCHAR.name}_NoIndex", f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_NoIndex"],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        # insert unicode string
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=self.nb, default_values={
            f"{DataType.VARCHAR.name}_BITMAP": cf.gen_unicode_string_batch(nb=self.nb, string_len=30),
            f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_BITMAP": cf.gen_unicode_string_array_batch(
                nb=self.nb, string_len=1, max_capacity=100),
            f"{DataType.VARCHAR.name}_INVERTED": cf.gen_unicode_string_batch(nb=self.nb, string_len=30),
            f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_INVERTED": cf.gen_unicode_string_array_batch(
                nb=self.nb, string_len=1, max_capacity=100),
            f"{DataType.VARCHAR.name}_NoIndex": cf.gen_unicode_string_batch(nb=self.nb, string_len=30),
            f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_NoIndex": cf.gen_unicode_string_array_batch(
                nb=self.nb, string_len=1, max_capacity=100),
        })

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build scalar index
        index_params = {
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            # build BITMAP index
            **DefaultScalarIndexParams.list_bitmap([f"{DataType.VARCHAR.name}_BITMAP",
                                                    f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_BITMAP"]),
            # build INVERTED index
            **DefaultScalarIndexParams.list_inverted([f"{DataType.VARCHAR.name}_INVERTED",
                                                      f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}_INVERTED"])
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("expr, expr_field, rex",
                             cf.gen_varchar_unicode_expression(['VARCHAR_BITMAP', 'VARCHAR_INVERTED']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_index_unicode_string_query(self, expr, expr_field, limit, rex):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if re.search(rex, i) is not None])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("obj", cf.gen_varchar_unicode_expression_array(
        ['ARRAY_VARCHAR_BITMAP', 'ARRAY_VARCHAR_INVERTED', 'ARRAY_VARCHAR_NoIndex']))
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_index_unicode_string_array_query(self, limit, obj):
        """
        target:
            1. check string expression
        method:
            1. prepare some data and build `BITMAP index` on scalar fields
            2. query with the different expr and limit
            3. check query result
        expected:
            1. query response equal to min(insert data, limit)
        """
        # the total number of inserted data that matches the expression
        expr_count = len([i for i in self.insert_data.get(obj.field, []) if eval(obj.rex.format(str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=obj.field_expr, limit=limit, output_fields=[obj.field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"


class TestMixScenes(TestcaseBase):
    """
    Testing cross-combination scenarios

    Author: Ting.Wang
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_bitmap_upsert_and_delete(self, request):
        """
        target:
            1. upsert data and query returns the updated data
        method:
            1. create a collection with scalar fields
            2. insert some data and build BITMAP index
            3. query the data of the specified primary key value
            4. upsert the specified primary key value
            5. re-query and check data equal to the updated data
            6. delete the specified primary key value
            7. re-query and check result is []
        expected:
            1. check whether the upsert and delete data is effective
        """
        # init params
        collection_name, primary_field, nb = f"{request.function.__name__}", "int64_pk", 3000
        # scalar fields
        scalar_fields, expr = [DataType.INT64.name, f"{DataType.ARRAY.name}_{DataType.VARCHAR.name}"], 'int64_pk == 10'

        # connect to server before testing
        self._connect()

        # create a collection with fields that can build `BITMAP` index
        self.collection_wrap.init_collection(
            name=collection_name,
            schema=cf.set_collection_schema(
                fields=[primary_field, DataType.FLOAT_VECTOR.name, *scalar_fields],
                field_params={primary_field: FieldParams(is_primary=True).to_dict},
            )
        )

        # prepare data (> 1024 triggering index building)
        insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)
        self.collection_wrap.insert(data=list(insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # rebuild `BITMAP` index
        self.build_multi_index(index_params={
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            **DefaultScalarIndexParams.list_bitmap(scalar_fields)
        })

        # load collection
        self.collection_wrap.load()

        # query before upsert
        expected_res = [{k: v[10] for k, v in insert_data.items() if k != DataType.FLOAT_VECTOR.name}]
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": expected_res, "primary_field": primary_field})

        # upsert int64_pk = 10
        upsert_data = cf.gen_field_values(self.collection_wrap.schema, nb=1,
                                          default_values={primary_field: [10]}, start_id=10)
        self.collection_wrap.upsert(data=list(upsert_data.values()))
        # re-query
        expected_upsert_res = [{k: v[0] for k, v in upsert_data.items() if k != DataType.FLOAT_VECTOR.name}]
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": expected_upsert_res, "primary_field": primary_field})

        # delete int64_pk = 10
        self.collection_wrap.delete(expr=expr)
        # re-query
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": []})

    @pytest.mark.tags(CaseLabel.L2)
    def test_bitmap_offset_cache_and_mmap(self, request):
        """
        target:
            1. enable offset cache and mmap at the same time to verify DQL & DML operations
        method:
            1. create a collection with scalar fields
            2. insert some data and build BITMAP index
            3. alter all BITMAP fields: enabled offset cache and mmap
            4. load collection
            5. query the data of `not exist` primary key value
            6. upsert the `not exist` primary key value
            7. re-query and check data equal to the updated data
            8. delete the upserted primary key value
            9. re-query and check result is []
            10. search with compound expressions and check result
        expected:
            1. DQL & DML operations are successful and the results are as expected
        """
        # init params
        collection_name, primary_field, nb = f"{request.function.__name__}", "int64_pk", 3000
        scalar_fields, expr = [primary_field, *self.bitmap_support_dtype_names], 'int64_pk == 33333'

        # connect to server before testing
        self._connect()

        # create a collection with fields that can build `BITMAP` index
        self.collection_wrap.init_collection(
            name=collection_name,
            schema=cf.set_collection_schema(
                fields=[primary_field, DataType.FLOAT_VECTOR.name, *self.bitmap_support_dtype_names],
                field_params={primary_field: FieldParams(is_primary=True).to_dict},
            )
        )

        # prepare data (> 1024 triggering index building)
        insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)
        self.collection_wrap.insert(data=list(insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `BITMAP` index
        self.build_multi_index(index_params={
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            **DefaultScalarIndexParams.list_bitmap(self.bitmap_support_dtype_names)
        })

        # enable offset cache and mmap
        for index_name in self.bitmap_support_dtype_names:
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_offset_cache())
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_mmap())

        # load collection
        self.collection_wrap.load()

        # query before upsert
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": []})

        # upsert int64_pk = 33333
        upsert_data = cf.gen_field_values(self.collection_wrap.schema, nb=1,
                                          default_values={primary_field: [33333]}, start_id=33333)
        self.collection_wrap.upsert(data=list(upsert_data.values()))
        # re-query
        expected_upsert_res = [{k: v[0] for k, v in upsert_data.items() if k != DataType.FLOAT_VECTOR.name}]
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": expected_upsert_res, "primary_field": primary_field})

        # delete int64_pk = 33333
        self.collection_wrap.delete(expr=expr)
        # re-query
        self.collection_wrap.query(expr=expr, output_fields=scalar_fields, check_task=CheckTasks.check_query_results,
                                   check_items={"exp_res": []})

        # search
        expr_left, expr_right = Expr.GT(Expr.SUB('INT64', 37).subset, 13).value, Expr.LIKE('VARCHAR', '%a').value
        expr, rex, nq, limit = Expr.AND(expr_left, expr_right).value, r'.*a$', 10, 10
        # counts data match expr
        counts = sum([eval(expr_left.replace('INT64', str(i))) and re.search(rex, j) is not None for i, j in
                      zip(insert_data.get('INT64', []), insert_data.get('VARCHAR', []))])
        check_task = None if counts == 0 else CheckTasks.check_search_results
        self.collection_wrap.search(
            data=cf.gen_field_values(self.collection_wrap.schema, nb=nq).get(DataType.FLOAT_VECTOR.name),
            anns_field=DataType.FLOAT_VECTOR.name, param={"metric_type": MetricType.L2, "ef": 32}, limit=limit,
            expr=expr, output_fields=scalar_fields, check_task=check_task,
            check_items={"nq": nq, "ids": insert_data.get(primary_field), "limit": min(limit, counts),
                         "output_fields": scalar_fields})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_field, data_type, expr_data", [('INT64', 'int', 3), ('VARCHAR', 'str', '3')])
    def test_bitmap_partition_keys(self, request, scalar_field, data_type, expr_data):
        """
        target:
            1. build BITMAP index on partition key field
        method:
            1. create a collection with scalar field that enable partition key
            2. insert some data and build BITMAP index
            4. load collection
            5. query via partition key field expr
        expected:
            1. build index and query are successful
        """
        # init params
        collection_name, primary_field, nb = f"{request.function.__name__}_{scalar_field}", "int64_pk", 10000

        # connect to server before testing
        self._connect()

        # create a collection with fields that can build `BITMAP` index
        self.collection_wrap.init_collection(
            name=collection_name,
            schema=cf.set_collection_schema(
                fields=[primary_field, DataType.FLOAT_VECTOR.name, scalar_field],
                field_params={primary_field: FieldParams(is_primary=True).to_dict,
                              scalar_field: FieldParams(is_partition_key=True).to_dict},
            )
        )

        # prepare data (> 1024 triggering index building)
        self.collection_wrap.insert(data=cf.gen_values(self.collection_wrap.schema, nb=nb, default_values={
            scalar_field: [eval(f"{data_type}({random.randint(1, 4)})") for _ in range(nb)]
        }), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `BITMAP` index
        self.build_multi_index(index_params={
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name),
            **DefaultScalarIndexParams.BITMAP(scalar_field)
        })

        # load collection
        self.collection_wrap.load()

        # query
        expr = f'{scalar_field} == {expr_data}' if scalar_field == 'INT64' else f'{scalar_field} == "{expr_data}"'
        res, _ = self.collection_wrap.query(expr=expr, output_fields=[scalar_field], limit=100)
        assert set([r.get(scalar_field) for r in res]) == {expr_data}


@pytest.mark.xdist_group("TestGroupSearch")
class TestGroupSearch(TestCaseClassBase):
    """
    Testing group search scenarios
    1. collection schema:
        int64_pk(auto_id), varchar,
        float16_vector, float_vector, bfloat16_vector, sparse_vector,
        inverted_varchar
    2. varchar field is inserted with dup values for group by
    3. index for each vector field with different index types, dims and metric types
    Author: Yanliang567
    """
    def setup_class(self):
        super().setup_class(self)

        # connect to server before testing
        self._connect(self)

        # init params
        self.primary_field = "int64_pk"
        self.inverted_string_field = "varchar_inverted"

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("TestGroupSearch"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.VARCHAR.name, DataType.FLOAT16_VECTOR.name,
                        DataType.FLOAT_VECTOR.name, DataType.BFLOAT16_VECTOR.name, DataType.SPARSE_FLOAT_VECTOR.name,
                        DataType.INT8.name, DataType.INT64.name, DataType.BOOL.name,
                        self.inverted_string_field],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=31).to_dict,
                    DataType.FLOAT_VECTOR.name: FieldParams(dim=64).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=24).to_dict
                },
                auto_id=True
            )
        )

        self.vector_fields = [DataType.FLOAT16_VECTOR.name, DataType.FLOAT_VECTOR.name,
                              DataType.BFLOAT16_VECTOR.name, DataType.SPARSE_FLOAT_VECTOR.name]
        self.dims = [31, 64, 24, 99]
        self.index_types = [cp.IndexName.IVF_SQ8, cp.IndexName.HNSW, cp.IndexName.IVF_FLAT, cp.IndexName.SPARSE_WAND]

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        # prepare data (> 1024 triggering index building)
        nb = 100
        for _ in range(100):
            string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
            data = [string_values]
            for i in range(len(self.vector_fields)):
                data.append(cf.gen_vectors(dim=self.dims[i], nb=nb, vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[i])))
            data.append(pd.Series(data=[np.int8(i) for i in range(nb)], dtype="int8"))
            data.append(pd.Series(data=[np.int64(i) for i in range(nb)], dtype="int64"))
            data.append(pd.Series(data=[np.bool_(i) for i in range(nb)], dtype="bool"))
            data.append(pd.Series(data=[str(i) for i in range(nb)], dtype="string"))
            self.collection_wrap.insert(data)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build index for each vector field
        index_params = {
            **DefaultVectorIndexParams.IVF_SQ8(DataType.FLOAT16_VECTOR.name, metric_type=MetricType.L2),
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT_VECTOR.name, metric_type=MetricType.IP),
            **DefaultVectorIndexParams.DISKANN(DataType.BFLOAT16_VECTOR.name, metric_type=MetricType.COSINE),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name, metric_type=MetricType.IP),
            # index params for varchar field
            **DefaultScalarIndexParams.INVERTED(self.inverted_string_field)
        }

        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("group_by_field", [DataType.VARCHAR.name, "varchar_inverted"])
    def test_search_group_size(self, group_by_field):
        """
        target:
            1. search on 4 different float vector fields with group by varchar field with group size
        verify results entity = limit * group_size  and group size is full if strict_group_size is True
        verify results group counts = limit if strict_group_size is False
        """
        nq = 2
        limit = 50
        group_size = 5
        for j in range(len(self.vector_fields)):
            search_vectors = cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[j]))
            search_params = {"params": cf.get_search_params_params(self.index_types[j])}
            # when strict_group_size=true, it shall return results with entities = limit * group_size
            res1 = self.collection_wrap.search(data=search_vectors, anns_field=self.vector_fields[j],
                                               param=search_params, limit=limit,
                                               group_by_field=group_by_field,
                                               group_size=group_size, strict_group_size=True,
                                               output_fields=[group_by_field])[0]
            for i in range(nq):
                assert len(res1[i]) == limit * group_size
                for l in range(limit):
                    group_values = []
                    for k in range(group_size):
                        group_values.append(res1[i][l*group_size+k].fields.get(group_by_field))
                    assert len(set(group_values)) == 1

            # when strict_group_size=false, it shall return results with group counts = limit
            res1 = self.collection_wrap.search(data=search_vectors, anns_field=self.vector_fields[j],
                                               param=search_params, limit=limit,
                                               group_by_field=group_by_field,
                                               group_size=group_size, strict_group_size=False,
                                               output_fields=[group_by_field])[0]
            for i in range(nq):
                group_values = []
                for l in range(len(res1[i])):
                    group_values.append(res1[i][l].fields.get(group_by_field))
                assert len(set(group_values)) == limit

    @pytest.mark.tags(CaseLabel.L0)
    def test_hybrid_search_group_size(self):
        """
        hybrid search group by on 4 different float vector fields with group by varchar field with group size
        verify results returns with de-dup group values and group distances are in order as rank_group_scorer
        """
        nq = 2
        limit = 50
        group_size = 5
        req_list = []
        for j in range(len(self.vector_fields)):
            search_params = {
                "data": cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[j])),
                "anns_field": self.vector_fields[j],
                "param": {"params": cf.get_search_params_params(self.index_types[j])},
                "limit": limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_params)
            req_list.append(req)
        # 4. hybrid search group by
        rank_scorers = ["max", "avg", "sum"]
        for scorer in rank_scorers:
            res = self.collection_wrap.hybrid_search(req_list, WeightedRanker(0.1, 0.3, 0.9, 0.6),
                                                     limit=limit,
                                                     group_by_field=DataType.VARCHAR.name,
                                                     group_size=group_size, rank_group_scorer=scorer,
                                                     output_fields=[DataType.VARCHAR.name])[0]
            for i in range(nq):
                group_values = []
                for l in range(len(res[i])):
                    group_values.append(res[i][l].fields.get(DataType.VARCHAR.name))
                assert len(set(group_values)) == limit

                # group_distances = []
                tmp_distances = [100 for _ in range(group_size)]  # init with a large value
                group_distances = [res[i][0].distance]  # init with the first value
                for l in range(len(res[i]) - 1):
                    curr_group_value = res[i][l].fields.get(DataType.VARCHAR.name)
                    next_group_value = res[i][l + 1].fields.get(DataType.VARCHAR.name)
                    if curr_group_value == next_group_value:
                        group_distances.append(res[i][l + 1].distance)
                    else:
                        if scorer == 'sum':
                            assert np.sum(group_distances) <= np.sum(tmp_distances)
                        elif scorer == 'avg':
                            assert np.mean(group_distances) <= np.mean(tmp_distances)
                        else:  # default max
                            assert np.max(group_distances) <= np.max(tmp_distances)

                        tmp_distances = group_distances
                        group_distances = [res[i][l + 1].distance]

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_group_by(self):
        """
        verify hybrid search group by works with different Rankers
        """
        # 3. prepare search params
        req_list = []
        for i in range(len(self.vector_fields)):
            search_param = {
                "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[i])),
                "anns_field": self.vector_fields[i],
                "param": {},
                "limit": ct.default_limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search group by
        res = self.collection_wrap.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 0.2, 0.3), ct.default_limit,
                                                 group_by_field=DataType.VARCHAR.name,
                                                 output_fields=[DataType.VARCHAR.name],
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": ct.default_nq, "limit": ct.default_limit})[0]
        print(res)
        for i in range(ct.default_nq):
            group_values = []
            for l in range(ct.default_limit):
                group_values.append(res[i][l].fields.get(DataType.VARCHAR.name))
            assert len(group_values) == len(set(group_values))

        # 5. hybrid search with RRFRanker on one vector field with group by
        req_list = []
        for i in range(1, len(self.vector_fields)):
            search_param = {
                "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[i])),
                "anns_field": self.vector_fields[i],
                "param": {},
                "limit": ct.default_limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            self.collection_wrap.hybrid_search(req_list, RRFRanker(), ct.default_limit,
                                               group_by_field=self.inverted_string_field,
                                               check_task=CheckTasks.check_search_results,
                                               check_items={"nq": ct.default_nq, "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("support_field", [DataType.INT8.name,  DataType.INT64.name,
                                               DataType.BOOL.name, DataType.VARCHAR.name])
    def test_search_group_by_supported_scalars(self, support_field):
        """
        verify search group by works with supported scalar fields
        """
        nq = 2
        limit = 15
        for j in range(len(self.vector_fields)):
            search_vectors = cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[j]))
            search_params = {"params": cf.get_search_params_params(self.index_types[j])}
            res1 = self.collection_wrap.search(data=search_vectors, anns_field=self.vector_fields[j],
                                               param=search_params, limit=limit,
                                               group_by_field=support_field,
                                               output_fields=[support_field])[0]
            for i in range(nq):
                grpby_values = []
                dismatch = 0
                results_num = 2 if support_field == DataType.BOOL.name else limit
                for l in range(results_num):
                    top1 = res1[i][l]
                    top1_grpby_pk = top1.id
                    top1_grpby_value = top1.fields.get(support_field)
                    expr = f"{support_field}=={top1_grpby_value}"
                    if support_field == DataType.VARCHAR.name:
                        expr = f"{support_field}=='{top1_grpby_value}'"
                    grpby_values.append(top1_grpby_value)
                    res_tmp = self.collection_wrap.search(data=[search_vectors[i]], anns_field=self.vector_fields[j],
                                                          param=search_params, limit=1, expr=expr,
                                                          output_fields=[support_field])[0]
                    top1_expr_pk = res_tmp[0][0].id
                    if top1_grpby_pk != top1_expr_pk:
                        dismatch += 1
                        log.info(f"{support_field} on {self.vector_fields[j]} dismatch_item, top1_grpby_dis: {top1.distance}, top1_expr_dis: {res_tmp[0][0].distance}")
                log.info(f"{support_field} on {self.vector_fields[j]}  top1_dismatch_num: {dismatch}, results_num: {results_num}, dismatch_rate: {dismatch / results_num}")
                baseline = 1 if support_field == DataType.BOOL.name else 0.2    # skip baseline check for boolean
                assert dismatch / results_num <= baseline
                # verify no dup values of the group_by_field in results
                assert len(grpby_values) == len(set(grpby_values))

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_group_by(self):
        """
        verify search group by works with pagination
        """
        limit = 10
        page_rounds = 3
        search_param = {}
        default_search_exp = f"{self.primary_field} >= 0"
        grpby_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[1]))
        all_pages_ids = []
        all_pages_grpby_field_values = []
        for r in range(page_rounds):
            page_res = self.collection_wrap.search(search_vectors, anns_field=default_search_field,
                                                   param=search_param, limit=limit, offset=limit * r,
                                                   expr=default_search_exp, group_by_field=grpby_field,
                                                   output_fields=[grpby_field],
                                                   check_task=CheckTasks.check_search_results,
                                                   check_items={"nq": 1, "limit": limit},
                                                   )[0]
            for j in range(limit):
                all_pages_grpby_field_values.append(page_res[0][j].get(grpby_field))
            all_pages_ids += page_res[0].ids
        hit_rate = round(len(set(all_pages_grpby_field_values)) / len(all_pages_grpby_field_values), 3)
        assert hit_rate >= 0.8

        total_res = self.collection_wrap.search(search_vectors, anns_field=default_search_field,
                                                param=search_param, limit=limit * page_rounds,
                                                expr=default_search_exp, group_by_field=grpby_field,
                                                output_fields=[grpby_field],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1, "limit": limit * page_rounds}
                                                )[0]
        hit_num = len(set(total_res[0].ids).intersection(set(all_pages_ids)))
        hit_rate = round(hit_num / (limit * page_rounds), 3)
        assert hit_rate >= 0.8
        log.info(f"search pagination with groupby hit_rate: {hit_rate}")
        grpby_field_values = []
        for i in range(limit * page_rounds):
            grpby_field_values.append(total_res[0][i].fields.get(grpby_field))
        assert len(grpby_field_values) == len(set(grpby_field_values))

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_pagination_group_size(self):
        limit = 10
        group_size = 5
        page_rounds = 3
        search_param = {}
        default_search_exp = f"{self.primary_field} >= 0"
        grpby_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[1]))
        all_pages_ids = []
        all_pages_grpby_field_values = []
        res_count = limit * group_size
        for r in range(page_rounds):
            page_res = self.collection_wrap.search(search_vectors, anns_field=default_search_field,
                                                   param=search_param, limit=limit, offset=limit * r,
                                                   expr=default_search_exp,
                                                   group_by_field=grpby_field, group_size=group_size,
                                                   strict_group_size=True,
                                                   output_fields=[grpby_field],
                                                   check_task=CheckTasks.check_search_results,
                                                   check_items={"nq": 1, "limit": res_count},
                                                   )[0]
            for j in range(res_count):
                all_pages_grpby_field_values.append(page_res[0][j].get(grpby_field))
            all_pages_ids += page_res[0].ids

        hit_rate = round(len(set(all_pages_grpby_field_values)) / len(all_pages_grpby_field_values), 3)
        expect_hit_rate = round(1 / group_size, 3) * 0.7
        log.info(f"expect_hit_rate :{expect_hit_rate}, hit_rate:{hit_rate}, "
                 f"unique_group_by_value_count:{len(set(all_pages_grpby_field_values))},"
                 f"total_group_by_value_count:{len(all_pages_grpby_field_values)}")
        assert hit_rate >= expect_hit_rate

        total_count = limit * group_size * page_rounds
        total_res = self.collection_wrap.search(search_vectors, anns_field=default_search_field,
                                                param=search_param, limit=limit * page_rounds,
                                                expr=default_search_exp,
                                                group_by_field=grpby_field, group_size=group_size,
                                                strict_group_size=True,
                                                output_fields=[grpby_field],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1, "limit": total_count}
                                                )[0]
        hit_num = len(set(total_res[0].ids).intersection(set(all_pages_ids)))
        hit_rate = round(hit_num / (limit * page_rounds), 3)
        assert hit_rate >= 0.8
        log.info(f"search pagination with groupby hit_rate: {hit_rate}")
        grpby_field_values = []
        for i in range(total_count):
            grpby_field_values.append(total_res[0][i].fields.get(grpby_field))
        assert len(grpby_field_values) == total_count
        assert len(set(grpby_field_values)) == limit * page_rounds

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_group_size_min_max(self):
        """
        verify search group by works with min and max group size
        """
        group_by_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1], vector_data_type=cf.get_field_dtype_by_field_name(self.collection_wrap, self.vector_fields[1]))
        search_params = {}
        limit = 10
        max_group_size = 10
        self.collection_wrap.search(data=search_vectors, anns_field=default_search_field,
                                    param=search_params, limit=limit,
                                    group_by_field=group_by_field,
                                    group_size=max_group_size, strict_group_size=True,
                                    output_fields=[group_by_field])
        exceed_max_group_size = max_group_size + 1
        error = {ct.err_code: 999,
                 ct.err_msg: f"input group size:{exceed_max_group_size} exceeds configured max "
                             f"group size:{max_group_size}"}
        self.collection_wrap.search(data=search_vectors, anns_field=default_search_field,
                                    param=search_params, limit=limit,
                                    group_by_field=group_by_field,
                                    group_size=exceed_max_group_size, strict_group_size=True,
                                    output_fields=[group_by_field],
                                    check_task=CheckTasks.err_res, check_items=error)

        min_group_size = 1
        self.collection_wrap.search(data=search_vectors, anns_field=default_search_field,
                                    param=search_params, limit=limit,
                                    group_by_field=group_by_field,
                                    group_size=max_group_size, strict_group_size=True,
                                    output_fields=[group_by_field])
        below_min_group_size = min_group_size - 1
        error = {ct.err_code: 999,
                 ct.err_msg: f"input group size:{below_min_group_size} is negative"}
        self.collection_wrap.search(data=search_vectors, anns_field=default_search_field,
                                    param=search_params, limit=limit,
                                    group_by_field=group_by_field,
                                    group_size=below_min_group_size, strict_group_size=True,
                                    output_fields=[group_by_field],
                                    check_task=CheckTasks.err_res, check_items=error)
