import re
import math  # do not remove `math`
import pytest
import numpy as np
from pymilvus import DataType, AnnSearchRequest, RRFRanker, WeightedRanker

from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
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
        self.primary_field, nb = "int64_pk", 3000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_no_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name, *self().all_scalar_fields],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
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

    @pytest.mark.tags(CaseLabel.L2)
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
        self.primary_field, self.nb = "int64_pk", 3000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_hybrid_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name, *self().all_scalar_fields],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
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

        # build `Hybrid index`
        index_params = {
            **DefaultVectorIndexParams.DISKANN(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.IVF_SQ8(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_INVERTED_INDEX(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_IVF_FLAT(DataType.BINARY_VECTOR.name),
            # build Hybrid index
            **DefaultScalarIndexParams.list_default([self.primary_field] + self.all_index_scalar_fields)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
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

    @pytest.mark.tags(CaseLabel.L2)
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
        self.primary_field, nb = "int64_pk", 3000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_inverted_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name, *self().all_scalar_fields],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
                },
            )
        )

        # prepare data (> 1024 triggering index building)
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `INVERTED index`
        index_params = {
            **DefaultVectorIndexParams.IVF_FLAT(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.HNSW(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_FLAT(DataType.BINARY_VECTOR.name),
            # build INVERTED index
            **DefaultScalarIndexParams.list_inverted([self.primary_field] + self.inverted_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
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

    @pytest.mark.tags(CaseLabel.L2)
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
        self.primary_field, self.nb = "int64_pk", 3000

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_dql_expr"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.SPARSE_FLOAT_VECTOR.name, DataType.BINARY_VECTOR.name, *self().all_scalar_fields],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=3).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=6).to_dict,
                    DataType.BINARY_VECTOR.name: FieldParams(dim=16).to_dict
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
            **DefaultVectorIndexParams.HNSW(DataType.FLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.DISKANN(DataType.BFLOAT16_VECTOR.name),
            **DefaultVectorIndexParams.SPARSE_WAND(DataType.SPARSE_FLOAT_VECTOR.name),
            **DefaultVectorIndexParams.BIN_IVF_FLAT(DataType.BINARY_VECTOR.name),
            # build BITMAP index
            **DefaultScalarIndexParams.list_bitmap(self.bitmap_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    # https://github.com/milvus-io/milvus/issues/36221
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
            check_items={ct.err_code: 65535, ct.err_msg: "fail to Query on QueryNode"})

        self.collection_wrap.query(
            expr=Expr.array_contains_all('ARRAY_VARCHAR', [['a', 'b']]).value, limit=1, check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "fail to Query on QueryNode"})

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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [10, 1000])
    @pytest.mark.parametrize("group_by_field", ['INT8', 'INT16', 'INT32', 'INT64', 'BOOL', 'VARCHAR'])
    @pytest.mark.parametrize(
        "dim, search_params, vector_field",
        [(3, {"metric_type": MetricType.L2, "ef": 32}, DataType.FLOAT16_VECTOR.name),
         (1000, {"metric_type": MetricType.IP, "drop_ratio_search": 0.2}, DataType.SPARSE_FLOAT_VECTOR.name)])
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
        res, _ = self.collection_wrap.search(cf.gen_vectors(nb=1, dim=dim, vector_data_type=vector_field), vector_field,
                                             search_params, limit, group_by_field=group_by_field,
                                             output_fields=[group_by_field])
        output_values = [i.fields for r in res for i in r]

        # check output field
        assert len([True for i in output_values if set(i.keys()) != {group_by_field}]) == 0, f"res: {output_values}"

        # check `group_by_field` field values are unique
        values = [v for i in output_values for k, v in i.items()]

        assert len(values) == len(set(values)), f"values: {values}, output_values:{output_values}"

    @pytest.mark.tags(CaseLabel.L2)
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
        search_params, vector_field = {"metric_type": "L2", "ef": 32}, DataType.FLOAT16_VECTOR.name
        self.collection_wrap.search_iterator(
            cf.gen_vectors(nb=1, dim=3, vector_data_type=vector_field), vector_field, search_params, batch_size,
            expr='INT16 > 15', check_task=CheckTasks.check_search_iterator, check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
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
            check_items={"nq": nq, "ids": self.insert_data.get('int64_pk'), "limit": limit})


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

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_offset_cache"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT_VECTOR.name, *self().all_scalar_fields],
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

        # enable offset cache
        for index_name in self.bitmap_support_dtype_names:
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_offset_cache())

        # load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("expr, expr_field, rex", cf.gen_varchar_expression(['VARCHAR']))
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
            check_items={"nq": nq, "ids": self.insert_data.get('int64_pk'), "limit": limit})


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

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_bitmap_index_bitmap"),
            schema=cf.set_collection_schema(
                fields=[self.primary_field, DataType.FLOAT_VECTOR.name, *self().all_scalar_fields],
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

        # enable offset cache
        for index_name in self.bitmap_support_dtype_names:
            self.collection_wrap.alter_index(index_name=index_name, extra_params=AlterIndexParams.index_mmap())

        # load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
            check_items={"nq": nq, "ids": self.insert_data.get('int64_pk'), "limit": limit})


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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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


@pytest.mark.xdist_group("TestGroupSearch")
class TestGroupSearch(TestCaseClassBase):
    """
    Testing group search scenarios
    1. collection schema: int64_pk(auto_id), float16_vector, float_vector, bfloat16_vector, varchar
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
        self.indexed_string_field = "varchar_with_index"

        # create a collection with fields
        self.collection_wrap.init_collection(
            name=cf.gen_unique_str("test_group_search"),
            schema=cf.set_collection_schema(
                fields=[DataType.VARCHAR.name, self.primary_field, DataType.FLOAT16_VECTOR.name,
                        DataType.FLOAT_VECTOR.name, DataType.BFLOAT16_VECTOR.name,
                        DataType.INT8.name, DataType.INT64.name, DataType.BOOL.name,
                        self.indexed_string_field],
                field_params={
                    self.primary_field: FieldParams(is_primary=True).to_dict,
                    DataType.FLOAT16_VECTOR.name: FieldParams(dim=31).to_dict,
                    DataType.FLOAT_VECTOR.name: FieldParams(dim=64).to_dict,
                    DataType.BFLOAT16_VECTOR.name: FieldParams(dim=24).to_dict,
                },
                auto_id=True
            )
        )

        self.vector_fields = [DataType.FLOAT16_VECTOR.name, DataType.FLOAT_VECTOR.name, DataType.BFLOAT16_VECTOR.name]
        self.dims = [31, 64, 24]
        self.index_types = ["IVF_SQ8", "HNSW", "IVF_FLAT"]

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        # prepare data (> 1024 triggering index building)
        import pandas as pd
        nb = 100
        for _ in range(100):
            string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
            data = [string_values]
            for i in range(len(self.vector_fields)):
                data.append(cf.gen_vectors(dim=self.dims[i], nb=nb, vector_data_type=self.vector_fields[i]))
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
            **DefaultVectorIndexParams.IVF_FLAT(DataType.BFLOAT16_VECTOR.name, metric_type=MetricType.COSINE),
            # index params for varchar field
            **DefaultScalarIndexParams.INVERTED(self.indexed_string_field)
        }

        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("group_by_field", [DataType.VARCHAR.name, "varchar_with_index"])
    def test_search_group_size(self, group_by_field):
        """
        target:
            1. search on 3 different float vector fields with group by varchar field with group size
        verify results entity = limit * group_size  and group size is full if group_strict_size is True
        verify results group counts = limit if group_strict_size is False
        """
        nq = 2
        limit = 50
        group_size = 5
        for j in range(len(self.vector_fields)):
            search_vectors = cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=self.vector_fields[j])
            search_params = {"params": cf.get_search_params_params(self.index_types[j])}
            # when group_strict_size=true, it shall return results with entities = limit * group_size
            res1 = self.collection_wrap.search(data=search_vectors, anns_field=self.vector_fields[j],
                                               param=search_params, limit=limit,
                                               group_by_field=group_by_field,
                                               group_size=group_size, group_strict_size=True,
                                               output_fields=[group_by_field])[0]
            for i in range(nq):
                assert len(res1[i]) == limit * group_size
                for l in range(limit):
                    group_values = []
                    for k in range(group_size):
                        group_values.append(res1[i][l*group_size+k].fields.get(group_by_field))
                    assert len(set(group_values)) == 1

            # when group_strict_size=false, it shall return results with group counts = limit
            res1 = self.collection_wrap.search(data=search_vectors, anns_field=self.vector_fields[j],
                                               param=search_params, limit=limit,
                                               group_by_field=group_by_field,
                                               group_size=group_size, group_strict_size=False,
                                               output_fields=[group_by_field])[0]
            for i in range(nq):
                group_values = []
                for l in range(len(res1[i])):
                    group_values.append(res1[i][l].fields.get(group_by_field))
                assert len(set(group_values)) == limit

    @pytest.mark.tags(CaseLabel.L0)
    def test_hybrid_search_group_size(self):
        """
        hybrid search group by on 3 different float vector fields with group by varchar field with group size
        verify results returns with de-dup group values and group distances are in order as rank_group_scorer
        """
        nq = 2
        limit = 50
        group_size = 5
        req_list = []
        for j in range(len(self.vector_fields)):
            search_params = {
                "data": cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=self.vector_fields[j]),
                "anns_field": self.vector_fields[j],
                "param": {"params": cf.get_search_params_params(self.index_types[j])},
                "limit": limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_params)
            req_list.append(req)
        # 4. hybrid search group by
        rank_scorers = ["max", "avg", "sum"]
        for scorer in rank_scorers:
            res = self.collection_wrap.hybrid_search(req_list, WeightedRanker(0.3, 0.3, 0.3), limit=limit,
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
                            assert np.sum(group_distances) < np.sum(tmp_distances)
                        elif scorer == 'avg':
                            assert np.mean(group_distances) < np.mean(tmp_distances)
                        else:  # default max
                            assert np.max(group_distances) < np.max(tmp_distances)

                        tmp_distances = group_distances
                        group_distances = [res[i][l + 1].distance]

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_group_by(self):
        """
        verify hybrid search group by works with different Rankers
        """
        # 3. prepare search params
        req_list = []
        for i in range(len(self.vector_fields)):
            search_param = {
                "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i], vector_data_type=self.vector_fields[i]),
                "anns_field": self.vector_fields[i],
                "param": {},
                "limit": ct.default_limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search group by
        res = self.collection_wrap.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 0.2), ct.default_limit,
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
                "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i], vector_data_type=self.vector_fields[i]),
                "anns_field": self.vector_fields[i],
                "param": {},
                "limit": ct.default_limit,
                "expr": f"{self.primary_field} > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            self.collection_wrap.hybrid_search(req_list, RRFRanker(), ct.default_limit,
                                               group_by_field=self.indexed_string_field,
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
            search_vectors = cf.gen_vectors(nq, dim=self.dims[j], vector_data_type=self.vector_fields[j])
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
        grpby_field = self.indexed_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1], vector_data_type=self.vector_fields[1])
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
    @pytest.mark.skip(reason="issue #36401")
    def test_search_pagination_group_size(self):
        limit = 10
        group_size = 5
        page_rounds = 3
        search_param = {}
        default_search_exp = f"{self.primary_field} >= 0"
        grpby_field = self.indexed_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1], vector_data_type=self.vector_fields[1])
        all_pages_ids = []
        all_pages_grpby_field_values = []
        for r in range(page_rounds):
            page_res = self.collection_wrap.search(search_vectors, anns_field=default_search_field,
                                                   param=search_param, limit=limit, offset=limit * r,
                                                   expr=default_search_exp,
                                                   group_by_field=grpby_field, group_size=group_size,
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
                                                expr=default_search_exp,
                                                group_by_field=grpby_field, group_size=group_size,
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