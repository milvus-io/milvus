import re
import math  # do not remove `math`
import pytest
from pymilvus import DataType, AnnSearchRequest, RRFRanker

from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common.code_mapping import QueryErrorMessage as qem
from common.common_params import (
    FieldParams, MetricType, DefaultVectorIndexParams, DefaultScalarIndexParams, Expr, AlterIndexParams
)
from base.client_base import TestcaseBase, TestCaseClassBase


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
            # build Hybrid index
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
            # build Hybrid index
            **DefaultScalarIndexParams.list_bitmap(self.bitmap_support_dtype_names)
        }
        self.build_multi_index(index_params=index_params)
        assert sorted([n.field_name for n in self.collection_wrap.indexes]) == sorted(index_params.keys())

        # load collection
        self.collection_wrap.load()

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
            expr='int64_pk > 15', check_task=CheckTasks.check_search_iterator, check_items={"batch_size": batch_size})

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
            name=cf.gen_unique_str("test_bitmap_index_dql_expr"),
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
            name=cf.gen_unique_str("test_bitmap_index_dql_expr"),
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
