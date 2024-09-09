import re
import pytest
from pymilvus import DataType

from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common.code_mapping import QueryErrorMessage as qem
from common.common_params import (
    IndexName, FieldParams, IndexPrams, DefaultVectorIndexParams, DefaultScalarIndexParams, MetricType, Expr
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

        # build `Hybrid index` on empty collection
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

    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36054")
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
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

    @pytest.mark.tags(CaseLabel.L1)
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
        self.primary_field, nb = "int64_pk", 3000

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
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `Hybrid index` on empty collection
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

    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36054")
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
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

    @pytest.mark.tags(CaseLabel.L1)
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

        # build `Hybrid index` on empty collection
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

    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36054")
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
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

    @pytest.mark.tags(CaseLabel.L1)
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
        self.primary_field, nb = "int64_pk", 3000

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
        self.insert_data = cf.gen_field_values(self.collection_wrap.schema, nb=nb)

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self):
        self.collection_wrap.insert(data=list(self.insert_data.values()), check_task=CheckTasks.check_insert_result)

        # flush collection, segment sealed
        self.collection_wrap.flush()

        # build `Hybrid index` on empty collection
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

    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36054")
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
        expr_count = len([i for i in self.insert_data.get(expr_field, []) if eval(expr.replace(expr_field, str(i)))])

        # query
        res, _ = self.collection_wrap.query(expr=expr, limit=limit, output_fields=[expr_field])
        assert len(res) == min(expr_count, limit), f"actual: {len(res)} == expect: {min(expr_count, limit)}"

    @pytest.mark.tags(CaseLabel.L1)
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
