import numpy as np
import random
import pytest
import math
import threading
import multiprocessing
from pymilvus import DataType
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
half_nb = ct.default_nb // 2
field_name = ct.default_float_vec_field_name


@pytest.mark.xdist_group("TestSearchV2Shared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchV2Shared(TestMilvusClientV2Base):
    """Test search with shared collection.
    Schema: int64(PK), int32, int16, int8, bool, float, double, varchar(65535), json, float_vector(128)
    Data: 5000 rows, gen_row_data_by_schema(nb=5000, schema=schema)
    Index: HNSW on float_vector (M=16, efConstruction=500)
    Dynamic: False
    """

    shared_alias = "TestSearchV2Shared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchV2Shared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32)
        schema.add_field(ct.default_int16_field_name, DataType.INT16)
        schema.add_field(ct.default_int8_field_name, DataType.INT8)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        self.__class__.shared_nb = 5000
        self.__class__.shared_dim = default_dim
        data = cf.gen_row_data_by_schema(nb=self.shared_nb, schema=schema)
        self.__class__.shared_data = data
        self.__class__.shared_insert_ids = [i for i in range(self.shared_nb)]
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="HNSW", params={"M": 16, "efConstruction": 500})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        search_limit = nb // 2
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        # filter result with expression in collection
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_search_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                local_vars = {"int64": data[i][ct.default_int64_field_name],
                              "float": data[i][ct.default_float_field_name]}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            expected_limit = min(search_limit, len(filter_ids))

            # 3. search with expression
            search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=search_limit,
                                        filter=expr)
            # verify: results are subset of filter_ids (filter correctness)
            # and result count is within acceptable recall range for HNSW
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=search_limit,
                                        filter=expr, filter_params=expr_params)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

            # 5. search again with expression template and search hints
            search_param = default_search_params.copy()
            search_param.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, self.collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_param,
                                        limit=search_limit,
                                        filter=expr, filter_params=expr_params)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, bool_type):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        nb = self.shared_nb
        dim = self.shared_dim
        # Use nb//2 to avoid HNSW recall issues while still covering enough results
        search_limit = nb // 2
        client = self._client(alias=self.shared_alias)
        data = self.shared_data
        insert_ids = self.shared_insert_ids

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i, _id in enumerate(insert_ids):
            if data[i].get(ct.default_bool_field_name) == bool_type_cmp:
                filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        expected_limit = min(search_limit, len(filter_ids))
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=search_limit,
                                    filter=expression)

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = [hit[ct.default_int64_field_name] for hit in hits]
            assert set(ids).issubset(filter_ids_set)
            assert len(hits) >= expected_limit * 0.8, \
                f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", ct.all_scalar_data_types[:3])
    def test_search_expression_different_data_type(self, field):
        """
        target: test search expression using different supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        field_name_str = field.name.lower()
        num = int(field_name_str[3:])
        offset = 2 ** (num - 1)

        client = self._client(alias=self.shared_alias)

        # 3. search using expression which field value is out of bound
        expression = f"{field_name_str} >= {offset}"
        self.search(client, self.collection_name,
                    data=vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=expression,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq, "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        # 4. search normal using all the scalar type as output fields
        self.search(client, self.collection_name,
                    data=vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "output_fields": [field_name_str],
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: test search with valid round decimal
        method: search with valid round decimal
        expected: search successfully
        """
        tmp_nq = 1
        tmp_limit = 5
        client = self._client(alias=self.shared_alias)

        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % self.collection_name)
        res, _ = self.search(client, self.collection_name,
                             data=vectors[:tmp_nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=tmp_limit)

        res_round, _ = self.search(client, self.collection_name,
                                   data=vectors[:tmp_nq],
                                   anns_field=default_search_field,
                                   search_params=default_search_params,
                                   limit=tmp_limit,
                                   round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        # Build pk→distance map from unrounded results and match by entity pk
        pk = ct.default_int64_field_name
        dist_map = {res[0][i][pk]: res[0][i]["distance"] for i in range(tmp_limit)}
        for i in range(len(res_round[0])):
            _id = res_round[0][i][pk]
            if _id in dist_map:
                dis_expect = round(dist_map[_id], round_decimal)
                dis_actual = res_round[0][i]["distance"]
                assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)


class TestSearchV2LegacyIndependent(TestMilvusClientV2Base):
    """ Test case of search interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_with_expression_nullable(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        enable_dynamic_field = False
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # Create schema: int64 (pk), float (nullable), varchar, json, float_vector
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT,
                         nullable=True if null_data_percent > 0 else False)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # Sentinel that mimics SQL NULL semantics: all comparisons return False
        _null = type('_Null', (), {
            '__eq__': lambda s, o: False, '__ne__': lambda s, o: False,
            '__lt__': lambda s, o: False, '__le__': lambda s, o: False,
            '__gt__': lambda s, o: False, '__ge__': lambda s, o: False,
            '__hash__': lambda s: hash(None)})()

        # filter result with expression in collection
        insert_ids = [i for i in range(nb)]
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"test_search_with_expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                float_val = data[i][ct.default_float_field_name]
                local_vars = {"int64": data[i][ct.default_int64_field_name],
                              "float": float_val if float_val is not None else _null}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)

            # 3. search with expression
            search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and search hints
            search_param = default_search_params.copy()
            search_param.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_param,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_with_expression_bool(self, bool_type, null_data_percent):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create all-data-type schema
        schema = cf.gen_collection_schema_all_datatype(auto_id=auto_id, dim=dim,
                                                       enable_dynamic_field=enable_dynamic_field,
                                                       nullable_fields={ct.default_bool_field_name: null_data_percent})
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        for i, _id in enumerate(insert_ids):
            if data[i].get(ct.default_bool_field_name) == bool_type_cmp:
                filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=nb,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": default_nq,
                                                 "ids": insert_ids,
                                                 "limit": min(nb, len(filter_ids)),
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name})

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = [hit[ct.default_int64_field_name] for hit in hits]
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_with_expression_array(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = False
        # 1. create a collection
        nb = ct.default_nb
        dim = ct.default_dim
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Build array collection schema manually for V2
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(ct.default_int32_array_field_name, DataType.ARRAY,
                         element_type=DataType.INT32, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_float_array_field_name, DataType.ARRAY,
                         element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
        schema.add_field(ct.default_string_array_field_name, DataType.ARRAY,
                         element_type=DataType.VARCHAR, max_capacity=ct.default_max_capacity,
                         max_length=100, nullable=True)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        array_length = 10
        data = []
        for i in range(int(nb * (1 - null_data_percent))):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: [str(i) for i in range(array_length)]}
            data.append(arr)
        for i in range(int(nb * (1 - null_data_percent)), nb):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: None}
            data.append(arr)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 3. create FLAT index for 100% recall and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 4. filter result with expression in collection
        for expressions in cf.gen_array_field_expressions_and_templates():
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i in range(nb):
                int32_array = data[i][ct.default_int32_array_field_name]
                float_array = data[i][ct.default_float_array_field_name]
                string_array = data[i][ct.default_string_array_field_name]
                if ct.default_string_array_field_name in expr and string_array is None:
                    continue
                if not expr or eval(expr):
                    filter_ids.append(i)

            # 5. search with expression
            search_res, _ = self.search(client, collection_name,
                                        data=vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # 6. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

            # 7. search again with expression template and hints
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids) == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("exists", ["exists"])
    @pytest.mark.parametrize("json_field_name", ["json_field", "json_field['number']", "json_field['name']",
                                                 "float_array", "not_exist_field", "new_added_field"])
    def test_search_with_expression_exists(self, exists, json_field_name):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = True
        # 1. initialize with data
        nb = 100
        dim = ct.default_dim
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Build array collection schema with json for V2 with dynamic field
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Generate and insert data using row data by schema
        data = cf.gen_row_data_by_schema(nb, schema=schema)
        for i in range(nb):
            data[i]["new_added_field"] = i
        log.info(data[0])
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with expression
        expression = exists + " " + json_field_name
        limit = nb if json_field_name in data[0].keys() else 0
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        self.search(client, collection_name,
                    data=vectors[:default_nq],
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=nb,
                    filter=expression,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": limit,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_auto_id(self):
        """
        target: test search with different expressions
        method: test search with different expressions with auto id
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        # Use nb//2 to avoid IVF_FLAT recall issues while still covering enough results
        search_limit = nb // 2
        enable_dynamic_field = True
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with auto_id and dynamic fields
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="IVF_FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                local_vars = {default_float_field_name: data[i][default_float_field_name]}
                if not expr or eval(expr, {}, local_vars):
                    filter_ids.append(_id)
            # 3. search expressions
            expected_limit = min(search_limit, len(filter_ids))
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=search_limit,
                                        filter=expr)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=search_limit,
                                        filter=expr, filter_params=expr_params)
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
                assert len(hits) >= expected_limit * 0.8, \
                    f"recall too low: got {len(hits)}, expected >= {expected_limit * 0.8}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expr_json_field(self):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        nb = 2000
        dim = 64
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with dynamic field enabled
        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data - use gen_default_rows_data to get json with {"number": i, "float": i*1.0}
        data = cf.gen_default_rows_data(nb=nb, dim=dim, with_json=True)
        self.insert(client, collection_name, data=data)
        insert_ids = [i for i in range(nb)]
        self.flush(client, collection_name)

        # Create FLAT index for 100% recall and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for expressions in cf.gen_json_field_expressions_and_templates():
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            json_field = {}
            for i, _id in enumerate(insert_ids):
                json_field['number'] = data[i][ct.default_json_field_name]['number']
                json_field['float'] = data[i][ct.default_json_field_name]['float']
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # 3. search expressions
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            # 6. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            # 7. create json index
            idx2 = self.prepare_index_params(client)[0]
            idx2.add_index(field_name=ct.default_json_field_name,
                           index_type="INVERTED",
                           index_name=f"{ct.default_json_field_name}_0",
                           params={"json_cast_type": "double",
                                   "json_path": f"{ct.default_json_field_name}['number']"})
            self.create_index(client, collection_name, index_params=idx2)

            idx3 = self.prepare_index_params(client)[0]
            idx3.add_index(field_name=ct.default_json_field_name,
                           index_type="AUTOINDEX",
                           index_name=f"{ct.default_json_field_name}_1",
                           params={"json_cast_type": "double",
                                   "json_path": f"{ct.default_json_field_name}['float']"})
            self.create_index(client, collection_name, index_params=idx3)

            # 8. release and load to make sure the new index is loaded
            self.release_collection(client, collection_name)
            self.load_collection(client, collection_name)
            # 9. search expressions after json path index
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 10. search again with expression template after json path index
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=default_search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

            # 11. search again with expression template and hint after json path index
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors[:default_nq],
                                        anns_field=default_search_field,
                                        search_params=search_params,
                                        limit=nb,
                                        filter=expr, filter_params=expr_params,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": default_nq,
                                                     "ids": insert_ids,
                                                     "limit": min(nb, len(filter_ids)),
                                                     "enable_milvus_client_api": True,
                                                     "pk_name": ct.default_int64_field_name})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = [hit[ct.default_int64_field_name] for hit in hits]
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [200])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_expression_all_data_type(self, nq, null_data_percent):
        """
        target: test search using all supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create all-data-type schema with multiple vectors
        schema = cf.gen_collection_schema_all_datatype(auto_id=auto_id, dim=dim,
                                                       multiple_dim_array=[dim, dim],
                                                       nullable_fields=nullable_fields)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        # Extract all vector field names from schema (schema has 3 vector fields:
        # gen_collection_schema_all_datatype inserts dim at index 0 of multiple_dim_array)
        all_vector_names = []
        for f in schema.fields:
            if hasattr(f, 'dtype') and f.dtype in [DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR,
                                                    DataType.BFLOAT16_VECTOR, DataType.SPARSE_FLOAT_VECTOR]:
                all_vector_names.append(f.name)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        for vec_name in all_vector_names:
            if "SPARSE" in vec_name.upper():
                idx.add_index(field_name=vec_name, metric_type="IP",
                              index_type="SPARSE_INVERTED_INDEX", params={})
            else:
                idx.add_index(field_name=vec_name, metric_type="COSINE",
                              index_type="FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 2. search
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        limit = default_limit
        if null_data_percent == 1:
            limit = 0
            insert_ids = []
        for vector_field_name in all_vector_names:
            # Determine data type of the vector field
            vector_data_type = DataType.FLOAT_VECTOR
            for f in schema.fields:
                if hasattr(f, 'name') and f.name == vector_field_name:
                    vector_data_type = f.dtype
                    break
            search_vectors = cf.gen_vectors(nq, dim, vector_data_type)
            res, _ = self.search(client, collection_name,
                                 data=search_vectors[:nq],
                                 anns_field=vector_field_name,
                                 search_params=default_search_params,
                                 limit=default_limit,
                                 filter=search_exp,
                                 output_fields=[default_int64_field_name,
                                                default_float_field_name,
                                                default_bool_field_name],
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": nq,
                                              "ids": insert_ids,
                                              "limit": limit,
                                              "enable_milvus_client_api": True,
                                              "pk_name": ct.default_int64_field_name})
            if limit:
                assert default_int64_field_name in res[0][0]["entity"]
                assert default_float_field_name in res[0][0]["entity"]
                assert default_bool_field_name in res[0][0]["entity"]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", ct.all_scalar_data_types[:3])
    @pytest.mark.parametrize("null_data_percent", [0, 0.5, 1])
    def test_search_expression_different_data_type_nullable(self, field, null_data_percent):
        """
        target: test search expression using different supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        field_name_str = field.name.lower()
        num = int(field_name_str[3:])
        offset = 2 ** (num - 1)
        nullable_fields = {field_name_str: null_data_percent}

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        default_schema = cf.gen_collection_schema_all_datatype(nullable_fields=nullable_fields)
        self.create_collection(client, collection_name, schema=default_schema)

        # Insert data using all data type rows
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=default_schema, start=offset - 1000)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search using expression which field value is out of bound
        expression = f"{field_name_str} >= {offset}"
        self.search(client, collection_name,
                    data=vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    filter=expression,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq, "limit": 0,
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})
        # 4. search normal using all the scalar type as output fields
        self.search(client, collection_name,
                    data=vectors,
                    anns_field=default_search_field,
                    search_params=default_search_params,
                    limit=default_limit,
                    output_fields=[field_name_str],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "output_fields": [field_name_str],
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self):
        """
        target: test search with expression comparing two fields
        method: create a collection, insert data and search with comparative expression
        expected: search successfully
        """
        # 1. create a collection
        nb = 10
        dim = 2
        nq = 1
        client = self._client()
        collection_name = cf.gen_unique_str("comparison")

        schema = self.create_schema(client)[0]
        schema.add_field("int64_1", DataType.INT64, is_primary=True)
        schema.add_field("int64_2", DataType.INT64)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # 2. insert data
        data = []
        insert_ids = []
        for i in range(nb):
            row = {"int64_1": i, "int64_2": i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, dim)[0]}
            data.append(row)
            insert_ids.append(i)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        filter_ids = []
        for i in range(nb):
            if data[i]["int64_1"] <= data[i]["int64_2"]:
                filter_ids.append(data[i]["int64_1"])

        # 3. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        expression = "int64_1 <= int64_2"
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        res, _ = self.search(client, collection_name,
                             data=search_vectors[:nq],
                             anns_field=default_search_field,
                             search_params=default_search_params,
                             limit=default_limit,
                             filter=expression,
                             check_task=CheckTasks.check_search_results,
                             check_items={"nq": nq,
                                          "ids": insert_ids,
                                          "limit": default_limit,
                                          "enable_milvus_client_api": True,
                                          "pk_name": "int64_1"})
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = [hit["int64_1"] for hit in hits]
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: test search with expressions with double quotes
        method: test search with expressions with double quotes
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create default schema
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # Generate and insert initial data
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        # Override string values with special double-quote strings
        string_value = [(f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\""
                         f"{cf.gen_str_by_length(3)}\"") for _ in range(default_nb)]
        for i in range(default_nb):
            data[i][default_string_field_name] = string_value[i]
        insert_ids = [i for i in range(default_nb)]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with expression
        _id = random.randint(0, default_nb - 1)
        string_value[_id] = string_value[_id].replace("\"", "\\\"")
        expression = f"{default_string_field_name} == \"{string_value[_id]}\""
        log.debug("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = self.search(client, collection_name,
                                    data=vectors[:default_nq],
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": default_nq,
                                                 "ids": insert_ids,
                                                 "limit": 1,
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name})
        assert search_res[0][0][ct.default_int64_field_name] == _id

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [200])
    def test_search_concurrent_two_collections_nullable(self, nq):
        """
        target: test concurrent load/search with multi-processes between two collections with null data in json field
        method: concurrent load, and concurrent search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        enable_dynamic_field = False
        threads_num = 10
        threads = []

        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"

        # Create schema with nullable json
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name_1, schema=schema)
        self.create_collection(client, collection_name_2, schema=schema)

        # Insert data (with null json)
        vectors_data = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        data = []
        for i in range(default_nb):
            row = {
                ct.default_float_field_name: np.float32(i),
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: None,
                ct.default_float_vec_field_name: vectors_data[i]
            }
            data.append(row)
        insert_res_1, _ = self.insert(client, collection_name_1, data=data)
        insert_ids = insert_res_1["ids"]
        self.insert(client, collection_name_2, data=data)
        self.flush(client, collection_name_1)
        self.flush(client, collection_name_2)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, collection_name_1, index_params=idx)
        self.create_index(client, collection_name_2, index_params=idx)
        self.load_collection(client, collection_name_1)
        self.load_collection(client, collection_name_2)

        def search(coll_name):
            search_vectors = [[random.random() for _ in range(dim)]
                              for _ in range(nq)]
            self.search(client, coll_name,
                        data=search_vectors[:nq],
                        anns_field=default_search_field,
                        search_params=default_search_params,
                        limit=default_limit,
                        filter=default_search_exp,
                        check_task=CheckTasks.check_search_results,
                        check_items={"nq": nq,
                                     "ids": insert_ids,
                                     "limit": default_limit,
                                     "enable_milvus_client_api": True,
                                     "pk_name": ct.default_int64_field_name})

        # 2. search with multi-threads
        log.info("test_search_concurrent_two_collections_nullable: searching with %s threads" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_name_1,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @pytest.mark.skip(reason="Not running for now")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_insert_in_parallel(self):
        """
        target: test search and insert in parallel
        method: One process do search while other process do insert
        expected: No exception
        """
        nq = 1
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with schema
        self.create_collection(client, collection_name, dimension=ct.default_dim)

        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="vector", metric_type="L2",
                      index_type="IVF_FLAT", params={"nlist": 128})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        def do_insert():
            data = []
            for j in range(10000):
                data.append({"id": j, "vector": cf.gen_vectors(1, ct.default_dim)[0]})
            for i in range(11):
                self.insert(client, collection_name, data=data)
                log.info(f'Inserted batch {i}')

        def do_search():
            while True:
                results, _ = self.search(client, collection_name,
                                         data=cf.gen_vectors(nq, ct.default_dim),
                                         anns_field="vector",
                                         search_params=default_search_params,
                                         limit=default_limit,
                                         filter=default_search_exp)
                ids = []
                for res in results:
                    ids.extend([hit[ct.default_int64_field_name] for hit in res])
                if ids:
                    expr = f'{ct.default_int64_field_name} in {ids}'
                    self.query(client, collection_name, filter=expr,
                               output_fields=[ct.default_int64_field_name, ct.default_float_field_name])

        p_insert = multiprocessing.Process(target=do_insert, args=())
        p_search = multiprocessing.Process(target=do_search, args=(), daemon=True)

        p_insert.start()
        p_search.start()

        p_insert.join()

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: test search with large expression
        method: test search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with dynamic field, no json
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="IVF_FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nums,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: test search with large expression
        method: test one of the collection ids to another collection search for it, with the large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with dynamic field, no json
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        insert_res, _ = self.insert(client, collection_name, data=data)
        insert_ids = insert_res["ids"]
        self.flush(client, collection_name)

        # 2. create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE",
                      index_type="IVF_FLAT", params={"nlist": 100})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        nums = 5000
        search_vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        vectors_id = [random.randint(0, nums) for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=default_search_field,
                                    search_params=default_search_params,
                                    limit=default_limit,
                                    filter=expression,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={
                                        "nq": nums,
                                        "ids": insert_ids,
                                        "limit": default_limit,
                                        "enable_milvus_client_api": True,
                                        "pk_name": ct.default_int64_field_name,
                                    })
