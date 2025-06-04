import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
import numpy as np

prefix = "milvus_client_api_query"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name


@pytest.mark.xdist_group("TestStaticFieldNoIndexAllExpr")
class TestStaticFieldNoIndexAllExpr(TestMilvusClientV2Base):
    """
    Scalar fields are not indexed, and verify DQL requests
    """

    def setup_class(self):
        super().setup_class(self)
        # init params
        self.collection_name = cf.gen_collection_name_by_testcase_name(module_index=1)
        self.enable_dynamic_field = False
        self.ground_truth = {}

    @pytest.fixture(scope="class", autouse=True)
    def prepare_data(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()

        # Create collection
        # create schema
        schema = self.create_schema(client, enable_dynamic_field=self.enable_dynamic_field)[0]
        schema.add_field(ct.default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(ct.default_vector_field_name, DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field(ct.default_bool_field_name, DataType.BOOL, nullable=True)
        schema.add_field(ct.default_int8_field_name, DataType.INT8, nullable=True)
        schema.add_field(ct.default_int16_field_name, DataType.INT16, nullable=True)
        schema.add_field(ct.default_int32_field_name, DataType.INT32, nullable=True)
        schema.add_field(ct.default_int64_field_name, DataType.INT64, nullable=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=True)
        schema.add_field(ct.default_double_field_name, DataType.DOUBLE, nullable=True)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=100, nullable=True)
        schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=True)
        schema.add_field(ct.default_int8_array_field_name, datatype=DataType.ARRAY, element_type=DataType.INT8,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_int16_array_field_name, datatype=DataType.ARRAY, element_type=DataType.INT16,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_int32_array_field_name, datatype=DataType.ARRAY, element_type=DataType.INT32,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_int64_array_field_name, datatype=DataType.ARRAY, element_type=DataType.INT64,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_bool_array_field_name, datatype=DataType.ARRAY, element_type=DataType.BOOL,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_float_array_field_name, datatype=DataType.ARRAY, element_type=DataType.FLOAT,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_double_array_field_name, datatype=DataType.ARRAY, element_type=DataType.DOUBLE,
                         max_capacity=5, nullable=True)
        schema.add_field(ct.default_string_array_field_name, datatype=DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_capacity=5, max_length=100, nullable=True)
        # prepare index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        # create collection with the above schema and index params
        self.create_collection(client, self.collection_name, schema=schema,
                               index_params=index_params, force_teardown=False)
        # Generate vectors and all scalar data
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = 50
        rows_list = []
        for i in range(len(inserted_data_distribution)):
            rows = [{ct.default_primary_key_field_name: j, ct.default_vector_field_name: vectors[j],
                     ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                     ct.default_int8_field_name: np.int8(j) if (i % 2 == 0) else None,
                     ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                     ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                     ct.default_int64_field_name: j if (i % 2 == 0) else None,
                     ct.default_float_field_name: j * 1.0 if (i % 2 == 0) else None,
                     ct.default_double_field_name: j * 1.0 if (i % 2 == 0) else None,
                     ct.default_string_field_name: f'{j}' if (i % 2 == 0) else None,
                     ct.default_json_field_name: inserted_data_distribution[i],
                     ct.default_int8_array_field_name: [np.int8(j), np.int8(j)] if (i % 2 == 0) else None,
                     ct.default_int16_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                     ct.default_int32_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                     ct.default_int64_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                     ct.default_bool_array_field_name: [bool(j), bool(j + 1)] if (i % 2 == 0) else None,
                     ct.default_float_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                     ct.default_double_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                     ct.default_string_array_field_name: [f'{j}', f'{j + 1}'] if (i % 2 == 0) else None
                     } for j in range(i * nb_single, (i + 1) * nb_single)]
            assert len(rows) == nb_single
            # insert
            self.insert(client, collection_name=self.collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
            rows_list.extend(rows)
        assert len(rows_list) == nb_single * len(inserted_data_distribution)
        # calculated the ground truth for all fields with its supported expressions
        expr_fields = ct.all_expr_fields
        compare_dict = {}
        one_dict = {}
        for field in expr_fields:
            globals()[field] = rows_list[0][field]
        for field in expr_fields:
            express_list = cf.gen_field_expressions_all_single_operator_each_field(field)
            for i in range(len(express_list)):
                expression = express_list[i].replace("&&", "and").replace("||", "or")
                compare_dict.setdefault(field, {})
                one_dict.setdefault(f'{field}', [])
                compare_dict[field].setdefault(f'{i}', one_dict)
                compare_dict[field][f'{i}'].setdefault("id_list", [])
                for j in range(nb_single*len(inserted_data_distribution)):
                    globals()[field] = rows_list[j][field]
                    log.info("binbin_debug1")
                    log.info(field)
                    if (int8 is None) or (int16 is None) or (int32 is None) or (int64 is None)\
                            or (float is None) or (double is None) or (varchar is None) or (bool_field is None)\
                            or (int8_array is None) or (int16_array is None) or (int32_array is None) or (int64_array is None)\
                            or (bool_array is None) or (float_array is None) or (double_array is None) or (string_array is None):
                        if "is null" or "IS NULL" in expression:
                            compare_dict[field][f'{i}'][field].append(rows_list[j][field])
                            compare_dict[field][f'{i}']["id_list"].append(
                                rows_list[j][ct.default_primary_key_field_name])
                        continue
                    else:
                        if ("is not null" in expression) or ("IS NOT NULL" in expression):
                            compare_dict[field][f'{i}'][field].append(rows_list[j][field])
                            compare_dict[field][f'{i}']["id_list"].append(
                                rows_list[j][ct.default_primary_key_field_name])
                            continue
                        if ("is null" in expression) or ("IS NULL" in expression):
                            continue
                    log.info("binbin_debug")
                    log.info(expression)
                    if not expression or eval(expression):
                        compare_dict[field][f'{i}'][field].append(rows_list[j][field])
                        compare_dict[field][f'{i}']["id_list"].append(rows_list[j][ct.default_primary_key_field_name])
        log.info("binbin_debug_2")
        # log.info(compare_dict)
        self.ground_truth = compare_dict
        # flush collection, segment sealed
        self.flush(client, self.collection_name)
        # load collection
        self.load_collection(client, self.collection_name)
        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

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

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("expr_field", ct.all_expr_fields)
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_all(self, expr_field):
        """
        target: test query using expression fields with all supported field type after all supported scalar index
                with all supported basic expressions
        method: Query using expression on all supported fields after all scalar indexes with all supported basic expressions
        step: 1. create collection
              2. insert with different data distribution
              3. flush if specified
              4. query when there is no index applying on each field under all supported expressions
              5. release if specified
              6. prepare index params with all supported scalar index on all scalar fields
              7. create index
              8. create same index twice
              9. reload collection if released before to make sure the new index load successfully
              10. sleep for 60s to make sure the new index load successfully without release and reload operations
              11. query after there is index applying on each supported field under all supported expressions
                  which should get the same result with that without index
        expected: query successfully after there is index applying on each supported field under all expressions which
                  should get the same result with that without index
        """
        client = self._client()

        express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
        compare_dict = self.ground_truth[expr_field]
        for i in range(len(express_list)):
            expression = express_list[i]
            json_list = []
            id_list = []
            log.info(f"query with filter '{expression}' without scalar index is:")
            count = self.query(client, collection_name=self.collection_name, filter=expression,
                               output_fields=["count(*)"])[0]
            log.info(f"The count(*) after query with filter '{expression}' without scalar index is: {count}")
            assert count == len(compare_dict[f'{i}']["id_list"])
            res = self.query(client, collection_name=self.collection_name, filter=expression,
                             output_fields=[expr_field])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[expr_field])
            if len(json_list) != len(compare_dict[f'{i}'][expr_field]):
                log.debug(f"the field {expr_field} value without scalar index under expression '{expression}' is:")
                log.debug(json_list)
                log.debug(f"the field {expr_field} value without scalar index to be compared under expression '{expression}' is:")
                log.debug(compare_dict[f'{i}'][expr_field])
            assert json_list == compare_dict[f'{i}'][expr_field]
            if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
                log.debug(f"primary key field {default_primary_key_field_name} without scalar index under expression '{expression}' is:")
                log.debug(id_list)
                log.debug(f"primary key field {default_primary_key_field_name} without scalar index to be compared under expression '{expression}' is:")
                log.debug(compare_dict[f'{i}']["id_list"])
            assert id_list == compare_dict[f'{i}']["id_list"]
            log.info(f"PASS with expression {expression}")




