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


class TestMilvusClientDataIntegrity(TestMilvusClientV2Base):
    """ Test case of data integrity interface """

    @pytest.fixture(scope="function", params=["INVERTED", "BITMAP"])
    def supported_bool_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def supported_numeric_float_double_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED", "BITMAP"])
    def supported_numeric_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["TRIE", "INVERTED", "BITMAP"])
    def supported_varchar_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_json_path_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["INVERTED", "BITMAP"])
    def supported_array_scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["INVERTED"])
    def supported_array_double_float_scalar_index(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True])
    @pytest.mark.parametrize("is_release", [True])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize("expr_field", [ct.default_int64_field_name,
                                            ct.default_string_field_name,
                                            ct.default_float_array_field_name])
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array(self,
                                                                                            enable_dynamic_field,
                                                                                            supported_numeric_scalar_index,
                                                                                            supported_varchar_scalar_index,
                                                                                            supported_json_path_index,
                                                                                            supported_array_double_float_scalar_index,
                                                                                            is_flush,
                                                                                            is_release,
                                                                                            single_data_num,
                                                                                            expr_field):
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
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        default_dim = 5
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        if not enable_dynamic_field:
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
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [{default_primary_key_field_name: j, default_vector_field_name: vectors[j],
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
            # log.info(rows)
            self.insert(client, collection_name=collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no index under all expressions
        express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
        compare_dict = {}
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' before scalar index")
            res = self.query(client, collection_name=collection_name,
                             filter=express_list[i], output_fields=["count(*)"])[0]
            count = res[0]['count(*)']
            # log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(client, collection_name=collection_name,
                             filter=express_list[i], output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f'{i}', {})
            compare_dict[f'{i}']["id_list"] = id_list
            compare_dict[f'{i}']["json_list"] = json_list
        # 5. release if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 6. prepare index params with json path index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        # index_params.add_index(field_name=ct.default_bool_field_name, index_type=supported_bool_scalar_index)
        # index_params.add_index(field_name=ct.default_int8_field_name, index_type=supported_numeric_scalar_index)
        # index_params.add_index(field_name=ct.default_int16_field_name, index_type=supported_numeric_scalar_index)
        # index_params.add_index(field_name=ct.default_int32_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int64_field_name, index_type=supported_numeric_scalar_index)
        # index_params.add_index(field_name=ct.default_float_field_name, index_type=supported_numeric_float_double_index)
        # index_params.add_index(field_name=ct.default_double_field_name, index_type=supported_numeric_float_double_index)
        # index_params.add_index(field_name=ct.default_string_field_name, index_type=supported_varchar_scalar_index)
        # index_params.add_index(field_name=ct.default_int8_array_field_name, index_type=supported_array_scalar_index)
        # index_params.add_index(field_name=ct.default_int16_array_field_name, index_type=supported_array_scalar_index)
        # index_params.add_index(field_name=ct.default_int32_array_field_name, index_type=supported_array_scalar_index)
        # index_params.add_index(field_name=ct.default_int64_array_field_name, index_type=supported_array_scalar_index)
        # index_params.add_index(field_name=ct.default_bool_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_float_array_field_name,
                               index_type=supported_array_double_float_scalar_index)
        # index_params.add_index(field_name=ct.default_double_array_field_name,
        #                        index_type=supported_array_double_float_scalar_index)
        # index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [f"{ct.default_json_field_name}",
                          f"{ct.default_json_field_name}[0]",
                          f"{ct.default_json_field_name}[1]",
                          f"{ct.default_json_field_name}[6]",
                          f"{ct.default_json_field_name}[10000]",
                          f"{ct.default_json_field_name}['a']",
                          f"{ct.default_json_field_name}['a']['b']",
                          f"{ct.default_json_field_name}['a'][0]",
                          f"{ct.default_json_field_name}['a'][6]",
                          f"{ct.default_json_field_name}['a'][0]['b']",
                          f"{ct.default_json_field_name}['a']['b']['c']",
                          f"{ct.default_json_field_name}['a']['b'][0]['d']",
                          f"{ct.default_json_field_name}['a']['c'][0]['d']"]
        for i in range(len(json_path_list)):
            index_params.add_index(field_name=ct.default_json_field_name, index_name=json_index_name + f'{i}',
                                   index_type=supported_json_path_index,
                                   params={"json_cast_type": "DOUBLE",
                                           "json_path": json_path_list[i]})
        # 7. create index
        self.create_index(client, collection_name, index_params)
        # 8. create same twice
        self.create_index(client, collection_name, index_params)
        # 9. reload collection if released before to make sure the new index load successfully
        if is_release:
            self.load_collection(client, collection_name)
        else:
            # 10. sleep for 60s to make sure the new index load successfully without release and reload operations
            time.sleep(60)
        # 11. query after there is index under all expressions which should get the same result
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' after index")
            count = self.query(client, collection_name=collection_name, filter=express_list[i],
                               output_fields=["count(*)"])[0]
            # log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            # if len(json_list) != len(compare_dict[f'{i}']["json_list"]):
            #     log.debug(
            #         f"the field {expr_field} value after indexed under expression '{express_list[i]}' is:")
            #     log.debug(json_list)
            #     log.debug(
            #         f"the field {expr_field} value before index to be compared under expression '{express_list[i]}' is:")
            #     log.debug(compare_dict[f'{i}']["json_list"])
            assert json_list == compare_dict[f'{i}']["json_list"]
            # if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
            #     log.debug(
            #         f"primary key field {default_primary_key_field_name} after indexed under expression '{express_list[i]}' is:")
            #     log.debug(id_list)
            #     log.debug(
            #         f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:")
            #     log.debug(compare_dict[f'{i}']["id_list"])
            assert id_list == compare_dict[f'{i}']["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize("expr_field", [ct.default_int8_field_name, ct.default_int16_field_name,
                                            ct.default_int32_field_name, ct.default_int64_field_name,
                                            ct.default_float_field_name, ct.default_double_field_name,
                                            ct.default_string_field_name, ct.default_bool_field_name,
                                            ct.default_int8_array_field_name, ct.default_int16_array_field_name,
                                            ct.default_int32_array_field_name, ct.default_int64_array_field_name,
                                            ct.default_bool_array_field_name, ct.default_float_array_field_name,
                                            ct.default_double_array_field_name, ct.default_string_array_field_name])
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_all(self, enable_dynamic_field,
                                                                                                supported_bool_scalar_index,
                                                                                                supported_numeric_float_double_index,
                                                                                                supported_numeric_scalar_index,
                                                                                                supported_varchar_scalar_index,
                                                                                                supported_json_path_index,
                                                                                                supported_array_scalar_index,
                                                                                                supported_array_double_float_scalar_index,
                                                                                                is_flush,
                                                                                                is_release,
                                                                                                single_data_num,
                                                                                                expr_field):
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
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        default_dim = 5
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        if not enable_dynamic_field:
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
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [{default_primary_key_field_name: j, default_vector_field_name: vectors[j],
                     ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                     ct.default_int8_field_name: np.int8(j) if (i % 2 == 0) else None,
                     ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                     ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                     ct.default_int64_field_name: j if (i % 2 == 0) else None,
                     ct.default_float_field_name: j*1.0 if (i % 2 == 0) else None,
                     ct.default_double_field_name: j*1.0 if (i % 2 == 0) else None,
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
            # log.info(rows)
            self.insert(client, collection_name=collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no index under all expressions
        express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
        compare_dict = {}
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' before scalar index")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"])[0]
            count = res[0]['count(*)']
            # log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f'{i}', {})
            compare_dict[f'{i}']["id_list"] = id_list
            compare_dict[f'{i}']["json_list"] = json_list
        # 5. release if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 6. prepare index params with json path index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=ct.default_bool_field_name, index_type=supported_bool_scalar_index)
        index_params.add_index(field_name=ct.default_int8_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int16_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int32_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int64_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_float_field_name, index_type=supported_numeric_float_double_index)
        index_params.add_index(field_name=ct.default_double_field_name, index_type=supported_numeric_float_double_index)
        index_params.add_index(field_name=ct.default_string_field_name, index_type=supported_varchar_scalar_index)
        index_params.add_index(field_name=ct.default_int8_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int16_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int32_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int64_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_bool_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_float_array_field_name, index_type=supported_array_double_float_scalar_index)
        index_params.add_index(field_name=ct.default_double_array_field_name, index_type=supported_array_double_float_scalar_index)
        index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [f"{ct.default_json_field_name}",
                          f"{ct.default_json_field_name}[0]",
                          f"{ct.default_json_field_name}[1]",
                          f"{ct.default_json_field_name}[6]",
                          f"{ct.default_json_field_name}[10000]",
                          f"{ct.default_json_field_name}['a']",
                          f"{ct.default_json_field_name}['a']['b']",
                          f"{ct.default_json_field_name}['a'][0]",
                          f"{ct.default_json_field_name}['a'][6]",
                          f"{ct.default_json_field_name}['a'][0]['b']",
                          f"{ct.default_json_field_name}['a']['b']['c']",
                          f"{ct.default_json_field_name}['a']['b'][0]['d']",
                          f"{ct.default_json_field_name}['a']['c'][0]['d']"]
        for i in range(len(json_path_list)):
            index_params.add_index(field_name=ct.default_json_field_name, index_name=json_index_name + f'{i}',
                                   index_type=supported_json_path_index,
                                   params={"json_cast_type": "DOUBLE",
                                           "json_path": json_path_list[i]})
        # 7. create index
        self.create_index(client, collection_name, index_params)
        # 8. create same twice
        self.create_index(client, collection_name, index_params)
        # 9. reload collection if released before to make sure the new index load successfully
        if is_release:
            self.load_collection(client, collection_name)
        else:
            # 10. sleep for 60s to make sure the new index load successfully without release and reload operations
            time.sleep(60)
        # 11. query after there is index under all expressions which should get the same result
        # with that without index
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' after index")
            count = self.query(client, collection_name=collection_name, filter=express_list[i],
                               output_fields=["count(*)"])[0]
            # log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            if len(json_list) != len(compare_dict[f'{i}']["json_list"]):
                log.debug(f"the field {expr_field} value after index {supported_array_scalar_index} under expression '{express_list[i]}' is:")
                log.debug(json_list)
                log.debug(f"the field {expr_field} value before index to be compared under expression '{express_list[i]}' is:")
                log.debug(compare_dict[f'{i}']["json_list"])
            assert json_list == compare_dict[f'{i}']["json_list"]
            if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
                log.debug(f"primary key field {default_primary_key_field_name} after index {supported_array_scalar_index} under expression '{express_list[i]}' is:")
                log.debug(id_list)
                log.debug(f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:")
                log.debug(compare_dict[f'{i}']["id_list"])
            assert id_list == compare_dict[f'{i}']["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize("expr_field", [ct.default_int8_field_name, ct.default_int16_field_name,
                                            ct.default_int32_field_name, ct.default_int64_field_name,
                                            ct.default_float_field_name, ct.default_double_field_name,
                                            ct.default_string_field_name, ct.default_bool_field_name,
                                            ct.default_int8_array_field_name, ct.default_int16_array_field_name,
                                            ct.default_int32_array_field_name,ct.default_int64_array_field_name,
                                            ct.default_bool_array_field_name, ct.default_float_array_field_name,
                                            ct.default_double_array_field_name, ct.default_string_array_field_name])
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_auto_index(self, enable_dynamic_field,
                                                                                                       supported_json_path_index,
                                                                                                       is_flush, is_release,
                                                                                                       single_data_num, expr_field):
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
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        default_dim = 5
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        if not enable_dynamic_field:
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
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb+60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [{default_primary_key_field_name: j, default_vector_field_name: vectors[j],
                     ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                     ct.default_int8_field_name: np.int8(j) if (i % 2 == 0) else None,
                     ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                     ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                     ct.default_int64_field_name: j if (i % 2 == 0) else None,
                     ct.default_float_field_name: j*1.0 if (i % 2 == 0) else None,
                     ct.default_double_field_name: j*1.0 if (i % 2 == 0) else None,
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
            log.info(rows)
            self.insert(client, collection_name=collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no index under all expressions
        express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
        compare_dict = {}
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' before scalar index is:")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"])[0]
            count = res[0]['count(*)']
            log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f'{i}', {})
            compare_dict[f'{i}']["id_list"] = id_list
            compare_dict[f'{i}']["json_list"] = json_list
        # 5. release if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 6. prepare index params with json path index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=ct.default_bool_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int8_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int16_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int32_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int64_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_float_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_double_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_string_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int8_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int16_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int32_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_int64_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_bool_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_float_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_double_array_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=ct.default_string_array_field_name, index_type="AUTOINDEX")
        json_index_name = "json_index_name"
        json_path_list = [f"{ct.default_json_field_name}",
                          f"{ct.default_json_field_name}[0]",
                          f"{ct.default_json_field_name}[1]",
                          f"{ct.default_json_field_name}[6]",
                          f"{ct.default_json_field_name}[10000]",
                          f"{ct.default_json_field_name}['a']",
                          f"{ct.default_json_field_name}['a']['b']",
                          f"{ct.default_json_field_name}['a'][0]",
                          f"{ct.default_json_field_name}['a'][6]",
                          f"{ct.default_json_field_name}['a'][0]['b']",
                          f"{ct.default_json_field_name}['a']['b']['c']",
                          f"{ct.default_json_field_name}['a']['b'][0]['d']",
                          f"{ct.default_json_field_name}['a']['c'][0]['d']"]
        for i in range(len(json_path_list)):
            index_params.add_index(field_name=ct.default_json_field_name, index_name=json_index_name + f'{i}',
                                   index_type=supported_json_path_index,
                                   params={"json_cast_type": "DOUBLE",
                                           "json_path": json_path_list[i]})
        # 7. create index
        self.create_index(client, collection_name, index_params)
        # 8. create same twice
        self.create_index(client, collection_name, index_params)
        # 9. reload collection if released before to make sure the new index load successfully
        if is_release:
            self.load_collection(client, collection_name)
        else:
            # 10. sleep for 60s to make sure the new index load successfully without release and reload operations
            time.sleep(60)
        # 11. query after there is index under all expressions which should get the same result
        # with that without index
        for i in range(len(express_list)):
            json_list = []
            id_list = []
            log.info(f"query with filter '{express_list[i]}' after index is:")
            count = self.query(client, collection_name=collection_name, filter=express_list[i],
                               output_fields=["count(*)"])[0]
            log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=[f"{expr_field}"])[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            if len(json_list) != len(compare_dict[f'{i}']["json_list"]):
                log.debug(f"the field {expr_field} value after index 'AUTOINDEX' under expression '{express_list[i]}' is:")
                log.debug(json_list)
                log.debug(f"the field {expr_field} value before index to be compared under expression '{express_list[i]}' is:")
                log.debug(compare_dict[f'{i}']["json_list"])
            assert json_list == compare_dict[f'{i}']["json_list"]
            if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
                log.debug(f"primary key field {default_primary_key_field_name} after index 'AUTOINDEX' under expression '{express_list[i]}' is:")
                log.debug(id_list)
                log.debug(f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:")
                log.debug(compare_dict[f'{i}']["id_list"])
            assert id_list == compare_dict[f'{i}']["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize("random_filter_field_number", [2, 6, 16])
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_multiple_fields(self,
                                                                                               enable_dynamic_field,
                                                                                               supported_bool_scalar_index,
                                                                                               supported_numeric_float_double_index,
                                                                                               supported_numeric_scalar_index,
                                                                                               supported_varchar_scalar_index,
                                                                                               supported_json_path_index,
                                                                                               supported_array_scalar_index,
                                                                                               supported_array_double_float_scalar_index,
                                                                                               is_flush,
                                                                                               is_release,
                                                                                               single_data_num,
                                                                                               random_filter_field_number):
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
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        default_dim = 5
        schema = self.create_schema(client, enable_dynamic_field=enable_dynamic_field)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        if not enable_dynamic_field:
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
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [{default_primary_key_field_name: j, default_vector_field_name: vectors[j],
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
            self.insert(client, collection_name=collection_name, data=rows)
            log.debug(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no index under all expressions
        express_list, field_lists = cf.gen_multiple_field_expressions(random_field_number=random_filter_field_number)
        compare_dict = {}
        for i in range(len(express_list)):
            id_list = []
            log.info(f"query with filter '{express_list[i]}' before scalar index is:")
            res = \
            self.query(client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"])[0]
            count = res[0]['count(*)']
            log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=field_lists)[0]
            # compare_dict.setdefault(f'{i}', {})
            one_dict = {}
            # init the compared dict
            for field_name in field_lists:
                one_dict.setdefault(f'{field_name}', [])
                compare_dict.setdefault(f'{i}', one_dict)
            # extract and store the id and output_fields value used for compare after index
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                for field_name in field_lists:
                    compare_dict[f'{i}'][f'{field_name}'].append(single[f"{field_name}"])
            assert count == len(id_list)
            for field_name in field_lists:
                assert count == len(compare_dict[f'{i}'][f'{field_name}'])
            compare_dict[f'{i}']['id_list'] = id_list
        # 5. release if specified
        if is_release:
            self.release_collection(client, collection_name)
            self.drop_index(client, collection_name, default_vector_field_name)
        # 6. prepare index params with json path index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index(field_name=ct.default_bool_field_name, index_type=supported_bool_scalar_index)
        index_params.add_index(field_name=ct.default_int8_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int16_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int32_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_int64_field_name, index_type=supported_numeric_scalar_index)
        index_params.add_index(field_name=ct.default_float_field_name, index_type=supported_numeric_float_double_index)
        index_params.add_index(field_name=ct.default_double_field_name, index_type=supported_numeric_float_double_index)
        index_params.add_index(field_name=ct.default_string_field_name, index_type=supported_varchar_scalar_index)
        index_params.add_index(field_name=ct.default_int8_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int16_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int32_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_int64_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_bool_array_field_name, index_type=supported_array_scalar_index)
        index_params.add_index(field_name=ct.default_float_array_field_name,
                               index_type=supported_array_double_float_scalar_index)
        index_params.add_index(field_name=ct.default_double_array_field_name,
                               index_type=supported_array_double_float_scalar_index)
        index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [f"{ct.default_json_field_name}",
                          f"{ct.default_json_field_name}[0]",
                          f"{ct.default_json_field_name}[1]",
                          f"{ct.default_json_field_name}[6]",
                          f"{ct.default_json_field_name}[10000]",
                          f"{ct.default_json_field_name}['a']",
                          f"{ct.default_json_field_name}['a']['b']",
                          f"{ct.default_json_field_name}['a'][0]",
                          f"{ct.default_json_field_name}['a'][6]",
                          f"{ct.default_json_field_name}['a'][0]['b']",
                          f"{ct.default_json_field_name}['a']['b']['c']",
                          f"{ct.default_json_field_name}['a']['b'][0]['d']",
                          f"{ct.default_json_field_name}['a']['c'][0]['d']"]
        for i in range(len(json_path_list)):
            index_params.add_index(field_name=ct.default_json_field_name, index_name=json_index_name + f'{i}',
                                   index_type=supported_json_path_index,
                                   params={"json_cast_type": "DOUBLE",
                                           "json_path": json_path_list[i]})
        # 7. create index
        self.create_index(client, collection_name, index_params)
        # # 8. create same twice
        # self.create_index(client, collection_name, index_params)
        # 9. reload collection if released before to make sure the new index load successfully
        if is_release:
            self.load_collection(client, collection_name)
        else:
            # 10. sleep for 60s to make sure the new index load successfully without release and reload operations
            time.sleep(60)
        # 11. query after there is index under all expressions which should get the same result
        # with that without index
        for i in range(len(express_list)):
            id_list = []
            log.info(f"query with filter '{express_list[i]}' after index is:")
            count = self.query(client, collection_name=collection_name, filter=express_list[i],
                               output_fields=["count(*)"])[0]
            log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(client, collection_name=collection_name, filter=express_list[i],
                             output_fields=field_lists)[0]
            # compare each filtered field before and after index
            for field_name in field_lists:
                json_list = []
                for single in res:
                    json_list.append(single[f"{field_name}"])
                if len(json_list) != len(compare_dict[f'{i}'][f'{field_name}']):
                    log.debug(f"the field {field_name} value after index under expression '{express_list[i]}' is: {json_list}")
                    log.debug(f"the field {field_name} value before index to be compared under expression '{express_list[i]}' is: {compare_dict[f'{i}'][f'{field_name}']}")
                assert json_list == compare_dict[f'{i}'][f'{field_name}']
            # compare id before and after index
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
            if len(id_list) != len(compare_dict[f'{i}']['id_list']):
                log.debug(f"primary key field {default_primary_key_field_name} after index under expression '{express_list[i]}' is: {id_list}")
                log.debug(f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is: {compare_dict[f'{i}']['id_list']}")
            assert id_list == compare_dict[f'{i}']['id_list']
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)