import json
import struct
import time

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from pymilvus import DataType
from utils.util_log import test_log as log

prefix = "milvus_client_api_query"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = 'varchar >= "0"'
default_search_mix_exp = 'int64 >= 0 && varchar >= "0"'
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = 'json_field["number"] >= 0'
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
    """Test case of data integrity interface"""

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
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array(
        self,
        enable_dynamic_field,
        supported_numeric_scalar_index,
        supported_json_path_index,
        supported_array_double_float_scalar_index,
        is_flush,
        is_release,
        single_data_num,
    ):
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
            schema.add_field(
                ct.default_int8_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT8,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int16_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT16,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int32_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT32,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int64_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT64,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_bool_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.BOOL,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_float_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.FLOAT,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_double_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.DOUBLE,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_string_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.VARCHAR,
                max_capacity=5,
                max_length=100,
                nullable=True,
            )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [
                {
                    default_primary_key_field_name: j,
                    default_vector_field_name: vectors[j],
                    ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                    ct.default_int8_field_name: j % 128 if (i % 2 == 0) else None,
                    ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                    ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                    ct.default_int64_field_name: j if (i % 2 == 0) else None,
                    ct.default_float_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_double_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_string_field_name: f"{j}" if (i % 2 == 0) else None,
                    ct.default_json_field_name: inserted_data_distribution[i],
                    ct.default_int8_array_field_name: [j % 128, j % 128] if (i % 2 == 0) else None,
                    ct.default_int16_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int32_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int64_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_bool_array_field_name: [bool(j), bool(j + 1)] if (i % 2 == 0) else None,
                    ct.default_float_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_double_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_string_array_field_name: [f"{j}", f"{j + 1}"] if (i % 2 == 0) else None,
                }
                for j in range(i * nb_single, (i + 1) * nb_single)
            ]
            assert len(rows) == nb_single
            # log.info(rows)
            self.insert(client, collection_name=collection_name, data=rows)
            log.info(f"inserted {nb_single} {inserted_data_distribution[i]}")
        # 3. flush if specified
        if is_flush:
            self.flush(client, collection_name)
        # 4. query when there is no index under all expressions
        expr_fields = [ct.default_int64_field_name, ct.default_string_field_name, ct.default_float_array_field_name]
        compare_dict_by_field = {}
        for expr_field in expr_fields:
            express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
            compare_dict = {}
            for i in range(len(express_list)):
                json_list = []
                id_list = []
                log.info(f"query field '{expr_field}' with filter '{express_list[i]}' before scalar index")
                res = self.query(
                    client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
                )[0]
                count = res[0]["count(*)"]
                # log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
                res = self.query(
                    client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
                )[0]
                for single in res:
                    id_list.append(single[f"{default_primary_key_field_name}"])
                    json_list.append(single[f"{expr_field}"])
                assert count == len(id_list)
                assert count == len(json_list)
                compare_dict.setdefault(f"{i}", {})
                compare_dict[f"{i}"]["id_list"] = id_list
                compare_dict[f"{i}"]["json_list"] = json_list
            compare_dict_by_field[expr_field] = compare_dict
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
        index_params.add_index(
            field_name=ct.default_float_array_field_name, index_type=supported_array_double_float_scalar_index
        )
        # index_params.add_index(field_name=ct.default_double_array_field_name,
        #                        index_type=supported_array_double_float_scalar_index)
        # index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [
            f"{ct.default_json_field_name}",
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
            f"{ct.default_json_field_name}['a']['c'][0]['d']",
        ]
        for i in range(len(json_path_list)):
            index_params.add_index(
                field_name=ct.default_json_field_name,
                index_name=json_index_name + f"{i}",
                index_type=supported_json_path_index,
                params={"json_cast_type": "DOUBLE", "json_path": json_path_list[i]},
            )
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
        for expr_field in expr_fields:
            express_list = cf.gen_field_expressions_all_single_operator_each_field(expr_field)
            compare_dict = compare_dict_by_field[expr_field]
            for i in range(len(express_list)):
                json_list = []
                id_list = []
                log.info(f"query field '{expr_field}' with filter '{express_list[i]}' after index")
                count = self.query(
                    client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
                )[0]
                # log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
                res = self.query(
                    client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
                )[0]
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
                assert json_list == compare_dict[f"{i}"]["json_list"]
                # if len(id_list) != len(compare_dict[f'{i}']["id_list"]):
                #     log.debug(
                #         f"primary key field {default_primary_key_field_name} after indexed under expression '{express_list[i]}' is:")
                #     log.debug(id_list)
                #     log.debug(
                #         f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:")
                #     log.debug(compare_dict[f'{i}']["id_list"])
                assert id_list == compare_dict[f"{i}"]["id_list"]
                log.info(f"PASS with field {expr_field} and expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize(
        "expr_field",
        [
            ct.default_int8_field_name,
            ct.default_int16_field_name,
            ct.default_int32_field_name,
            ct.default_int64_field_name,
            ct.default_float_field_name,
            ct.default_double_field_name,
            ct.default_string_field_name,
            ct.default_bool_field_name,
            ct.default_int8_array_field_name,
            ct.default_int16_array_field_name,
            ct.default_int32_array_field_name,
            ct.default_int64_array_field_name,
            ct.default_bool_array_field_name,
            ct.default_float_array_field_name,
            ct.default_double_array_field_name,
            ct.default_string_array_field_name,
        ],
    )
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_all(
        self,
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
        expr_field,
    ):
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
            schema.add_field(
                ct.default_int8_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT8,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int16_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT16,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int32_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT32,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int64_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT64,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_bool_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.BOOL,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_float_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.FLOAT,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_double_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.DOUBLE,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_string_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.VARCHAR,
                max_capacity=5,
                max_length=100,
                nullable=True,
            )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [
                {
                    default_primary_key_field_name: j,
                    default_vector_field_name: vectors[j],
                    ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                    ct.default_int8_field_name: j % 128 if (i % 2 == 0) else None,
                    ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                    ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                    ct.default_int64_field_name: j if (i % 2 == 0) else None,
                    ct.default_float_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_double_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_string_field_name: f"{j}" if (i % 2 == 0) else None,
                    ct.default_json_field_name: inserted_data_distribution[i],
                    ct.default_int8_array_field_name: [j % 128, j % 128] if (i % 2 == 0) else None,
                    ct.default_int16_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int32_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int64_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_bool_array_field_name: [bool(j), bool(j + 1)] if (i % 2 == 0) else None,
                    ct.default_float_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_double_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_string_array_field_name: [f"{j}", f"{j + 1}"] if (i % 2 == 0) else None,
                }
                for j in range(i * nb_single, (i + 1) * nb_single)
            ]
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
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            count = res[0]["count(*)"]
            # log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
            )[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f"{i}", {})
            compare_dict[f"{i}"]["id_list"] = id_list
            compare_dict[f"{i}"]["json_list"] = json_list
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
        index_params.add_index(
            field_name=ct.default_float_array_field_name, index_type=supported_array_double_float_scalar_index
        )
        index_params.add_index(
            field_name=ct.default_double_array_field_name, index_type=supported_array_double_float_scalar_index
        )
        index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [
            f"{ct.default_json_field_name}",
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
            f"{ct.default_json_field_name}['a']['c'][0]['d']",
        ]
        for i in range(len(json_path_list)):
            index_params.add_index(
                field_name=ct.default_json_field_name,
                index_name=json_index_name + f"{i}",
                index_type=supported_json_path_index,
                params={"json_cast_type": "DOUBLE", "json_path": json_path_list[i]},
            )
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
            count = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            # log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
            )[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            if len(json_list) != len(compare_dict[f"{i}"]["json_list"]):
                log.debug(
                    f"the field {expr_field} value after index {supported_array_scalar_index} under expression '{express_list[i]}' is:"
                )
                log.debug(json_list)
                log.debug(
                    f"the field {expr_field} value before index to be compared under expression '{express_list[i]}' is:"
                )
                log.debug(compare_dict[f"{i}"]["json_list"])
            assert json_list == compare_dict[f"{i}"]["json_list"]
            if len(id_list) != len(compare_dict[f"{i}"]["id_list"]):
                log.debug(
                    f"primary key field {default_primary_key_field_name} after index {supported_array_scalar_index} under expression '{express_list[i]}' is:"
                )
                log.debug(id_list)
                log.debug(
                    f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:"
                )
                log.debug(compare_dict[f"{i}"]["id_list"])
            assert id_list == compare_dict[f"{i}"]["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize(
        "expr_field",
        [
            ct.default_int8_field_name,
            ct.default_int16_field_name,
            ct.default_int32_field_name,
            ct.default_int64_field_name,
            ct.default_float_field_name,
            ct.default_double_field_name,
            ct.default_string_field_name,
            ct.default_bool_field_name,
            ct.default_int8_array_field_name,
            ct.default_int16_array_field_name,
            ct.default_int32_array_field_name,
            ct.default_int64_array_field_name,
            ct.default_bool_array_field_name,
            ct.default_float_array_field_name,
            ct.default_double_array_field_name,
            ct.default_string_array_field_name,
        ],
    )
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_auto_index(
        self, enable_dynamic_field, supported_json_path_index, is_flush, is_release, single_data_num, expr_field
    ):
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
            schema.add_field(
                ct.default_int8_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT8,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int16_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT16,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int32_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT32,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int64_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT64,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_bool_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.BOOL,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_float_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.FLOAT,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_double_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.DOUBLE,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_string_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.VARCHAR,
                max_capacity=5,
                max_length=100,
                nullable=True,
            )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [
                {
                    default_primary_key_field_name: j,
                    default_vector_field_name: vectors[j],
                    ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                    ct.default_int8_field_name: j % 128 if (i % 2 == 0) else None,
                    ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                    ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                    ct.default_int64_field_name: j if (i % 2 == 0) else None,
                    ct.default_float_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_double_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_string_field_name: f"{j}" if (i % 2 == 0) else None,
                    ct.default_json_field_name: inserted_data_distribution[i],
                    ct.default_int8_array_field_name: [j % 128, j % 128] if (i % 2 == 0) else None,
                    ct.default_int16_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int32_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int64_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_bool_array_field_name: [bool(j), bool(j + 1)] if (i % 2 == 0) else None,
                    ct.default_float_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_double_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_string_array_field_name: [f"{j}", f"{j + 1}"] if (i % 2 == 0) else None,
                }
                for j in range(i * nb_single, (i + 1) * nb_single)
            ]
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
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            count = res[0]["count(*)"]
            log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
            )[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            assert count == len(id_list)
            assert count == len(json_list)
            compare_dict.setdefault(f"{i}", {})
            compare_dict[f"{i}"]["id_list"] = id_list
            compare_dict[f"{i}"]["json_list"] = json_list
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
        json_path_list = [
            f"{ct.default_json_field_name}",
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
            f"{ct.default_json_field_name}['a']['c'][0]['d']",
        ]
        for i in range(len(json_path_list)):
            index_params.add_index(
                field_name=ct.default_json_field_name,
                index_name=json_index_name + f"{i}",
                index_type=supported_json_path_index,
                params={"json_cast_type": "DOUBLE", "json_path": json_path_list[i]},
            )
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
            count = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=[f"{expr_field}"]
            )[0]
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                json_list.append(single[f"{expr_field}"])
            if len(json_list) != len(compare_dict[f"{i}"]["json_list"]):
                log.debug(
                    f"the field {expr_field} value after index 'AUTOINDEX' under expression '{express_list[i]}' is:"
                )
                log.debug(json_list)
                log.debug(
                    f"the field {expr_field} value before index to be compared under expression '{express_list[i]}' is:"
                )
                log.debug(compare_dict[f"{i}"]["json_list"])
            assert json_list == compare_dict[f"{i}"]["json_list"]
            if len(id_list) != len(compare_dict[f"{i}"]["id_list"]):
                log.debug(
                    f"primary key field {default_primary_key_field_name} after index 'AUTOINDEX' under expression '{express_list[i]}' is:"
                )
                log.debug(id_list)
                log.debug(
                    f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is:"
                )
                log.debug(compare_dict[f"{i}"]["id_list"])
            assert id_list == compare_dict[f"{i}"]["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("enable_dynamic_field", [False])
    @pytest.mark.parametrize("is_flush", [True, False])
    @pytest.mark.parametrize("is_release", [True, False])
    @pytest.mark.parametrize("single_data_num", [50])
    @pytest.mark.parametrize("random_filter_field_number", [2, 6, 16])
    def test_milvus_client_query_all_field_type_all_data_distribution_all_expressions_array_multiple_fields(
        self,
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
        random_filter_field_number,
    ):
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
            schema.add_field(
                ct.default_int8_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT8,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int16_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT16,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int32_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT32,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_int64_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.INT64,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_bool_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.BOOL,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_float_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.FLOAT,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_double_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.DOUBLE,
                max_capacity=5,
                nullable=True,
            )
            schema.add_field(
                ct.default_string_array_field_name,
                datatype=DataType.ARRAY,
                element_type=DataType.VARCHAR,
                max_capacity=5,
                max_length=100,
                nullable=True,
            )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        # 2. insert with different data distribution
        vectors = cf.gen_vectors(default_nb + 60, default_dim)
        inserted_data_distribution = ct.get_all_kind_data_distribution
        nb_single = single_data_num
        for i in range(len(inserted_data_distribution)):
            rows = [
                {
                    default_primary_key_field_name: j,
                    default_vector_field_name: vectors[j],
                    ct.default_bool_field_name: bool(j) if (i % 2 == 0) else None,
                    ct.default_int8_field_name: j % 128 if (i % 2 == 0) else None,
                    ct.default_int16_field_name: np.int16(j) if (i % 2 == 0) else None,
                    ct.default_int32_field_name: np.int32(j) if (i % 2 == 0) else None,
                    ct.default_int64_field_name: j if (i % 2 == 0) else None,
                    ct.default_float_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_double_field_name: j * 1.0 if (i % 2 == 0) else None,
                    ct.default_string_field_name: f"{j}" if (i % 2 == 0) else None,
                    ct.default_json_field_name: inserted_data_distribution[i],
                    ct.default_int8_array_field_name: [j % 128, j % 128] if (i % 2 == 0) else None,
                    ct.default_int16_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int32_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_int64_array_field_name: [j, j + 1] if (i % 2 == 0) else None,
                    ct.default_bool_array_field_name: [bool(j), bool(j + 1)] if (i % 2 == 0) else None,
                    ct.default_float_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_double_array_field_name: [j * 1.0, (j + 1) * 1.0] if (i % 2 == 0) else None,
                    ct.default_string_array_field_name: [f"{j}", f"{j + 1}"] if (i % 2 == 0) else None,
                }
                for j in range(i * nb_single, (i + 1) * nb_single)
            ]
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
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            count = res[0]["count(*)"]
            log.info(f"The count(*) after query with filter '{express_list[i]}' before scalar index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=field_lists
            )[0]
            # compare_dict.setdefault(f'{i}', {})
            one_dict = {}
            # init the compared dict
            for field_name in field_lists:
                one_dict.setdefault(f"{field_name}", [])
                compare_dict.setdefault(f"{i}", one_dict)
            # extract and store the id and output_fields value used for compare after index
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
                for field_name in field_lists:
                    compare_dict[f"{i}"][f"{field_name}"].append(single[f"{field_name}"])
            assert count == len(id_list)
            for field_name in field_lists:
                assert count == len(compare_dict[f"{i}"][f"{field_name}"])
            compare_dict[f"{i}"]["id_list"] = id_list
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
        index_params.add_index(
            field_name=ct.default_float_array_field_name, index_type=supported_array_double_float_scalar_index
        )
        index_params.add_index(
            field_name=ct.default_double_array_field_name, index_type=supported_array_double_float_scalar_index
        )
        index_params.add_index(field_name=ct.default_string_array_field_name, index_type=supported_array_scalar_index)
        json_index_name = "json_index_name"
        json_path_list = [
            f"{ct.default_json_field_name}",
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
            f"{ct.default_json_field_name}['a']['c'][0]['d']",
        ]
        for i in range(len(json_path_list)):
            index_params.add_index(
                field_name=ct.default_json_field_name,
                index_name=json_index_name + f"{i}",
                index_type=supported_json_path_index,
                params={"json_cast_type": "DOUBLE", "json_path": json_path_list[i]},
            )
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
            count = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=["count(*)"]
            )[0]
            log.info(f"The count(*) after query with filter '{express_list[i]}' after index is: {count}")
            res = self.query(
                client, collection_name=collection_name, filter=express_list[i], output_fields=field_lists
            )[0]
            # compare each filtered field before and after index
            for field_name in field_lists:
                json_list = []
                for single in res:
                    json_list.append(single[f"{field_name}"])
                if len(json_list) != len(compare_dict[f"{i}"][f"{field_name}"]):
                    log.debug(
                        f"the field {field_name} value after index under expression '{express_list[i]}' is: {json_list}"
                    )
                    log.debug(
                        f"the field {field_name} value before index to be compared under expression '{express_list[i]}' is: {compare_dict[f'{i}'][f'{field_name}']}"
                    )
                assert json_list == compare_dict[f"{i}"][f"{field_name}"]
            # compare id before and after index
            for single in res:
                id_list.append(single[f"{default_primary_key_field_name}"])
            if len(id_list) != len(compare_dict[f"{i}"]["id_list"]):
                log.debug(
                    f"primary key field {default_primary_key_field_name} after index under expression '{express_list[i]}' is: {id_list}"
                )
                log.debug(
                    f"primary key field {default_primary_key_field_name} before index to be compared under expression '{express_list[i]}' is: {compare_dict[f'{i}']['id_list']}"
                )
            assert id_list == compare_dict[f"{i}"]["id_list"]
            log.info(f"PASS with expression {express_list[i]}")
        self.drop_collection(client, collection_name)


COMPACTION_INTEGRITY_ROWS_PER_SEGMENT = 1000
COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND = 10
COMPACTION_INTEGRITY_ROUNDS = 3
COMPACTION_INTEGRITY_VECTOR_DIM = 128
COMPACTION_INTEGRITY_DEFAULT_VALUE = -91919530
COMPACTION_INTEGRITY_OUTPUT_FIELDS = [
    "id",
    "explicit_test_ts",
    "bool_value",
    "int64_value",
    "float_value",
    "double_value",
    "default_value",
    "varchar_payload",
    "json_payload",
    "int64_array",
    "float_array",
    "string_array",
    "vector",
    "dynamic_payload",
]
COMPACTION_INTEGRITY_ACTIVE_STATES = {"Growing", "Sealed", "Flushing", "Flushed", "Importing"}
COMPACTION_INTEGRITY_STABLE_STATES = {"Sealed", "Flushed"}
COMPACTION_INTEGRITY_RUNNING_TASK_STATES = {
    "executing",
    "pipelining",
    "analyzing",
    "indexing",
    "meta_saved",
    "statistic",
}
COMPACTION_INTEGRITY_SUCCESS_TASK_STATES = {"completed", "cleaned"}
COMPACTION_INTEGRITY_FAILED_TASK_STATES = {"failed", "timeout"}


def _compaction_integrity_signature(run_id, pk, explicit_test_ts, field_id):
    return f"run={run_id}|pk={pk}|ts={explicit_test_ts}|field={field_id}"


def _build_compaction_integrity_row(run_id, pk, explicit_test_ts):
    varchar_signature = _compaction_integrity_signature(run_id, pk, explicit_test_ts, 7)
    dynamic_signature = _compaction_integrity_signature(run_id, pk, explicit_test_ts, 13)
    vector = [
        (((pk % 997) + explicit_test_ts * 13 + index * 17) % 2048 - 1024) / 1024.0
        for index in range(COMPACTION_INTEGRITY_VECTOR_DIM)
    ]
    row = {
        "id": pk,
        "explicit_test_ts": explicit_test_ts,
        "bool_value": None if pk % 11 == 0 else bool(pk % 2),
        "int64_value": pk * 17 - explicit_test_ts,
        "float_value": None if pk % 13 == 0 else ((pk % 1024) - 512) / 8.0,
        "double_value": (pk * 31 + explicit_test_ts) / 16.0,
        "varchar_payload": f"{varchar_signature}|{varchar_signature[::-1]}|{'x' * (pk % 37)}",
        "json_payload": (
            None
            if pk % 7 == 0
            else {
                "pk": pk,
                "ts": explicit_test_ts,
                "field": 8,
                "nested": [pk % 19, bool(pk % 2), None, varchar_signature],
            }
        ),
        "int64_array": None if pk % 5 == 0 else [pk, explicit_test_ts, 9, pk ^ explicit_test_ts],
        "float_array": [((pk + offset * 5) % 256 - 128) / 16.0 for offset in range(4)],
        "string_array": [
            _compaction_integrity_signature(run_id, pk, explicit_test_ts, 11) + f"|element={index}"
            for index in range(3)
        ],
        "vector": vector,
        "dynamic_payload": {
            "signature": dynamic_signature,
            "pk": pk,
            "ts": explicit_test_ts,
            "field": 13,
        },
    }
    expected = dict(row)
    if pk % 4 == 0:
        expected["default_value"] = COMPACTION_INTEGRITY_DEFAULT_VALUE
    else:
        row["default_value"] = pk + 1000000
        expected["default_value"] = row["default_value"]
    return row, expected


def _length_prefixed_bytes(values):
    encoded = bytearray()
    for value in values:
        encoded.extend(struct.pack("<I", len(value)))
        encoded.extend(value)
    return bytes(encoded)


def _canonical_compaction_integrity_cell(field_name, value):
    if value is None:
        return b"\x00"
    if field_name in {"id", "explicit_test_ts", "int64_value", "default_value"}:
        encoded = struct.pack("<q", int(value))
    elif field_name == "bool_value":
        encoded = struct.pack("<?", bool(value))
    elif field_name == "float_value":
        encoded = struct.pack("<f", float(value))
    elif field_name == "double_value":
        encoded = struct.pack("<d", float(value))
    elif field_name == "varchar_payload":
        encoded = value.encode("utf-8")
    elif field_name in {"json_payload", "dynamic_payload"}:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    elif field_name == "int64_array":
        encoded = struct.pack(f"<{len(value)}q", *(int(item) for item in value))
    elif field_name == "float_array":
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    elif field_name == "string_array":
        encoded = _length_prefixed_bytes([item.encode("utf-8") for item in value])
    elif field_name == "vector":
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    else:
        raise AssertionError(f"no canonical encoder for field {field_name}")
    return b"\x01" + struct.pack("<I", len(encoded)) + encoded


def _snapshot_compaction_integrity_segments(segments):
    return {
        segment.segment_id: {
            "segment_id": segment.segment_id,
            "state": segment.state_name,
            "num_rows": segment.num_rows,
            "is_sorted": segment.is_sorted,
            "storage_version": segment.storage_version,
            "partition_id": segment.partition_id,
            "insert_channel": segment.insert_channel,
            "compaction_from": tuple(segment.compaction_from),
        }
        for segment in segments
    }


def _snapshot_compaction_integrity_tasks(tasks):
    return {
        task.task_id: {
            "task_id": task.task_id,
            "trigger_id": task.trigger_id,
            "type": task.compaction_type,
            "state": task.state.lower(),
            "failure_reason": task.failure_reason,
            "sources": tuple(task.sources),
            "targets": tuple(task.targets),
        }
        for task in tasks.plans
    }


def _assert_no_compaction_integrity_task_failure(task_snapshot):
    failed = [
        task
        for task in task_snapshot.values()
        if task["failure_reason"] or task["state"] in COMPACTION_INTEGRITY_FAILED_TASK_STATES
    ]
    assert not failed, f"compaction tasks failed: {failed}"


def _wait_for_compaction_integrity_job(client, collection_name, job_id, timeout=600):
    deadline = time.time() + timeout
    last_job_tasks = {}
    while time.time() < deadline:
        collection_tasks = _snapshot_compaction_integrity_tasks(client.list_compaction_tasks(collection_name))
        _assert_no_compaction_integrity_task_failure(collection_tasks)
        job_tasks = _snapshot_compaction_integrity_tasks(client.get_compaction_plans(job_id))
        _assert_no_compaction_integrity_task_failure(job_tasks)
        last_job_tasks = job_tasks
        if job_tasks and all(
            task["trigger_id"] == job_id
            and task["state"] in COMPACTION_INTEGRITY_SUCCESS_TASK_STATES
            and task["targets"]
            for task in job_tasks.values()
        ):
            return job_tasks
        time.sleep(2)
    raise AssertionError(f"compaction job {job_id} did not complete with persisted targets: {last_job_tasks}")


def _wait_for_compaction_integrity_checkpoint(client, collection_name, expected_rows, timeout=600):
    deadline = time.time() + timeout
    stable_polls = 0
    last_signature = None
    last_observation = {}
    while time.time() < deadline:
        all_segments = _snapshot_compaction_integrity_segments(client.list_segments(collection_name))
        active_segments = {
            segment_id: segment
            for segment_id, segment in all_segments.items()
            if segment["state"] in COMPACTION_INTEGRITY_ACTIVE_STATES and segment["num_rows"] != 0
        }
        serving_segments = {
            segment_id: segment
            for segment_id, segment in _snapshot_compaction_integrity_segments(
                client.list_serving_segments(collection_name)
            ).items()
            if segment["num_rows"] != 0
        }
        tasks = _snapshot_compaction_integrity_tasks(client.list_compaction_tasks(collection_name))
        _assert_no_compaction_integrity_task_failure(tasks)
        running_tasks = [task for task in tasks.values() if task["state"] in COMPACTION_INTEGRITY_RUNNING_TASK_STATES]
        storage_versions = {segment["storage_version"] for segment in active_segments.values()}
        ready = (
            bool(active_segments)
            and not running_tasks
            and all(
                segment["state"] in COMPACTION_INTEGRITY_STABLE_STATES and segment["is_sorted"]
                for segment in active_segments.values()
            )
            and set(serving_segments) == set(active_segments)
            and all(segment["state"] == "Sealed" for segment in serving_segments.values())
            and sum(segment["num_rows"] for segment in active_segments.values()) == expected_rows
            and len(storage_versions) == 1
            and storage_versions.issubset({2, 3})
        )
        signature = (
            tuple(sorted((segment_id, tuple(sorted(segment.items()))) for segment_id, segment in all_segments.items())),
            tuple(sorted(serving_segments)),
            tuple(sorted((task_id, tuple(sorted(task.items()))) for task_id, task in tasks.items())),
        )
        last_observation = {
            "all": all_segments,
            "active": active_segments,
            "serving": serving_segments,
            "tasks": tasks,
            "storage_versions": storage_versions,
        }
        if ready and signature == last_signature:
            stable_polls += 1
        elif ready:
            stable_polls = 1
        else:
            stable_polls = 0
        if stable_polls >= 3:
            return last_observation
        last_signature = signature
        time.sleep(2)
    raise AssertionError(f"collection did not reach a stable compaction checkpoint: {last_observation}")


def _assert_compaction_integrity_dataset(client, collection_name, expected_by_pk):
    iterator = client.query_iterator(
        collection_name,
        batch_size=2000,
        filter="id >= 0",
        output_fields=COMPACTION_INTEGRITY_OUTPUT_FIELDS,
        consistency_level="Strong",
    )
    actual_rows = []
    try:
        while True:
            batch = iterator.next()
            if not batch:
                break
            actual_rows.extend(batch)
    finally:
        iterator.close()

    actual_by_pk = {row["id"]: row for row in actual_rows}
    assert len(actual_rows) == len(expected_by_pk), (
        f"row count mismatch: actual={len(actual_rows)}, expected={len(expected_by_pk)}"
    )
    assert len(actual_by_pk) == len(actual_rows), "retrieve returned duplicate primary keys"
    assert set(actual_by_pk) == set(expected_by_pk), (
        f"primary key set mismatch: missing={set(expected_by_pk) - set(actual_by_pk)}, "
        f"unexpected={set(actual_by_pk) - set(expected_by_pk)}"
    )
    for pk, expected in expected_by_pk.items():
        actual = actual_by_pk[pk]
        assert set(actual) == set(expected), (
            f"field set mismatch for pk={pk}: actual={set(actual)}, expected={set(expected)}"
        )
        for field_name in COMPACTION_INTEGRITY_OUTPUT_FIELDS:
            actual_bytes = _canonical_compaction_integrity_cell(field_name, actual[field_name])
            expected_bytes = _canonical_compaction_integrity_cell(field_name, expected[field_name])
            assert actual_bytes == expected_bytes, (
                f"canonical value mismatch for pk={pk}, field={field_name}, "
                f"actual_len={len(actual_bytes)}, expected_len={len(expected_bytes)}, "
                f"actual_prefix={actual_bytes[:64].hex()}, expected_prefix={expected_bytes[:64].hex()}"
            )


def _assert_compaction_integrity_transition(before_active, after_checkpoint, job_tasks):
    after_active = after_checkpoint["active"]
    all_after = after_checkpoint["all"]
    source_ids = {source for task in job_tasks.values() for source in task["sources"]}
    target_ids = {target for task in job_tasks.values() for target in task["targets"]}
    assert source_ids and source_ids.issubset(before_active), (
        f"compaction sources are not a non-empty subset of the prior active set: sources={source_ids}, "
        f"active={set(before_active)}"
    )
    assert target_ids and source_ids.isdisjoint(target_ids), (
        f"compaction targets are empty or overlap sources: sources={source_ids}, targets={target_ids}"
    )
    expected_active_ids = (set(before_active) - source_ids) | target_ids
    assert set(after_active) == expected_active_ids, (
        f"active-set transition mismatch: actual={set(after_active)}, expected={expected_active_ids}"
    )
    assert all(all_after[source]["state"] == "Dropped" for source in source_ids)
    for task in job_tasks.values():
        assert task["type"] == "MixCompaction", f"manual job produced unexpected task type: {task}"
        for target in task["targets"]:
            assert target in after_active
            assert set(all_after[target]["compaction_from"]) == set(task["sources"]), (
                f"target lineage mismatch for target={target}: "
                f"actual={all_after[target]['compaction_from']}, expected={task['sources']}"
            )
    source_rows = sum(before_active[source]["num_rows"] for source in source_ids)
    target_rows = sum(after_active[target]["num_rows"] for target in target_ids)
    assert target_rows == source_rows, (
        f"physical row count changed across compaction: sources={source_rows}, targets={target_rows}"
    )


@pytest.mark.xdist_group("TestMilvusClientCompactionDataIntegrity")
class TestMilvusClientCompactionDataIntegrity(TestMilvusClientV2Base):
    """Compaction lifecycle data-integrity tests with isolated mutable collections."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_compaction_active_set_transitions_preserve_all_rows_and_fields(self):
        """
        target: verify full-row data safety across repeated sort and manual compaction transitions
        method: append ten flushed segments per round, observe sort lineage, compact, verify handoff and exact cells
        expected: every transition persists new targets, serves only the active set, and preserves the full dataset
        """
        client = self._client()
        collection_name = cf.gen_unique_str("compaction_data_integrity")
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=True)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("explicit_test_ts", DataType.INT64)
        schema.add_field("bool_value", DataType.BOOL, nullable=True)
        schema.add_field("int64_value", DataType.INT64)
        schema.add_field("float_value", DataType.FLOAT, nullable=True)
        schema.add_field("double_value", DataType.DOUBLE)
        schema.add_field(
            "default_value",
            DataType.INT64,
            nullable=True,
            default_value=COMPACTION_INTEGRITY_DEFAULT_VALUE,
        )
        schema.add_field("varchar_payload", DataType.VARCHAR, max_length=512)
        schema.add_field("json_payload", DataType.JSON, nullable=True)
        schema.add_field(
            "int64_array",
            DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=4,
            nullable=True,
        )
        schema.add_field(
            "float_array",
            DataType.ARRAY,
            element_type=DataType.FLOAT,
            max_capacity=4,
        )
        schema.add_field(
            "string_array",
            DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_capacity=3,
            max_length=256,
        )
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("vector", index_type="AUTOINDEX", metric_type="COSINE")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
            num_shards=1,
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"collection.autocompaction.enabled": "false"},
        )

        expected_by_pk = {}
        lineage_history = {}
        known_segment_ids = set()
        known_task_ids = set()
        previous_active = {}
        observed_storage_version = None

        for round_index in range(COMPACTION_INTEGRITY_ROUNDS):
            round_leaf_ids = set()
            for segment_index in range(COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND):
                explicit_test_ts = round_index * COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND + segment_index + 1
                pk_start = explicit_test_ts * COMPACTION_INTEGRITY_ROWS_PER_SEGMENT
                rows = []
                expected_rows = {}
                for pk in range(pk_start, pk_start + COMPACTION_INTEGRITY_ROWS_PER_SEGMENT):
                    row, expected = _build_compaction_integrity_row(collection_name, pk, explicit_test_ts)
                    rows.append(row)
                    expected_rows[pk] = expected
                insert_result = self.insert(client, collection_name, rows)[0]
                assert insert_result["insert_count"] == COMPACTION_INTEGRITY_ROWS_PER_SEGMENT
                expected_by_pk.update(expected_rows)
                self.flush(client, collection_name)
                immediate_segments = _snapshot_compaction_integrity_segments(client.list_segments(collection_name))
                lineage_history.update(immediate_segments)
                new_leaves = {
                    segment_id
                    for segment_id, segment in immediate_segments.items()
                    if segment_id not in known_segment_ids
                    and segment["num_rows"] != 0
                    and not segment["compaction_from"]
                }
                round_leaf_ids.update(new_leaves)

            sorted_checkpoint = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=len(expected_by_pk),
            )
            lineage_history.update(sorted_checkpoint["all"])
            round_new_segments = set(lineage_history) - known_segment_ids
            round_leaf_ids.update(
                segment_id
                for segment_id in round_new_segments
                if lineage_history[segment_id]["num_rows"] != 0 and not lineage_history[segment_id]["compaction_from"]
            )
            round_tasks = {
                task_id: task for task_id, task in sorted_checkpoint["tasks"].items() if task_id not in known_task_ids
            }
            sort_tasks = {task_id: task for task_id, task in round_tasks.items() if task["type"] == "SortCompaction"}
            sort_sources = {source for task in sort_tasks.values() for source in task["sources"]}
            sort_targets = {target for task in sort_tasks.values() for target in task["targets"]}
            assert len(round_leaf_ids) == COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND, (
                f"expected one flushed G0 leaf per batch, got {round_leaf_ids}"
            )
            assert round_leaf_ids == sort_sources, (
                f"not every flushed G0 leaf was consumed by sort compaction: "
                f"leaves={round_leaf_ids}, sources={sort_sources}"
            )
            assert set(sorted_checkpoint["active"]) == set(previous_active) | sort_targets, (
                f"sort active-set transition mismatch: prior={set(previous_active)}, "
                f"targets={sort_targets}, actual={set(sorted_checkpoint['active'])}"
            )
            for task in sort_tasks.values():
                assert task["state"] in COMPACTION_INTEGRITY_SUCCESS_TASK_STATES and task["targets"]
                for target in task["targets"]:
                    assert set(lineage_history[target]["compaction_from"]) == set(task["sources"])

            storage_version = next(iter(sorted_checkpoint["storage_versions"]))
            if observed_storage_version is None:
                observed_storage_version = storage_version
            assert storage_version == observed_storage_version
            _assert_compaction_integrity_dataset(client, collection_name, expected_by_pk)

            before_manual_active = sorted_checkpoint["active"]
            compact_id = self.compact(client, collection_name)[0]
            assert compact_id > 0, "manual compaction did not create a job"
            manual_job_tasks = _wait_for_compaction_integrity_job(client, collection_name, compact_id)
            compacted_checkpoint = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=len(expected_by_pk),
            )
            lineage_history.update(compacted_checkpoint["all"])
            _assert_compaction_integrity_transition(
                before_manual_active,
                compacted_checkpoint,
                manual_job_tasks,
            )
            assert compacted_checkpoint["storage_versions"] == {observed_storage_version}
            _assert_compaction_integrity_dataset(client, collection_name, expected_by_pk)

            previous_active = compacted_checkpoint["active"]
            known_segment_ids.update(lineage_history)
            known_task_ids.update(compacted_checkpoint["tasks"])
            log.info(
                f"round={round_index + 1} rows={len(expected_by_pk)} storage_version={observed_storage_version} "
                f"active_segments={sorted(previous_active)} lineage_segments={len(lineage_history)}"
            )
