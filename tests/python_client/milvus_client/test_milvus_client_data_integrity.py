import json
import os
import struct
import time

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from pymilvus import DataType
from utils.etcd_config import MilvusEtcdConfigController
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
COMPACTION_INTEGRITY_ROUND_COUNTS = [int(os.getenv("MILVUS_COMPACTION_INTEGRITY_ROUNDS", "3"))]
COMPACTION_INTEGRITY_VECTOR_DIM = 128
COMPACTION_INTEGRITY_DEFAULT_VALUE = -91919530
COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT = 20
COMPACTION_INTEGRITY_TEXT_INLINE_THRESHOLD = int(os.getenv("MILVUS_TEXT_INLINE_THRESHOLD", "65536"))
COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG = "common.storage.useLoonFFI"
COMPACTION_INTEGRITY_RUN_STORAGE_TRANSITION = (
    os.getenv("MILVUS_RUN_STORAGE_VERSION_TRANSITION_E2E", "false").lower() == "true"
)
COMPACTION_INTEGRITY_BASE_OUTPUT_FIELDS = [
    "id",
    "explicit_test_ts",
    "bool_value",
    "int8_value",
    "int16_value",
    "int32_value",
    "int64_value",
    "float_value",
    "double_value",
    "default_value",
    "varchar_payload",
    "json_payload",
    "int64_array",
    "float_array",
    "string_array",
    "float_vector",
    "binary_vector",
    "float16_vector",
    "bfloat16_vector",
    "sparse_vector",
    "int8_vector",
    "nullable_float_vector",
    "nullable_binary_vector",
    "nullable_sparse_vector",
    "nullable_int8_vector",
    "dynamic_payload",
]
COMPACTION_INTEGRITY_TEXT_FIELD = "text_payload"


def _log_compaction_integrity_evidence(event, **evidence):
    log.info(
        f"compaction_integrity_evidence {json.dumps({'event': event, **evidence}, sort_keys=True)}",
        extra={"persist_on_pass": True},
    )


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
COMPACTION_INTEGRITY_TERMINAL_TASK_STATES = (
    COMPACTION_INTEGRITY_SUCCESS_TASK_STATES | COMPACTION_INTEGRITY_FAILED_TASK_STATES
)
COMPACTION_INTEGRITY_KNOWN_TASK_STATES = (
    COMPACTION_INTEGRITY_RUNNING_TASK_STATES | COMPACTION_INTEGRITY_TERMINAL_TASK_STATES
)


def _compaction_integrity_signature(run_id, pk, explicit_test_ts, field_id):
    return f"run={run_id}|pk={pk}|ts={explicit_test_ts}|field={field_id}"


def _compaction_integrity_physical_pk(logical_pk, primary_key_type):
    if primary_key_type == DataType.INT64:
        return logical_pk
    return f"pk-{logical_pk:020d}"


def _compaction_integrity_float32(logical_pk, explicit_test_ts, field_id, element_index=0):
    # The largest generated integer is below 2**24, so FLOAT stores it exactly and without collisions.
    return float(logical_pk * 512 + explicit_test_ts * 8 + field_id + element_index)


def _compaction_integrity_float64(logical_pk, explicit_test_ts, field_id):
    # Integer and the dyadic division are both exact in IEEE-754 binary64 for this test's key range.
    return float(logical_pk * (1 << 20) + explicit_test_ts * 1024 + field_id) / 8.0


def _compaction_integrity_signed_integer(logical_pk, explicit_test_ts, field_id, bit_width):
    value = (logical_pk * 2654435761 + explicit_test_ts * 2246822519 + field_id) & ((1 << bit_width) - 1)
    sign_bit = 1 << (bit_width - 1)
    return value - (1 << bit_width) if value & sign_bit else value


def _compaction_integrity_is_null(logical_pk, field_id):
    # Every consecutive 1,000-row segment contains exactly 5% nulls per nullable field.
    return (logical_pk + field_id) % 20 == 0


def _compaction_integrity_float16_vector(logical_pk, explicit_test_ts, field_id):
    words = []
    for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM):
        if element_index == 0:
            mantissa = logical_pk & 0x03FF
        elif element_index == 1:
            mantissa = (logical_pk >> 10) & 0x03FF
        elif element_index == 2:
            mantissa = explicit_test_ts & 0x03FF
        elif element_index == 3:
            mantissa = field_id & 0x03FF
        else:
            mantissa = (logical_pk * 17 + explicit_test_ts * 13 + field_id * 7 + element_index) & 0x03FF
        words.append(0x3C00 | mantissa)
    return struct.pack(f"<{len(words)}H", *words)


def _compaction_integrity_bfloat16_vector(logical_pk, explicit_test_ts, field_id):
    words = []
    for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM):
        if element_index < 3:
            mantissa = (logical_pk >> (element_index * 7)) & 0x007F
        elif element_index == 3:
            mantissa = explicit_test_ts & 0x007F
        elif element_index == 4:
            mantissa = field_id & 0x007F
        else:
            mantissa = (logical_pk * 29 + explicit_test_ts * 11 + field_id * 5 + element_index) & 0x007F
        words.append(0x3F80 | mantissa)
    return struct.pack(f"<{len(words)}H", *words)


def _compaction_integrity_int8_vector(logical_pk, explicit_test_ts, field_id):
    values = [
        logical_pk & 0xFF,
        (logical_pk >> 8) & 0xFF,
        (logical_pk >> 16) & 0xFF,
        explicit_test_ts & 0xFF,
        field_id & 0xFF,
    ]
    values.extend(
        (logical_pk * 31 + explicit_test_ts * 17 + field_id * 13 + element_index) & 0xFF
        for element_index in range(5, COMPACTION_INTEGRITY_VECTOR_DIM)
    )
    return bytes(values)


def _compaction_integrity_text(signature, logical_pk):
    if logical_pk % COMPACTION_INTEGRITY_ROWS_PER_SEGMENT != 0:
        return f"{signature}|inline"
    target_length = COMPACTION_INTEGRITY_TEXT_INLINE_THRESHOLD + 257
    assert target_length > len(signature) + 1
    return f"{signature}|" + "L" * (target_length - len(signature) - 1)


def _build_compaction_integrity_row(run_id, logical_pk, explicit_test_ts, primary_key_type, include_text):
    physical_pk = _compaction_integrity_physical_pk(logical_pk, primary_key_type)
    varchar_signature = _compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, 7)
    dynamic_signature = _compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, 19)
    row = {
        "id": physical_pk,
        "explicit_test_ts": explicit_test_ts,
        "bool_value": None if _compaction_integrity_is_null(logical_pk, 3) else bool(logical_pk % 2),
        "int8_value": _compaction_integrity_signed_integer(logical_pk, explicit_test_ts, 20, 8),
        "int16_value": _compaction_integrity_signed_integer(logical_pk, explicit_test_ts, 21, 16),
        "int32_value": _compaction_integrity_signed_integer(logical_pk, explicit_test_ts, 22, 32),
        "int64_value": logical_pk * 17 - explicit_test_ts,
        "float_value": (
            None
            if _compaction_integrity_is_null(logical_pk, 4)
            else _compaction_integrity_float32(logical_pk, explicit_test_ts, 4)
        ),
        "double_value": _compaction_integrity_float64(logical_pk, explicit_test_ts, 5),
        "varchar_payload": f"{varchar_signature}|{varchar_signature[::-1]}|{'x' * (logical_pk % 37)}",
        "json_payload": (
            None
            if _compaction_integrity_is_null(logical_pk, 8)
            else {
                "pk": logical_pk,
                "ts": explicit_test_ts,
                "field": 8,
                "nested": [logical_pk % 19, bool(logical_pk % 2), None, varchar_signature],
            }
        ),
        "int64_array": (
            None
            if _compaction_integrity_is_null(logical_pk, 9)
            else [logical_pk, explicit_test_ts, 9, logical_pk ^ explicit_test_ts]
        ),
        "float_array": [
            _compaction_integrity_float32(logical_pk, explicit_test_ts, 10, element_index) for element_index in range(4)
        ],
        "string_array": [
            _compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, 11) + f"|element={element_index}"
            for element_index in range(3)
        ],
        "float_vector": [
            _compaction_integrity_float32(logical_pk, explicit_test_ts, 12, element_index)
            for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM)
        ],
        "binary_vector": struct.pack("<QII", logical_pk, explicit_test_ts, 13),
        "float16_vector": _compaction_integrity_float16_vector(logical_pk, explicit_test_ts, 14),
        "bfloat16_vector": _compaction_integrity_bfloat16_vector(logical_pk, explicit_test_ts, 15),
        "sparse_vector": {
            element_index: _compaction_integrity_float32(logical_pk, explicit_test_ts, 16, element_index)
            for element_index in (0, 31, 63, 127)
        },
        "int8_vector": _compaction_integrity_int8_vector(logical_pk, explicit_test_ts, 17),
        "nullable_float_vector": (
            None
            if _compaction_integrity_is_null(logical_pk, 23)
            else [
                _compaction_integrity_float32(logical_pk, explicit_test_ts, 23, element_index)
                for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM)
            ]
        ),
        "nullable_binary_vector": (
            None
            if _compaction_integrity_is_null(logical_pk, 24)
            else struct.pack("<QII", logical_pk, explicit_test_ts, 24)
        ),
        "nullable_sparse_vector": (
            None
            if _compaction_integrity_is_null(logical_pk, 25)
            else {
                element_index: _compaction_integrity_float32(logical_pk, explicit_test_ts, 25, element_index)
                for element_index in (0, 31, 63, 127)
            }
        ),
        "nullable_int8_vector": (
            None
            if _compaction_integrity_is_null(logical_pk, 26)
            else _compaction_integrity_int8_vector(logical_pk, explicit_test_ts, 26)
        ),
        "dynamic_payload": {
            "signature": dynamic_signature,
            "pk": logical_pk,
            "ts": explicit_test_ts,
            "field": 19,
        },
    }
    expected = dict(row)
    default_pattern = logical_pk % 20
    if default_pattern in {0, 1}:
        expected["default_value"] = COMPACTION_INTEGRITY_DEFAULT_VALUE
        if default_pattern == 1:
            row["default_value"] = None
    else:
        row["default_value"] = logical_pk + 1000000
        expected["default_value"] = row["default_value"]
    if include_text:
        text_signature = _compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, 18)
        row[COMPACTION_INTEGRITY_TEXT_FIELD] = _compaction_integrity_text(text_signature, logical_pk)
        expected[COMPACTION_INTEGRITY_TEXT_FIELD] = row[COMPACTION_INTEGRITY_TEXT_FIELD]
    return row, expected


def _length_prefixed_bytes(values):
    encoded = bytearray()
    for value in values:
        encoded.extend(struct.pack("<I", len(value)))
        encoded.extend(value)
    return bytes(encoded)


def _canonical_compaction_integrity_cell(field_name, value, primary_key_type):
    if value is None:
        return b"\x00"
    if field_name == "id" and primary_key_type == DataType.VARCHAR:
        encoded = value.encode("utf-8")
    elif field_name in {"id", "explicit_test_ts", "int64_value", "default_value"}:
        encoded = struct.pack("<q", int(value))
    elif field_name == "bool_value":
        encoded = struct.pack("<?", bool(value))
    elif field_name == "int8_value":
        encoded = struct.pack("<b", int(value))
    elif field_name == "int16_value":
        encoded = struct.pack("<h", int(value))
    elif field_name == "int32_value":
        encoded = struct.pack("<i", int(value))
    elif field_name == "float_value":
        encoded = struct.pack("<f", float(value))
    elif field_name == "double_value":
        encoded = struct.pack("<d", float(value))
    elif field_name in {"varchar_payload", COMPACTION_INTEGRITY_TEXT_FIELD}:
        encoded = value.encode("utf-8")
    elif field_name in {"json_payload", "dynamic_payload"}:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    elif field_name == "int64_array":
        encoded = struct.pack(f"<{len(value)}q", *(int(item) for item in value))
    elif field_name == "float_array":
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    elif field_name == "string_array":
        encoded = _length_prefixed_bytes([item.encode("utf-8") for item in value])
    elif field_name in {"float_vector", "nullable_float_vector"}:
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    elif field_name in {
        "binary_vector",
        "float16_vector",
        "bfloat16_vector",
        "int8_vector",
        "nullable_binary_vector",
        "nullable_int8_vector",
    }:
        if isinstance(value, list):
            assert len(value) == 1, f"expected one byte-vector payload for {field_name}, got {len(value)}"
            value = value[0]
        encoded = bytes(value)
    elif field_name in {"sparse_vector", "nullable_sparse_vector"}:
        encoded = b"".join(
            struct.pack("<If", int(index), float(item))
            for index, item in sorted(value.items(), key=lambda pair: int(pair[0]))
        )
    else:
        raise AssertionError(f"no canonical encoder for field {field_name}")
    return b"\x01" + struct.pack("<I", len(encoded)) + encoded


def _canonical_compaction_integrity_row(row, output_fields, primary_key_type):
    return {
        field_name: _canonical_compaction_integrity_cell(field_name, row[field_name], primary_key_type)
        for field_name in output_fields
    }


def _compaction_integrity_canonical_mismatch(actual_bytes, expected_bytes, field_name):
    common_length = min(len(actual_bytes), len(expected_bytes))
    differing_byte_count = abs(len(actual_bytes) - len(expected_bytes))
    first_offset = None
    for offset in range(common_length):
        if actual_bytes[offset] != expected_bytes[offset]:
            differing_byte_count += 1
            if first_offset is None:
                first_offset = offset
    if first_offset is None:
        first_offset = common_length
    window_start = max(0, first_offset - 8)
    window_end = min(max(len(actual_bytes), len(expected_bytes)), first_offset + 9)
    detail = {
        "actual_len": len(actual_bytes),
        "expected_len": len(expected_bytes),
        "differing_byte_count": differing_byte_count,
        "first_differing_byte_offset": first_offset,
        "actual_window": actual_bytes[window_start:window_end].hex(),
        "expected_window": expected_bytes[window_start:window_end].hex(),
        "window_start": window_start,
    }
    element_width = {
        "float_vector": 4,
        "nullable_float_vector": 4,
        "float16_vector": 2,
        "bfloat16_vector": 2,
        "int8_vector": 1,
        "nullable_int8_vector": 1,
    }.get(field_name)
    canonical_header_size = 5
    if element_width is not None and first_offset >= canonical_header_size:
        payload_offset = first_offset - canonical_header_size
        detail["element_index"] = payload_offset // element_width
        detail["byte_offset_in_element"] = payload_offset % element_width
    return detail


def _compaction_integrity_batch_corruption_summary(
    batch_index,
    batch,
    expected_by_pk,
    output_fields,
    primary_key_type,
    seen_primary_keys,
):
    issue_counts = {}
    field_mismatch_counts = {}
    corrupted_row_offsets = set()
    affected_primary_keys = set()
    samples = []
    total_sample_candidates = 0

    def record_issue(row_offset, issue, physical_pk=None, field_name=None, **detail):
        nonlocal total_sample_candidates
        issue_counts[issue] = issue_counts.get(issue, 0) + 1
        corrupted_row_offsets.add(row_offset)
        if physical_pk is not None:
            affected_primary_keys.add(physical_pk)
        if field_name is not None:
            field_mismatch_counts[field_name] = field_mismatch_counts.get(field_name, 0) + 1
        total_sample_candidates += 1
        if len(samples) < COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT:
            samples.append(
                {
                    "row_offset": row_offset,
                    "issue": issue,
                    "pk": physical_pk,
                    "field": field_name,
                    **detail,
                }
            )

    for row_offset, actual in enumerate(batch):
        if "id" not in actual:
            record_issue(row_offset, "missing_primary_key")
            continue
        physical_pk = actual["id"]
        if physical_pk in seen_primary_keys:
            record_issue(row_offset, "duplicate_primary_key", physical_pk=physical_pk)
        else:
            seen_primary_keys.add(physical_pk)
        if physical_pk not in expected_by_pk:
            record_issue(row_offset, "unexpected_primary_key", physical_pk=physical_pk)
            continue

        expected = expected_by_pk[physical_pk]
        actual_fields = set(actual)
        expected_fields = set(expected)
        if actual_fields != expected_fields:
            record_issue(
                row_offset,
                "field_set_mismatch",
                physical_pk=physical_pk,
                missing_fields=sorted(expected_fields - actual_fields),
                unexpected_fields=sorted(actual_fields - expected_fields),
            )
        for field_name in output_fields:
            if field_name not in actual:
                record_issue(
                    row_offset,
                    "missing_field",
                    physical_pk=physical_pk,
                    field_name=field_name,
                )
                continue
            actual_bytes = _canonical_compaction_integrity_cell(
                field_name,
                actual[field_name],
                primary_key_type,
            )
            expected_bytes = expected[field_name]
            if actual_bytes != expected_bytes:
                record_issue(
                    row_offset,
                    "canonical_value_mismatch",
                    physical_pk=physical_pk,
                    field_name=field_name,
                    **_compaction_integrity_canonical_mismatch(actual_bytes, expected_bytes, field_name),
                )

    if not issue_counts:
        return None
    return {
        "batch": batch_index,
        "batch_rows": len(batch),
        "corrupted_row_count": len(corrupted_row_offsets),
        "corrupted_cell_count": sum(field_mismatch_counts.values()),
        "affected_primary_key_count": len(affected_primary_keys),
        "affected_primary_key_sample": sorted(affected_primary_keys)[:COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT],
        "issue_counts": dict(sorted(issue_counts.items())),
        "field_mismatch_counts": dict(sorted(field_mismatch_counts.items())),
        "sample_count": len(samples),
        "samples_truncated": total_sample_candidates - len(samples),
        "samples": samples,
    }


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


def _assert_compaction_integrity_task_snapshot(task_snapshot):
    unknown = [task for task in task_snapshot.values() if task["state"] not in COMPACTION_INTEGRITY_KNOWN_TASK_STATES]
    assert not unknown, f"compaction tasks reported unknown states: {unknown}"


def _compaction_integrity_failed_tasks(task_snapshot):
    return {
        task_id: task
        for task_id, task in task_snapshot.items()
        if task["failure_reason"] or task["state"] in COMPACTION_INTEGRITY_FAILED_TASK_STATES
    }


def _compaction_integrity_descendants(segment_id, children):
    descendants = set()
    pending = [segment_id]
    while pending:
        current = pending.pop()
        for child in children.get(current, ()):
            assert child != segment_id, f"cycle detected in compaction blood graph at segment {child}"
            if child in descendants:
                continue
            descendants.add(child)
            pending.append(child)
    return descendants


def _assert_compaction_integrity_graph_transition(
    before_checkpoint,
    after_checkpoint,
    require_new_inputs=True,
):
    before_all = before_checkpoint["all"]
    before_active = before_checkpoint["active"]
    after_all = after_checkpoint["all"]
    after_active = after_checkpoint["active"]
    missing_history = set(before_all) - set(after_all)
    assert not missing_history, f"segment history disappeared during the test: {missing_history}"

    children = {}
    for target_id, target in after_all.items():
        for source_id in target["compaction_from"]:
            assert source_id in after_all, f"lineage source {source_id} for target {target_id} is missing"
            assert source_id != target_id, f"segment {target_id} lists itself as a compaction source"
            children.setdefault(source_id, set()).add(target_id)

    new_segment_ids = set(after_all) - set(before_all)
    round_edges = {
        (source_id, target_id) for target_id in new_segment_ids for source_id in after_all[target_id]["compaction_from"]
    }
    assert round_edges, f"round created no observable compaction transition: new_segments={new_segment_ids}"
    assert all(after_all[source_id]["state"] == "Dropped" for source_id, _ in round_edges), (
        f"compaction sources are not dropped at the checkpoint: {round_edges}"
    )

    for segment_id in before_active:
        descendants = _compaction_integrity_descendants(segment_id, children)
        assert segment_id in after_active or descendants.intersection(after_active), (
            f"previously active segment {segment_id} has no active descendant"
        )

    if require_new_inputs:
        new_roots = {
            segment_id
            for segment_id in new_segment_ids
            if after_all[segment_id]["num_rows"] != 0 and not after_all[segment_id]["compaction_from"]
        }
        assert new_roots, f"round created no observable flushed input segments: {new_segment_ids}"
        for segment_id in new_roots:
            descendants = _compaction_integrity_descendants(segment_id, children)
            assert segment_id in after_active or descendants.intersection(after_active), (
                f"new input segment {segment_id} has no active descendant"
            )
    return round_edges


def _wait_for_compaction_integrity_checkpoint(
    client,
    collection_name,
    expected_rows,
    before_checkpoint,
    require_new_inputs=True,
    timeout=120,
):
    start_time = time.time()
    deadline = time.time() + timeout
    poll_count = 0
    stable_polls = 0
    last_signature = None
    last_observation = {}
    reported_task_failures = set()
    while time.time() < deadline:
        poll_count += 1
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
        _assert_compaction_integrity_task_snapshot(tasks)
        failed_tasks = _compaction_integrity_failed_tasks(tasks)
        new_task_failures = {
            task_id: task
            for task_id, task in failed_tasks.items()
            if (task_id, task["state"], task["failure_reason"]) not in reported_task_failures
        }
        if new_task_failures:
            _log_compaction_integrity_evidence(
                "compaction_task_failures_observed",
                collection=collection_name,
                failed_tasks=[new_task_failures[task_id] for task_id in sorted(new_task_failures)],
            )
            reported_task_failures.update(
                (task_id, task["state"], task["failure_reason"])
                for task_id, task in new_task_failures.items()
            )
        storage_versions = {segment["storage_version"] for segment in active_segments.values()}
        last_observation = {
            "all": all_segments,
            "active": active_segments,
            "serving": serving_segments,
            "tasks": tasks,
            "storage_versions": storage_versions,
        }
        has_graph_transition = False
        try:
            _assert_compaction_integrity_graph_transition(
                before_checkpoint,
                last_observation,
                require_new_inputs=require_new_inputs,
            )
            has_graph_transition = True
        except AssertionError:
            pass
        tasks_terminal = all(task["state"] in COMPACTION_INTEGRITY_TERMINAL_TASK_STATES for task in tasks.values())
        active_stable = bool(active_segments) and all(
            segment["state"] in COMPACTION_INTEGRITY_STABLE_STATES and segment["is_sorted"]
            for segment in active_segments.values()
        )
        serving_matches_active = set(serving_segments) == set(active_segments)
        serving_sealed = all(segment["state"] == "Sealed" for segment in serving_segments.values())
        active_rows = sum(segment["num_rows"] for segment in active_segments.values())
        row_count_matches = active_rows == expected_rows
        storage_version_valid = len(storage_versions) == 1 and storage_versions.issubset({2, 3})
        ready = (
            tasks_terminal
            and active_stable
            and serving_matches_active
            and serving_sealed
            and row_count_matches
            and storage_version_valid
            and has_graph_transition
        )
        signature = (
            tuple(sorted((segment_id, tuple(sorted(segment.items()))) for segment_id, segment in all_segments.items())),
            tuple(sorted(serving_segments)),
            tuple(sorted((task_id, tuple(sorted(task.items()))) for task_id, task in tasks.items())),
        )
        if ready and signature == last_signature:
            stable_polls += 1
        elif ready:
            stable_polls = 1
        else:
            stable_polls = 0
        if signature != last_signature or ready or poll_count % 15 == 0:
            segment_state_counts = {
                state: sum(segment["state"] == state for segment in all_segments.values())
                for state in sorted({segment["state"] for segment in all_segments.values()})
            }
            task_state_counts = {
                state: sum(task["state"] == state for task in tasks.values())
                for state in sorted({task["state"] for task in tasks.values()})
            }
            log.info(
                f"compaction checkpoint poll collection={collection_name} poll={poll_count} "
                f"elapsed={time.time() - start_time:.1f}s ready={ready} stable_polls={stable_polls}/3 "
                f"tasks_terminal={tasks_terminal} failed_task_ids={sorted(failed_tasks)} "
                f"active_stable={active_stable} "
                f"serving_matches_active={serving_matches_active} serving_sealed={serving_sealed} "
                f"row_count_matches={row_count_matches} graph_transition={has_graph_transition} "
                f"storage_version_valid={storage_version_valid} rows={active_rows}/{expected_rows} "
                f"segment_states={segment_state_counts} task_states={task_state_counts} "
                f"active_ids={sorted(active_segments)} serving_ids={sorted(serving_segments)} "
                f"storage_versions={sorted(storage_versions)}"
            )
        if stable_polls >= 3:
            log.info(
                f"compaction checkpoint reached collection={collection_name} polls={poll_count} "
                f"elapsed={time.time() - start_time:.1f}s active_ids={sorted(active_segments)} "
                f"serving_ids={sorted(serving_segments)} rows={active_rows}"
            )
            return last_observation
        last_signature = signature
        time.sleep(2)
    raise AssertionError(f"collection did not reach a stable compaction checkpoint: {last_observation}")


def _assert_compaction_integrity_dataset(
    client,
    collection_name,
    expected_by_pk,
    output_fields,
    primary_key_type,
):
    log.info(
        f"data integrity validation start collection={collection_name} rows={len(expected_by_pk)} "
        f"fields={len(output_fields)} output_fields={output_fields}"
    )
    query_filter = "id >= 0" if primary_key_type == DataType.INT64 else 'id != ""'
    iterator = client.query_iterator(
        collection_name,
        batch_size=2000,
        filter=query_filter,
        output_fields=output_fields,
        consistency_level="Strong",
    )
    seen_primary_keys = set()
    actual_count = 0
    batch_index = 0
    try:
        while True:
            batch = iterator.next()
            if not batch:
                break
            batch_index += 1
            actual_count += len(batch)
            corruption_summary = _compaction_integrity_batch_corruption_summary(
                batch_index,
                batch,
                expected_by_pk,
                output_fields,
                primary_key_type,
                seen_primary_keys,
            )
            log.info(
                f"data integrity validation progress collection={collection_name} batch={batch_index} "
                f"batch_rows={len(batch)} validated_rows={actual_count}/{len(expected_by_pk)} "
                f"corrupted_rows={0 if corruption_summary is None else corruption_summary['corrupted_row_count']} "
                f"corrupted_cells={0 if corruption_summary is None else corruption_summary['corrupted_cell_count']}"
            )
            if corruption_summary is not None:
                _log_compaction_integrity_evidence(
                    "data_integrity_batch_corruption",
                    collection=collection_name,
                    **corruption_summary,
                )
                raise AssertionError(
                    f"data corruption detected in retrieve batch: "
                    f"{json.dumps(corruption_summary, sort_keys=True, separators=(',', ':'))}"
                )
    finally:
        iterator.close()

    assert actual_count == len(expected_by_pk), (
        f"row count mismatch: actual={actual_count}, expected={len(expected_by_pk)}"
    )
    assert seen_primary_keys == set(expected_by_pk), (
        f"primary key set mismatch: missing={set(expected_by_pk) - seen_primary_keys}, "
        f"unexpected={seen_primary_keys - set(expected_by_pk)}"
    )
    log.info(
        f"data integrity validation complete collection={collection_name} rows={actual_count} "
        f"fields={len(output_fields)} batches={batch_index}"
    )


def test_compaction_integrity_corruption_scans_current_batch_then_stops():
    output_fields = ["id", "int64_value", "float_vector"]
    expected_rows = [
        {"id": 1, "int64_value": 10, "float_vector": [1.0, 2.0]},
        {"id": 2, "int64_value": 20, "float_vector": [3.0, 4.0]},
    ]
    expected_by_pk = {
        row["id"]: _canonical_compaction_integrity_row(row, output_fields, DataType.INT64) for row in expected_rows
    }

    class FakeIterator:
        def __init__(self):
            self.next_calls = 0
            self.closed = False

        def next(self):
            self.next_calls += 1
            assert self.next_calls == 1, "validation queried another batch after detecting corruption"
            return [
                {"id": 1, "int64_value": 10, "float_vector": [1.0, 9.0]},
                {"id": 2, "int64_value": 21, "float_vector": [8.0, 4.0]},
            ]

        def close(self):
            self.closed = True

    class FakeClient:
        def __init__(self):
            self.iterator = FakeIterator()

        def query_iterator(self, *args, **kwargs):
            return self.iterator

    client = FakeClient()
    with pytest.raises(AssertionError, match="data corruption detected in retrieve batch") as error:
        _assert_compaction_integrity_dataset(
            client,
            "corruption_summary_test",
            expected_by_pk,
            output_fields,
            DataType.INT64,
        )

    summary = json.loads(str(error.value).split(": ", 1)[1])
    assert summary["batch"] == 1
    assert summary["batch_rows"] == 2
    assert summary["corrupted_row_count"] == 2
    assert summary["corrupted_cell_count"] == 3
    assert summary["affected_primary_key_count"] == 2
    assert summary["issue_counts"] == {"canonical_value_mismatch": 3}
    assert summary["field_mismatch_counts"] == {"float_vector": 2, "int64_value": 1}
    assert {(sample["pk"], sample["field"], sample.get("element_index")) for sample in summary["samples"]} == {
        (1, "float_vector", 1),
        (2, "int64_value", None),
        (2, "float_vector", 0),
    }
    assert client.iterator.next_calls == 1
    assert client.iterator.closed


@pytest.mark.xdist_group("TestMilvusClientCompactionDataIntegrity")
class TestMilvusClientCompactionDataIntegrity(TestMilvusClientV2Base):
    """Compaction lifecycle data-integrity tests with isolated mutable collections."""

    def _detect_compaction_integrity_storage_version(self, client, expected_version=None, timeout=120):
        deadline = time.time() + timeout
        last_versions = set()
        while time.time() < deadline:
            probe_name = cf.gen_unique_str("compaction_storage_probe")
            created = False
            try:
                log.info(
                    f"storage version probe start collection={probe_name} expected_version={expected_version}"
                )
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                index_params = self.prepare_index_params(client)[0]
                index_params.add_index("vector", index_type="AUTOINDEX", metric_type="COSINE")
                self.create_collection(
                    client,
                    probe_name,
                    schema=schema,
                    index_params=index_params,
                    consistency_level="Strong",
                    num_shards=1,
                )
                created = True
                self.insert(client, probe_name, [{"id": 1, "vector": [1.0, 0.0]}])
                self.flush(client, probe_name)
                probe_deadline = min(deadline, time.time() + 20)
                while time.time() < probe_deadline:
                    last_versions = {
                        segment.storage_version
                        for segment in client.list_segments(probe_name)
                        if segment.num_rows != 0
                        and segment.state_name in COMPACTION_INTEGRITY_ACTIVE_STATES
                        and segment.storage_version in {2, 3}
                    }
                    if expected_version is None and len(last_versions) == 1:
                        storage_version = next(iter(last_versions))
                        log.info(
                            f"storage version probe complete collection={probe_name} "
                            f"storage_version={storage_version}"
                        )
                        return storage_version
                    if last_versions == {expected_version}:
                        log.info(
                            f"storage version adoption verified collection={probe_name} "
                            f"storage_version={expected_version}"
                        )
                        return expected_version
                    if last_versions:
                        break
                    time.sleep(2)
            finally:
                if created:
                    self.drop_collection(client, probe_name)
            time.sleep(2)
        raise AssertionError(
            f"server did not create a flushed segment with expected storage version "
            f"{expected_version}: last_versions={last_versions}"
        )

    def _create_compaction_integrity_collection(
        self,
        client,
        collection_name,
        primary_key_type,
        include_text,
    ):
        output_fields = list(COMPACTION_INTEGRITY_BASE_OUTPUT_FIELDS)
        if include_text:
            output_fields.append(COMPACTION_INTEGRITY_TEXT_FIELD)
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=True)[0]
        if primary_key_type == DataType.INT64:
            schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        else:
            schema.add_field("id", DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False)
        schema.add_field("explicit_test_ts", DataType.INT64)
        schema.add_field("bool_value", DataType.BOOL, nullable=True)
        schema.add_field("int8_value", DataType.INT8)
        schema.add_field("int16_value", DataType.INT16)
        schema.add_field("int32_value", DataType.INT32)
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
        schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field("binary_vector", DataType.BINARY_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field("float16_vector", DataType.FLOAT16_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field("bfloat16_vector", DataType.BFLOAT16_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field("sparse_vector", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field("int8_vector", DataType.INT8_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field(
            "nullable_float_vector",
            DataType.FLOAT_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
            nullable=True,
        )
        schema.add_field(
            "nullable_binary_vector",
            DataType.BINARY_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
            nullable=True,
        )
        schema.add_field("nullable_sparse_vector", DataType.SPARSE_FLOAT_VECTOR, nullable=True)
        schema.add_field(
            "nullable_int8_vector",
            DataType.INT8_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
            nullable=True,
        )
        if include_text:
            schema.add_field(COMPACTION_INTEGRITY_TEXT_FIELD, DataType.TEXT)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index("float_vector", index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index("binary_vector", index_type="BIN_FLAT", metric_type="HAMMING")
        index_params.add_index("float16_vector", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index("bfloat16_vector", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index("sparse_vector", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        index_params.add_index("int8_vector", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index("nullable_float_vector", index_type="AUTOINDEX", metric_type="COSINE")
        index_params.add_index("nullable_binary_vector", index_type="BIN_FLAT", metric_type="HAMMING")
        index_params.add_index("nullable_sparse_vector", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        index_params.add_index("nullable_int8_vector", index_type="AUTOINDEX", metric_type="L2")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
            num_shards=1,
        )
        return output_fields

    def _append_compaction_integrity_round(
        self,
        client,
        collection_name,
        expected_by_pk,
        output_fields,
        primary_key_type,
        include_text,
        round_index,
    ):
        for segment_index in range(COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND):
            explicit_test_ts = round_index * COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND + segment_index + 1
            pk_start = explicit_test_ts * COMPACTION_INTEGRITY_ROWS_PER_SEGMENT
            rows = []
            for logical_pk in range(pk_start, pk_start + COMPACTION_INTEGRITY_ROWS_PER_SEGMENT):
                row, expected = _build_compaction_integrity_row(
                    collection_name,
                    logical_pk,
                    explicit_test_ts,
                    primary_key_type,
                    include_text,
                )
                rows.append(row)
                expected_by_pk[row["id"]] = _canonical_compaction_integrity_row(
                    expected,
                    output_fields,
                    primary_key_type,
                )
            insert_result = self.insert(client, collection_name, rows)[0]
            assert insert_result["insert_count"] == COMPACTION_INTEGRITY_ROWS_PER_SEGMENT
            self.flush(client, collection_name)
            log.info(
                f"compaction integrity batch flushed collection={collection_name} round={round_index + 1} "
                f"batch={segment_index + 1}/{COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND} "
                f"explicit_test_ts={explicit_test_ts} logical_pk_range="
                f"[{pk_start},{pk_start + COMPACTION_INTEGRITY_ROWS_PER_SEGMENT - 1}] "
                f"inserted_rows={insert_result['insert_count']} expected_total={len(expected_by_pk)}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "primary_key_type",
        [DataType.INT64, DataType.VARCHAR],
        ids=["int64_pk", "varchar_pk"],
    )
    @pytest.mark.parametrize(
        "round_count",
        COMPACTION_INTEGRITY_ROUND_COUNTS,
        ids=lambda value: f"{value}_rounds",
    )
    def test_compaction_active_set_transitions_preserve_all_rows_and_fields(self, primary_key_type, round_count):
        """
        target: verify all-type row safety at configurable round-level compaction lifecycle checkpoints
        method: append and flush ten batches, request compaction, observe the blood graph and serving handoff, then query
        expected: every round has a real transition and preserves every PK, persisted field, null/default, and cell byte
        """
        assert round_count > 0, "compaction integrity round count must be positive"
        client = self._client()
        collection_name = cf.gen_unique_str("compaction_data_integrity")
        storage_version = self._detect_compaction_integrity_storage_version(client)
        include_text = storage_version == 3
        output_fields = self._create_compaction_integrity_collection(
            client,
            collection_name,
            primary_key_type,
            include_text,
        )
        log.info(
            f"compaction integrity case start collection={collection_name} pk_type={primary_key_type.name} "
            f"storage_version={storage_version} include_text={include_text} rounds={round_count} "
            f"segments_per_round={COMPACTION_INTEGRITY_SEGMENTS_PER_ROUND} "
            f"rows_per_segment={COMPACTION_INTEGRITY_ROWS_PER_SEGMENT} fields={len(output_fields)} "
            f"output_fields={output_fields}"
        )
        log.info(f"compaction integrity collection ready collection={collection_name}")

        expected_by_pk = {}
        previous_checkpoint = {
            "all": {},
            "active": {},
            "serving": {},
            "tasks": {},
            "storage_versions": set(),
        }

        for round_index in range(round_count):
            log.info(
                f"compaction integrity round start collection={collection_name} round={round_index + 1}/"
                f"{round_count} prior_rows={len(expected_by_pk)} "
                f"prior_active_ids={sorted(previous_checkpoint['active'])} "
                f"prior_lineage_segments={len(previous_checkpoint['all'])}"
            )
            self._append_compaction_integrity_round(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                include_text,
                round_index,
            )
            compact_id = self.compact(client, collection_name)[0]
            log.info(
                f"compaction requested collection={collection_name} round={round_index + 1} "
                f"manual_job={compact_id} expected_rows={len(expected_by_pk)}"
            )
            checkpoint = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=len(expected_by_pk),
                before_checkpoint=previous_checkpoint,
            )
            round_edges = _assert_compaction_integrity_graph_transition(previous_checkpoint, checkpoint)
            assert checkpoint["storage_versions"] == {storage_version}
            _log_compaction_integrity_evidence(
                "round_checkpoint",
                collection=collection_name,
                round=round_index + 1,
                manual_job=compact_id,
                expected_rows=len(expected_by_pk),
                storage_versions=sorted(checkpoint["storage_versions"]),
                round_edges=[
                    {"source": source_id, "target": target_id} for source_id, target_id in sorted(round_edges)
                ],
                all_segments=[checkpoint["all"][segment_id] for segment_id in sorted(checkpoint["all"])],
                active_segment_ids=sorted(checkpoint["active"]),
                serving_segment_ids=sorted(checkpoint["serving"]),
                compaction_tasks=[checkpoint["tasks"][task_id] for task_id in sorted(checkpoint["tasks"])],
            )
            log.info(
                f"compaction transition verified collection={collection_name} round={round_index + 1} "
                f"manual_job={compact_id} round_edges={sorted(round_edges)} "
                f"active_ids={sorted(checkpoint['active'])} serving_ids={sorted(checkpoint['serving'])}"
            )
            _assert_compaction_integrity_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
            )
            _log_compaction_integrity_evidence(
                "round_data_validated",
                collection=collection_name,
                round=round_index + 1,
                rows=len(expected_by_pk),
                fields=len(output_fields),
                primary_key_type=primary_key_type.name,
                storage_version=storage_version,
            )
            previous_checkpoint = checkpoint
            log.info(
                f"compaction integrity round complete collection={collection_name} round={round_index + 1} "
                f"manual_job={compact_id} rows={len(expected_by_pk)} "
                f"storage_version={storage_version} active_segments={sorted(checkpoint['active'])} "
                f"round_compaction_edges={len(round_edges)} lineage_segments={len(checkpoint['all'])}"
            )

        log.info(
            f"compaction integrity final validation start collection={collection_name} "
            f"rows={len(expected_by_pk)} rounds={round_count}"
        )
        _assert_compaction_integrity_dataset(
            client,
            collection_name,
            expected_by_pk,
            output_fields,
            primary_key_type,
        )
        _log_compaction_integrity_evidence(
            "final_data_validated",
            collection=collection_name,
            rounds=round_count,
            rows=len(expected_by_pk),
            fields=len(output_fields),
            primary_key_type=primary_key_type.name,
            storage_version=storage_version,
        )
        log.info(
            f"compaction integrity case complete collection={collection_name} pk_type={primary_key_type.name} "
            f"storage_version={storage_version} rows={len(expected_by_pk)} fields={len(output_fields)}"
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.skipif(
        not COMPACTION_INTEGRITY_RUN_STORAGE_TRANSITION,
        reason="storage-version transition mutates cluster-wide config and requires an exclusive serial E2E stage",
    )
    @pytest.mark.parametrize(
        "primary_key_type",
        [DataType.INT64, DataType.VARCHAR],
        ids=["int64_pk", "varchar_pk"],
    )
    @pytest.mark.parametrize(
        "round_count",
        COMPACTION_INTEGRITY_ROUND_COUNTS,
        ids=lambda value: f"{value}_rounds",
    )
    def test_compaction_storage_version_transitions_preserve_all_rows_and_fields(
        self,
        primary_key_type,
        round_count,
        etcd_host,
        etcd_port,
        etcd_root_path,
        etcd_user,
        etcd_password,
    ):
        """
        target: verify all-type row safety while persisted segments are rewritten V2 -> V3 -> V2
        method: atomically switch the storage config, prove adoption with probe segments, and observe both lineage rewrites
        expected: both transitions hand off a complete serving set with every PK, field, null/default, and cell byte intact
        """
        client = self._client()
        collection_name = cf.gen_unique_str("compaction_storage_transition")
        include_text = False
        expected_by_pk = {}
        created = False
        with MilvusEtcdConfigController(
            host=etcd_host,
            port=etcd_port,
            root_path=etcd_root_path,
            user=etcd_user,
            password=etcd_password,
        ) as config_controller:
            original = config_controller.read_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                "storage_transition_config_original",
                etcd_endpoint=config_controller.endpoint,
                etcd_root_path=etcd_root_path,
                config_key=COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                value=None if original.value is None else original.value.decode(),
                mod_revision=original.mod_revision,
            )
            with config_controller.preserve_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG):
                try:
                    v2_config = config_controller.set_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG, "false")
                    _log_compaction_integrity_evidence(
                        "storage_transition_config_committed",
                        target_storage_version=2,
                        value="false",
                        mod_revision=v2_config.mod_revision,
                    )
                    self._detect_compaction_integrity_storage_version(client, expected_version=2)
                    output_fields = self._create_compaction_integrity_collection(
                        client,
                        collection_name,
                        primary_key_type,
                        include_text,
                    )
                    created = True
                    v2_checkpoint = {
                        "all": {},
                        "active": {},
                        "serving": {},
                        "tasks": {},
                        "storage_versions": set(),
                    }
                    for round_index in range(round_count):
                        previous_v2_checkpoint = v2_checkpoint
                        self._append_compaction_integrity_round(
                            client,
                            collection_name,
                            expected_by_pk,
                            output_fields,
                            primary_key_type,
                            include_text,
                            round_index=round_index,
                        )
                        v2_job = self.compact(client, collection_name)[0]
                        v2_checkpoint = _wait_for_compaction_integrity_checkpoint(
                            client,
                            collection_name,
                            expected_rows=len(expected_by_pk),
                            before_checkpoint=previous_v2_checkpoint,
                        )
                        assert v2_checkpoint["storage_versions"] == {2}
                        v2_edges = _assert_compaction_integrity_graph_transition(
                            previous_v2_checkpoint,
                            v2_checkpoint,
                        )
                        _assert_compaction_integrity_dataset(
                            client,
                            collection_name,
                            expected_by_pk,
                            output_fields,
                            primary_key_type,
                        )
                        _log_compaction_integrity_evidence(
                            "storage_transition_v2_round_validated",
                            collection=collection_name,
                            round=round_index + 1,
                            rounds=round_count,
                            manual_job=v2_job,
                            rows=len(expected_by_pk),
                            transition_edges=sorted(v2_edges),
                            storage_versions=sorted(v2_checkpoint["storage_versions"]),
                            all_segments=[
                                v2_checkpoint["all"][segment_id]
                                for segment_id in sorted(v2_checkpoint["all"])
                            ],
                            active_segment_ids=sorted(v2_checkpoint["active"]),
                            serving_segment_ids=sorted(v2_checkpoint["serving"]),
                            compaction_tasks=[
                                v2_checkpoint["tasks"][task_id]
                                for task_id in sorted(v2_checkpoint["tasks"])
                            ],
                        )

                    v3_config = config_controller.set_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG, "true")
                    _log_compaction_integrity_evidence(
                        "storage_transition_config_committed",
                        target_storage_version=3,
                        value="true",
                        mod_revision=v3_config.mod_revision,
                    )
                    self._detect_compaction_integrity_storage_version(client, expected_version=3)
                    v3_job = self.compact(client, collection_name)[0]
                    v3_checkpoint = _wait_for_compaction_integrity_checkpoint(
                        client,
                        collection_name,
                        expected_rows=len(expected_by_pk),
                        before_checkpoint=v2_checkpoint,
                        require_new_inputs=False,
                    )
                    assert v3_checkpoint["storage_versions"] == {3}
                    v3_edges = _assert_compaction_integrity_graph_transition(
                        v2_checkpoint,
                        v3_checkpoint,
                        require_new_inputs=False,
                    )
                    v2_to_v3_edges = {
                        (source_id, target_id)
                        for source_id, target_id in v3_edges
                        if v3_checkpoint["all"][source_id]["storage_version"] == 2
                        and v3_checkpoint["all"][target_id]["storage_version"] == 3
                    }
                    assert v2_to_v3_edges, f"no V2 -> V3 lineage edge observed: {v3_edges}"
                    _assert_compaction_integrity_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        primary_key_type,
                    )
                    _log_compaction_integrity_evidence(
                        "storage_transition_v2_to_v3_validated",
                        collection=collection_name,
                        manual_job=v3_job,
                        rows=len(expected_by_pk),
                        transition_edges=sorted(v2_to_v3_edges),
                        storage_versions=sorted(v3_checkpoint["storage_versions"]),
                        all_segments=[
                            v3_checkpoint["all"][segment_id] for segment_id in sorted(v3_checkpoint["all"])
                        ],
                        active_segment_ids=sorted(v3_checkpoint["active"]),
                        serving_segment_ids=sorted(v3_checkpoint["serving"]),
                        compaction_tasks=[
                            v3_checkpoint["tasks"][task_id] for task_id in sorted(v3_checkpoint["tasks"])
                        ],
                    )

                    v2_restore_config = config_controller.set_config(
                        COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                        "false",
                    )
                    _log_compaction_integrity_evidence(
                        "storage_transition_config_committed",
                        target_storage_version=2,
                        value="false",
                        mod_revision=v2_restore_config.mod_revision,
                    )
                    self._detect_compaction_integrity_storage_version(client, expected_version=2)
                    v2_restore_job = self.compact(client, collection_name)[0]
                    final_checkpoint = _wait_for_compaction_integrity_checkpoint(
                        client,
                        collection_name,
                        expected_rows=len(expected_by_pk),
                        before_checkpoint=v3_checkpoint,
                        require_new_inputs=False,
                    )
                    assert final_checkpoint["storage_versions"] == {2}
                    final_edges = _assert_compaction_integrity_graph_transition(
                        v3_checkpoint,
                        final_checkpoint,
                        require_new_inputs=False,
                    )
                    v3_to_v2_edges = {
                        (source_id, target_id)
                        for source_id, target_id in final_edges
                        if final_checkpoint["all"][source_id]["storage_version"] == 3
                        and final_checkpoint["all"][target_id]["storage_version"] == 2
                    }
                    assert v3_to_v2_edges, f"no V3 -> V2 lineage edge observed: {final_edges}"
                    _assert_compaction_integrity_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        primary_key_type,
                    )
                    _log_compaction_integrity_evidence(
                        "storage_transition_v3_to_v2_validated",
                        collection=collection_name,
                        manual_job=v2_restore_job,
                        rows=len(expected_by_pk),
                        transition_edges=sorted(v3_to_v2_edges),
                        storage_versions=sorted(final_checkpoint["storage_versions"]),
                        all_segments=[
                            final_checkpoint["all"][segment_id]
                            for segment_id in sorted(final_checkpoint["all"])
                        ],
                        active_segment_ids=sorted(final_checkpoint["active"]),
                        serving_segment_ids=sorted(final_checkpoint["serving"]),
                        compaction_tasks=[
                            final_checkpoint["tasks"][task_id]
                            for task_id in sorted(final_checkpoint["tasks"])
                        ],
                    )
                finally:
                    if created:
                        self.drop_collection(client, collection_name)
            restored = config_controller.read_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                "storage_transition_config_restored",
                config_key=COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                value=None if restored.value is None else restored.value.decode(),
                mod_revision=restored.mod_revision,
            )
