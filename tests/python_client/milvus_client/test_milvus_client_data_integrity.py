import hashlib
import json
import os
import struct
import time
from contextlib import contextmanager

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from pymilvus import BulkInsertState, DataType, DefaultConfig, FieldSchema, Function, FunctionType, MilvusException
from pymilvus.bulk_writer import BulkFileType, RemoteBulkWriter
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


COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH = 1000
COMPACTION_INTEGRITY_INSERT_BATCHES_PER_ROUND = 10
COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND = int(
    os.getenv("MILVUS_COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND", "10000")
)
COMPACTION_INTEGRITY_ROUND_COUNTS = [int(os.getenv("MILVUS_COMPACTION_INTEGRITY_ROUNDS", "3"))]
COMPACTION_INTEGRITY_VECTOR_DIM = 16
COMPACTION_INTEGRITY_LOB_ROW_INTERVAL = 1000
COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD = "struct_array_payload"
COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD_ID = 27
COMPACTION_INTEGRITY_STRUCT_ARRAY_MAX_CAPACITY = 4
COMPACTION_INTEGRITY_STRUCT_SUBFIELD_IDS = {
    "struct_bool": 28,
    "struct_int8": 29,
    "struct_int16": 30,
    "struct_int32": 31,
    "struct_int64": 32,
    "struct_float": 33,
    "struct_double": 34,
    "struct_varchar": 35,
    "struct_float_vector": 36,
    "struct_binary_vector": 37,
    "struct_float16_vector": 38,
    "struct_bfloat16_vector": 39,
    "struct_int8_vector": 40,
}
COMPACTION_INTEGRITY_STRUCT_SUBFIELDS = tuple(COMPACTION_INTEGRITY_STRUCT_SUBFIELD_IDS)
COMPACTION_INTEGRITY_DEFAULT_VALUE = -91919530
COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT = 20
COMPACTION_INTEGRITY_TEXT_INLINE_THRESHOLD = int(os.getenv("MILVUS_TEXT_INLINE_THRESHOLD", "65536"))
COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG = "common.storage.useLoonFFI"
COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG = "dataCoord.compaction.bumpSchemaVersion.enabled"
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
    COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
    "sparse_vector",
    "nullable_float_vector",
    "nullable_binary_vector",
    "nullable_sparse_vector",
    "nullable_int8_vector",
    "dynamic_payload",
]
COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS = (
    "float_vector",
    "binary_vector",
    "float16_vector",
    "bfloat16_vector",
    "sparse_vector",
    "int8_vector",
)
COMPACTION_INTEGRITY_NULLABLE_VECTOR_FIELDS = {
    "nullable_float_vector",
    "nullable_binary_vector",
    "nullable_sparse_vector",
    "nullable_int8_vector",
}
COMPACTION_INTEGRITY_TEXT_FIELD = "text_payload"
COMPACTION_INTEGRITY_BM25_TEXT_FIELD = "bm25_text"
COMPACTION_INTEGRITY_BM25_BASE_FIELD = "sparse_base"
COMPACTION_INTEGRITY_BM25_BASE_FUNCTION = "bm25_base_fn"
COMPACTION_INTEGRITY_BM25_ADDED_FIELD = "sparse_added"
COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION = "bm25_added_fn"
COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD = "added_default_payload"
COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE = -771923
COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH = int(os.getenv("MILVUS_COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH", "5000"))
COMPACTION_INTEGRITY_DDL_BATCHES = int(os.getenv("MILVUS_COMPACTION_INTEGRITY_DDL_BATCHES", "5"))
COMPACTION_INTEGRITY_KEEP_DDL_COLLECTION = (
    os.getenv("MILVUS_COMPACTION_INTEGRITY_KEEP_DDL_COLLECTION", "false").lower() == "true"
)
COMPACTION_INTEGRITY_QUERY_BATCH_SIZE = int(os.getenv("MILVUS_COMPACTION_INTEGRITY_QUERY_BATCH_SIZE", "500"))
COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE = int(os.getenv("MILVUS_COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE", "256"))
COMPACTION_INTEGRITY_BUMP_TASK_TYPE = "BumpSchemaVersionCompaction"
COMPACTION_INTEGRITY_SCHEMA_REWRITE_TASK_TYPES = {
    COMPACTION_INTEGRITY_BUMP_TASK_TYPE,
    "MixCompaction",
    "SortCompaction",
}


def _log_compaction_integrity_evidence(event, **evidence):
    log.info(
        f"compaction_integrity_evidence {json.dumps({'event': event, **evidence}, sort_keys=True)}",
        extra={"persist_on_pass": True},
    )


def _compaction_integrity_pk_digest(primary_keys, primary_key_type):
    digest = hashlib.sha256()
    for primary_key in sorted(primary_keys):
        encoded = _canonical_compaction_integrity_cell("id", primary_key, primary_key_type)
        digest.update(struct.pack("<I", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def _compaction_integrity_ingress_identity(
    run_id,
    ingress_type,
    round_index,
    batch_index,
    explicit_test_ts,
    expected_delta,
    primary_key_type,
):
    primary_keys = sorted(expected_delta)
    assert primary_keys, "ingress identity requires a non-empty expected delta"
    return {
        "run_id": run_id,
        "ingress_type": ingress_type,
        "round": round_index + 1,
        "batch": batch_index + 1,
        "explicit_test_ts": explicit_test_ts,
        "pk_start": primary_keys[0],
        "pk_end": primary_keys[-1],
        "row_count": len(primary_keys),
        "pk_digest": _compaction_integrity_pk_digest(primary_keys, primary_key_type),
    }


def _prepare_compaction_integrity_bulk_writer_row(row, expected, logical_pk, explicit_test_ts):
    """Adapt the deterministic dataset to RemoteBulkWriter's supported input representations."""
    writer_row = dict(row)
    writer_expected = dict(expected)
    for field_name in ("int8_vector", "nullable_int8_vector"):
        value = writer_row.get(field_name)
        if isinstance(value, bytes | bytearray):
            writer_row[field_name] = np.frombuffer(value, dtype=np.int8).copy()
    if writer_row.get(COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD) is None:
        # RemoteBulkWriter currently rejects None for a nullable StructArray, so ImportIngress
        # exercises empty and non-empty StructArray values while InsertIngress owns null coverage.
        writer_row[COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD] = []
        writer_expected[COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD] = []
    if writer_row.get("nullable_sparse_vector") is None:
        # Parquet currently reads nullable sparse-vector nulls back as empty vectors, so
        # ImportIngress uses a non-empty per-row fingerprint while InsertIngress owns null coverage.
        sparse_fingerprint = {
            0: _compaction_integrity_float32(logical_pk, explicit_test_ts, 25, 0),
        }
        writer_row["nullable_sparse_vector"] = sparse_fingerprint
        writer_expected["nullable_sparse_vector"] = sparse_fingerprint
    return writer_row, writer_expected


COMPACTION_INTEGRITY_ACTIVE_STATES = {"Growing", "Sealed", "Flushing", "Flushed", "Importing"}
COMPACTION_INTEGRITY_STABLE_STATES = {"Flushed"}
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


def _compaction_integrity_output_fields(include_struct_array, include_text, include_bm25_control):
    output_fields = list(COMPACTION_INTEGRITY_BASE_OUTPUT_FIELDS)
    if not include_struct_array:
        output_fields = [
            field_name
            for field_name in output_fields
            if field_name != COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD
            and field_name not in COMPACTION_INTEGRITY_NULLABLE_VECTOR_FIELDS
            and field_name != "sparse_vector"
        ]
        dynamic_field_index = output_fields.index("dynamic_payload")
        output_fields[dynamic_field_index:dynamic_field_index] = COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS
    if include_text:
        output_fields.append(COMPACTION_INTEGRITY_TEXT_FIELD)
    if include_bm25_control:
        output_fields.append(COMPACTION_INTEGRITY_BM25_TEXT_FIELD)
    return output_fields


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


def _compaction_integrity_binary_vector(logical_pk, explicit_test_ts, field_id, value_index=0):
    assert COMPACTION_INTEGRITY_VECTOR_DIM % 8 == 0
    byte_count = COMPACTION_INTEGRITY_VECTOR_DIM // 8
    fingerprint = (logical_pk * 40503 + explicit_test_ts * 251 + field_id * 17 + value_index) & (
        (1 << COMPACTION_INTEGRITY_VECTOR_DIM) - 1
    )
    return fingerprint.to_bytes(byte_count, byteorder="little")


def _compaction_integrity_float16_vector(logical_pk, explicit_test_ts, field_id, value_index=0):
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
        elif element_index == 4:
            mantissa = value_index & 0x03FF
        else:
            mantissa = (
                logical_pk * 17 + explicit_test_ts * 13 + field_id * 7 + value_index * 19 + element_index
            ) & 0x03FF
        words.append(0x3C00 | mantissa)
    return struct.pack(f"<{len(words)}H", *words)


def _compaction_integrity_bfloat16_vector(logical_pk, explicit_test_ts, field_id, value_index=0):
    words = []
    for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM):
        if element_index < 3:
            mantissa = (logical_pk >> (element_index * 7)) & 0x007F
        elif element_index == 3:
            mantissa = explicit_test_ts & 0x007F
        elif element_index == 4:
            mantissa = field_id & 0x007F
        elif element_index == 5:
            mantissa = value_index & 0x007F
        else:
            mantissa = (
                logical_pk * 29 + explicit_test_ts * 11 + field_id * 5 + value_index * 23 + element_index
            ) & 0x007F
        words.append(0x3F80 | mantissa)
    return struct.pack(f"<{len(words)}H", *words)


def _compaction_integrity_int8_vector(logical_pk, explicit_test_ts, field_id, value_index=0):
    values = [
        logical_pk & 0xFF,
        (logical_pk >> 8) & 0xFF,
        (logical_pk >> 16) & 0xFF,
        explicit_test_ts & 0xFF,
        field_id & 0xFF,
        value_index & 0xFF,
    ]
    values.extend(
        (logical_pk * 31 + explicit_test_ts * 17 + field_id * 13 + value_index * 29 + element_index) & 0xFF
        for element_index in range(6, COMPACTION_INTEGRITY_VECTOR_DIM)
    )
    return bytes(values)


def _compaction_integrity_text(signature, logical_pk):
    if logical_pk % COMPACTION_INTEGRITY_LOB_ROW_INTERVAL != 0:
        return f"{signature}|inline"
    target_length = COMPACTION_INTEGRITY_TEXT_INLINE_THRESHOLD + 257
    assert target_length > len(signature) + 1
    return f"{signature}|" + "L" * (target_length - len(signature) - 1)


def _build_compaction_integrity_struct_array(run_id, logical_pk, explicit_test_ts):
    parent_field_id = COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD_ID
    if _compaction_integrity_is_null(logical_pk, parent_field_id):
        return None
    if (logical_pk + parent_field_id) % 20 == 1:
        return []

    array_length = 1 + (logical_pk + parent_field_id) % COMPACTION_INTEGRITY_STRUCT_ARRAY_MAX_CAPACITY
    struct_array = []
    for element_index in range(array_length):
        field_ids = COMPACTION_INTEGRITY_STRUCT_SUBFIELD_IDS
        vector_index_base = element_index * COMPACTION_INTEGRITY_VECTOR_DIM
        bfloat16_bits = _compaction_integrity_bfloat16_vector(
            logical_pk,
            explicit_test_ts,
            field_ids["struct_bfloat16_vector"],
            value_index=element_index,
        )
        struct_array.append(
            {
                "struct_bool": bool((logical_pk + explicit_test_ts + element_index) % 2),
                "struct_int8": _compaction_integrity_signed_integer(
                    logical_pk, explicit_test_ts, field_ids["struct_int8"] + element_index * 131, 8
                ),
                "struct_int16": _compaction_integrity_signed_integer(
                    logical_pk, explicit_test_ts, field_ids["struct_int16"] + element_index * 131, 16
                ),
                "struct_int32": _compaction_integrity_signed_integer(
                    logical_pk, explicit_test_ts, field_ids["struct_int32"] + element_index * 131, 32
                ),
                "struct_int64": logical_pk * 257 + explicit_test_ts * 17 + element_index,
                "struct_float": _compaction_integrity_float32(
                    logical_pk,
                    explicit_test_ts,
                    field_ids["struct_float"],
                    element_index,
                ),
                "struct_double": _compaction_integrity_float64(
                    logical_pk,
                    explicit_test_ts,
                    field_ids["struct_double"] + element_index * 131,
                ),
                "struct_varchar": (
                    f"{_compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, field_ids['struct_varchar'])}"
                    f"|element={element_index}|{'s' * ((logical_pk + element_index) % 29)}"
                ),
                "struct_float_vector": [
                    _compaction_integrity_float32(
                        logical_pk,
                        explicit_test_ts,
                        field_ids["struct_float_vector"],
                        vector_index_base + vector_element_index,
                    )
                    for vector_element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM)
                ],
                "struct_binary_vector": _compaction_integrity_binary_vector(
                    logical_pk,
                    explicit_test_ts,
                    field_ids["struct_binary_vector"],
                    value_index=element_index,
                ),
                "struct_float16_vector": np.frombuffer(
                    _compaction_integrity_float16_vector(
                        logical_pk,
                        explicit_test_ts,
                        field_ids["struct_float16_vector"],
                        value_index=element_index,
                    ),
                    dtype=np.float16,
                ).copy(),
                "struct_bfloat16_vector": np.frombuffer(bfloat16_bits, dtype=np.uint16).view(cf.bfloat16).copy(),
                "struct_int8_vector": np.frombuffer(
                    _compaction_integrity_int8_vector(
                        logical_pk,
                        explicit_test_ts,
                        field_ids["struct_int8_vector"],
                        value_index=element_index,
                    ),
                    dtype=np.int8,
                ).copy(),
            }
        )
    return struct_array


def _compaction_integrity_bm25_token(logical_pk):
    return f"integrityanchor{logical_pk}x"


def _build_compaction_integrity_row(
    run_id,
    logical_pk,
    explicit_test_ts,
    primary_key_type,
    include_text,
    include_bm25_control=False,
    include_struct_array=True,
):
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
        COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD: _build_compaction_integrity_struct_array(
            run_id,
            logical_pk,
            explicit_test_ts,
        ),
        "sparse_vector": {
            element_index: _compaction_integrity_float32(logical_pk, explicit_test_ts, 16, element_index)
            for element_index in (0, 31, 63, 127)
        },
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
            else _compaction_integrity_binary_vector(logical_pk, explicit_test_ts, 24)
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
    if not include_struct_array:
        row.pop(COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD)
        for field_name in COMPACTION_INTEGRITY_NULLABLE_VECTOR_FIELDS:
            row.pop(field_name)
        row.update(
            {
                "float_vector": [
                    _compaction_integrity_float32(logical_pk, explicit_test_ts, 12, element_index)
                    for element_index in range(COMPACTION_INTEGRITY_VECTOR_DIM)
                ],
                "binary_vector": _compaction_integrity_binary_vector(logical_pk, explicit_test_ts, 13),
                "float16_vector": _compaction_integrity_float16_vector(logical_pk, explicit_test_ts, 14),
                "bfloat16_vector": _compaction_integrity_bfloat16_vector(logical_pk, explicit_test_ts, 15),
                "int8_vector": _compaction_integrity_int8_vector(logical_pk, explicit_test_ts, 17),
            }
        )
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
    if include_bm25_control:
        bm25_signature = _compaction_integrity_signature(run_id, logical_pk, explicit_test_ts, 41)
        row[COMPACTION_INTEGRITY_BM25_TEXT_FIELD] = (
            f"{_compaction_integrity_bm25_token(logical_pk)} stable bm25 control {bm25_signature}"
        )
        expected[COMPACTION_INTEGRITY_BM25_TEXT_FIELD] = row[COMPACTION_INTEGRITY_BM25_TEXT_FIELD]
    return row, expected


def _length_prefixed_bytes(values):
    encoded = bytearray()
    for value in values:
        encoded.extend(struct.pack("<I", len(value)))
        encoded.extend(value)
    return bytes(encoded)


def _canonical_compaction_integrity_struct_subfield(field_name, value):
    if value is None:
        return b"\x00"
    if field_name == "struct_bool":
        encoded = struct.pack("<?", bool(value))
    elif field_name == "struct_int8":
        encoded = struct.pack("<b", int(value))
    elif field_name == "struct_int16":
        encoded = struct.pack("<h", int(value))
    elif field_name == "struct_int32":
        encoded = struct.pack("<i", int(value))
    elif field_name == "struct_int64":
        encoded = struct.pack("<q", int(value))
    elif field_name == "struct_float":
        encoded = struct.pack("<f", float(value))
    elif field_name == "struct_double":
        encoded = struct.pack("<d", float(value))
    elif field_name == "struct_varchar":
        encoded = value.encode("utf-8")
    elif field_name == "struct_float_vector":
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    elif field_name == "struct_binary_vector":
        if isinstance(value, list):
            if len(value) == 1:
                value = value[0]
            else:
                encoded = b"invalid_binary_vector_list" + _length_prefixed_bytes([bytes(item) for item in value])
                return b"\x01" + struct.pack("<I", len(encoded)) + encoded
        encoded = bytes(value)
    elif field_name == "struct_float16_vector":
        if isinstance(value, bytes | bytearray):
            encoded = bytes(value)
        else:
            encoded = np.asarray(value, dtype=np.float16).astype("<f2", copy=False).tobytes()
    elif field_name == "struct_bfloat16_vector":
        if isinstance(value, bytes | bytearray):
            encoded = bytes(value)
        else:
            array = np.asarray(value)
            if array.dtype == np.dtype(cf.bfloat16):
                array = array.view(np.uint16)
            else:
                array = array.astype(np.uint16, copy=False)
            encoded = array.astype("<u2", copy=False).tobytes()
    elif field_name == "struct_int8_vector":
        encoded = np.asarray(value, dtype=np.int8).tobytes()
    else:
        raise AssertionError(f"no canonical encoder for Struct Array subfield {field_name}")
    return b"\x01" + struct.pack("<I", len(encoded)) + encoded


def _canonical_compaction_integrity_struct_array(value):
    element_payloads = []
    for element_index, element in enumerate(value):
        if not isinstance(element, dict):
            element_payloads.append(
                b"invalid_struct_element"
                + struct.pack("<I", element_index)
                + type(element).__name__.encode("utf-8")
                + repr(element).encode("utf-8")
            )
            continue
        subfield_payloads = []
        subfield_payloads.append(
            _length_prefixed_bytes([str(field_name).encode("utf-8") for field_name in sorted(element)])
        )
        for field_name in COMPACTION_INTEGRITY_STRUCT_SUBFIELDS:
            subfield_payloads.append(field_name.encode("utf-8"))
            if field_name in element:
                subfield_payloads.append(b"\x01")
                subfield_payloads.append(
                    _canonical_compaction_integrity_struct_subfield(field_name, element[field_name])
                )
            else:
                subfield_payloads.append(b"\x00")
        element_payloads.append(_length_prefixed_bytes(subfield_payloads))
    return struct.pack("<I", len(value)) + _length_prefixed_bytes(element_payloads)


def _canonical_compaction_integrity_cell(field_name, value, primary_key_type):
    if value is None:
        return b"\x00"
    if field_name == "id" and primary_key_type == DataType.VARCHAR:
        encoded = value.encode("utf-8")
    elif field_name in {
        "id",
        "explicit_test_ts",
        "int64_value",
        "default_value",
        COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
    }:
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
    elif field_name in {
        "varchar_payload",
        COMPACTION_INTEGRITY_TEXT_FIELD,
        COMPACTION_INTEGRITY_BM25_TEXT_FIELD,
    }:
        encoded = value.encode("utf-8")
    elif field_name in {"json_payload", "dynamic_payload"}:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    elif field_name == "int64_array":
        encoded = struct.pack(f"<{len(value)}q", *(int(item) for item in value))
    elif field_name == "float_array":
        encoded = struct.pack(f"<{len(value)}f", *(float(item) for item in value))
    elif field_name == "string_array":
        encoded = _length_prefixed_bytes([item.encode("utf-8") for item in value])
    elif field_name == COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD:
        encoded = _canonical_compaction_integrity_struct_array(value)
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
        "_affected_primary_keys": affected_primary_keys,
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


def _compaction_integrity_successful_new_tasks(before_checkpoint, after_checkpoint, task_types=None):
    before_tasks = before_checkpoint.get("tasks", {}) if before_checkpoint else {}
    if isinstance(task_types, str):
        expected_types = {task_types.lower()}
    elif task_types:
        expected_types = {str(expected_type).lower() for expected_type in task_types}
    else:
        expected_types = None
    return {
        task_id: task
        for task_id, task in after_checkpoint["tasks"].items()
        if task_id not in before_tasks
        and task["state"] in COMPACTION_INTEGRITY_SUCCESS_TASK_STATES
        and not task["failure_reason"]
        and (expected_types is None or str(task["type"]).lower() in expected_types)
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


def _compaction_integrity_task_reaches_active_frontier(task, checkpoint):
    active_segment_ids = set(checkpoint["active"])
    target_segment_ids = set(task["targets"])
    if target_segment_ids.intersection(active_segment_ids):
        return True

    children = {}
    for target_id, segment in checkpoint["all"].items():
        for source_id in segment["compaction_from"]:
            children.setdefault(source_id, set()).add(target_id)
    return any(
        _compaction_integrity_descendants(target_id, children).intersection(active_segment_ids)
        for target_id in target_segment_ids
    )


def _compaction_integrity_new_roots(before_checkpoint, after_checkpoint):
    new_segment_ids = set(after_checkpoint["all"]) - set(before_checkpoint["all"])
    return {
        segment_id
        for segment_id in new_segment_ids
        if after_checkpoint["all"][segment_id]["num_rows"] != 0
        and not after_checkpoint["all"][segment_id]["compaction_from"]
    }


def _compaction_integrity_participating_roots(after_checkpoint, new_roots, round_edges):
    children = {}
    for target_id, target in after_checkpoint["all"].items():
        for source_id in target["compaction_from"]:
            children.setdefault(source_id, set()).add(target_id)

    edge_sources = {source_id for source_id, _ in round_edges}
    return {
        root_id
        for root_id in new_roots
        if ({root_id} | _compaction_integrity_descendants(root_id, children)).intersection(edge_sources)
    }


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
        new_roots = _compaction_integrity_new_roots(before_checkpoint, after_checkpoint)
        assert new_roots, f"round created no observable flushed input segments: {new_segment_ids}"
        participating_roots = _compaction_integrity_participating_roots(after_checkpoint, new_roots, round_edges)
        assert participating_roots, (
            f"no attributable new input segment participated in the round lineage transition: "
            f"new_roots={new_roots}, round_edges={round_edges}"
        )
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
    transition_policy="lineage",
    required_task_types=None,
    expected_storage_version=None,
    timeout=120,
):
    assert transition_policy in {"stable", "in_place_schema_bump", "replacement_schema_bump", "lineage"}
    if transition_policy in {"in_place_schema_bump", "replacement_schema_bump"}:
        assert required_task_types, f"{transition_policy} requires compaction task types"
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
                (task_id, task["state"], task["failure_reason"]) for task_id, task in new_task_failures.items()
            )
        storage_versions = {segment["storage_version"] for segment in active_segments.values()}
        last_observation = {
            "all": all_segments,
            "active": active_segments,
            "serving": serving_segments,
            "tasks": tasks,
            "storage_versions": storage_versions,
        }
        requires_graph_transition = transition_policy in {"lineage", "replacement_schema_bump"}
        has_graph_transition = not requires_graph_transition
        if requires_graph_transition:
            try:
                _assert_compaction_integrity_graph_transition(
                    before_checkpoint,
                    last_observation,
                    require_new_inputs=require_new_inputs,
                )
                has_graph_transition = True
            except AssertionError:
                pass
        successful_new_tasks = _compaction_integrity_successful_new_tasks(
            before_checkpoint,
            last_observation,
            task_types=required_task_types,
        )
        successful_new_tasks = {
            task_id: task
            for task_id, task in successful_new_tasks.items()
            if _compaction_integrity_task_reaches_active_frontier(task, last_observation)
        }
        task_requirement_satisfied = required_task_types is None or bool(successful_new_tasks)
        tasks_terminal = all(task["state"] in COMPACTION_INTEGRITY_TERMINAL_TASK_STATES for task in tasks.values())
        active_stable = (bool(active_segments) or expected_rows == 0) and all(
            segment["state"] in COMPACTION_INTEGRITY_STABLE_STATES and segment["is_sorted"]
            for segment in active_segments.values()
        )
        serving_matches_active = set(serving_segments) == set(active_segments)
        serving_sealed = all(segment["state"] == "Sealed" for segment in serving_segments.values())
        active_rows = sum(segment["num_rows"] for segment in active_segments.values())
        row_count_matches = active_rows == expected_rows
        storage_version_valid = (
            not storage_versions
            if expected_rows == 0
            else (len(storage_versions) == 1 and storage_versions.issubset({2, 3}))
        )
        if expected_storage_version is not None and expected_rows != 0:
            storage_version_valid = storage_versions == {expected_storage_version}
        ready = (
            tasks_terminal
            and active_stable
            and serving_matches_active
            and serving_sealed
            and row_count_matches
            and storage_version_valid
            and has_graph_transition
            and task_requirement_satisfied
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
                f"task_requirement_satisfied={task_requirement_satisfied} "
                f"new_successful_task_ids={sorted(successful_new_tasks)} transition_policy={transition_policy} "
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
    raise AssertionError(f"collection did not reach a stable {transition_policy} checkpoint: {last_observation}")


def _assert_compaction_integrity_dataset(
    client,
    collection_name,
    expected_by_pk,
    output_fields,
    primary_key_type,
):
    assert COMPACTION_INTEGRITY_QUERY_BATCH_SIZE > 0
    log.info(
        f"data integrity validation start collection={collection_name} rows={len(expected_by_pk)} "
        f"fields={len(output_fields)} output_fields={output_fields}"
    )
    query_filter = "id >= 0" if primary_key_type == DataType.INT64 else 'id != ""'
    iterator = client.query_iterator(
        collection_name,
        batch_size=COMPACTION_INTEGRITY_QUERY_BATCH_SIZE,
        filter=query_filter,
        output_fields=output_fields,
        consistency_level="Strong",
    )
    seen_primary_keys = set()
    actual_count = 0
    batch_index = 0
    validated_cell_count = 0
    corrupted_batch_count = 0
    corrupted_row_count = 0
    corrupted_cell_count = 0
    affected_primary_keys = set()
    issue_counts = {}
    field_mismatch_counts = {}
    corruption_samples = []
    total_sample_candidates = 0
    try:
        while True:
            batch = iterator.next()
            if not batch:
                break
            batch_index += 1
            actual_count += len(batch)
            validated_cell_count += sum(
                field_name in actual
                for actual in batch
                if actual.get("id") in expected_by_pk
                for field_name in output_fields
            )
            corruption_summary = _compaction_integrity_batch_corruption_summary(
                batch_index,
                batch,
                expected_by_pk,
                output_fields,
                primary_key_type,
                seen_primary_keys,
            )
            if corruption_summary is not None:
                corrupted_batch_count += 1
                corrupted_row_count += corruption_summary["corrupted_row_count"]
                corrupted_cell_count += corruption_summary["corrupted_cell_count"]
                affected_primary_keys.update(corruption_summary.pop("_affected_primary_keys"))
                total_sample_candidates += corruption_summary["sample_count"] + corruption_summary["samples_truncated"]
                for issue, count in corruption_summary["issue_counts"].items():
                    issue_counts[issue] = issue_counts.get(issue, 0) + count
                for field_name, count in corruption_summary["field_mismatch_counts"].items():
                    field_mismatch_counts[field_name] = field_mismatch_counts.get(field_name, 0) + count
                for sample in corruption_summary["samples"]:
                    if len(corruption_samples) >= COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT:
                        break
                    corruption_samples.append({"batch": batch_index, **sample})
            log.info(
                f"data integrity validation progress collection={collection_name} batch={batch_index} "
                f"batch_rows={len(batch)} validated_rows={actual_count}/{len(expected_by_pk)} "
                f"corrupted_rows={0 if corruption_summary is None else corruption_summary['corrupted_row_count']} "
                f"corrupted_cells={0 if corruption_summary is None else corruption_summary['corrupted_cell_count']}"
            )
    finally:
        iterator.close()

    expected_primary_keys = set(expected_by_pk)
    missing_primary_keys = expected_primary_keys - seen_primary_keys
    unexpected_primary_keys = seen_primary_keys - expected_primary_keys
    affected_primary_keys.update(missing_primary_keys)
    affected_primary_keys.update(unexpected_primary_keys)
    if missing_primary_keys:
        issue_counts["missing_expected_primary_key"] = len(missing_primary_keys)
        corrupted_row_count += len(missing_primary_keys)
    validation_evidence = {
        "expected_rows": len(expected_by_pk),
        "retrieved_rows": actual_count,
        "expected_pk_digest": _compaction_integrity_pk_digest(expected_primary_keys, primary_key_type),
        "retrieved_pk_digest": _compaction_integrity_pk_digest(seen_primary_keys, primary_key_type),
        "validated_field_count": len(output_fields),
        "validated_cell_count": validated_cell_count,
        "query_batch_count": batch_index,
    }
    if issue_counts or actual_count != len(expected_by_pk) or seen_primary_keys != expected_primary_keys:
        corruption_summary = {
            **validation_evidence,
            "corrupted_batch_count": corrupted_batch_count,
            "corrupted_row_count": corrupted_row_count,
            "corrupted_cell_count": corrupted_cell_count,
            "affected_primary_key_count": len(affected_primary_keys),
            "affected_primary_key_sample": sorted(affected_primary_keys)[:COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT],
            "missing_primary_key_count": len(missing_primary_keys),
            "missing_primary_key_sample": sorted(missing_primary_keys)[:COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT],
            "unexpected_primary_key_count": len(unexpected_primary_keys),
            "unexpected_primary_key_sample": sorted(unexpected_primary_keys)[
                :COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT
            ],
            "issue_counts": dict(sorted(issue_counts.items())),
            "field_mismatch_counts": dict(sorted(field_mismatch_counts.items())),
            "sample_count": len(corruption_samples),
            "samples_truncated": total_sample_candidates - len(corruption_samples),
            "samples": corruption_samples,
        }
        _log_compaction_integrity_evidence(
            "data_integrity_dataset_corruption",
            collection=collection_name,
            **corruption_summary,
        )
        raise AssertionError(
            "data corruption detected in complete dataset: "
            f"{json.dumps(corruption_summary, sort_keys=True, separators=(',', ':'))}"
        )
    _log_compaction_integrity_evidence(
        "data_integrity_dataset_validated",
        collection=collection_name,
        **validation_evidence,
    )
    return validation_evidence


def _compaction_integrity_frontier_signature(checkpoint):
    return (
        tuple(
            sorted(
                (
                    segment_id,
                    segment["state"],
                    segment["num_rows"],
                    segment["is_sorted"],
                    segment["storage_version"],
                    segment["partition_id"],
                    segment["insert_channel"],
                    segment["compaction_from"],
                )
                for segment_id, segment in checkpoint["active"].items()
            )
        ),
        tuple(
            sorted(
                (
                    segment_id,
                    segment["state"],
                    segment["num_rows"],
                    segment["partition_id"],
                    segment["insert_channel"],
                )
                for segment_id, segment in checkpoint["serving"].items()
            )
        ),
    )


def _compaction_integrity_checkpoint_audit(checkpoint):
    return {
        "all_segments": [checkpoint["all"][segment_id] for segment_id in sorted(checkpoint["all"])],
        "active_segments": [checkpoint["active"][segment_id] for segment_id in sorted(checkpoint["active"])],
        "serving_segments": [checkpoint["serving"][segment_id] for segment_id in sorted(checkpoint["serving"])],
        "compaction_tasks": [checkpoint["tasks"][task_id] for task_id in sorted(checkpoint["tasks"])],
        "storage_versions": sorted(checkpoint["storage_versions"]),
    }


def _assert_compaction_integrity_bm25(
    client,
    collection_name,
    token_to_pk,
    anns_fields,
    baseline_by_token=None,
):
    assert anns_fields, "BM25 validation requires at least one function output field"
    assert COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE > 0
    token_items = list(token_to_pk.items())
    observed_by_field = {field_name: {} for field_name in anns_fields}
    failure_counts = {}
    failure_samples = []
    score_delta_stats = {}

    def record_failure(kind, **details):
        failure_counts[kind] = failure_counts.get(kind, 0) + 1
        if len(failure_samples) < 20:
            failure_samples.append({"kind": kind, **details})

    def record_score_mismatch(kind, comparison, token, actual_score, expected_score):
        abs_delta = abs(actual_score - expected_score)
        record_failure(
            kind,
            comparison=comparison,
            token=token,
            actual_score=actual_score,
            expected_score=expected_score,
            abs_delta=abs_delta,
        )
        stats = score_delta_stats.setdefault(
            comparison,
            {
                "count": 0,
                "sum_abs_delta": 0.0,
                "max_abs_delta": 0.0,
                "max_delta_token": None,
                "max_delta_actual_score": None,
                "max_delta_expected_score": None,
            },
        )
        stats["count"] += 1
        stats["sum_abs_delta"] += abs_delta
        if abs_delta >= stats["max_abs_delta"]:
            stats["max_abs_delta"] = abs_delta
            stats["max_delta_token"] = token
            stats["max_delta_actual_score"] = actual_score
            stats["max_delta_expected_score"] = expected_score

    for field_name in anns_fields:
        for batch_start in range(0, len(token_items), COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE):
            batch_items = token_items[batch_start : batch_start + COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE]
            search_results = client.search(
                collection_name=collection_name,
                data=[token for token, _ in batch_items],
                anns_field=field_name,
                limit=1,
                output_fields=["id"],
                search_params={"metric_type": "BM25"},
                consistency_level="Strong",
            )
            if len(search_results) != len(batch_items):
                record_failure(
                    "nq_mismatch",
                    field=field_name,
                    batch_start=batch_start,
                    actual=len(search_results),
                    expected=len(batch_items),
                )
            for offset, (token, expected_pk) in enumerate(batch_items):
                if offset >= len(search_results):
                    record_failure("missing_query_result", field=field_name, token=token)
                    continue
                hits = search_results[offset]
                if len(hits) != 1:
                    record_failure(
                        "hit_count_mismatch",
                        field=field_name,
                        token=token,
                        actual=len(hits),
                        expected=1,
                    )
                if not hits:
                    continue
                hit = hits[0]
                actual_pk = hit["id"]
                score = float(hit["distance"])
                if actual_pk != expected_pk:
                    record_failure(
                        "pk_mismatch",
                        field=field_name,
                        token=token,
                        actual_pk=actual_pk,
                        expected_pk=expected_pk,
                    )
                if baseline_by_token is not None:
                    baseline = baseline_by_token.get(token)
                    if baseline is None:
                        record_failure("missing_baseline", field=field_name, token=token)
                    else:
                        baseline_pk, baseline_score = baseline
                        if actual_pk != baseline_pk:
                            record_failure(
                                "baseline_pk_mismatch",
                                field=field_name,
                                token=token,
                                actual_pk=actual_pk,
                                baseline_pk=baseline_pk,
                            )
                        if abs(score - baseline_score) >= 1e-6:
                            record_score_mismatch(
                                "baseline_score_mismatch",
                                f"{field_name}_vs_baseline",
                                token,
                                score,
                                baseline_score,
                            )
                observed_by_field[field_name][token] = (actual_pk, score)
            log.info(
                f"BM25 integrity validation progress collection={collection_name} field={field_name} "
                f"validated_queries={min(batch_start + len(batch_items), len(token_items))}/{len(token_items)}"
            )

    control = observed_by_field[anns_fields[0]]
    for field_name in anns_fields[1:]:
        for token in token_to_pk:
            if token not in control or token not in observed_by_field[field_name]:
                continue
            control_pk, control_score = control[token]
            actual_pk, actual_score = observed_by_field[field_name][token]
            if actual_pk != control_pk:
                record_failure(
                    "cross_field_pk_mismatch",
                    control_field=anns_fields[0],
                    field=field_name,
                    token=token,
                    control_pk=control_pk,
                    actual_pk=actual_pk,
                )
            if abs(actual_score - control_score) >= 1e-6:
                record_score_mismatch(
                    "cross_field_score_mismatch",
                    f"{field_name}_vs_{anns_fields[0]}",
                    token,
                    actual_score,
                    control_score,
                )

    if failure_counts:
        score_delta_summary = {
            comparison: {
                "count": stats["count"],
                "mean_abs_delta": stats["sum_abs_delta"] / stats["count"],
                "max_abs_delta": stats["max_abs_delta"],
                "max_delta_token": stats["max_delta_token"],
                "max_delta_actual_score": stats["max_delta_actual_score"],
                "max_delta_expected_score": stats["max_delta_expected_score"],
            }
            for comparison, stats in score_delta_stats.items()
        }
        failure_summary = {
            "failure_counts": failure_counts,
            "score_delta_summary": score_delta_summary,
            "samples": failure_samples,
        }
        _log_compaction_integrity_evidence(
            "bm25_data_validation_failed",
            collection=collection_name,
            fields=list(anns_fields),
            query_count=len(token_items),
            **failure_summary,
        )
        pytest.fail(f"BM25 integrity validation found mismatches: {json.dumps(failure_summary, sort_keys=True)}")

    _log_compaction_integrity_evidence(
        "bm25_data_validated",
        collection=collection_name,
        fields=list(anns_fields),
        query_count=len(token_items),
        first_token=token_items[0][0] if token_items else None,
        last_token=token_items[-1][0] if token_items else None,
    )
    return control


def _assert_compaction_integrity_fenced_dataset(
    client,
    collection_name,
    expected_by_pk,
    output_fields,
    primary_key_type,
    expected_storage_version,
    additional_validator=None,
    timeout=300,
):
    deadline = time.time() + timeout
    attempt = 0
    empty_checkpoint = {"all": {}, "active": {}, "serving": {}, "tasks": {}, "storage_versions": set()}
    while time.time() < deadline:
        attempt += 1
        remaining = max(1, deadline - time.time())
        before = _wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=len(expected_by_pk),
            before_checkpoint=empty_checkpoint,
            require_new_inputs=False,
            transition_policy="stable",
            expected_storage_version=expected_storage_version,
            timeout=remaining,
        )
        validation_error = None
        data_validation_evidence = None
        validation_result = None
        try:
            data_validation_evidence = _assert_compaction_integrity_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
            )
            if additional_validator is not None:
                validation_result = additional_validator()
        except Exception as error:  # Re-evaluate only when a concurrent handoff invalidated this attempt.
            validation_error = error

        remaining = max(1, deadline - time.time())
        after = _wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=len(expected_by_pk),
            before_checkpoint=empty_checkpoint,
            require_new_inputs=False,
            transition_policy="stable",
            expected_storage_version=expected_storage_version,
            timeout=remaining,
        )
        before_signature = _compaction_integrity_frontier_signature(before)
        after_signature = _compaction_integrity_frontier_signature(after)
        if before_signature == after_signature:
            if validation_error is not None:
                raise validation_error
            _log_compaction_integrity_evidence(
                "fenced_data_validation_passed",
                collection=collection_name,
                attempt=attempt,
                rows=len(expected_by_pk),
                before_frontier=before_signature,
                after_frontier=after_signature,
                before_checkpoint=_compaction_integrity_checkpoint_audit(before),
                after_checkpoint=_compaction_integrity_checkpoint_audit(after),
                data_validation=data_validation_evidence,
            )
            log.info(
                f"fenced data integrity validation complete collection={collection_name} attempt={attempt} "
                f"active_ids={sorted(after['active'])} serving_ids={sorted(after['serving'])}"
            )
            return after, validation_result, data_validation_evidence
        _log_compaction_integrity_evidence(
            "fenced_validation_retried",
            collection=collection_name,
            attempt=attempt,
            before_active_ids=sorted(before["active"]),
            after_active_ids=sorted(after["active"]),
            before_serving_ids=sorted(before["serving"]),
            after_serving_ids=sorted(after["serving"]),
            validation_error=None if validation_error is None else repr(validation_error),
        )
    raise AssertionError(f"collection frontier did not remain stable around full validation: {collection_name}")


def _compaction_integrity_schema_snapshot(client, collection_name):
    description = client.describe_collection(collection_name)
    return {
        "schema_version": description["schema_version"],
        "fields": {
            field["name"]: {
                "field_id": field["field_id"],
                "type": str(field["type"]),
            }
            for field in description["fields"]
        },
        "functions": {
            function["name"]: {
                "id": function["id"],
                "input_field_ids": tuple(function.get("input_field_ids", [])),
                "output_field_ids": tuple(function.get("output_field_ids", [])),
            }
            for function in description.get("functions", [])
        },
        "indexes": tuple(sorted(client.list_indexes(collection_name))),
    }


def _log_compaction_integrity_checkpoint(stage, collection_name, checkpoint, schema_snapshot, **extra):
    _log_compaction_integrity_evidence(
        "ddl_lifecycle_checkpoint",
        stage=stage,
        collection=collection_name,
        schema=schema_snapshot,
        **_compaction_integrity_checkpoint_audit(checkpoint),
        **extra,
    )


def test_compaction_integrity_corruption_scans_complete_dataset_then_summarizes():
    output_fields = ["id", "int64_value", "float_vector"]
    expected_rows = [
        {"id": 1, "int64_value": 10, "float_vector": [1.0, 2.0]},
        {"id": 2, "int64_value": 20, "float_vector": [3.0, 4.0]},
        {"id": 3, "int64_value": 30, "float_vector": [5.0, 6.0]},
        {"id": 4, "int64_value": 40, "float_vector": [7.0, 8.0]},
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
            if self.next_calls == 1:
                return [
                    {"id": 1, "int64_value": 10, "float_vector": [1.0, 9.0]},
                    {"id": 2, "int64_value": 21, "float_vector": [8.0, 4.0]},
                ]
            if self.next_calls == 2:
                return [
                    {"id": 3, "int64_value": 31, "float_vector": [5.0, 6.0]},
                    {"id": 4, "int64_value": 40, "float_vector": [7.0, 8.0]},
                ]
            return []

        def close(self):
            self.closed = True

    class FakeClient:
        def __init__(self):
            self.iterator = FakeIterator()

        def query_iterator(self, *args, **kwargs):
            return self.iterator

    client = FakeClient()
    with pytest.raises(AssertionError, match="data corruption detected in complete dataset") as error:
        _assert_compaction_integrity_dataset(
            client,
            "corruption_summary_test",
            expected_by_pk,
            output_fields,
            DataType.INT64,
        )

    summary = json.loads(str(error.value).split(": ", 1)[1])
    assert summary["query_batch_count"] == 2
    assert summary["corrupted_batch_count"] == 2
    assert summary["corrupted_row_count"] == 3
    assert summary["corrupted_cell_count"] == 4
    assert summary["affected_primary_key_count"] == 3
    assert summary["issue_counts"] == {"canonical_value_mismatch": 4}
    assert summary["field_mismatch_counts"] == {"float_vector": 2, "int64_value": 2}
    assert {(sample["pk"], sample["field"], sample.get("element_index")) for sample in summary["samples"]} == {
        (1, "float_vector", 1),
        (2, "int64_value", None),
        (2, "float_vector", 0),
        (3, "int64_value", None),
    }
    assert client.iterator.next_calls == 3
    assert client.iterator.closed


def test_compaction_integrity_struct_array_canonical_bytes_are_recursive_and_ordered():
    struct_array = _build_compaction_integrity_struct_array("canonical_test", 2, 1)
    assert struct_array is not None and len(struct_array) > 1

    canonical = _canonical_compaction_integrity_cell(
        COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
        struct_array,
        DataType.INT64,
    )
    reordered_keys = [{key: element[key] for key in reversed(element)} for element in struct_array]
    assert (
        _canonical_compaction_integrity_cell(
            COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
            reordered_keys,
            DataType.INT64,
        )
        == canonical
    )

    reversed_elements = list(reversed(struct_array))
    assert (
        _canonical_compaction_integrity_cell(
            COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
            reversed_elements,
            DataType.INT64,
        )
        != canonical
    )
    assert _canonical_compaction_integrity_cell(
        COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
        None,
        DataType.INT64,
    ) != _canonical_compaction_integrity_cell(
        COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
        [],
        DataType.INT64,
    )

    round_values = [
        _build_compaction_integrity_struct_array("population_test", logical_pk, 1)
        for logical_pk in range(COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH)
    ]
    assert sum(value is None for value in round_values) == 50
    assert sum(value == [] for value in round_values) == 50
    assert {len(value) for value in round_values if value} == {1, 2, 3, 4}

    missing_subfield = [dict(element) for element in struct_array]
    del missing_subfield[0]["struct_int16"]
    assert (
        _canonical_compaction_integrity_cell(
            COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
            missing_subfield,
            DataType.INT64,
        )
        != canonical
    )

    corrupted = [dict(element) for element in struct_array]
    corrupted[0]["struct_float_vector"] = list(corrupted[0]["struct_float_vector"])
    corrupted[0]["struct_float_vector"][COMPACTION_INTEGRITY_VECTOR_DIM - 1] += 1.0
    assert (
        _canonical_compaction_integrity_cell(
            COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
            corrupted,
            DataType.INT64,
        )
        != canonical
    )


def test_compaction_integrity_bulk_writer_adapter_preserves_the_oracle_boundary():
    int8_bytes = bytes(range(COMPACTION_INTEGRITY_VECTOR_DIM))
    original_row = {
        COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD: None,
        "nullable_int8_vector": int8_bytes,
        "nullable_float_vector": None,
        "nullable_sparse_vector": None,
    }
    original_expected = dict(original_row)

    writer_row, writer_expected = _prepare_compaction_integrity_bulk_writer_row(
        original_row,
        original_expected,
        logical_pk=1000,
        explicit_test_ts=1,
    )

    assert original_row[COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD] is None
    assert writer_row[COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD] == []
    assert writer_expected[COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD] == []
    assert writer_row["nullable_int8_vector"].dtype == np.int8
    assert writer_row["nullable_int8_vector"].tobytes() == int8_bytes
    assert writer_expected["nullable_int8_vector"] == int8_bytes
    assert writer_row["nullable_float_vector"] is None
    expected_sparse_fingerprint = {0: _compaction_integrity_float32(1000, 1, 25, 0)}
    assert writer_row["nullable_sparse_vector"] == expected_sparse_fingerprint
    assert writer_expected["nullable_sparse_vector"] == expected_sparse_fingerprint


def test_compaction_integrity_ddl_vector_profile_covers_all_top_level_types_without_struct_array():
    output_fields = _compaction_integrity_output_fields(
        include_struct_array=False,
        include_text=True,
        include_bm25_control=True,
    )
    assert COMPACTION_INTEGRITY_VECTOR_DIM == 16
    assert COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD not in output_fields
    assert not COMPACTION_INTEGRITY_NULLABLE_VECTOR_FIELDS.intersection(output_fields)
    assert all(output_fields.count(field_name) == 1 for field_name in COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS)

    row, expected = _build_compaction_integrity_row(
        "ddl_vector_profile_test",
        logical_pk=5000,
        explicit_test_ts=1,
        primary_key_type=DataType.INT64,
        include_text=True,
        include_bm25_control=True,
        include_struct_array=False,
    )
    assert all(
        field_name in row and field_name in expected for field_name in COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS
    )
    assert len(row["float_vector"]) == COMPACTION_INTEGRITY_VECTOR_DIM
    assert len(row["binary_vector"]) == COMPACTION_INTEGRITY_VECTOR_DIM // 8
    assert len(row["float16_vector"]) == COMPACTION_INTEGRITY_VECTOR_DIM * 2
    assert len(row["bfloat16_vector"]) == COMPACTION_INTEGRITY_VECTOR_DIM * 2
    assert len(row["int8_vector"]) == COMPACTION_INTEGRITY_VECTOR_DIM
    assert len(row["sparse_vector"]) == 4


def test_compaction_integrity_task_retry_accepts_new_success_after_old_failure():
    failed = {
        "task_id": 1,
        "type": COMPACTION_INTEGRITY_BUMP_TASK_TYPE,
        "state": "cleaned",
        "failure_reason": "first attempt failed",
    }
    succeeded = {
        "task_id": 2,
        "type": "MixCompaction",
        "state": "completed",
        "failure_reason": "",
    }
    before = {"tasks": {1: failed}}
    after = {"tasks": {1: failed, 2: succeeded}}

    assert _compaction_integrity_successful_new_tasks(
        before,
        after,
        task_types=COMPACTION_INTEGRITY_SCHEMA_REWRITE_TASK_TYPES,
    ) == {2: succeeded}


def test_compaction_integrity_schema_rewrite_task_must_reach_active_frontier():
    checkpoint = {
        "active": {30: {}},
        "all": {
            20: {"compaction_from": ()},
            30: {"compaction_from": (20,)},
            40: {"compaction_from": ()},
        },
    }

    assert _compaction_integrity_task_reaches_active_frontier({"targets": (30,)}, checkpoint)
    assert _compaction_integrity_task_reaches_active_frontier({"targets": (20,)}, checkpoint)
    assert not _compaction_integrity_task_reaches_active_frontier({"targets": (40,)}, checkpoint)


def test_compaction_integrity_round_requires_an_attributable_new_root_to_participate():
    before = {
        "all": {1: {"state": "Flushed", "num_rows": 100, "compaction_from": ()}},
        "active": {1: {}},
    }
    after = {
        "all": {
            1: {"state": "Dropped", "num_rows": 100, "compaction_from": ()},
            2: {"state": "Flushed", "num_rows": 100, "compaction_from": ()},
            3: {"state": "Flushed", "num_rows": 100, "compaction_from": (1,)},
        },
        "active": {2: {}, 3: {}},
    }

    with pytest.raises(AssertionError, match="no attributable new input segment participated"):
        _assert_compaction_integrity_graph_transition(before, after)

    after["all"][2]["state"] = "Dropped"
    after["all"][4] = {"state": "Flushed", "num_rows": 100, "compaction_from": (2,)}
    after["active"] = {3: {}, 4: {}}
    assert _assert_compaction_integrity_graph_transition(before, after) == {(1, 3), (2, 4)}


def test_compaction_integrity_empty_round_start_is_a_stable_checkpoint(monkeypatch):
    class EmptyTasks:
        plans = ()

    class EmptyClient:
        def list_segments(self, collection_name):
            return ()

        def list_serving_segments(self, collection_name):
            return ()

        def list_compaction_tasks(self, collection_name):
            return EmptyTasks()

    monkeypatch.setattr(time, "sleep", lambda _: None)
    empty_checkpoint = {"all": {}, "active": {}, "serving": {}, "tasks": {}, "storage_versions": set()}

    checkpoint = _wait_for_compaction_integrity_checkpoint(
        EmptyClient(),
        "empty_round_start",
        expected_rows=0,
        before_checkpoint=empty_checkpoint,
        require_new_inputs=False,
        transition_policy="stable",
        expected_storage_version=2,
        timeout=1,
    )

    assert checkpoint == empty_checkpoint


def test_compaction_integrity_frontier_signature_ignores_history_and_tasks_but_detects_handoff():
    active = {
        10: {
            "state": "Flushed",
            "num_rows": 100,
            "is_sorted": True,
            "storage_version": 3,
            "partition_id": 1,
            "insert_channel": "channel-1",
            "compaction_from": (1, 2),
        }
    }
    serving = {
        10: {
            "state": "Sealed",
            "num_rows": 100,
            "partition_id": 1,
            "insert_channel": "channel-1",
        }
    }
    before = {"active": active, "serving": serving, "all": {10: active[10]}, "tasks": {}}
    after_task_cleanup = {
        "active": active,
        "serving": serving,
        "all": {1: {"state": "Dropped"}, 10: active[10]},
        "tasks": {99: {"state": "cleaned"}},
    }
    after_handoff = {
        **after_task_cleanup,
        "serving": {11: {**serving[10]}},
    }

    assert _compaction_integrity_frontier_signature(before) == _compaction_integrity_frontier_signature(
        after_task_cleanup
    )
    assert _compaction_integrity_frontier_signature(before) != _compaction_integrity_frontier_signature(after_handoff)


@pytest.mark.xdist_group("TestMilvusClientCompactionDataIntegrity")
@pytest.mark.compaction_data_integrity_serial
class TestMilvusClientCompactionDataIntegrity(TestMilvusClientV2Base):
    """Compaction lifecycle data-integrity tests with isolated mutable collections."""

    @contextmanager
    def _preserve_compaction_integrity_runtime_config(
        self,
        config_controller,
        config_key,
        original_config,
        evidence_prefix,
    ):
        try:
            with config_controller.preserve_config(config_key):
                yield
        finally:
            restored_config = config_controller.read_config(config_key)
            _log_compaction_integrity_evidence(
                f"{evidence_prefix}_config_restored",
                config_key=config_key,
                expected_value=None if original_config.value is None else original_config.value.decode(),
                actual_value=None if restored_config.value is None else restored_config.value.decode(),
                mod_revision=restored_config.mod_revision,
            )
            assert restored_config.value == original_config.value, (
                f"failed to restore {config_key}: expected {original_config.value!r}, got {restored_config.value!r}"
            )
            _log_compaction_integrity_evidence(
                f"{evidence_prefix}_config_restore_verified",
                config_key=config_key,
                restored_value=None if restored_config.value is None else restored_config.value.decode(),
            )

    @contextmanager
    def _preserve_compaction_integrity_storage_config(
        self,
        config_controller,
        client,
        original_storage_version,
        evidence_prefix,
    ):
        try:
            with config_controller.preserve_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG):
                yield
        finally:
            restored_config = config_controller.read_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                f"{evidence_prefix}_config_restored",
                config_key=COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                expected_storage_version=original_storage_version,
                value=None if restored_config.value is None else restored_config.value.decode(),
                mod_revision=restored_config.mod_revision,
            )
            self._detect_compaction_integrity_storage_version(
                client,
                expected_version=original_storage_version,
            )
            _log_compaction_integrity_evidence(
                f"{evidence_prefix}_config_restore_verified",
                expected_storage_version=original_storage_version,
                actual_storage_version=original_storage_version,
            )

    @pytest.fixture
    def compaction_integrity_storage_config(
        self,
        request,
        etcd_host,
        etcd_port,
        etcd_root_path,
        etcd_user,
        etcd_password,
    ):
        expected_version = int(request.param)
        assert expected_version in {2, 3}, f"unsupported compaction integrity storage version: {expected_version}"
        client = self._client()
        original_storage_version = self._detect_compaction_integrity_storage_version(client)
        with MilvusEtcdConfigController(
            host=etcd_host,
            port=etcd_port,
            root_path=etcd_root_path,
            user=etcd_user,
            password=etcd_password,
        ) as config_controller:
            original_config = config_controller.read_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                "workload_storage_config_setup_started",
                etcd_endpoint=config_controller.endpoint,
                etcd_root_path=etcd_root_path,
                expected_storage_version=expected_version,
                original_storage_version=original_storage_version,
                original_config_value=None if original_config.value is None else original_config.value.decode(),
                original_config_revision=original_config.mod_revision,
            )
            with self._preserve_compaction_integrity_storage_config(
                config_controller,
                client,
                original_storage_version,
                evidence_prefix="workload_storage",
            ):
                configured = config_controller.set_config(
                    COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                    "true" if expected_version == 3 else "false",
                )
                _log_compaction_integrity_evidence(
                    "workload_storage_config_committed",
                    expected_storage_version=expected_version,
                    value=configured.value.decode(),
                    mod_revision=configured.mod_revision,
                )
                self._detect_compaction_integrity_storage_version(
                    client,
                    expected_version=expected_version,
                )
                _log_compaction_integrity_evidence(
                    "workload_storage_config_verified",
                    expected_storage_version=expected_version,
                    actual_storage_version=expected_version,
                )
                yield {
                    "controller": config_controller,
                    "storage_version": expected_version,
                    "original_storage_version": original_storage_version,
                }

    @pytest.fixture
    def compaction_integrity_bump_schema_config(
        self,
        compaction_integrity_storage_config,
        etcd_host,
        etcd_port,
        etcd_root_path,
        etcd_user,
        etcd_password,
    ):
        assert compaction_integrity_storage_config["storage_version"] == 3
        with MilvusEtcdConfigController(
            host=etcd_host,
            port=etcd_port,
            root_path=etcd_root_path,
            user=etcd_user,
            password=etcd_password,
        ) as config_controller:
            original_config = config_controller.read_config(COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                "ddl_bump_schema_config_setup_started",
                etcd_endpoint=config_controller.endpoint,
                etcd_root_path=etcd_root_path,
                config_key=COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
                original_value=None if original_config.value is None else original_config.value.decode(),
                original_revision=original_config.mod_revision,
            )
            with self._preserve_compaction_integrity_runtime_config(
                config_controller,
                COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
                original_config,
                evidence_prefix="ddl_bump_schema",
            ):
                configured = config_controller.set_config(
                    COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
                    "true",
                )
                observed = config_controller.read_config(COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG)
                _log_compaction_integrity_evidence(
                    "ddl_bump_schema_config_committed",
                    config_key=COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
                    value=configured.value.decode(),
                    mod_revision=configured.mod_revision,
                )
                assert observed.value == b"true", (
                    f"failed to enable {COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG}: got {observed.value!r}"
                )
                assert observed.mod_revision == configured.mod_revision, (
                    f"concurrent update detected for {COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG}: "
                    f"committed revision {configured.mod_revision}, observed revision {observed.mod_revision}"
                )
                _log_compaction_integrity_evidence(
                    "ddl_bump_schema_config_verified",
                    config_key=COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
                    value=observed.value.decode(),
                    mod_revision=observed.mod_revision,
                )
                yield {
                    "controller": config_controller,
                    "enabled": True,
                    "original_value": original_config.value,
                }

    def _detect_compaction_integrity_storage_version(self, client, expected_version=None, timeout=120):
        deadline = time.time() + timeout
        last_versions = set()
        while time.time() < deadline:
            probe_name = cf.gen_unique_str("compaction_storage_probe")
            created = False
            try:
                log.info(f"storage version probe start collection={probe_name} expected_version={expected_version}")
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
                        and segment.state_name == "Flushed"
                        and segment.storage_version in {2, 3}
                    }
                    if expected_version is None and len(last_versions) == 1:
                        storage_version = next(iter(last_versions))
                        log.info(
                            f"storage version probe complete collection={probe_name} storage_version={storage_version}"
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
        include_bm25_control=False,
        include_struct_array=True,
    ):
        output_fields = _compaction_integrity_output_fields(
            include_struct_array,
            include_text,
            include_bm25_control,
        )
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
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("struct_bool", DataType.BOOL)
        struct_schema.add_field("struct_int8", DataType.INT8)
        struct_schema.add_field("struct_int16", DataType.INT16)
        struct_schema.add_field("struct_int32", DataType.INT32)
        struct_schema.add_field("struct_int64", DataType.INT64)
        struct_schema.add_field("struct_float", DataType.FLOAT)
        struct_schema.add_field("struct_double", DataType.DOUBLE)
        struct_schema.add_field("struct_varchar", DataType.VARCHAR, max_length=512)
        struct_schema.add_field(
            "struct_float_vector",
            DataType.FLOAT_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
        )
        struct_schema.add_field(
            "struct_binary_vector",
            DataType.BINARY_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
        )
        struct_schema.add_field(
            "struct_float16_vector",
            DataType.FLOAT16_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
        )
        struct_schema.add_field(
            "struct_bfloat16_vector",
            DataType.BFLOAT16_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
        )
        struct_schema.add_field(
            "struct_int8_vector",
            DataType.INT8_VECTOR,
            dim=COMPACTION_INTEGRITY_VECTOR_DIM,
        )
        if include_struct_array:
            schema.add_field(
                COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD,
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=COMPACTION_INTEGRITY_STRUCT_ARRAY_MAX_CAPACITY,
                nullable=True,
            )
        else:
            schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
            schema.add_field("binary_vector", DataType.BINARY_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
            schema.add_field("float16_vector", DataType.FLOAT16_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
            schema.add_field("bfloat16_vector", DataType.BFLOAT16_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        schema.add_field("sparse_vector", DataType.SPARSE_FLOAT_VECTOR)
        if include_struct_array and not include_bm25_control:
            schema.add_field(
                "nullable_float_vector",
                DataType.FLOAT_VECTOR,
                dim=COMPACTION_INTEGRITY_VECTOR_DIM,
                nullable=True,
            )
        if include_struct_array:
            schema.add_field(
                "nullable_binary_vector",
                DataType.BINARY_VECTOR,
                dim=COMPACTION_INTEGRITY_VECTOR_DIM,
                nullable=True,
            )
            schema.add_field("nullable_sparse_vector", DataType.SPARSE_FLOAT_VECTOR, nullable=True)
        if include_struct_array and not include_bm25_control:
            schema.add_field(
                "nullable_int8_vector",
                DataType.INT8_VECTOR,
                dim=COMPACTION_INTEGRITY_VECTOR_DIM,
                nullable=True,
            )
        if not include_struct_array:
            schema.add_field("int8_vector", DataType.INT8_VECTOR, dim=COMPACTION_INTEGRITY_VECTOR_DIM)
        if include_text:
            schema.add_field(COMPACTION_INTEGRITY_TEXT_FIELD, DataType.TEXT)
        if include_bm25_control:
            schema.add_field(
                COMPACTION_INTEGRITY_BM25_TEXT_FIELD,
                DataType.VARCHAR,
                max_length=512,
                enable_analyzer=True,
            )
            schema.add_field(COMPACTION_INTEGRITY_BM25_BASE_FIELD, DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(
                Function(
                    name=COMPACTION_INTEGRITY_BM25_BASE_FUNCTION,
                    function_type=FunctionType.BM25,
                    input_field_names=[COMPACTION_INTEGRITY_BM25_TEXT_FIELD],
                    output_field_names=[COMPACTION_INTEGRITY_BM25_BASE_FIELD],
                )
            )
        index_params = self.prepare_index_params(client)[0]
        if include_struct_array:
            for struct_vector_field, metric_type in (
                ("struct_float_vector", "MAX_SIM_COSINE"),
                ("struct_binary_vector", "MAX_SIM_HAMMING"),
                ("struct_float16_vector", "MAX_SIM_COSINE"),
                ("struct_bfloat16_vector", "MAX_SIM_COSINE"),
                ("struct_int8_vector", "MAX_SIM_COSINE"),
            ):
                index_params.add_index(
                    f"{COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD}[{struct_vector_field}]",
                    index_type="HNSW",
                    metric_type=metric_type,
                    params={"M": 8, "efConstruction": 64},
                )
        else:
            index_params.add_index("float_vector", index_type="AUTOINDEX", metric_type="COSINE")
            index_params.add_index("binary_vector", index_type="BIN_FLAT", metric_type="HAMMING")
            index_params.add_index("float16_vector", index_type="AUTOINDEX", metric_type="L2")
            index_params.add_index("bfloat16_vector", index_type="AUTOINDEX", metric_type="L2")
            index_params.add_index("int8_vector", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index("sparse_vector", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        if include_struct_array and not include_bm25_control:
            index_params.add_index("nullable_float_vector", index_type="AUTOINDEX", metric_type="COSINE")
        if include_struct_array:
            index_params.add_index("nullable_binary_vector", index_type="BIN_FLAT", metric_type="HAMMING")
            index_params.add_index("nullable_sparse_vector", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        if include_struct_array and not include_bm25_control:
            index_params.add_index("nullable_int8_vector", index_type="AUTOINDEX", metric_type="L2")
        if include_bm25_control:
            index_params.add_index(
                COMPACTION_INTEGRITY_BM25_BASE_FIELD,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
            )
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
            num_shards=1,
        )
        return output_fields, schema

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
        batch_identities = []
        for batch_index in range(COMPACTION_INTEGRITY_INSERT_BATCHES_PER_ROUND):
            explicit_test_ts = round_index * COMPACTION_INTEGRITY_INSERT_BATCHES_PER_ROUND + batch_index + 1
            pk_start = explicit_test_ts * COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH
            rows = []
            batch_expected = {}
            for logical_pk in range(pk_start, pk_start + COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH):
                row, expected = _build_compaction_integrity_row(
                    collection_name,
                    logical_pk,
                    explicit_test_ts,
                    primary_key_type,
                    include_text,
                )
                rows.append(row)
                batch_expected[row["id"]] = _canonical_compaction_integrity_row(
                    expected,
                    output_fields,
                    primary_key_type,
                )
            insert_result = self.insert(client, collection_name, rows)[0]
            assert insert_result["insert_count"] == COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH
            expected_by_pk.update(batch_expected)
            batch_identity = _compaction_integrity_ingress_identity(
                collection_name,
                "insert",
                round_index,
                batch_index,
                explicit_test_ts,
                batch_expected,
                primary_key_type,
            )
            _log_compaction_integrity_evidence(
                "insert_ingress_mutation_committed",
                **batch_identity,
                expected_total=len(expected_by_pk),
            )
            batch_identities.append(batch_identity)
            self.flush(client, collection_name)
            _log_compaction_integrity_evidence(
                "insert_ingress_persistence_requested",
                **batch_identity,
                expected_total=len(expected_by_pk),
            )
        return batch_identities

    def _ensure_compaction_integrity_utility_connection(self):
        if self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
            return
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        token = cf.param_info.param_token
        if token:
            self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, uri=uri, token=token)
        else:
            self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, uri=uri)

    def _import_compaction_integrity_round(
        self,
        collection_name,
        schema,
        expected_by_pk,
        output_fields,
        primary_key_type,
        include_text,
        round_index,
        minio_host,
        minio_bucket,
    ):
        assert COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND > 0
        explicit_test_ts = round_index + 1
        pk_start = explicit_test_ts * COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND
        rows = []
        round_expected = {}
        for logical_pk in range(pk_start, pk_start + COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND):
            row, expected = _build_compaction_integrity_row(
                collection_name,
                logical_pk,
                explicit_test_ts,
                primary_key_type,
                include_text,
            )
            row, expected = _prepare_compaction_integrity_bulk_writer_row(
                row,
                expected,
                logical_pk,
                explicit_test_ts,
            )
            rows.append(row)
            round_expected[row["id"]] = _canonical_compaction_integrity_row(
                expected,
                output_fields,
                primary_key_type,
            )

        identity = _compaction_integrity_ingress_identity(
            collection_name,
            "import",
            round_index,
            0,
            explicit_test_ts,
            round_expected,
            primary_key_type,
        )
        with RemoteBulkWriter(
            schema=schema,
            remote_path=f"compaction_integrity/{collection_name}/round_{round_index + 1}",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name=minio_bucket,
                endpoint=f"{minio_host}:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=BulkFileType.PARQUET,
        ) as remote_writer:
            for row in rows:
                remote_writer.append_row(row)
            remote_writer.commit()
            batch_files = remote_writer.batch_files
        assert len(batch_files) == 1, (
            f"ImportIngress requires one auditable file group per round, got {len(batch_files)}: {batch_files}"
        )
        _log_compaction_integrity_evidence(
            "import_ingress_payload_persisted",
            **identity,
            files=batch_files[0],
        )

        self._ensure_compaction_integrity_utility_connection()
        task_id, _ = self.utility_wrap.do_bulk_insert(
            collection_name=collection_name,
            files=batch_files[0],
        )
        _log_compaction_integrity_evidence(
            "import_ingress_submitted",
            **identity,
            job_id=task_id,
            files=batch_files[0],
        )
        completed, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
            task_ids=[task_id],
            timeout=600,
        )
        state = states.get(task_id)
        assert completed and state is not None, f"import job {task_id} did not complete: states={states}"
        assert state.state == BulkInsertState.ImportCompleted, (
            f"import job {task_id} reached {state.state_name}: {state.failed_reason}"
        )
        assert state.row_count == len(round_expected), (
            f"import job {task_id} row count mismatch: actual={state.row_count}, expected={len(round_expected)}"
        )
        assert state.progress == 100, f"import job {task_id} completed with progress={state.progress}"
        expected_by_pk.update(round_expected)
        receipt = {
            **identity,
            "job_id": task_id,
            "state": state.state_name,
            "progress": state.progress,
            "reported_row_count": state.row_count,
            "failure_reason": state.failed_reason,
            "files": batch_files[0],
        }
        _log_compaction_integrity_evidence(
            "import_ingress_mutation_committed",
            **receipt,
            expected_total=len(expected_by_pk),
        )
        return [receipt]

    def _run_compaction_integrity_rounds(
        self,
        client,
        collection_name,
        output_fields,
        primary_key_type,
        storage_version,
        round_count,
        ingress_type,
        ingress_step,
    ):
        expected_by_pk = {}
        empty_checkpoint = {
            "all": {},
            "active": {},
            "serving": {},
            "tasks": {},
            "storage_versions": set(),
        }
        latest_checkpoint = empty_checkpoint

        for round_index in range(round_count):
            round_start_checkpoint = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=len(expected_by_pk),
                before_checkpoint=empty_checkpoint,
                require_new_inputs=False,
                transition_policy="stable",
                expected_storage_version=storage_version,
            )
            _log_compaction_integrity_evidence(
                "round_start_checkpoint",
                collection=collection_name,
                ingress_type=ingress_type,
                round=round_index + 1,
                expected_rows=len(expected_by_pk),
                expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                **_compaction_integrity_checkpoint_audit(round_start_checkpoint),
            )

            ingress_receipts = ingress_step(round_index, expected_by_pk)
            ingress_checkpoint, _, ingress_validation = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=storage_version,
            )
            _log_compaction_integrity_evidence(
                "round_ingress_checkpoint",
                collection=collection_name,
                ingress_type=ingress_type,
                round=round_index + 1,
                expected_rows=len(expected_by_pk),
                expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                expected_cell_count=len(expected_by_pk) * len(output_fields),
                ingress_receipts=ingress_receipts,
                data_validation=ingress_validation,
                **_compaction_integrity_checkpoint_audit(ingress_checkpoint),
            )

            compact_id = self.compact(client, collection_name)[0]
            _log_compaction_integrity_evidence(
                "round_compaction_requested",
                collection=collection_name,
                ingress_type=ingress_type,
                round=round_index + 1,
                manual_job=compact_id,
                expected_rows=len(expected_by_pk),
            )
            checkpoint = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=len(expected_by_pk),
                before_checkpoint=round_start_checkpoint,
                expected_storage_version=storage_version,
            )
            checkpoint, _, round_validation = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=storage_version,
            )
            round_edges = _assert_compaction_integrity_graph_transition(round_start_checkpoint, checkpoint)
            round_roots = _compaction_integrity_new_roots(round_start_checkpoint, checkpoint)
            participating_roots = _compaction_integrity_participating_roots(checkpoint, round_roots, round_edges)
            assert checkpoint["storage_versions"] == {storage_version}
            _log_compaction_integrity_evidence(
                "round_checkpoint",
                collection=collection_name,
                ingress_type=ingress_type,
                round=round_index + 1,
                manual_job=compact_id,
                expected_rows=len(expected_by_pk),
                expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                expected_cell_count=len(expected_by_pk) * len(output_fields),
                ingress_receipts=ingress_receipts,
                data_validation=round_validation,
                round_edges=[
                    {"source": source_id, "target": target_id} for source_id, target_id in sorted(round_edges)
                ],
                round_root_ids=sorted(round_roots),
                participating_round_root_ids=sorted(participating_roots),
                **_compaction_integrity_checkpoint_audit(checkpoint),
            )
            latest_checkpoint = checkpoint

        final_checkpoint, _, final_validation = _assert_compaction_integrity_fenced_dataset(
            client,
            collection_name,
            expected_by_pk,
            output_fields,
            primary_key_type,
            expected_storage_version=storage_version,
        )
        _log_compaction_integrity_evidence(
            "final_data_validated",
            collection=collection_name,
            ingress_type=ingress_type,
            rounds=round_count,
            expected_rows=len(expected_by_pk),
            expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
            expected_cell_count=len(expected_by_pk) * len(output_fields),
            primary_key_type=primary_key_type.name,
            storage_version=storage_version,
            prior_round_active_segment_ids=sorted(latest_checkpoint["active"]),
            data_validation=final_validation,
            **_compaction_integrity_checkpoint_audit(final_checkpoint),
        )
        return final_checkpoint

    def _ingest_ddl_compaction_integrity_dataset(
        self,
        client,
        collection_name,
        output_fields,
        primary_key_type,
    ):
        assert COMPACTION_INTEGRITY_DDL_BATCHES > 0
        assert COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH > 0
        expected_by_pk = {}
        token_to_pk = {}
        for batch_index in range(COMPACTION_INTEGRITY_DDL_BATCHES):
            explicit_test_ts = batch_index + 1
            pk_start = explicit_test_ts * COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH
            rows = []
            batch_expected = {}
            batch_tokens = {}
            for logical_pk in range(pk_start, pk_start + COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH):
                row, expected = _build_compaction_integrity_row(
                    collection_name,
                    logical_pk,
                    explicit_test_ts,
                    primary_key_type,
                    include_text=True,
                    include_bm25_control=True,
                    include_struct_array=False,
                )
                physical_pk = row["id"]
                token = _compaction_integrity_bm25_token(logical_pk)
                rows.append(row)
                batch_expected[physical_pk] = _canonical_compaction_integrity_row(
                    expected,
                    output_fields,
                    primary_key_type,
                )
                batch_tokens[token] = physical_pk

            insert_result = self.insert(client, collection_name, rows)[0]
            assert insert_result["insert_count"] == COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH
            expected_by_pk.update(batch_expected)
            token_to_pk.update(batch_tokens)
            batch_identity = _compaction_integrity_ingress_identity(
                collection_name,
                "insert",
                0,
                batch_index,
                explicit_test_ts,
                batch_expected,
                primary_key_type,
            )
            _log_compaction_integrity_evidence(
                "ddl_ingress_mutation_committed",
                **batch_identity,
                batches=COMPACTION_INTEGRITY_DDL_BATCHES,
                expected_total=len(expected_by_pk),
            )
            self.flush(client, collection_name)
            _log_compaction_integrity_evidence(
                "ddl_ingress_persistence_requested",
                **batch_identity,
                batches=COMPACTION_INTEGRITY_DDL_BATCHES,
                expected_total=len(expected_by_pk),
            )
        return expected_by_pk, token_to_pk

    def _wait_for_ddl_schema_transition(
        self,
        client,
        collection_name,
        expected_rows,
        before_checkpoint,
        transition_policy,
        expected_schema_version,
        timeout=300,
    ):
        assert self.wait_for_schema_version_consistency(client, collection_name, timeout=timeout), (
            f"schema version did not converge after {transition_policy}: collection={collection_name}, "
            f"expected_schema_version={expected_schema_version}"
        )
        description = client.describe_collection(collection_name)
        assert description["schema_version"] == expected_schema_version
        return _wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=expected_rows,
            before_checkpoint=before_checkpoint,
            require_new_inputs=False,
            transition_policy=transition_policy,
            required_task_types=COMPACTION_INTEGRITY_SCHEMA_REWRITE_TASK_TYPES,
            expected_storage_version=3,
            timeout=timeout,
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "compaction_integrity_storage_config",
        [3],
        indirect=True,
        ids=["storage_v3"],
    )
    def test_v3_schema_evolution_compaction_preserves_all_rows_and_fields(
        self,
        compaction_integrity_storage_config,
        compaction_integrity_bump_schema_config,
    ):
        """
        target: verify V3 add/drop field and function-field rewrites preserve every retained row and field
        method: persist one all-type dataset, then validate C0 through add field, add function, drop field, drop function
        expected: each schema transition converges, uses its rewritten data, and preserves exact ordinary/BM25 results
        """
        client = self._client()
        collection_name = cf.gen_unique_str("v3_ddl_compaction_integrity")
        primary_key_type = DataType.VARCHAR
        created = False
        storage_version = compaction_integrity_storage_config["storage_version"]
        assert compaction_integrity_bump_schema_config["enabled"] is True

        try:
            output_fields, _ = self._create_compaction_integrity_collection(
                client,
                collection_name,
                primary_key_type,
                include_text=True,
                include_bm25_control=True,
                include_struct_array=False,
            )
            created = True
            assert self.wait_for_index_ready(
                client,
                collection_name,
                index_name=COMPACTION_INTEGRITY_BM25_BASE_FIELD,
                timeout=300,
            )
            expected_by_pk, token_to_pk = self._ingest_ddl_compaction_integrity_dataset(
                client,
                collection_name,
                output_fields,
                primary_key_type,
            )
            expected_rows = len(expected_by_pk)
            assert expected_rows == COMPACTION_INTEGRITY_DDL_BATCHES * COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH
            _log_compaction_integrity_evidence(
                "ddl_lifecycle_case_started",
                collection=collection_name,
                storage_version=storage_version,
                rows=expected_rows,
                ingress_batches=COMPACTION_INTEGRITY_DDL_BATCHES,
                rows_per_batch=COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH,
                ordinary_output_fields=output_fields,
            )

            empty_checkpoint = {"all": {}, "active": {}, "serving": {}, "tasks": {}, "storage_versions": set()}
            c0 = _wait_for_compaction_integrity_checkpoint(
                client,
                collection_name,
                expected_rows=expected_rows,
                before_checkpoint=empty_checkpoint,
                require_new_inputs=False,
                transition_policy="stable",
                expected_storage_version=3,
                timeout=300,
            )
            c0_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert COMPACTION_INTEGRITY_BM25_BASE_FIELD in c0_schema["fields"]
            assert COMPACTION_INTEGRITY_BM25_BASE_FUNCTION in c0_schema["functions"]
            assert COMPACTION_INTEGRITY_BM25_BASE_FIELD in c0_schema["indexes"]
            c0, bm25_baseline, _ = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=3,
                additional_validator=lambda: _assert_compaction_integrity_bm25(
                    client,
                    collection_name,
                    token_to_pk,
                    [COMPACTION_INTEGRITY_BM25_BASE_FIELD],
                ),
                timeout=600,
            )
            _log_compaction_integrity_checkpoint(
                "C0_initial",
                collection_name,
                c0,
                c0_schema,
                rows=expected_rows,
                ordinary_field_count=len(output_fields),
                bm25_query_count=len(token_to_pk),
            )

            self.add_collection_field(
                client,
                collection_name,
                COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
                DataType.INT64,
                nullable=True,
                default_value=COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE,
            )
            c1_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert c1_schema["schema_version"] > c0_schema["schema_version"]
            assert COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD in c1_schema["fields"]
            output_fields = [*output_fields, COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD]
            added_default_bytes = _canonical_compaction_integrity_cell(
                COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
                COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE,
                primary_key_type,
            )
            for expected in expected_by_pk.values():
                expected[COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD] = added_default_bytes
            c1 = self._wait_for_ddl_schema_transition(
                client,
                collection_name,
                expected_rows,
                c0,
                transition_policy="in_place_schema_bump",
                expected_schema_version=c1_schema["schema_version"],
            )
            c1, _, _ = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=3,
                additional_validator=lambda: _assert_compaction_integrity_bm25(
                    client,
                    collection_name,
                    token_to_pk,
                    [COMPACTION_INTEGRITY_BM25_BASE_FIELD],
                    baseline_by_token=bm25_baseline,
                ),
                timeout=600,
            )
            _log_compaction_integrity_checkpoint(
                "C1_add_field",
                collection_name,
                c1,
                c1_schema,
                rows=expected_rows,
                added_field=COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
                added_field_default=COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE,
                ordinary_field_count=len(output_fields),
                bm25_query_count=len(token_to_pk),
            )

            sparse_added = FieldSchema(
                name=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                dtype=DataType.SPARSE_FLOAT_VECTOR,
            )
            bm25_added_function = Function(
                name=COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION,
                function_type=FunctionType.BM25,
                input_field_names=[COMPACTION_INTEGRITY_BM25_TEXT_FIELD],
                output_field_names=[COMPACTION_INTEGRITY_BM25_ADDED_FIELD],
            )
            bound_index_params = client.prepare_index_params()
            bound_index_params.add_index(
                field_name=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
            )
            self.add_function_field(
                client,
                collection_name,
                sparse_added,
                bm25_added_function,
                index_params=bound_index_params,
            )
            c2_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert c2_schema["schema_version"] > c1_schema["schema_version"]
            assert COMPACTION_INTEGRITY_BM25_ADDED_FIELD in c2_schema["fields"]
            assert COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION in c2_schema["functions"]
            assert self.wait_for_index_ready(
                client,
                collection_name,
                index_name=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                timeout=300,
            )
            c2 = self._wait_for_ddl_schema_transition(
                client,
                collection_name,
                expected_rows,
                c1,
                transition_policy="in_place_schema_bump",
                expected_schema_version=c2_schema["schema_version"],
            )
            c2, _, _ = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=3,
                additional_validator=lambda: _assert_compaction_integrity_bm25(
                    client,
                    collection_name,
                    token_to_pk,
                    [COMPACTION_INTEGRITY_BM25_BASE_FIELD, COMPACTION_INTEGRITY_BM25_ADDED_FIELD],
                    baseline_by_token=bm25_baseline,
                ),
                timeout=600,
            )
            c2_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert COMPACTION_INTEGRITY_BM25_ADDED_FIELD in c2_schema["indexes"]
            _log_compaction_integrity_checkpoint(
                "C2_add_function_field",
                collection_name,
                c2,
                c2_schema,
                rows=expected_rows,
                added_function=COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION,
                added_function_field=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                ordinary_field_count=len(output_fields),
                bm25_query_count=len(token_to_pk),
            )

            self.drop_collection_field(
                client,
                collection_name,
                field_name=COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
            )
            c3_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert c3_schema["schema_version"] > c2_schema["schema_version"]
            assert COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD not in c3_schema["fields"]
            output_fields.remove(COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD)
            for expected in expected_by_pk.values():
                expected.pop(COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD)
            c3 = self._wait_for_ddl_schema_transition(
                client,
                collection_name,
                expected_rows,
                c2,
                transition_policy="replacement_schema_bump",
                expected_schema_version=c3_schema["schema_version"],
            )
            c3_edges = _assert_compaction_integrity_graph_transition(c2, c3, require_new_inputs=False)

            c3, _, _ = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=3,
                additional_validator=lambda: _assert_compaction_integrity_bm25(
                    client,
                    collection_name,
                    token_to_pk,
                    [COMPACTION_INTEGRITY_BM25_BASE_FIELD, COMPACTION_INTEGRITY_BM25_ADDED_FIELD],
                    baseline_by_token=bm25_baseline,
                ),
                timeout=600,
            )
            _log_compaction_integrity_checkpoint(
                "C3_drop_field",
                collection_name,
                c3,
                c3_schema,
                rows=expected_rows,
                dropped_field=COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
                accepted_edges=sorted(c3_edges),
                ordinary_field_count=len(output_fields),
                bm25_query_count=len(token_to_pk),
            )

            self.drop_function_field(
                client,
                collection_name,
                COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION,
            )
            c4_schema = _compaction_integrity_schema_snapshot(client, collection_name)
            assert c4_schema["schema_version"] > c3_schema["schema_version"]
            assert COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION not in c4_schema["functions"]
            assert COMPACTION_INTEGRITY_BM25_ADDED_FIELD not in c4_schema["fields"]
            assert COMPACTION_INTEGRITY_BM25_ADDED_FIELD not in c4_schema["indexes"]
            assert COMPACTION_INTEGRITY_BM25_BASE_FUNCTION in c4_schema["functions"]
            assert COMPACTION_INTEGRITY_BM25_BASE_FIELD in c4_schema["fields"]
            assert COMPACTION_INTEGRITY_BM25_BASE_FIELD in c4_schema["indexes"]
            c4 = self._wait_for_ddl_schema_transition(
                client,
                collection_name,
                expected_rows,
                c3,
                transition_policy="replacement_schema_bump",
                expected_schema_version=c4_schema["schema_version"],
            )
            c4_edges = _assert_compaction_integrity_graph_transition(c3, c4, require_new_inputs=False)

            def validate_c4_bm25():
                result = _assert_compaction_integrity_bm25(
                    client,
                    collection_name,
                    token_to_pk,
                    [COMPACTION_INTEGRITY_BM25_BASE_FIELD],
                    baseline_by_token=bm25_baseline,
                )
                with pytest.raises(MilvusException) as dropped_field_error:
                    client.search(
                        collection_name=collection_name,
                        data=[next(iter(token_to_pk))],
                        anns_field=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                        limit=1,
                        search_params={"metric_type": "BM25"},
                    )
                dropped_field_message = str(dropped_field_error.value).lower()
                assert COMPACTION_INTEGRITY_BM25_ADDED_FIELD in dropped_field_message
                assert "not found" in dropped_field_message or "not exist" in dropped_field_message
                return result

            c4, _, _ = _assert_compaction_integrity_fenced_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                expected_storage_version=3,
                additional_validator=validate_c4_bm25,
                timeout=600,
            )
            _log_compaction_integrity_checkpoint(
                "C4_drop_function_field",
                collection_name,
                c4,
                c4_schema,
                rows=expected_rows,
                dropped_function=COMPACTION_INTEGRITY_BM25_ADDED_FUNCTION,
                dropped_function_field=COMPACTION_INTEGRITY_BM25_ADDED_FIELD,
                accepted_edges=sorted(c4_edges),
                ordinary_field_count=len(output_fields),
                bm25_query_count=len(token_to_pk),
            )
        finally:
            if created and COMPACTION_INTEGRITY_KEEP_DDL_COLLECTION:
                _log_compaction_integrity_evidence(
                    "ddl_collection_preserved",
                    collection=collection_name,
                    storage_version=storage_version,
                )
            elif created:
                self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "compaction_integrity_storage_config",
        [2, 3],
        indirect=True,
        ids=["storage_v2", "storage_v3"],
    )
    @pytest.mark.parametrize(
        "round_count",
        COMPACTION_INTEGRITY_ROUND_COUNTS,
        ids=lambda value: f"{value}_rounds",
    )
    def test_compaction_active_set_transitions_preserve_all_rows_and_fields(
        self,
        compaction_integrity_storage_config,
        round_count,
    ):
        """
        target: verify all-type row safety at configurable round-level compaction lifecycle checkpoints
        method: append and flush ten batches, request compaction, observe the blood graph and serving handoff, then query
        expected: every round has a real transition and preserves every PK, persisted field, null/default, and cell byte
        """
        assert round_count > 0, "compaction integrity round count must be positive"
        client = self._client()
        collection_name = cf.gen_unique_str("compaction_data_integrity")
        primary_key_type = DataType.INT64
        storage_version = compaction_integrity_storage_config["storage_version"]
        include_text = storage_version == 3
        output_fields, _ = self._create_compaction_integrity_collection(
            client,
            collection_name,
            primary_key_type,
            include_text,
        )
        log.info(
            f"compaction integrity case start collection={collection_name} pk_type={primary_key_type.name} "
            f"storage_version={storage_version} include_text={include_text} rounds={round_count} "
            f"insert_batches_per_round={COMPACTION_INTEGRITY_INSERT_BATCHES_PER_ROUND} "
            f"rows_per_insert_batch={COMPACTION_INTEGRITY_INSERT_ROWS_PER_BATCH} fields={len(output_fields)} "
            f"output_fields={output_fields}"
        )
        log.info(f"compaction integrity collection ready collection={collection_name}")

        self._run_compaction_integrity_rounds(
            client,
            collection_name,
            output_fields,
            primary_key_type,
            storage_version,
            round_count,
            ingress_type="insert",
            ingress_step=lambda round_index, expected_by_pk: self._append_compaction_integrity_round(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                primary_key_type,
                include_text,
                round_index,
            ),
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "compaction_integrity_storage_config",
        [2, 3],
        indirect=True,
        ids=["storage_v2", "storage_v3"],
    )
    @pytest.mark.parametrize(
        "round_count",
        COMPACTION_INTEGRITY_ROUND_COUNTS,
        ids=lambda value: f"{value}_rounds",
    )
    def test_bulk_import_compaction_preserves_all_rows_and_fields(
        self,
        compaction_integrity_storage_config,
        round_count,
        minio_host,
        minio_bucket,
    ):
        """
        target: verify all-type row safety through Bulk Import, SortCompaction, later compaction, and serving handoff
        method: submit one deterministic Parquet import job per round and reuse the common lifecycle checkpoint verifier
        expected: every import has a definite terminal receipt and every lifecycle boundary preserves all canonical cells
        """
        assert round_count > 0, "compaction integrity round count must be positive"
        client = self._client()
        collection_name = cf.gen_unique_str("import_compaction_data_integrity")
        primary_key_type = DataType.VARCHAR
        storage_version = compaction_integrity_storage_config["storage_version"]
        include_text = storage_version == 3
        output_fields, schema = self._create_compaction_integrity_collection(
            client,
            collection_name,
            primary_key_type,
            include_text,
        )
        _log_compaction_integrity_evidence(
            "import_compaction_case_started",
            collection=collection_name,
            primary_key_type=primary_key_type.name,
            storage_version=storage_version,
            include_text=include_text,
            rounds=round_count,
            rows_per_import_round=COMPACTION_INTEGRITY_IMPORT_ROWS_PER_ROUND,
            fields=output_fields,
            minio_endpoint=f"{minio_host}:9000",
            minio_bucket=minio_bucket,
        )
        self._run_compaction_integrity_rounds(
            client,
            collection_name,
            output_fields,
            primary_key_type,
            storage_version,
            round_count,
            ingress_type="import",
            ingress_step=lambda round_index, expected_by_pk: self._import_compaction_integrity_round(
                collection_name,
                schema,
                expected_by_pk,
                output_fields,
                primary_key_type,
                include_text,
                round_index,
                minio_host,
                minio_bucket,
            ),
        )

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize(
        "round_count",
        COMPACTION_INTEGRITY_ROUND_COUNTS,
        ids=lambda value: f"{value}_rounds",
    )
    def test_compaction_storage_version_transitions_preserve_all_rows_and_fields(
        self,
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
        primary_key_type = DataType.VARCHAR
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
            original_storage_version = self._detect_compaction_integrity_storage_version(client)
            original = config_controller.read_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG)
            _log_compaction_integrity_evidence(
                "storage_transition_config_original",
                etcd_endpoint=config_controller.endpoint,
                etcd_root_path=etcd_root_path,
                config_key=COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG,
                value=None if original.value is None else original.value.decode(),
                mod_revision=original.mod_revision,
            )
            with self._preserve_compaction_integrity_storage_config(
                config_controller,
                client,
                original_storage_version,
                evidence_prefix="storage_transition",
            ):
                try:
                    v2_config = config_controller.set_config(COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG, "false")
                    _log_compaction_integrity_evidence(
                        "storage_transition_config_committed",
                        target_storage_version=2,
                        value="false",
                        mod_revision=v2_config.mod_revision,
                    )
                    self._detect_compaction_integrity_storage_version(client, expected_version=2)
                    output_fields, _ = self._create_compaction_integrity_collection(
                        client,
                        collection_name,
                        primary_key_type,
                        include_text,
                    )
                    created = True
                    empty_checkpoint = {
                        "all": {},
                        "active": {},
                        "serving": {},
                        "tasks": {},
                        "storage_versions": set(),
                    }
                    v2_checkpoint = empty_checkpoint
                    for round_index in range(round_count):
                        round_start_checkpoint = _wait_for_compaction_integrity_checkpoint(
                            client,
                            collection_name,
                            expected_rows=len(expected_by_pk),
                            before_checkpoint=empty_checkpoint,
                            require_new_inputs=False,
                            transition_policy="stable",
                            expected_storage_version=2,
                        )
                        _log_compaction_integrity_evidence(
                            "storage_transition_v2_round_start_checkpoint",
                            collection=collection_name,
                            round=round_index + 1,
                            rows=len(expected_by_pk),
                            expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                            **_compaction_integrity_checkpoint_audit(round_start_checkpoint),
                        )
                        ingress_receipts = self._append_compaction_integrity_round(
                            client,
                            collection_name,
                            expected_by_pk,
                            output_fields,
                            primary_key_type,
                            include_text,
                            round_index=round_index,
                        )
                        ingress_checkpoint, _, _ = _assert_compaction_integrity_fenced_dataset(
                            client,
                            collection_name,
                            expected_by_pk,
                            output_fields,
                            primary_key_type,
                            expected_storage_version=2,
                        )
                        _log_compaction_integrity_evidence(
                            "storage_transition_v2_ingress_checkpoint",
                            collection=collection_name,
                            round=round_index + 1,
                            rows=len(expected_by_pk),
                            expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                            expected_cell_count=len(expected_by_pk) * len(output_fields),
                            ingress_receipts=ingress_receipts,
                            **_compaction_integrity_checkpoint_audit(ingress_checkpoint),
                        )
                        v2_job = self.compact(client, collection_name)[0]
                        v2_checkpoint = _wait_for_compaction_integrity_checkpoint(
                            client,
                            collection_name,
                            expected_rows=len(expected_by_pk),
                            before_checkpoint=round_start_checkpoint,
                            expected_storage_version=2,
                        )
                        v2_checkpoint, _, _ = _assert_compaction_integrity_fenced_dataset(
                            client,
                            collection_name,
                            expected_by_pk,
                            output_fields,
                            primary_key_type,
                            expected_storage_version=2,
                        )
                        assert v2_checkpoint["storage_versions"] == {2}
                        v2_edges = _assert_compaction_integrity_graph_transition(
                            round_start_checkpoint,
                            v2_checkpoint,
                        )
                        v2_round_roots = _compaction_integrity_new_roots(
                            round_start_checkpoint,
                            v2_checkpoint,
                        )
                        v2_participating_roots = _compaction_integrity_participating_roots(
                            v2_checkpoint,
                            v2_round_roots,
                            v2_edges,
                        )
                        _log_compaction_integrity_evidence(
                            "storage_transition_v2_round_validated",
                            collection=collection_name,
                            round=round_index + 1,
                            rounds=round_count,
                            manual_job=v2_job,
                            rows=len(expected_by_pk),
                            transition_edges=sorted(v2_edges),
                            round_root_ids=sorted(v2_round_roots),
                            participating_round_root_ids=sorted(v2_participating_roots),
                            expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                            expected_cell_count=len(expected_by_pk) * len(output_fields),
                            ingress_receipts=ingress_receipts,
                            **_compaction_integrity_checkpoint_audit(v2_checkpoint),
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
                        expected_storage_version=3,
                    )
                    v3_checkpoint, _, _ = _assert_compaction_integrity_fenced_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        primary_key_type,
                        expected_storage_version=3,
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
                    _log_compaction_integrity_evidence(
                        "storage_transition_v2_to_v3_validated",
                        collection=collection_name,
                        manual_job=v3_job,
                        rows=len(expected_by_pk),
                        transition_edges=sorted(v2_to_v3_edges),
                        expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                        expected_cell_count=len(expected_by_pk) * len(output_fields),
                        **_compaction_integrity_checkpoint_audit(v3_checkpoint),
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
                        expected_storage_version=2,
                    )
                    final_checkpoint, _, _ = _assert_compaction_integrity_fenced_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        primary_key_type,
                        expected_storage_version=2,
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
                    _log_compaction_integrity_evidence(
                        "storage_transition_v3_to_v2_validated",
                        collection=collection_name,
                        manual_job=v2_restore_job,
                        rows=len(expected_by_pk),
                        transition_edges=sorted(v3_to_v2_edges),
                        expected_pk_digest=_compaction_integrity_pk_digest(expected_by_pk, primary_key_type),
                        expected_cell_count=len(expected_by_pk) * len(output_fields),
                        **_compaction_integrity_checkpoint_audit(final_checkpoint),
                    )
                finally:
                    if created:
                        self.drop_collection(client, collection_name)


def test_compaction_integrity_storage_config_restore_is_verified_after_workload_failure():
    class FakeConfigController:
        def __init__(self):
            self.restored = False

        @contextmanager
        def preserve_config(self, key):
            assert key == COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG
            try:
                yield
            finally:
                self.restored = True

        def read_config(self, key):
            assert key == COMPACTION_INTEGRITY_STORAGE_VERSION_CONFIG
            assert self.restored

            class RestoredConfig:
                value = b"false"
                mod_revision = 7

            return RestoredConfig()

    case = TestMilvusClientCompactionDataIntegrity()
    config_controller = FakeConfigController()
    verified_versions = []
    case._detect_compaction_integrity_storage_version = lambda client, expected_version: verified_versions.append(
        expected_version
    )

    with pytest.raises(RuntimeError, match="workload failed"):
        with case._preserve_compaction_integrity_storage_config(
            config_controller,
            client=object(),
            original_storage_version=2,
            evidence_prefix="test",
        ):
            raise RuntimeError("workload failed")

    assert config_controller.restored
    assert verified_versions == [2]


def test_compaction_integrity_runtime_config_restore_is_verified_after_workload_failure():
    class ConfigValue:
        def __init__(self, value, mod_revision):
            self.value = value
            self.mod_revision = mod_revision

    class FakeConfigController:
        def __init__(self):
            self.restored = False

        @contextmanager
        def preserve_config(self, key):
            assert key == COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG
            try:
                yield
            finally:
                self.restored = True

        def read_config(self, key):
            assert key == COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG
            assert self.restored
            return ConfigValue(b"false", 7)

    case = TestMilvusClientCompactionDataIntegrity()
    config_controller = FakeConfigController()
    original_config = ConfigValue(b"false", 1)

    with pytest.raises(RuntimeError, match="workload failed"):
        with case._preserve_compaction_integrity_runtime_config(
            config_controller,
            COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG,
            original_config,
            evidence_prefix="test",
        ):
            raise RuntimeError("workload failed")

    assert config_controller.restored
