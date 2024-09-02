import os
import random
import math
import string
import json
import time
import uuid
from functools import singledispatch
import numpy as np
import pandas as pd
from ml_dtypes import bfloat16
from sklearn import preprocessing
from npy_append_array import NpyAppendArray
from faker import Faker
from pathlib import Path
from minio import Minio
from pymilvus import DataType
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from common import common_type as ct
from utils.util_log import test_log as log
from customize.milvus_operator import MilvusOperator
import pickle
fake = Faker()
"""" Methods of processing data """


@singledispatch
def to_serializable(val):
    """Used by default."""
    return str(val)


@to_serializable.register(np.float32)
def ts_float32(val):
    """Used if *val* is an instance of numpy.float32."""
    return np.float64(val)


class ParamInfo:
    def __init__(self):
        self.param_host = ""
        self.param_port = ""
        self.param_handler = ""
        self.param_user = ""
        self.param_password = ""
        self.param_secure = False
        self.param_replica_num = ct.default_replica_num
        self.param_uri = ""
        self.param_token = ""

    def prepare_param_info(self, host, port, handler, replica_num, user, password, secure, uri, token):
        self.param_host = host
        self.param_port = port
        self.param_handler = handler
        self.param_user = user
        self.param_password = password
        self.param_secure = secure
        self.param_replica_num = replica_num
        self.param_uri = uri
        self.param_token = token


param_info = ParamInfo()


def generate_array_dataset(size, array_length, hit_probabilities, target_values):
    dataset = []
    target_array_length = target_values.get('array_length_field', None)
    target_array_access = target_values.get('array_access', None)
    all_target_values = set(
        val for sublist in target_values.values() for val in (sublist if isinstance(sublist, list) else [sublist]))
    for i in range(size):
        entry = {"id": i}

        # Generate random arrays for each condition
        for condition in hit_probabilities.keys():
            available_values = [val for val in range(1, 100) if val not in all_target_values]
            array = random.sample(available_values, array_length)

            # Ensure the array meets the condition based on its probability
            if random.random() < hit_probabilities[condition]:
                if condition == 'contains':
                    if target_values[condition] not in array:
                        array[random.randint(0, array_length - 1)] = target_values[condition]
                elif condition == 'contains_any':
                    if not any(val in array for val in target_values[condition]):
                        array[random.randint(0, array_length - 1)] = random.choice(target_values[condition])
                elif condition == 'contains_all':
                    indices = random.sample(range(array_length), len(target_values[condition]))
                    for idx, val in zip(indices, target_values[condition]):
                        array[idx] = val
                elif condition == 'equals':
                    array = target_values[condition][:]
                elif condition == 'array_length_field':
                    array = [random.randint(0, 10) for _ in range(target_array_length)]
                elif condition == 'array_access':
                    array = [random.randint(0, 10) for _ in range(random.randint(10, 20))]
                    array[target_array_access[0]] = target_array_access[1]
                else:
                    raise ValueError(f"Unknown condition: {condition}")

            entry[condition] = array

        dataset.append(entry)

    return dataset

def prepare_array_test_data(data_size, hit_rate=0.005, dim=128):
    size = data_size  # Number of arrays in the dataset
    array_length = 10  # Length of each array

    # Probabilities that an array hits the target condition
    hit_probabilities = {
        'contains': hit_rate,
        'contains_any': hit_rate,
        'contains_all': hit_rate,
        'equals': hit_rate,
        'array_length_field': hit_rate,
        'array_access': hit_rate
    }

    # Target values for each condition
    target_values = {
        'contains': 42,
        'contains_any': [21, 37, 42],
        'contains_all': [15, 30],
        'equals': [1,2,3,4,5],
        'array_length_field': 5, # array length == 5
        'array_access': [0, 5] # index=0, and value == 5
    }

    # Generate dataset
    dataset = generate_array_dataset(size, array_length, hit_probabilities, target_values)
    data = {
        "id": pd.Series([x["id"] for x in dataset]),
        "contains": pd.Series([x["contains"] for x in dataset]),
        "contains_any": pd.Series([x["contains_any"] for x in dataset]),
        "contains_all": pd.Series([x["contains_all"] for x in dataset]),
        "equals": pd.Series([x["equals"] for x in dataset]),
        "array_length_field": pd.Series([x["array_length_field"] for x in dataset]),
        "array_access": pd.Series([x["array_access"] for x in dataset]),
        "emb": pd.Series([np.array([random.random() for j in range(dim)], dtype=np.dtype("float32")) for _ in
                          range(size)])
    }
    # Define testing conditions
    contains_value = target_values['contains']
    contains_any_values = target_values['contains_any']
    contains_all_values = target_values['contains_all']
    equals_array = target_values['equals']

    # Perform tests
    contains_result = [d for d in dataset if contains_value in d["contains"]]
    contains_any_result = [d for d in dataset if any(val in d["contains_any"] for val in contains_any_values)]
    contains_all_result = [d for d in dataset if all(val in d["contains_all"] for val in contains_all_values)]
    equals_result = [d for d in dataset if d["equals"] == equals_array]
    array_length_result = [d for d in dataset if len(d["array_length_field"]) == target_values['array_length_field']]
    array_access_result = [d for d in dataset if d["array_access"][0] == target_values['array_access'][1]]
    # Calculate and log.info proportions
    contains_ratio = len(contains_result) / size
    contains_any_ratio = len(contains_any_result) / size
    contains_all_ratio = len(contains_all_result) / size
    equals_ratio = len(equals_result) / size
    array_length_ratio = len(array_length_result) / size
    array_access_ratio = len(array_access_result) / size

    log.info(f"\nProportion of arrays that contain the value: {contains_ratio}")
    log.info(f"Proportion of arrays that contain any of the values: {contains_any_ratio}")
    log.info(f"Proportion of arrays that contain all of the values: {contains_all_ratio}")
    log.info(f"Proportion of arrays that equal the target array: {equals_ratio}")
    log.info(f"Proportion of arrays that have the target array length: {array_length_ratio}")
    log.info(f"Proportion of arrays that have the target array access: {array_access_ratio}")



    train_df = pd.DataFrame(data)

    target_id = {
        "contains": [r["id"] for r in contains_result],
        "contains_any": [r["id"] for r in contains_any_result],
        "contains_all": [r["id"] for r in contains_all_result],
        "equals": [r["id"] for r in equals_result],
        "array_length": [r["id"] for r in array_length_result],
        "array_access": [r["id"] for r in array_access_result]
    }
    target_id_list = [target_id[key] for key in ["contains", "contains_any", "contains_all", "equals", "array_length", "array_access"]]


    filters = [
        "array_contains(contains, 42)",
        "array_contains_any(contains_any, [21, 37, 42])",
        "array_contains_all(contains_all, [15, 30])",
        "equals == [1,2,3,4,5]",
        "array_length(array_length_field) == 5",
        "array_access[0] == 5"

    ]
    query_expr = []
    for i in range(len(filters)):
        item = {
            "expr": filters[i],
            "ground_truth": target_id_list[i],
        }
        query_expr.append(item)
    return train_df, query_expr



def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_str_by_length(length=8, letters_only=False):
    if letters_only:
        return "".join(random.choice(string.ascii_letters) for _ in range(length))
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def gen_digits_by_length(length=8):
    return "".join(random.choice(string.digits) for _ in range(length))


def gen_bool_field(name=ct.default_bool_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    bool_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BOOL, description=description,
                                                              is_primary=is_primary, **kwargs)
    return bool_field


def gen_string_field(name=ct.default_string_field_name, description=ct.default_desc, is_primary=False,
                     max_length=ct.default_length, **kwargs):
    string_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.VARCHAR,
                                                                description=description, max_length=max_length,
                                                                is_primary=is_primary, **kwargs)
    return string_field


def gen_json_field(name=ct.default_json_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    json_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.JSON, description=description,
                                                              is_primary=is_primary, **kwargs)
    return json_field


def gen_array_field(name=ct.default_array_field_name, element_type=DataType.INT64, max_capacity=ct.default_max_capacity,
                    description=ct.default_desc, is_primary=False, **kwargs):

    array_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.ARRAY,
                                                               element_type=element_type, max_capacity=max_capacity,
                                                               description=description, is_primary=is_primary, **kwargs)
    return array_field


def gen_int8_field(name=ct.default_int8_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    int8_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.INT8, description=description,
                                                              is_primary=is_primary, **kwargs)
    return int8_field


def gen_int16_field(name=ct.default_int16_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    int16_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.INT16, description=description,
                                                               is_primary=is_primary, **kwargs)
    return int16_field


def gen_int32_field(name=ct.default_int32_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    int32_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.INT32, description=description,
                                                               is_primary=is_primary, **kwargs)
    return int32_field


def gen_int64_field(name=ct.default_int64_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    int64_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.INT64, description=description,
                                                               is_primary=is_primary, **kwargs)
    return int64_field


def gen_float_field(name=ct.default_float_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    float_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT, description=description,
                                                               is_primary=is_primary, **kwargs)
    return float_field


def gen_double_field(name=ct.default_double_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    double_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.DOUBLE,  description=description,
                                                                is_primary=is_primary, **kwargs)
    return double_field


def gen_float_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                        description=ct.default_desc, vector_data_type="FLOAT_VECTOR", **kwargs):
    if vector_data_type == "SPARSE_FLOAT_VECTOR":
        dtype = DataType.SPARSE_FLOAT_VECTOR
        float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=dtype,
                                                                       description=description,
                                                                       is_primary=is_primary, **kwargs)
        return float_vec_field
    if vector_data_type == "FLOAT_VECTOR":
        dtype = DataType.FLOAT_VECTOR
    elif vector_data_type == "FLOAT16_VECTOR":
        dtype = DataType.FLOAT16_VECTOR
    elif vector_data_type == "BFLOAT16_VECTOR":
        dtype = DataType.BFLOAT16_VECTOR
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=dtype,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field


def gen_binary_vec_field(name=ct.default_binary_vec_field_name, is_primary=False, dim=ct.default_dim,
                         description=ct.default_desc, **kwargs):
    binary_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BINARY_VECTOR,
                                                                    description=description, dim=dim,
                                                                    is_primary=is_primary, **kwargs)
    return binary_vec_field


def gen_float16_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                          description=ct.default_desc, **kwargs):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT16_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field


def gen_bfloat16_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                           description=ct.default_desc, **kwargs):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BFLOAT16_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field


def gen_sparse_vec_field(name=ct.default_sparse_vec_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    sparse_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.SPARSE_FLOAT_VECTOR,
                                                                    description=description,
                                                                    is_primary=is_primary, **kwargs)
    return sparse_vec_field


def gen_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=True,
                                  multiple_dim_array=[], is_partition_key=None, vector_data_type="FLOAT_VECTOR",
                                  **kwargs):
    if enable_dynamic_field:
        if primary_field is ct.default_int64_field_name:
            if is_partition_key is None:
                fields = [gen_int64_field(), gen_float_vec_field(dim=dim, vector_data_type=vector_data_type)]
            else:
                fields = [gen_int64_field(is_partition_key=(is_partition_key == ct.default_int64_field_name)),
                          gen_float_vec_field(dim=dim, vector_data_type=vector_data_type)]
        elif primary_field is ct.default_string_field_name:
            if is_partition_key is None:
                fields = [gen_string_field(), gen_float_vec_field(dim=dim, vector_data_type=vector_data_type)]
            else:
                fields = [gen_string_field(is_partition_key=(is_partition_key == ct.default_string_field_name)),
                          gen_float_vec_field(dim=dim, vector_data_type=vector_data_type)]
        else:
            log.error("Primary key only support int or varchar")
            assert False
    else:
        if is_partition_key is None:
            int64_field = gen_int64_field()
            vchar_field = gen_string_field()
        else:
            int64_field = gen_int64_field(is_partition_key=(is_partition_key == ct.default_int64_field_name))
            vchar_field = gen_string_field(is_partition_key=(is_partition_key == ct.default_string_field_name))
        fields = [int64_field, gen_float_field(), vchar_field, gen_json_field(),
                  gen_float_vec_field(dim=dim, vector_data_type=vector_data_type)]
        if with_json is False:
            fields.remove(gen_json_field())

    if len(multiple_dim_array) != 0:
        for other_dim in multiple_dim_array:
            fields.append(gen_float_vec_field(gen_unique_str("multiple_vector"), dim=other_dim,
                                              vector_data_type=vector_data_type))

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_all_datatype_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim, enable_dynamic_field=True, **kwargs):
    fields = [
        gen_int64_field(),
        gen_float_field(),
        gen_string_field(),
        gen_json_field(),
        gen_array_field(name="array_int", element_type=DataType.INT64),
        gen_array_field(name="array_float", element_type=DataType.FLOAT),
        gen_array_field(name="array_varchar", element_type=DataType.VARCHAR, max_length=200),
        gen_array_field(name="array_bool", element_type=DataType.BOOL),
        gen_float_vec_field(dim=dim),
        gen_float_vec_field(name="image_emb", dim=dim),
        gen_float_vec_field(name="text_emb", dim=dim),
        gen_float_vec_field(name="voice_emb", dim=dim),
    ]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_array_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name, auto_id=False,
                                dim=ct.default_dim, enable_dynamic_field=False, max_capacity=ct.default_max_capacity,
                                max_length=100, with_json=False, **kwargs):
    if enable_dynamic_field:
        if primary_field is ct.default_int64_field_name:
            fields = [gen_int64_field(), gen_float_vec_field(dim=dim)]
        elif primary_field is ct.default_string_field_name:
            fields = [gen_string_field(), gen_float_vec_field(dim=dim)]
        else:
            log.error("Primary key only support int or varchar")
            assert False
    else:
        fields = [gen_int64_field(), gen_float_vec_field(dim=dim), gen_json_field(),
                  gen_array_field(name=ct.default_int32_array_field_name, element_type=DataType.INT32,
                                  max_capacity=max_capacity),
                  gen_array_field(name=ct.default_float_array_field_name, element_type=DataType.FLOAT,
                                  max_capacity=max_capacity),
                  gen_array_field(name=ct.default_string_array_field_name, element_type=DataType.VARCHAR,
                                  max_capacity=max_capacity, max_length=max_length)]
        if with_json is False:
            fields.remove(gen_json_field())

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_bulk_insert_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name, with_varchar_field=True,
                                      auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=False):
    if enable_dynamic_field:
        if primary_field is ct.default_int64_field_name:
            fields = [gen_int64_field(), gen_float_vec_field(dim=dim)]
        elif primary_field is ct.default_string_field_name:
            fields = [gen_string_field(), gen_float_vec_field(dim=dim)]
        else:
            log.error("Primary key only support int or varchar")
            assert False
    else:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(),
                  gen_float_vec_field(dim=dim)]
        if with_json is False:
            fields.remove(gen_json_field())
        if with_varchar_field is False:
            fields.remove(gen_string_field())
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field)
    return schema


def gen_general_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, is_binary=False, dim=ct.default_dim, **kwargs):
    if is_binary:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_binary_vec_field(dim=dim)]
    else:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_string_pk_default_collection_schema(description=ct.default_desc, primary_field=ct.default_string_field_name,
                                            auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_json_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_multiple_json_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                                auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(name="json1"),
              gen_json_field(name="json2"), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_collection_schema_all_datatype(description=ct.default_desc,
                                       primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim,
                                       enable_dynamic_field=False, with_json=True, multiple_dim_array=[], **kwargs):
    if enable_dynamic_field:
        fields = [gen_int64_field()]
    else:
        fields = [gen_int64_field(), gen_int32_field(), gen_int16_field(), gen_int8_field(),
                  gen_bool_field(), gen_float_field(), gen_double_field(), gen_string_field(),
                  gen_json_field()]
        if with_json is False:
            fields.remove(gen_json_field())

    if len(multiple_dim_array) == 0:
        fields.append(gen_float_vec_field(dim=dim))
    else:
        multiple_dim_array.insert(0, dim)
        for i in range(len(multiple_dim_array)):
            if ct.append_vector_type[i%3] != ct.sparse_vector:
                fields.append(gen_float_vec_field(name=f"multiple_vector_{ct.append_vector_type[i%3]}",
                                              dim=multiple_dim_array[i],
                                              vector_data_type=ct.append_vector_type[i%3]))
            else:
                # The field of a sparse vector cannot be dimensioned
                fields.append(gen_float_vec_field(name=f"multiple_vector_{ct.sparse_vector}",
                                                  vector_data_type=ct.sparse_vector))

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_collection_schema(fields, primary_field=None, description=ct.default_desc, auto_id=False, **kwargs):
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, primary_field=primary_field,
                                                                    description=description, auto_id=auto_id, **kwargs)
    return schema


def gen_default_binary_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                         auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_binary_vec_field(dim=dim)]
    binary_schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                           primary_field=primary_field,
                                                                           auto_id=auto_id, **kwargs)
    return binary_schema


def gen_default_sparse_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                         auto_id=False, with_json=False, multiple_dim_array=[], **kwargs):

    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_sparse_vec_field()]
    if with_json:
        fields.insert(-1, gen_json_field())

    if len(multiple_dim_array) != 0:
        for i in range(len(multiple_dim_array)):
            vec_name = ct.default_sparse_vec_field_name + "_" + str(i)
            vec_field = gen_sparse_vec_field(name=vec_name)
            fields.append(vec_field)
    sparse_schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                           primary_field=primary_field,
                                                                           auto_id=auto_id, **kwargs)
    return sparse_schema


def gen_schema_multi_vector_fields(vec_fields):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field()]
    fields.extend(vec_fields)
    primary_field = ct.default_int64_field_name
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=ct.default_desc,
                                                                    primary_field=primary_field, auto_id=False)
    return schema


def gen_schema_multi_string_fields(string_fields):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field()]
    fields.extend(string_fields)
    primary_field = ct.default_int64_field_name
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=ct.default_desc,
                                                                    primary_field=primary_field, auto_id=False)
    return schema


def gen_vectors(nb, dim, vector_data_type="FLOAT_VECTOR"):
    vectors = []
    if vector_data_type == "FLOAT_VECTOR":
        vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    elif vector_data_type == "FLOAT16_VECTOR":
        vectors = gen_fp16_vectors(nb, dim)[1]
    elif vector_data_type == "BFLOAT16_VECTOR":
        vectors = gen_bf16_vectors(nb, dim)[1]
    elif vector_data_type == "SPARSE_FLOAT_VECTOR":
        vectors = gen_sparse_vectors(nb, dim)

    if dim > 1:
        if vector_data_type == "FLOAT_VECTOR":
            vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
            vectors = vectors.tolist()
    return vectors


def gen_string(nb):
    string_values = [str(random.random()) for _ in range(nb)]
    return string_values


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for _ in range(num):
        raw_vector = [random.randint(0, 1) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        # packs a binary-valued array into bits in a unit8 array, and bytes array_of_ints
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def gen_default_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                               random_primary_key=False, multiple_dim_array=[], multiple_vector_field_name=[],
                               vector_data_type="FLOAT_VECTOR", auto_id=False, primary_field = ct.default_int64_field_name):
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "float": i*1.0} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim, vector_data_type=vector_data_type)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values
    })

    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)
    if len(multiple_dim_array) != 0:
        if len(multiple_vector_field_name) != len(multiple_dim_array):
            log.error("multiple vector feature is enabled, please input the vector field name list "
                      "not including the default vector field")
            assert len(multiple_vector_field_name) == len(multiple_dim_array)
        for i in range(len(multiple_dim_array)):
            new_float_vec_values = gen_vectors(nb, multiple_dim_array[i], vector_data_type=vector_data_type)
            df[multiple_vector_field_name[i]] = new_float_vec_values

    return df


def gen_general_default_list_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                  random_primary_key=False, multiple_dim_array=[], multiple_vector_field_name=[],
                                  vector_data_type="FLOAT_VECTOR", auto_id=False,
                                  primary_field=ct.default_int64_field_name):
    insert_list = []
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "float": i*1.0} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim, vector_data_type=vector_data_type)
    insert_list = [int_values, float_values, string_values]

    if with_json is True:
        insert_list.append(json_values)
    insert_list.append(float_vec_values)

    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            index = 0
        elif primary_field == ct.default_string_field_name:
            index = 2
        del insert_list[index]
    if len(multiple_dim_array) != 0:
        # if len(multiple_vector_field_name) != len(multiple_dim_array):
        #     log.error("multiple vector feature is enabled, please input the vector field name list "
        #               "not including the default vector field")
        #     assert len(multiple_vector_field_name) == len(multiple_dim_array)
        for i in range(len(multiple_dim_array)):
            new_float_vec_values = gen_vectors(nb, multiple_dim_array[i], vector_data_type=vector_data_type)
            insert_list.append(new_float_vec_values)

    return insert_list


def gen_default_rows_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True, multiple_dim_array=[],
                          multiple_vector_field_name=[], vector_data_type="FLOAT_VECTOR", auto_id=False,
                          primary_field = ct.default_int64_field_name):
    array = []
    for i in range(start, start + nb):
        dict = {ct.default_int64_field_name: i,
                ct.default_float_field_name: i*1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i, "float": i*1.0},
                ct.default_float_vec_field_name: gen_vectors(1, dim, vector_data_type=vector_data_type)[0]
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        if auto_id is True:
            if primary_field == ct.default_int64_field_name:
                dict.pop(ct.default_int64_field_name)
            elif primary_field == ct.default_string_field_name:
                dict.pop(ct.default_string_field_name)
        array.append(dict)
        if len(multiple_dim_array) != 0:
            for i in range(len(multiple_dim_array)):
                dict[multiple_vector_field_name[i]] = gen_vectors(1, multiple_dim_array[i],
                                                                  vector_data_type=vector_data_type)[0]
    log.debug("generated default row data")

    return array


def gen_json_data_for_diff_json_types(nb=ct.default_nb, start=0, json_type="json_embedded_object"):
    """
    Method: gen json data for different json types. Refer to RFC7159
    """
    if json_type == "json_embedded_object":                 # a json object with an embedd json object
        return [{json_type: {"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i)}, "float": i*1.0}, "str": str(i)}
                       for i in range(start, start + nb)]
    if json_type == "json_objects_array":                   # a json-objects array with 2 json objects
        return [[{"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i)}, "float": i*1.0, "str": str(i)},
                 {"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i)}, "float": i*1.0, "str": str(i)}
                 ] for i in range(start, start + nb)]
    if json_type == "json_array":                           # single array as json value
        return [[i for i in range(j, j + 10)] for j in range(start, start + nb)]
    if json_type == "json_int":                             # single int as json value
        return [i for i in range(start, start + nb)]
    if json_type == "json_float":                           # single float as json value
        return [i*1.0 for i in range(start, start + nb)]
    if json_type == "json_string":                          # single string as json value
        return [str(i) for i in range(start, start + nb)]
    if json_type == "json_bool":                            # single bool as json value
        return [bool(i) for i in range(start, start + nb)]
    else:
        return []


def gen_default_data_for_upsert(nb=ct.default_nb, dim=ct.default_dim, start=0, size=10000):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[np.float32(i + size) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i + size) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "string": str(i)} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values
    })
    return df, float_values


def gen_array_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, auto_id=False,
                             array_length=ct.default_max_capacity, with_json=False, random_primary_key=False):
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    float_vec_values = gen_vectors(nb, dim)
    json_values = [{"number": i, "float": i * 1.0} for i in range(start, start + nb)]

    int32_values = pd.Series(data=[[np.int32(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])
    float_values = pd.Series(data=[[np.float32(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])
    string_values = pd.Series(data=[[str(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])

    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_vec_field_name: float_vec_values,
        ct.default_json_field_name: json_values,
        ct.default_int32_array_field_name: int32_values,
        ct.default_float_array_field_name: float_values,
        ct.default_string_array_field_name: string_values,
    })
    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id:
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)

    return df


def gen_dataframe_multi_vec_fields(vec_fields, nb=ct.default_nb):
    """
    gen dataframe data for fields: int64, float, float_vec and vec_fields
    :param nb: num of entities, default default_nb
    :param vec_fields: list of FieldSchema
    :return: dataframe
    """
    int_values = pd.Series(data=[i for i in range(0, nb)])
    float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_float_vec_field_name: gen_vectors(nb, ct.default_dim)
    })
    for field in vec_fields:
        dim = field.params['dim']
        if field.dtype == DataType.FLOAT_VECTOR:
            vec_values = gen_vectors(nb, dim)
        elif field.dtype == DataType.BINARY_VECTOR:
            vec_values = gen_binary_vectors(nb, dim)[1]
        df[field.name] = vec_values
    return df


def gen_dataframe_multi_string_fields(string_fields, nb=ct.default_nb):
    """
    gen dataframe data for fields: int64, float, float_vec and vec_fields
    :param nb: num of entities, default default_nb
    :param vec_fields: list of FieldSchema
    :return: dataframe
    """
    int_values = pd.Series(data=[i for i in range(0, nb)])
    float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_float_vec_field_name: gen_vectors(nb, ct.default_dim)
    })
    for field in string_fields:
        if field.dtype == DataType.VARCHAR:
            string_values = gen_string(nb)
        df[field.name] = string_values
    return df


def gen_dataframe_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                auto_id=False, random_primary_key=False, multiple_dim_array=[],
                                multiple_vector_field_name=[], primary_field=ct.default_int64_field_name):
    if not random_primary_key:
        int64_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int64_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    int32_values = pd.Series(data=[np.int32(i) for i in range(start, start + nb)], dtype="int32")
    int16_values = pd.Series(data=[np.int16(i) for i in range(start, start + nb)], dtype="int16")
    int8_values = pd.Series(data=[np.int8(i) for i in range(start, start + nb)], dtype="int8")
    bool_values = pd.Series(data=[np.bool_(i) for i in range(start, start + nb)], dtype="bool")
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    double_values = pd.Series(data=[np.double(i) for i in range(start, start + nb)], dtype="double")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int64_values,
        ct.default_int32_field_name: int32_values,
        ct.default_int16_field_name: int16_values,
        ct.default_int8_field_name: int8_values,
        ct.default_bool_field_name: bool_values,
        ct.default_float_field_name: float_values,
        ct.default_double_field_name: double_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values
    })

    if len(multiple_dim_array) == 0:
        df[ct.default_float_vec_field_name] = float_vec_values
    else:
        for i in range(len(multiple_dim_array)):
            df[multiple_vector_field_name[i]] = gen_vectors(nb, multiple_dim_array[i], ct.append_vector_type[i%3])

    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)
    log.debug("generated data completed")

    return df


def gen_general_list_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                   auto_id=False, random_primary_key=False, multiple_dim_array=[],
                                   multiple_vector_field_name=[], primary_field=ct.default_int64_field_name):
    if not random_primary_key:
        int64_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int64_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    int32_values = pd.Series(data=[np.int32(i) for i in range(start, start + nb)], dtype="int32")
    int16_values = pd.Series(data=[np.int16(i) for i in range(start, start + nb)], dtype="int16")
    int8_values = pd.Series(data=[np.int8(i) for i in range(start, start + nb)], dtype="int8")
    bool_values = pd.Series(data=[np.bool_(i) for i in range(start, start + nb)], dtype="bool")
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    double_values = pd.Series(data=[np.double(i) for i in range(start, start + nb)], dtype="double")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    insert_list = [int64_values, int32_values, int16_values, int8_values, bool_values, float_values, double_values,
                   string_values, json_values]

    if len(multiple_dim_array) == 0:
        insert_list.append(float_vec_values)
    else:
        for i in range(len(multiple_dim_array)):
            insert_list.append(gen_vectors(nb, multiple_dim_array[i], ct.append_vector_type[i%3]))

    if with_json is False:
        # index = insert_list.index(json_values)
        del insert_list[8]
    if auto_id:
        if primary_field == ct.default_int64_field_name:
            index = insert_list.index(int64_values)
        elif primary_field == ct.default_string_field_name:
            index = insert_list.index(string_values)
        del insert_list[index]
    log.debug("generated data completed")

    return insert_list


def gen_default_rows_data_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                        multiple_dim_array=[], multiple_vector_field_name=[], partition_id=0,
                                        auto_id=False, primary_field=ct.default_int64_field_name):
    array = []
    for i in range(start, start + nb):
        dict = {ct.default_int64_field_name: i,
                ct.default_int32_field_name: i,
                ct.default_int16_field_name: i,
                ct.default_int8_field_name: i,
                ct.default_bool_field_name: bool(i),
                ct.default_float_field_name: i*1.0,
                ct.default_double_field_name: i * 1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i, "string": str(i), "bool": bool(i),
                                             "list": [j for j in range(i, i + ct.default_json_list_length)]}
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        if auto_id is True:
            if primary_field == ct.default_int64_field_name:
                dict.pop(ct.default_int64_field_name, None)
            elif primary_field == ct.default_string_field_name:
                dict.pop(ct.default_string_field_name, None)
        array.append(dict)
        if len(multiple_dim_array) == 0:
            dict[ct.default_float_vec_field_name] = gen_vectors(1, dim)[0]
        else:
            for i in range(len(multiple_dim_array)):
                dict[multiple_vector_field_name[i]] = gen_vectors(nb, multiple_dim_array[i],
                                                                  ct.append_vector_type[i])[0]
    if len(multiple_dim_array) != 0:
        with open(ct.rows_all_data_type_file_path + f'_{partition_id}' + f'_dim{dim}.txt', 'wb') as json_file:
            pickle.dump(array, json_file)
            log.info("generated rows data")

    return array


def gen_default_binary_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, auto_id=False,
                                      primary_field=ct.default_int64_field_name):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_binary_vec_field_name: binary_vec_values
    })
    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)

    return df, binary_raw_values


def gen_default_list_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True):
    int_values = [i for i in range(start, start + nb)]
    float_values = [np.float32(i) for i in range(start, start + nb)]
    string_values = [str(i) for i in range(start, start + nb)]
    json_values = [{"number": i, "string": str(i), "bool": bool(i), "list": [j for j in range(0, i)]}
                   for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    if with_json is False:
        data = [int_values, float_values, string_values, float_vec_values]
    else:
        data = [int_values, float_values, string_values, json_values, float_vec_values]
    return data


def gen_default_list_sparse_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=False):
    int_values = [i for i in range(start, start + nb)]
    float_values = [np.float32(i) for i in range(start, start + nb)]
    string_values = [str(i) for i in range(start, start + nb)]
    json_values = [{"number": i, "string": str(i), "bool": bool(i), "list": [j for j in range(0, i)]}
                   for i in range(start, start + nb)]
    sparse_vec_values = gen_vectors(nb, dim, vector_data_type="SPARSE_FLOAT_VECTOR")
    if with_json:
        data = [int_values, float_values, string_values, json_values, sparse_vec_values]
    else:
        data = [int_values, float_values, string_values, sparse_vec_values]
    return data


def gen_default_list_data_for_bulk_insert(nb=ct.default_nb, varchar_len=2000, with_varchar_field=True):
    str_value = gen_str_by_length(length=varchar_len)
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [f"{str(i)}_{str_value}" for i in range(nb)]
    # in case of large nb, float_vec_values will be too large in memory
    # then generate float_vec_values in each loop instead of generating all at once during generate npy or json file
    float_vec_values = []  # placeholder for float_vec
    data = [int_values, float_values, string_values, float_vec_values]
    if with_varchar_field is False:
        data = [int_values, float_values, float_vec_values]
    return data


def prepare_bulk_insert_data(schema=None,
                             nb=ct.default_nb,
                             file_type="npy",
                             minio_endpoint="127.0.0.1:9000",
                             bucket_name="milvus-bucket"):
    schema = gen_default_collection_schema() if schema is None else schema
    dim = get_dim_by_schema(schema=schema)
    log.info(f"start to generate raw data for bulk insert")
    t0 = time.time()
    data = get_column_data_by_schema(schema=schema, nb=nb, skip_vectors=True)
    log.info(f"generate raw data for bulk insert cost {time.time() - t0} s")
    data_dir = "/tmp/bulk_insert_data"
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    log.info(f"schema:{schema}, nb:{nb}, file_type:{file_type}, minio_endpoint:{minio_endpoint}, bucket_name:{bucket_name}")
    files = []
    log.info(f"generate {file_type} files for bulk insert")
    if file_type == "json":
        files = gen_json_files_for_bulk_insert(data, schema, data_dir)
    if file_type == "npy":
        files = gen_npy_files_for_bulk_insert(data, schema, data_dir)
    log.info(f"generated {len(files)} {file_type} files for bulk insert, cost {time.time() - t0} s")
    log.info("upload file to minio")
    client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
    for file_name in files:
        file_size = os.path.getsize(os.path.join(data_dir, file_name)) / 1024 / 1024
        t0 = time.time()
        client.fput_object(bucket_name, file_name, os.path.join(data_dir, file_name))
        log.info(f"upload file {file_name} to minio, size: {file_size:.2f} MB, cost {time.time() - t0:.2f} s")
    return files


def get_column_data_by_schema(nb=ct.default_nb, schema=None, skip_vectors=False, start=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    fields_not_auto_id = []
    for field in fields:
        if not field.auto_id:
            fields_not_auto_id.append(field)
    data = []
    for field in fields_not_auto_id:
        if field.dtype == DataType.FLOAT_VECTOR and skip_vectors is True:
            tmp = []
        else:
            tmp = gen_data_by_collection_field(field, nb=nb, start=start)
        data.append(tmp)
    return data


def gen_row_data_by_schema(nb=ct.default_nb, schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    fields_not_auto_id = []
    for field in fields:
        if not field.auto_id:
            fields_not_auto_id.append(field)
    data = []
    for i in range(nb):
        tmp = {}
        for field in fields_not_auto_id:
            tmp[field.name] = gen_data_by_collection_field(field)
        data.append(tmp)
    return data


def get_fields_map(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    fields_map = {}
    for field in fields:
        fields_map[field.name] = field.dtype
    return fields_map


def get_int64_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.INT64:
            return field.name
    return None


def get_float_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT or field.dtype == DataType.DOUBLE:
            return field.name
    return None


def get_float_vec_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT_VECTOR:
            return field.name
    return None


def get_float_vec_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR]:
            vec_fields.append(field.name)
    return vec_fields


def get_scalar_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.BOOL, DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT,
                           DataType.DOUBLE, DataType.VARCHAR]:
            vec_fields.append(field.name)
    return vec_fields


def get_binary_vec_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.BINARY_VECTOR:
            return field.name
    return None


def get_binary_vec_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.BINARY_VECTOR]:
            vec_fields.append(field.name)
    return vec_fields


def get_dim_by_schema(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT_VECTOR or field.dtype == DataType.BINARY_VECTOR:
            dim = field.params['dim']
            return dim
    return None


def gen_data_by_collection_field(field, nb=None, start=None):
    # if nb is None, return one data, else return a list of data
    data_type = field.dtype
    if data_type == DataType.BOOL:
        if nb is None:
            return random.choice([True, False])
        return [random.choice([True, False]) for _ in range(nb)]
    if data_type == DataType.INT8:
        if nb is None:
            return random.randint(-128, 127)
        return [random.randint(-128, 127) for _ in range(nb)]
    if data_type == DataType.INT16:
        if nb is None:
            return random.randint(-32768, 32767)
        return [random.randint(-32768, 32767) for _ in range(nb)]
    if data_type == DataType.INT32:
        if nb is None:
            return random.randint(-2147483648, 2147483647)
        return [random.randint(-2147483648, 2147483647) for _ in range(nb)]
    if data_type == DataType.INT64:
        if nb is None:
            return random.randint(-9223372036854775808, 9223372036854775807)
        if start is not None:
            return [i for i in range(start, start+nb)]
        return [random.randint(-9223372036854775808, 9223372036854775807) for _ in range(nb)]
    if data_type == DataType.FLOAT:
        if nb is None:
            return np.float32(random.random())
        return [np.float32(random.random()) for _ in range(nb)]
    if data_type == DataType.DOUBLE:
        if nb is None:
            return np.float64(random.random())
        return [np.float64(random.random()) for _ in range(nb)]
    if data_type == DataType.VARCHAR:
        max_length = field.params['max_length']
        max_length = min(20, max_length-1)
        length = random.randint(0, max_length)
        if nb is None:
            return "".join([chr(random.randint(97, 122)) for _ in range(length)])
        return ["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(nb)]
    if data_type == DataType.JSON:
        if nb is None:
            return {"name": fake.name(), "address": fake.address()}
        data = [{"name": str(i), "address": i} for i in range(nb)]
        return data
    if data_type == DataType.FLOAT_VECTOR:
        dim = field.params['dim']
        if nb is None:
            return [random.random() for i in range(dim)]
        return [[random.random() for i in range(dim)] for _ in range(nb)]
    if data_type == DataType.BFLOAT16_VECTOR:
        dim = field.params['dim']
        if nb is None:
            raw_vector = [random.random() for _ in range(dim)]
            bf16_vector = np.array(raw_vector, dtype=bfloat16).view(np.uint8).tolist()
            return bytes(bf16_vector)
        bf16_vectors = []
        for i in range(nb):
            raw_vector = [random.random() for _ in range(dim)]
            bf16_vector = np.array(raw_vector, dtype=bfloat16).view(np.uint8).tolist()
            bf16_vectors.append(bytes(bf16_vector))
        return bf16_vectors
    if data_type == DataType.FLOAT16_VECTOR:
        dim = field.params['dim']
        if nb is None:
            return [random.random() for i in range(dim)]
        return [[random.random() for i in range(dim)] for _ in range(nb)]
    if data_type == DataType.BINARY_VECTOR:
        dim = field.params['dim']
        if nb is None:
            raw_vector = [random.randint(0, 1) for _ in range(dim)]
            binary_byte = bytes(np.packbits(raw_vector, axis=-1).tolist())
            return binary_byte
        return [bytes(np.packbits([random.randint(0, 1) for _ in range(dim)], axis=-1).tolist()) for _ in range(nb)]
    if data_type == DataType.ARRAY:
        max_capacity = field.params['max_capacity']
        element_type = field.element_type
        if element_type == DataType.INT32:
            if nb is None:
                return [random.randint(-2147483648, 2147483647) for _ in range(max_capacity)]
            return [[random.randint(-2147483648, 2147483647) for _ in range(max_capacity)] for _ in range(nb)]
        if element_type == DataType.INT64:
            if nb is None:
                return [random.randint(-9223372036854775808, 9223372036854775807) for _ in range(max_capacity)]
            return [[random.randint(-9223372036854775808, 9223372036854775807) for _ in range(max_capacity)] for _ in range(nb)]

        if element_type == DataType.BOOL:
            if nb is None:
                return [random.choice([True, False]) for _ in range(max_capacity)]
            return [[random.choice([True, False]) for _ in range(max_capacity)] for _ in range(nb)]

        if element_type == DataType.FLOAT:
            if nb is None:
                return [np.float32(random.random()) for _ in range(max_capacity)]
            return [[np.float32(random.random()) for _ in range(max_capacity)] for _ in range(nb)]
        if element_type == DataType.VARCHAR:
            max_length = field.params['max_length']
            max_length = min(20, max_length - 1)
            length = random.randint(0, max_length)
            if nb is None:
                return ["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(max_capacity)]
            return [["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(max_capacity)] for _ in range(nb)]

    return None


def gen_data_by_collection_schema(schema, nb, r=0):
    """
    gen random data by collection schema, regardless of primary key or auto_id
    vector type only support for DataType.FLOAT_VECTOR
    """
    data = []
    start_uid = r * nb
    fields = schema.fields
    for field in fields:
        data.append(gen_data_by_collection_field(field, nb, start_uid))
    return data


def gen_json_files_for_bulk_insert(data, schema, data_dir):
    for d in data:
        if len(d) > 0:
            nb = len(d)
    dim = get_dim_by_schema(schema)
    vec_field_name = get_float_vec_field_name(schema)
    fields_name = [field.name for field in schema.fields]
    # get vec field index
    vec_field_index = fields_name.index(vec_field_name)
    uuid_str = str(uuid.uuid4())
    log.info(f"file dir name: {uuid_str}")
    file_name = f"{uuid_str}/bulk_insert_data_source_dim_{dim}_nb_{nb}.json"
    files = [file_name]
    data_source = os.path.join(data_dir, file_name)
    Path(data_source).parent.mkdir(parents=True, exist_ok=True)
    log.info(f"file name: {data_source}")
    with open(data_source, "w") as f:
        f.write("{")
        f.write("\n")
        f.write('"rows":[')
        f.write("\n")
        for i in range(nb):
            entity_value = [None for _ in range(len(fields_name))]
            for j in range(len(data)):
                if j == vec_field_index:
                    entity_value[j] = [random.random() for _ in range(dim)]
                else:
                    entity_value[j] = data[j][i]
            entity = dict(zip(fields_name, entity_value))
            f.write(json.dumps(entity, indent=4, default=to_serializable))
            if i != nb - 1:
                f.write(",")
            f.write("\n")
        f.write("]")
        f.write("\n")
        f.write("}")
    return files


def gen_npy_files_for_bulk_insert(data, schema, data_dir):
    for d in data:
        if len(d) > 0:
            nb = len(d)
    dim = get_dim_by_schema(schema)
    vec_field_name = get_float_vec_field_name(schema)
    fields_name = [field.name for field in schema.fields]
    files = []
    uuid_str = uuid.uuid4()
    for field in fields_name:
        files.append(f"{uuid_str}/{field}.npy")
    for i, file in enumerate(files):
        data_source = os.path.join(data_dir, file)
        #  mkdir for npy file
        Path(data_source).parent.mkdir(parents=True, exist_ok=True)
        log.info(f"save file {data_source}")
        if vec_field_name in file:
            log.info(f"generate {nb} vectors with dim {dim} for {data_source}")
            with NpyAppendArray(data_source, "wb") as npaa:
                for j in range(nb):
                    vector = np.array([[random.random() for _ in range(dim)]])
                    npaa.append(vector)

        elif isinstance(data[i][0], dict):
            tmp = []
            for d in data[i]:
                tmp.append(json.dumps(d))
            data[i] = tmp
            np.save(data_source, np.array(data[i]))
        else:
            np.save(data_source, np.array(data[i]))
    return files


def gen_default_tuple_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [str(i) for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = (int_values, float_values, string_values, float_vec_values)
    return data


def gen_numpy_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = np.arange(nb, dtype='int64')
    float_values = np.arange(nb, dtype='float32')
    string_values = [np.str_(i) for i in range(nb)]
    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = [int_values, float_values, string_values, json_values, float_vec_values]
    return data


def gen_default_binary_list_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [str(i) for i in range(nb)]
    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    data = [int_values, float_values, string_values, binary_vec_values]
    return data, binary_raw_values


def gen_simple_index():
    index_params = []
    for i in range(len(ct.all_index_types)):
        if ct.all_index_types[i] in ct.binary_support:
            continue
        elif ct.all_index_types[i] in ct.sparse_support:
            continue
        dic = {"index_type": ct.all_index_types[i], "metric_type": "L2"}
        dic.update({"params": ct.default_all_indexes_params[i]})
        index_params.append(dic)
    return index_params


def gen_autoindex_params():
    index_params = [
        {},
        {"metric_type": "IP"},
        {"metric_type": "L2"},
        {"metric_type": "COSINE"},
        {"index_type": "AUTOINDEX"},
        {"index_type": "AUTOINDEX", "metric_type": "L2"},
        {"index_type": "AUTOINDEX", "metric_type": "COSINE"},
        {"index_type": "IVF_FLAT", "metric_type": "L2", "nlist": "1024", "m": "100"},
        {"index_type": "DISKANN", "metric_type": "L2"},
        {"index_type": "IVF_PQ", "nlist": "128", "m": "16", "nbits": "8", "metric_type": "IP"},
        {"index_type": "IVF_SQ8", "nlist": "128", "metric_type": "COSINE"}
    ]
    return index_params


def gen_invalid_field_types():
    field_types = [
        6,
        1.0,
        [[]],
        {},
        (),
        "",
        "a"
    ]
    return field_types


def gen_invalid_search_params_type():
    invalid_search_key = 100
    search_params = []
    for index_type in ct.all_index_types:
        if index_type == "FLAT":
            continue
        # search_params.append({"index_type": index_type, "search_params": {"invalid_key": invalid_search_key}})
        if index_type in ["IVF_FLAT", "IVF_SQ8", "IVF_PQ"]:
            for nprobe in ct.get_invalid_ints:
                ivf_search_params = {"index_type": index_type, "search_params": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
        elif index_type in ["HNSW"]:
            for ef in ct.get_invalid_ints:
                hnsw_search_param = {"index_type": index_type, "search_params": {"ef": ef}}
                search_params.append(hnsw_search_param)
        elif index_type == "ANNOY":
            for search_k in ct.get_invalid_ints:
                if isinstance(search_k, int):
                    continue
                annoy_search_param = {"index_type": index_type, "search_params": {"search_k": search_k}}
                search_params.append(annoy_search_param)
        elif index_type == "SCANN":
            for reorder_k in ct.get_invalid_ints:
                if isinstance(reorder_k, int):
                    continue
                scann_search_param = {"index_type": index_type, "search_params": {"nprobe": 8, "reorder_k": reorder_k}}
                search_params.append(scann_search_param)
        elif index_type == "DISKANN":
            for search_list in ct.get_invalid_ints[1:]:
                diskann_search_param = {"index_type": index_type, "search_params": {"search_list": search_list}}
                search_params.append(diskann_search_param)
    return search_params


def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "GPU_IVF_FLAT", "GPU_IVF_PQ"]:
        if index_type in ["GPU_FLAT"]:
            ivf_search_params = {"metric_type": metric_type, "params": {}}
            search_params.append(ivf_search_params)
        else:
            for nprobe in [64]:
                ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        if metric_type not in ct.binary_metrics:
            log.error("Metric type error: binary index only supports distance type in (%s)" % ct.binary_metrics)
            # default metric type for binary index
            metric_type = "JACCARD"
        for nprobe in [64, 128]:
            binary_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(binary_search_params)
    elif index_type in ["HNSW"]:
        for ef in [64, 1500, 32768]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000, 5000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    elif index_type == "SCANN":
        for reorder_k in [1200, 3000]:
            scann_search_param = {"metric_type": metric_type, "params": {"nprobe": 64, "reorder_k": reorder_k}}
            search_params.append(scann_search_param)
    elif index_type == "DISKANN":
        for search_list in [20, 300, 1500]:
            diskann_search_param = {"metric_type": metric_type, "params": {"search_list": search_list}}
            search_params.append(diskann_search_param)
    else:
        log.error("Invalid index_type.")
        raise Exception("Invalid index_type.")
    log.debug(search_params)

    return search_params


def gen_autoindex_search_params():
    search_params = [
        {},
        {"metric_type": "IP"},
        {"nlist": "1024"},
        {"efSearch": "100"},
        {"search_k": "1000"}
    ]
    return search_params


def gen_invalid_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"] \
            or index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [-1]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["HNSW"]:
        for ef in [-1]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in ["-2"]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    elif index_type == "DISKANN":
        for search_list in ["-1"]:
            diskann_search_param = {"metric_type": metric_type, "params": {"search_list": search_list}}
            search_params.append(diskann_search_param)
    elif index_type == "SCANN":
        for reorder_k in [-1]:
            scann_search_param = {"metric_type": metric_type, "params": {"reorder_k": reorder_k, "nprobe": 10}}
            search_params.append(scann_search_param)
    else:
        log.error("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params


def gen_all_type_fields():
    fields = []
    for k, v in DataType.__members__.items():
        if v != DataType.UNKNOWN:
            field, _ = ApiFieldSchemaWrapper().init_field_schema(name=k.lower(), dtype=v)
            fields.append(field)
    return fields


def gen_normal_expressions():
    expressions = [
        "",
        "int64 > 0",
        "(int64 > 0 && int64 < 400) or (int64 > 500 && int64 < 1000)",
        "int64 not in [1, 2, 3]",
        "int64 in [1, 2, 3] and float != 2",
        "int64 == 0 || float == 10**2 || (int64 + 1) == 3",
        "0 <= int64 < 400 and int64 % 100 == 0",
        "200+300 < int64 <= 500+500",
        "int64 > 400 && int64 < 200",
        "int64 in [300/2, 900%40, -10*30+800, (100+200)*2] or float in [+3**6, 2**10/2]",
        "float <= -4**5/2 && float > 500-1 && float != 500/2+260"
    ]
    return expressions


def gen_json_field_expressions():
    expressions = [
        "json_field['number'] > 0",
        "0 <= json_field['number'] < 400 or 1000 > json_field['number'] >= 500",
        "json_field['number'] not in [1, 2, 3]",
        "json_field['number'] in [1, 2, 3] and json_field['float'] != 2",
        "json_field['number'] == 0 || json_field['float'] == 10**2 || json_field['number'] + 1 == 3",
        "json_field['number'] < 400 and json_field['number'] >= 100 and json_field['number'] % 100 == 0",
        "json_field['float'] > 400 && json_field['float'] < 200",
        "json_field['number'] in [300/2, -10*30+800, (100+200)*2] or json_field['float'] in [+3**6, 2**10/2]",
        "json_field['float'] <= -4**5/2 && json_field['float'] > 500-1 && json_field['float'] != 500/2+260"
    ]
    return expressions


def gen_array_field_expressions():
    expressions = [
        "int32_array[0] > 0",
        "0 <= int32_array[0] < 400 or 1000 > float_array[1] >= 500",
        "int32_array[1] not in [1, 2, 3]",
        "int32_array[1] in [1, 2, 3] and string_array[1] != '2'",
        "int32_array == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",
        "int32_array[1] + 1 == 3 && int32_array[0] - 1 != 1",
        "int32_array[1] % 100 == 0 && string_array[1] in ['1', '2']",
        "int32_array[1] in [300/2, -10*30+800, (200-100)*2] "
        "or (float_array[1] <= -4**5/2 || 100 <= int32_array[1] < 200)"
    ]
    return expressions


def gen_field_compare_expressions(fields1=None, fields2=None):
    if fields1 is None:
        fields1 = ["int64_1"]
        fields2 = ["int64_2"]
    expressions = []
    for field1, field2 in zip(fields1, fields2):
        expression = [
            f"{field1} | {field2} == 1",
            f"{field1} + {field2} <= 10 || {field1} - {field2} == 2",
            f"{field1} * {field2} >= 8 && {field1} / {field2} < 2",
            f"{field1} ** {field2} != 4 and {field1} + {field2} > 5",
            f"{field1} not in {field2}",
            f"{field1} in {field2}",
        ]
        expressions.extend(expression)
    return expressions


def gen_normal_string_expressions(fields=None):
    if fields is None:
        fields = [ct.default_string_field_name]
    expressions = []
    for field in fields:
        expression = [
            f"\"0\"< {field} < \"3\"",
            f"{field} >= \"0\"",
            f"({field} > \"0\" && {field} < \"100\") or ({field} > \"200\" && {field} < \"300\")",
            f"\"0\" <= {field} <= \"100\"",
            f"{field} == \"0\"|| {field} == \"1\"|| {field} ==\"2\"",
            f"{field} != \"0\"",
            f"{field} not in [\"0\", \"1\", \"2\"]",
            f"{field} in [\"0\", \"1\", \"2\"]"
        ]
        expressions.extend(expression)
    return expressions


def gen_invalid_string_expressions():
    expressions = [
        "varchar in [0, \"1\"]",
        "varchar not in [\"0\", 1, 2]"
    ]
    return expressions


def gen_invalid_bool_expressions():
    expressions = [
        "bool",
        "!bool",
        "true",
        "false",
        "int64 > 0 and bool",
        "int64 > 0 or false"
    ]
    return expressions


def gen_normal_expressions_field(field):
    expressions = [
        "",
        f"{field} > 0",
        f"({field} > 0 && {field} < 400) or ({field} > 500 && {field} < 1000)",
        f"{field} not in [1, 2, 3]",
        f"{field} in [1, 2, 3] and {field} != 2",
        f"{field} == 0 || {field} == 1 || {field} == 2",
        f"0 < {field} < 400",
        f"500 <= {field} <= 1000",
        f"200+300 <= {field} <= 500+500",
        f"{field} in [300/2, 900%40, -10*30+800, 2048/2%200, (100+200)*2]",
        f"{field} in [+3**6, 2**10/2]",
        f"{field} <= 4**5/2 && {field} > 500-1 && {field} != 500/2+260",
        f"{field} > 400 && {field} < 200",
        f"{field} < -2**8",
        f"({field} + 1) == 3 || {field} * 2 == 64 || {field} == 10**2"
    ]
    return expressions


def gen_integer_overflow_expressions():
    expressions = [
        "int8 < - 128",
        "int8 > 127",
        "int8 > -129 && int8 < 128",
        "int16 < -32768",
        "int16 >= 32768",
        "int16 > -32769 && int16 <32768",
        "int32 < -2147483648",
        "int32 == 2147483648",
        "int32 < 2147483648 || int32 == -2147483648",
        "int8 in  [-129, 1] || int16 in [32769] || int32 in [2147483650, 0]"
    ]
    return expressions


def l2(x, y):
    return np.linalg.norm(np.array(x) - np.array(y))


def ip(x, y):
    return np.inner(np.array(x), np.array(y))


def cosine(x, y):
    return np.dot(x, y)/(np.linalg.norm(x)*np.linalg.norm(y))


def jaccard(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())


def hamming(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return np.bitwise_xor(x, y).sum()


def tanimoto(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    res = np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())
    if res == 0:
        value = float("inf")
    else:
        value = -np.log2(res)
    return value


def tanimoto_calc(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return np.double((len(x) - np.bitwise_xor(x, y).sum())) / (len(y) + np.bitwise_xor(x, y).sum())


def substructure(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(y)


def superstructure(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(x)


def compare_distance_2d_vector(x, y, distance, metric, sqrt):
    for i in range(len(x)):
        for j in range(len(y)):
            if metric == "L2":
                distance_i = l2(x[i], y[j])
                if not sqrt:
                    distance_i = math.pow(distance_i, 2)
            elif metric == "IP":
                distance_i = ip(x[i], y[j])
            elif metric == "HAMMING":
                distance_i = hamming(x[i], y[j])
            elif metric == "TANIMOTO":
                distance_i = tanimoto_calc(x[i], y[j])
            elif metric == "JACCARD":
                distance_i = jaccard(x[i], y[j])
            else:
                raise Exception("metric type is invalid")
            assert abs(distance_i - distance[i][j]) < ct.epsilon

    return True


def compare_distance_vector_and_vector_list(x, y, metric, distance):
    """
    target: compare the distance between x and y[i] with the expected distance array
    method: compare the distance between x and y[i] with the expected distance array
    expected: return true if all distances are matched
    """
    if not isinstance(y, list):
        log.error("%s is not a list." % str(y))
        assert False
    for i in range(len(y)):
        if metric == "L2":
            distance_i = l2(x, y[i])
        elif metric == "IP":
            distance_i = ip(x, y[i])
        elif metric == "COSINE":
            distance_i = cosine(x, y[i])
        else:
            raise Exception("metric type is invalid")
        if abs(distance_i - distance[i]) > ct.epsilon:
            log.error("The distance between %f and %f is not equal with %f" % (x, y[i], distance[i]))
            assert abs(distance_i - distance[i]) < ct.epsilon

    return True


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            log.debug("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                log.debug("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                log.info("[modify_file] file(%s) modification is complete." % file_path_list)


def index_to_dict(index):
    return {
        "collection_name": index.collection_name,
        "field_name": index.field_name,
        # "name": index.name,
        "params": index.params
    }


def get_index_params_params(index_type):
    """get default params of index params  by index type"""
    return ct.default_all_indexes_params[ct.all_index_types.index(index_type)].copy()


def get_search_params_params(index_type):
    """get default params of search params by index type"""
    return ct.default_all_search_params_params[ct.all_index_types.index(index_type)].copy()


def assert_json_contains(expr, list_data):
    opposite = False
    if expr.startswith("not"):
        opposite = True
        expr = expr.split("not ", 1)[1]
    result_ids = []
    expr_prefix = expr.split('(', 1)[0]
    exp_ids = eval(expr.split(', ', 1)[1].split(')', 1)[0])
    if expr_prefix in ["json_contains", "JSON_CONTAINS", "array_contains", "ARRAY_CONTAINS"]:
        for i in range(len(list_data)):
            if exp_ids in list_data[i]:
                result_ids.append(i)
    elif expr_prefix in ["json_contains_all", "JSON_CONTAINS_ALL", "array_contains_all", "ARRAY_CONTAINS_ALL"]:
        for i in range(len(list_data)):
            set_list_data = set(tuple(element) if isinstance(element, list) else element for element in list_data[i])
            if set(exp_ids).issubset(set_list_data):
                result_ids.append(i)
    elif expr_prefix in ["json_contains_any", "JSON_CONTAINS_ANY", "array_contains_any", "ARRAY_CONTAINS_ANY"]:
        for i in range(len(list_data)):
            set_list_data = set(tuple(element) if isinstance(element, list) else element for element in list_data[i])
            if set(exp_ids) & set_list_data:
                result_ids.append(i)
    else:
        log.warning("unknown expr: %s" % expr)
    if opposite:
        result_ids = [i for i in range(len(list_data)) if i not in result_ids]
    return result_ids


def assert_equal_index(index_1, index_2):
    return index_to_dict(index_1) == index_to_dict(index_2)


def gen_partitions(collection_w, partition_num=1):
    """
    target: create extra partitions except for _default
    method: create more than one partitions
    expected: return collection and raw data
    """
    log.info("gen_partitions: creating partitions")
    for i in range(partition_num):
        partition_name = "search_partition_" + str(i)
        collection_w.create_partition(partition_name=partition_name,
                                      description="search partition")
    par = collection_w.partitions
    assert len(par) == (partition_num + 1)
    log.info("gen_partitions: created partitions %s" % par)


def insert_data(collection_w, nb=ct.default_nb, is_binary=False, is_all_data_type=False,
                auto_id=False, dim=ct.default_dim, insert_offset=0, enable_dynamic_field=False, with_json=True,
                random_primary_key=False, multiple_dim_array=[], primary_field=ct.default_int64_field_name,
                vector_data_type="FLOAT_VECTOR"):
    """
    target: insert non-binary/binary data
    method: insert non-binary/binary data into partitions if any
    expected: return collection and raw data
    """
    par = collection_w.partitions
    num = len(par)
    vectors = []
    binary_raw_vectors = []
    insert_ids = []
    start = insert_offset
    log.info(f"inserting {nb} data into collection {collection_w.name}")
    # extract the vector field name list
    vector_name_list = extract_vector_field_name_list(collection_w)
    # prepare data
    for i in range(num):
        log.debug("Dynamic field is enabled: %s" % enable_dynamic_field)
        if not is_binary:
            if not is_all_data_type:
                if not enable_dynamic_field:
                    if vector_data_type == "FLOAT_VECTOR":
                        default_data = gen_default_dataframe_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                                  random_primary_key=random_primary_key,
                                                                  multiple_dim_array=multiple_dim_array,
                                                                  multiple_vector_field_name=vector_name_list,
                                                                  vector_data_type=vector_data_type,
                                                                  auto_id=auto_id, primary_field=primary_field)
                    elif vector_data_type in ct.append_vector_type:
                        default_data = gen_general_default_list_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                                     random_primary_key=random_primary_key,
                                                                     multiple_dim_array=multiple_dim_array,
                                                                     multiple_vector_field_name=vector_name_list,
                                                                     vector_data_type=vector_data_type,
                                                                     auto_id=auto_id, primary_field=primary_field)

                else:
                    default_data = gen_default_rows_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                         multiple_dim_array=multiple_dim_array,
                                                         multiple_vector_field_name=vector_name_list,
                                                         vector_data_type=vector_data_type,
                                                         auto_id=auto_id, primary_field=primary_field)

            else:
                if not enable_dynamic_field:
                    if vector_data_type == "FLOAT_VECTOR":
                        default_data = gen_general_list_all_data_type(nb // num, dim=dim, start=start, with_json=with_json,
                                                                      random_primary_key=random_primary_key,
                                                                      multiple_dim_array=multiple_dim_array,
                                                                      multiple_vector_field_name=vector_name_list,
                                                                      auto_id=auto_id, primary_field=primary_field)
                    elif vector_data_type == "FLOAT16_VECTOR" or "BFLOAT16_VECTOR":
                        default_data = gen_general_list_all_data_type(nb // num, dim=dim, start=start, with_json=with_json,
                                                                      random_primary_key=random_primary_key,
                                                                      multiple_dim_array=multiple_dim_array,
                                                                      multiple_vector_field_name=vector_name_list,
                                                                      auto_id=auto_id, primary_field=primary_field)
                else:
                    if os.path.exists(ct.rows_all_data_type_file_path + f'_{i}' + f'_dim{dim}.txt'):
                        with open(ct.rows_all_data_type_file_path + f'_{i}' + f'_dim{dim}.txt', 'rb') as f:
                            default_data = pickle.load(f)
                    else:
                        default_data = gen_default_rows_data_all_data_type(nb // num, dim=dim, start=start,
                                                                           with_json=with_json,
                                                                           multiple_dim_array=multiple_dim_array,
                                                                           multiple_vector_field_name=vector_name_list,
                                                                           partition_id=i, auto_id=auto_id,
                                                                           primary_field=primary_field)
        else:
            default_data, binary_raw_data = gen_default_binary_dataframe_data(nb // num, dim=dim, start=start,
                                                                              auto_id=auto_id,
                                                                              primary_field=primary_field)
            binary_raw_vectors.extend(binary_raw_data)
        insert_res = collection_w.insert(default_data, par[i].name)[0]
        log.info(f"inserted {nb // num} data into collection {collection_w.name}")
        time_stamp = insert_res.timestamp
        insert_ids.extend(insert_res.primary_keys)
        vectors.append(default_data)
        start += nb // num
    return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp


def _check_primary_keys(primary_keys, nb):
    if primary_keys is None:
        raise Exception("The primary_keys is None")
    assert len(primary_keys) == nb
    for i in range(nb - 1):
        if primary_keys[i] >= primary_keys[i + 1]:
            return False
    return True


def get_segment_distribution(res):
    """
    Get segment distribution
    """
    from collections import defaultdict
    segment_distribution = defaultdict(lambda: {"sealed": []})
    for r in res:
        for node_id in r.nodeIds:
            if r.state == 3:
                segment_distribution[node_id]["sealed"].append(r.segmentID)

    return segment_distribution


def percent_to_int(string):
    """
    transform percent(0%--100%) to int
    """

    new_int = -1
    if not isinstance(string, str):
        log.error("%s is not a string" % string)
        return new_int
    if "%" not in string:
        log.error("%s is not a percent" % string)
    else:
        new_int = int(string.strip("%"))

    return new_int


def gen_grant_list(collection_name):
    grant_list = [{"object": "Collection", "object_name": collection_name, "privilege": "Load"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Release"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Compaction"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Delete"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "GetStatistics"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "CreateIndex"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "IndexDetail"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "DropIndex"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Search"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Flush"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Query"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "LoadBalance"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Import"},
                  {"object": "Global", "object_name": "*", "privilege": "All"},
                  {"object": "Global", "object_name": "*", "privilege": "CreateCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "DropCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "DescribeCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "ShowCollections"},
                  {"object": "Global", "object_name": "*", "privilege": "CreateOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "DropOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "SelectOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "ManageOwnership"},
                  {"object": "User", "object_name": "*", "privilege": "UpdateUser"},
                  {"object": "User", "object_name": "*", "privilege": "SelectUser"}]
    return grant_list


def install_milvus_operator_specific_config(namespace, milvus_mode, release_name, image,
                                            rate_limit_enable, collection_rate_limit):
    """
    namespace : str
    milvus_mode : str -> standalone or cluster
    release_name : str
    image: str -> image tag including repository
    rate_limit_enable: str -> true or false, switch for rate limit
    collection_rate_limit: int -> collection rate limit numbers
    input_content ：the content that need to insert to the file
    return: milvus host name
    """

    if not isinstance(namespace, str):
        log.error("[namespace] is not a string.")

    if not isinstance(milvus_mode, str):
        log.error("[milvus_mode] is not a string.")

    if not isinstance(release_name, str):
        log.error("[release_name] is not a string.")

    if not isinstance(image, str):
        log.error("[image] is not a string.")

    if not isinstance(rate_limit_enable, str):
        log.error("[rate_limit_enable] is not a string.")

    if not isinstance(collection_rate_limit, int):
        log.error("[collection_rate_limit] is not an integer.")

    if milvus_mode not in ["standalone", "cluster"]:
        log.error("[milvus_mode] is not 'standalone' or 'cluster'")

    if rate_limit_enable not in ["true", "false"]:
        log.error("[rate_limit_enable] is not 'true' or 'false'")

    data_config = {
        'metadata.namespace': namespace,
        'spec.mode': milvus_mode,
        'metadata.name': release_name,
        'spec.components.image': image,
        'spec.components.proxy.serviceType': 'LoadBalancer',
        'spec.components.dataNode.replicas': 2,
        'spec.config.common.retentionDuration': 60,
        'spec.config.quotaAndLimits.enable': rate_limit_enable,
        'spec.config.quotaAndLimits.ddl.collectionRate': collection_rate_limit,
    }
    mil = MilvusOperator()
    mil.install(data_config)
    if mil.wait_for_healthy(release_name, NAMESPACE, timeout=TIMEOUT):
        host = mic.endpoint(release_name, NAMESPACE).split(':')[0]
    else:
        raise MilvusException(message=f'Milvus healthy timeout 1800s')

    return host


def get_wildcard_output_field_names(collection_w, output_fields):
    all_fields = [field.name for field in collection_w.schema.fields]
    output_fields = output_fields.copy()
    if "*" in output_fields:
        output_fields.remove("*")
        output_fields.extend(all_fields)
    return output_fields


def extract_vector_field_name_list(collection_w):
    """
    extract the vector field name list
    collection_w : the collection object to be extracted thea name of all the vector fields
    return: the vector field name list without the default float vector field name
    """
    schema_dict = collection_w.schema.to_dict()
    fields = schema_dict.get('fields')
    vector_name_list = []
    for field in fields:
        if field['type'] == DataType.FLOAT_VECTOR \
                or field['type'] == DataType.FLOAT16_VECTOR \
                or field['type'] == DataType.BFLOAT16_VECTOR \
                or field['type'] == DataType.SPARSE_FLOAT_VECTOR:
            if field['name'] != ct.default_float_vec_field_name:
                vector_name_list.append(field['name'])

    return vector_name_list


def get_activate_func_from_metric_type(metric_type):
    activate_function = lambda x: x
    if metric_type == "COSINE":
        activate_function = lambda x: (1 + x) * 0.5
    elif metric_type == "IP":
        activate_function = lambda x: 0.5 + math.atan(x)/ math.pi
    else:
        activate_function  = lambda x: 1.0 - 2*math.atan(x) / math.pi
    return activate_function


def get_hybrid_search_base_results_rrf(search_res_dict_array, round_decimal=-1):
    """
    merge the element in the dicts array
    search_res_dict_array : the dict array in which the elements to be merged
    return: the sorted id and score answer
    """
    # calculate hybrid search base line

    search_res_dict_merge = {}
    ids_answer = []
    score_answer = []

    for i, result in enumerate(search_res_dict_array, 0):
        for key, distance in result.items():
            search_res_dict_merge[key] = search_res_dict_merge.get(key, 0) + distance

    if round_decimal != -1 :
        for k, v in search_res_dict_merge.items():
            multiplier = math.pow(10.0, round_decimal)
            v = math.floor(v*multiplier+0.5) / multiplier
            search_res_dict_merge[k] = v

    sorted_list = sorted(search_res_dict_merge.items(), key=lambda x: x[1], reverse=True)

    for sort in sorted_list:
        ids_answer.append(int(sort[0]))
        score_answer.append(float(sort[1]))

    return ids_answer, score_answer


def get_hybrid_search_base_results(search_res_dict_array, weights, metric_types, round_decimal=-1):
    """
    merge the element in the dicts array
    search_res_dict_array : the dict array in which the elements to be merged
    return: the sorted id and score answer
    """
    # calculate hybrid search base line

    search_res_dict_merge = {}
    ids_answer = []
    score_answer = []

    for i, result in enumerate(search_res_dict_array, 0):
        activate_function = get_activate_func_from_metric_type(metric_types[i])
        for key, distance in result.items():
            activate_distance = activate_function(distance)
            weight = weights[i]
            search_res_dict_merge[key] = search_res_dict_merge.get(key, 0) + activate_function(distance) * weights[i]

    if round_decimal != -1 :
        for k, v in search_res_dict_merge.items():
            multiplier = math.pow(10.0, round_decimal)
            v = math.floor(v*multiplier+0.5) / multiplier
            search_res_dict_merge[k] = v

    sorted_list = sorted(search_res_dict_merge.items(), key=lambda x: x[1], reverse=True)

    for sort in sorted_list:
        ids_answer.append(int(sort[0]))
        score_answer.append(float(sort[1]))

    return ids_answer, score_answer


def gen_bf16_vectors(num, dim):
    """
    generate brain float16 vector data
    raw_vectors : the vectors
    bf16_vectors: the bytes used for insert
    return: raw_vectors and bf16_vectors
    """
    raw_vectors = []
    bf16_vectors = []
    for _ in range(num):
        raw_vector = [random.random() for _ in range(dim)]
        raw_vectors.append(raw_vector)
        bf16_vector = np.array(raw_vector, dtype=bfloat16)
        bf16_vectors.append(bf16_vector)

    return raw_vectors, bf16_vectors


def gen_fp16_vectors(num, dim):
    """
    generate float16 vector data
    raw_vectors : the vectors
    fp16_vectors: the bytes used for insert
    return: raw_vectors and fp16_vectors
    """
    raw_vectors = []
    fp16_vectors = []
    for _ in range(num):
        raw_vector = [random.random() for _ in range(dim)]
        raw_vectors.append(raw_vector)
        fp16_vector = np.array(raw_vector, dtype=np.float16)
        fp16_vectors.append(fp16_vector)

    return raw_vectors, fp16_vectors


def gen_sparse_vectors(nb, dim=1000, sparse_format="dok"):
    # default sparse format is dok, dict of keys
    # another option is coo, coordinate List

    rng = np.random.default_rng()
    vectors = [{
        d: rng.random() for d in random.sample(range(dim), random.randint(20, 30))
    } for _ in range(nb)]
    if sparse_format == "coo":
        vectors = [
            {"indices": list(x.keys()), "values": list(x.values())} for x in vectors
        ]
    return vectors


def gen_vectors_based_on_vector_type(num, dim, vector_data_type):
    """
    generate float16 vector data
    raw_vectors : the vectors
    fp16_vectors: the bytes used for insert
    return: raw_vectors and fp16_vectors
    """
    if vector_data_type == ct.float_type:
        vectors = [[random.random() for _ in range(dim)] for _ in range(num)]
    elif vector_data_type == ct.float16_type:
        vectors = gen_fp16_vectors(num, dim)[1]
    elif vector_data_type == ct.bfloat16_type:
        vectors = gen_bf16_vectors(num, dim)[1]
    elif vector_data_type == ct.sparse_vector:
        vectors = gen_sparse_vectors(num, dim)

    return vectors
