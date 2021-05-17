import os
import random
import string
import numpy as np
from sklearn import preprocessing

from pymilvus_orm.types import DataType
from pymilvus_orm.schema import CollectionSchema, FieldSchema
from utils.util_log import test_log as log
from common.common_type import *

"""" Methods of processing data """
l2 = lambda x, y: np.linalg.norm(np.array(x) - np.array(y))

get_unique_str = "test_" + "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))


def gen_int64_field(is_primary=False):
    description = "int64 type field"
    int64_field = FieldSchema(name=default_int64_field, dtype=DataType.INT64, description=description,
                              is_primary=is_primary)
    return int64_field


def gen_float_field(is_primary=False):
    description = "float type field"
    float_field = FieldSchema(name=default_float_field, dtype=DataType.FLOAT, description=description,
                              is_primary=is_primary)
    return float_field


def gen_float_vec_field(is_primary=False):
    description = "float vector type field"
    float_vec_field = FieldSchema(name=default_float_vec_field_name, dtype=DataType.FLOAT_VECTOR,
                                  description=description, dim=default_dim, is_primary=is_primary)
    return float_vec_field


def gen_binary_vec_field(is_primary=False):
    description = "binary vector type field"
    binary_vec_field = FieldSchema(name=default_binary_vec_field_name, dtype=DataType.BINARY_VECTOR,
                                   description=description, is_primary=is_primary)
    return binary_vec_field


def gen_default_collection_schema(description=default_collection_desc, primary_field=None):
    fields = [gen_int64_field(), gen_float_field(), gen_float_vec_field()]
    schema = CollectionSchema(fields=fields, description=description, primary_field=primary_field)
    return schema


def gen_collection_schema(fields, description="collection", **kwargs):
    schema = CollectionSchema(fields=fields, description=description, **kwargs)
    return schema


def gen_default_binary_collection_schema():
    fields = [gen_int64_field(), gen_float_field(), gen_binary_vec_field()]
    binary_schema = CollectionSchema(fields=fields, description="default binary collection")
    return binary_schema


def get_binary_default_fields(auto_id=True):
    default_fields = {
        "fields": [
            {"name": "int64", "type": DataType.INT64},
            {"name": "float", "type": DataType.FLOAT},
            {"name": default_binary_vec_field_name, "type": DataType.BINARY_VECTOR, "params": {"dim": default_dim}}
        ],
        "segment_row_limit": default_segment_row_limit,
        "auto_id": auto_id
    }
    return default_fields


def gen_simple_index():
    index_params = []
    for i in range(len(all_index_types)):
        if all_index_types[i] in binary_support:
            continue
        dic = {"index_type": all_index_types[i], "metric_type": "L2"}
        dic.update({"params": default_index_params[i]})
        index_params.append(dic)
    return index_params


def get_vectors(num, dim, is_normal=True):
    vectors = [[random.random() for _ in range(dim)] for _ in range(num)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


def get_entities(nb=default_nb, is_normal=False):
    vectors = get_vectors(nb, default_dim, is_normal)
    entities = [
        {"name": "int64", "type": DataType.INT64, "values": [i for i in range(nb)]},
        {"name": "float", "type": DataType.FLOAT, "values": [float(i) for i in range(nb)]},
        {"name": default_float_vec_field_name, "type": DataType.FLOAT_VECTOR, "values": vectors}
    ]
    return entities


def modify_file(file_name_list, input_content=""):
    if not isinstance(file_name_list, list):
        log.error("[modify_file] file is not a list.")

    for file_name in file_name_list:
        if not os.path.isfile(file_name):
            log.error("[modify_file] file(%s) is not exist." % file_name)

        with open(file_name, "r+") as f:
            f.seek(0)
            f.truncate()
            f.write(input_content)
            f.close()

    log.info("[modify_file] File(%s) modification is complete." % file_name_list)
