import os
import random
import string
import numpy as np
from sklearn import preprocessing

from pymilvus_orm.types import DataType
from pymilvus_orm.schema import CollectionSchema, FieldSchema

from common.common_type import int_field_desc, float_field_desc, float_vec_field_desc, binary_vec_field_desc
from common.common_type import default_nb, all_index_types, binary_support, default_index_params
from common.common_type import default_int64_field, default_float_field, default_float_vec_field_name, default_binary_vec_field_name
from common.common_type import default_dim, collection_desc, default_collection_desc, default_binary_desc
from utils.util_log import test_log as log

"""" Methods of processing data """
l2 = lambda x, y: np.linalg.norm(np.array(x) - np.array(y))


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_int64_field(is_primary=False, description=int_field_desc):
    int64_field = FieldSchema(name=default_int64_field, dtype=DataType.INT64, description=description,
                              is_primary=is_primary)
    return int64_field


def gen_float_field(is_primary=False, description=float_field_desc):
    float_field = FieldSchema(name=default_float_field, dtype=DataType.FLOAT, description=description,
                              is_primary=is_primary)
    return float_field


def gen_float_vec_field(is_primary=False, dim=default_dim, description=float_vec_field_desc):
    float_vec_field = FieldSchema(name=default_float_vec_field_name, dtype=DataType.FLOAT_VECTOR,
                                  description=description, dim=dim, is_primary=is_primary)
    return float_vec_field


def gen_binary_vec_field(is_primary=False, dim=default_dim, description=binary_vec_field_desc):
    binary_vec_field = FieldSchema(name=default_binary_vec_field_name, dtype=DataType.BINARY_VECTOR,
                                   description=description, dim=dim, is_primary=is_primary)
    return binary_vec_field


def gen_default_collection_schema(description=default_collection_desc, primary_field=None):
    fields = [gen_int64_field(), gen_float_field(), gen_float_vec_field()]
    schema = CollectionSchema(fields=fields, description=description, primary_field=primary_field)
    return schema


def gen_collection_schema(fields, description=collection_desc, **kwargs):
    schema = CollectionSchema(fields=fields, description=description, **kwargs)
    return schema


def gen_default_binary_collection_schema(description=default_binary_desc, primary_field=None):
    fields = [gen_int64_field(), gen_float_field(), gen_binary_vec_field()]
    binary_schema = CollectionSchema(fields=fields, description=description, primary_field=primary_field)
    return binary_schema


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
