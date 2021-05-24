import os
import random
import string
import numpy as np
from sklearn import preprocessing

from pymilvus_orm.types import DataType
from pymilvus_orm.schema import CollectionSchema, FieldSchema
from common import common_type as ct
from utils.util_log import test_log as log

"""" Methods of processing data """
l2 = lambda x, y: np.linalg.norm(np.array(x) - np.array(y))


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_int64_field(name=ct.default_int64_field, is_primary=False, description=ct.default_desc):
    int64_field = FieldSchema(name=name, dtype=DataType.INT64, description=description, is_primary=is_primary)
    return int64_field


def gen_float_field(name=ct.default_float_field, is_primary=False, description=ct.default_desc):
    float_field = FieldSchema(name=name, dtype=DataType.FLOAT, description=description, is_primary=is_primary)
    return float_field


def gen_float_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                        description=ct.default_desc):
    float_vec_field = FieldSchema(name=name, dtype=DataType.FLOAT_VECTOR, description=description, dim=dim,
                                  is_primary=is_primary)
    return float_vec_field


def gen_binary_vec_field(name=ct.default_binary_vec_field_name, is_primary=False, dim=ct.default_dim,
                         description=ct.default_desc):
    binary_vec_field = FieldSchema(name=name, dtype=DataType.BINARY_VECTOR, description=description, dim=dim,
                                   is_primary=is_primary)
    return binary_vec_field


def gen_default_collection_schema(description=ct.default_desc, primary_field=None):
    fields = [gen_int64_field(), gen_float_field(), gen_float_vec_field()]
    schema = CollectionSchema(fields=fields, description=description, primary_field=primary_field)
    return schema


def gen_collection_schema(fields, primary_field=None, description=ct.default_desc):
    schema = CollectionSchema(fields=fields, primary_field=primary_field, description=description)
    return schema


def gen_default_binary_collection_schema(description=ct.default_binary_desc, primary_field=None):
    fields = [gen_int64_field(), gen_float_field(), gen_binary_vec_field()]
    binary_schema = CollectionSchema(fields=fields, description=description, primary_field=primary_field)
    return binary_schema


def gen_vectors(nb, dim):
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


def gen_default_dataframe_data(nb=ct.default_nb):
    import pandas as pd
    int_values = pd.Series(data=[i for i in range(nb)])
    float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
    float_vec_values = gen_vectors(nb, ct.default_dim)
    df = pd.DataFrame({
        ct.default_int64_field: int_values,
        ct.default_float_field: float_values,
        ct.default_float_vec_field_name: float_vec_values
    })
    return df


def gen_default_list_data(nb=ct.default_nb):
    int_values = [i for i in range(nb)]
    float_values = [float(i) for i in range(nb)]
    float_vec_values = gen_vectors(nb, ct.default_dim)
    data = [int_values, float_values, float_vec_values]
    return data


def gen_simple_index():
    index_params = []
    for i in range(len(ct.all_index_types)):
        if ct.all_index_types[i] in ct.binary_support:
            continue
        dic = {"index_type": ct.all_index_types[i], "metric_type": "L2"}
        dic.update({"params": ct.default_index_params[i]})
        index_params.append(dic)
    return index_params


def get_vectors(num, dim, is_normal=True):
    vectors = [[random.random() for _ in range(dim)] for _ in range(num)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for i in range(num):
        raw_vector = [random.randint(0, 1) for i in range(dim)]
        raw_vectors.append(raw_vector)
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


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


def gen_all_type_fields():
    fields = []
    for k, v in DataType.__members__.items():
        field = FieldSchema(name=k.lower(), dtype=v)
        fields.append(field)
    return fields


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
