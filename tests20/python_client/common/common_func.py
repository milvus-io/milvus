import os
import random
import string
import numpy as np
from sklearn import preprocessing

from pymilvus_orm.types import DataType
from utils.util_log import my_log as log
from common.common_type import *


"""" Methods of processing data """
l2 = lambda x, y: np.linalg.norm(np.array(x) - np.array(y))


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


def get_unique_str(str_value="test_"):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return str_value + "_" + prefix


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
