import json
import os
import random
import string
import numpy as np
from enum import Enum
from common import common_type as ct
from utils.util_log import test_log as log


class ParamInfo:
    def __init__(self):
        self.param_host = ""
        self.param_port = ""

    def prepare_param_info(self, host, http_port):
        self.param_host = host
        self.param_port = http_port


param_info = ParamInfo()


class DataType(Enum):
    Bool: 1
    Int8: 2
    Int16: 3
    Int32: 4
    Int64: 5
    Float: 10
    Double: 11
    String: 20
    VarChar: 21
    BinaryVector: 100
    FloatVector: 101


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_field(name=ct.default_bool_field_name, description=ct.default_desc, type_params=None, index_params=None,
              data_type="Int64", is_primary_key=False, auto_id=False, dim=128, max_length=256):
    data_type_map = {
        "Bool": 1,
        "Int8": 2,
        "Int16": 3,
        "Int32": 4,
        "Int64": 5,
        "Float": 10,
        "Double": 11,
        "String": 20,
        "VarChar": 21,
        "BinaryVector": 100,
        "FloatVector": 101,
    }
    if data_type == "Int64":
        is_primary_key = True
        auto_id = True
    if type_params is None:
        type_params = []
    if index_params is None:
        index_params = []
    if data_type in ["FloatVector", "BinaryVector"]:
        type_params = [{"key": "dim", "value": str(dim)}]
    if data_type in ["String", "VarChar"]:
        type_params = [{"key": "max_length", "value": str(dim)}]
    return {
        "name": name,
        "description": description,
        "data_type": data_type_map.get(data_type, 0),
        "type_params": type_params,
        "index_params": index_params,
        "is_primary_key": is_primary_key,
        "auto_id": auto_id,
    }


def gen_schema(name, fields, description=ct.default_desc, auto_id=False):
    return {
        "name": name,
        "description": description,
        "auto_id": auto_id,
        "fields": fields,
    }


def gen_default_schema(data_types=None, dim=ct.default_dim, collection_name=None):
    if data_types is None:
        data_types = ["Int64", "Float", "VarChar", "FloatVector"]
    fields = []
    for data_type in data_types:
        if data_type in ["FloatVector", "BinaryVector"]:
            fields.append(gen_field(name=data_type, data_type=data_type, type_params=[{"key": "dim", "value": dim}]))
        else:
            fields.append(gen_field(name=data_type, data_type=data_type))
    return {
        "autoID": True,
        "fields": fields,
        "description": ct.default_desc,
        "name": collection_name,
    }


def gen_fields_data(schema=None, nb=ct.default_nb,):
    if schema is None:
        schema = gen_default_schema()
    fields = schema["fields"]
    fields_data = []
    for field in fields:
        if field["data_type"] == 1:
            fields_data.append([random.choice([True, False]) for i in range(nb)])
        elif field["data_type"] == 2:
            fields_data.append([i for i in range(nb)])
        elif field["data_type"] == 3:
            fields_data.append([i for i in range(nb)])
        elif field["data_type"] == 4:
            fields_data.append([i for i in range(nb)])
        elif field["data_type"] == 5:
            fields_data.append([i for i in range(nb)])
        elif field["data_type"] == 10:
            fields_data.append([np.float64(i) for i in range(nb)])  # json not support float32
        elif field["data_type"] == 11:
            fields_data.append([np.float64(i) for i in range(nb)])
        elif field["data_type"] == 20:
            fields_data.append([gen_unique_str((str(i))) for i in range(nb)])
        elif field["data_type"] == 21:
            fields_data.append([gen_unique_str(str(i)) for i in range(nb)])
        elif field["data_type"] == 100:
            dim = ct.default_dim
            for k, v in field["type_params"]:
                if k == "dim":
                    dim = int(v)
                    break
            fields_data.append(gen_binary_vectors(nb, dim))
        elif field["data_type"] == 101:
            dim = ct.default_dim
            for k, v in field["type_params"]:
                if k == "dim":
                    dim = int(v)
                    break
            fields_data.append(gen_float_vectors(nb, dim))
        else:
            log.error("Unknown data type.")
    fields_data_body = []
    for i, field in enumerate(fields):
        fields_data_body.append({
            "field_name": field["name"],
            "type": field["data_type"],
            "field": fields_data[i],
        })
    return fields_data_body


def get_vector_field(schema):
    for field in schema["fields"]:
        if field["data_type"] in [100, 101]:
            return field["name"]
    return None


def get_varchar_field(schema):
    for field in schema["fields"]:
        if field["data_type"] == 21:
            return field["name"]
    return None


def gen_vectors(nq=None, schema=None):
    if nq is None:
        nq = ct.default_nq
    dim = ct.default_dim
    data_type = 101
    for field in schema["fields"]:
        if field["data_type"] in [100, 101]:
            dim = ct.default_dim
            data_type = field["data_type"]
            for k, v in field["type_params"]:
                if k == "dim":
                    dim = int(v)
                    break
    if data_type == 100:
        return gen_binary_vectors(nq, dim)
    if data_type == 101:
        return gen_float_vectors(nq, dim)


def gen_float_vectors(nb, dim):
    return [[np.float64(random.uniform(-1.0, 1.0)) for _ in range(dim)] for _ in range(nb)]  # json not support float32


def gen_binary_vectors(nb, dim):
    raw_vectors = []
    binary_vectors = []
    for _ in range(nb):
        raw_vector = [random.randint(0, 1) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        # packs a binary-valued array into bits in a unit8 array, and bytes array_of_ints
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return binary_vectors


def gen_index_params(index_type=None):
    if index_type is None:
        index_params = ct.default_index_params
    else:
        index_params = ct.all_index_params_map[index_type]
    extra_params = []
    for k, v in index_params.items():
        item = {"key": k, "value": json.dumps(v) if isinstance(v, dict) else str(v)}
        extra_params.append(item)
    return extra_params

def gen_search_param_by_index_type(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]:
        for nprobe in [10]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [10]:
            bin_search_params = {"metric_type": "HAMMING", "params": {"nprobe": nprobe}}
            search_params.append(bin_search_params)
    elif index_type in ["HNSW"]:
        for ef in [64]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    else:
        log.info("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params


def gen_search_params(index_type=None, anns_field=ct.default_float_vec_field_name,
                      topk=ct.default_top_k):
    if index_type is None:
        search_params = gen_search_param_by_index_type(ct.default_index_type)[0]
    else:
        search_params = gen_search_param_by_index_type(index_type)[0]
    extra_params = []
    for k, v in search_params.items():
        item = {"key": k, "value": json.dumps(v) if isinstance(v, dict) else str(v)}
        extra_params.append(item)
    extra_params.append({"key": "anns_field", "value": anns_field})
    extra_params.append({"key": "topk", "value": str(topk)})
    return extra_params




def gen_search_vectors(dim, nb, is_binary=False):
    if is_binary:
        return gen_binary_vectors(nb, dim)
    return gen_float_vectors(nb, dim)


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


if __name__ == '__main__':
    a = gen_binary_vectors(10, 128)
    print(a)
