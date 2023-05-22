import os
import random
import math
import string
import json
from functools import singledispatch
import numpy as np
import pandas as pd
from sklearn import preprocessing
from npy_append_array import NpyAppendArray
from pymilvus import DataType
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from common import common_type as ct
from utils.util_log import test_log as log
from customize.milvus_operator import MilvusOperator

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
                        description=ct.default_desc, **kwargs):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field


def gen_binary_vec_field(name=ct.default_binary_vec_field_name, is_primary=False, dim=ct.default_dim,
                         description=ct.default_desc, **kwargs):
    binary_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BINARY_VECTOR,
                                                                    description=description, dim=dim,
                                                                    is_primary=is_primary, **kwargs)
    return binary_vec_field


def gen_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=True, **kwargs):
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

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
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
                                       enable_dynamic_field=False, with_json=True, **kwargs):
    if enable_dynamic_field:
        fields = [gen_int64_field(), gen_float_vec_field(dim=dim)]
    else:
        fields = [gen_int64_field(), gen_int32_field(), gen_int16_field(), gen_int8_field(),
                  gen_bool_field(), gen_float_field(), gen_double_field(), gen_string_field(),
                  gen_json_field(), gen_float_vec_field(dim=dim)]
        if with_json is False:
            fields.remove(gen_json_field())
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


def gen_vectors(nb, dim):
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    if dim > 1:
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


def gen_default_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "float": i*1.0} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values
    })
    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)

    return df


def gen_default_rows_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True):
    array = []
    for i in range(start, start + nb):
        dict = {ct.default_int64_field_name: i,
                ct.default_float_field_name: i*1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i},
                ct.default_float_vec_field_name: gen_vectors(1, dim)[0]
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        array.append(dict)

    return array


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


def gen_dataframe_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True):
    int64_values = pd.Series(data=[i for i in range(start, start + nb)])
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
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values

    })
    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)

    return df


def gen_default_rows_data_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True):
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
                                             "list": [j for j in range(i, i + ct.default_json_list_length)]},
                ct.default_float_vec_field_name: gen_vectors(1, dim)[0]
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        array.append(dict)

    return array


def gen_default_binary_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0):
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


def gen_default_list_data_for_bulk_insert(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [str(i) for i in range(nb)]
    float_vec_values = []  # placeholder for float_vec
    data = [int_values, float_values, string_values, float_vec_values]
    return data


def gen_json_files_for_bulk_insert(data, schema, data_dir, **kwargs):
    nb = kwargs.get("nb", ct.default_nb)
    dim = kwargs.get("dim", ct.default_dim)
    fields_name = [field.name for field in schema.fields]
    file_name = f"bulk_insert_data_source_dim_{dim}_nb_{nb}.json"
    files = [file_name]
    data_source = os.path.join(data_dir, file_name)
    with open(data_source, "w") as f:
        f.write("{")
        f.write("\n")
        f.write('"rows":[')
        f.write("\n")
        for i in range(nb):
            entity_value = [field_values[i] for field_values in data[:-1]]
            vector = [random.random() for _ in range(dim)]
            entity_value.append(vector)
            entity = dict(zip(fields_name, entity_value))
            f.write(json.dumps(entity, indent=4, default=to_serializable))
            if i != nb - 1:
                f.write(",")
            f.write("\n")
        f.write("]")
        f.write("\n")
        f.write("}")
    return files


def gen_npy_files_for_bulk_insert(data, schema, data_dir, **kwargs):
    nb = kwargs.get("nb", ct.default_nb)
    dim = kwargs.get("dim", ct.default_dim)
    fields_name = [field.name for field in schema.fields]
    files = []
    for field in fields_name:
        files.append(f"{field}.npy")
    for i, file in enumerate(files):
        data_source = os.path.join(data_dir, file)
        if "vector" in file:
            log.info(f"generate {nb} vectors with dim {dim} for {data_source}")
            with NpyAppendArray(data_source, "wb") as npaa:
                for j in range(nb):
                    vector = np.array([[random.random() for _ in range(dim)]])
                    npaa.append(vector)

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
        dic = {"index_type": ct.all_index_types[i], "metric_type": "L2"}
        dic.update({"params": ct.default_index_params[i]})
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
        search_params.append({"index_type": index_type, "search_params": {"invalid_key": invalid_search_key}})
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
        elif index_type == "DISKANN":
            for search_list in ct.get_invalid_ints:
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
            for nprobe in [64,]:
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
        for ef in [64, 32768]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000, 5000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    elif index_type == "DISKANN":
        for search_list in [20, 30]:
            diskann_search_param = {"metric_type": metric_type, "params": {"search_list": search_list}}
            search_params.append(diskann_search_param)
    else:
        log.error("Invalid index_type.")
        raise Exception("Invalid index_type.")
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
        "int64 == 0 || int64 == 1 || int64 == 2",
        "0 < int64 < 400",
        "500 <= int64 < 1000",
        "200+300 < int64 <= 500+500",
        "int64 in [300/2, 900%40, -10*30+800, 2048/2%200, (100+200)*2]",
        "float in [+3**6, 2**10/2]",
        "(int64 % 100 == 0) && int64 < 500",
        "float <= 4**5/2 && float > 500-1 && float != 500/2+260",
        "int64 > 400 && int64 < 200",
        "float < -2**8",
        "(int64 + 1) == 3 || int64 * 2 == 64 || float == 10**2"
    ]
    return expressions


def gen_field_compare_expressions():
    expressions = [
        "int64_1 | int64_2 == 1",
        "int64_1 && int64_2 ==1",
        "int64_1 + int64_2 == 10",
        "int64_1 - int64_2 == 2",
        "int64_1 * int64_2 == 8",
        "int64_1 / int64_2 == 2",
        "int64_1 ** int64_2 == 4",
        "int64_1 % int64_2 == 0",
        "int64_1 in int64_2",
        "int64_1 + int64_2 >= 10"
    ]
    return expressions


def gen_normal_string_expressions(field):
    expressions = [
        f"\"0\"< {field} < \"3\"",
        f"{field} >= \"0\"",
        f"({field} > \"0\" && {field} < \"100\") or ({field} > \"200\" && {field} < \"300\")",
        f"\"0\" <= {field} <= \"100\"",
        f"{field} == \"0\"|| {field} == \"1\"|| {field} ==\"2\"",
        f"{field} != \"0\"",
        f"{field} not in [\"0\", \"1\", \"2\"]",
        f"{field} in [\"0\", \"1\", \"2\"]"
    ]
    return expressions


def gen_invalid_string_expressions():
    expressions = [
        "varchar in [0,  \"1\"]",
        "varchar not in [\"0\", 1, 2]"
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
                auto_id=False, dim=ct.default_dim, insert_offset=0, enable_dynamic_field=False, with_json=True):
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
    log.info(f"inserted {nb} data into collection {collection_w.name}")
    for i in range(num):
        log.debug("Dynamic field is enabled: %s" % enable_dynamic_field)
        default_data = gen_default_dataframe_data(nb // num, dim=dim, start=start, with_json=with_json)
        if enable_dynamic_field:
            default_data = gen_default_rows_data(nb // num, dim=dim, start=start, with_json=with_json)
        if is_binary:
            default_data, binary_raw_data = gen_default_binary_dataframe_data(nb // num, dim=dim, start=start)
            binary_raw_vectors.extend(binary_raw_data)
        if is_all_data_type:
            default_data = gen_dataframe_all_data_type(nb // num, dim=dim, start=start, with_json=with_json)
            if enable_dynamic_field:
                default_data = gen_default_rows_data_all_data_type(nb // num, dim=dim, start=start, with_json=with_json)
        if auto_id:
            if enable_dynamic_field:
                for data in default_data:
                    data.pop(ct.default_int64_field_name, None)
            else:
                default_data.drop(ct.default_int64_field_name, axis=1, inplace=True)
        insert_res = collection_w.insert(default_data, par[i].name)[0]
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
    all_fields = collection_w.schema.fields
    scalar_fields = []
    vector_field = []
    for field in all_fields:
        if field.dtype == DataType.FLOAT_VECTOR:
            vector_field.append(field.name)
        else:
            scalar_fields.append(field.name)
    if "*" in output_fields:
        output_fields.remove("*")
        output_fields.extend(scalar_fields)
    if "%" in output_fields:
        output_fields.remove("%")
        output_fields.extend(vector_field)
    return output_fields
