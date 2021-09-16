import os
import random
import math
import string
import numpy as np
import pandas as pd
from sklearn import preprocessing

from pymilvus import DataType
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from common import common_type as ct
from utils.util_log import test_log as log
import traceback

"""" Methods of processing data """


# l2 = lambda x, y: np.linalg.norm(np.array(x) - np.array(y))


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_str_by_length(length=8):
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def gen_bool_field(name=ct.default_bool_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    bool_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BOOL, description=description,
                                                              is_primary=is_primary, **kwargs)
    return bool_field


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


def gen_float_field(name=ct.default_float_field_name, is_primary=False, description=ct.default_desc):
    float_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT, description=description,
                                                               is_primary=is_primary)
    return float_field


def gen_double_field(name=ct.default_double_field_name, is_primary=False, description=ct.default_desc):
    double_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.DOUBLE,
                                                                description=description, is_primary=is_primary)
    return double_field


def gen_float_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                        description=ct.default_desc):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary)
    return float_vec_field


def gen_binary_vec_field(name=ct.default_binary_vec_field_name, is_primary=False, dim=ct.default_dim,
                         description=ct.default_desc):
    binary_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BINARY_VECTOR,
                                                                    description=description, dim=dim,
                                                                    is_primary=is_primary)
    return binary_vec_field


def gen_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, dim=ct.default_dim):
    fields = [gen_int64_field(), gen_float_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id)
    return schema


def gen_collection_schema_all_datatype(description=ct.default_desc,
                                       primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim):
    fields = [gen_int64_field(), gen_int32_field(), gen_int16_field(), gen_int8_field(),
              gen_bool_field(), gen_float_field(), gen_double_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id)
    return schema


def gen_collection_schema(fields, primary_field=None, description=ct.default_desc, auto_id=False):
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, primary_field=primary_field,
                                                                    description=description, auto_id=auto_id)
    return schema


def gen_default_binary_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                         auto_id=False, dim=ct.default_dim):
    fields = [gen_int64_field(), gen_float_field(), gen_binary_vec_field(dim=dim)]
    binary_schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                           primary_field=primary_field,
                                                                           auto_id=auto_id)
    return binary_schema


def gen_schema_multi_vector_fields(vec_fields):
    fields = [gen_int64_field(), gen_float_field(), gen_float_vec_field()]
    fields.extend(vec_fields)
    primary_field = ct.default_int64_field_name
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=ct.default_desc,
                                                                    primary_field=primary_field, auto_id=False)
    return schema


def gen_vectors(nb, dim):
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for _ in range(num):
        raw_vector = [random.randint(0, 1) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        # packs a binary-valued array into bits in a unit8 array, and bytes array_of_ints
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def gen_default_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[float(i) for i in range(start, start + nb)], dtype="float32")
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_float_vec_field_name: float_vec_values
    })
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
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
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


def gen_dataframe_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0):
    int64_values = pd.Series(data=[i for i in range(start, start + nb)])
    int32_values = pd.Series(data=[np.int32(i) for i in range(start, start + nb)], dtype="int32")
    int16_values = pd.Series(data=[np.int16(i) for i in range(start, start + nb)], dtype="int16")
    int8_values = pd.Series(data=[np.int8(i) for i in range(start, start + nb)], dtype="int8")
    bool_values = pd.Series(data=[np.bool(i) for i in range(start, start + nb)], dtype="bool")
    float_values = pd.Series(data=[float(i) for i in range(start, start + nb)], dtype="float32")
    double_values = pd.Series(data=[np.double(i) for i in range(start, start + nb)], dtype="double")
    # string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int64_values,
        ct.default_int32_field_name: int32_values,
        ct.default_int16_field_name: int16_values,
        ct.default_int8_field_name: int8_values,
        ct.default_bool_field_name: bool_values,
        ct.default_float_field_name: float_values,
        # ct.default_string_field_name: string_values,
        ct.default_double_field_name: double_values,
        ct.default_float_vec_field_name: float_vec_values
    })
    return df


def gen_default_binary_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[float(i) for i in range(start, start + nb)], dtype="float32")
    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_binary_vec_field_name: binary_vec_values
    })
    return df, binary_raw_values


def gen_default_list_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = [int_values, float_values, float_vec_values]
    return data


def gen_default_tuple_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [float(i) for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = (int_values, float_values, float_vec_values)
    return data


def gen_numpy_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = np.arange(nb, dtype='int64')
    float_values = np.arange(nb, dtype='float32')
    float_vec_values = gen_vectors(nb, dim)
    data = [int_values, float_values, float_vec_values]
    return data


def gen_default_binary_list_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    data = [int_values, float_values, binary_vec_values]
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


def gen_invaild_search_params_type():
    invalid_search_key = 100
    search_params = []
    for index_type in ct.all_index_types:
        if index_type == "FLAT":
            continue
        search_params.append({"index_type": index_type, "search_params": {"invalid_key": invalid_search_key}})
        if index_type in ["IVF_FLAT", "IVF_SQ8", "IVF_SQ8H", "IVF_PQ"]:
            for nprobe in ct.get_invalid_ints:
                ivf_search_params = {"index_type": index_type, "search_params": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
        elif index_type in ["HNSW", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ"]:
            for ef in ct.get_invalid_ints:
                hnsw_search_param = {"index_type": index_type, "search_params": {"ef": ef}}
                search_params.append(hnsw_search_param)
        elif index_type in ["NSG", "RNSG"]:
            for search_length in ct.get_invalid_ints:
                nsg_search_param = {"index_type": index_type, "search_params": {"search_length": search_length}}
                search_params.append(nsg_search_param)
            search_params.append({"index_type": index_type, "search_params": {"invalid_key": invalid_search_key}})
        elif index_type == "ANNOY":
            for search_k in ct.get_invalid_ints:
                if isinstance(search_k, int):
                    continue
                annoy_search_param = {"index_type": index_type, "search_params": {"search_k": search_k}}
                search_params.append(annoy_search_param)
    return search_params


def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_SQ8H", "IVF_PQ"] \
            or index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [64, 128]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["HNSW", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ"]:
        for ef in [64, 32768]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type in ["NSG", "RNSG"]:
        for search_length in [100, 300]:
            nsg_search_param = {"metric_type": metric_type, "params": {"search_length": search_length}}
            search_params.append(nsg_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000, 5000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
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
        "200+300 < int64 <= 500+500"
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
        f"200+300 <= {field} <= 500+500"
    ]
    return expressions


def l2(x, y):
    return np.linalg.norm(np.array(x) - np.array(y))


def ip(x, y):
    return np.inner(np.array(x), np.array(y))


def jaccard(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())


def hamming(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return np.bitwise_xor(x, y).sum()


def tanimoto(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return -np.log2(np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum()))


def tanimoto_calc(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return np.double((len(x) - np.bitwise_xor(x, y).sum())) / (len(y) + np.bitwise_xor(x, y).sum())


def substructure(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(y)


def superstructure(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
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


def insert_data(collection_w, nb=3000, is_binary=False, is_all_data_type=False,
                auto_id=False, dim=ct.default_dim):
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
    log.info("insert_data: inserting data into collection %s (num_entities: %s)"
             % (collection_w.name, nb))
    for i in range(num):
        default_data = gen_default_dataframe_data(nb // num, dim=dim)
        if is_binary:
            default_data, binary_raw_data = gen_default_binary_dataframe_data(nb // num, dim=dim)
            binary_raw_vectors.extend(binary_raw_data)
        if is_all_data_type:
            default_data = gen_dataframe_all_data_type(nb // num, dim=dim)
        if auto_id:
            default_data.drop(ct.default_int64_field_name, axis=1, inplace=True)
        insert_res = collection_w.insert(default_data, par[i].name)[0]
        insert_ids.extend(insert_res.primary_keys)
        vectors.append(default_data)
    log.info("insert_data: inserted data into collection %s (num_entities: %s)"
             % (collection_w.name, nb))
    return collection_w, vectors, binary_raw_vectors, insert_ids


def _check_primary_keys(primary_keys, nb):
    if primary_keys is None:
        raise Exception("The primary_keys is None")
    assert len(primary_keys) == nb
    for i in range(nb - 1):
        if primary_keys[i] >= primary_keys[i + 1]:
            return False
    return True
