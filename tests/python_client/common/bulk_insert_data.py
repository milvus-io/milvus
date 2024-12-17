import copy
import json
import os
import time

import numpy as np
from ml_dtypes import bfloat16
import pandas as pd
import random
from pathlib import Path
import uuid
from faker import Faker
from sklearn import preprocessing
from common.common_func import gen_unique_str
from common.minio_comm import copy_files_to_minio
from utils.util_log import test_log as log

data_source = "/tmp/bulk_insert_data"
fake = Faker()
BINARY = "binary"
FLOAT = "float"


class DataField:
    pk_field = "uid"
    vec_field = "vectors"
    float_vec_field = "float32_vectors"
    sparse_vec_field = "sparse_vectors"
    image_float_vec_field = "image_float_vec_field"
    text_float_vec_field = "text_float_vec_field"
    binary_vec_field = "binary_vec_field"
    bf16_vec_field = "brain_float16_vec_field"
    fp16_vec_field = "float16_vec_field"
    int_field = "int_scalar"
    string_field = "string_scalar"
    bool_field = "bool_scalar"
    float_field = "float_scalar"
    double_field = "double_scalar"
    json_field = "json"
    array_bool_field = "array_bool"
    array_int_field = "array_int"
    array_float_field = "array_float"
    array_string_field = "array_string"


class DataErrorType:
    one_entity_wrong_dim = "one_entity_wrong_dim"
    str_on_int_pk = "str_on_int_pk"
    int_on_float_scalar = "int_on_float_scalar"
    float_on_int_pk = "float_on_int_pk"
    typo_on_bool = "typo_on_bool"
    str_on_float_scalar = "str_on_float_scalar"
    str_on_vector_field = "str_on_vector_field"
    empty_array_field = "empty_array_field"
    mismatch_type_array_field = "mismatch_type_array_field"


def gen_file_prefix(is_row_based=True, auto_id=True, prefix=""):
    if is_row_based:
        if auto_id:
            return f"{prefix}_row_auto"
        else:
            return f"{prefix}_row_cust"
    else:
        if auto_id:
            return f"{prefix}_col_auto"
        else:
            return f"{prefix}_col_cust"


def entity_suffix(rows):
    if rows // 1000000 > 0:
        suffix = f"{rows // 1000000}m"
    elif rows // 1000 > 0:
        suffix = f"{rows // 1000}k"
    else:
        suffix = f"{rows}"
    return suffix


def gen_float_vectors(nb, dim):
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


def gen_str_invalid_vectors(nb, dim):
    vectors = [[str(gen_unique_str()) for _ in range(dim)] for _ in range(nb)]
    return vectors


def gen_binary_vectors(nb, dim):
    # binary: each int presents 8 dimension
    # so if binary vector dimension is 16，use [x, y], which x and y could be any int between 0 and 255
    vectors = [[random.randint(0, 255) for _ in range(dim)] for _ in range(nb)]
    return vectors


def gen_fp16_vectors(num, dim, for_json=False):
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
        if for_json:
            fp16_vector = np.array(raw_vector, dtype=np.float16).tolist()
        else:
            fp16_vector = np.array(raw_vector, dtype=np.float16).view(np.uint8).tolist()
        fp16_vectors.append(fp16_vector)

    return raw_vectors, fp16_vectors


def gen_bf16_vectors(num, dim, for_json=False):
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
        if for_json:
            bf16_vector = np.array(raw_vector, dtype=bfloat16).tolist()
        else:
            bf16_vector = np.array(raw_vector, dtype=bfloat16).view(np.uint8).tolist()
        bf16_vectors.append(bf16_vector)

    return raw_vectors, bf16_vectors


def gen_row_based_json_file(row_file, str_pk, data_fields, float_vect,
                            rows, dim, start_uid=0, err_type="", enable_dynamic_field=False,  **kwargs):

    if err_type == DataErrorType.str_on_int_pk:
        str_pk = True
    if err_type in [DataErrorType.one_entity_wrong_dim, DataErrorType.str_on_vector_field]:
        wrong_dim = dim + 8     # add 8 to compatible with binary vectors
        wrong_row = kwargs.get("wrong_position", start_uid)

    with open(row_file, "w") as f:
        f.write("{")
        f.write("\n")
        f.write('"rows":[')
        f.write("\n")
        for i in range(rows):
            if i > 0:
                f.write(",")
                f.write("\n")

            # scalar fields
            f.write('{')
            for j in range(len(data_fields)):
                data_field = data_fields[j]
                if data_field == DataField.pk_field:
                    if str_pk:
                        line = '"uid":"' + str(gen_unique_str()) + '"'
                        f.write(line)
                        # f.write('"uid":"' + str(gen_unique_str()) + '"')
                    else:
                        if err_type == DataErrorType.float_on_int_pk:
                            f.write('"uid":' + str(i + start_uid + random.random()) + '')
                        else:
                            f.write('"uid":' + str(i + start_uid) + '')
                if data_field == DataField.int_field:
                    if DataField.pk_field in data_fields:
                        # if not auto_id, use the same value as pk to check the query results later
                        f.write('"int_scalar":' + str(i + start_uid) + '')
                    else:
                        line = '"int_scalar":' + str(random.randint(-999999, 9999999)) + ''
                        f.write(line)
                if data_field == DataField.float_field:
                    if err_type == DataErrorType.int_on_float_scalar:
                        f.write('"float_scalar":' + str(random.randint(-999999, 9999999)) + '')
                    elif err_type == DataErrorType.str_on_float_scalar:
                        f.write('"float_scalar":"' + str(gen_unique_str()) + '"')
                    else:
                        line = '"float_scalar":' + str(random.random()) + ''
                        f.write(line)
                if data_field == DataField.double_field:
                    if err_type == DataErrorType.int_on_float_scalar:
                        f.write('"double_scalar":' + str(random.randint(-999999, 9999999)) + '')
                    elif err_type == DataErrorType.str_on_float_scalar:
                        f.write('"double_scalar":"' + str(gen_unique_str()) + '"')
                    else:
                        line = '"double_scalar":' + str(random.random()) + ''
                        f.write(line)
                if data_field == DataField.string_field:
                    f.write('"string_scalar":"' + str(gen_unique_str()) + '"')
                if data_field == DataField.bool_field:
                    if err_type == DataErrorType.typo_on_bool:
                        f.write('"bool_scalar":' + str(random.choice(["True", "False", "TRUE", "FALSE", "0", "1"])) + '')
                    else:
                        f.write('"bool_scalar":' + str(random.choice(["true", "false"])) + '')
                if data_field == DataField.json_field:
                    data = {
                        gen_unique_str(): random.randint(-999999, 9999999),
                    }
                    f.write('"json":' + json.dumps(data) + '')
                if data_field == DataField.array_bool_field:
                    if err_type == DataErrorType.empty_array_field:
                        f.write('"array_bool":[]')
                    elif err_type == DataErrorType.mismatch_type_array_field:
                        f.write('"array_bool": "mistype"')
                    else:

                        f.write('"array_bool":[' + str(random.choice(["true", "false"])) + ',' + str(random.choice(["true", "false"])) + ']')
                if data_field == DataField.array_int_field:
                    if err_type == DataErrorType.empty_array_field:
                        f.write('"array_int":[]')
                    elif err_type == DataErrorType.mismatch_type_array_field:
                        f.write('"array_int": "mistype"')
                    else:
                        f.write('"array_int":[' + str(random.randint(-999999, 9999999)) + ',' + str(random.randint(-999999, 9999999)) + ']')
                if data_field == DataField.array_float_field:
                    if err_type == DataErrorType.empty_array_field:
                        f.write('"array_float":[]')
                    elif err_type == DataErrorType.mismatch_type_array_field:
                        f.write('"array_float": "mistype"')
                    else:
                        f.write('"array_float":[' + str(random.random()) + ',' + str(random.random()) + ']')
                if data_field == DataField.array_string_field:
                    if err_type == DataErrorType.empty_array_field:
                        f.write('"array_string":[]')
                    elif err_type == DataErrorType.mismatch_type_array_field:
                        f.write('"array_string": "mistype"')
                    else:
                        f.write('"array_string":["' + str(gen_unique_str()) + '","' + str(gen_unique_str()) + '"]')

                if data_field == DataField.vec_field:
                    # vector field
                    if err_type == DataErrorType.one_entity_wrong_dim and i == wrong_row:
                        vectors = gen_float_vectors(1, wrong_dim) if float_vect else gen_binary_vectors(1, (wrong_dim//8))
                    elif err_type == DataErrorType.str_on_vector_field and i == wrong_row:
                        vectors = gen_str_invalid_vectors(1, dim) if float_vect else gen_str_invalid_vectors(1, dim//8)
                    else:
                        vectors = gen_float_vectors(1, dim) if float_vect else gen_binary_vectors(1, (dim//8))
                    line = '"vectors":' + ",".join(str(x).replace("'", '"') for x in vectors) + ''
                    f.write(line)
                # not write common for the last field
                if j != len(data_fields) - 1:
                    f.write(',')
            if enable_dynamic_field:
                d = {str(i+start_uid): i+start_uid, "name": fake.name(), "address": fake.address()}
                d_str = json.dumps(d)
                d_str = d_str[1:-1] # remove {}
                f.write("," + d_str)
            f.write('}')
        f.write("\n")
        f.write("]")
        f.write("\n")
        f.write("}")
        f.write("\n")


def gen_column_base_json_file(col_file, str_pk, data_fields, float_vect,
                              rows, dim, start_uid=0, err_type="", **kwargs):
    if err_type == DataErrorType.str_on_int_pk:
        str_pk = True

    with open(col_file, "w") as f:
        f.write("{")
        f.write("\n")
        if rows > 0:
            # data columns
            for j in range(len(data_fields)):
                data_field = data_fields[j]
                if data_field == DataField.pk_field:
                    if str_pk:
                        f.write('"uid":["' + ',"'.join(str(gen_unique_str()) + '"' for i in range(rows)) + ']')
                        f.write("\n")
                    else:
                        if err_type == DataErrorType.float_on_int_pk:
                            f.write('"uid":[' + ",".join(
                                str(i + random.random()) for i in range(start_uid, start_uid + rows)) + "]")
                        else:
                            f.write('"uid":[' + ",".join(str(i) for i in range(start_uid, start_uid + rows)) + "]")
                        f.write("\n")
                if data_field == DataField.int_field:
                    if DataField.pk_field in data_fields:
                        # if not auto_id, use the same value as pk to check the query results later
                        f.write('"int_scalar":[' + ",".join(str(i) for i in range(start_uid, start_uid + rows)) + "]")
                    else:
                        f.write('"int_scalar":[' + ",".join(str(
                            random.randint(-999999, 9999999)) for i in range(rows)) + "]")
                    f.write("\n")
                if data_field == DataField.float_field:
                    if err_type == DataErrorType.int_on_float_scalar:
                        f.write('"float_scalar":[' + ",".join(
                            str(random.randint(-999999, 9999999)) for i in range(rows)) + "]")
                    elif err_type == DataErrorType.str_on_float_scalar:
                        f.write('"float_scalar":["' + ',"'.join(str(
                            gen_unique_str()) + '"' for i in range(rows)) + ']')
                    else:
                        f.write('"float_scalar":[' + ",".join(
                            str(random.random()) for i in range(rows)) + "]")
                    f.write("\n")
                if data_field == DataField.string_field:
                    f.write('"string_scalar":["' + ',"'.join(str(
                        gen_unique_str()) + '"' for i in range(rows)) + ']')
                    f.write("\n")
                if data_field == DataField.bool_field:
                    if err_type == DataErrorType.typo_on_bool:
                        f.write('"bool_scalar":[' + ",".join(
                            str(random.choice(["True", "False", "TRUE", "FALSE", "1", "0"])) for i in range(rows)) + "]")
                    else:
                        f.write('"bool_scalar":[' + ",".join(
                            str(random.choice(["true", "false"])) for i in range(rows)) + "]")
                    f.write("\n")
                if data_field == DataField.vec_field:
                    # vector columns
                    if err_type == DataErrorType.one_entity_wrong_dim:
                        wrong_dim = dim + 8  # add 8 to compatible with binary vectors
                        wrong_row = kwargs.get("wrong_position", 0)
                        if wrong_row <= 0:
                            vectors1 = []
                        else:
                            vectors1 = gen_float_vectors(wrong_row, dim) if float_vect else \
                                gen_binary_vectors(wrong_row, (dim//8))
                        if wrong_row >= rows -1:
                            vectors2 = []
                        else:
                            vectors2 = gen_float_vectors(rows-wrong_row-1, dim) if float_vect else\
                                gen_binary_vectors(rows-wrong_row-1, (dim//8))
                        vectors_wrong_dim = gen_float_vectors(1, wrong_dim) if float_vect else \
                            gen_binary_vectors(1, (wrong_dim//8))
                        vectors = vectors1 + vectors_wrong_dim + vectors2
                    elif err_type == DataErrorType.str_on_vector_field:
                        wrong_row = kwargs.get("wrong_position", 0)
                        if wrong_row <= 0:
                            vectors1 = []
                        else:
                            vectors1 = gen_float_vectors(wrong_row, dim) if float_vect else \
                                gen_binary_vectors(wrong_row, (dim//8))
                        if wrong_row >= rows - 1:
                            vectors2 = []
                        else:
                            vectors2 = gen_float_vectors(rows - wrong_row - 1, dim) if float_vect else \
                                gen_binary_vectors(rows - wrong_row - 1, (dim // 8))
                        invalid_str_vectors = gen_str_invalid_vectors(1, dim) if float_vect else \
                            gen_str_invalid_vectors(1, (dim // 8))
                        vectors = vectors1 + invalid_str_vectors + vectors2
                    else:
                        vectors = gen_float_vectors(rows, dim) if float_vect else gen_binary_vectors(rows, (dim//8))
                    f.write('"vectors":[' + ",".join(str(x).replace("'", '"') for x in vectors) + "]")
                    f.write("\n")
                if j != len(data_fields) - 1:
                    f.write(",")
        f.write("}")
        f.write("\n")


def gen_vectors_in_numpy_file(dir, data_field, float_vector, rows, dim, vector_type="float32", force=False):
    file_name = f"{data_field}.npy"
    file = f'{dir}/{file_name}'

    if not os.path.exists(file) or force:
        # vector columns
        vectors = []
        if rows > 0:
            if vector_type == "float32":
                vectors = gen_float_vectors(rows, dim)
                arr = np.array(vectors)
            elif vector_type == "fp16":
                vectors = gen_fp16_vectors(rows, dim)[1]
                arr = np.array(vectors, dtype=np.dtype("uint8"))
            elif vector_type == "bf16":
                vectors = gen_bf16_vectors(rows, dim)[1]
                arr = np.array(vectors, dtype=np.dtype("uint8"))
            elif vector_type == "binary":
                vectors = gen_binary_vectors(rows, (dim // 8))
                arr = np.array(vectors, dtype=np.dtype("uint8"))
            else:
                vectors = gen_binary_vectors(rows, (dim // 8))
                arr = np.array(vectors, dtype=np.dtype("uint8"))
            log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
            np.save(file, arr)
    return file_name


def gen_string_in_numpy_file(dir, data_field, rows, start=0, force=False):
    file_name = f"{data_field}.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        # non vector columns
        data = []
        if rows > 0:
            data = [gen_unique_str(str(i)) for i in range(start, rows+start)]
        arr = np.array(data)
        # print(f"file_name: {file_name} data type: {arr.dtype}")
        log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
        np.save(file, arr)
    return file_name


def gen_dynamic_field_in_numpy_file(dir, rows, start=0, force=False):
    file_name = f"$meta.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        # non vector columns
        data = []
        if rows > 0:
            data = [json.dumps({str(i): i, "name": fake.name(), "address": fake.address()}) for i in range(start, rows+start)]
        arr = np.array(data)
        log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
        np.save(file, arr)
    return file_name


def gen_bool_in_numpy_file(dir, data_field, rows, start=0, force=False):
    file_name = f"{data_field}.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        # non vector columns
        data = []
        if rows > 0:
            data = [random.choice([True, False]) for i in range(start, rows+start)]
        arr = np.array(data)
        # print(f"file_name: {file_name} data type: {arr.dtype}")
        log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
        np.save(file, arr)
    return file_name


def gen_json_in_numpy_file(dir, data_field, rows, start=0, force=False):
    file_name = f"{data_field}.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        data = []
        if rows > 0:
            data = [json.dumps({"name": fake.name(), "address": fake.address()}) for i in range(start, rows+start)]
        arr = np.array(data)
        log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
        np.save(file, arr)
    return file_name


def gen_int_or_float_in_numpy_file(dir, data_field, rows, start=0, force=False):
    file_name = f"{data_field}.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        # non vector columns
        data = []
        # arr = np.array([])
        if rows > 0:
            if data_field == DataField.float_field:
                data = [np.float32(random.random()) for _ in range(rows)]
            elif data_field == DataField.double_field:
                data = [np.float64(random.random()) for _ in range(rows)]
            elif data_field == DataField.pk_field:
                data = [i for i in range(start, start + rows)]
            elif data_field == DataField.int_field:
                data = [random.randint(-999999, 9999999) for _ in range(rows)]
            arr = np.array(data)
            log.info(f"file_name: {file_name} data type: {arr.dtype} data shape: {arr.shape}")
            np.save(file, arr)
    return file_name


def gen_vectors(float_vector, rows, dim):
    vectors = []
    if rows > 0:
        if float_vector:
            vectors = gen_float_vectors(rows, dim)
        else:
            vectors = gen_binary_vectors(rows, (dim // 8))
    return vectors


def gen_sparse_vectors(rows, sparse_format="dok"):
    # default sparse format is dok, dict of keys
    # another option is coo, coordinate List

    rng = np.random.default_rng()
    vectors = [{
        d: rng.random() for d in random.sample(range(1000), random.randint(20, 30))
    } for _ in range(rows)]
    if sparse_format == "coo":
        vectors = [
            {"indices": list(x.keys()), "values": list(x.values())} for x in vectors
        ]
    return vectors


def gen_data_by_data_field(data_field, rows, start=0, float_vector=True, dim=128, array_length=None, sparse_format="dok", **kwargs):
    if array_length is None:
        array_length = random.randint(0, 10)
    schema = kwargs.get("schema", None)
    schema = schema.to_dict() if schema is not None else None
    if schema is not None:
        fields = schema.get("fields", [])
        for field in fields:
            if data_field == field["name"] and "params" in field:
                dim = field["params"].get("dim", dim)
    data = []
    if rows > 0:
        if "vec" in data_field:
            if "float" in data_field and "16" not in data_field:
                data = gen_vectors(float_vector=True, rows=rows, dim=dim)
                data = pd.Series([np.array(x, dtype=np.dtype("float32")) for x in data])
            elif "sparse" in data_field:
                data = gen_sparse_vectors(rows, sparse_format=sparse_format)
                data = pd.Series([json.dumps(x) for x in data], dtype=np.dtype("str"))
            elif "float16" in data_field:
                data = gen_fp16_vectors(rows, dim)[1]
                data = pd.Series([np.array(x, dtype=np.dtype("uint8")) for x in data])
            elif "brain_float16" in data_field:
                data = gen_bf16_vectors(rows, dim)[1]
                data = pd.Series([np.array(x, dtype=np.dtype("uint8")) for x in data])
            elif "binary" in data_field:
                data = gen_vectors(float_vector=False, rows=rows, dim=dim)
                data = pd.Series([np.array(x, dtype=np.dtype("uint8")) for x in data])
            else:
                data = gen_vectors(float_vector=float_vector, rows=rows, dim=dim)
        elif data_field == DataField.float_field:
            data = [np.float32(random.random()) for _ in range(rows)]
        elif data_field == DataField.double_field:
            data = [np.float64(random.random()) for _ in range(rows)]
        elif data_field == DataField.pk_field:
            data = [np.int64(i) for i in range(start, start + rows)]
        elif data_field == DataField.int_field:
            data = [np.int64(random.randint(-999999, 9999999)) for _ in range(rows)]
        elif data_field == DataField.string_field:
            data = [gen_unique_str(str(i)) for i in range(start, rows + start)]
        elif data_field == DataField.bool_field:
            data = [random.choice([True, False]) for i in range(start, rows + start)]
        elif data_field == DataField.json_field:
            data = pd.Series([json.dumps({
                gen_unique_str(): random.randint(-999999, 9999999)
            }) for i in range(start, rows + start)], dtype=np.dtype("str"))
        elif data_field == DataField.array_bool_field:
            data = pd.Series(
                    [np.array([random.choice([True, False]) for _ in range(array_length)], dtype=np.dtype("bool"))
                     for i in range(start, rows + start)])
        elif data_field == DataField.array_int_field:
            data = pd.Series(
                    [np.array([random.randint(-999999, 9999999) for _ in range(array_length)], dtype=np.dtype("int64"))
                     for i in range(start, rows + start)])
        elif data_field == DataField.array_float_field:
            data = pd.Series(
                    [np.array([random.random() for _ in range(array_length)], dtype=np.dtype("float32"))
                     for i in range(start, rows + start)])
        elif data_field == DataField.array_string_field:
            data = pd.Series(
                    [np.array([gen_unique_str(str(i)) for _ in range(array_length)], dtype=np.dtype("str"))
                     for i in range(start, rows + start)])
    return data


def gen_file_name(is_row_based, rows, dim, auto_id, str_pk,
                  float_vector, data_fields, file_num, file_type, err_type):
    row_suffix = entity_suffix(rows)
    field_suffix = ""
    if len(data_fields) > 3:
        field_suffix = "multi_scalars_"
    else:
        for data_field in data_fields:
            if data_field != DataField.vec_field:
                field_suffix += f"{data_field}_"

    vt = ""
    if DataField.vec_field in data_fields:
        vt = "float_vectors_" if float_vector else "binary_vectors_"

    pk = ""
    if str_pk:
        pk = "str_pk_"
    prefix = gen_file_prefix(is_row_based=is_row_based, auto_id=auto_id, prefix=err_type)

    file_name = f"{prefix}_{pk}{vt}{field_suffix}{dim}d_{row_suffix}_{file_num}_{int(time.time())}{file_type}"
    return file_name


def gen_subfolder(root, dim, rows, file_num):
    suffix = entity_suffix(rows)
    subfolder = f"{dim}d_{suffix}_{file_num}"
    path = f"{root}/{subfolder}"
    if not os.path.isdir(path):
        os.mkdir(path)
    return subfolder


def gen_json_files(is_row_based, rows, dim, auto_id, str_pk,
                   float_vector, data_fields, file_nums, multi_folder,
                   file_type, err_type, force, **kwargs):
    # gen json files
    files = []
    start_uid = 0
    # make sure pk field exists when not auto_id
    if (not auto_id) and (DataField.pk_field not in data_fields):
        data_fields.append(DataField.pk_field)
    for i in range(file_nums):
        file_name = gen_file_name(is_row_based=is_row_based, rows=rows, dim=dim,
                                  auto_id=auto_id, str_pk=str_pk, float_vector=float_vector,
                                  data_fields=data_fields, file_num=i, file_type=file_type, err_type=err_type)
        file = f"{data_source}/{file_name}"
        if multi_folder:
            subfolder = gen_subfolder(root=data_source, dim=dim, rows=rows, file_num=i)
            file = f"{data_source}/{subfolder}/{file_name}"
        if not os.path.exists(file) or force:
            if is_row_based:
                gen_row_based_json_file(row_file=file, str_pk=str_pk, float_vect=float_vector,
                                        data_fields=data_fields, rows=rows, dim=dim,
                                        start_uid=start_uid, err_type=err_type, **kwargs)
            else:
                gen_column_base_json_file(col_file=file, str_pk=str_pk, float_vect=float_vector,
                                          data_fields=data_fields, rows=rows, dim=dim,
                                          start_uid=start_uid, err_type=err_type, **kwargs)
            start_uid += rows
        if multi_folder:
            files.append(f"{subfolder}/{file_name}")
        else:
            files.append(file_name)
    return files


def gen_dict_data_by_data_field(data_fields, rows, start=0, float_vector=True, dim=128, array_length=None, enable_dynamic_field=False, **kwargs):
    schema = kwargs.get("schema", None)
    schema = schema.to_dict() if schema is not None else None
    data = []
    for r in range(rows):
        d = {}
        for data_field in data_fields:
            if schema is not None:
                fields = schema.get("fields", [])
                for field in fields:
                    if data_field == field["name"] and "params" in field:
                        dim = field["params"].get("dim", dim)

            if "vec" in data_field:
                if "float" in data_field:
                    float_vector = True
                    d[data_field] = gen_vectors(float_vector=float_vector, rows=1, dim=dim)[0]
                if "sparse" in data_field:
                    sparse_format = kwargs.get("sparse_format", "dok")
                    d[data_field] = gen_sparse_vectors(1, sparse_format=sparse_format)[0]
                if "binary" in data_field:
                    float_vector = False
                    d[data_field] = gen_vectors(float_vector=float_vector, rows=1, dim=dim)[0]
                if "bf16" in data_field:
                    d[data_field] = gen_bf16_vectors(1, dim, True)[1][0]
                if "fp16" in data_field:
                    d[data_field] = gen_fp16_vectors(1, dim, True)[1][0]
            elif data_field == DataField.float_field:
                d[data_field] = random.random()
            elif data_field == DataField.double_field:
                d[data_field] = random.random()
            elif data_field == DataField.pk_field:
                d[data_field] = r+start
            elif data_field == DataField.int_field:
                d[data_field] =random.randint(-999999, 9999999)
            elif data_field == DataField.string_field:
                d[data_field] = gen_unique_str(str(r + start))
            elif data_field == DataField.bool_field:
                d[data_field] = random.choice([True, False])
            elif data_field == DataField.json_field:
                d[data_field] = {str(r+start): r+start}
            elif data_field == DataField.array_bool_field:
                array_length = random.randint(0, 10) if array_length is None else array_length
                d[data_field] = [random.choice([True, False]) for _ in range(array_length)]
            elif data_field == DataField.array_int_field:
                array_length = random.randint(0, 10) if array_length is None else array_length
                d[data_field] = [random.randint(-999999, 9999999) for _ in range(array_length)]
            elif data_field == DataField.array_float_field:
                array_length = random.randint(0, 10) if array_length is None else array_length
                d[data_field] = [random.random() for _ in range(array_length)]
            elif data_field == DataField.array_string_field:
                array_length = random.randint(0, 10) if array_length is None else array_length
                d[data_field] = [gen_unique_str(str(i)) for i in range(array_length)]
        if enable_dynamic_field:
            d[str(r+start)] = r+start
            d["name"] = fake.name()
            d["address"] = fake.address()
        data.append(d)

    return data


def gen_new_json_files(float_vector, rows, dim, data_fields, file_nums=1, array_length=None, file_size=None, err_type="", enable_dynamic_field=False, **kwargs):
    schema = kwargs.get("schema", None)
    dir_prefix = f"json-{uuid.uuid4()}"
    data_source_new = f"{data_source}/{dir_prefix}"
    schema_file = f"{data_source_new}/schema.json"
    Path(schema_file).parent.mkdir(parents=True, exist_ok=True)
    if schema is not None:
        data = schema.to_dict()
        with open(schema_file, "w") as f:
            json.dump(data, f)
    files = []
    if file_size is not None:
        rows = 5000
    start_uid = 0
    for i in range(file_nums):
        file_name = f"data-fields-{len(data_fields)}-rows-{rows}-dim-{dim}-file-num-{i}-{int(time.time())}.json"
        file = f"{data_source_new}/{file_name}"
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        data = gen_dict_data_by_data_field(data_fields=data_fields, rows=rows, start=start_uid, float_vector=float_vector, dim=dim, array_length=array_length, enable_dynamic_field=enable_dynamic_field, **kwargs)
        # log.info(f"data: {data}")
        with open(file, "w") as f:
            json.dump(data, f)
        # get the file size
        if file_size is not None:
            batch_file_size = os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {rows} for {file_name}: {batch_file_size/1024/1024} MB")
            # calculate the rows to be generated
            total_batch = int(file_size*1024*1024*1024/batch_file_size)
            total_rows = total_batch * rows
            log.info(f"total_rows: {total_rows}")
            all_data = []
            for _ in range(total_batch):
                all_data += data
            file_name = f"data-fields-{len(data_fields)}-rows-{total_rows}-dim-{dim}-file-num-{i}-{int(time.time())}.json"
            with open(f"{data_source_new}/{file_name}", "w") as f:
                json.dump(all_data, f)
            batch_file_size = os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {total_rows} for {file_name}: {batch_file_size/1024/1024/1024} GB")
        files.append(file_name)
        start_uid += rows
    files = [f"{dir_prefix}/{f}" for f in files]
    return files


def gen_npy_files(float_vector, rows, dim, data_fields, file_size=None, file_nums=1, err_type="", force=False, enable_dynamic_field=False, include_meta=True, **kwargs):
    # gen numpy files
    schema = kwargs.get("schema", None)
    schema = schema.to_dict() if schema is not None else None
    u_id = f"numpy-{uuid.uuid4()}"
    data_source_new = f"{data_source}/{u_id}"
    schema_file = f"{data_source_new}/schema.json"
    Path(schema_file).parent.mkdir(parents=True, exist_ok=True)
    if schema is not None:
        with open(schema_file, "w") as f:
            json.dump(schema, f)
    files = []
    start_uid = 0
    if file_nums == 1:
        # gen the numpy file without subfolders if only one set of files
        for data_field in data_fields:
            if schema is not None:
                fields = schema.get("fields", [])
                for field in fields:
                    if data_field == field["name"] and "params" in field:
                        dim = field["params"].get("dim", dim)
            if "vec" in data_field:
                vector_type = "float32"
                if "float" in data_field:
                    float_vector = True
                    vector_type = "float32"
                if "binary" in data_field:
                    float_vector = False
                    vector_type = "binary"
                if "brain_float16" in data_field:
                    float_vector = True
                    vector_type = "bf16"
                if "float16" in data_field:
                    float_vector = True
                    vector_type = "fp16"

                file_name = gen_vectors_in_numpy_file(dir=data_source_new, data_field=data_field, float_vector=float_vector,
                                                      vector_type=vector_type, rows=rows, dim=dim, force=force)
            elif data_field == DataField.string_field:  # string field for numpy not supported yet at 2022-10-17
                file_name = gen_string_in_numpy_file(dir=data_source_new, data_field=data_field, rows=rows, force=force)
            elif data_field == DataField.bool_field:
                file_name = gen_bool_in_numpy_file(dir=data_source_new, data_field=data_field, rows=rows, force=force)
            elif data_field == DataField.json_field:
                file_name = gen_json_in_numpy_file(dir=data_source_new, data_field=data_field, rows=rows, force=force)
            else:
                file_name = gen_int_or_float_in_numpy_file(dir=data_source_new, data_field=data_field,
                                                           rows=rows, force=force)
            files.append(file_name)
        if enable_dynamic_field and include_meta:
            file_name = gen_dynamic_field_in_numpy_file(dir=data_source_new, rows=rows, force=force)
            files.append(file_name)
        if file_size is not None:
            batch_file_size = 0
            for file_name in files:
                batch_file_size += os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {rows} for {files}: {batch_file_size/1024/1024} MB")
            # calculate the rows to be generated
            total_batch = int(file_size*1024*1024*1024/batch_file_size)
            total_rows = total_batch * rows
            new_files = []
            for f in files:
                arr = np.load(f"{data_source_new}/{f}")
                all_arr = np.concatenate([arr for _ in range(total_batch)], axis=0)
                file_name = f
                np.save(f"{data_source_new}/{file_name}", all_arr)
                log.info(f"file_name: {file_name} data type: {all_arr.dtype} data shape: {all_arr.shape}")
                new_files.append(file_name)
            files = new_files
            batch_file_size = 0
            for file_name in files:
                batch_file_size += os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {total_rows} for {files}: {batch_file_size/1024/1024/1024} GB")

    else:
        for i in range(file_nums):
            subfolder = gen_subfolder(root=data_source_new, dim=dim, rows=rows, file_num=i)
            dir = f"{data_source_new}/{subfolder}"
            for data_field in data_fields:
                if DataField.vec_field in data_field:
                    file_name = gen_vectors_in_numpy_file(dir=dir, data_field=data_field, float_vector=float_vector, rows=rows, dim=dim, force=force)
                else:
                    file_name = gen_int_or_float_in_numpy_file(dir=dir, data_field=data_field, rows=rows, start=start_uid, force=force)
                files.append(f"{subfolder}/{file_name}")
            if enable_dynamic_field:
                file_name = gen_dynamic_field_in_numpy_file(dir=dir, rows=rows, start=start_uid, force=force)
                files.append(f"{subfolder}/{file_name}")
            start_uid += rows
    files = [f"{u_id}/{f}" for f in files]
    return files


def gen_dynamic_field_data_in_parquet_file(rows, start=0):
    data = []
    if rows > 0:
        data = pd.Series([json.dumps({str(i): i, "name": fake.name(), "address": fake.address()}) for i in range(start, rows+start)], dtype=np.dtype("str"))
    return data


def gen_parquet_files(float_vector, rows, dim, data_fields, file_size=None, row_group_size=None, file_nums=1, array_length=None, err_type="", enable_dynamic_field=False, include_meta=True, sparse_format="doc", **kwargs):
    schema = kwargs.get("schema", None)
    u_id = f"parquet-{uuid.uuid4()}"
    data_source_new = f"{data_source}/{u_id}"
    schema_file = f"{data_source_new}/schema.json"
    Path(schema_file).parent.mkdir(parents=True, exist_ok=True)
    if schema is not None:
        data = schema.to_dict()
        with open(schema_file, "w") as f:
            json.dump(data, f)

    # gen numpy files
    if err_type == "":
        err_type = "none"
    files = []
    #  generate 5000 entities and check the file size, then calculate the rows to be generated
    if file_size is not None:
        rows = 5000
    start_uid = 0
    if file_nums == 1:
        all_field_data = {}
        for data_field in data_fields:
            data = gen_data_by_data_field(data_field=data_field, rows=rows, start=0,
                                          float_vector=float_vector, dim=dim, array_length=array_length, sparse_format=sparse_format, **kwargs)
            all_field_data[data_field] = data
        if enable_dynamic_field and include_meta:
            all_field_data["$meta"] = gen_dynamic_field_data_in_parquet_file(rows=rows, start=0)
        df = pd.DataFrame(all_field_data)
        log.info(f"df: \n{df}")
        file_name = f"data-fields-{len(data_fields)}-rows-{rows}-dim-{dim}-file-num-{file_nums}-error-{err_type}-{int(time.time())}.parquet"
        if row_group_size is not None:
            df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow', row_group_size=row_group_size)
        else:
            df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow')
        # get the file size
        if file_size is not None:
            batch_file_size = os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {rows} for {file_name}: {batch_file_size/1024/1024} MB")
            # calculate the rows to be generated
            total_batch = int(file_size*1024*1024*1024/batch_file_size)
            total_rows = total_batch * rows
            all_df = pd.concat([df for _ in range(total_batch)], axis=0, ignore_index=True)
            file_name = f"data-fields-{len(data_fields)}-rows-{total_rows}-dim-{dim}-file-num-{file_nums}-error-{err_type}-{int(time.time())}.parquet"
            log.info(f"all df: \n {all_df}")
            if row_group_size is not None:
                all_df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow', row_group_size=row_group_size)
            else:
                all_df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow')
            batch_file_size = os.path.getsize(f"{data_source_new}/{file_name}")
            log.info(f"file_size with rows {total_rows} for {file_name}: {batch_file_size/1024/1024} MB")
        files.append(file_name)
    else:
        for i in range(file_nums):
            all_field_data = {}
            for data_field in data_fields:
                data = gen_data_by_data_field(data_field=data_field, rows=rows, start=0,
                                              float_vector=float_vector, dim=dim, array_length=array_length)
                all_field_data[data_field] = data
            if enable_dynamic_field:
                all_field_data["$meta"] = gen_dynamic_field_data_in_parquet_file(rows=rows, start=0)
            df = pd.DataFrame(all_field_data)
            file_name = f"data-fields-{len(data_fields)}-rows-{rows}-dim-{dim}-file-num-{i}-error-{err_type}-{int(time.time())}.parquet"
            if row_group_size is not None:
                df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow', row_group_size=row_group_size)
            else:
                df.to_parquet(f"{data_source_new}/{file_name}", engine='pyarrow')
            files.append(file_name)
            start_uid += rows
    files = [f"{u_id}/{f}" for f in files]
    return files


def prepare_bulk_insert_json_files(minio_endpoint="", bucket_name="milvus-bucket",
                                   is_row_based=True, rows=100, dim=128,
                                   auto_id=True, str_pk=False, float_vector=True,
                                   data_fields=[], file_nums=1, multi_folder=False,
                                   file_type=".json", err_type="", force=False, **kwargs):
    """
    Generate files based on the params in json format and copy them to minio

    :param minio_endpoint: the minio_endpoint of minio
    :type minio_endpoint: str

    :param bucket_name: the bucket name of Milvus
    :type bucket_name: str

    :param is_row_based: indicate the file(s) to be generated is row based or not
    :type is_row_based: boolean

    :param rows: the number entities to be generated in the file(s)
    :type rows: int

    :param dim: dim of vector data
    :type dim: int

    :param auto_id: generate primary key data or not
    :type auto_id: boolean

    :param str_pk: generate string or int as primary key
    :type str_pk: boolean

    :param: float_vector: generate float vectors or binary vectors
    :type float_vector: boolean

    :param: data_fields: data fields to be generated in the file(s):
            It supports one or all of [pk, vectors, int, float, string, boolean]
            Note: it automatically adds pk field if auto_id=False
    :type data_fields: list

    :param file_nums: file numbers to be generated
    :type file_nums: int

    :param multi_folder: generate the files in bucket root folder or new subfolders
    :type multi_folder: boolean

    :param file_type: specify the file suffix to be generate
    :type file_type: str

    :param err_type: inject some errors in the file(s).
        All errors should be predefined in DataErrorType
    :type err_type: str

    :param force: re-generate the file(s) regardless existing or not
    :type force: boolean

    :param **kwargs
        * *wrong_position* (``int``) --
        indicate the error entity in the file if DataErrorType.one_entity_wrong_dim

    :return list
        file names list
    """
    data_fields_c = copy.deepcopy(data_fields)
    log.info(f"data_fields: {data_fields}")
    log.info(f"data_fields_c: {data_fields_c}")

    files = gen_json_files(is_row_based=is_row_based, rows=rows, dim=dim,
                           auto_id=auto_id, str_pk=str_pk, float_vector=float_vector,
                           data_fields=data_fields_c, file_nums=file_nums, multi_folder=multi_folder,
                           file_type=file_type, err_type=err_type, force=force, **kwargs)

    copy_files_to_minio(host=minio_endpoint, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files


def prepare_bulk_insert_new_json_files(minio_endpoint="", bucket_name="milvus-bucket",
                                    rows=100, dim=128, float_vector=True, file_size=None,
                                    data_fields=[], file_nums=1, enable_dynamic_field=False,
                                    err_type="", force=False, **kwargs):

    log.info(f"data_fields: {data_fields}")
    files = gen_new_json_files(float_vector=float_vector, rows=rows, dim=dim,  data_fields=data_fields, file_nums=file_nums, file_size=file_size, err_type=err_type, enable_dynamic_field=enable_dynamic_field, **kwargs)

    copy_files_to_minio(host=minio_endpoint, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files


def prepare_bulk_insert_numpy_files(minio_endpoint="", bucket_name="milvus-bucket", rows=100, dim=128, enable_dynamic_field=False, file_size=None,
                                    data_fields=[DataField.vec_field], float_vector=True, file_nums=1, force=False, include_meta=True, **kwargs):
    """
    Generate column based files based on params in numpy format and copy them to the minio
    Note: each field in data_fields would be generated one numpy file.

    :param rows: the number entities to be generated in the file(s)
    :type rows: int

    :param dim: dim of vector data
    :type dim: int

    :param: float_vector: generate float vectors or binary vectors
    :type float_vector: boolean

    :param: data_fields: data fields to be generated in the file(s):
            it supports one or all of [int_pk, vectors, int, float]
            Note: it does not automatically add pk field
    :type data_fields: list

    :param file_nums: file numbers to be generated
        The file(s) would be  generated in data_source folder if file_nums = 1
        The file(s) would be generated in different sub-folders if file_nums > 1
    :type file_nums: int

    :param force: re-generate the file(s) regardless existing or not
    :type force: boolean

    Return: List
        File name list or file name with sub-folder list
    """
    files = gen_npy_files(rows=rows, dim=dim, float_vector=float_vector, file_size=file_size,
                          data_fields=data_fields, enable_dynamic_field=enable_dynamic_field,
                          file_nums=file_nums, force=force, include_meta=include_meta, **kwargs)

    copy_files_to_minio(host=minio_endpoint, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files


def prepare_bulk_insert_parquet_files(minio_endpoint="", bucket_name="milvus-bucket", rows=100, dim=128, array_length=None, file_size=None, row_group_size=None,
                                    enable_dynamic_field=False, data_fields=[DataField.vec_field], float_vector=True, file_nums=1, force=False, include_meta=True, sparse_format="doc", **kwargs):
    """
    Generate column based files based on params in parquet format and copy them to the minio
    Note: each field in data_fields would be generated one parquet file.

    :param rows: the number entities to be generated in the file(s)
    :type rows: int

    :param dim: dim of vector data
    :type dim: int

    :param: float_vector: generate float vectors or binary vectors
    :type float_vector: boolean

    :param: data_fields: data fields to be generated in the file(s):
            it supports one or all of [int_pk, vectors, int, float]
            Note: it does not automatically add pk field
    :type data_fields: list

    :param file_nums: file numbers to be generated
        The file(s) would be  generated in data_source folder if file_nums = 1
        The file(s) would be generated in different sub-folders if file_nums > 1
    :type file_nums: int

    :param force: re-generate the file(s) regardless existing or not
    :type force: boolean

    Return: List
        File name list or file name with sub-folder list
    """
    files = gen_parquet_files(rows=rows, dim=dim, float_vector=float_vector, enable_dynamic_field=enable_dynamic_field,
                              data_fields=data_fields, array_length=array_length, file_size=file_size, row_group_size=row_group_size,
                              file_nums=file_nums, include_meta=include_meta, sparse_format=sparse_format, **kwargs)
    copy_files_to_minio(host=minio_endpoint, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files


def gen_csv_file(file, float_vector, data_fields, rows, dim, start_uid):
    with open(file, "w") as f:
        # field name
        for i in range(len(data_fields)):
            f.write(data_fields[i])
            if i != len(data_fields) - 1:
                f.write(",")
        f.write("\n")

        for i in range(rows):
            # field value
            for j in range(len(data_fields)):
                data_field = data_fields[j]
                if data_field == DataField.pk_field:
                    f.write(str(i + start_uid))
                if data_field == DataField.int_field:
                    f.write(str(random.randint(-999999, 9999999)))
                if data_field == DataField.float_field:
                    f.write(str(random.random()))
                if data_field == DataField.string_field:
                    f.write(str(gen_unique_str()))
                if data_field == DataField.bool_field:
                    f.write(str(random.choice(["true", "false"])))
                if data_field == DataField.vec_field:
                    vectors = gen_float_vectors(1, dim) if float_vector else gen_binary_vectors(1, dim//8)
                    f.write('"' + ','.join(str(x) for x in vectors) + '"')
                if j != len(data_fields) - 1:
                    f.write(",")
            f.write("\n")


def gen_csv_files(rows, dim, auto_id, float_vector, data_fields, file_nums, force):
    files = []
    start_uid = 0
    if (not auto_id) and (DataField.pk_field not in data_fields):
        data_fields.append(DataField.pk_field)
    for i in range(file_nums):
        file_name = gen_file_name(is_row_based=True, rows=rows, dim=dim, auto_id=auto_id, float_vector=float_vector, data_fields=data_fields, file_num=i, file_type=".csv", str_pk=False, err_type="")
        file = f"{data_source}/{file_name}"
        if not os.path.exists(file) or force:
            gen_csv_file(file, float_vector, data_fields, rows, dim, start_uid)
        start_uid += rows
        files.append(file_name)
    return files


def prepare_bulk_insert_csv_files(minio_endpoint="", bucket_name="milvus-bucket", rows=100, dim=128, auto_id=True, float_vector=True, data_fields=[], file_nums=1, force=False):
    """
    Generate row based files based on params in csv format and copy them to minio

    :param minio_endpoint: the minio_endpoint of minio
    :type minio_endpoint: str

    :param bucket_name: the bucket name of Milvus
    :type bucket_name: str

    :param rows: the number entities to be generated in the file
    :type rows: int

    :param dim: dim of vector data
    :type dim: int

    :param auto_id: generate primary key data or not
    :type auto_id: bool

    :param float_vector: generate float vectors or binary vectors
    :type float_vector: boolean

    :param: data_fields: data fields to be generated in the file(s):
            It supports one or all of [pk, vectors, int, float, string, boolean]
            Note: it automatically adds pk field if auto_id=False
    :type data_fields: list

    :param file_nums: file numbers to be generated
    :type file_nums: int

    :param force: re-generate the file(s) regardless existing or not
    :type force: boolean
    """
    data_fields_c = copy.deepcopy(data_fields)
    log.info(f"data_fields: {data_fields}")
    log.info(f"data_fields_c: {data_fields_c}")
    files = gen_csv_files(rows=rows, dim=dim, auto_id=auto_id, float_vector=float_vector, data_fields=data_fields_c, file_nums=file_nums, force=force)
    copy_files_to_minio(host=minio_endpoint, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files
