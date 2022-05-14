import time
import os
import numpy as np
import random
from sklearn import preprocessing
from common.common_func import gen_unique_str
from minio_comm import copy_files_to_minio

# TODO: remove hardcode with input configurations
minio = "minio_address:port"     # minio service and port
bucket_name = "milvus-bulk-load"  # bucket name of milvus is using

data_source = "/tmp/bulk_load_data"

BINARY = "binary"
FLOAT = "float"


class DataField:
    pk_field = "uid"
    vec_field = "vectors"
    int_field = "int_scalar"
    string_field = "string_scalar"
    bool_field = "bool_scalar"
    float_field = "float_scalar"


class DataErrorType:
    one_entity_wrong_dim = "one_entity_wrong_dim"
    str_on_int_pk = "str_on_int_pk"
    int_on_float_scalar = "int_on_float_scalar"
    float_on_int_pk = "float_on_int_pk"
    typo_on_bool = "typo_on_bool"


def gen_file_prefix(row_based=True, auto_id=True, prefix=""):
    if row_based:
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


def gen_binary_vectors(nb, dim):
    # binary: each int presents 8 dimension
    # so if binary vector dimension is 16，use [x, y], which x and y could be any int between 0 to 255
    vectors = [[random.randint(0, 255) for _ in range(dim)] for _ in range(nb)]
    # vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors


def gen_row_based_json_file(row_file, str_pk, data_fields, float_vect,
                            rows, dim, start_uid=0, err_type="", **kwargs):

    if err_type == DataErrorType.str_on_int_pk:
        str_pk = True
    if err_type == DataErrorType.one_entity_wrong_dim:
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
                        f.write('"uid":"' + str(gen_unique_str()) + '"')
                    else:
                        if err_type == DataErrorType.float_on_int_pk:
                            f.write('"uid":' + str(i + start_uid + random.random()) + '')
                        else:
                            f.write('"uid":' + str(i + start_uid) + '')
                if data_field == DataField.int_field:
                    f.write('"int_scalar":' + str(random.randint(-999999, 9999999)) + '')
                if data_field == DataField.float_field:
                    if err_type == DataErrorType.int_on_float_scalar:
                        f.write('"float_scalar":' + str(random.randint(-999999, 9999999)) + '')
                    else:
                        f.write('"float_scalar":' + str(random.random()) + '')
                if data_field == DataField.string_field:
                    f.write('"string_scalar":"' + str(gen_unique_str()) + '"')
                if data_field == DataField.bool_field:
                    if err_type == DataErrorType.typo_on_bool:
                        f.write('"bool_scalar":' + str(random.choice(["True", "False", "TRUE", "FALSE", "0", "1"])) + '')
                    else:
                        f.write('"bool_scalar":' + str(random.choice(["true", "false"])) + '')
                if data_field == DataField.vec_field:
                    # vector field
                    if err_type == DataErrorType.one_entity_wrong_dim and i == wrong_row:
                        vectors = gen_float_vectors(1, wrong_dim) if float_vect else gen_binary_vectors(1, (wrong_dim // 8))
                    else:
                        vectors = gen_float_vectors(1, dim) if float_vect else gen_binary_vectors(1, (dim//8))
                    f.write('"vectors":' + ",".join(str(x) for x in vectors) + '')
                # not write common for the last field
                if j != len(data_fields) - 1:
                    f.write(',')
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
                    f.write('"int_scalar":[' + ",".join(str(
                        random.randint(-999999, 9999999)) for i in range(rows)) + "]")
                    f.write("\n")
                if data_field == DataField.float_field:
                    if err_type == DataErrorType.int_on_float_scalar:
                        f.write('"float_scalar":[' + ",".join(
                            str(random.randint(-999999, 9999999)) for i in range(rows)) + "]")
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
                            vectors1 = gen_float_vectors(wrong_row, dim) if float_vect else gen_binary_vectors(wrong_row, (dim//8))
                        if wrong_row >= rows -1:
                            vectors2 = []
                        else:
                            vectors2 = gen_float_vectors(rows-wrong_row-1, dim) if float_vect else gen_binary_vectors(rows-wrong_row-1, (dim//8))

                        vectors_wrong_dim = gen_float_vectors(1, wrong_dim) if float_vect else gen_binary_vectors(1, (wrong_dim//8))
                        vectors = vectors1 + vectors_wrong_dim + vectors2
                    else:
                        vectors = gen_float_vectors(rows, dim) if float_vect else gen_binary_vectors(rows, (dim//8))
                    f.write('"vectors":[' + ",".join(str(x) for x in vectors) + "]")
                    f.write("\n")
                if j != len(data_fields) - 1:
                    f.write(",")
        f.write("}")
        f.write("\n")


def gen_vectors_in_numpy_file(dir, float_vector, rows, dim, force=False):
    file_name = f"{DataField.vec_field}.npy"
    file = f'{dir}/{file_name}'

    if not os.path.exists(file) or force:
        # vector columns
        vectors = []
        if rows > 0:
            if float_vector:
                vectors = gen_float_vectors(rows, dim)
            else:
                vectors = gen_binary_vectors(rows, (dim // 8))
        arr = np.array(vectors)
        np.save(file, arr)
    return file_name


def gen_int_or_float_in_numpy_file(dir, data_field, rows, start=0, force=False):
    file_name = f"{data_field}.npy"
    file = f"{dir}/{file_name}"
    if not os.path.exists(file) or force:
        # non vector columns
        data = []
        if rows > 0:
            if data_field == DataField.float_field:
                data = [random.random() for _ in range(rows)]
            elif data_field == DataField.pk_field:
                data = [i for i in range(start, start + rows)]
            elif data_field == DataField.int_field:
                data = [random.randint(-999999, 9999999) for _ in range(rows)]
        arr = np.array(data)
        np.save(file, arr)
    return file_name


def gen_file_name(row_based, rows, dim, auto_id, str_pk,
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
    prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id, prefix=err_type)

    file_name = f"{prefix}_{pk}{vt}{field_suffix}{dim}d_{row_suffix}_{file_num}{file_type}"
    return file_name


def gen_subfolder(root, dim, rows, file_num):
    suffix = entity_suffix(rows)
    subfolder = f"{dim}d_{suffix}_{file_num}"
    path = f"{root}/{subfolder}"
    if not os.path.isdir(path):
        os.mkdir(path)
    return subfolder


def gen_json_files(row_based, rows, dim, auto_id, str_pk,
                   float_vector, data_fields, file_nums, multi_folder,
                   file_type, err_type, force, **kwargs):
    # gen json files
    files = []
    start_uid = 0
    # make sure pk field exists when not auto_id
    if not auto_id and DataField.pk_field not in data_fields:
        data_fields.append(DataField.pk_field)
    for i in range(file_nums):
        file_name = gen_file_name(row_based=row_based, rows=rows, dim=dim,
                                  auto_id=auto_id, str_pk=str_pk, float_vector=float_vector,
                                  data_fields=data_fields, file_num=i, file_type=file_type, err_type=err_type)
        file = f"{data_source}/{file_name}"
        if multi_folder:
            subfolder = gen_subfolder(root=data_source, dim=dim, rows=rows, file_num=i)
            file = f"{data_source}/{subfolder}/{file_name}"
        if not os.path.exists(file) or force:
            if row_based:
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


def gen_npy_files(float_vector, rows, dim, data_fields, file_nums=1, force=False):
    # gen numpy files
    files = []
    start_uid = 0
    if file_nums == 1:
        # gen the numpy file without subfolders if only one set of files
        for data_field in data_fields:
            if data_field == DataField.vec_field:
                file_name = gen_vectors_in_numpy_file(dir=data_source, float_vector=float_vector,
                                                      rows=rows, dim=dim, force=force)
            else:
                file_name = gen_int_or_float_in_numpy_file(dir=data_source, data_field=data_field,
                                                           rows=rows, force=force)
            files.append(file_name)
    else:
        for i in range(file_nums):
            subfolder = gen_subfolder(root=data_source, dim=dim, rows=rows, file_num=i)
            dir = f"{data_source}/{subfolder}"
            for data_field in data_fields:
                if data_field == DataField.vec_field:
                    file_name = gen_vectors_in_numpy_file(dir=dir, float_vector=float_vector, rows=rows, dim=dim, force=force)
                else:
                    file_name = gen_int_or_float_in_numpy_file(dir=dir, data_field=data_field, rows=rows, start=start_uid, force=force)
                files.append(f"{subfolder}/{file_name}")
            start_uid += rows
    return files


def prepare_bulk_load_json_files(row_based=True, rows=100, dim=128,
                                 auto_id=True, str_pk=False, float_vector=True,
                                 data_fields=[], file_nums=1, multi_folder=False,
                                 file_type=".json", err_type="", force=False, **kwargs):
    """
    Generate files based on the params in json format and copy them to minio

    :param row_based: indicate the file(s) to be generated is row based or not
    :type row_based: boolean

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
    files = gen_json_files(row_based=row_based, rows=rows, dim=dim,
                           auto_id=auto_id, str_pk=str_pk, float_vector=float_vector,
                           data_fields=data_fields, file_nums=file_nums, multi_folder=multi_folder,
                           file_type=file_type, err_type=err_type, force=force, **kwargs)

    copy_files_to_minio(host=minio, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files


def prepare_bulk_load_numpy_files(rows, dim, data_fields=[DataField.vec_field],
                                  float_vector=True, file_nums=1, force=False):
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
            it support one or all of [int_pk, vectors, int, float]
            Note: it does not automatically adds pk field
    :type data_fields: list

    :param file_nums: file numbers to be generated
        The file(s) would be  geneated in data_source folder if file_nums = 1
        The file(s) would be generated in different subfolers if file_nums > 1
    :type file_nums: int

    :param force: re-generate the file(s) regardless existing or not
    :type force: boolean

    Return: List
        File name list or file name with subfolder list
    """
    files = gen_npy_files(rows=rows, dim=dim, float_vector=float_vector,
                          data_fields=data_fields,
                          file_nums=file_nums, force=force)

    copy_files_to_minio(host=minio, r_source=data_source, files=files, bucket_name=bucket_name, force=force)
    return files
