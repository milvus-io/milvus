import time
import os
from minio import Minio
from minio.error import S3Error
import numpy as np
import random
from sklearn import preprocessing
from common.common_func import gen_unique_str


minio = "10.96.1.23:9000"  # TODO update hardcode
bucket_name = "yanliang-bulk-load"  # TODO update hardcode

data_source = "/tmp/bulk_load_data"

BINARY = "binary"
FLOAT = "float"


def gen_file_prefix(row_based=True, auto_id=True, prefix=""):
    if row_based:
        if auto_id:
            return f"{prefix}row_auto"
        else:
            return f"{prefix}row_cust"
    else:
        if auto_id:
            return f"{prefix}col_auto"
        else:
            return f"{prefix}col_cust"


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
    # binary: 每个int表示8个dimension
    # 如果binary vector dimension=16的话，就用2个0～255的int数字来表示
    vectors = [[random.randint(0, 255) for _ in range(dim)] for _ in range(nb)]
    # vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors


def gen_row_based_json_file(row_file, str_pk, multi_scalars, float_vect, rows, dim, autoid):
    with open(row_file, "w") as f:
        f.write("{")
        f.write("\n")
        f.write('"rows":[')
        f.write("\n")
        for i in range(rows):
            if i > 0:
                f.write(",")
                f.write("\n")
            # pk fields
            if not autoid:
                if str_pk:
                    f.write('{"uid":"' + str(gen_unique_str()) + '",')
                else:
                    f.write('{"uid":' + str(i) + ',')
            else:
                f.write('{')

            # scalar fields
            if multi_scalars:
                f.write('"int_scalar":' + str(random.randint(-999999, 9999999)) + ',')
                f.write('"float_scalar":' + str(random.random()) + ',')
                f.write('"string_scalar":"' + str(gen_unique_str()) + '",')
                f.write('"bool_scalar":' + str(random.choice(["true", "false"])) + ',')

            # vector field
            if float_vect:
                vectors = gen_float_vectors(1, dim)
            else:
                vectors = gen_binary_vectors(1, (dim//8))
            f.write('"vectors":' + ",".join(str(x) for x in vectors) + "}")
        f.write("\n")
        f.write("]")
        f.write("\n")
        f.write("}")
        f.write("\n")


def gen_column_base_json_file(col_file, str_pk, float_vect, multi_scalars, rows, dim, autoid):
    with open(col_file, "w") as f:
        f.write("{")
        f.write("\n")
        # pk columns
        if not autoid:
            if str_pk == "str_pk":
                f.write('"uid":["' + ',"'.join(str(gen_unique_str()) + '"' for i in range(rows)) + '],')
                f.write("\n")
            else:
                f.write('"uid":[' + ",".join(str(i) for i in range(rows)) + "],")
                f.write("\n")

        # scalar columns
        if multi_scalars:
            f.write('"int_scalar":[' + ",".join(str(random.randint(-999999, 9999999)) for i in range(rows)) + "],")
            f.write("\n")
            f.write('"float_scalar":[' + ",".join(str(random.random()) for i in range(rows)) + "],")
            f.write("\n")
            f.write('"string_scalar":["' + ',"'.join(str(gen_unique_str()) + '"' for i in range(rows)) + '],')
            f.write("\n")
            f.write('"bool_scalar":[' + ",".join(str(random.choice(["true", "false"])) for i in range(rows)) + "],")
            f.write("\n")

        # vector columns
        if float_vect:
            vectors = gen_float_vectors(rows, dim)
            f.write('"vectors":[' + ",".join(str(x) for x in vectors) + "]")
            f.write("\n")
        else:
            vectors = gen_binary_vectors(rows, (dim//8))
            f.write('"vectors":[' + ",".join(str(x) for x in vectors) + "]")
            f.write("\n")

        f.write("}")
        f.write("\n")


def gen_vectors_in_numpy_file(dir, vector_type, rows, dim, num):
    # vector columns
    if vector_type == FLOAT:
        vectors = gen_float_vectors(rows, dim)
    else:
        vectors = gen_binary_vectors(rows, (dim // 8))

    suffix = entity_suffix(rows)
    # print(vectors)
    arr = np.array(vectors)
    path = f"{dir}/{dim}d_{suffix}_{num}"
    if not os.path.isdir(path):
        os.mkdir(path)
    file = f'{path}/vectors_{dim}d_{suffix}.npy'
    np.save(file, arr)


def gen_scalars_in_numpy_file(dir, vector_type, rows, dim, num, start):
    # scalar columns
    if vector_type == FLOAT:
        data = [random.random() for i in range(rows)]
    elif vector_type == "int":
        data = [i for i in range(start, start + rows)]

    suffix = entity_suffix(rows)
    path = f"{dir}/{dim}d_{suffix}_{num}"
    arr = np.array(data)
    file = f'{path}/uid.npy'
    np.save(file, arr)


def gen_json_file_name(row_based, rows, dim, auto_id, str_pk, float_vector, multi_scalars, file_num):
    suffix = entity_suffix(rows)
    scalars = "only"
    if multi_scalars:
        scalars = "multi_scalars"
    vt = FLOAT
    if not float_vector:
        vt = BINARY
    pk = ""
    if str_pk:
        pk = "str_pk_"
    prefix = gen_file_prefix(row_based=row_based, auto_id=auto_id)
    return f"{prefix}_{pk}{vt}_vectors_{scalars}_{dim}d_{suffix}_{file_num}.json"


def gen_json_files(row_based, rows, dim, auto_id, str_pk, float_vector, multi_scalars, file_nums, force=False):
    """
    row_based: Boolean
        generate row based json file if True
        generate column base json file if False
    rows: entities of data
    dim: dim of vector data
    auto_id: Boolean
        generate primary key data if False, else not
    str_pk: Boolean
         generate string as primary key if True, else generate INT64 as pk
    float_vector: Boolean
        generate float vectors if True, else binary vectors
    multi_scalars: Boolean
        only generate vector data (and pk data depended on auto_switches) if False
        besides vector data, generate INT, STRING, BOOLEAN, etc scalar data if True
    file_nums: file numbers that to be generated
    """

    # gen json files
    for i in range(file_nums):
        file_name = gen_json_file_name(row_based=row_based, rows=rows,dim=dim,
                                       auto_id=auto_id,str_pk=str_pk,float_vector=float_vector,
                                       multi_scalars=multi_scalars, file_num=i)
        file = f"{data_source}/{file_name}"
        if not os.path.exists(file) or force:
            if row_based:
                gen_row_based_json_file(row_file=file, str_pk=str_pk,
                                        float_vect=float_vector, multi_scalars=multi_scalars,
                                        rows=rows, dim=dim, autoid=auto_id)
            else:
                gen_column_base_json_file(col_file=file, str_pk=str_pk,
                                          float_vect=float_vector, multi_scalars=multi_scalars,
                                          rows=rows, dim=dim, autoid=auto_id)

def gen_npy_files():
    # # gen numpy files
    # uid = 0
    # for i in range(file_nums):
    #     gen_vectors_in_numpy_file(data_dir, FLOAT, rows=rows_list[0], dim=dim_list[0], num=i)
    #     gen_scalars_in_numpy_file(data_dir, "int", rows=rows_list[0], dim=dim_list[0], num=i, start=uid)
    #     uid += rows_list[0]
    pass


def copy_files_to_bucket(client, r_source, bucket_name, force=False):
    # check 'xxx' bucket exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        print(f"Bucket {bucket_name} not found, create it.")
        client.make_bucket(bucket_name)

    # copy files from root source folder
    os.chdir(r_source)
    onlyfiles = [f for f in os.listdir(r_source) if
                 os.path.isfile(os.path.join(r_source, f))]
    for file in onlyfiles:
        if not file.startswith("."):
            found = False
            try:
                result = client.stat_object(bucket_name, file)
                found = True
            except S3Error as exc:
                pass

            if force:
                res = client.fput_object(bucket_name, file, f"{r_source}/{file}")
                print(res.object_name)
            elif not found:
                res = client.fput_object(bucket_name, file, f"{r_source}/{file}")
                print(res.object_name)

    # copy subfolders
    sub_folders = [f.name for f in os.scandir(r_source) if f.is_dir()]
    for sub_folder in sub_folders:
        if sub_folder not in ["backup", "tested"]:
            source = f"{r_source}/{sub_folder}"
            os.chdir(source)
            onlyfiles = [f for f in os.listdir(source) if
                         os.path.isfile(os.path.join(source, f))]
            for file in onlyfiles:
                if not file.startswith("."):
                    found = False
                    try:
                        result = client.stat_object(bucket_name, f"{sub_folder}/{file}")
                        found = True
                    except S3Error as exc:
                        pass

                    if force:
                        res = client.fput_object(bucket_name, f"{sub_folder}/{file}", f"{source}/{file}")
                        print(res.object_name)
                    elif not found:
                        res = client.fput_object(bucket_name, f"{sub_folder}/{file}", f"{source}/{file}")
                        print(res.object_name)


def copy_files_to_minio(host, bucket_name, access_key="minioadmin", secret_key="minioadmin", secure=False):
    client = Minio(
        host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    try:
        # TODO: not copy all the files, just copy the new generated files
        copy_files_to_bucket(client, r_source=data_source, bucket_name=bucket_name, force=False)
    except S3Error as exc:
        print("error occurred.", exc)


def parpar_bulk_load_data(json_file, row_based, rows, dim, auto_id, str_pk, float_vector, multi_scalars, file_nums, force=False):
    if json_file:
        gen_json_files(row_based=row_based, rows=rows, dim=dim,
                       auto_id=auto_id, str_pk=str_pk, float_vector=float_vector,
                       multi_scalars=multi_scalars, file_nums=file_nums, force=force)

        copy_files_to_minio(host=minio, bucket_name=bucket_name)
    else:
        # TODO: for npy files
        # gen_npy_files()
        # copy()
        pass

# if __name__ == '__main__':
#     gen_json_files(row_based=True, rows=10,
#                    dim=4, auto_id=False, str_pk=False,
#                    float_vector=True, multi_scalars=False, file_nums=2)
#
#     copy_files_to_minio(host=minio, bucket_name=bucket_name)
