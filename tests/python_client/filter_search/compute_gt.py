from sklearn.metrics.pairwise import pairwise_distances
from numpy import transpose
import pandas as pd
import numpy as np
from loguru import logger
import time
import numba as nb
import glob
import os
import argparse
import re

def convert_numbers_to_quoted_strings(text):
    result = re.sub(r'\d+', lambda x: f"'{x.group()}'", text)
    return result

@nb.njit('int64[:,::1](float32[:,::1])', parallel=True)
def fastSort(a):
    b = np.empty(a.shape, dtype=np.int64)
    for i in nb.prange(a.shape[0]):
        b[i,:] = np.argsort(a[i,:])
    return b

def compute_neighbors(train_data_file_name, test_data_file_name, expr=None, top_k=1000):
    logger.info(f"train data file {train_data_file_name}, test data file {test_data_file_name}, expr {expr}")
    t0 = time.time()
    test_df = pd.read_parquet(test_data_file_name)
    train_df = pd.read_parquet(train_data_file_name)
    tt= time.time() - t0
    logger.info(f"read parquet cost {tt}")
    if expr:
        logger.info(f"expr {expr}")
        train_df.query(expr=expr, inplace=True)
    test_data = test_df["emb"].tolist()
    test_id_list = test_df["id"].tolist()
    # logger.info(f"test data {len(test_data)}")
    train_data = train_df["emb"].tolist()
    logger.info(f"train data after filter: {len(train_data)}")
    t0 = time.time()
    distance = pairwise_distances(train_data, Y=test_data, metric="euclidean")
    tt = time.time() - t0
    logger.info(f"compute distance cost {tt}")
    distance = transpose(distance)
    distance = np.array(distance, dtype=np.float32, order='C')

    idx = train_df["id"].tolist()
    t0 = time.time()
    distance_sorted_arg = fastSort(distance)
    tt =time.time() - t0
    logger.info(f"sort cost time {tt}")
    top_k_result = distance_sorted_arg[:,:top_k]
    logger.info(top_k_result[0])
    result = np.empty(top_k_result.shape, dtype=[('idx', "i8"), ('distance', "f8")])
    t0 = time.time()
    for index, value in np.ndenumerate(top_k_result):
        # idx[value] 数组下标映射回真正的id,
        # 比如切分了的文件ids是无序的或者不是从0开始的
        result[index] = (idx[value], distance[index[0],value])
    tt = time.time() - t0
    logger.info(f"map cost time {tt}")
    logger.info(result)

    df_neighbors = pd.DataFrame({
        "id": test_id_list, # 这个地方用test_data的id更好，因为test_data的id并一定是从0-1000有序的
        # 这里的result包含两个字段，一个是train data的id，一个是distance
        "neighbors_id": result.tolist()
    })

    logger.info(df_neighbors)
    # save neighbors
    neighbors_file_name = train_data_file_name.replace("train", f"neighbors")
    df_neighbors.to_parquet(neighbors_file_name)
    return neighbors_file_name


def merge_neighbors(file_list, final_file_name):
    logger.info(f"{file_list}, final file name {final_file_name}")
    t0 = time.time()
    files_num = len(file_list)
    # logger.info(f"merge neighbors files {files_num}")
    df_tmp = pd.read_parquet(file_list[0])
    test_id_list = df_tmp["id"].tolist()
    neighbors_id = np.array(df_tmp["neighbors_id"].tolist())

    for i in range(1, files_num):
        df_tmp = pd.read_parquet(file_list[i])
        tmp_neighbors_id = np.array(df_tmp["neighbors_id"].tolist())
        neighbors_id = np.concatenate((neighbors_id, tmp_neighbors_id), axis=1)
    # logger.info(neighbors_id)
    result = np.empty(neighbors_id.shape, dtype=[('idx', "i8"), ('distance', "f8")])

    for index, value in np.ndenumerate(neighbors_id):
        result[index] = (neighbors_id[index][0],neighbors_id[index][1])
    # logger.info(result)
    # logger.info(result[0])

    sorted_result = np.sort(result, axis=1, order=["distance"])
    # logger.info(sorted_result[0])

    result = np.empty(sorted_result.shape, dtype="i8")
    distance_result = np.empty(sorted_result.shape, dtype="f8")
    # 仅需要id，所以（id, distance）--> id
    for index, value in np.ndenumerate(sorted_result):
        result[index] = sorted_result[index][0]
    # logger.info(result)
    # 仅需要distance，所以（id, distance）--> distance
    for index, value in np.ndenumerate(sorted_result):
        distance_result[index] = sorted_result[index][1]
    df = pd.DataFrame(data={
        "id": test_id_list,
        "neighbors_id": result[:,:1000].tolist(),
        "distance": distance_result[:,:1000].tolist()
    })
    tt = time.time() - t0
    logger.info(f"merge cost time {tt}")
    # print(df)
    df.to_parquet(final_file_name)

def check_file_exists(file_path):
    return os.path.exists(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prepare data for perf test")
    parser.add_argument("--filter", type=str, default="")
    args = parser.parse_args()
    train_data_file_name = glob.glob('/root/dataset/laion_with_scalar_medium_10m/train*.parquet')
    test_data_file_name = "/root/dataset/laion_with_scalar_medium_10m/test.parquet"
    neighbor_dir = "/root/dataset/laion_with_scalar_medium_10m"

    expr = args.filter
    ascii_codes = [str(ord(char)) for char in expr]
    expr_ascii = "".join(ascii_codes)
    # check neighbors file is exist
    if expr_ascii:
        neighbors_file_name = f"{neighbor_dir}/neighbors-{expr_ascii}.parquet"
    else:
        neighbors_file_name = f"{neighbor_dir}/neighbors.parquet"
    if check_file_exists(neighbors_file_name):
        df = pd.read_parquet(neighbors_file_name)
        print(df.head())
        print("File exists")
        exit(0)
    neighbors_file_name_list = []
    expr = convert_numbers_to_quoted_strings(expr)
    for train_data_f in train_data_file_name:
        print(train_data_f)
        neighbors_file_name = compute_neighbors(train_data_f, test_data_file_name, expr)
        neighbors_file_name_list.append(neighbors_file_name)
    if expr_ascii:
        merge_neighbors(neighbors_file_name_list, f"{neighbor_dir}/neighbors-{expr_ascii}.parquet")
    else:
        merge_neighbors(neighbors_file_name_list, f"{neighbor_dir}/neighbors.parquet")
