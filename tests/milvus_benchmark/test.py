import logging
import random
import pdb
import time, datetime
import copy
import psutil
from multiprocessing import Process

import numpy
import sklearn.preprocessing
from client import MilvusClient

SERVER_HOST_DEFAULT = "192.168.1.29"
SERVER_HOST_DEFAULT = "127.0.0.1"
# SERVER_HOST_DEFAULT = "192.168.1.189"

SERVER_PORT_DEFAULT = 19531

# NAS_ROOT = "/poc/yuncong"

# def get_id_file_name(table_name):
#     id_file_name = NAS_ROOT+"/"+table_name+"/id"
#     return id_file_name


# def get_search_file_name(table_name, nq, top_k):
#     return table_name+'_%d_%d' % (nq, top_k)


# # compute and print accuracy
# def compute_accuracy(dataset, base_dataset, nq, top_k):
#     base_id_list = []; id_list = []

#     search_res_file = get_search_file_name(dataset, nq, top_k)
#     dataset_id_file = get_id_file_name(dataset)
#     base_dataset_id_file = get_id_file_name(base_dataset)
#     base_search_res_file = get_search_file_name(base_dataset, nq, top_k)
#     with open(base_search_res_file, 'r') as base_dataset_fd:
#         for line in base_dataset_fd:
#             tmp_list = line.strip("\n").strip().split("\t")
#             base_id_list.append(tmp_list)

#     # check length of base_id_list
#     if len(base_id_list) != nq:
#         logging.warning("Base dataset search result is not correct")
#         return
#     # loading id maps: {dataset_id:base_dataset_id}
#     ids_maps = {}
#     dataset_ids = []

#     logging.info("Dataset: %s start loading" % dataset_id_file)
#     with open(dataset_id_file) as dataset_id_fd:
#         for line in dataset_id_fd:
#             dataset_ids.append(line.strip("\n"))

#     logging.info("Base dataset: %s start loading" % base_dataset_id_file)
#     with open(base_dataset_id_file) as base_dataset_id_fd:
#         for index, line in enumerate(base_dataset_id_fd):
#             ids_maps[dataset_ids[index]] = line.strip("\n")

#     with open(search_res_file, 'r') as dataset_fd:
#         for line in dataset_fd:
#             tmp_list = line.strip("\n").strip().split("\t")
#             id_list.append([ids_maps[item] for item in tmp_list])

#     # print(id_list)
#     # print(base_id_list)
#     # get the accuracy
#     sum_radio = 0.0
#     for index, item in enumerate(id_list):
#         tmp = set(item).intersection(set(base_id_list[index]))
#         sum_radio = sum_radio + len(tmp) / top_k
#     return sum_radio / nq

# avg_acc = compute_accuracy("ip_ivflat_100", "ip_flat_100", 20, 10)
# print(round(avg_acc, 2))


def gen_vectors(num, dim):
    return [[random.random() for _ in range(dim)] for _ in range(num)]


def time_wrapper(func):
    """
    This decorator prints the execution time for the decorated function.
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print("{} run in {}s".format(func.__name__, round(end - start, 2)))
        return result
    return wrapper


def get_memory_usage():
    """Return the current memory usage of this algorithm instance
    (in kilobytes), or None if this information is not available."""
    # return in kB for backwards compatibility
    return psutil.Process().memory_info().rss / 1024


if __name__ == "__main__":
    import sys
    import numpy as np
    table_name = "random_501m_1024_512_ip"
    m = MilvusClient(table_name, ip=SERVER_HOST_DEFAULT, port=SERVER_PORT_DEFAULT)
    m.create_table(table_name, 128, 1024, "ip")
    print(m.count())
    print(m.describe())
    print(m.describe_index())
    # print(m._milvus._cmd("mode"))

    # m.drop_index()
    m.create_index('ivf_sq8h', 16384)
    sys.exit()
    # batch = 1
    # dimension = 512
    # def search(table_name, top_k, nprobe, vectors, ntimes):        
    #     total_cost = 0
    #     for i in range(ntimes):
    #         start = time.time()
    #         status, result = m._milvus.search(table_name=table_name, query_records=vectors, top_k=top_k, nprobe=nprobe)
    #         finish = time.time()
    #         total_cost += finish - start
            
    #         if not status.OK():
    #             print(status)
    #             return 0
    #     return total_cost

    # def search_new(table_name, top_k, nprobe, vectors, ntimes):        
    #     total_cost = 0
    #     for i in range(ntimes):
    #         start = time.time()
    #         status, result = m.query(vectors, top_k, nprobe)
    #         finish = time.time()
    #         total_cost += finish - start
            
    #         if not status.OK():
    #             print(status)
    #             return 0
    #     return total_cost

    # def test(nq, top_k, nprobe):
    #     query_records = [[random.random() for _ in range(dimension)] for _ in range(nq)]
    #     # data = np.load("/poc/yuncong/yunfeng/random_data/binary_512d_00499.npy")
    #     query_records = data[0:nq].tolist()
    #     # # pdb.set_trace()
    #     # query_records_new_new = np.array(data[0:nq].tolist())
    #     # batch_cost = search(table_name, top_k, nprobe, query_records, batch)
    #     batch_cost = search(table_name, top_k, nprobe, query_records, batch)
    #     # batch_cost = search(table_name, top_k, nprobe, query_records, batch)
    #     # batch_cost_2 = search(table_name, top_k, nprobe, query_records_new_new, batch)
    #     # batch_cost_new = search_new(table_name, top_k, nprobe, query_records, batch)
    #     # batch_cost_new = search_new(table_name, top_k, nprobe, query_records_new, batch)
    #     print(f'nq: {nq}, top_k: {top_k}, nprobe: {nprobe}, avgcost: {batch_cost / batch}')

    # # test(1, 8, 8)
    # # test(10, 8, 8)
    # # test(100, 8, 8)
    # test(500, 8, 8)
    # # test(800, 8, 8)
    # # test(999, 8, 8)

    # # print(m.drop_index())
    # sys.exit()
    # nq = 10000
    # insert_records = gen_vectors(nq, 128)
    # insert_records = sklearn.preprocessing.normalize(insert_records, axis=1, norm='l2').tolist()
    # # m.insert(insert_records, [i for i in range(nq)])
    nlist = 16384
    index_type = "ivf_sq8"
    m.drop_index()
    print(m.create_index(index_type, nlist))
    print(m.count())
    print(m.describe())
    print(m.describe_index())
    # print(m._milvus._cmd("mode"))
    # sys.exit()
    random_q = gen_vectors(1000000, 128)
    q_records = [insert_records[0], insert_records[1]]
    for item in random_q:
        q_records.append(item)
    status, res = m._milvus.search_vectors(table_name=table_name, query_records=q_records, top_k=10, nprobe=1)
    print(status)
    for item in res:
        # pdb.set_trace()
        if len(item) != 10:
            print("length != top_k")
            break
        print("top-1: %d, top-100: %d" % (item[0].id, item[9].id))
        # print(item[0].distance, item[9].distance)
        print("-------------------------")

    sys.exit()

    nq = 10000
    # top_k = 1000
    # query_vectors = gen_vectors(nq, 128)
    # while True:
    #     status, result = m.query(query_vectors, top_k)
    #     print(status)
    #     time.sleep(0.1)
    # m.build_index()
    # print(m.count())
    X = gen_vectors(nq, 10)
    # print(m.fit(X))
    # print(m._milvus.show_tables())
    X1 = copy.deepcopy(X)
    X2 = copy.deepcopy(X)

    X1 /= numpy.linalg.norm(X1)
    X2 = sklearn.preprocessing.normalize(X2, axis=1, norm='l2')
    sum_ = 0
    for item in X1:
        print(item)
        for i in item:
            sum_ = sum_ + i * i
            print(sum_)
            break
        break
    # print(X1[0][0])

    # X2 = sklearn.preprocessing.normalize(X2, axis=1, norm='l2')
    # print(X2[0][0])
