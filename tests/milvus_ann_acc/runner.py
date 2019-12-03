import os
import pdb
import time
import random
import sys
import logging
import h5py
import numpy
from influxdb import InfluxDBClient

INSERT_INTERVAL = 100000
# s
DELETE_INTERVAL_TIME = 5
INFLUXDB_HOST = "192.168.1.194"
INFLUXDB_PORT = 8086
INFLUXDB_USER = "admin"
INFLUXDB_PASSWD = "admin"
INFLUXDB_NAME = "test_result"
influxdb_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWD, database=INFLUXDB_NAME)

logger = logging.getLogger("milvus_acc.runner")


def parse_dataset_name(dataset_name):
    data_type = dataset_name.split("-")[0]
    dimension = int(dataset_name.split("-")[1])
    metric = dataset_name.split("-")[-1]
    # metric = dataset.attrs['distance']
    # dimension = len(dataset["train"][0])
    if metric == "euclidean":
        metric_type = "l2"
    elif metric  == "angular":
        metric_type = "ip"
    return ("ann"+data_type, dimension, metric_type)


def get_dataset(hdf5_path, dataset_name):
    file_path = os.path.join(hdf5_path, '%s.hdf5' % dataset_name)
    if not os.path.exists(file_path):
        raise Exception("%s not existed" % file_path)
    dataset = h5py.File(file_path)
    return dataset


def get_table_name(hdf5_path, dataset_name, index_file_size):
    data_type, dimension, metric_type = parse_dataset_name(dataset_name)
    dataset = get_dataset(hdf5_path, dataset_name)
    table_size = len(dataset["train"])
    table_size = str(table_size // 1000000)+"m"
    table_name = data_type+'_'+table_size+'_'+str(index_file_size)+'_'+str(dimension)+'_'+metric_type
    return table_name


def recall_calc(result_ids, true_ids, top_k, recall_k):
    sum_intersect_num = 0
    recall = 0.0
    for index, result_item in enumerate(result_ids):
        if len(set(true_ids[index][:top_k])) != len(set(result_item)):
            logger.warning("Error happened: query result length is wrong")
            continue
        tmp = set(true_ids[index][:recall_k]).intersection(set(result_item))
        sum_intersect_num = sum_intersect_num + len(tmp)
    recall = round(sum_intersect_num / (len(result_ids) * recall_k), 4)
    return recall


def run(milvus, config, hdf5_path, force=True):
    server_version = milvus.get_server_version()
    logger.info(server_version)
    
    for dataset_name, config_value in config.items():
        dataset = get_dataset(hdf5_path, dataset_name)
        index_file_sizes = config_value["index_file_sizes"]
        index_types = config_value["index_types"]
        nlists = config_value["nlists"]
        search_param = config_value["search_param"]
        top_ks = search_param["top_ks"]
        nprobes = search_param["nprobes"]
        nqs = search_param["nqs"]

        for index_file_size in index_file_sizes:
            table_name = get_table_name(hdf5_path, dataset_name, index_file_size)  
            if milvus.exists_table(table_name):
                if force is True:
                    logger.info("Re-create table: %s" % table_name)
                    milvus.delete(table_name)
                    time.sleep(DELETE_INTERVAL_TIME)
                else:
                    logger.warning("Table name: %s existed" % table_name)
                    continue
            data_type, dimension, metric_type = parse_dataset_name(dataset_name)
            milvus.create_table(table_name, dimension, index_file_size, metric_type)
            logger.info(milvus.describe())
            insert_vectors = numpy.array(dataset["train"])
            # milvus.insert(insert_vectors)

            loops = len(insert_vectors) // INSERT_INTERVAL + 1
            for i in range(loops):
                start = i*INSERT_INTERVAL
                end = min((i+1)*INSERT_INTERVAL, len(insert_vectors))
                tmp_vectors = insert_vectors[start:end]
                if start < end:
                    milvus.insert(tmp_vectors, ids=[i for i in range(start, end)])
            time.sleep(20)
            row_count = milvus.count()
            logger.info("Table: %s, row count: %s" % (table_name, row_count))
            if milvus.count() != len(insert_vectors):
                logger.error("Table row count is not equal to insert vectors")
                return
            for index_type in index_types:
                for nlist in nlists:
                    milvus.create_index(index_type, nlist)
                    logger.info(milvus.describe_index())
                    logger.info("Start preload table: %s, index_type: %s, nlist: %s" % (table_name, index_type, nlist))
                    milvus.preload_table()
                    true_ids = numpy.array(dataset["neighbors"])
                    for nprobe in nprobes:
                        for nq in nqs:
                            query_vectors = numpy.array(dataset["test"][:nq])
                            for top_k in top_ks:
                                rec1 = 0.0
                                rec10 = 0.0
                                rec100 = 0.0
                                result_ids = milvus.query(query_vectors, top_k, nprobe)
                                logger.info("Query result: %s" % len(result_ids))
                                rec1 = recall_calc(result_ids, true_ids, top_k, 1)
                                if top_k == 10:
                                    rec10 = recall_calc(result_ids, true_ids, top_k, 10)
                                if top_k == 100:
                                    rec10 = recall_calc(result_ids, true_ids, top_k, 10)
                                    rec100 = recall_calc(result_ids, true_ids, top_k, 100)
                                avg_radio = recall_calc(result_ids, true_ids, top_k, top_k)
                                logger.debug("Recall_1: %s" % rec1)
                                logger.debug("Recall_10: %s" % rec10)
                                logger.debug("Recall_100: %s" % rec100)
                                logger.debug("Accuracy: %s" % avg_radio)
                                acc_record = [{
                                    "measurement": "accuracy",
                                    "tags": {
                                        "server_version": server_version,
                                        "dataset": dataset_name,
                                        "index_file_size": index_file_size,
                                        "index_type": index_type,
                                        "nlist": nlist,
                                        "search_nprobe": nprobe,
                                        "top_k": top_k,
                                        "nq": len(query_vectors)
                                    },
                                    # "time": time.ctime(),
                                    "time": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "fields": {
                                        "recall1": rec1,
                                        "recall10": rec10,
                                        "recall100": rec100,
                                        "avg_radio": avg_radio
                                    }
                                }]
                                logger.info(acc_record)
                                try:
                                    res = influxdb_client.write_points(acc_record)
                                except Exception as e:
                                    logger.error("Insert infuxdb failed: %s" % str(e))
