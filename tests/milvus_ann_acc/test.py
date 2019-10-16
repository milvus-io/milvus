import os
import pdb
import time
import random
import sys
import h5py
import numpy
import logging
from logging import handlers

from client import MilvusClient

LOG_FOLDER = "logs"
logger = logging.getLogger("milvus_ann_acc")

formatter = logging.Formatter('[%(asctime)s] [%(levelname)-4s] [%(pathname)s:%(lineno)d] %(message)s')
if not os.path.exists(LOG_FOLDER):
    os.system('mkdir -p %s' % LOG_FOLDER)
fileTimeHandler = handlers.TimedRotatingFileHandler(os.path.join(LOG_FOLDER, 'acc'), "D", 1, 10)
fileTimeHandler.suffix = "%Y%m%d.log"
fileTimeHandler.setFormatter(formatter)
logging.basicConfig(level=logging.DEBUG)
fileTimeHandler.setFormatter(formatter)
logger.addHandler(fileTimeHandler)


def get_dataset_fn(dataset_name):
    file_path = "/test/milvus/ann_hdf5/"
    if not os.path.exists(file_path):
        raise Exception("%s not exists" % file_path)
    return os.path.join(file_path, '%s.hdf5' % dataset_name)


def get_dataset(dataset_name):
    hdf5_fn = get_dataset_fn(dataset_name)
    hdf5_f = h5py.File(hdf5_fn)
    return hdf5_f


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


def get_table_name(dataset_name, index_file_size):
    data_type, dimension, metric_type = parse_dataset_name(dataset_name)
    dataset = get_dataset(dataset_name)
    table_size = len(dataset["train"])
    table_size = str(table_size // 1000000)+"m"
    table_name = data_type+'_'+table_size+'_'+str(index_file_size)+'_'+str(dimension)+'_'+metric_type
    return table_name


def main(dataset_name, index_file_size, nlist=16384, force=False):
    top_k = 10
    nprobes = [32, 128]

    dataset = get_dataset(dataset_name)
    table_name = get_table_name(dataset_name, index_file_size)
    m = MilvusClient(table_name)
    if m.exists_table():
        if force is True:
            logger.info("Re-create table: %s" % table_name)
            m.delete()
            time.sleep(10)
        else:
            logger.info("Table name: %s existed" % table_name)
            return
    data_type, dimension, metric_type = parse_dataset_name(dataset_name)
    m.create_table(table_name, dimension, index_file_size, metric_type)
    print(m.describe())
    vectors = numpy.array(dataset["train"])
    query_vectors = numpy.array(dataset["test"])
    # m.insert(vectors)

    interval = 100000
    loops = len(vectors) // interval + 1

    for i in range(loops):
        start = i*interval
        end = min((i+1)*interval, len(vectors))
        tmp_vectors = vectors[start:end]
        if start < end:
            m.insert(tmp_vectors, ids=[i for i in range(start, end)])
    
    time.sleep(60)
    print(m.count())

    for index_type in ["ivf_flat", "ivf_sq8", "ivf_sq8h"]:
        m.create_index(index_type, nlist)
        print(m.describe_index())
        if m.count() != len(vectors):
            return
        m.preload_table()
        true_ids = numpy.array(dataset["neighbors"])
        for nprobe in nprobes:
            print("nprobe: %s" % nprobe)
            sum_radio = 0.0; avg_radio = 0.0
            result_ids = m.query(query_vectors, top_k, nprobe)
            # print(result_ids[:10])
            for index, result_item in enumerate(result_ids):
                if len(set(true_ids[index][:top_k])) != len(set(result_item)):
                    logger.info("Error happened")
                    # logger.info(query_vectors[index])
                    # logger.info(true_ids[index][:top_k], result_item)
                tmp = set(true_ids[index][:top_k]).intersection(set(result_item))
                sum_radio = sum_radio + (len(tmp) / top_k)
            avg_radio = round(sum_radio / len(result_ids), 4) 
            logger.info(avg_radio)
        m.drop_index()


if __name__ == "__main__":
    print("glove-25-angular")
    # main("sift-128-euclidean", 1024, force=True)
    for index_file_size in [50, 1024]:
        print("Index file size: %d" % index_file_size)
        main("glove-25-angular", index_file_size, force=True)

    print("sift-128-euclidean")
    for index_file_size in [50, 1024]:
        print("Index file size: %d" % index_file_size)
        main("sift-128-euclidean", index_file_size, force=True)
    # m = MilvusClient()