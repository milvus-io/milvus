import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *


dim = 128
index_file_size = 10
collection_id = "test_partition_restart"
nprobe = 1
tag = "1970-01-01"


class TestRestartBase:

    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    @pytest.fixture(scope="function", autouse=True)
    def skip_check(self, connect, args):
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)


    @pytest.mark.level(2)
    def test_create_partition_insert_restart(self, connect, collection, args):
        '''
        target: return the same row count after server restart
        method: call function: create partition, then insert, restart server and assert row count
        expected: status ok, and row count keep the same
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        nq = 1000
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.count_entities(collection)
        logging.getLogger().info(res)
        assert res == nq

        # restart server
        if restart_server(args["service_name"]):
            logging.getLogger().info("Restart success") 
        else:
            logging.getLogger().info("Restart failed")
        # assert row count again

        # debug
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        status, res = new_connect.count_entities(collection)
        logging.getLogger().info(status)
        logging.getLogger().info(res)
        assert status.OK()
        assert res == nq

    @pytest.mark.level(2)
    def test_during_creating_index_restart(self, connect, collection, args, get_simple_index):
        '''
        target: return the same row count after server restart
        method: call function: insert, flush, and create index, server do restart during creating index
        expected: row count, vector-id, index info keep the same
        '''
        # reset auto_flush_interval
        # auto_flush_interval = 100
        get_ids_length = 500
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, res_set = connect.set_config("db_config", "auto_flush_interval", auto_flush_interval)
        # assert status.OK()
        # status, res_get = connect.get_config("db_config", "auto_flush_interval")
        # assert status.OK()
        # assert res_get == str(auto_flush_interval)
        # insert and create index
        vectors = gen_vectors(big_nb, dim)
        status, ids = connect.insert(collection, vectors, ids=[i for i in range(big_nb)])
        status = connect.flush([collection])
        assert status.OK()
        status, res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert status.OK()
        assert res_count == big_nb

        def create_index():
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            status = milvus.create_index(collection, index_type, index_param)
            logging.getLogger().info(status)
            assert status.OK()

        p = Process(target=create_index, args=(collection, ))
        p.start()
        # restart server
        if restart_server(args["service_name"]):
            logging.getLogger().info("Restart success")
        else:
            logging.getLogger().info("Restart failed")
        # check row count, index_type, vertor-id after server restart
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"])
        status, res_count = new_connect.count_entities(collection)
        assert status.OK()
        assert res_count == big_nb
        status, res_info = connect.get_index_info(collection)
        logging.getLogger().info(res_info)
        assert res_info._params == index_param
        assert res_info._collection_name == collection
        assert res_info._index_type == index_type
        get_ids = random.sample(ids, get_ids_length)
        status, res = connect.get_entity_by_id(collection, get_ids)
        assert status.OK()
        for index, item_id in enumerate(get_ids):
            logging.getLogger().info(index)
            assert_equal_vector(res[index], vectors[item_id])
