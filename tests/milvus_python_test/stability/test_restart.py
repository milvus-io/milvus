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
tag = "1970_01_01"


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
    def _test_create_partition_insert_restart(self, connect, collection, args):
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
    def _test_during_creating_index_restart(self, connect, collection, args):
        '''
        target: return the same row count after server restart
        method: call function: insert, flush, and create index, server do restart during creating index
        expected: row count, vector-id, index info keep the same
        '''
        # reset auto_flush_interval
        # auto_flush_interval = 100
        get_ids_length = 500
        timeout = 60
        big_nb = 20000
        index_param = {"nlist": 1024, "m": 16}
        index_type = IndexType.IVF_PQ
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
        logging.getLogger().info("Start create index async")
        status = connect.create_index(collection, index_type, index_param, _async=True)
        time.sleep(2)
        # restart server
        logging.getLogger().info("Before restart server")
        if restart_server(args["service_name"]):
            logging.getLogger().info("Restart success")
        else:
            logging.getLogger().info("Restart failed")
        # check row count, index_type, vertor-id after server restart
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"])
        status, res_count = new_connect.count_entities(collection)
        assert status.OK()
        assert res_count == big_nb
        status, res_info = new_connect.get_index_info(collection)
        logging.getLogger().info(res_info)
        assert res_info._params == index_param
        assert res_info._collection_name == collection
        assert res_info._index_type == index_type
        start_time = time.time()
        i = 1
        while time.time() - start_time < timeout:
            stauts, stats = new_connect.get_collection_stats(collection)
            logging.getLogger().info(i)
            logging.getLogger().info(stats["partitions"])
            index_name = stats["partitions"][0]["segments"][0]["index_name"]
            if index_name == "PQ":
                break
            time.sleep(4)
            i += 1
        if time.time() - start_time >= timeout:
            logging.getLogger().info("Timeout")
            assert False
        get_ids = random.sample(ids, get_ids_length)
        status, res = new_connect.get_entity_by_id(collection, get_ids)
        assert status.OK()
        for index, item_id in enumerate(get_ids):
            assert_equal_vector(res[index], vectors[item_id])
