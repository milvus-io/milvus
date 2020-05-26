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
