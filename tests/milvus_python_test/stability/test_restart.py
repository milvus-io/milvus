import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *


collection_id = "wal"
TIMEOUT = 120
tag = "1970_01_01"
insert_interval_time = 1.5
big_nb = 100000
field_name = "float_vector"
entity = gen_entities(1)
binary_entity = gen_binary_entities(1)
entities = gen_entities(nb)
big_entities = gen_entities(big_nb)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields() 
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}


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
    def test_insert_flush(self, connect, collection, args):
        '''
        target: return the same row count after server restart
        method: call function: create collection, then insert/flush, restart server and assert row count
        expected: row count keep the same
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == 2 * nb
        # restart server
        logging.getLogger().info("Start restart server")
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count = new_connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == 2 * nb

    @pytest.mark.level(2)
    def test_insert_during_flushing(self, connect, collection, args):
        '''
        target: flushing will recover
        method: call function: create collection, then insert/flushing, restart server and assert row count
        expected: row count equals 0
        '''
        # disable_autoflush()
        ids = connect.insert(collection, big_entities)
        connect.flush([collection], _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        if res_count < big_nb:
            # restart server
            assert restart_server(args["service_name"])
            # assert row count again
            new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
            res_count_2 = new_connect.count_entities(collection)
            logging.getLogger().info(res_count_2)
            timeout = 300
            start_time = time.time()
            while new_connect.count_entities(collection) != big_nb and (time.time() - start_time < timeout):
                time.sleep(10)
                logging.getLogger().info(new_connect.count_entities(collection))
            res_count_3 = new_connect.count_entities(collection)
            logging.getLogger().info(res_count_3)
            assert res_count_3 == big_nb

    @pytest.mark.level(2)
    def test_delete_during_flushing(self, connect, collection, args):
        '''
        target: flushing will recover
        method: call function: create collection, then delete/flushing, restart server and assert row count
        expected: row count equals (nb - delete_length)
        '''
        # disable_autoflush()
        ids = connect.insert(collection, big_entities)
        connect.flush([collection])
        delete_length = 1000
        delete_ids = ids[big_nb//4:big_nb//4+delete_length]
        delete_res = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection], _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count_2 = new_connect.count_entities(collection)
        logging.getLogger().info(res_count_2)
        timeout = 100
        start_time = time.time()
        while new_connect.count_entities(collection) != big_nb - delete_length and (time.time() - start_time < timeout):
            time.sleep(10)
            logging.getLogger().info(new_connect.count_entities(collection))
        if new_connect.count_entities(collection) == big_nb - delete_length:
            time.sleep(10)
            res_count_3 = new_connect.count_entities(collection)
            logging.getLogger().info(res_count_3)
            assert res_count_3 == big_nb - delete_length

    @pytest.mark.level(2)
    def _test_during_indexing(self, connect, collection, args):
        '''
        target: flushing will recover
        method: call function: create collection, then indexing, restart server and assert row count
        expected: row count equals nb, server contitue to build index after restart
        '''
        # disable_autoflush()
        ids = connect.insert(collection, big_entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, default_index, _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count_2 = new_connect.count_entities(collection)
        logging.getLogger().info(res_count_2)
        timeout = 300
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(5)
            assert new_connect.count_entities(collection) == big_nb
            stats = connect.get_collection_stats(collection)
            assert stats["row_count"] == big_nb
            for file in stats["partitions"][0]["segments"][0]["files"]:
                if file["field"] == field_name and file["name"] != "_raw":
                    assert file["data_size"] > 0
                    if file["index_type"] != default_index["index_type"]:
                        continue
        for file in stats["partitions"][0]["segments"][0]["files"]:
            if file["field"] == field_name and file["name"] != "_raw":
                assert file["data_size"] > 0
                if file["index_type"] != default_index["index_type"]:
                    assert False
                else:
                    assert True

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
