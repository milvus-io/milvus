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
    @pytest.fixture(scope="module", autouse=True)
    def skip_check(self, args):
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
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
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)
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
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)
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
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)
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
    def test_during_indexing(self, connect, collection, args):
        '''
        target: flushing will recover
        method: call function: create collection, then indexing, restart server and assert row count
        expected: row count equals nb, server contitue to build index after restart
        '''
        # disable_autoflush()
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)
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
    def test_delete_flush_during_compacting(self, connect, collection, args):
        '''
        target: verify server work after restart during compaction
        method: call function: create collection, then delete/flush/compacting, restart server and assert row count
            call `compact` again, compact pass
        expected: row count equals (nb - delete_length)
        '''
        # disable_autoflush()
        if "service_name" not in args or args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        ids = connect.insert(collection, big_entities)
        connect.flush([collection])
        delete_length = 1000
        delete_ids = ids[:delete_length]
        delete_res = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        connect.compact(collection, _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == big_nb - delete_length
        info = connect.get_collection_stats(collection)
        size_old = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_old)
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count_2 = new_connect.count_entities(collection)
        logging.getLogger().info(res_count_2)
        assert res_count_2 == big_nb - delete_length
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert size_before > size_after