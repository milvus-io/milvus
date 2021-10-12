import time
import random
import pdb
import threading
import logging
import json
from multiprocessing import Pool, Process
import pytest
from utils.utils import get_milvus, restart_server, gen_entities, gen_unique_str, default_nb
from common.constants import default_fields, default_entities
from common.common_type import CaseLabel


uid = "wal"
TIMEOUT = 120
insert_interval_time = 1.5
big_nb = 100000
field_name = "float_vector"
big_entities = gen_entities(big_nb)
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}


class TestRestartBase:
    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    @pytest.fixture(scope="module", autouse=True)
    def skip_check(self, args):
        logging.getLogger().info(args)
        if "service_name" not in args or not args["service_name"]:
            reason = "Skip if service name not provided"
            logging.getLogger().info(reason)
            pytest.skip(reason)
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)

    @pytest.mark.tags(CaseLabel.L2)
    def _test_insert_flush(self, connect, collection, args):
        """
        target: return the same row count after server restart
        method: call function: create collection, then insert/flush, restart server and assert row count
        expected: row count keep the same
        """
        ids = connect.bulk_insert(collection, default_entities)
        connect.flush([collection])
        ids = connect.bulk_insert(collection, default_entities)
        connect.flush([collection])
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == 2 * default_nb
        # restart server
        logging.getLogger().info("Start restart server")
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"])
        res_count = new_connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == 2 * default_nb

    @pytest.mark.tags(CaseLabel.L2)
    def _test_insert_during_flushing(self, connect, collection, args):
        """
        target: flushing will recover
        method: call function: create collection, then insert/flushing, restart server and assert row count
        expected: row count equals 0
        """
        # disable_autoflush()
        ids = connect.bulk_insert(collection, big_entities)
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

    @pytest.mark.tags(CaseLabel.L2)
    def _test_delete_during_flushing(self, connect, collection, args):
        """
        target: flushing will recover
        method: call function: create collection, then delete/flushing, restart server and assert row count
        expected: row count equals (nb - delete_length)
        """
        # disable_autoflush()
        ids = connect.bulk_insert(collection, big_entities)
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

    @pytest.mark.tags(CaseLabel.L2)
    def _test_during_indexed(self, connect, collection, args):
        """
        target: flushing will recover
        method: call function: create collection, then indexed, restart server and assert row count
        expected: row count equals nb
        """
        # disable_autoflush()
        ids = connect.bulk_insert(collection, big_entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, default_index)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        stats = connect.get_collection_stats(collection)
        # logging.getLogger().info(stats)
        # pdb.set_trace()
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        assert new_connect.count_entities(collection) == big_nb
        stats = connect.get_collection_stats(collection)
        for file in stats["partitions"][0]["segments"][0]["files"]:
            if file["field"] == field_name and file["name"] != "_raw":
                assert file["data_size"] > 0
                if file["index_type"] != default_index["index_type"]:
                    assert False
                else:
                    assert True

    @pytest.mark.tags(CaseLabel.L2)
    def _test_during_indexing(self, connect, collection, args):
        """
        target: flushing will recover
        method: call function: create collection, then indexing, restart server and assert row count
        expected: row count equals nb, server contitue to build index after restart
        """
        # disable_autoflush()
        loop = 5
        for i in range(loop):
            ids = connect.bulk_insert(collection, big_entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, default_index, _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        stats = connect.get_collection_stats(collection)
        # logging.getLogger().info(stats)
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count_2 = new_connect.count_entities(collection)
        logging.getLogger().info(res_count_2)
        assert res_count_2 == loop * big_nb
        status = new_connect._cmd("status")
        assert json.loads(status)["indexing"] == True
        # timeout = 100
        # start_time = time.time()
        # while time.time() - start_time < timeout:
        #     time.sleep(5)
        #     assert new_connect.count_entities(collection) == loop * big_nb
        #     stats = connect.get_collection_stats(collection)
        #     assert stats["row_count"] == loop * big_nb
        #     for file in stats["partitions"][0]["segments"][0]["files"]:
        #         # logging.getLogger().info(file)
        #         if file["field"] == field_name and file["name"] != "_raw":
        #             assert file["data_size"] > 0
        #             if file["index_type"] != default_index["index_type"]:
        #                 continue
        # for file in stats["partitions"][0]["segments"][0]["files"]:
        #     if file["field"] == field_name and file["name"] != "_raw":
        #         assert file["data_size"] > 0
        #         if file["index_type"] != default_index["index_type"]:
        #             assert False
        #         else:
        #             assert True

    @pytest.mark.tags(CaseLabel.L2)
    def _test_delete_flush_during_compacting(self, connect, collection, args):
        """
        target: verify server work after restart during compaction
        method: call function: create collection, then delete/flush/compacting, restart server and assert row count
            call `compact` again, compact pass
        expected: row count equals (nb - delete_length)
        """
        # disable_autoflush()
        ids = connect.bulk_insert(collection, big_entities)
        connect.flush([collection])
        delete_length = 1000
        loop = 10
        for i in range(loop):
            delete_ids = ids[i*delete_length:(i+1)*delete_length]
            delete_res = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        connect.compact(collection, _async=True)
        res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert res_count == big_nb - delete_length*loop
        info = connect.get_collection_stats(collection)
        size_old = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_old)
        # restart server
        assert restart_server(args["service_name"])
        # assert row count again
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
        res_count_2 = new_connect.count_entities(collection)
        logging.getLogger().info(res_count_2)
        assert res_count_2 == big_nb - delete_length*loop
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert size_before > size_after


    @pytest.mark.tags(CaseLabel.L2)
    def _test_insert_during_flushing_multi_collections(self, connect, args):
        """
        target: flushing will recover
        method: call function: create collections, then insert/flushing, restart server and assert row count
        expected: row count equals 0
        """
        # disable_autoflush()
        collection_num = 2
        collection_list = []
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            ids = connect.bulk_insert(collection_name, big_entities)
        connect.flush(collection_list, _async=True)
        res_count = connect.count_entities(collection_list[-1])
        logging.getLogger().info(res_count)
        if res_count < big_nb:
            # restart server
            assert restart_server(args["service_name"])
            # assert row count again
            new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"]) 
            res_count_2 = new_connect.count_entities(collection_list[-1])
            logging.getLogger().info(res_count_2)
            timeout = 300
            start_time = time.time()
            while time.time() - start_time < timeout:
                count_list = []
                break_flag = True
                for index, name in enumerate(collection_list):
                    tmp_count = new_connect.count_entities(name)
                    count_list.append(tmp_count)
                    logging.getLogger().info(count_list)
                    if tmp_count != big_nb:
                        break_flag = False
                        break
                if break_flag == True:
                    break
                time.sleep(10)
            for name in collection_list:
                assert new_connect.count_entities(name) == big_nb

    @pytest.mark.tags(CaseLabel.L2)
    def _test_insert_during_flushing_multi_partitions(self, connect, collection, args):
        """
        target: flushing will recover
        method: call function: create collection/partition, then insert/flushing, restart server and assert row count
        expected: row count equals 0
        """
        # disable_autoflush()
        partitions_num = 2
        partitions = []
        for i in range(partitions_num):
            tag_tmp = gen_unique_str()
            partitions.append(tag_tmp)
            connect.create_partition(collection, tag_tmp)
            ids = connect.bulk_insert(collection, big_entities, partition_name=tag_tmp)
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
            while new_connect.count_entities(collection) != big_nb * 2 and (time.time() - start_time < timeout):
                time.sleep(10)
                logging.getLogger().info(new_connect.count_entities(collection))
            res_count_3 = new_connect.count_entities(collection)
            logging.getLogger().info(res_count_3)
            assert res_count_3 == big_nb * 2