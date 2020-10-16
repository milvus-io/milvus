import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

TIMEOUT = 120

class TestCreateBase:
    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    def test_create_partition(self, connect, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        connect.create_partition(collection, default_tag)

    # TODO: enable
    @pytest.mark.level(2)
    @pytest.mark.timeout(600)
    def test_create_partition_limit(self, connect, collection, args):
        '''
        target: test create partitions, check status returned
        method: call function: create_partition for 4097 times
        expected: exception raised
        '''
        threads_num = 8
        threads = []
        if args["handler"] == "HTTP":
            pytest.skip("skip in http mode")

        def create(connect, threads_num):
            for i in range(max_partition_num // threads_num):
                tag_tmp = gen_unique_str()
                connect.create_partition(collection, tag_tmp)

        for i in range(threads_num):
            m = get_milvus(host=args["ip"], port=args["port"], handler=args["handler"])
            t = threading.Thread(target=create, args=(m, threads_num, ))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        tag_tmp = gen_unique_str()
        with pytest.raises(Exception) as e:
            connect.create_partition(collection, tag_tmp)

    def test_create_partition_repeat(self, connect, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.create_partition(collection, default_tag)

    def test_create_partition_collection_not_existed(self, connect):
        '''
        target: test create partition, its owner collection name not existed in db, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        collection_name = gen_unique_str()
        with pytest.raises(Exception) as e:
            connect.create_partition(collection_name, default_tag)

    def test_create_partition_tag_name_None(self, connect, collection):
        '''
        target: test create partition, tag name set None, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        tag_name = None
        with pytest.raises(Exception) as e:
            connect.create_partition(collection, tag_name)

    def test_create_different_partition_tags(self, connect, collection):
        '''
        target: test create partition twice with different names
        method: call function: create_partition, and again
        expected: status ok
        '''
        connect.create_partition(collection, default_tag)
        tag_name = gen_unique_str()
        connect.create_partition(collection, tag_name)
        tag_list = connect.list_partitions(collection)
        assert default_tag in tag_list
        assert tag_name in tag_list
        assert "_default" in tag_list

    def test_create_partition_insert_default(self, connect, id_collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        insert_ids = connect.insert(id_collection, default_entities, ids)
        assert len(insert_ids) == len(ids)
 
    def test_create_partition_insert_with_tag(self, connect, id_collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        insert_ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        assert len(insert_ids) == len(ids)

    def test_create_partition_insert_with_tag_not_existed(self, connect, collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        tag_new = "tag_new"
        connect.create_partition(collection, default_tag)
        ids = [i for i in range(default_nb)]
        with pytest.raises(Exception) as e:
            insert_ids = connect.insert(collection, default_entities, ids, partition_tag=tag_new)

    def test_create_partition_insert_same_tags(self, connect, id_collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        insert_ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        ids = [(i+default_nb) for i in range(default_nb)]
        new_insert_ids = connect.insert(id_collection, default_entities, ids, partition_tag=default_tag)
        connect.flush([id_collection])
        res = connect.count_entities(id_collection)
        assert res == default_nb * 2

    @pytest.mark.level(2)
    def test_create_partition_insert_same_tags_two_collections(self, connect, collection):
        '''
        target: test create two partitions, and insert vectors with the same tag to each collection, check status returned
        method: call function: create_partition
        expected: status ok, collection length is correct
        '''
        connect.create_partition(collection, default_tag)
        collection_new = gen_unique_str()
        connect.create_collection(collection_new, default_fields)
        connect.create_partition(collection_new, default_tag)
        ids = connect.insert(collection, default_entities, partition_tag=default_tag)
        ids = connect.insert(collection_new, default_entities, partition_tag=default_tag)
        connect.flush([collection, collection_new])
        res = connect.count_entities(collection)
        assert res == default_nb
        res = connect.count_entities(collection_new)
        assert res == default_nb


class TestShowBase:

    """
    ******************************************************************
      The following cases are used to test `list_partitions` function 
    ******************************************************************
    """
    def test_list_partitions(self, connect, collection):
        '''
        target: test show partitions, check status and partitions returned
        method: create partition first, then call function: list_partitions
        expected: status ok, partition correct
        '''
        connect.create_partition(collection, default_tag)
        res = connect.list_partitions(collection)
        assert default_tag in res

    def test_list_partitions_no_partition(self, connect, collection):
        '''
        target: test show partitions with collection name, check status and partitions returned
        method: call function: list_partitions
        expected: status ok, partitions correct
        '''
        res = connect.list_partitions(collection)
        assert len(res) == 1

    def test_show_multi_partitions(self, connect, collection):
        '''
        target: test show partitions, check status and partitions returned
        method: create partitions first, then call function: list_partitions
        expected: status ok, partitions correct
        '''
        tag_new = gen_unique_str()
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, tag_new)
        res = connect.list_partitions(collection)
        assert default_tag in res
        assert tag_new in res


class TestHasBase:

    """
    ******************************************************************
      The following cases are used to test `has_partition` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_tag_name(self, request):
        yield request.param

    def test_has_partition(self, connect, collection):
        '''
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        '''
        connect.create_partition(collection, default_tag)
        res = connect.has_partition(collection, default_tag)
        logging.getLogger().info(res)
        assert res

    def test_has_partition_multi_partitions(self, connect, collection):
        '''
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        '''
        for tag_name in [default_tag, "tag_new", "tag_new_new"]:
            connect.create_partition(collection, tag_name)
        for tag_name in [default_tag, "tag_new", "tag_new_new"]:
            res = connect.has_partition(collection, tag_name)
            assert res

    def test_has_partition_tag_not_existed(self, connect, collection):
        '''
        target: test has_partition, check status and result
        method: then call function: has_partition, with tag not existed
        expected: status ok, result empty
        '''
        res = connect.has_partition(collection, default_tag)
        logging.getLogger().info(res)
        assert not res

    def test_has_partition_collection_not_existed(self, connect, collection):
        '''
        target: test has_partition, check status and result
        method: then call function: has_partition, with collection not existed
        expected: status not ok
        '''
        with pytest.raises(Exception) as e:
            res = connect.has_partition("not_existed_collection", default_tag)

    @pytest.mark.level(2)
    def test_has_partition_with_invalid_tag_name(self, connect, collection, get_tag_name):
        '''
        target: test has partition, with invalid tag name, check status returned
        method: call function: has_partition
        expected: status ok
        '''
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            res = connect.has_partition(collection, tag_name)


class TestDropBase:

    """
    ******************************************************************
      The following cases are used to test `drop_partition` function 
    ******************************************************************
    """
    def test_drop_partition(self, connect, collection):
        '''
        target: test drop partition, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status ok, no partitions in db
        '''
        connect.create_partition(collection, default_tag)
        connect.drop_partition(collection, default_tag)
        res = connect.list_partitions(collection)
        tag_list = []
        assert default_tag not in tag_list

    def test_drop_partition_tag_not_existed(self, connect, collection):
        '''
        target: test drop partition, but tag not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        connect.create_partition(collection, default_tag)
        new_tag = "new_tag"
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection, new_tag)

    def test_drop_partition_tag_not_existed_A(self, connect, collection):
        '''
        target: test drop partition, but collection not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        connect.create_partition(collection, default_tag)
        new_collection = gen_unique_str()
        with pytest.raises(Exception) as e:
            connect.drop_partition(new_collection, default_tag)

    @pytest.mark.level(2)
    def test_drop_partition_repeatedly(self, connect, collection):
        '''
        target: test drop partition twice, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok, no partitions in db
        '''
        connect.create_partition(collection, default_tag)
        connect.drop_partition(collection, default_tag)
        time.sleep(2)
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection, default_tag)
        tag_list = connect.list_partitions(collection)
        assert default_tag not in tag_list

    def test_drop_partition_create(self, connect, collection):
        '''
        target: test drop partition, and create again, check status
        method: create partitions first, then call function: drop_partition, create_partition
        expected: status not ok, partition in db
        '''
        connect.create_partition(collection, default_tag)
        connect.drop_partition(collection, default_tag)
        time.sleep(2)
        connect.create_partition(collection, default_tag)
        tag_list = connect.list_partitions(collection)
        assert default_tag in tag_list


class TestNameInvalid(object):
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_drop_partition_with_invalid_collection_name(self, connect, collection, get_collection_name):
        '''
        target: test drop partition, with invalid collection name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        collection_name = get_collection_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection_name, default_tag)

    @pytest.mark.level(2)
    def test_drop_partition_with_invalid_tag_name(self, connect, collection, get_tag_name):
        '''
        target: test drop partition, with invalid tag name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection, tag_name)

    @pytest.mark.level(2)
    def test_list_partitions_with_invalid_collection_name(self, connect, collection, get_collection_name):
        '''
        target: test show partitions, with invalid collection name, check status returned
        method: call function: list_partitions
        expected: status not ok
        '''
        collection_name = get_collection_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            res = connect.list_partitions(collection_name)
