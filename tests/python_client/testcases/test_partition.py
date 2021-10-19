import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils.utils import *
from common.constants import *
from common.common_type import CaseLabel

TIMEOUT = 120


class TestCreateBase:
    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_a(self, connect, collection):
        """
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        """
        connect.create_partition(collection, default_tag)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="skip temporarily for debug")
    @pytest.mark.timeout(600)
    def test_create_partition_limit(self, connect, collection, args):
        """
        target: test create partitions, check status returned
        method: call function: create_partition for 4097 times
        expected: exception raised
        """
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
            t = threading.Thread(target=create, args=(m, threads_num))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        tag_tmp = gen_unique_str()
        with pytest.raises(Exception) as e:
            connect.create_partition(collection, tag_tmp)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_repeat(self, connect, collection):
        """
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        """
        connect.create_partition(collection, default_tag)
        try:
            connect.create_partition(collection, default_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "create partition failed: partition name = %s already exists" % default_tag
        assert compare_list_elements(connect.list_partitions(collection), [default_tag, '_default'])

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_partition_collection_not_existed(self, connect):
        """
        target: verify the response when creating a partition with a non_existing collection
        method: create a partition with a non_existing collection
        expected: raise an exception
        """
        collection_name = gen_unique_str()
        try:
            connect.create_partition(collection_name, default_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "create partition failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_name_name_none(self, connect, collection):
        """
        target: test create partition, tag name set None, check status returned
        method: call function: create_partition
        expected: status ok
        """
        tag_name = None
        try:
            connect.create_partition(collection, tag_name)
        except Exception as e:
            assert e.args[0] == "`partition_name` value None is illegal"

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_different_partition_names(self, connect, collection):
        """
        target: test create partition twice with different names
        method: call function: create_partition, and again
        expected: status ok
        """
        connect.create_partition(collection, default_tag)
        tag_name = gen_unique_str()
        connect.create_partition(collection, tag_name)
        assert compare_list_elements(connect.list_partitions(collection), [default_tag, tag_name, '_default'])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_insert_default(self, connect, id_collection):
        """
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        """
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        result = connect.insert(id_collection, default_entities)
        assert len(result.primary_keys) == len(ids)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_insert_with_tag(self, connect, id_collection):
        """
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        """
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        result = connect.insert(id_collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == len(ids)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_insert_with_tag_not_existed(self, connect, collection):
        """
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status not ok
        """
        tag_new = "tag_new"
        connect.create_partition(collection, default_tag)
        ids = [i for i in range(default_nb)]
        try:
            connect.insert(collection, default_entities, partition_name=tag_new)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "partitionID of partitionName:%s can not be find" % tag_new

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_insert_same_tags(self, connect, id_collection):
        """
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        """
        connect.create_partition(id_collection, default_tag)
        ids = [i for i in range(default_nb)]
        result = connect.insert(id_collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        ids = [(i+default_nb) for i in range(default_nb)]
        new_result = connect.insert(id_collection, default_entities, partition_name=default_tag)
        assert len(new_result.primary_keys) == default_nb
        connect.flush([id_collection])
        res = connect.get_collection_stats(id_collection)
        assert res["row_count"] == default_nb * 2

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_partition_insert_same_tags_two_collections(self, connect, collection):
        """
        target: test create two partitions, and insert vectors with the same tag to each collection, check status returned
        method: call function: create_partition
        expected: status ok, collection length is correct
        """
        connect.create_partition(collection, default_tag)
        collection_new = gen_unique_str()
        connect.create_collection(collection_new, default_fields)
        connect.create_partition(collection_new, default_tag)
        result = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(result.primary_keys) == default_nb
        new_result = connect.insert(collection_new, default_entities, partition_name=default_tag)
        assert len(new_result.primary_keys) == default_nb
        connect.flush([collection, collection_new])
        res = connect.get_collection_stats(collection)
        assert res["row_count"] == default_nb
        res = connect.get_collection_stats(collection_new)
        assert res["row_count"] == default_nb


class TestShowBase:

    """
    ******************************************************************
      The following cases are used to test `list_partitions` function 
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_list_partitions(self, connect, collection):
        """
        target: test show partitions, check status and partitions returned
        method: create partition first, then call function: list_partitions
        expected: status ok, partition correct
        """
        connect.create_partition(collection, default_tag)
        assert compare_list_elements(connect.list_partitions(collection), [default_tag, '_default'])

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_partitions_no_partition(self, connect, collection):
        """
        target: test show partitions with collection name, check status and partitions returned
        method: call function: list_partitions
        expected: status ok, partitions correct
        """
        res = connect.list_partitions(collection)
        assert compare_list_elements(res, ['_default'])

    @pytest.mark.tags(CaseLabel.L0)
    def test_show_multi_partitions(self, connect, collection):
        """
        target: test show partitions, check status and partitions returned
        method: create partitions first, then call function: list_partitions
        expected: status ok, partitions correct
        """
        tag_new = gen_unique_str()
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, tag_new)
        res = connect.list_partitions(collection)
        assert compare_list_elements(res, [default_tag, tag_new, '_default'])


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

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_a(self, connect, collection):
        """
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        """
        connect.create_partition(collection, default_tag)
        res = connect.has_partition(collection, default_tag)
        logging.getLogger().info(res)
        assert res

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_multi_partitions(self, connect, collection):
        """
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        """
        for tag_name in [default_tag, "tag_new", "tag_new_new"]:
            connect.create_partition(collection, tag_name)
        for tag_name in [default_tag, "tag_new", "tag_new_new"]:
            res = connect.has_partition(collection, tag_name)
            assert res

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_name_not_existed(self, connect, collection):
        """
        target: test has_partition, check status and result
        method: then call function: has_partition, with tag not existed
        expected: status ok, result empty
        """
        res = connect.has_partition(collection, default_tag)
        logging.getLogger().info(res)
        assert not res

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_collection_not_existed(self, connect, collection):
        """
        target: test has_partition, check status and result
        method: then call function: has_partition, with collection not existed
        expected: status not ok
        """
        collection_name = "not_existed_collection"
        try:
            connect.has_partition(collection_name, default_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "HasPartition failed: can't find collection: %s" % collection_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_with_invalid_tag_name(self, connect, collection, get_tag_name):
        """
        target: test has partition, with invalid tag name, check status returned
        method: call function: has_partition
        expected: status ok
        """
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.has_partition(collection, tag_name)


class TestDropBase:

    """
    ******************************************************************
      The following cases are used to test `drop_partition` function 
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_a(self, connect, collection):
        """
        target: test drop partition, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status ok, no partitions in db
        """
        connect.create_partition(collection, default_tag)
        res1 = connect.list_partitions(collection)
        assert default_tag in res1
        connect.drop_partition(collection, default_tag)
        res2 = connect.list_partitions(collection)
        assert default_tag not in res2

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_name_not_existed(self, connect, collection):
        """
        target: test drop partition, but tag not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        """
        connect.create_partition(collection, default_tag)
        new_tag = "new_tag"
        try:
            connect.drop_partition(collection, new_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "DropPartition failed: partition %s does not exist" % new_tag

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_name_not_existed_A(self, connect, collection):
        """
        target: test drop partition, but collection not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        """
        connect.create_partition(collection, default_tag)
        new_collection = gen_unique_str()
        try:
            connect.drop_partition(new_collection, default_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "DropPartition failed: can't find collection: %s" % new_collection

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_repeatedly(self, connect, collection):
        """
        target: test drop partition twice, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok, no partitions in db
        """
        connect.create_partition(collection, default_tag)
        connect.drop_partition(collection, default_tag)
        time.sleep(2)
        try:
            connect.drop_partition(collection, default_tag)
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "DropPartition failed: partition %s does not exist" % default_tag
        tag_list = connect.list_partitions(collection)
        assert default_tag not in tag_list

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_create(self, connect, collection):
        """
        target: test drop partition, and create again, check status
        method: create partitions first, then call function: drop_partition, create_partition
        expected: status not ok, partition in db
        """
        connect.create_partition(collection, default_tag)
        assert compare_list_elements(connect.list_partitions(collection), [default_tag, '_default'])
        connect.drop_partition(collection, default_tag)
        assert compare_list_elements(connect.list_partitions(collection), ['_default'])
        time.sleep(2)
        connect.create_partition(collection, default_tag)
        assert compare_list_elements(connect.list_partitions(collection), [default_tag, '_default'])


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

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_with_invalid_collection_name(self, connect, collection, get_collection_name):
        """
        target: test drop partition, with invalid collection name, check status returned
        method: call function: drop_partition
        expected: status not ok
        """
        collection_name = get_collection_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection_name, default_tag)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_with_invalid_tag_name(self, connect, collection, get_tag_name):
        """
        target: test drop partition, with invalid tag name, check status returned
        method: call function: drop_partition
        expected: status not ok
        """
        tag_name = get_tag_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.drop_partition(collection, tag_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_partitions_with_invalid_collection_name(self, connect, collection, get_collection_name):
        """
        target: test show partitions, with invalid collection name, check status returned
        method: call function: list_partitions
        expected: status not ok
        """
        collection_name = get_collection_name
        connect.create_partition(collection, default_tag)
        with pytest.raises(Exception) as e:
            connect.list_partitions(collection_name)


class TestNewCase(object):

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_default_partition_A(self, connect, collection):
        """
        target: test drop partition of default, check status returned
        method: call function: drop_partition
        expected: status not ok
        """
        try:
            connect.drop_partition(collection, partition_name='_default')
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "DropPartition failed: default partition cannot be deleted"
        list_partition = connect.list_partitions(collection)
        assert '_default' in list_partition

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_default_partition_B(self, connect, collection):
        """
        target: test drop partition of default, check status returned
        method: call function: drop_partition
        expected: status not ok
        """
        connect.create_partition(collection, default_tag)
        try:
            connect.drop_partition(collection, partition_name='_default')
        except Exception as e:
            code = getattr(e, 'code', "The exception does not contain the field of code.")
            assert code == 1
            message = getattr(e, 'message', "The exception does not contain the field of message.")
            assert message == "DropPartition failed: default partition cannot be deleted"
        list_partition = connect.list_partitions(collection)
        assert '_default' in list_partition
