import pytest
import time
from utils.utils import *
from common.constants import *

uid = "list_collections"


class TestListCollections:
    """
    ******************************************************************
      The following cases are used to test `list_collections` function
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_list_collections(self, connect, collection):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        assert collection in connect.list_collections()

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_list_collections_multi_collections(self, connect):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        collection_num = 50
        collection_names = []
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            collection_names.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            assert collection_name in connect.list_collections()
        for i in range(collection_num):
            connect.drop_collection(collection_names[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_without_connection(self, dis_connect):
        '''
        target: test list collections, without connection
        method: calling list collections with correct params, with a disconnected instance
        expected: list collections raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.list_collections()

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, create this collection then drop it,
            assert the value returned by list_collections method
        expected: False
        '''
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)
        assert collection_name in connect.list_collections()
        connect.drop_collection(collection_name)
        assert collection_name not in connect.list_collections()

    # TODO: make sure to run this case in the end
    @pytest.mark.skip("r0.3-test")
    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_no_collection(self, connect):
        '''
        target: test show collections is correct or not, if no collection in db
        method: delete all collections,
            assert the value returned by list_collections method is equal to []
        expected: the status is ok, and the result is equal to []      
        '''
        result = connect.list_collections()
        if result:
            for collection_name in result:
                assert connect.has_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_multithread(self, connect):
        '''
        target: test list collection with multithread
        method: list collection using multithread,
        expected: list collections correctly
        '''
        threads_num = 10
        threads = []
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)

        def _list():
            assert collection_name in connect.list_collections()

        for i in range(threads_num):
            t = MyThread(target=_list)
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

