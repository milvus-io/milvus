import pytest
from .utils import *
from .constants import *

uid = "list_collections"

class TestListCollections:
    """
    ******************************************************************
      The following cases are used to test `list_collections` function
    ******************************************************************
    """
    def test_list_collections(self, connect, collection):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        assert collection in connect.list_collections()

    def test_list_collections_multi_collections(self, connect):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        collection_num = 50
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            connect.create_collection(collection_name, default_fields)
            assert collection_name in connect.list_collections()

    @pytest.mark.level(2)
    def test_list_collections_without_connection(self, dis_connect):
        '''
        target: test list collections, without connection
        method: calling list collections with correct params, with a disconnected instance
        expected: list collections raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.list_collections()

    def test_list_collections_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the value returned by list_collections method
        expected: False
        '''
        collection_name = gen_unique_str(uid)
        assert collection_name not in connect.list_collections()

    @pytest.mark.level(2)
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

    @pytest.mark.level(2)
    def test_list_collections_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread, 
        expected: collections are created
        '''
        threads_num = 4 
        threads = []
        collection_name = gen_unique_str(uid)
        connect.create_collection(collection_name, default_fields)

        def _list():
            assert collection_name in connect.list_collections()
        for i in range(threads_num):
            t = threading.Thread(target=_list, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()
