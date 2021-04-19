from tests.utils import *
from tests.constants import *

uniq_id = "load_collection"

class TestLoadCollection:
    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """
    def test_load_collection(self, connect, collection_without_loading):
        '''
        target: test load collection and wait for loading collection
        method: insert then flush, when flushed, try load collection
        expected: no errors
        '''
        collection = collection_without_loading
        ids = connect.insert(collection, default_entities)
        ids = connect.insert(collection, default_entity)
        connect.flush([collection])
        connect.load_collection(collection)
