from tests.utils import *
from tests.constants import *

uniq_id = "load_partitions"

class TestLoadPartitions:
    """
    ******************************************************************
      The following cases are used to test `load_partitions` function
    ******************************************************************
    """
    def test_load_partitions(self, connect, collection):
        '''
        target: test load collection and wait for loading collection
        method: insert then flush, when flushed, try load collection
        expected: no errors
        '''
        partition_tag = "lvn9pq34u8rasjk"
        connect.create_partition(collection, partition_tag + "1")
        ids = connect.insert(collection, default_entities, partition_tag=partition_tag + "1")

        connect.create_partition(collection, partition_tag + "2")
        ids = connect.insert(collection, default_entity, partition_tag=partition_tag + "2")

        connect.flush([collection])
        connect.load_partitions(collection, [partition_tag + "2"])
