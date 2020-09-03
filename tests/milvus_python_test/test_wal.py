import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *

dim = 128
collection_id = "test_wal"
segment_row_count = 5000
WAL_TIMEOUT = 60
tag = "1970_01_01"
insert_interval_time = 1.5
nb = 6000
field_name = "float_vector"
entity = gen_entities(1)
binary_entity = gen_binary_entities(1)
entities = gen_entities(nb)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields() 


class TestWalBase:
    """
    ******************************************************************
      The following cases are used to test WAL functionality
    ******************************************************************
    """

    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_server_crashed_recovery(self, connect, collection):
        '''
        target: test wal when server crashed unexpectedly and restarted
        method: add vectors, server killed before flush, restarted server and flush
        expected: status ok, add request is recovered and vectors added
        '''
        ids = connect.insert(collection, entity)
        connect.flush([collection])
        res = connect.count_entities(collection)
        logging.getLogger().info(res) # should be 0 because no auto flush
        logging.getLogger().info("Stop server and restart")
        # kill server and restart. auto flush should be set to 15 seconds.
        # time.sleep(15)
        connect.flush([collection])
        res = connect.count_entities(collection)
        assert res == 1
        res = connect.get_entity_by_id(collection, [ids[0]]) 
        logging.getLogger().info(res)
