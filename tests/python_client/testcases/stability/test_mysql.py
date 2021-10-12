import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from pymilvus import IndexType

from utils.utils import get_milvus, gen_vectors, default_dim
from common.common_type import CaseLabel


class TestMysql:
    """
    ******************************************************************
      The following cases are used to test mysql failure
    ******************************************************************
    """

    @pytest.fixture(scope="function", autouse=True)
    def skip_check(self, connect, args):
        if args["service_name"].find("shards") != -1:
            reason = "Skip restart cases in shards mode"
            logging.getLogger().info(reason)
            pytest.skip(reason)

    @pytest.mark.tags(CaseLabel.L2)
    def _test_kill_mysql_during_index(self, connect, collection, args):
        big_nb = 20000
        index_param = {"nlist": 1024, "m": 16}
        index_type = IndexType.IVF_PQ
        vectors = gen_vectors(big_nb, default_dim)
        status, ids = connect.bulk_insert(collection, vectors, ids=[i for i in range(big_nb)])
        status = connect.flush([collection])
        assert status.OK()
        status, res_count = connect.count_entities(collection)
        logging.getLogger().info(res_count)
        assert status.OK()
        assert res_count == big_nb
        logging.getLogger().info("Start create index async")
        status = connect.create_index(collection, index_type, index_param, _async=True)
        time.sleep(2)
        logging.getLogger().info("Start play mysql failure")
        # pass
        new_connect = get_milvus(args["ip"], args["port"], handler=args["handler"])
        status, res_count = new_connect.count_entities(collection)
        assert status.OK()
        assert res_count == big_nb
