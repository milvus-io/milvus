import time
import pytest
import threading
import json
from time import sleep
from pymilvus import connections, db
from chaos.checker import (InsertChecker,
                           UpsertChecker,
                           SearchChecker,
                           QueryChecker,
                           DeleteChecker,
                           Op,
                           ResultAnalyzer
                           )
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from chaos import constants


def get_all_collections():
    try:
        with open("/tmp/ci_logs/all_collections.json", "r") as f:
            data = json.load(f)
            all_collections = data["all"]
    except Exception as e:
        log.warning(f"get_all_collections error: {e}")
        return [None]
    return all_collections


class TestBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_compact = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    host = '127.0.0.1'
    port = 19530
    _chaos_config = None
    health_checkers = {}


class TestOperations(TestBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, user, password, db_name, milvus_ns):
        if user and password:
            log.info(f"connect to {host}:{port} with user {user} and password {password}")
            connections.connect('default', uri=f"{host}:{port}", token=f"{user}:{password}")
        else:
            connections.connect('default', host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        all_dbs = db.list_database()
        log.info(f"all dbs: {all_dbs}")
        if db_name not in all_dbs:
            db.create_database(db_name)
        db.using_database(db_name)
        log.info(f"connect to milvus {host}:{port}, db {db_name} successfully")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_ns = milvus_ns

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name),
            Op.upsert: UpsertChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.query: QueryChecker(collection_name=c_name),
            Op.delete: DeleteChecker(collection_name=c_name),
        }
        self.health_checkers = checkers
        return checkers

    @pytest.fixture(scope="function", params=get_all_collections())
    def collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, request_duration, is_check, collection_name, collection_num, db_name):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        all_checkers = []

        def worker(c_name):
            log.info(f"start checker for collection name: {c_name}")
            op_checker = self.init_health_checkers(collection_name=c_name)
            all_checkers.append(op_checker)
            # insert data in init stage
            try:
                num_entities = op_checker[Op.insert].c_wrap.num_entities
                if num_entities < 200000:
                    nb = 5000
                    num_to_insert = 200000 - num_entities
                    for i in range(num_to_insert//nb):
                        op_checker[Op.insert].insert_data(nb=nb)
                else:
                    log.info(f"collection {c_name} has enough data {num_entities}, skip insert data")
            except Exception as e:
                log.error(f"insert data error: {e}")
        threads = []
        for i in range(collection_num):
            c_name = collection_name if collection_name else f"DB_{db_name}_Collection_{i}_Checker"
            thread = threading.Thread(target=worker, args=(c_name,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        for checker in all_checkers:
            cc.start_monitor_threads(checker)

        log.info("*********************Load Start**********************")
        request_duration = request_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        for i in range(10):
            sleep(request_duration//10)
            for checker in all_checkers:
                for k, v in checker.items():
                    v.check_result()
        try:
            ra = ResultAnalyzer()
            ra.get_stage_success_rate()
            ra.show_result_table()
        except Exception as e:
            log.error(f"get stage success rate error: {e}")
        log.info("*********************Chaos Test Completed**********************")
