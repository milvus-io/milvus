import time

import pytest
from time import sleep
from pymilvus import connections
from chaos.checker import (CollectionCreateChecker,
                           InsertChecker,
                           BulkInsertChecker,
                           UpsertChecker,
                           FlushChecker,
                           SearchChecker,
                           FullTextSearchChecker,
                           HybridSearchChecker,
                           QueryChecker,
                           TextMatchChecker,
                           IndexCreateChecker,
                           DeleteChecker,
                           CollectionDropChecker,
                           Op,
                           EventRecords,
                           ResultAnalyzer
                           )
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common.milvus_sys import MilvusSys
from chaos.chaos_commons import assert_statistic
from chaos import constants
from delayed_assert import assert_expectations


class TestBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    host = '127.0.0.1'
    port = 19530
    _chaos_config = None
    health_checkers = {}


class TestOperations(TestBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, user, password, milvus_ns, minio_host, enable_import):
        if user and password:
            # log.info(f"connect to {host}:{port} with user {user} and password {password}")
            connections.connect('default', host=host, port=port, user=user, password=password, secure=True)
        else:
            connections.connect('default', host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        log.info("connect to milvus successfully")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_sys = MilvusSys(alias='default')
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.enable_import = enable_import
        self.minio_endpoint = f"{minio_host}:9000"
        self.ms = MilvusSys()
        self.bucket_name = self.ms.index_nodes[0]["infos"]["system_configurations"]["minio_bucket_name"]

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        checkers = {
            Op.create: CollectionCreateChecker(collection_name=c_name),
            Op.insert: InsertChecker(collection_name=c_name),
            Op.upsert: UpsertChecker(collection_name=c_name),
            Op.flush: FlushChecker(collection_name=c_name),
            Op.index: IndexCreateChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.full_text_search: FullTextSearchChecker(collection_name=c_name),
            Op.hybrid_search: HybridSearchChecker(collection_name=c_name),
            Op.query: QueryChecker(collection_name=c_name),
            Op.text_match: TextMatchChecker(collection_name=c_name),
            Op.delete: DeleteChecker(collection_name=c_name),
            Op.drop: CollectionDropChecker(collection_name=c_name)
        }
        if bool(self.enable_import):
            checkers[Op.bulk_insert] = BulkInsertChecker(collection_name=c_name,
                                                         bucket_name=self.bucket_name,
                                                         minio_endpoint=self.minio_endpoint)
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, request_duration, is_check):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        event_records = EventRecords()
        c_name = None
        event_records.insert("init_health_checkers", "start")
        self.init_health_checkers(collection_name=c_name)
        event_records.insert("init_health_checkers", "finished")
        tasks = cc.start_monitor_threads(self.health_checkers)
        log.info("*********************Load Start**********************")
        # wait request_duration
        request_duration = request_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        for i in range(10):
            sleep(request_duration // 10)
            # add an event so that the chaos can start to apply
            if i == 3:
                event_records.insert("init_chaos", "ready")
            for k, v in self.health_checkers.items():
                v.check_result()
        if is_check:
            assert_statistic(self.health_checkers, succ_rate_threshold=0.98)
            assert_expectations()
        # wait all pod ready
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={self.release_name}")
        time.sleep(60)
        cc.check_thread_status(tasks)
        for k, v in self.health_checkers.items():
            v.pause()
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        ra.show_result_table()
        log.info("*********************Chaos Test Completed**********************")
