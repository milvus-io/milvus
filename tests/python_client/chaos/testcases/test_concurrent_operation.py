import time
import pytest
import json
from time import sleep
from pymilvus import connections
from chaos.checker import (InsertChecker,
                           UpsertChecker,
                           FlushChecker,
                           SearchChecker,
                           FullTextSearchChecker,
                           HybridSearchChecker,
                           QueryChecker,
                           TextMatchChecker,
                           PhraseMatchChecker,
                           JsonQueryChecker,
                           GeoQueryChecker,
                           DeleteChecker,
                           AddFieldChecker,
                           SnapshotChecker,
                           SnapshotRestoreChecker,
                           Op,
                           ResultAnalyzer
                           )
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common import common_func as cf
from common.milvus_sys import MilvusSys
from chaos.chaos_commons import assert_statistic
from common.common_type import CaseLabel
from chaos import constants
from delayed_assert import assert_expectations


def get_all_collections():
    try:
        with open("/tmp/ci_logs/chaos_test_all_collections.json", "r") as f:
            data = json.load(f)
            all_collections = data["all"]
            log.info(f"all_collections: {all_collections}")
            if all_collections == [] or all_collections == "":
                return [None]
            else:
                return all_collections
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
    def connection(self, host, port, user, password, uri, token, milvus_ns):
        # Prioritize uri and token for connection
        if uri:
            actual_uri = uri
        else:
            actual_uri = f"http://{host}:{port}"

        if token:
            actual_token = token
        else:
            actual_token = f"{user}:{password}" if user and password else None

        if actual_token:
            connections.connect('default', uri=actual_uri, token=actual_token)
        else:
            connections.connect('default', uri=actual_uri)

        if connections.has_connection("default") is False:
            raise Exception("no connections")
        log.info(f"connect to milvus successfully, uri: {actual_uri}")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.uri = actual_uri
        self.token = actual_token
        self.milvus_sys = MilvusSys(alias='default')
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name),
            Op.upsert: UpsertChecker(collection_name=c_name),
            Op.flush: FlushChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.full_text_search: FullTextSearchChecker(collection_name=c_name),
            Op.hybrid_search: HybridSearchChecker(collection_name=c_name),
            Op.query: QueryChecker(collection_name=c_name),
            Op.text_match: TextMatchChecker(collection_name=c_name),
            Op.phrase_match: PhraseMatchChecker(collection_name=c_name),
            Op.json_query: JsonQueryChecker(collection_name=c_name),
            Op.geo_query: GeoQueryChecker(collection_name=c_name),
            Op.delete: DeleteChecker(collection_name=c_name),
            Op.add_field: AddFieldChecker(collection_name=c_name),
            Op.snapshot: SnapshotChecker(collection_name=c_name),
            Op.restore_snapshot: SnapshotRestoreChecker(),
        }
        log.info(f"init_health_checkers: {checkers}")
        self.health_checkers = checkers

    @pytest.fixture(scope="function", params=get_all_collections())
    def collection_name(self, request):
        log.info(f"collection_name: {request.param}")
        if request.param == [] or request.param == "":
            yield None
        else:
            yield request.param

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, request_duration, is_check, collection_name):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        # event_records = EventRecords()
        c_name = collection_name if collection_name else cf.gen_unique_str("Checker_")
        # event_records.insert("init_health_checkers", "start")
        self.init_health_checkers(collection_name=c_name)
        # event_records.insert("init_health_checkers", "finished")
        cc.start_monitor_threads(self.health_checkers)
        log.info("*********************Load Start**********************")
        request_duration = request_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        for i in range(10):
            sleep(request_duration//10)
            for k, v in self.health_checkers.items():
                v.check_result()
                # log.info(v.check_result())
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={self.release_name}")
        time.sleep(60)
        ra = ResultAnalyzer()
        ra.get_stage_success_rate()
        if is_check:
            assert_statistic(self.health_checkers)
            assert_expectations()
        log.info("*********************Chaos Test Completed**********************")
