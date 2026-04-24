import time
from time import sleep

import pymilvus
import pytest
from chaos import chaos_commons as cc
from chaos import constants
from chaos.chaos_commons import assert_statistic
from chaos.checker import (
    AddFieldChecker,
    AlterCollectionChecker,
    CollectionCreateChecker,
    CollectionDropChecker,
    CollectionRenameChecker,
    DeleteChecker,
    EventRecords,
    FlushChecker,
    FullTextSearchChecker,
    GeoQueryChecker,
    HybridSearchChecker,
    IndexCreateChecker,
    InsertChecker,
    JsonQueryChecker,
    Op,
    PartialUpdateChecker,
    PhraseMatchChecker,
    QueryChecker,
    ResultAnalyzer,
    SearchChecker,
    TextMatchChecker,
    UpsertChecker,
)
from common import common_func as cf
from common.common_type import CaseLabel
from common.milvus_sys import MilvusSys
from delayed_assert import assert_expectations
from pymilvus import FunctionType, connections, utility
from utils.util_k8s import get_milvus_instance_name, wait_pods_ready
from utils.util_log import test_log as log


def _build_checker_schema(dim=8):
    """Build the shared all-datatype schema minus the MinHash function.

    The deployed chaos-test image (2.6-latest) rejects FunctionType.MINHASH
    with "unknown function type". Mutate the schema returned by the shared
    helper to drop the MinHash function and its output field so every
    Checker can create its collection.
    """
    schema = cf.gen_all_datatype_collection_schema(dim=dim)
    schema.functions[:] = [f for f in schema.functions if f.type != FunctionType.MINHASH]
    schema.fields[:] = [f for f in schema.fields if f.name != "minhash_emb"]
    return schema


class TestBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    host = "127.0.0.1"
    port = 19530
    _chaos_config = None
    health_checkers = {}


class TestOperations(TestBase):
    @pytest.fixture(scope="function", autouse=True)
    def connection(self, upstream_uri, upstream_token, milvus_ns):
        connections.connect("default", uri=upstream_uri, token=upstream_token)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        log.info("connect to milvus successfully")
        pymilvus_version = pymilvus.__version__
        server_version = utility.get_server_version()
        log.info(f"server version: {server_version}")
        log.info(f"pymilvus version: {pymilvus_version}")
        self.milvus_sys = MilvusSys(alias="default")
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        schema = _build_checker_schema()
        checkers = {
            Op.create: CollectionCreateChecker(collection_name=c_name, schema=schema),
            Op.insert: InsertChecker(collection_name=c_name, schema=schema),
            Op.upsert: UpsertChecker(collection_name=c_name, schema=schema),
            Op.partial_update: PartialUpdateChecker(collection_name=c_name, schema=schema),
            Op.flush: FlushChecker(collection_name=c_name, schema=schema),
            Op.index: IndexCreateChecker(collection_name=c_name, schema=schema),
            Op.search: SearchChecker(collection_name=c_name, schema=schema),
            Op.full_text_search: FullTextSearchChecker(collection_name=c_name, schema=schema),
            Op.hybrid_search: HybridSearchChecker(collection_name=c_name, schema=schema),
            Op.query: QueryChecker(collection_name=c_name, schema=schema),
            Op.text_match: TextMatchChecker(collection_name=c_name, schema=schema),
            Op.phrase_match: PhraseMatchChecker(collection_name=c_name, schema=schema),
            Op.json_query: JsonQueryChecker(collection_name=c_name, schema=schema),
            Op.geo_query: GeoQueryChecker(collection_name=c_name, schema=schema),
            Op.delete: DeleteChecker(collection_name=c_name, schema=schema),
            Op.drop: CollectionDropChecker(collection_name=c_name, schema=schema),
            Op.alter_collection: AlterCollectionChecker(collection_name=c_name, schema=schema),
            Op.add_field: AddFieldChecker(collection_name=c_name, schema=schema),
            Op.rename_collection: CollectionRenameChecker(collection_name=c_name, schema=schema),
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.CDC)
    def test_operations(self, request_duration, is_check):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr("default"))
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
