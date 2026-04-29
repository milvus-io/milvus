import json
import time
from time import sleep

import pytest
from chaos import chaos_commons as cc
from chaos import constants
from chaos.chaos_commons import assert_statistic
from chaos.checker import (
    AddFieldChecker,
    DeleteChecker,
    FlushChecker,
    FullTextSearchChecker,
    GeoQueryChecker,
    HybridSearchChecker,
    InsertChecker,
    JsonQueryChecker,
    Op,
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
from pymilvus import DataType, FunctionType, connections
from utils.util_k8s import get_milvus_instance_name, wait_pods_ready
from utils.util_log import test_log as log

_VECTOR_DTYPES = {
    DataType.FLOAT_VECTOR,
    DataType.FLOAT16_VECTOR,
    DataType.BFLOAT16_VECTOR,
    DataType.BINARY_VECTOR,
    DataType.SPARSE_FLOAT_VECTOR,
    DataType.INT8_VECTOR,
}


def _build_checker_schema(dim=8):
    """Build the shared all-datatype schema, stripped for the 2.6-latest image.

    The chaos-test image used by milvus_cdc_chaos_test/verify_test rejects
    two things the shared gen_all_datatype_collection_schema includes by
    default:
      - FunctionType.MINHASH (error: "check function params with unknown
        function type")
      - nullable=True on FLOAT_VECTOR (error: "vector type not support null")

    Drop the MinHash function and its output field, and force nullable=False
    on every vector field so the server accepts the schema.
    """
    schema = cf.gen_all_datatype_collection_schema(dim=dim)
    schema.functions[:] = [f for f in schema.functions if f.type != FunctionType.MINHASH]
    schema.fields[:] = [f for f in schema.fields if f.name != "minhash_emb"]
    for f in schema.fields:
        if f.dtype in _VECTOR_DTYPES:
            f.nullable = False
    return schema


def get_all_collections():
    try:
        with open("/tmp/ci_logs/chaos_test_all_collections.json") as f:
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
        self.milvus_sys = MilvusSys(alias="default")
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        schema = _build_checker_schema()
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name, schema=schema),
            Op.upsert: UpsertChecker(collection_name=c_name, schema=schema),
            Op.flush: FlushChecker(collection_name=c_name, schema=schema),
            Op.search: SearchChecker(collection_name=c_name, schema=schema),
            Op.full_text_search: FullTextSearchChecker(collection_name=c_name, schema=schema),
            Op.hybrid_search: HybridSearchChecker(collection_name=c_name, schema=schema),
            Op.query: QueryChecker(collection_name=c_name, schema=schema),
            Op.text_match: TextMatchChecker(collection_name=c_name, schema=schema),
            Op.phrase_match: PhraseMatchChecker(collection_name=c_name, schema=schema),
            Op.json_query: JsonQueryChecker(collection_name=c_name, schema=schema),
            Op.geo_query: GeoQueryChecker(collection_name=c_name, schema=schema),
            Op.delete: DeleteChecker(collection_name=c_name, schema=schema),
            Op.add_field: AddFieldChecker(collection_name=c_name, schema=schema),
        }
        log.info(f"init_health_checkers: {checkers}")
        self.health_checkers = checkers

    @pytest.fixture(scope="function", params=get_all_collections())
    def collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    @pytest.mark.tags(CaseLabel.CDC)
    def test_operations(self, request_duration, is_check, collection_name):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr("default"))
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
            sleep(request_duration // 10)
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
