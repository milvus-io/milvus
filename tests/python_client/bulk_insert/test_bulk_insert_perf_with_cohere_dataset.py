import pytest
import threading
from time import sleep
from pymilvus import connections, DataType, FieldSchema, CollectionSchema
from chaos.checker import (BulkInsertChecker, Op)
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants
from delayed_assert import expect, assert_expectations


def assert_statistic(checkers, expectations={}):
    for k in checkers.keys():
        # expect succ if no expectations
        succ_rate = checkers[k].succ_rate()
        total = checkers[k].total()
        average_time = checkers[k].average_time
        if expectations.get(k, '') == constants.FAIL:
            log.info(
                f"Expect Fail: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
            expect(succ_rate < 0.49 or total < 2,
                   f"Expect Fail: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
        else:
            log.info(
                f"Expect Succ: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
            expect(succ_rate > 0.90 and total > 2,
                   f"Expect Succ: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")


class TestBulkInsertBase:
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


class TestBUlkInsertPerf(TestBulkInsertBase):

    def teardown_method(self):
        sleep(10)
        log.info(f'Alive threads: {threading.enumerate()}')

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, milvus_ns):
        connections.add_connection(default={"host": host, "port": port})
        connections.connect(alias='default')

        if connections.has_connection("default") is False:
            raise Exception("no connections")

    def init_health_checkers(self, collection_name=None, file_type="npy"):
        log.info("init health checkers")
        c_name = collection_name if collection_name else cf.gen_unique_str("BulkInsertChecker")
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="url", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="wiki_id", dtype=DataType.INT64),
            FieldSchema(name="views", dtype=DataType.DOUBLE),
            FieldSchema(name="paragraph_id", dtype=DataType.INT64),
            FieldSchema(name="langs", dtype=DataType.INT64),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        fields_name = ["id", "title", "text", "url", "wiki_id", "views", "paragraph_id", "langs", "emb"]
        files = []
        if file_type == "json":
            files = ["train-00000-of-00252.json"]
        if file_type == "npy":
            for field_name in fields_name:
                files.append(f"{field_name}.npy")
        if file_type == "parquet":
            files = ["train-00000-of-00252.parquet"]
        checkers = {
            Op.bulk_insert: BulkInsertChecker(collection_name=c_name, use_one_collection=False, schema=schema,
                                              files=files, insert_data=False)
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_bulk_insert_perf(self):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        self.init_health_checkers()
        cc.start_monitor_threads(self.health_checkers)
        # wait 600s
        while self.health_checkers[Op.bulk_insert].total() <= 10:
            sleep(constants.WAIT_PER_OP)
        assert_statistic(self.health_checkers)

        assert_expectations()
        for k, checker in self.health_checkers.items():
            checker.check_result()
            checker.terminate()

        log.info("*********************Test Completed**********************")
