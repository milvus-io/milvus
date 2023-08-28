import time
from pathlib import Path
import subprocess
import pytest
from time import sleep

from yaml import full_load
from pymilvus import connections, utility
from chaos.checker import (CreateChecker,
                           InsertChecker,
                           FlushChecker,
                           SearchChecker,
                           QueryChecker,
                           IndexChecker,
                           DeleteChecker,
                           DropChecker,
                           Op)
from utils.util_k8s import wait_pods_ready
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
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
    def connection(self, host, port, user, password, minio_host):
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
        self.minio_endpoint = f"{minio_host}:9000"

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        schema = cf.gen_default_collection_schema(auto_id=False)

        checkers = {
            Op.create: CreateChecker(collection_name=None, schema=schema),
            Op.insert: InsertChecker(collection_name=c_name, schema=schema),
            Op.flush: FlushChecker(collection_name=c_name, schema=schema),
            Op.index: IndexChecker(collection_name=None, schema=schema),
            Op.search: SearchChecker(collection_name=c_name, schema=schema),
            Op.query: QueryChecker(collection_name=c_name, schema=schema),
            Op.delete: DeleteChecker(collection_name=c_name, schema=schema),
            Op.drop: DropChecker(collection_name=None, schema=schema)
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, request_duration, is_check):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        c_name = None
        self.init_health_checkers(collection_name=c_name)
        # prepare data by bulk insert
        log.info("*********************Prepare Data by bulk insert**********************")
        for k, v in self.health_checkers.items():
            if k in [Op.search, Op.query]:
                log.info(f"prepare bulk insert data for {k}")
                v.prepare_bulk_insert_data(minio_endpoint=self.minio_endpoint)
                completed = False
                retry_times = 0
                while not completed and retry_times < 3:
                    completed, result = v.do_bulk_insert()
                    if not completed:
                        log.info(f"do bulk insert failed: {result}")
                        retry_times += 1
                        sleep(5)
                # wait for index building complete
                utility.wait_for_index_building_complete(v.c_name, timeout=120)
                res = utility.index_building_progress(v.c_name)
                index_completed = res["pending_index_rows"] == 0
                while not index_completed:
                    time.sleep(10)
                    res = utility.index_building_progress(v.c_name)
                    log.info(f"index building progress: {res}")
                    index_completed = res["pending_index_rows"] == 0
                log.info(f"index building progress: {res}")

        log.info("*********************Load Start**********************")
        cc.start_monitor_threads(self.health_checkers)

        # wait request_duration
        request_duration = request_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        for i in range(10):
            sleep(request_duration // 10)
            if i == 3:
                # apply rolling update after 30% time of request_duration
                log.info("*********************Apply Rolling Update**********************")
                file_path = f"{str(Path(__file__).parent.parent.parent)}/deploy/milvus_crd.yaml"
                cmd = f"kubectl apply -f {file_path}"
                log.info(f"cmd: {cmd}")
                res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = res.communicate()
                log.info(f"{cmd}, stdout: {stdout}, stderr: {stderr}")
                # reset all
                for k, v in self.health_checkers.items():
                    v.reset()
            for k, v in self.health_checkers.items():
                v.check_result()
        for k, v in self.health_checkers.items():
            v.pause()
        for k, v in self.health_checkers.items():
            v.check_result()
        for k, v in self.health_checkers.items():  
            log.info(f"{k} failed request: {v.fail_records}")
        for k, v in self.health_checkers.items():  
            log.info(f"{k} rto: {v.get_rto()}")
        if is_check:
            assert_statistic(self.health_checkers, succ_rate_threshold=0.98)
            # get each checker's rto
            for k, v in self.health_checkers.items():
                log.info(f"{k} rto: {v.get_rto()}")
                rto = v.get_rto()
                pytest.assume(rto < 30,  f"{k} rto expect 30s but get {rto}s")  # rto should be less than 30s

            if Op.insert in self.health_checkers:
                # verify the no insert data loss
                log.info("*********************Verify Data Completeness**********************")
                self.health_checkers[Op.insert].verify_data_completeness()

        #
        for k, v in self.health_checkers.items():
            v.reset()
        # wait all pod running
        file_path = f"{str(Path(__file__).parent.parent.parent)}/deploy/milvus_crd.yaml"
        with open(file_path, "r") as f:
            config = full_load(f)
        meta_name = config["metadata"]["name"]
        label_selector = f"app.kubernetes.io/instance={meta_name}"
        is_ready = wait_pods_ready("chaos-testing", label_selector)
        pytest.assume(is_ready is True, f"expect all pods ready but got {is_ready}")
        cc.start_monitor_threads(self.health_checkers)
        sleep(60)
        log.info("check succ rate after rolling update finished")
        for k, v in self.health_checkers.items():
            v.check_result()
        for k, v in self.health_checkers.items():
            log.info(f"{k} failed request: {v.fail_records}")
        for k, v in self.health_checkers.items():
            log.info(f"{k} rto: {v.get_rto()}")
        assert_statistic(self.health_checkers, succ_rate_threshold=1.0)
        log.info("*********************Test Completed**********************")
