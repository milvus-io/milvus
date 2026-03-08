import pytest
import os
import time
import threading
from pathlib import Path
from time import sleep
from minio import Minio
from pymilvus import connections
from chaos.checker import (BulkInsertChecker, Op)
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import get_milvus_deploy_tool, get_pod_ip_name_pairs, get_milvus_instance_name
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


def get_querynode_info(release_name):
    querynode_id_pod_pair = {}
    querynode_ip_pod_pair = get_pod_ip_name_pairs(
        "chaos-testing", f"app.kubernetes.io/instance={release_name}, component=querynode")
    ms = MilvusSys()
    for node in ms.query_nodes:
        ip = node["infos"]['hardware_infos']["ip"].split(":")[0]
        querynode_id_pod_pair[node["identifier"]] = querynode_ip_pod_pair[ip]
    return querynode_id_pod_pair


class TestChaosBase:
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


class TestChaos(TestChaosBase):

    def teardown_method(self):
        sleep(10)
        log.info(f'Alive threads: {threading.enumerate()}')

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, milvus_ns):
        connections.add_connection(default={"host": host, "port": port})
        connections.connect(alias='default')

        if connections.has_connection("default") is False:
            raise Exception("no connections")
        instance_name = get_milvus_instance_name(constants.CHAOS_NAMESPACE, host)
        self.host = host
        self.port = port
        self.instance_name = instance_name
        self.milvus_sys = MilvusSys(alias='default')
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)

    def init_health_checkers(self, collection_name=None, dim=2048):
        log.info("init health checkers")
        c_name = collection_name if collection_name else cf.gen_unique_str("Checker_")
        checkers = {
            Op.bulk_insert: BulkInsertChecker(collection_name=c_name, use_one_collection=False, dim=dim,),
        }
        self.health_checkers = checkers

    def prepare_bulk_insert(self, nb=3000, file_type="json", dim=768, varchar_len=2000, with_varchar_field=True):
        if Op.bulk_insert not in self.health_checkers:
            log.info("bulk_insert checker is not in  health checkers, skip prepare bulk load")
            return
        log.info("bulk_insert checker is in health checkers, prepare data firstly")
        deploy_tool = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)
        if deploy_tool == "helm":
            release_name = self.instance_name
        else:
            release_name = self.instance_name + "-minio"
        minio_ip_pod_pair = get_pod_ip_name_pairs("chaos-testing", f"release={release_name}, app=minio")
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        minio_endpoint = f"{minio_ip}:{minio_port}"
        bucket_name = ms.data_nodes[0]["infos"]["system_configurations"]["minio_bucket_name"]
        schema = cf.gen_bulk_insert_collection_schema(dim=dim, with_varchar_field=with_varchar_field)
        data = cf.gen_default_list_data_for_bulk_insert(nb=nb, varchar_len=varchar_len,
                                                        with_varchar_field=with_varchar_field)
        data_dir = "/tmp/bulk_insert_data"
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        files = []
        if file_type == "json":
            files = cf.gen_json_files_for_bulk_insert(data, schema, data_dir, nb=nb, dim=dim)
        if file_type == "npy":
            files = cf.gen_npy_files_for_bulk_insert(data, schema, data_dir, nb=nb, dim=dim)
        log.info("upload file to minio")
        client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
        for file_name in files:
            file_size = os.path.getsize(os.path.join(data_dir, file_name)) / 1024 / 1024
            t0 = time.time()
            client.fput_object(bucket_name, file_name, os.path.join(data_dir, file_name))
            log.info(f"upload file {file_name} to minio, size: {file_size:.2f} MB, cost {time.time() - t0:.2f} s")
        self.health_checkers[Op.bulk_insert].update(schema=schema, files=files)
        log.info("prepare data for bulk load done")

    @pytest.mark.tags(CaseLabel.L3)
    def test_bulk_insert_perf(self, file_type, nb, dim, varchar_len, with_varchar_field):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        log.info(f"file_type: {file_type}, nb: {nb}, dim: {dim}, varchar_len: {varchar_len}, with_varchar_field: {with_varchar_field}")
        self.init_health_checkers(dim=int(dim))
        nb = int(nb)
        if str(with_varchar_field) in ["true", "True"]:
            with_varchar_field = True
        else:
            with_varchar_field = False
        varchar_len = int(varchar_len)

        self.prepare_bulk_insert(file_type=file_type, nb=nb, dim=int(dim), varchar_len=varchar_len, with_varchar_field=with_varchar_field)
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
