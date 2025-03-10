import threading

import pytest
import os
import time
import json
from time import sleep
from pathlib import Path
from minio import Minio
from pymilvus import connections
from chaos.checker import (InsertChecker, SearchChecker, QueryChecker, BulkInsertChecker, Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_milvus_deploy_tool, get_pod_ip_name_pairs, get_milvus_instance_name
from utils.util_common import update_key_value
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

    @pytest.fixture(scope="function", autouse=True)
    def init_health_checkers(self, collection_name=None):
        log.info("init health checkers")
        c_name = collection_name if collection_name else cf.gen_unique_str("Checker_")
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.bulk_insert: BulkInsertChecker(collection_name=c_name, use_one_collection=True),
            Op.query: QueryChecker(collection_name=c_name)
        }
        self.health_checkers = checkers

    @pytest.fixture(scope="function", autouse=True)
    def prepare_bulk_insert(self, nb=3000):
        if Op.bulk_insert not in self.health_checkers:
            log.info("bulk_insert checker is not in  health checkers, skip prepare bulk load")
            return
        log.info("bulk_insert checker is in  health checkers, prepare data firstly")
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
        schema = cf.gen_default_collection_schema()
        data = cf.gen_default_list_data_for_bulk_insert(nb=nb)
        fields_name = [field.name for field in schema.fields]
        entities = []
        for i in range(nb):
            entity_value = [field_values[i] for field_values in data]
            entity = dict(zip(fields_name, entity_value))
            entities.append(entity)
        data_dict = {"rows": entities}
        data_source = "/tmp/ci_logs/bulk_insert_data_source.json"
        file_name = "bulk_insert_data_source.json"
        files = ["bulk_insert_data_source.json"]
        # TODO: npy file type is not supported so far
        log.info("generate bulk load file")
        with open(data_source, "w") as f:
            f.write(json.dumps(data_dict, indent=4))
        log.info("upload file to minio")
        client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
        client.fput_object(bucket_name, file_name, data_source)
        self.health_checkers[Op.bulk_insert].update(schema=schema, files=files)
        log.info("prepare data for bulk load done")

    @pytest.mark.tags(CaseLabel.L3)
    def test_bulk_insert(self):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        # c_name = cf.gen_unique_str("BulkInsertChecker_")
        # self.init_health_checkers(collection_name=c_name)
        cc.start_monitor_threads(self.health_checkers)
        # wait 120s
        sleep(constants.WAIT_PER_OP * 12)
        assert_statistic(self.health_checkers)

        assert_expectations()

        log.info("*********************Test Completed**********************")
