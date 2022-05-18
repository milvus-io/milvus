
import threading

import pytest
import os
import time
import json
from time import sleep
from pathlib import Path
from minio import Minio
from pymilvus import connections
from chaos.checker import (InsertFlushChecker, SearchChecker, QueryChecker, BulkLoadChecker, Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_pod_list, get_pod_ip_name_pairs, get_milvus_instance_name
from utils.util_common import findkeys, update_key_value
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants
# from bulk_load.bulk_load_data import gen_file_name
from bulk_load.minio_comm import copy_files_to_minio
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
	def connection(self, host, port):
		connections.add_connection(default={"host": host, "port": port})
		connections.connect(alias='default')

		if connections.has_connection("default") is False:
			raise Exception("no connections")
		instance_name = get_milvus_instance_name(constants.CHAOS_NAMESPACE, host)
		self.host = host
		self.port = port
		self.instance_name = instance_name

	@pytest.fixture(scope="function", autouse=True)
	def init_health_checkers(self):
		log.info("init health checkers")
		checkers = {
			# Op.insert: InsertFlushChecker(collection_name=c_name),
			# Op.search: SearchChecker(collection_name=c_name, replica_number=2),
			Op.bulk_load: BulkLoadChecker()
			# Op.query: QueryChecker(collection_name=c_name, replica_number=2)
		}
		self.health_checkers = checkers

	@pytest.fixture(scope="function", autouse=True)
	def prepare_bulk_load(self, nb=1000, row_based=True):
		if Op.bulk_load not in self.health_checkers:
			log.info("bulk_load checker is not in  health checkers, skip prepare bulk load")
			return
		log.info("bulk_load checker is in  health checkers, prepare data firstly")
		release_name = self.instance_name
		minio_ip_pod_pair = get_pod_ip_name_pairs("chaos-testing", f"release={release_name}, app=minio")
		ms = MilvusSys()
		minio_ip = list(minio_ip_pod_pair.keys())[0]
		minio_port = "9000"
		minio_endpoint = f"{minio_ip}:{minio_port}"
		bucket_name = ms.index_nodes[0]["infos"]["system_configurations"]["minio_bucket_name"]
		schema = cf.gen_default_collection_schema()
		data = cf.gen_default_list_data_for_bulk_load(nb=nb)
		fields_name = [field.name for field in schema.fields]
		if not row_based:
			data_dict = dict(zip(fields_name, data))
		if row_based:
			entities = []
			for i in range(nb):
				entity_value = [field_values[i] for field_values in data]
				entity = dict(zip(fields_name, entity_value))
				entities.append(entity)
			data_dict = {"rows": entities}
		file_name = "bulk_load_data_source.json"
		files = [file_name]
		#TODO: npy file type is not supported so far
		log.info("generate bulk load file")
		with open(file_name, "w") as f:
			f.write(json.dumps(data_dict))
		log.info("upload file to minio")
		client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
		client.fput_object(bucket_name, file_name, file_name)
		self.health_checkers[Op.bulk_load].update(schema=schema, files=files, row_based=row_based)
		log.info("prepare data for bulk load done")

	def teardown(self):
		chaos_res = CusResource(kind=self._chaos_config['kind'],
								group=constants.CHAOS_GROUP,
								version=constants.CHAOS_VERSION,
								namespace=constants.CHAOS_NAMESPACE)
		meta_name = self._chaos_config.get('metadata', None).get('name', None)
		chaos_res.delete(meta_name, raise_ex=False)
		sleep(2)
		log.info(f'Alive threads: {threading.enumerate()}')

	@pytest.mark.tags(CaseLabel.L3)
	@pytest.mark.parametrize("target_component", ["minio"]) # "minio", "proxy", "rootcoord", "datacoord", "datanode", "etcd"
	@pytest.mark.parametrize("chaos_type", ["pod_kill"]) # "pod_kill", "pod_failure"
	def test_bulk_load(self, chaos_type, target_component):
		# start the monitor threads to check the milvus ops
		log.info("*********************Chaos Test Start**********************")
		log.info(connections.get_connection_addr('default'))
		release_name = self.instance_name
		cc.start_monitor_threads(self.health_checkers)
		chaos_config = cc.gen_experiment_config(f"{str(Path(__file__).absolute().parent)}/chaos_objects/{chaos_type}/chaos_{target_component}_{chaos_type}.yaml")
		chaos_config['metadata']['name'] = f"test-bulk-load-{int(time.time())}"
		kind = chaos_config['kind']
		meta_name = chaos_config.get('metadata', None).get('name', None)
		update_key_value(chaos_config, "release", release_name)
		update_key_value(chaos_config, "app.kubernetes.io/instance", release_name)
		self._chaos_config = chaos_config  # cache the chaos config for tear down
		log.info(f"chaos_config: {chaos_config}")
		# wait 20s
		sleep(constants.WAIT_PER_OP * 10)
		# assert statistic:all ops 100% succ
		log.info("******1st assert before chaos: ")
		assert_statistic(self.health_checkers)
		# apply chaos object
		chaos_res = CusResource(kind=chaos_config['kind'],
								group=constants.CHAOS_GROUP,
								version=constants.CHAOS_VERSION,
								namespace=constants.CHAOS_NAMESPACE)
		chaos_res.create(chaos_config)
		log.info("chaos injected")
		sleep(constants.WAIT_PER_OP * 10)
		# reset counting
		cc.reset_counting(self.health_checkers)
		# wait 120s
		sleep(constants.CHAOS_DURATION)
		log.info(f'Alive threads: {threading.enumerate()}')
		# assert statistic
		log.info("******2nd assert after chaos injected: ")
		assert_statistic(self.health_checkers,
						 expectations={
							 Op.bulk_load: constants.FAIL,
						 })
		# delete chaos
		chaos_res.delete(meta_name)
		log.info("chaos deleted")
		sleep(2)
		# wait all pods ready
		log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={release_name}")
		wait_pods_ready(constants.CHAOS_NAMESPACE ,f"app.kubernetes.io/instance={release_name}")
		log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={release_name}")
		wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={release_name}")
		log.info("all pods are ready")
		# reconnect if needed
		sleep(constants.WAIT_PER_OP * 2)
		cc.reconnect(connections, alias='default')
		# recheck failed tasks in third assert
		self.health_checkers[Op.bulk_load].recheck_failed_task = True
		# reset counting again
		cc.reset_counting(self.health_checkers)
		# wait 50s (varies by feature)
		sleep(constants.WAIT_PER_OP * 10)
		# assert statistic: all ops success again
		log.info("******3rd assert after chaos deleted: ")
		assert_statistic(self.health_checkers)
		# assert all expectations
		assert_expectations()

		log.info("*********************Chaos Test Completed**********************")
