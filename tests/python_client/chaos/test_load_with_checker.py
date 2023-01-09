import threading
import pytest
import json
from time import sleep
from minio import Minio
from pymilvus import connections
from chaos.checker import (CreateChecker,
                           InsertChecker,
                           FlushChecker,
                           SearchChecker,
                           QueryChecker,
                           IndexChecker,
                           DeleteChecker,
                           CompactChecker,
                           DropChecker,
                           LoadBalanceChecker,
                           BulkInsertChecker,
                           Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import get_pod_ip_name_pairs, get_milvus_instance_name
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants


class TestChaosBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    expect_delete = constants.SUCC
    host = '127.0.0.1'
    port = 19530
    _chaos_config = None
    health_checkers = {}


class TestChaos(TestChaosBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port):
        connections.connect("default", host=host, port=port)

        if connections.has_connection("default") is False:
            raise Exception("no connections")
        self.host = host
        self.port = port
        self.instance_name = get_milvus_instance_name(constants.CHAOS_NAMESPACE, host)

    @pytest.fixture(scope="function", autouse=True)
    def init_health_checkers(self):
        c_name = cf.gen_unique_str("Checker_")
        checkers = {
            # Op.create: CreateChecker(collection_name=c_name),
            # Op.insert: InsertChecker(collection_name=c_name),
            # Op.flush: FlushChecker(collection_name=c_name),
            # Op.query: QueryChecker(collection_name=c_name),
            # Op.search: SearchChecker(collection_name=c_name),
            # Op.delete: DeleteChecker(collection_name=c_name),
            # Op.compact: CompactChecker(collection_name=c_name),
            # Op.index: IndexChecker(),
            # Op.drop: DropChecker(),
            # Op.bulk_insert: BulkInsertChecker(),
            Op.load_balance: LoadBalanceChecker()
        }
        self.health_checkers = checkers
        ms = MilvusSys()
        self.prepare_bulk_insert()

    def prepare_bulk_insert(self, nb=30000, row_based=True):
        if Op.bulk_insert not in self.health_checkers:
            log.info("bulk_insert checker is not in  health checkers, skip prepare bulk insert")
            return
        log.info("bulk_insert checker is in  health checkers, prepare data firstly")
        release_name = self.instance_name
        minio_ip_pod_pair = get_pod_ip_name_pairs("chaos-testing", f"release={release_name}, app=minio")
        ms = MilvusSys()
        minio_ip = list(minio_ip_pod_pair.keys())[0]
        minio_port = "9000"
        minio_endpoint = f"{minio_ip}:{minio_port}"
        bucket_name = ms.index_nodes[0]["infos"]["system_configurations"]["minio_bucket_name"]
        schema = cf.gen_default_collection_schema()
        data = cf.gen_default_list_data_for_bulk_insert(nb=nb)
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
        file_name = "bulk_insert_data_source.json"
        files = [file_name]
        #TODO: npy file type is not supported so far
        log.info("generate bulk insert file")
        with open(file_name, "w") as f:
            f.write(json.dumps(data_dict))
        log.info("upload file to minio")
        client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
        client.fput_object(bucket_name, file_name, file_name)
        self.health_checkers[Op.bulk_insert].update(schema=schema, files=files, row_based=row_based)
        log.info("prepare data for bulk insert done")

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
    def test_load_generator(self):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        cc.start_monitor_threads(self.health_checkers)

        log.info("start checkers")
        sleep(30)
        for k, v in self.health_checkers.items():
            v.check_result()
        sleep(600)
