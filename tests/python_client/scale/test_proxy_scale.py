import multiprocessing
import time

import pytest
from pymilvus import connections

from base.collection_wrapper import ApiCollectionWrapper
from customize.milvus_operator import MilvusOperator
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from scale import constants
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag

prefix = "proxy_scale"


class TestProxyScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_scale_proxy(self):
        """
        target: test milvus operation after proxy expand
        method: 1.deploy 1 proxy replicas
                2.milvus e2e test in parallel
                3.expand proxy pod from 1 to 5
                4.milvus e2e test
                5.shrink proxy from 5 to 2
        expected: 1.verify data consistent and func work
        """
        # deploy milvus cluster with one proxy
        release_name = "scale-proxy"
        image_tag = get_latest_tag()
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        init_replicas = 1
        scale_out_replicas = 5
        scale_in_replicas = 2
        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.proxy.replicas': init_replicas,
            'spec.components.dataNode.replicas': 2,
            'spec.config.dataCoord.enableCompaction': True,
            'spec.config.dataCoord.enableGarbageCollection': True
        }
        mic = MilvusOperator()
        mic.install(data_config)
        healthy = mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1200)
        log.info(f"milvus healthy: {healthy}")
        host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        # host = "10.98.0.7"

        try:
            c_name = cf.gen_unique_str(prefix)
            connections.connect("default", host=host, port="19530")

            # create
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name, schema=cf.gen_default_collection_schema())

            # insert
            df = cf.gen_default_dataframe_data()
            mutation_res, _ = collection_w.insert(df)
            assert mutation_res.insert_count == ct.default_nb
            log.debug(collection_w.num_entities)

            # create index
            collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
            assert collection_w.has_index()[0]
            collection_w.load()

            def do_insert():
                for i in range(100):
                    df = cf.gen_default_dataframe_data()
                    collection_w.insert(df)
                    log.debug(f'After {i+1} insert num entities is {collection_w.num_entities}')

            def do_search():
                # search
                while True:
                    time.sleep(5)
                    search_res, _ = collection_w.search(cf.gen_vectors(1, dim=ct.default_dim),
                                                        ct.default_float_vec_field_name,
                                                        ct.default_search_params, ct.default_limit)
                    assert len(search_res[0]) == ct.default_limit
                    log.debug(search_res[0].ids)

            t_insert = multiprocessing.Process(target=do_insert, args=())
            t_search = multiprocessing.Process(target=do_search, args=(), daemon=True)
            t_insert.start()
            t_search.start()

            # expand proxy replicas from 1 to 5
            log.info(f'Scale out proxy replicas from {init_replicas} to {scale_out_replicas}')
            mic.upgrade(release_name, {'spec.components.proxy.replicas': scale_out_replicas}, constants.NAMESPACE)
            time.sleep(5)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            time.sleep(60)

            # shrink proxy replicas from 5 to 2
            log.info(f'Scale in proxy replicas from {scale_out_replicas} to {scale_in_replicas}')
            mic.upgrade(release_name, {'spec.components.proxy.replicas': scale_in_replicas}, constants.NAMESPACE)
            time.sleep(5)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            t_insert.join()

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)