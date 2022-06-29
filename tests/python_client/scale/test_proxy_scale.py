import multiprocessing

import pytest
from pymilvus import MilvusException, connections

from base.collection_wrapper import ApiCollectionWrapper
from customize.milvus_operator import MilvusOperator
from common import common_func as cf
from common.common_type import default_nb
from common.common_type import CaseLabel
from scale import scale_common as sc, constants
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, read_pod_log
from utils.util_pymilvus import get_latest_tag

tag_prefix = '2.1.0'
tag_latest = '2.1.0-latest'


def e2e_milvus_parallel(process_num, host, c_name):
    """ e2e milvus """
    process_list = []
    for i in range(process_num):
        p = multiprocessing.Process(target=sc.e2e_milvus, args=(host, c_name))
        p.start()
        process_list.append(p)

    for p in process_list:
        p.join()


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
        fail_count = 0
        release_name = "scale-proxy"
        image_tag = get_latest_tag(tag_prefix=tag_prefix, tag_latest=tag_latest)
        image = f'{constants.IMAGE_REPOSITORY}:{image_tag}'
        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.proxy.replicas': 1,
            'spec.components.dataNode.replicas': 2,
            'spec.config.common.retentionDuration': 60
        }
        mic = MilvusOperator()
        mic.install(data_config)
        if mic.wait_for_healthy(release_name, constants.NAMESPACE, timeout=1800):
            host = mic.endpoint(release_name, constants.NAMESPACE).split(':')[0]
        else:
            raise MilvusException(message=f'Milvus healthy timeout 1800s')

        try:
            c_name = cf.gen_unique_str("proxy_scale")
            e2e_milvus_parallel(2, host, c_name)
            log.info('Milvus test before expand')

            # expand proxy replicas from 1 to 5
            mic.upgrade(release_name, {'spec.components.proxy.replicas': 5}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            e2e_milvus_parallel(5, host, c_name)
            log.info('Milvus test after expand')

            # expand proxy replicas from 5 to 2
            mic.upgrade(release_name, {'spec.components.proxy.replicas': 2}, constants.NAMESPACE)
            mic.wait_for_healthy(release_name, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            e2e_milvus_parallel(2, host, c_name)
            log.info('Milvus test after shrink')

            connections.connect('default', host=host, port=19530)
            collection_w = ApiCollectionWrapper()
            collection_w.init_collection(name=c_name)
            """
            total start 2+5+2 process to run e2e, each time insert default_nb data, But one of the 2 processes started
            for the first time did not insert due to collection creation exception. So actually insert eight times
            """
            assert collection_w.num_entities == 8 * default_nb

        except Exception as e:
            log.error(str(e))
            fail_count += 1
            # raise Exception(str(e))

        finally:
            log.info(f'Test finished with {fail_count} fail request')
            assert fail_count <= 1
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            read_pod_log(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)
