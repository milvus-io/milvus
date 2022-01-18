import multiprocessing

import pytest
from customize.milvus_operator import MilvusOperator
from common import common_func as cf
from common.common_type import CaseLabel
from scale import scale_common as sc, constants
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, export_pod_logs
from utils.util_pymilvus import get_latest_tag

prefix = "proxy_scale"


class TestProxyScale:

    def e2e_milvus_parallel(self, process_num, host, c_name):
        process_list = []
        for i in range(process_num):
            p = multiprocessing.Process(target=sc.e2e_milvus, args=(host, c_name))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()

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
        data_config = {
            'metadata.namespace': constants.NAMESPACE,
            'metadata.name': release_name,
            'spec.components.image': image,
            'spec.components.proxy.serviceType': 'LoadBalancer',
            'spec.components.proxy.replicas': 1,
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
            self.e2e_milvus_parallel(5, host, c_name)
            log.info('Milvus test before expand')

            # expand proxy replicas from 1 to 5
            mic.upgrade(release_name, {'spec.components.proxy.replicas': 5}, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            self.e2e_milvus_parallel(5, host, c_name)
            log.info('Milvus test after expand')

            # expand proxy replicas from 5 to 2
            mic.upgrade(release_name, {'spec.components.proxy.replicas': 2}, constants.NAMESPACE)
            wait_pods_ready(constants.NAMESPACE, f"app.kubernetes.io/instance={release_name}")

            self.e2e_milvus_parallel(2, host, c_name)
            log.info('Milvus test after shrink')

        except Exception as e:
            raise Exception(str(e))

        finally:
            label = f"app.kubernetes.io/instance={release_name}"
            log.info('Start to export milvus pod logs')
            export_pod_logs(namespace=constants.NAMESPACE, label_selector=label, release_name=release_name)
            mic.uninstall(release_name, namespace=constants.NAMESPACE)
