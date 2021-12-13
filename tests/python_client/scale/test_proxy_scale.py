import multiprocessing
import pytest
from customize.milvus_operator import MilvusOperator
from scale.helm_env import HelmEnv
from common import common_func as cf
from common.common_type import CaseLabel
from scale import scale_common as sc, constants
from utils.util_log import test_log as log

prefix = "proxy_scale"


class TestProxyScale:

    @pytest.mark.tags(CaseLabel.L3)
    def test_expand_proxy(self):
        """
        target: test milvus operation after proxy expand
        method: 1.deploy two proxy pods
                2.milvus e2e test
                3.expand proxy pod from 1 to 2
                4.milvus e2e test
        expected: 1.verify data consistent and func work
        """
        # deploy all nodes one pod cluster milvus with helm
        release_name = "scale-proxy"
        image = f'{constants.IMAGE_REPOSITORY}:{constants.IMAGE_TAG}'
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

        c_name = cf.gen_unique_str(prefix)
        process_list = []
        for i in range(5):
            p = multiprocessing.Process(target=sc.e2e_milvus, args=(host, c_name))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()

        log.info('Milvus test before scale up')

        mic.upgrade(release_name, {'spec.components.proxy.replicas': 5}, constants.NAMESPACE)

        for i in range(5):
            p = multiprocessing.Process(target=sc.e2e_milvus, args=(host, c_name))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()

        log.info('Milvus test after scale up')

    def test_shrink_proxy(self):
        """
        target: test shrink proxy pod from 2 to 1
        method: 1.deploy two proxy node
                2.e2e test
                3.shrink proxy pods
                4.e2e test
        expected:
        """
        # deploy all nodes one pod cluster milvus with helm
        release_name = "scale-proxy"
        env = HelmEnv(release_name=release_name, proxy=2)
        host = env.helm_install_cluster_milvus()

        c_name = cf.gen_unique_str(prefix)
        sc.e2e_milvus(host, c_name)

        # scale proxy
        env.helm_upgrade_cluster_milvus(proxy=1)

        # c_name_2 = cf.gen_unique_str(prefix)
        sc.e2e_milvus(host, c_name, collection_exist=True)
