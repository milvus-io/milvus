import pytest

from scale.helm_env import HelmEnv
from common import common_func as cf
from common.common_type import CaseLabel
from scale import scale_common as sc

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
        env = HelmEnv(release_name=release_name)
        host = env.helm_install_cluster_milvus()

        c_name = cf.gen_unique_str(prefix)
        sc.e2e_milvus(host, c_name)

        # scale proxy
        env.helm_upgrade_cluster_milvus(proxy=2)

        # c_name_2 = cf.gen_unique_str(prefix)
        sc.e2e_milvus(host, c_name, collection_exist=True)

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
