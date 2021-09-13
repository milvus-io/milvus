import os
from time import sleep

from scale import constants
from utils.util_log import test_log as log
from common import common_func as cf
from scale import scale_common as sc


class HelmEnv:
    milvus_chart_path = sc.get_milvus_chart_env_var()

    def __init__(self, release_name=None, **kwargs):
        self.release_name = release_name if release_name else cf.gen_unique_str("scale")
        self.proxy = kwargs.get(constants.PROXY, 1)
        self.data_node = kwargs.get(constants.DATA_NODE, 1)
        self.index_node = kwargs.get(constants.INDEX_NODE, 1)
        self.query_node = kwargs.get(constants.QUERY_NODE, 1)

    def helm_install_cluster_milvus(self, image_pull_policy=constants.IF_NOT_PRESENT):
        """
        default deploy cluster milvus with only one xxxNode
        helm install --wait --timeout 180s --set image.all.repository=milvusdb/milvus-dev --set image.all.tag=master-latest
                                           --set cluster.enabled=true --set service.type=LoadBalancer
                                           --set image.all=Always clu-zong .
        :param image_pull_policy: image pullPolicy includes: IF_NOT_PRESENT and ALWAYS
        :param kwargs: PROXY, DATA_NODE, INDEX_NODE, QUERY_NODE
        :return: svc ip
        """
        install_cmd = f'helm install --wait --timeout 360s ' \
                      f'--set image.all.repository={constants.IMAGE_REPOSITORY} ' \
                      f'--set image.all.tag={constants.IMAGE_TAG} ' \
                      f'--set cluster.enabled=true ' \
                      f'--set service.type=LoadBalancer ' \
                      f'--set image.all.pullPolicy={image_pull_policy} ' \
                      f'--set proxy.replicas={self.proxy} ' \
                      f'--set dataNode.replicas={self.data_node} ' \
                      f'--set indexNode.replicas={self.index_node} ' \
                      f'--set queryNode.replicas={self.query_node} ' \
                      f'{self.release_name} . '
        log.debug(f'install_cmd: {install_cmd}')
        try:
            os.system(f'cd {self.milvus_chart_path} && {install_cmd}')
        except Exception:
            raise Exception("Failed to deploy cluster milvus")
        return self.get_service_ip()

    def helm_upgrade_cluster_milvus(self, **kwargs):
        """
        scale milvus pod num by helm upgrade
        when upgrading pod nums, other --set need to be the same as helm install
        :param kwargs: PROXY, DATA_NODE, INDEX_NODE, QUERY_NODE
        :return: None
        """
        proxy = kwargs.get(constants.PROXY, self.proxy)
        data_node = kwargs.get(constants.DATA_NODE, self.data_node)
        index_node = kwargs.get(constants.INDEX_NODE, self.index_node)
        query_node = kwargs.get(constants.QUERY_NODE, self.query_node)
        upgrade_cmd = f'helm upgrade {self.release_name} . ' \
                      f'--set proxy.replicas={proxy} ' \
                      f'--set dataNode.replicas={data_node} ' \
                      f'--set indexNode.replicas={index_node} ' \
                      f'--set queryNode.replicas={query_node} ' \
                      f'--reuse-values'
        log.debug(f'upgrade_cmd: {upgrade_cmd}')
        if os.system(f'cd {self.milvus_chart_path} && {upgrade_cmd}'):
            raise Exception(f'Failed to upgrade cluster milvus with {kwargs}')

    def helm_uninstall_cluster_milvus(self):
        """
        helm uninstall and delete etcd pvc
        :return: None
        """
        uninstall_cmd = f'helm uninstall {self.release_name}'
        if os.system(uninstall_cmd):
            raise Exception(f'Failed to uninstall {self.release_name}')
        # delete etcd pvc
        delete_pvc_cmd = f'kubectl delete pvc data-{self.release_name}-etcd-0'
        if os.system(delete_pvc_cmd):
            raise Exception(f'Failed to delete {self.release_name} etcd pvc')
        # delete plusar
        # delete_pvc_plusar_cmd = "kubectl delete pvc scale-test-milvus-pulsar"

    def get_service_ip(self):
        from kubernetes import client, config
        # from kubernetes.client.rest import ApiException
        config.load_kube_config()
        v1 = client.CoreV1Api()
        service = v1.read_namespaced_service(f'{self.release_name}-milvus', constants.NAMESPACE)
        return service.status.load_balancer.ingress[0].ip

    def export_all_logs(self):
        """
        export all cluster logs to /tmp/milvus, and temporarily missing minio pod logs
        :return: export all pods' log to constants.MILVUS_LOGS_PATH
        """
        pods = self.list_all_pods()
        for pod in pods:
            os.system(f'kubectl logs {pod} > {constants.MILVUS_LOGS_PATH}/{pod}.log 2>&1')

    def list_all_pods(self):
        from kubernetes import client, config
        config.load_kube_config()
        v1 = client.CoreV1Api()
        label_selector = f'app.kubernetes.io/instance={self.release_name}'
        ret = v1.list_namespaced_pod(namespace=constants.NAMESPACE, label_selector=label_selector)
        pods = []
        # # label_selector = 'release=zong-single'
        for i in ret.items:
            pods.append(i.metadata.name)
        #     # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
        return pods


if __name__ == '__main__':
    # default deploy q replicas
    release_name = "scale-proxy"
    env = HelmEnv(release_name=release_name)
    # env.helm_install_cluster_milvus()
    # host = env.get_service_ip()
    # env.helm_upgrade_cluster_milvus(queryNode=2)
    # env.helm_uninstall_cluster_milvus()
    env.export_all_logs()
    # sleep(5)
