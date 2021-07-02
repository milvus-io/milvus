import logging
from operator import methodcaller

from kubernetes import client, config
from milvus_benchmark import config as cf


logger = logging.getLogger("milvus_benchmark.chaos.utils")


def list_pod_for_namespace(label_selector="app.kubernetes.io/instance=zong-standalone"):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_namespaced_pod(namespace=cf.NAMESPACE, label_selector=label_selector)
    pods = []
    # label_selector = 'release=zong-single'
    for i in ret.items:
        pods.append(i.metadata.name)
        # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
    return pods


def assert_fail(func, milvus_client, **params):
    try:
        methodcaller(func, **params)(milvus_client)
    except Exception as e:
        logger.debug("11111111111111111111111111")
        logger.info(str(e))
        pass
    else:
        raise Exception("fail-assert failed")


def assert_pass(func, milvus_client, **params):
    try:
        methodcaller(func, **params)(milvus_client)
        logger.debug("&&&&&&&&&&&&&&&&&&&&")
    except Exception as e:
        raise
