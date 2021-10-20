from benedict import benedict
from utils.util_log import test_log as log
from common.cus_resource_opts import CustomResourceOperations as CusResource
import time

template_yaml = 'template/default.yaml'
MILVUS_GRP = 'milvus.io'
MILVUS_VER = 'v1alpha1'
MILVUS_PLURAL = 'milvusclusters'


def update_configs(configs, template=template_yaml, namespace='default'):
    if not isinstance(configs, dict):
        log.error("customize configurations must be in dict type")
        return None

    d_configs = benedict.from_yaml(template)

    for key in configs.keys():
        d_configs[key] = configs[key]
    # Overwrite namespace with input param
    d_configs['metadata.namespace'] = namespace

    # return a python dict for common use
    log.info(f"customized configs: {d_configs._dict}")
    return d_configs._dict


def install_milvus(configs, template, namespace='default'):

    new_configs = update_configs(configs, template, namespace)
    # apply custom resource object to deploy milvus
    cus_res = CusResource(kind=MILVUS_PLURAL, group=MILVUS_GRP,
                          version=MILVUS_VER, namespace=namespace)
    return cus_res.create(new_configs)


def uninstall_milvus(release_name, namespace='default'):

    # delete custom resource object to uninstall milvus
    cus_res = CusResource(kind=MILVUS_PLURAL, group=MILVUS_GRP,
                          version=MILVUS_VER, namespace=namespace)
    cus_res.delete(release_name)


def upgrade_milvus(release_name, configs, namespace='default'):
    if not isinstance(configs, dict):
        log.error("customize configurations must be in dict type")
        return None

    d_configs = benedict()

    for key in configs.keys():
        d_configs[key] = configs[key]

    cus_res = CusResource(kind=MILVUS_PLURAL, group=MILVUS_GRP,
                          version=MILVUS_VER, namespace=namespace)
    log.debug(f"upgrade milvus with configs: {d_configs}")
    cus_res.patch(release_name, d_configs)


def wait_for_milvus_healthy(release_name, namespace='default', timeout=600):

    cus_res = CusResource(kind=MILVUS_PLURAL, group=MILVUS_GRP,
                          version=MILVUS_VER, namespace=namespace)
    starttime = time.time()
    log.info(f"start to check healthy: {starttime}")
    while time.time() < starttime + timeout:
        time.sleep(10)
        res_object = cus_res.get(release_name)
        if res_object.get('status', None) is not None:
            if 'Healthy' == res_object['status']['status']:
                log.info(f"milvus healthy in {time.time()-starttime} seconds")
                return True
    log.info(f"end to check healthy until timeout {timeout}")
    return False


if __name__ == '__main__':

    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-20211020-4d7252739',
                   'metadata.namespace': 'def',
                   'metadata.name': 'milvus-2739',
                   'spec.components.queryNode.replicas': 2,
                   'spec.components.queryNode.resources.limits.memory': '2048Mi'
                   }
    namespace = 'chaos-testing'
    name = 'milvus-2739'

    milvus_instance = install_milvus(cus_configs, template_yaml, namespace=namespace)
    result = wait_for_milvus_healthy(name, namespace=namespace)
    log.info(f"install milvus healthy: {result}")
    n_configs = {'spec.components.queryNode.replicas': 1,
                 'spec.components.dataNode.resources.limits.memory': '2048Mi'
                 }
    upgrade_milvus(name, n_configs, namespace=namespace)
    result = wait_for_milvus_healthy(name, namespace=namespace)
    log.info(f"upgrade milvus healthy: {result}")

    # uninstall_milvus(name, namespace=namespace)

