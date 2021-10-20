from benedict import benedict
from utils.util_log import test_log as log
from common.cus_resource_opts import CustomResourceOperations as CusResource

template_yaml = 'template/default.yaml'
MILVUS_GRP = 'milvus.io'
MILVUS_VER = 'v1alpha1'
MILVUS_PLURAL = 'milvusclusters'


def update_configs(yaml, template):
    if not isinstance(yaml, dict):
        log.error("customize configurations must be in dict type")
        return None

    d_configs = benedict.from_yaml(template)

    for key in yaml.keys():
        d_configs[key] = yaml[key]

    # return a python dict for common use
    log.info(f"customized configs: {d_configs._dict}")
    return d_configs._dict


def install_milvus(cus_configs, template=template_yaml):

    _configs = update_configs(cus_configs, template)
    # apply custom resource object to deploy milvus
    cus_res = CusResource(kind=MILVUS_PLURAL,
                          group=MILVUS_GRP,
                          version=MILVUS_VER,
                          namespace='chaos-testing')
    print(_configs)
    return cus_res.create(_configs)


def uninstall_milvus(release_name, namespace='default'):

    # delete custom resource object to uninstall milvus
    cus_res = CusResource(kind=MILVUS_PLURAL,
                          group=MILVUS_GRP,
                          version=MILVUS_VER,
                          namespace=namespace)
    cus_res.delete(release_name)


def upgrade_milvus(release_name, configs):
    # TODO: upgrade milvus with new configs
    pass


if __name__ == '__main__':

    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-20211013-91d8f85',
                   'metadata.namespace': 'chaos-testing',
                   'metadata.name': 'milvus-dbl-testop2',
                   'spec.components.queryNode.replicas': 2,
                   'spec.components.queryNode.resources.limits.memory': '2048Mi'
                   }
    milvus_instance = install_milvus(cus_configs, template_yaml)
    print(milvus_instance)
    # upgrade_milvus(cus_configs)
    # uninstall_milvus('milvus-dbl-testop2', namespace='chaos-testing')
    # update_configs(cus_configs)
