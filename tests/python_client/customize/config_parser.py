from benedict import benedict
from common.common_func import gen_unique_str
from utils.util_log import test_log as log
from common.cus_resource_opts import CustomResourceOperations as CusResource

template_yaml = 'template/default.yaml'
MILVUS_GRP = 'milvus.io'
MILVUS_VER = 'v1alpha1'
MILVUS_KIND = 'MilvusCluster'


def update_configs(yaml, template):
    if not isinstance(yaml, dict):
        log.error("customize configurations must be in dict type")
        return None

    _configs = benedict.from_yaml(template)

    for key in yaml.keys():
        _configs[key] = yaml[key]

    print(_configs)
    filename = gen_unique_str('cus_config')
    customized_yaml = f'template/{filename}.yaml'

    customized_yaml = _configs.to_yaml(filepath=customized_yaml)

    return _configs.to_json()


def install_milvus(cus_configs, template=template_yaml):

    _configs = update_configs(cus_configs, template)

    # apply custom resource object to deploy milvus
    cus_res = CusResource(kind=MILVUS_KIND,
                          group=MILVUS_GRP,
                          version=MILVUS_VER,
                          namespace=_configs['metadata']['namespace'])
    return cus_res.create(_configs)


def uninstall_milvus(release_name, namespace='default'):

    # delete custom resource object to uninstall milvus
    cus_res = CusResource(kind=MILVUS_KIND,
                          group=MILVUS_GRP,
                          version=MILVUS_VER,
                          namespace=namespace)
    cus_res.delete(release_name)


if __name__ == '__main__':

    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-20211013-91d8f85',
                   'metadata.namespace': 'chaos-testing',
                   'metadata.name': 'milvus-dbl-testop',
                   'spec.components.queryNode.replicas': 2,
                   'spec.components.queryNode.resources.limits.memory': '2048Mi'
                   }
    # milvus_instance = install_milvus(cus_configs, template_yaml)
    # print(milvus_instance)
    # upgrade_milvus(cus_configs)
    uninstall_milvus('milvus-dbl-testop', namespace='chaos-testing')
    # update_configs(cus_configs)
