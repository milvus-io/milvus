from benedict import benedict
from common.common_func import gen_unique_str
from utils.util_log import test_log as log


def update_configs(yaml):
    if not isinstance(yaml, dict):
        log.error("customize configurations must be in dict type")
        return None

    template_yaml = 'template/default.yaml'
    _configs = benedict.from_yaml(template_yaml)

    for key in yaml.keys():
        _configs[key] = yaml[key]

    filename = gen_unique_str('cus_config')
    customized_yaml = f'template/{filename}.yaml'

    customized_yaml = _configs.to_yaml(filepath=customized_yaml)
    return customized_yaml


if __name__ == '__main__':

    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-latest',
                   'metadata.namespace': 'qa-milvus',
                   'spec.components.dataNode.replicas': 2,
                   'spec.components.queryNode.replicas': 2,
                   'spec.components.queryNode.resources.limits.memory': '2048Mi'
                   }
    update_configs(cus_configs)
