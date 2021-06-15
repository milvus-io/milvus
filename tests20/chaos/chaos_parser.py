import logging
import os
from yaml import full_load
from milvus_benchmark.chaos import utils

logger = logging.getLogger("milvus_benchmark.chaos.base")


class BaseChaos(object):
    cur_path = os.path.abspath(os.path.dirname(__file__))

    def __init__(self, api_version, kind, metadata, spec):
        self.api_version = api_version
        self.kind = kind
        self.metadata = metadata
        self.spec = spec

    def gen_experiment_config(self):
        pass
        """
        1. load dict from default yaml
        2. merge dict between dict and self.x
        """

    def check_config(self):
        if not self.kind:
            raise Exception("kind is must be specified")
        if not self.spec:
            raise Exception("spec is must be specified")
        if "action" not in self.spec:
            raise Exception("action is must be specified in spec")
        if "selector" not in self.spec:
            raise Exception("selector is must be specified in spec")
        return True

    def replace_label_selector(self):
        self.check_config()
        label_selectors_dict = self.spec["selector"]["labelSelectors"]
        label_selector = next(iter(label_selectors_dict.items()))
        label_selector_value = label_selector[1]
        # pods = utils.list_pod_for_namespace(label_selector[0] + "=" + label_selector_value)
        pods = utils.list_pod_for_namespace()
        real_label_selector_value = list(map(lambda pod: pod, filter(lambda pod: label_selector_value in pod, pods)))[0]
        self.spec["selector"]["labelSelectors"].update({label_selector[0]: real_label_selector_value})


class PodChaos(BaseChaos):
    default_yaml = BaseChaos.cur_path + '/template/PodChaos.yaml'

    def __init__(self, api_version, kind, metadata, spec):
        super(PodChaos, self).__init__(api_version, kind, metadata, spec)

    def gen_experiment_config(self):
        with open(self.default_yaml) as f:
            default_config = full_load(f)
            f.close()
        self.replace_label_selector()
        experiment_config = default_config
        experiment_config.update({"apiVersion": self.api_version})
        experiment_config.update({"kind": self.kind})
        experiment_config["metadata"].update(self.metadata)
        experiment_config["spec"].update(self.spec)
        return experiment_config


class NetworkChaos(BaseChaos):
    def __init__(self, api_version, kind, metadata, spec):
        super(NetworkChaos, self).__init__(api_version, kind, metadata, spec)

    def gen_experiment_config(self):
        pass