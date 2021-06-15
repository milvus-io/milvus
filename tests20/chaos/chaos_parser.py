import logging
import os
from yaml import full_load
#from milvus_benchmark.chaos import utils

logger = logging.getLogger("milvus_benchmark.chaos.base")


class BaseChaos(object):
    cur_path = os.path.abspath(os.path.dirname(__file__))

    def __init__(self, yaml):
        self.yaml = yaml
        pass

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

    def gen_experiment_config(self):
        with open(self.yaml) as f:
            default_config = full_load(f)
            f.close()
        return default_config


class PodChaos(BaseChaos):
    default_yaml = BaseChaos.cur_path + 'chaos_objects/chaos_standalone_podkill.yaml'

    def __init__(self, api_version, kind, metadata, spec):
        super(PodChaos, self).__init__()


class NetworkChaos(BaseChaos):
    def __init__(self, api_version, kind, metadata, spec):
        super(NetworkChaos, self).__init__(api_version, kind, metadata, spec)

    def gen_experiment_config(self):
        pass

