import copy
import logging
import pdb
import time
from operator import methodcaller
from yaml import full_load, dump
import threading
from milvus_benchmark import utils
from milvus_benchmark.runners import utils as runner_utils
from milvus_benchmark.chaos import utils as chaos_utils
from milvus_benchmark.runners.base import BaseRunner
from chaos.chaos_opt import ChaosOpt
from milvus_benchmark import config
from milvus_benchmark.chaos.chaos_mesh import PodChaos, NetworkChaos

logger = logging.getLogger("milvus_benchmark.runners.chaos")

kind_chaos_mapping = {
    "PodChaos": PodChaos,
    "NetworkChaos": NetworkChaos
}

assert_func_mapping = {
    "fail": chaos_utils.assert_fail,
    "pass": chaos_utils.assert_pass
}


class SimpleChaosRunner(BaseRunner):
    """run chaos"""
    name = "simple_chaos"

    def __init__(self, env, metric):
        super(SimpleChaosRunner, self).__init__(env, metric)

    async def async_call(self, func, **kwargs):
        future = methodcaller(func, **kwargs)(self.milvus)

    def run_step(self, interface_name, interface_params):
        if interface_name == "create_collection":
            collection_name = utils.get_unique_name("chaos")
            self.data_type = interface_params["data_type"]
            self.dimension = interface_params["dimension"]
            self.milvus.set_collection(collection_name)
            vector_type = runner_utils.get_vector_type(self.data_type)
            self.milvus.create_collection(self.dimension, data_type=vector_type)
        elif interface_name == "insert":
            batch_size = interface_params["batch_size"]
            collection_size = interface_params["collection_size"]
            self.insert(self.milvus, self.milvus.collection_name, self.data_type, self.dimension, collection_size,
                        batch_size)
        elif interface_name == "create_index":
            metric_type = interface_params["metric_type"]
            index_type = interface_params["index_type"]
            index_param = interface_params["index_param"]
            vector_type = runner_utils.get_vector_type(self.data_type)
            field_name = runner_utils.get_default_field_name(vector_type)
            self.milvus.create_index(field_name, index_type, metric_type, index_param=index_param)
        elif interface_name == "flush":
            self.milvus.flush()

    def extract_cases(self, collection):
        before_steps = collection["before"]
        after = collection["after"] if "after" in collection else None
        processing = collection["processing"]
        case_metrics = []
        case_params = [{
            "before_steps": before_steps,
            "after": after,
            "processing": processing
        }]
        self.init_metric(self.name, {}, {}, None)
        case_metric = copy.deepcopy(self.metric)
        case_metric.set_case_metric_type()
        case_metrics.append(case_metric)
        return case_params, case_metrics

    def prepare(self, **case_param):
        steps = case_param["before_steps"]
        for step in steps:
            interface_name = step["interface_name"]
            params = step["params"]
            self.run_step(interface_name, params)

    def run_case(self, case_metric, **case_param):
        processing = case_param["processing"]
        after = case_param["after"]
        user_chaos = processing["chaos"]
        kind = user_chaos["kind"]
        spec = user_chaos["spec"]
        metadata_name = config.NAMESPACE + "-" + kind.lower()
        metadata = {"name": metadata_name}
        process_assertion = processing["assertion"]
        after_assertion = after["assertion"]
        # load yaml from default template to generate stand chaos dict
        chaos_mesh = kind_chaos_mapping[kind](config.DEFAULT_API_VERSION, kind, metadata, spec)
        experiment_config = chaos_mesh.gen_experiment_config()
        process_func = processing["interface_name"]
        process_params = processing["params"] if "params" in processing else {}
        after_func = after["interface_name"]
        after_params = after["params"] if "params" in after else {}
        logger.debug(chaos_mesh.kind)
        chaos_opt = ChaosOpt(chaos_mesh.kind)
        chaos_objects = chaos_opt.list_chaos_object()
        if len(chaos_objects["items"]) != 0:
            logger.debug(chaos_objects["items"])
            chaos_opt.delete_chaos_object(chaos_mesh.metadata["name"])
        # with open('./pod-newq.yaml', "w") as f:
        #     dump(experiment_config, f)
        #     f.close()
        # concurrent inject chaos and run func
        # logger.debug(experiment_config)
        t_milvus = threading.Thread(target=assert_func_mapping[process_assertion], args=(process_func, self.milvus,), kwargs=process_params)
        try:
            t_milvus.start()
            chaos_opt.create_chaos_object(experiment_config)
        # processing assert exception
        except Exception as e:
            logger.info("exception {}".format(str(e)))
        else:
            chaos_opt.delete_chaos_object(chaos_mesh.metadata["name"])
            # TODO retry connect milvus
            time.sleep(15)
            assert_func_mapping[after_assertion](after_func, self.milvus, **after_params)
        finally:
            chaos_opt.delete_all_chaos_object()
            logger.info(chaos_opt.list_chaos_object())
