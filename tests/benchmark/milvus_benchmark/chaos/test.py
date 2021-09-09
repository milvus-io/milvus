from gevent import monkey
monkey.patch_all()
from yaml import full_load, dump
from chaos.chaos_opt import ChaosOpt
from milvus_benchmark.chaos.chaos_mesh import PodChaos, NetworkChaos
from milvus_benchmark import config

kind_chaos_mapping = {
    "PodChaos": PodChaos,
    "NetworkChaos": NetworkChaos
}


if __name__ == '__main__':
    with open('./pod.yaml') as f:
        conf = full_load(f)
        f.close()
    chaos_config = conf["chaos"]
    kind = chaos_config["kind"]
    spec = chaos_config["spec"]
    metadata_name = config.NAMESPACE + "-" + kind.lower()
    metadata = {"name": metadata_name}
    chaos_mesh = kind_chaos_mapping[kind](config.DEFAULT_API_VERSION, kind, metadata, spec)
    experiment_params = chaos_mesh.gen_experiment_config()
    # print(experiment_params)
    # with open('./pod-new-chaos.yaml', "w") as f:
    #     dump(experiment_params, f)
    #     f.close()
    chaos_opt = ChaosOpt(chaos_mesh.kind)
    res = chaos_opt.list_chaos_object()
    print(res)
    if len(res["items"]) != 0:
        # chaos_opt.delete_chaos_object("milvus-pod-chaos")
        print(res["items"][0]["metadata"]["name"])
        chaos_opt.delete_all_chaos_object()
    print(chaos_opt.list_chaos_object())
