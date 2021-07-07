import logging
from .helm import HelmEnv
from .docker import DockerEnv
from .local import LocalEnv

logger = logging.getLogger("milvus_benchmark.env")


def get_env(env_mode, deploy_mode=None):
    return {
        "helm": HelmEnv(deploy_mode),
        "docker": DockerEnv(None),
        "local": LocalEnv(None),
    }.get(env_mode)
