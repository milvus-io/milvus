import logging
from milvus_benchmark.env.base import BaseEnv

logger = logging.getLogger("milvus_benchmark.env.docker")


class DockerEnv(BaseEnv):
    """docker env class wrapper"""
    env_mode = "docker"

    def __init__(self, deploy_mode=None):
        super(DockerEnv, self).__init__(deploy_mode)
