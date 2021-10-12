import logging
from milvus_benchmark.env.base import BaseEnv

logger = logging.getLogger("milvus_benchmark.env.local")


class LocalEnv(BaseEnv):
    """docker env class wrapper"""
    """ The current method of using the deployed Kubernetes environment to call """
    env_mode = "local"

    def __init__(self, deploy_mode=None):
        super(LocalEnv, self).__init__(deploy_mode)

    def start_up(self, hostname, port):
        res = True
        try:
            self.set_hostname(hostname)
            self.set_port(port)
        except Exception as e:
            logger.error(str(e))
            res = False
        return res
