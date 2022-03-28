import logging
from milvus_benchmark import utils
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.env.env")


class BaseEnv(object):
    """docstring for Env"""
    def __init__(self, deploy_mode="single"):
        self.deploy_mode = deploy_mode
        self._name = utils.get_unique_name()
        self._hostname = None
        self._port = config.SERVER_PORT_DEFAULT

    def start_up(self, **kwargs):
        logger.debug("IN ENV CLASS")
        pass

    def tear_down(self):
        pass

    def restart(self):
        pass

    def set_hostname(self, hostname):
        self._hostname = hostname

    def set_port(self, port):
        self._port = port

    def resources(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def hostname(self):
        return self._hostname
    
    @property
    def port(self):
        return self._port
    
