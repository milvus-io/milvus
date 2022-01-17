import os
import time
import pdb
import logging
import traceback

from milvus_benchmark.env import helm_utils
from milvus_benchmark.env.base import BaseEnv
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.env.helm")
TIMEOUT = 5

class HelmEnv(BaseEnv):
    """helm env class wrapper"""
    env_mode = "helm"

    def __init__(self, deploy_mode="single"):
        super(HelmEnv, self).__init__(deploy_mode)
        self._name_space = config.HELM_NAMESPACE

    def start_up(self, helm_path, helm_install_params):
        if "namespace" in helm_install_params:
            self._name_space = helm_install_params["namespace"]
        server_name = helm_install_params["server_name"]
        server_tag = helm_install_params["server_tag"] if "server_tag" in helm_install_params else None
        server_config = helm_install_params["server_config"] if "server_config" in helm_install_params else None
        milvus_config = helm_install_params["milvus_config"]
        image_tag = helm_install_params["image_tag"]
        image_type = helm_install_params["image_type"]

        logger.debug(self.deploy_mode)
        server_config = helm_utils.update_server_config(server_name, server_tag, server_config)
        # update values
        values_file_path = helm_path + "/values.yaml"
        if not os.path.exists(values_file_path):
            raise Exception("File {} not existed".format(values_file_path))
        lock_file_path = helm_path + "/values.yaml.lock"
        start_time = time.time()
        while os.path.exists(lock_file_path) and time.time() < start_time+TIMEOUT:
            logger.debug("Waiting for the lock file to release")
            time.sleep(1)
        if not os.path.exists(lock_file_path):
            # generate lock file
            open(lock_file_path, 'a').close()
        try:
            if milvus_config:
                helm_utils.update_values(values_file_path, self.deploy_mode, server_name, server_tag, milvus_config, server_config)
                logger.debug("Config file has been updated, remove the lock file")
            os.system("rm -rf %s" % lock_file_path)
            logger.debug("Start install server")
            hostname = helm_utils.helm_install_server(helm_path, self.deploy_mode, image_tag, image_type, self.name,
                                                       self._name_space)
            status_cmd = 'kubectl get pods -n milvus -l release=zong-standalone -o=jsonpath=\'{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}\''
            if not hostname:
                logger.error("Helm install server failed")
                return False
            else:
                self.set_hostname(hostname)
                while not helm_utils.running_status(self.name, self._name_space):
                    pass
                else:
                    return hostname
        except Exception as e:
            os.system("rm -rf %s" % lock_file_path)
            logger.error("Helm install server failed: %s" % (str(e)))
            logger.error(traceback.format_exc())
            return False

    def tear_down(self):
        logger.debug("Start clean up: {}.{}".format(self.name, self._name_space))
        helm_utils.helm_del_server(self.name, self._name_space)
