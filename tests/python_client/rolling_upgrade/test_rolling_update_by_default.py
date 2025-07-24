from pprint import pformat
from pathlib import Path
import subprocess
import pytest
from time import sleep
import yaml
from utils.util_log import test_log as log
from common.common_type import CaseLabel
from chaos import constants


class TestBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    host = "127.0.0.1"
    port = 19530
    _chaos_config = None
    health_checkers = {}


def run_cmd(cmd):
    log.info(f"cmd: {cmd}")
    res = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = res.communicate()
    output = stdout.decode("utf-8")
    log.info(f"{cmd}\n{output}\n")
    return output


class TestOperations(TestBase):
    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, new_image_repo, new_image_tag):
        log.info("*********************Rolling Update Start**********************")
        origin_file_path = f"{str(Path(__file__).parent)}/milvus_crd.yaml"
        with open(origin_file_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        target_image = f"{new_image_repo}:{new_image_tag}"
        config["spec"]["components"]["image"] = target_image
        config["spec"]["components"]["imageUpdateMode"] = "rollingUpgrade"
        log.info(f"config: {pformat(config['spec']['components'])}")
        components = [
            "indexNode",
            "rootCoord",
            "dataCoord",
            "indexCoord",
            "queryCoord",
            "dataNode",
            "queryNode",
            "proxy",
            "standalone",
            "mixCoord",
        ]
        # delete image in specific component
        for c in components:
            if c in config["spec"]["components"]:
                config["spec"]["components"][c]["image"] = ""
        # save config to a modified file
        modified_file_path = f"{str(Path(__file__).parent)}/milvus_crd_modified.yaml"
        with open(modified_file_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        kind = config["kind"]
        meta_name = config["metadata"]["name"]
        cmd = f"kubectl patch {kind} {meta_name} --patch-file {modified_file_path} --type merge"
        run_cmd(cmd)
        # check pod status
        log.info("wait 10s after rolling update patch")
        sleep(10)
        cmd = f"kubectl get pod|grep {meta_name}"
        run_cmd(cmd)
        # check milvus status
        ready = False
        while ready is False:
            cmd = f"kubectl get pod|grep {meta_name}"
            run_cmd(cmd)
            sleep(10)
            cmd = f"kubectl get mi {meta_name}"
            output = run_cmd(cmd)
            if "True" in output:
                ready = True
            else:
                log.info("wait 10s for milvus ready and updated")
                sleep(10)
        log.info("*********************Test Completed**********************")
