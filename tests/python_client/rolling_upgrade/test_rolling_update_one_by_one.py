from pprint import pformat
from pathlib import Path
import subprocess
import pytest
from time import sleep
import yaml
from datetime import datetime
from utils.util_log import test_log as log
from common.common_type import CaseLabel
from chaos import constants
from common.cus_resource_opts import CustomResourceOperations as CusResource
import time
from kubernetes import client, config


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


def interrupt_rolling(release_name, component, timeout=60):
    # get the querynode pod name which age is the newest
    cmd = f"kubectl get pod -n chaos-testing|grep {release_name}|grep {component}|awk '{{print $1}}'"
    _ = run_cmd(cmd)

    chaos_config = {}
    # apply chaos object
    chaos_res = CusResource(
        kind=chaos_config["kind"],
        group=constants.CHAOS_GROUP,
        version=constants.CHAOS_VERSION,
        namespace=constants.CHAOS_NAMESPACE,
    )
    chaos_res.create(chaos_config)
    create_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")
    log.info(f"chaos injected at {create_time}")


def pause_and_resume_rolling(
    metadata_name, deployment_name, namespace, updated_pod_count, pause_seconds
):
    """
    Method: pause and resume the rolling update of a Deployment
    Params:
        metadata_name: name of the Milvus custom resource
        deployment_name: name of the Deployment to pause and resume (choices: querynode, indexnode, datanode)
        namespace: namespace that the Milvus custom resource is running in
        updated_pod_count: number of Pods to update before pausing the rolling update
        pause_seconds: number of seconds to pause the rolling update
    example:
        pause_and_resume_rolling("milvus-cluster", "milvus-cluster-querynode", "default", 1, 60)
    """
    # Load Kubernetes configuration
    config.load_kube_config()
    api_instance = client.AppsV1Api()

    # resuem the rolling update of a Deployment if it is paused
    config = {"spec": {"components": {"paused": False}}}
    with open("milvus_resume.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    cmd = f"kubectl patch milvus {metadata_name} --patch-file milvus_resume.yaml --type merge"
    run_cmd(cmd)
    log.info(f"Resumed milvus {metadata_name} deployment {deployment_name}")

    # Monitor the number of updated Pods
    while True:
        res = api_instance.read_namespaced_deployment(deployment_name, namespace)
        log.info(f"deployment {deployment_name} status: {res.status}")

        updated_replicas = api_instance.read_namespaced_deployment(
            deployment_name, namespace
        ).status.updated_replicas

        replicas = api_instance.read_namespaced_deployment(
            deployment_name, namespace
        ).status.replicas
        log.info(f"updated_replicas: {updated_replicas}, replicas: {replicas}")
        if updated_replicas is None:
            updated_replicas = 0
            log.debug(
                f"updated_replicas: {updated_replicas}, replicas: {replicas}, no replicas updated, keep waiting"
            )
        if updated_replicas >= updated_pod_count and updated_replicas < replicas:
            log.debug(
                f"updated_replicas: {updated_replicas}, replicas: {replicas}, sastisfy the condition to pause the rolling update"
            )
            break
        if updated_replicas >= replicas:
            log.debug(
                f"updated_replicas: {updated_replicas}, replicas: {replicas}, rolling update completed, no need to pause"
            )
            return
        time.sleep(1)
    # Pause the Deployment's rolling update by patching the custom resource
    config = {"spec": {"components": {"paused": True}}}
    with open("milvus_pause.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    time.sleep(1)
    cmd = f"kubectl patch milvus {metadata_name} --patch-file milvus_pause.yaml --type merge"
    run_cmd(cmd)
    log.info(
        f"Paused Milvus {metadata_name} deployment {deployment_name} after updating {updated_pod_count} replicas. "
        f"Waiting for {pause_seconds} seconds..."
    )
    # Wait for the specified pause time
    t0 = time.time()
    cnt = 0
    while time.time() - t0 < pause_seconds:
        cmd = f"kubectl get milvus {metadata_name} -o json|jq .spec.components"
        run_cmd(cmd)
        res = api_instance.read_namespaced_deployment(deployment_name, namespace)
        log.debug(f"deployment {deployment_name} status: {res.status}")
        time.sleep(10)
        cnt += 1
        # show progress every 60s
        if cnt % 6 == 0:
            log.info(f"progress: paused for {cnt * 10}s / {pause_seconds}s")
    # Resume the Deployment's rolling update
    config = {"spec": {"components": {"paused": False}}}
    with open("milvus_resume.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    cmd = f"kubectl patch milvus {metadata_name} --patch-file milvus_resume.yaml --type merge"
    run_cmd(cmd)
    log.info(f"Resumed milvus {metadata_name} deployment {deployment_name}")


def run_cmd(cmd):
    log.info(f"cmd: {cmd}")
    res = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = res.communicate()
    output = stdout.decode("utf-8")
    log.info(f"{cmd}\n{output}\n")
    return output


def check_querynode_upgrade_complete(release_name, namespace):
    cmd = (
        f"kubectl get deployment -n {namespace} | grep {release_name} | grep querynode"
    )
    output = run_cmd(cmd)
    log.info(f"querynode deployment status: {output}")
    deployments = [line.split() for line in output.strip().split("\n")]
    status = []
    current_deployment = ""
    name_list = []
    for deployment in deployments:
        name, ready, *_ = deployment
        name_list.append(name)
    name_list = sorted(name_list)
    current_deployment = name_list[-1]

    for deployment in deployments:
        name, ready, *_ = deployment
        ready_pods, total_pods = map(int, ready.split("/"))
        # the old querynode deployment should have 0/0 pods ready
        if name != current_deployment:
            if ready_pods == 0:
                status.append(True)
            else:
                status.append(False)
        else:
            if ready_pods == total_pods:
                status.append(True)
            else:
                status.append(False)
    return all(status)


class TestOperations(TestBase):
    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(
        self,
        new_image_repo,
        new_image_tag,
        components_order,
        paused_components,
        paused_duration,
    ):
        log.info("*********************Rolling Update Start**********************")
        paused_components = eval(paused_components)
        log.info(f"paused_components: {paused_components}")
        paused_duration = int(paused_duration)
        origin_file_path = f"{str(Path(__file__).parent)}/milvus_crd.yaml"
        log.info(f"origin_file_path: {origin_file_path}")
        with open(origin_file_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        log.info(f"config: {pformat(config['spec']['components'])}")
        target_image = f"{new_image_repo}:{new_image_tag}"
        if "image" in config["spec"]["components"]:
            del config["spec"]["components"]["image"]
        config["spec"]["components"]["imageUpdateMode"] = "all"
        log.info(f"config: {pformat(config['spec']['components'])}")
        # save config to a modified file
        modified_file_path = f"{str(Path(__file__).parent)}/milvus_crd_modified.yaml"
        with open(modified_file_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        kind = config["kind"]
        meta_name = config["metadata"]["name"]
        namespace = config["metadata"]["namespace"]
        # default is ['indexNode', 'rootCoord', ['dataCoord', 'indexCoord'], 'queryCoord', 'queryNode', 'dataNode', 'proxy']
        components = eval(components_order)
        log.info(f"update order: {components}")
        component_time_map = {}
        for component in components:
            prefix = f"[update image for {component}]"
            # load config and update
            with open(modified_file_path, "r") as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
            if isinstance(component, list):
                for c in component:
                    config["spec"]["components"][c]["image"] = target_image
            else:
                config["spec"]["components"][component]["image"] = target_image
            log.info(prefix + f"config: {pformat(config['spec']['components'])}")
            # save config to file
            with open(modified_file_path, "w") as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            log.info(f"update image for component {component}")
            cmd = f"kubectl patch {kind} {meta_name} --patch-file {modified_file_path} --type merge"
            run_cmd(cmd)
            component_time_map[str(component)] = datetime.now()

            # if component in paused_components:
            #     log.info(f"start to interrupt rolling update for {component}")
            #     deploy_name = f"{meta_name}-milvus-{component.lower()}" if "milvus" not in component else f"{meta_name}-{component.lower()}"
            #     cmd = f"kubectl get deployment | grep {deploy_name}"
            #     run_cmd(cmd)
            #     pause_and_resume_rolling(meta_name, deploy_name, namespace, 2, paused_duration)
            # check pod status
            log.info(prefix + "wait 10s after rolling update patch")
            sleep(10)
            cmd = f"kubectl get pod|grep {meta_name}"
            run_cmd(cmd)
            # check milvus status
            ready = False
            while ready is False:
                cmd = f"kubectl get pod|grep {meta_name}"
                run_cmd(cmd)
                sleep(10)
                cmd = f"kubectl get mi |grep {meta_name}"
                output = run_cmd(cmd)
                log.info(f"output: {output}")

                if "True" in output and "Healthy" in output:
                    # Check if the status remains stable for 1 minute
                    stable = True
                    for _ in range(
                        6
                    ):  # Check every 10 seconds, 6 times in total (60 seconds)
                        sleep(10)
                        output = run_cmd(cmd)
                        log.info(f"Re-check output: {output}")
                        if "True" not in output or "Healthy" not in output:
                            stable = False
                            break
                    if component == "queryNode":
                        log.info(prefix + "add additional check for queryNode")
                        while not check_querynode_upgrade_complete(
                            meta_name, namespace
                        ):
                            print("Querynode upgrade not complete, waiting...")
                            time.sleep(10)

                    if stable:
                        ready = True
                    else:
                        log.info(prefix + "status not stable, continue waiting")
                else:
                    log.info(prefix + "wait 10s for milvus ready")
                    sleep(10)
            sleep(60)
        log.info(f"rolling update time: {component_time_map}")
        log.info("*********************Test Completed**********************")
