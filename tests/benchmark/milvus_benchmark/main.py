import os
import sys
import time
import argparse
import logging
import traceback
from yaml import full_load, dump
from milvus_benchmark.metrics.models.server import Server
from milvus_benchmark.metrics.models.hardware import Hardware
from milvus_benchmark.metrics.models.env import Env

from milvus_benchmark.env import get_env
from milvus_benchmark.runners import get_runner
from milvus_benchmark.metrics import api
from milvus_benchmark import config, utils
from milvus_benchmark import parser
# from scheduler import back_scheduler
from logs import log

log.setup_logging()
logger = logging.getLogger("milvus_benchmark.main")

def positive_int(s):
    i = None
    try:
        i = int(s)
    except ValueError:
        pass
    if not i or i < 1:
        raise argparse.ArgumentTypeError("%r is not a positive integer" % s)
    return i


def get_image_tag(image_version):
    """ Set the image version to the latest version """
    return "%s-latest" % (image_version)


# def shutdown(event):
#     logger.info("Check if there is scheduled jobs in scheduler")
#     if not back_scheduler.get_jobs():
#         logger.info("No job in scheduler, will shutdown the scheduler")
#         back_scheduler.shutdown(wait=False)


def run_suite(run_type, suite, env_mode, env_params, timeout=None):
    try:
        start_status = False
        # Initialize the class of the reported metric
        metric = api.Metric()
        deploy_mode = env_params["deploy_mode"]
        deploy_opology = env_params["deploy_opology"] if "deploy_opology" in env_params else None
        env = get_env(env_mode, deploy_mode)
        metric.set_run_id()
        metric.set_mode(env_mode)
        metric.env = Env()
        metric.server = Server(version=config.SERVER_VERSION, mode=deploy_mode, deploy_opology=deploy_opology)
        logger.info(env_params)
        if env_mode == "local":
            metric.hardware = Hardware("")
            if "server_tag" in env_params and env_params["server_tag"]:
                metric.hardware = Hardware("server_tag")
            start_status = env.start_up(env_params["host"], env_params["port"])
        elif env_mode == "helm":
            helm_params = env_params["helm_params"]
            helm_path = env_params["helm_path"]
            server_name = helm_params["server_name"] if "server_name" in helm_params else None
            server_tag = helm_params["server_tag"] if "server_tag" in helm_params else None
            if not server_name and not server_tag:
                metric.hardware = Hardware("")
            else:
                metric.hardware = Hardware(server_name) if server_name else Hardware(server_tag)
            start_status = env.start_up(helm_path, helm_params)
        if start_status:
            metric.update_status(status="DEPLOYE_SUCC")
            logger.debug("Get runner")
            runner = get_runner(run_type, env, metric)
            cases, case_metrics = runner.extract_cases(suite)
            # TODO: only run when the as_group is equal to True
            logger.info("Prepare to run cases")
            runner.prepare(**cases[0])
            logger.info("Start run case")
            suite_status = True
            for index, case in enumerate(cases):
                case_metric = case_metrics[index]
                result = None
                err_message = ""
                try:
                    result = runner.run_case(case_metric, **case)
                except Exception as e:
                    err_message = str(e) + "\n" + traceback.format_exc()
                    logger.error(traceback.format_exc())
                logger.info(result)
                if result:
                    # Save the result of this test as true, and save the related test value results
                    case_metric.update_status(status="RUN_SUCC")
                    case_metric.update_result(result)
                else:
                    # The test run fails, save the related errors of the run method
                    case_metric.update_status(status="RUN_FAILED")
                    case_metric.update_message(err_message)
                    suite_status = False
                logger.debug(case_metric.metrics)
                if deploy_mode:
                    api.save(case_metric)
            if suite_status:
                metric.update_status(status="RUN_SUCC")
            else:
                metric.update_status(status="RUN_FAILED")
        else:
            logger.info("Deploy failed on server")
            metric.update_status(status="DEPLOYE_FAILED")
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())
        metric.update_status(status="RUN_FAILED")
    finally:
        if deploy_mode:
            # Save all reported data to the database
            api.save(metric)
        # time.sleep(10)
        env.tear_down()
        if metric.status != "RUN_SUCC":
            return False
        else:
            return True


def main():
    # Parse the incoming parameters and run the corresponding test cases
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # helm mode with scheduler
    arg_parser.add_argument(
        "--image-version",
        default="",
        help="image version")
    arg_parser.add_argument(
        "--schedule-conf",
        metavar='FILE',
        default='',
        help="load test schedule from FILE")

    # local mode
    # Use the deployed milvus server, and pass host and port
    arg_parser.add_argument(
        '--local',
        action='store_true',
        help='use local milvus server')
    arg_parser.add_argument(
        '--host',
        help='server host ip param for local mode',
        default='127.0.0.1')
    arg_parser.add_argument(
        '--port',
        help='server port param for local mode',
        default='19530')

    # Client configuration file
    arg_parser.add_argument(
        '--suite',
        metavar='FILE',
        help='load test suite from FILE',
        default='')

    # Milvus deploy config file
    arg_parser.add_argument(
        '--server-config',
        metavar='FILE',
        help='load server config from FILE',
        default='')

    args = arg_parser.parse_args()

    if args.schedule_conf:
        if args.local:
            raise Exception("Helm mode with scheduler and other mode are incompatible")
        if not args.image_version:
            raise Exception("Image version not given")
        env_mode = "helm"
        image_version = args.image_version
        with open(args.schedule_conf) as f:
            schedule_config = full_load(f)
            f.close()
        helm_path = os.path.join(os.getcwd(), "..//milvus-helm-charts/charts/milvus-ha")
        for item in schedule_config:
            server_host = item["server"] if "server" in item else ""
            server_tag = item["server_tag"] if "server_tag" in item else ""
            deploy_mode = item["deploy_mode"] if "deploy_mode" in item else config.DEFAULT_DEPLOY_MODE
            suite_params = item["suite_params"]
            for suite_param in suite_params:
                suite_file = "suites/" + suite_param["suite"]
                with open(suite_file) as f:
                    suite_dict = full_load(f)
                    f.close()
                logger.debug(suite_dict)
                run_type, run_params = parser.operations_parser(suite_dict)
                collections = run_params["collections"]
                image_type = suite_param["image_type"]
                image_tag = get_image_tag(image_version)
                for suite in collections:
                    # run test cases
                    milvus_config = suite["milvus"] if "milvus" in suite else None
                    server_config = suite["server"] if "server" in suite else None
                    logger.debug(milvus_config)
                    logger.debug(server_config)
                    helm_params = {
                        "server_name": server_host,
                        "server_tag": server_tag,
                        "server_config": server_config,
                        "milvus_config": milvus_config,
                        "image_tag": image_tag,
                        "image_type": image_type
                    }
                    env_params = {
                        "deploy_mode": deploy_mode,
                        "helm_path": helm_path,
                        "helm_params": helm_params
                    }
                    # job = back_scheduler.add_job(run_suite, args=[run_type, suite, env_mode, env_params],
                    #                              misfire_grace_time=36000)
                    # logger.info(job)
                    # logger.info(job.id)

    elif args.local:
        # for local mode
        deploy_params = args.server_config
        deploy_params_dict = None
        if deploy_params:
            with open(deploy_params) as f:
                deploy_params_dict = full_load(f)
                f.close()
            logger.debug(deploy_params_dict)
        deploy_mode = utils.get_deploy_mode(deploy_params_dict)
        server_tag = utils.get_server_tag(deploy_params_dict)
        env_params = {
            "host": args.host,
            "port": args.port,
            "deploy_mode": deploy_mode,
            "server_tag": server_tag,
            "deploy_opology": deploy_params_dict
        }
        suite_file = args.suite
        with open(suite_file) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)
        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        if len(collections) > 1:
            raise Exception("Multi collections not supported in Local Mode")
        # ensure there is only one case in suite
        # suite = {"run_type": run_type, "run_params": collections[0]}
        suite = collections[0]
        timeout = suite["timeout"] if "timeout" in suite else None
        env_mode = "local"
        return run_suite(run_type, suite, env_mode, env_params, timeout=timeout)
        # job = back_scheduler.add_job(run_suite, args=[run_type, suite, env_mode, env_params], misfire_grace_time=36000)
        # logger.info(job)
        # logger.info(job.id)


if __name__ == "__main__":
    try:
        if not main():
            sys.exit(-1)
        # from apscheduler.events import EVENT_JOB_MISSED
        # back_scheduler.add_listener(listen_miss, EVENT_JOB_MISSED)
        # back_scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     logger.error("Received interruption")
    #     # back_scheduler.shutdown(wait=False)
    #     sys.exit(0)
    except Exception as e:
        logger.error(traceback.format_exc())
        # back_scheduler.shutdown(wait=False)
        sys.exit(-2)
    # block_scheduler.shutdown(wait=False)
    logger.info("All tests run finshed")
    sys.exit(0)
