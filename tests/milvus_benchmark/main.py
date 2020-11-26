import os
import sys
import time
import pdb
import argparse
import logging
import traceback
from multiprocessing import Process
from queue import Queue
from logging import handlers
from yaml import full_load, dump
from local_runner import LocalRunner
from docker_runner import DockerRunner
from k8s_runner import K8sRunner
import parser

DEFAULT_IMAGE = "milvusdb/milvus:latest"
LOG_FOLDER = "logs"
NAMESPACE = "milvus"

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)
logger = logging.getLogger("milvus_benchmark")


def positive_int(s):
    i = None
    try:
        i = int(s)
    except ValueError:
        pass
    if not i or i < 1:
        raise argparse.ArgumentTypeError("%r is not a positive integer" % s)
    return i


def get_image_tag(image_version, image_type):
    return "%s-%s-centos7-release" % (image_version, image_type)
    # return "%s-%s-centos7-release" % ("0.7.1", image_type)
    # return "%s-%s-centos7-release" % ("PR-2780", image_type)


def queue_worker(queue):
    """
    Using queue to make sure only one test process on each host,
    workers can be run concurrently on different host
    """
    while not queue.empty():
        q = queue.get()
        suite = q["suite"]
        server_host = q["server_host"]
        deploy_mode = q["deploy_mode"]
        image_type = q["image_type"]
        image_tag = q["image_tag"]

        with open(suite) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)

        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        for collection in collections:
            # run tests
            server_config = collection["server"] if "server" in collection else None
            logger.debug(server_config)
            runner = K8sRunner()
            if runner.init_env(server_config, server_host, deploy_mode, image_type, image_tag):
                logger.debug("Start run tests")
                try:
                    runner.run(run_type, collection)
                except Exception as e:
                    logger.error(str(e))
                    logger.error(traceback.format_exc())
                finally:
                    time.sleep(10)
                    runner.clean_up()
            else:
                logger.error("Runner init failed")
    if server_host:
        logger.debug("All task finished in queue: %s" % server_host)


def main():
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
    arg_parser.add_argument(
        "--deploy-mode",
        default='',
        help="single or shards")

    # local mode
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
    arg_parser.add_argument(
        '--suite',
        metavar='FILE',
        help='load test suite from FILE',
        default='')

    args = arg_parser.parse_args()

    if args.schedule_conf:
        if args.local:
            raise Exception("Helm mode with scheduler and other mode are incompatible")
        if not args.image_version:
            raise Exception("Image version not given")
        image_version = args.image_version
        deploy_mode = args.deploy_mode
        with open(args.schedule_conf) as f:
            schedule_config = full_load(f)
            f.close()
        queues = []
        # server_names = set()
        server_names = []
        for item in schedule_config:
            server_host = item["server"] if "server" in item else ""
            suite_params = item["suite_params"]
            server_names.append(server_host)
            q = Queue()
            for suite_param in suite_params:
                suite = "suites/"+suite_param["suite"]
                image_type = suite_param["image_type"]
                image_tag = get_image_tag(image_version, image_type)    
                q.put({
                    "suite": suite,
                    "server_host": server_host,
                    "deploy_mode": deploy_mode,
                    "image_tag": image_tag,
                    "image_type": image_type
                })
            queues.append(q)
        logging.error(queues)
        thread_num = len(server_names)
        processes = []

        for i in range(thread_num):
            x = Process(target=queue_worker, args=(queues[i], ))
            processes.append(x)
            x.start()
            time.sleep(10)
        for x in processes:
            x.join()

    elif args.local:
        # for local mode
        host = args.host
        port = args.port
        suite = args.suite
        with open(suite) as f:
            suite_dict = full_load(f)
            f.close()
        logger.debug(suite_dict)
        run_type, run_params = parser.operations_parser(suite_dict)
        collections = run_params["collections"]
        if len(collections) > 1:
            raise Exception("Multi collections not supported in Local Mode")
        collection = collections[0]
        runner = LocalRunner(host, port)
        logger.info("Start run local mode test, test type: %s" % run_type)
        runner.run(run_type, collection)


if __name__ == "__main__":
    main()
