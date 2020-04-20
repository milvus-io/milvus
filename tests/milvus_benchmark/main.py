import os
import sys
import time
import pdb
import argparse
import logging
import traceback
from logging import handlers

from yaml import full_load, dump
from parser import operations_parser
from local_runner import LocalRunner
from docker_runner import DockerRunner
from k8s_runner import K8sRunner

DEFAULT_IMAGE = "milvusdb/milvus:latest"
LOG_FOLDER = "logs"

# formatter = logging.Formatter('[%(asctime)s] [%(levelname)-4s] [%(pathname)s:%(lineno)d] %(message)s')
# if not os.path.exists(LOG_FOLDER):
#     os.system('mkdir -p %s' % LOG_FOLDER)
# fileTimeHandler = handlers.TimedRotatingFileHandler(os.path.join(LOG_FOLDER, 'milvus_benchmark'), "D", 1, 10)
# fileTimeHandler.suffix = "%Y%m%d.log"
# fileTimeHandler.setFormatter(formatter)
# logging.basicConfig(level=logging.DEBUG)
# fileTimeHandler.setFormatter(formatter)
# logger.addHandler(fileTimeHandler)
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


# # link random_data if not exists
# def init_env():
#     if not os.path.islink(BINARY_DATA_FOLDER):
#         try:
#             os.symlink(SRC_BINARY_DATA_FOLDER, BINARY_DATA_FOLDER)
#         except Exception as e:
#             logger.error("Create link failed: %s" % str(e))
#             sys.exit()


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--hostname",
        default="eros",
        help="server host name")
    parser.add_argument(
        "--image-tag",
        default="",
        help="image tag")
    parser.add_argument(
        "--image-type",
        default="",
        help="image type")
    # parser.add_argument(
    #     "--run-count",
    #     default=1,
    #     type=positive_int,
    #     help="run times for each test")
    # # performance / stability / accuracy test
    # parser.add_argument(
    #     "--run-type",
    #     default="search_performance",
    #     help="run type, default performance")
    parser.add_argument(
        '--suite',
        metavar='FILE',
        help='load test suite from FILE',
        default='suites/suite.yaml')
    parser.add_argument(
        '--local',
        action='store_true',
        help='use local milvus server')
    parser.add_argument(
        '--host',
        help='server host ip param for local mode',
        default='127.0.0.1')
    parser.add_argument(
        '--port',
        help='server port param for local mode',
        default='19530')

    args = parser.parse_args()

    # Get all benchmark test suites
    if args.suite:
        with open(args.suite) as f:
            suite_dict = full_load(f)
            f.close()
        # With definition order
        run_type, run_params = operations_parser(suite_dict)

    # init_env()
    # run_params = {"run_count": args.run_count}

    if args.image_tag:
        namespace = "milvus"
        logger.debug(args)
        # for docker mode
        if args.local:
            logger.error("Local mode and docker mode are incompatible")
            sys.exit(-1)
        # Docker pull image
        # if not utils.pull_image(args.image):
        #     raise Exception('Image %s pull failed' % image)
        # TODO: Check milvus server port is available
        # logger.info("Init: remove all containers created with image: %s" % args.image)
        # utils.remove_all_containers(args.image)
        # runner = DockerRunner(args)
        tables = run_params["tables"]
        for table in tables:
            # run tests
            server_config = table["server"]
            logger.debug(server_config)
            runner = K8sRunner()
            if runner.init_env(server_config, args):
                logger.debug("Start run tests")
                try:
                    runner.run(run_type, table)
                except Exception as e:
                    logger.error(str(e))
                    logger.error(traceback.format_exc())
                finally:
                    runner.clean_up()
            else:
                logger.error("Runner init failed")
        # for operation_type in operations:
        #     logger.info("Start run test, test type: %s" % operation_type)
        #     run_params["params"] = operations[operation_type]
        #     runner.run({operation_type: run_params}, run_type=args.run_type)
        #     logger.info("Run params: %s" % str(run_params))

    if args.local:
        # for local mode
        host = args.host
        port = args.port

        runner = LocalRunner(host, port)
        for operation_type in operations:
            logger.info("Start run local mode test, test type: %s" % operation_type)
            run_params["params"] = operations[operation_type]
            runner.run({operation_type: run_params}, run_type=args.run_type)
            logger.info("Run params: %s" % str(run_params))


if __name__ == "__main__":
    main()