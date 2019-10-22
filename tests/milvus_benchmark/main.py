import os
import sys
import time
import pdb
import argparse
import logging
import utils
from yaml import load, dump
from logging import handlers
from parser import operations_parser
from local_runner import LocalRunner
from docker_runner import DockerRunner

DEFAULT_IMAGE = "milvusdb/milvus:latest"
LOG_FOLDER = "benchmark_logs"
logger = logging.getLogger("milvus_benchmark")

formatter = logging.Formatter('[%(asctime)s] [%(levelname)-4s] [%(pathname)s:%(lineno)d] %(message)s')
if not os.path.exists(LOG_FOLDER):
    os.system('mkdir -p %s' % LOG_FOLDER)
fileTimeHandler = handlers.TimedRotatingFileHandler(os.path.join(LOG_FOLDER, 'milvus_benchmark'), "D", 1, 10)
fileTimeHandler.suffix = "%Y%m%d.log"
fileTimeHandler.setFormatter(formatter)
logging.basicConfig(level=logging.DEBUG)
fileTimeHandler.setFormatter(formatter)
logger.addHandler(fileTimeHandler)


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
        '--image',
        help='use the given image')
    parser.add_argument(
        '--local',
        action='store_true',
        help='use local milvus server')
    parser.add_argument(
        "--run-count",
        default=1,
        type=positive_int,
        help="run each db operation times")
    # performance / stability / accuracy test
    parser.add_argument(
        "--run-type",
        default="performance",
        help="run type, default performance")
    parser.add_argument(
        '--suites',
        metavar='FILE',
        help='load test suites from FILE',
        default='suites.yaml')
    parser.add_argument(
        '--ip',
        help='server ip param for local mode',
        default='127.0.0.1')
    parser.add_argument(
        '--port',
        help='server port param for local mode',
        default='19530')

    args = parser.parse_args()

    operations = None
    # Get all benchmark test suites
    if args.suites:
        with open(args.suites) as f:
            suites_dict = load(f)
            f.close()
        # With definition order
        operations = operations_parser(suites_dict, run_type=args.run_type)

    # init_env()
    run_params = {"run_count": args.run_count}

    if args.image:
        # for docker mode
        if args.local:
            logger.error("Local mode and docker mode are incompatible arguments")
            sys.exit(-1)
        # Docker pull image
        if not utils.pull_image(args.image):
            raise Exception('Image %s pull failed' % image)

        # TODO: Check milvus server port is available
        logger.info("Init: remove all containers created with image: %s" % args.image)
        utils.remove_all_containers(args.image)
        runner = DockerRunner(args.image)
        for operation_type in operations:
            logger.info("Start run test, test type: %s" % operation_type)
            run_params["params"] = operations[operation_type]
            runner.run({operation_type: run_params}, run_type=args.run_type)
            logger.info("Run params: %s" % str(run_params))

    if args.local:
        # for local mode
        ip = args.ip
        port = args.port

        runner = LocalRunner(ip, port)
        for operation_type in operations:
            logger.info("Start run local mode test, test type: %s" % operation_type)
            run_params["params"] = operations[operation_type]
            runner.run({operation_type: run_params}, run_type=args.run_type)
            logger.info("Run params: %s" % str(run_params))


if __name__ == "__main__":
    main()