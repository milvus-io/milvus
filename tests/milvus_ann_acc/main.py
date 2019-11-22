import os
import sys
import argparse
from yaml import load, dump
import logging
from logging import handlers
from client import MilvusClient
import runner

LOG_FOLDER = "logs"
logger = logging.getLogger("milvus_acc")
formatter = logging.Formatter('[%(asctime)s] [%(levelname)-4s] [%(pathname)s:%(lineno)d] %(message)s')
if not os.path.exists(LOG_FOLDER):
    os.system('mkdir -p %s' % LOG_FOLDER)
fileTimeHandler = handlers.TimedRotatingFileHandler(os.path.join(LOG_FOLDER, 'acc'), "D", 1, 10)
fileTimeHandler.suffix = "%Y%m%d.log"
fileTimeHandler.setFormatter(formatter)
logging.basicConfig(level=logging.DEBUG)
fileTimeHandler.setFormatter(formatter)
logger.addHandler(fileTimeHandler)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="server host")
    parser.add_argument(
        "--port",
        default=19530,
        help="server port")   
    parser.add_argument(
        '--suite',
        metavar='FILE',
        help='load config definitions from suite_czr'
             '.yaml',
        default='suite_czr.yaml')
    args = parser.parse_args()
    if args.suite:
        with open(args.suite, "r") as f:
            suite = load(f)
            hdf5_path = suite["hdf5_path"]
            dataset_configs = suite["datasets"]
            if not hdf5_path or not dataset_configs:
                logger.warning("No datasets given")
                sys.exit()
            f.close()
    for dataset_config in dataset_configs:
        logger.debug(dataset_config)
        milvus_instance = MilvusClient(host=args.host, port=args.port)
        runner.run(milvus_instance, dataset_config, hdf5_path)


if __name__ == "__main__":
    main()