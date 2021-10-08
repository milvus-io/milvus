import logging.config
from datetime import datetime
import os
import yaml
import config

cur_path = os.path.abspath(os.path.dirname(__file__))
LOG_CONFIG_PATH = cur_path + "/logging.yaml"
FILE_NAME = config.LOG_PATH + 'benchmark-{:%Y-%m-%d}.log'.format(datetime.now())


def setup_logging(config_path=LOG_CONFIG_PATH, default_level=logging.INFO):
    """
    Setup logging configuration
    """
    print(FILE_NAME)
    try:
        with open(config_path, 'rt') as f:
            log_config = yaml.safe_load(f.read())
        log_config["handlers"]["info_file_handler"].update({"filename": FILE_NAME})
        logging.config.dictConfig(log_config)
    except Exception:
        raise
        logging.error('Failed to open file', exc_info=True)
