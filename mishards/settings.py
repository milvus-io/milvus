import sys
import os

from environs import Env

env = Env()
env.read_env()

DEBUG = env.bool('DEBUG', False)

LOG_LEVEL = env.str('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')
LOG_PATH = env.str('LOG_PATH', '/tmp/mishards')
LOG_NAME = env.str('LOG_NAME', 'logfile')
TIMEZONE = env.str('TIMEZONE', 'UTC')

from utils.logger_helper import config
config(LOG_LEVEL, LOG_PATH, LOG_NAME, TIMEZONE)

SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_URI')
SQL_ECHO = env.bool('SQL_ECHO', False)

TIMEOUT = env.int('TIMEOUT', 60)
MAX_RETRY = env.int('MAX_RETRY', 3)
SEARCH_WORKER_SIZE = env.int('SEARCH_WORKER_SIZE', 10)

SERVER_PORT = env.int('SERVER_PORT', 19530)
WOSERVER = env.str('WOSERVER')

SD_NAMESPACE = env.str('SD_NAMESPACE', '')
SD_IN_CLUSTER = env.bool('SD_IN_CLUSTER', False)
SD_POLL_INTERVAL = env.int('SD_POLL_INTERVAL', 5)
SD_ROSERVER_POD_PATT = env.str('SD_ROSERVER_POD_PATT', '')
SD_LABEL_SELECTOR = env.str('SD_LABEL_SELECTOR', '')

TESTING = env.bool('TESTING', False)
TESTING_WOSERVER = env.str('TESTING_WOSERVER', 'tcp://127.0.0.1:19530')


if __name__ == '__main__':
    import logging
    logger = logging.getLogger(__name__)
    logger.debug('DEBUG')
    logger.info('INFO')
    logger.warn('WARN')
    logger.error('ERROR')
