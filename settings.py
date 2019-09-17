import sys
import os

from environs import Env

env = Env()
env.read_env()

DEBUG = env.bool('DEBUG', False)
TESTING = env.bool('TESTING', False)

METADATA_URI = env.str('METADATA_URI', '')

LOG_LEVEL = env.str('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')
LOG_PATH = env.str('LOG_PATH', '/tmp/mishards')
LOG_NAME = env.str('LOG_NAME', 'logfile')
TIMEZONE = env.str('TIMEZONE', 'UTC')

from utils.logger_helper import config
config(LOG_LEVEL, LOG_PATH, LOG_NAME, TIMEZONE)

TIMEOUT = env.int('TIMEOUT', 60)
MAX_RETRY = env.int('MAX_RETRY', 3)


if __name__ == '__main__':
    import logging
    logger = logging.getLogger(__name__)
    logger.debug('DEBUG')
    logger.info('INFO')
    logger.warn('WARN')
    logger.error('ERROR')
