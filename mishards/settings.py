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

SD_PROVIDER_SETTINGS = None
SD_PROVIDER = env.str('SD_PROVIDER', 'Kubernetes')
if SD_PROVIDER == 'Kubernetes':
    from sd.kubernetes_provider import KubernetesProviderSettings
    SD_PROVIDER_SETTINGS = KubernetesProviderSettings(
        namespace=env.str('SD_NAMESPACE', ''),
        in_cluster=env.bool('SD_IN_CLUSTER', False),
        poll_interval=env.int('SD_POLL_INTERVAL', 5),
        pod_patt=env.str('SD_ROSERVER_POD_PATT', ''),
        label_selector=env.str('SD_LABEL_SELECTOR', '')
    )
elif SD_PROVIDER == 'Static':
    from sd.static_provider import StaticProviderSettings
    SD_PROVIDER_SETTINGS = StaticProviderSettings(
            hosts=env.list('SD_STATIC_HOSTS', [])
            )

TESTING = env.bool('TESTING', False)
TESTING_WOSERVER = env.str('TESTING_WOSERVER', 'tcp://127.0.0.1:19530')

TRACING_ENABLED = env.bool('TRACING_ENABLED', False)
class TracingConfig:
    TRACING_LOGGING = env.bool('TRACING_LOGGING', True),
    TRACING_SERVICE_NAME = env.str('TRACING_SERVICE_NAME', 'mishards')
    TRACING_VALIDATE = env.bool('TRACING_VALIDATE', True)
    TRACING_LOG_PAYLOAD = env.bool('TRACING_LOG_PAYLOAD', DEBUG)
    TRACING_REPORTING_HOST = env.str('TRACING_REPORTING_HOST', '127.0.0.1')
    TRACING_REPORTING_PORT = env.str('TRACING_REPORTING_PORT', '5775')


if __name__ == '__main__':
    import logging
    logger = logging.getLogger(__name__)
    logger.debug('DEBUG')
    logger.info('INFO')
    logger.warn('WARN')
    logger.error('ERROR')
