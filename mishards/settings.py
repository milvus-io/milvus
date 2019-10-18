import sys
import os

from environs import Env
env = Env()

FROM_EXAMPLE = env.bool('FROM_EXAMPLE', False)
if FROM_EXAMPLE:
    from dotenv import load_dotenv
    load_dotenv('./mishards/.env.example')
else:
    env.read_env()

DEBUG = env.bool('DEBUG', False)

LOG_LEVEL = env.str('LOG_LEVEL', 'DEBUG' if DEBUG else 'INFO')
LOG_PATH = env.str('LOG_PATH', '/tmp/mishards')
LOG_NAME = env.str('LOG_NAME', 'logfile')
TIMEZONE = env.str('TIMEZONE', 'UTC')

from utils.logger_helper import config
config(LOG_LEVEL, LOG_PATH, LOG_NAME, TIMEZONE)

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
        label_selector=env.str('SD_LABEL_SELECTOR', ''))
elif SD_PROVIDER == 'Static':
    from sd.static_provider import StaticProviderSettings
    SD_PROVIDER_SETTINGS = StaticProviderSettings(
        hosts=env.list('SD_STATIC_HOSTS', []))

# TESTING_WOSERVER = env.str('TESTING_WOSERVER', 'tcp://127.0.0.1:19530')


class TracingConfig:
    TRACING_SERVICE_NAME = env.str('TRACING_SERVICE_NAME', 'mishards')
    TRACING_VALIDATE = env.bool('TRACING_VALIDATE', True)
    TRACING_LOG_PAYLOAD = env.bool('TRACING_LOG_PAYLOAD', False)
    TRACING_CONFIG = {
        'sampler': {
            'type': env.str('TRACING_SAMPLER_TYPE', 'const'),
            'param': env.str('TRACING_SAMPLER_PARAM', "1"),
        },
        'local_agent': {
            'reporting_host': env.str('TRACING_REPORTING_HOST', '127.0.0.1'),
            'reporting_port': env.str('TRACING_REPORTING_PORT', '5775')
        },
        'logging': env.bool('TRACING_LOGGING', True)
    }
    DEFAULT_TRACING_CONFIG = {
        'sampler': {
            'type': env.str('TRACING_SAMPLER_TYPE', 'const'),
            'param': env.str('TRACING_SAMPLER_PARAM', "0"),
        }
    }


class DefaultConfig:
    SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_URI')
    SQL_ECHO = env.bool('SQL_ECHO', False)
    TRACING_TYPE = env.str('TRACING_TYPE', '')
    ROUTER_CLASS_NAME = env.str('ROUTER_CLASS_NAME', 'FileBasedHashRingRouter')


class TestingConfig(DefaultConfig):
    SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_TEST_URI')
    SQL_ECHO = env.bool('SQL_TEST_ECHO', False)
    TRACING_TYPE = env.str('TRACING_TEST_TYPE', '')
    ROUTER_CLASS_NAME = env.str('ROUTER_CLASS_TEST_NAME', 'FileBasedHashRingRouter')


if __name__ == '__main__':
    import logging
    logger = logging.getLogger(__name__)
    logger.debug('DEBUG')
    logger.info('INFO')
    logger.warn('WARN')
    logger.error('ERROR')
