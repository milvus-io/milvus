import logging
from jaeger_client import Config
from grpc_opentracing.grpcext import intercept_server
from grpc_opentracing import open_tracing_server_interceptor
from tracer import Tracer

logger = logging.getLogger(__name__)

PLUGIN_NAME = __file__


class JaegerFactory:
    name = 'jaeger'
    @classmethod
    def Create(cls, plugin_config, **kwargs):
        tracing_config = plugin_config.TRACING_CONFIG
        span_decorator = kwargs.pop('span_decorator', None)
        service_name = plugin_config.TRACING_SERVICE_NAME
        validate = plugin_config.TRACING_VALIDATE
        config = Config(config=tracing_config,
                        service_name=service_name,
                        validate=validate)

        tracer = config.initialize_tracer()
        tracer_interceptor = open_tracing_server_interceptor(
            tracer,
            log_payloads=plugin_config.TRACING_LOG_PAYLOAD,
            span_decorator=span_decorator)
        jaeger_logger = logging.getLogger('jaeger_tracing')
        jaeger_logger.setLevel(logging.ERROR)

        return Tracer(tracer, tracer_interceptor, intercept_server)


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(PLUGIN_NAME, app.plugin_package_name))
    app.on_plugin_setup(JaegerFactory)
