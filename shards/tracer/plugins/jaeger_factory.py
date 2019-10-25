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
    def create(cls, tracer_config, span_decorator=None, **kwargs):
        tracing_config = tracer_config.TRACING_CONFIG
        service_name = tracer_config.TRACING_SERVICE_NAME
        validate = tracer_config.TRACING_VALIDATE
        config = Config(config=tracing_config,
                        service_name=service_name,
                        validate=validate)

        tracer = config.initialize_tracer()
        tracer_interceptor = open_tracing_server_interceptor(
            tracer,
            log_payloads=tracer_config.TRACING_LOG_PAYLOAD,
            span_decorator=span_decorator)

        return Tracer(tracer, tracer_interceptor, intercept_server)


def setup(app):
    logger.info('Plugin \'{}\' Installed In Package: {}'.format(PLUGIN_NAME, app.plugin_package_name))
    app.on_plugin_setup(JaegerFactory)
