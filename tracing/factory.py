import logging
from jaeger_client import Config
from grpc_opentracing.grpcext import intercept_server
from grpc_opentracing import open_tracing_server_interceptor

from tracing import (Tracer,
                     empty_server_interceptor_decorator)

logger = logging.getLogger(__name__)


class TracerFactory:
    @classmethod
    def new_tracer(cls, tracer_type, tracer_config, span_decorator=None, **kwargs):
        config = tracer_config.TRACING_CONFIG
        service_name = tracer_config.TRACING_SERVICE_NAME
        validate=tracer_config.TRACING_VALIDATE
        if not tracer_type:
            tracer_type = 'jaeger'
            config = tracer_config.DEFAULT_TRACING_CONFIG

        if tracer_type.lower() == 'jaeger':
            config = Config(config=config,
                            service_name=service_name,
                            validate=validate
                            )

            tracer = config.initialize_tracer()
            tracer_interceptor = open_tracing_server_interceptor(tracer,
                                                                 log_payloads=tracer_config.TRACING_LOG_PAYLOAD,
                                                                 span_decorator=span_decorator)

            return Tracer(tracer, tracer_interceptor, intercept_server)

        assert False, 'Unsupported tracer type: {}'.format(tracer_type)
