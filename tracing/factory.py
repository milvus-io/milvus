import logging
from jaeger_client import Config
from grpc_opentracing.grpcext import intercept_server
from grpc_opentracing import open_tracing_server_interceptor

from tracing import Tracer, empty_server_interceptor_decorator

logger = logging.getLogger(__name__)


class TracerFactory:
    @classmethod
    def new_tracer(cls, tracer_type, tracer_config, **kwargs):
        if not tracer_type:
            return Tracer()

        if tracer_type.lower() == 'jaeger':
            config = Config(config={
                    'sampler': {
                        'type': 'const',
                        'param': 1,
                        },
                    'local_agent': {
                        'reporting_host': tracer_config.TRACING_REPORTING_HOST,
                        'reporting_port': tracer_config.TRACING_REPORTING_PORT
                    },
                    'logging': tracer_config.TRACING_LOGGING,
                    },
                    service_name=tracer_config.TRACING_SERVICE_NAME,
                    validate=tracer_config.TRACING_VALIDATE
            )

            tracer = config.initialize_tracer()
            tracer_interceptor = open_tracing_server_interceptor(tracer,
                    log_payloads=tracer_config.TRACING_LOG_PAYLOAD)

            return Tracer(tracer, tracer_interceptor, intercept_server)

        assert False, 'Unsupported tracer type: {}'.format(tracer_type)
