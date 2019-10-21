import logging
from mishards import settings
logger = logging.getLogger()

from mishards.db_base import DB
db = DB()

from mishards.server import Server
grpc_server = Server()


def create_app(testing_config=None):
    config = testing_config if testing_config else settings.DefaultConfig
    db.init_db(uri=config.SQLALCHEMY_DATABASE_URI, echo=config.SQL_ECHO)

    from mishards.connections import ConnectionMgr
    connect_mgr = ConnectionMgr()

    from sd import ProviderManager

    sd_proiver_class = ProviderManager.get_provider(settings.SD_PROVIDER)
    discover = sd_proiver_class(settings=settings.SD_PROVIDER_SETTINGS, conn_mgr=connect_mgr)

    from tracing.factory import TracerFactory
    from mishards.grpc_utils import GrpcSpanDecorator
    tracer = TracerFactory.new_tracer(config.TRACING_TYPE, settings.TracingConfig,
                                      span_decorator=GrpcSpanDecorator())

    from mishards.routings import RouterFactory
    router = RouterFactory.new_router(config.ROUTER_CLASS_NAME, connect_mgr)

    grpc_server.init_app(conn_mgr=connect_mgr, tracer=tracer, router=router, discover=discover)

    from mishards import exception_handlers

    return grpc_server
