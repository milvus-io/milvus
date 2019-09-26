from mishards import settings

from mishards.db_base import DB
db = DB()
db.init_db(uri=settings.SQLALCHEMY_DATABASE_URI, echo=settings.SQL_ECHO)

from mishards.connections import ConnectionMgr
connect_mgr = ConnectionMgr()

from sd import ProviderManager

sd_proiver_class = ProviderManager.get_provider(settings.SD_PROVIDER)
discover = sd_proiver_class(settings=settings.SD_PROVIDER_SETTINGS, conn_mgr=connect_mgr)

from tracing.factory import TracerFactory
from grpc_utils import GrpcSpanDecorator
tracer = TracerFactory.new_tracer(settings.TRACING_TYPE, settings.TracingConfig,
        span_decorator=GrpcSpanDecorator())

from mishards.server import Server
grpc_server = Server(conn_mgr=connect_mgr, tracer=tracer)

from mishards import exception_handlers
