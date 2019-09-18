from mishards import settings

from mishards.db_base import DB
db = DB()
db.init_db(uri=settings.SQLALCHEMY_DATABASE_URI, echo=settings.SQL_ECHO)

from mishards.connections import ConnectionMgr
connect_mgr = ConnectionMgr()

from mishards.service_founder import ServiceFounder
discover = ServiceFounder(namespace=settings.SD_NAMESPACE,
        conn_mgr=connect_mgr,
        pod_patt=settings.SD_ROSERVER_POD_PATT,
        label_selector=settings.SD_LABEL_SELECTOR,
        in_cluster=settings.SD_IN_CLUSTER,
        poll_interval=settings.SD_POLL_INTERVAL)

from mishards.server import Server
grpc_server = Server(conn_mgr=connect_mgr)
