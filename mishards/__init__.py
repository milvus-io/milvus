import settings
from connections import ConnectionMgr
connect_mgr = ConnectionMgr()

from server import Server
grpc_server = Server(conn_mgr=connect_mgr)
