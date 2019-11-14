from mishards import exceptions


class RouterMixin:
    def __init__(self, conn_mgr):
        self.conn_mgr = conn_mgr

    def routing(self, table_name, metadata=None, **kwargs):
        raise NotImplemented()

    def connection(self, metadata=None):
        conn = self.conn_mgr.conn('WOSERVER', metadata=metadata)
        if conn:
            conn.on_connect(metadata=metadata)
        return conn.conn

    def query_conn(self, name, metadata=None):
        conn = self.conn_mgr.conn(name, metadata=metadata)
        if not conn:
            raise exceptions.ConnectionNotFoundError(name, metadata=metadata)
        conn.on_connect(metadata=metadata)
        return conn.conn
