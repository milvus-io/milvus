from mishards import exceptions


class RouterMixin:
    def __init__(self, writable_topo, readonly_topo):
        self.writable_topo = writable_topo
        self.readonly_topo = readonly_topo

    def routing(self, collection_name, metadata=None, **kwargs):
        raise NotImplemented()

    def connection(self, metadata=None):
        conn = self.writable_topo.get_group('default').get('WOSERVER').fetch()
        if conn:
            conn.on_connect(metadata=metadata)
        # PXU TODO: should return conn
        return conn.conn

    def query_conn(self, name, metadata=None):
        conn = self.readonly_topo.get_group(name).get(name).fetch()
        if not conn:
            raise exceptions.ConnectionNotFoundError(name, metadata=metadata)
        conn.on_connect(metadata=metadata)
        return conn
