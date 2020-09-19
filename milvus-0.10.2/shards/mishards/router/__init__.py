from mishards import exceptions


class RouterMixin:
    def __init__(self, writable_topo, readonly_topo):
        self.writable_topo = writable_topo
        self.readonly_topo = readonly_topo

    def routing(self, collection_name, metadata=None, **kwargs):
        raise NotImplemented()

    def connection(self, metadata=None):
        # conn = self.writable_topo.get_group('default').get('WOSERVER').fetch()
        conn = self.writable_topo.get_group('default').get('WOSERVER')
        # if conn:
        #     conn.on_connect(metadata=metadata)
        # PXU TODO: should return conn
        return conn
        # return conn.conn

    def query_conn(self, name, metadata=None):
        if not name:
            raise exceptions.ConnectionNotFoundError(
                    message=f'Conn Group is Empty. Please Check your configurations',
                    metadata=metadata)

        group = self.readonly_topo.get_group(name)
        if not group:
            raise exceptions.ConnectionNotFoundError(
                    message=f'Conn Group {name} is Empty. Please Check your configurations',
                    metadata=metadata)
        # conn = group.get(name).fetch()
        # if not conn:
        #     raise exceptions.ConnectionNotFoundError(
        #             message=f'Conn {name} Not Found', metadata=metadata)
        # conn.on_connect(metadata=metadata)

        # conn = self.readonly_topo.get_group(name).get(name).fetch()
        conn = self.readonly_topo.get_group(name).get(name)
        # if not conn:
        #     raise exceptions.ConnectionNotFoundError(name, metadata=metadata)
        # conn.on_connect(metadata=metadata)
        return conn
