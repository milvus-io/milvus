class MilvusClient:
    def __init__(self, host='localhost', port='19530', timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        # Initialize Milvus client here
        self.client = self.connect_to_milvus()

    def connect_to_milvus(self):
        # Logic to connect to Milvus
        pass

    def load(self, collection_name):
        # Load the collection with optional timeout
        if self.timeout is not None:
            # Use the timeout in the load operation
            self.client.load(collection_name, timeout=self.timeout)
        else:
            self.client.load(collection_name)

    def insert(self, collection_name, entities):
        # Insert entities with optional timeout
        if self.timeout is not None:
            # Use the timeout in the insert operation
            self.client.insert(collection_name, entities, timeout=self.timeout)
        else:
            self.client.insert(collection_name, entities)

    def search(self, collection_name, query, limit=10):
        # Search with optional timeout
        if self.timeout is not None:
            return self.client.search(collection_name, query, limit, timeout=self.timeout)
        else:
            return self.client.search(collection_name, query, limit)

milvus_client = MilvusClient(timeout=5.0)
milvus_client.load('my_collection')
results = milvus_client.search('my_collection', query_vector)