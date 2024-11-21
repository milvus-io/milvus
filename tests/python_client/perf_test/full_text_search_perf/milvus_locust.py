import gevent.monkey
gevent.monkey.patch_all()
import grpc.experimental.gevent as grpc_gevent
grpc_gevent.init_gevent()


from locust import User, events, task, constant_throughput, tag
from locust.runners import MasterRunner, WorkerRunner
from pymilvus import (connections, Collection, FieldSchema, CollectionSchema, DataType, FunctionType,
                      Function)
import numpy as np
import time
import logging
from faker import Faker
faker = Faker()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def setup_collection(environment):
    """在master上执行collection的初始化"""
    logger.info("Setting up collection in master...")

    # 获取配置参数
    collection_name = environment.parsed_options.milvus_collection
    connections.connect(uri=environment.host)
    analyzer_params = {
        "type": "standard"
    }
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=25536,
                    enable_analyzer=True, analyzer_params=analyzer_params, enable_match=True),
        FieldSchema(name="dense_emb", dtype=DataType.FLOAT_VECTOR, dim=environment.parsed_options.milvus_dim),
        FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
    ]
    schema = CollectionSchema(fields=fields, description="beir test collection")
    bm25_function = Function(
        name="text_bm25_emb",
        function_type=FunctionType.BM25,
        input_field_names=["text"],
        output_field_names=["sparse"],
        params={},
    )
    schema.add_function(bm25_function)
    collection = Collection(collection_name, schema)

    # 创建索引
    collection.create_index(
        "sparse",
        {
            "index_type": "SPARSE_INVERTED_INDEX",
            "metric_type": "BM25",
            "params": {
                "bm25_k1": 1.5,
                "bm25_b": 0.75,
            }
        }
    )
    collection.create_index(
        "dense_emb",
        {
            "index_type": "HNSW",
            "metric_type": "COSINE"
        }
    )
    collection.load()
    logger.info("Collection setup completed successfully")


@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    """初始化事件监听器，仅在master上执行setup"""
    if isinstance(environment.runner, MasterRunner):
        logger.info("Initializing in master...")
        setup_collection(environment)

    # 为所有runner添加setup状态标记
    environment.setup_completed = False


@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    """测试开始时的处理"""
    if isinstance(environment.runner, MasterRunner):
        logger.info("Test starting in master...")
        # Master已经完成setup
        environment.setup_completed = True
        logger.info(f"environment.setup_completed: {environment.setup_completed}")
        logger.info(" master setup confirmed completed")
    elif isinstance(environment.runner, WorkerRunner):
        logger.info("Test starting in worker...")
        # Worker等待master完成setup
        wait_for_setup(environment)
        logger.info("worker setup confirmed completed")


def wait_for_setup(environment):
    """等待setup完成"""
    timeout = 30  #
    connections.connect(uri=environment.host)
    collection = Collection(environment.parsed_options.milvus_collection)
    is_loaded = len(collection.get_replicas().groups) > 0
    start_time = time.time()

    while not is_loaded:
        if time.time() - start_time > timeout:
            raise Exception("Timeout waiting for collection setup")
        time.sleep(1)
        is_loaded = len(collection.get_replicas().groups) > 0
    logger.info("Setup confirmed completed")


class MilvusBaseUser(User):
    """Base Milvus user class that handles common functionality"""
    abstract = True

    def __init__(self, environment):
        super().__init__(environment)
        logger.debug("Initializing MilvusBaseUser")

        # Configuration from command line args
        self.top_k = environment.parsed_options.milvus_topk
        self.mode = environment.parsed_options.milvus_mode
        self.dim = environment.parsed_options.milvus_dim
        self.collection_name = environment.parsed_options.milvus_collection
        self.nlist = environment.parsed_options.milvus_nlist
        self.target_throughput = environment.parsed_options.milvus_throughput_per_user

        # These will be initialized in on_start
        self.client = None
        self.collection = None

    def on_start(self):
        """Called when a User starts running"""
        logger.debug("Starting MilvusBaseUser setup")
        connections.connect(uri=self.environment.host)
        self._init_client()

    def _init_client(self):
        """Initialize the appropriate client based on mode"""
        logger.debug(f"Initializing client in mode: {self.mode}")
        # if self.mode == "rest":
        #     self.client = MilvusRestClient(self.environment)
        # elif self.mode == "orm":
        self.client = MilvusORMClient(self.environment)
        # else:
        #     self.client = MilvusClientWrapper(self.environment)
        self.collection = Collection(self.collection_name)

    def _random_vector(self):
        return np.random.random([self.dim]).tolist()

    def wait_time(self):
        if self.target_throughput > 0:
            return constant_throughput(self.target_throughput)(self)
        return 0.1


class MilvusUser(MilvusBaseUser):
    """Main Milvus user class that defines the test tasks"""

    @tag('insert')
    @task(4)
    def insert(self):
        """Insert random vectors"""
        batch_size = 1000
        data = [
            {
                "id": int(time.time()*(10**6)),
                "text": faker.text(max_nb_chars=300),
                "dense_emb": self._random_vector(),
            }
            for _ in range(batch_size)
        ]

        self.client.insert(data)

    @tag('sparse', 'search')
    @task(4)
    def full_text_search(self):
        """full text search"""
        search_data = [faker.text(max_nb_chars=300)]
        logger.debug("Performing sparse vector search")
        self.client.search(data=search_data,
                           anns_field="sparse",
                           top_k=self.top_k,
                           search_type="full-text-search"
                           )

    @tag('dense', 'search')
    @task(4)
    def dense_search(self):
        """full text search"""
        logger.debug("Performing dense vector search")
        search_data = [self._random_vector()]
        self.client.search(data=search_data,
                           anns_field="dense_emb",
                           top_k=self.top_k,
                           search_type="dense-search")

    @tag('text_match', 'query')
    @task(2)
    def text_match(self):
        """Text Match"""
        search_data = faker.sentence()
        expr = f"TEXT_MATCH(text, '{search_data}')"
        logger.debug("Performing query")
        self.client.query(expr=expr)

    @tag('delete')
    @task(1)
    def delete(self):
        """delete random vectors in 30s window before 10 minutes"""
        _min = int((time.time()-600)*(10**6))
        _max = int((time.time()-530)*(10**6))

        expr = f"id >= {_min} and id <= {_max}"

        self.client.delete(expr)


class MilvusORMClient:
    """Wrapper for Milvus ORM"""

    def __init__(self, environment):
        logger.debug("Initializing MilvusORMClient")
        self.request_type = "ORM"
        self.collection_name = environment.parsed_options.milvus_collection
        self.collection = Collection(self.collection_name)
        self.sleep_time = 1

    def insert(self, data):
        start = time.time()
        try:
            self.collection.insert(data)
            total_time = (time.time() - start) * 1000
            events.request.fire(
                request_type=self.request_type,
                name="Insert",
                response_time=total_time,
                response_length=0,
                exception=None
            )
            self.sleep_time = 0.1
        except Exception as e:
            if "memory" in str(e) or "deny" in str(e) or "limit" in str(e):
                time.sleep(self.sleep_time)
                self.sleep_time *= 2
            else:
                events.request.fire(
                    request_type=self.request_type,
                    name="Insert",
                    response_time=(time.time() - start) * 1000,
                    response_length=0,
                    exception=e
                )

    def search(self, data, anns_field, top_k, param=None, output_fields=None, search_type="dense-search"):
        if param is None:
            param = {}
        if output_fields is None:
            output_fields = ["id"]
        name = search_type
        start = time.time()
        try:
            res = self.collection.search(
                data=data,
                anns_field=anns_field,
                param=param,
                limit=top_k,
                output_fields=output_fields
            )
            total_time = (time.time() - start) * 1000
            for r in res:
                logger.debug(f"Search result: {r}")
                if len(r) == 0:
                    logger.warning("No results found")
                    raise Exception("Empty results")
            events.request.fire(
                request_type=self.request_type,
                name=name,
                response_time=total_time,
                response_length=0,
                exception=None
            )
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            events.request.fire(
                request_type=self.request_type,
                name=name,
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=e
            )

    def query(self, expr, output_fields=None):
        if output_fields is None:
            output_fields = ["id"]
        start = time.time()
        try:
            res = self.collection.query(
                expr=expr,
                output_fields=output_fields
            )
            total_time = (time.time() - start) * 1000
            if len(res) == 0:
                logger.warning("No results found")
                raise Exception("Empty results")
            events.request.fire(
                request_type=self.request_type,
                name="Query",
                response_time=total_time,
                response_length=0,
                exception=None
            )
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            events.request.fire(
                request_type=self.request_type,
                name="Query",
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=e
            )

    def delete(self, expr):
        start = time.time()
        try:
            self.collection.delete(expr)
            total_time = (time.time() - start) * 1000
            events.request.fire(
                request_type=self.request_type,
                name="Delete",
                response_time=total_time,
                response_length=0,
                exception=None
            )
        except Exception as e:
            logger.error(f"Delete error: {str(e)}")
            events.request.fire(
                request_type=self.request_type,
                name="Delete",
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=e
            )


# 命令行参数配置
@events.init_command_line_parser.add_listener
def _(parser):
    milvus_options = parser.add_argument_group("Milvus-specific options")
    milvus_options.add_argument("--milvus-topk", type=int, metavar="<int>", default=10,
                                help="Number of results to return from a Milvus search request. Defaults to 10.")
    milvus_options.add_argument("--milvus-mode", choices=["rest", "orm", "client"],
                                default="orm",
                                help="How to connect to Milvus (default: %(default)s)")
    milvus_options.add_argument("--milvus-dim", type=int, default=128,
                                help="Vector dimension for the collection (default: %(default)s)")
    milvus_options.add_argument("--milvus-collection", type=str, default="test_collection",
                                help="Collection name to use (default: %(default)s)")
    milvus_options.add_argument("--milvus-nlist", type=int, default=1024,
                                help="Number of cluster units (default: %(default)s)")
    milvus_options.add_argument("--milvus-throughput-per-user", type=float, default=0,
                                help="How many requests per second each user should issue (default: %(default)s)")
