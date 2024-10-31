import gevent.monkey
gevent.monkey.patch_all()
import grpc.experimental.gevent as grpc_gevent
grpc_gevent.init_gevent()


from locust import User, FastHttpUser, events, task, constant_throughput, tag
from locust.runners import MasterRunner, WorkerRunner
from locust.env import Environment
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility, MilvusClient
import numpy as np
import random
import time
import logging
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def setup_collection(environment):
    """在master上执行collection的初始化"""
    logger.info("Setting up collection in master...")

    # 获取配置参数
    collection_name = environment.parsed_options.milvus_collection
    dim = environment.parsed_options.milvus_dim
    nlist = environment.parsed_options.milvus_nlist

    try:
        # 建立连接
        connections.connect(uri=environment.host)

        # 定义 collection schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # 如果collection已存在则删除
        if utility.has_collection(collection_name):
            logger.info(f"Dropping existing collection: {collection_name}")
            utility.drop_collection(collection_name)

        # 创建collection
        logger.info(f"Creating new collection: {collection_name}")
        collection = Collection(name=collection_name, schema=schema)

        # 创建索引
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": nlist}
        }
        logger.info("Creating index...")
        collection.create_index("vector", index_params)
        collection.load()
        logger.info("Collection setup completed successfully")

        # 存储setup状态
        environment.setup_completed = True

    except Exception as e:
        logger.error(f"Failed to setup collection: {str(e)}")
        raise
    finally:
        connections.disconnect("default")


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
    elif isinstance(environment.runner, WorkerRunner):
        logger.info("Test starting in worker...")
        # Worker等待master完成setup
        wait_for_setup(environment)


def wait_for_setup(environment):
    """等待setup完成"""
    timeout = 30  # 30秒超时
    start_time = time.time()
    while not environment.setup_completed:
        if time.time() - start_time > timeout:
            raise Exception("Timeout waiting for collection setup")
        time.sleep(1)
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
        # self._setup_collection()

    def _init_client(self):
        """Initialize the appropriate client based on mode"""
        logger.debug(f"Initializing client in mode: {self.mode}")
        # if self.mode == "rest":
        #     self.client = MilvusRestClient(self.environment)
        # elif self.mode == "orm":
        self.client = MilvusORMClient(self.environment)
        # else:
        #     self.client = MilvusClientWrapper(self.environment)

    def _setup_collection(self):
        """Setup the collection with fields and index"""
        logger.debug(f"Setting up collection: {self.collection_name}")

        # Setup connection first
        connections.connect(uri=self.environment.host)

        # Define fields and schema
        self.fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=self.dim)
        ]
        self.schema = CollectionSchema(fields=self.fields, description="test collection")

        # Create or recreate collection
        if utility.has_collection(self.collection_name):
            logger.debug(f"Dropping existing collection: {self.collection_name}")
            utility.drop_collection(self.collection_name)

        logger.debug(f"Creating new collection: {self.collection_name}")
        self.collection = Collection(name=self.collection_name, schema=self.schema)

        # Create IVF_FLAT index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": self.nlist}
        }
        logger.debug("Creating index")
        self.collection.create_index("vector", index_params)
        self.collection.load()
        logger.debug("Collection setup complete")

    def _random_vector(self):
        return np.random.random([self.dim]).tolist()

    def wait_time(self):
        if self.target_throughput > 0:
            return constant_throughput(self.target_throughput)(self)
        return 0


class MilvusUser(MilvusBaseUser):
    """Main Milvus user class that defines the test tasks"""

    @tag('insert')
    @task(4)
    def insert_vectors(self):
        """Insert random vectors"""
        num_vectors = 100
        vectors = [self._random_vector() for _ in range(num_vectors)]
        ids = [random.randint(0, 1000000) for _ in range(num_vectors)]

        logger.debug(f"Inserting {num_vectors} vectors")
        self.client.insert(vectors=vectors, ids=ids)

    @tag('search')
    @task(2)
    def search_vectors(self):
        """Search for similar vectors"""
        search_vector = self._random_vector()
        logger.debug("Performing vector search")
        self.client.search(vector=search_vector, top_k=self.top_k)


class MilvusORMClient:
    """Wrapper for Milvus ORM"""

    def __init__(self, environment):
        logger.debug("Initializing MilvusORMClient")
        self.request_type = "ORM"
        self.collection_name = environment.parsed_options.milvus_collection

    def insert(self, vectors, ids):
        start = time.time()
        try:
            logger.debug(f"ORM Insert: {len(vectors)} vectors")
            collection = Collection(self.collection_name)
            collection.insert([ids, vectors])
            total_time = (time.time() - start) * 1000
            events.request.fire(
                request_type=self.request_type,
                name="Insert",
                response_time=total_time,
                response_length=len(vectors),
                exception=None
            )
        except Exception as e:
            logger.error(f"Insert error: {str(e)}")
            events.request.fire(
                request_type=self.request_type,
                name="Insert",
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=e
            )

    def search(self, vector, top_k):
        start = time.time()
        try:
            logger.debug(f"ORM Search: top_k={top_k}")
            collection = Collection(self.collection_name)
            result = collection.search(
                data=[vector],
                anns_field="vector",
                param={"metric_type": "L2", "params": {"nprobe": 10}},
                limit=top_k,
                output_fields=["id"]
            )
            total_time = (time.time() - start) * 1000
            events.request.fire(
                request_type=self.request_type,
                name="Search",
                response_time=total_time,
                response_length=len(result[0]),
                exception=None
            )
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            events.request.fire(
                request_type=self.request_type,
                name="Search",
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
