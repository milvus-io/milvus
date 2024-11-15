import gevent.monkey
gevent.monkey.patch_all()
import grpc.experimental.gevent as grpc_gevent
grpc_gevent.init_gevent()


from locust import User, events, task, constant_throughput, tag, LoadTestShape
from locust.runners import MasterRunner, WorkerRunner
from pymilvus import (connections, Collection, FieldSchema, CollectionSchema, DataType, FunctionType,
                      Function)
import numpy as np
import time
import logging
from faker import Faker
import pandas as pd
faker = Faker()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


test_data = pd.read_parquet("/dataset/wikipedia-22-12-en-text/test.parquet")


def setup_collection(environment):
    """在master上执行collection的初始化"""
    logger.info("Setting up collection in master...")

    # 获取配置参数
    collection_name = environment.parsed_options.milvus_collection
    connections.connect(uri=environment.host)
    analyzer_params = {
        "type": "english",
    }
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=25536,
                    enable_analyzer=True, analyzer_params=analyzer_params, enable_match=True),
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
        self.collection_name = environment.parsed_options.milvus_collection

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
        return 0.1


class MilvusUser(MilvusBaseUser):
    """Main Milvus user class that defines the test tasks"""

    @tag('insert')
    @task(1)
    def insert(self):
        """Insert random vectors"""
        batch_size = 1000
        data = [
            {
                "id": int(time.time()*(10**6)),
                "text": faker.text(max_nb_chars=300),
            }
            for _ in range(batch_size)
        ]

        self.client.insert(data)

    @tag('fts')
    @task(1)
    def full_text_search(self):
        """full text search"""
        search_data = [test_data.sample(1)["text"].values[0]]
        logger.debug("Performing vector search")
        self.client.search(data=search_data,
                           anns_field="sparse",
                           top_k=self.top_k)

    @tag('text_match')
    @task(1)
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

class StagesShape(LoadTestShape):
    """
    A simple load test shape class that has different user and spawn_rate at
    different stages.
    Keyword arguments:
        stages -- A list of dicts, each representing a stage with the following keys:
            duration -- When this many seconds pass the test is advanced to the next stage
            users -- Total user count
            spawn_rate -- Number of users to start/stop per second
            stop -- A boolean that can stop that test at a specific stage
        stop_at_end -- Can be set to stop once all stages have run.
    """

    stages = [
        {"duration": 60, "users": 1, "spawn_rate": 1},
        {"duration": 120, "users": 10, "spawn_rate": 50},
        {"duration": 180, "users": 50, "spawn_rate": 50},
        {"duration": 240, "users": 100, "spawn_rate": 50},
        {"duration": 300, "users": 150, "spawn_rate": 50},
        {"duration": 360, "users": 200, "spawn_rate": 50, "stop": True},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None


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

    def search(self, data, anns_field, top_k, param=None, output_fields=None):
        if param is None:
            param = {}
        if output_fields is None:
            output_fields = ["id"]
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
                name="Search",
                response_time=total_time,
                response_length=0,
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
    milvus_options.add_argument("--milvus-collection", type=str, default="test_full_text_search_perf",
                                help="Collection name to use (default: %(default)s)")

