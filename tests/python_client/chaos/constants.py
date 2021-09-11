# MONGO_SERVER = 'mongodb://192.168.1.234:27017/'
MONGO_SERVER = 'mongodb://mongodb.test:27017/'

SCHEDULER_DB = "scheduler"
JOB_COLLECTION = "jobs"

REGISTRY_URL = "registry.zilliz.com/milvus/milvus"
IDC_NAS_URL = "//172.16.70.249/test"

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
SERVER_VERSION = "2.0"

RAW_DATA_DIR = "/test/milvus/raw_data/"

DEFAULT_DEPLOY_MODE = "single"

NAMESPACE = "chaos-testing"
DEFAULT_API_VERSION = 'chaos-mesh.org/v1alpha1'
DEFAULT_GROUP = 'chaos-mesh.org'
DEFAULT_VERSION = 'v1alpha1'
SUCC = 'succ'
FAIL = 'fail'
DELTA_PER_INS = 10
ENTITIES_FOR_SEARCH = 1000

CHAOS_CONFIG_ENV = 'CHAOS_CONFIG_PATH'      # env variables for chao path
TESTS_CONFIG_LOCATION = 'chaos_objects/'
ALL_CHAOS_YAMLS = 'chaos_datanode*.yaml'
WAIT_PER_OP = 10
DEFAULT_INDEX_PARAM = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
