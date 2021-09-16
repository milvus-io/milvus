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

CHAOS_NAMESPACE = "chaos-testing"
CHAOS_API_VERSION = 'chaos-mesh.org/v1alpha1'
CHAOS_GROUP = 'chaos-mesh.org'
CHAOS_VERSION = 'v1alpha1'
SUCC = 'succ'
FAIL = 'fail'
DELTA_PER_INS = 10
ENTITIES_FOR_SEARCH = 1000

CHAOS_CONFIG_ENV = 'CHAOS_CONFIG_PATH'      # env variables for chao path
TESTS_CONFIG_LOCATION = 'chaos_objects/'    # path to the chaos CRD
ALL_CHAOS_YAMLS = 'chaos_datanode*.yaml'    # chaos file name(s) to be run against
WAIT_PER_OP = 10                            # time to wait in seconds between operations
DEFAULT_INDEX_PARAM = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
