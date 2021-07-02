MONGO_SERVER = 'mongodb://192.168.1.234:27017/'
# MONGO_SERVER = 'mongodb://mongodb.test:27017/'

SCHEDULER_DB = "scheduler"
JOB_COLLECTION = "jobs"

REGISTRY_URL = "registry.zilliz.com/milvus/milvus"
IDC_NAS_URL = "//172.16.70.249/test"

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
SERVER_VERSION = "2.0"

HELM_NAMESPACE = "milvus"
BRANCH = "master"

DEFAULT_CPUS = 48

RAW_DATA_DIR = "/test/milvus/raw_data/"

# nars log
LOG_PATH = "/test/milvus/benchmark/logs/{}/".format(BRANCH)

DEFAULT_DEPLOY_MODE = "single"

NAMESPACE = "milvus"
CHAOS_NAMESPACE = "chaos-testing"
DEFAULT_API_VERSION = 'chaos-mesh.org/v1alpha1'
DEFAULT_GROUP = 'chaos-mesh.org'
DEFAULT_VERSION = 'v1alpha1'
