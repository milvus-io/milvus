MONGO_SERVER = 'mongodb://192.168.1.234:27017/'
# MONGO_SERVER = 'mongodb://mongodb.test:27017/'

SCHEDULER_DB = "scheduler"
JOB_COLLECTION = "jobs"

REGISTRY_URL = "registry.zilliz.com/milvus/milvus"
IDC_NAS_URL = "//172.16.70.249/test"
DEFAULT_IMAGE = "milvusdb/milvus:latest"

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
SERVER_VERSION = "2.0.0-RC7"
DEFUALT_DEPLOY_MODE = "single"


HELM_NAMESPACE = "milvus"
BRANCH = "master"

DEFAULT_CPUS = 48

RAW_DATA_DIR = "/test/milvus/raw_data/"

# nars log
LOG_PATH = "/test/milvus/benchmark/logs/{}/".format(BRANCH)

DEFAULT_DEPLOY_MODE = "single"
SINGLE_DEPLOY_MODE = "single"
CLUSTER_DEPLOY_MODE = "cluster"
CLUSTER_3RD_DEPLOY_MODE = "cluster_3rd"


NAMESPACE = "milvus"
CHAOS_NAMESPACE = "chaos-testing"
DEFAULT_API_VERSION = 'chaos-mesh.org/v1alpha1'
DEFAULT_GROUP = 'chaos-mesh.org'
DEFAULT_VERSION = 'v1alpha1'

# minio config
MINIO_HOST = "milvus-test-minio.qa-milvus.svc.cluster.local"
MINIO_PORT = 9000
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "test"