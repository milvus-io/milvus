SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
SERVER_VERSION = "2.0"

RAW_DATA_DIR = "/test/milvus/raw_data/"

DEFAULT_DEPLOY_MODE = "single"

CHAOS_NAMESPACE = "chaos-testing"  # namespace of chaos
CHAOS_API_VERSION = 'chaos-mesh.org/v1alpha1'  # chaos mesh api version
CHAOS_GROUP = 'chaos-mesh.org'  # chaos mesh group
CHAOS_VERSION = 'v1alpha1'  # chaos mesh version
SUCC = 'succ'
FAIL = 'fail'
DELTA_PER_INS = 10  # entities per insert
ENTITIES_FOR_SEARCH = 3000  # entities for search_collection
ENTITIES_FOR_BULKINSERT = 1000000  # entities for bulk insert
CHAOS_CONFIG_ENV = 'CHAOS_CONFIG_PATH'  # env variables for chaos path
TESTS_CONFIG_LOCATION = 'chaos_objects/pod_kill/'
ALL_CHAOS_YAMLS = 'chaos_allstandalone_pod_kill.yaml'
RELEASE_NAME = 'test-allstandalone-pod-kill-19-25-26'
WAIT_PER_OP = 10  # time to wait in seconds between operations
CHAOS_DURATION = 120  # chaos duration time in seconds
DEFAULT_INDEX_PARAM = {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 48, "efConstruction": 500}}
DEFAULT_SEARCH_PARAM = {"metric_type": "L2", "params": {"ef": 64}}
CHAOS_INFO_SAVE_PATH = "/tmp/ci_logs/chaos_info.json"
