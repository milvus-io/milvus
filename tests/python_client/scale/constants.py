# scale object
IMAGE_REPOSITORY = "registry.milvus.io/milvus/milvus"  # repository of milvus image
IMAGE_TAG = "master-20211213-d14fff0"  # tag of milvus image
NAMESPACE = "chaos-testing"  # namespace
IF_NOT_PRESENT = "IfNotPresent"  # image pullPolicy IfNotPresent
ALWAYS = "Always"  # image pullPolicy Always

MILVUS_LOGS_PATH = '/tmp/milvus'  # path of milvus pod logs

# default scale config
DEFAULT_RELEASE_PREFIX = "scale"
