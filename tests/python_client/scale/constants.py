# scale object
IMAGE_REPOSITORY = "milvusdb/milvus-dev"  # repository of milvus image
IMAGE_TAG = "master-latest"  # tag of milvus image
NAMESPACE = "chaos-testing"  # namespace
IF_NOT_PRESENT = "IfNotPresent"  # image pullPolicy IfNotPresent
ALWAYS = "Always"  # image pullPolicy Always
PROXY = "proxy"  # key proxy
DATA_NODE = "dataNode"  # key dataNode
INDEX_NODE = "indexNode"  # key indexNode
QUERY_NODE = "queryNode"  # key queryNode

# my values.yaml path
MILVUS_CHART_ENV = 'MILVUS_CHART_ENV'  # env of milvus chart path
MILVUS_LOGS_PATH = '/tmp/milvus'  # path of milvus pod logs
