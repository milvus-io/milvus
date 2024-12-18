from enum import Enum
from pymilvus import ExceptionsMessage


class ErrorCode(Enum):
    ErrorOk = 0
    Error = 1


ErrorMessage = {ErrorCode.ErrorOk: "",
                ErrorCode.Error: "is illegal"}


class ErrorMap:
    def __init__(self, err_code, err_msg):
        self.err_code = err_code
        self.err_msg = err_msg


class ConnectionErrorMessage(ExceptionsMessage):
    FailConnect = "Fail connecting to server on %s:%s. Timeout"
    ConnectExist = "The connection named %s already creating, but passed parameters don't match the configured parameters"


class CollectionErrorMessage(ExceptionsMessage):
    CollNotLoaded = "collection %s was not loaded into memory"


class PartitionErrorMessage(ExceptionsMessage):
    pass


class IndexErrorMessage(ExceptionsMessage):
    WrongFieldName = "cannot create index on non-vector field: %s"
    DropLoadedIndex = "index cannot be dropped, collection is loaded, please release it first"
    CheckVectorIndex = "data type {0} can't build with this index {1}"
    SparseFloatVectorMetricType = "only IP&BM25 is the supported metric type for sparse index"
    VectorMetricTypeExist = "metric type not set for vector index"
    # please update the msg below as #37543 fixed
    CheckBitmapIndex = "bitmap index are only supported on bool, int, string"
    CheckBitmapOnPK = "create bitmap index on primary key not supported"
    CheckBitmapCardinality = "failed to check bitmap cardinality limit, should be larger than 0 and smaller than 1000"
    NotConfigable = "{0} is not a configable index proptery"
    InvalidOffsetCache = "invalid offset cache index params"
    OneIndexPerField = "at most one distinct index is allowed per field"
    AlterOnLoadedCollection = "can't alter index on loaded collection, please release the collection first"


class QueryErrorMessage(ExceptionsMessage):
    ParseExpressionFailed = "failed to create query plan: cannot parse expression: "
