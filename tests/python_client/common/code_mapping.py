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
    SparseFloatVectorMetricType = "only IP is the supported metric type for sparse index"
    VectorMetricTypeExist = "metric type not set for vector index"
    CheckBitmapIndex = "bitmap index are only supported on bool, int, string and array field"
    CheckBitmapOnPK = "create bitmap index on primary key not supported"
