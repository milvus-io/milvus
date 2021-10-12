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
