from enum import Enum
from pymilvus_orm.exceptions import ExceptionsMessage


class ErrorCode(Enum):
    ErrorOk = 0
    Error = 1


ErrorMessage = {ErrorCode.ErrorOk: "",
                ErrorCode.Error: "is illegal"}


class ConnectionErrorMessage(ExceptionsMessage):
    FailConnect = "Fail connecting to server on %s:%s. Timeout"
    ConnectExist = "The connection named %s already creating, but passed parameters don't match the configured parameters"
