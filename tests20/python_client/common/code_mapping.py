from enum import Enum


class ErrorCode(Enum):
    ErrorOk = 0
    Error = 1


ErrorMessage = {ErrorCode.ErrorOk: "",
                ErrorCode.Error: "is illegal"}


class ConnectionErrorMessage:
    NoHostPort = "connection configuration must contain 'host' and 'port'"
    HostType = "Type of 'host' must be str!"
    PortType = "Type of port type must be str or int!"
    NotHostPort = "Connection configuration must be contained host and port"
    AliasExist = "alias of '%s' already creating connections, but the configure is not the same as passed in."
    AliasNotExist = "You need to pass in the configuration of the connection named '%s'"
    FailConnect = "Fail connecting to server on %s:%s. Timeout"
    ConnectExist = "The connection named %s already creating, but passed parameters don't match the configured parameters"
