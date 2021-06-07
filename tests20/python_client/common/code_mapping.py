from enum import Enum


class ErrorCode(Enum):
    ErrorOk = 0
    Error = 1


ErrorMessage = {ErrorCode.ErrorOk: "",
                ErrorCode.Error: "is illegal"}
