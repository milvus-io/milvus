import exception_codes as codes

class BaseException(Exception):
    code = codes.INVALID_CODE
    message = 'BaseException'
    def __init__(self, message=''):
        self.message = self.__class__.__name__ if not message else message

class ConnectionConnectError(BaseException):
    code = codes.CONNECT_ERROR_CODE

class ConnectionNotFoundError(BaseException):
    code = codes.CONNECTTION_NOT_FOUND_CODE
