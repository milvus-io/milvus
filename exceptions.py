import exception_codes as codes

class BaseException(Exception):
    code = codes.INVALID_CODE
    message = 'BaseException'
    def __init__(self, message='', code=None):
        self.message = self.__class__.__name__ if not message else message
        self.code = self.code if code is None else code

class ConnectionConnectError(BaseException):
    code = codes.CONNECT_ERROR_CODE
