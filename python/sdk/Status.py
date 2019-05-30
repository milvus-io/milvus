from enum import IntEnum


class Status(IntEnum):

    def __new__(cls, code, message=''):
        obj = int.__new__(cls, code)
        obj._code_ = code

        obj.message = message
        return obj

    def __str__(self):
        return str(self.code)

    # success
    OK = 200, 'OK'

    INVALID = 300, 'Invalid'
    UNKNOWN = 400, 'Unknown error'
    NOT_SUPPORTED = 500, 'Not supported'
