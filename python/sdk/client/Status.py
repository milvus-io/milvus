class Status(object):
    """
    :attribute code: int (optional) default as ok

    :attribute message: str (optional) current status message
    """
    SUCCESS = 0
    CONNECT_FAILED = 1
    PERMISSION_DENIED = 2
    TABLE_NOT_EXISTS = 3
    ILLEGAL_ARGUMENT = 4
    ILLEGAL_RANGE = 5
    ILLEGAL_DIMENSION = 6

    def __init__(self, code=SUCCESS, message=None):
        self.code = code
        self.message = message

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        """Make Status comparable with self by code"""
        if isinstance(other, int):
            return self.code == other
        else:
            return isinstance(other, self.__class__) and self.code == other.code

    def __ne__(self, other):
        return not (self == other)

