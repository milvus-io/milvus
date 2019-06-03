class Status(object):
    """
    :attribute code : int (optional) default as ok
    :attribute message : str (optional) current status message
    """
    OK = 0
    INVALID = 1
    UNKNOWN_ERROR = 2
    NOT_SUPPORTED = 3
    NOT_CONNECTED = 4

    CONNECT_FAILED = 5
    PERMISSION_DENIED = 6
    TABLE_NOT_EXISTS = 7
    PARTITION_NOT_EXIST = 8
    ILLEGAL_ARGUMENT = 9
    ILLEGAL_RANGE = 10
    ILLEGAL_DIMENSION = 11

    def __init__(self, code=OK, message=None):
        self.message = message
        self.code = code

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

