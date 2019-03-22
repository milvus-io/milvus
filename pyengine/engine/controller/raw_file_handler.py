
class RawFileHandler(object):
    @staticmethod
    def Create(filename, type):
        # type means: csv, parquet
        pass

    @staticmethod
    def Read(filename, type):
        pass

    @staticmethod
    def Append(filename, type, record):
        pass

    @staticmethod
    def GetRawFilename(group_id):
        return group_id + '.raw'