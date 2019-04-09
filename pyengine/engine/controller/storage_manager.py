import os, shutil
from engine.settings import DATABASE_DIRECTORY

class StorageManager(object):
    @staticmethod
    def AddGroup(group_name):
        path = StorageManager.GetGroupDirectory(group_name)
        path = path.strip()
        path=path.rstrip("\\")
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def GetGroupDirectory(group_name):
        return DATABASE_DIRECTORY + '/' + group_name

    @staticmethod
    def DeleteGroup(group_id):
        path = StorageManager.GetGroupDirectory(group_id)
        path = path.strip()
        path=path.rstrip("\\")
        if os.path.exists(path):
            shutil.rmtree(path)

    def Read():
        pass

    def Write():
        pass
