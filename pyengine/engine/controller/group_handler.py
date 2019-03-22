import os, shutil
from engine.settings import DATABASE_DIRECTORY

class GroupHandler(object):

    @staticmethod
    def CreateGroupDirectory(group_id):
        path = GroupHandler.GetGroupDirectory(group_id)
        path = path.strip()
        path=path.rstrip("\\")
        if not os.path.exists(path):
            os.makedirs(path)
        print("CreateGroupDirectory, Path: ", path)


    @staticmethod
    def DeleteGroupDirectory(group_id):
        path = GroupHandler.GetGroupDirectory(group_id)
        path = path.strip()
        path=path.rstrip("\\")
        if os.path.exists(path):
            shutil.rmtree(path)
        print("DeleteGroupDirectory, Path: ", path)

    @staticmethod
    def GetGroupDirectory(group_id):
        print("GetGroupDirectory, Path: ", DATABASE_DIRECTORY + '/' + group_id)
        return DATABASE_DIRECTORY + '/' + group_id

