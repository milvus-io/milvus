import os, shutil

class GroupHandler(object):

    @staticmethod
    def CreateGroupDirectory(group_id):
        path = GetGroupDirectory(group_id)
        path = path.strip()
        path=path.rstrip("\\")
        if not os.path.exists():
            os.makedirs(path)


    @staticmethod
    def DeleteGroupDirectory(group_id):
        path = GetGroupDirectory(group_id)
        path = path.strip()
        path=path.rstrip("\\")
        if os.path.exists():
            shutil.rmtree(path)

    @staticmethod
    def GetGroupDirectory(group_id):
        return DATABASE_DIRECTORY + '/' + group_id