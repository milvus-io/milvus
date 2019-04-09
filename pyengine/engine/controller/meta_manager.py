from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable
from engine.controller.error_code import ErrorCode

class MetaManager(object):

    def Sync(self):
        db.session.commit()

    def AddGroup(self, group_name, dimension):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            return ErrorCode.ALREADY_EXIST, group_name, group_filenumber
        else:
            new_group = GroupTable(group_name, dimension)
            GroupHandler.CreateGroupDirectory(group_id)

            # add into database
            db.session.add(new_group)
            return ErrorCode.SUCCESS_CODE, group_name, 0

    @staticmethod
    def GetGroup(group_name):
        group = GroupTable.query.filter(GroupTable.group_name==group_name).first()
        if group:
            return ErrorCode.SUCCESS_CODE, group
        else:
            return ErrorCode.FAULT_CODE, None

    # def DeleteGroup(group_id):
    #     group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
    #     if(group):
    #         db.session.delete(group)
    #     else: