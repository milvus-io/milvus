from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable
from engine.controller.error_code import ErrorCode
from engine import db

class MetaManager(object):

    @staticmethod
    def Sync():
        db.session.commit()

    @staticmethod
    def AddGroup(group_name, dimension):
        new_group = GroupTable(group_name, dimension)

        # add into database
        db.session.add(new_group)

        return ErrorCode.SUCCESS_CODE, group_name

    @staticmethod
    def GetGroup(group_name):
        group = GroupTable.query.filter(GroupTable.group_name==group_name).first()
        if group:
            return ErrorCode.SUCCESS_CODE, group
        else:
            return ErrorCode.FAULT_CODE, None

    @staticmethod
    def GetAllGroup():
        groups = GroupTable.query.all()
        return groups

    @staticmethod
    def DeleteGroup(group):
        db.session.delete(group)

    @staticmethod
    def DeleteGroupFiles(group_name):
        records = FileTable.query.filter(FileTable.group_name == group_name).all()
        for record in records:
            # print("record.group_name: ", record.group_name)
            db.session.delete(record)
    