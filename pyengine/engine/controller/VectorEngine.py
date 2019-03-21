from engine.model.GroupTable import GroupTable
from engine.model.FileTable import FileTable
from engine.controller.RawFileHandler import RawFileHandler
from engine.controller.GroupHandler import GroupHandler
from flask import jsonify
from engine import db
import sys, os

class VectorEngine(object):

    @staticmethod
    def AddGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            return jsonify({'code': 1, 'group_name': group_id, 'file_number': group.file_number})
        else:
            new_group = GroupTable(group_id)
            db.session.add(new_group)
            db.session.commit()
            GroupHandler.CreateGroupDirectory(group_id)
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': 0})

    @staticmethod
    def GetGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': group.file_number})
        else:
            return jsonify({'code': 1, 'group_name': group_id, 'file_number': 0}) # not found

    
    @staticmethod
    def DeleteGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if(group):
            # old_group = GroupTable(group_id)
            db.session.delete(group)
            db.session.commit()
            GroupHandler.DeleteGroupDirectory(group_id)
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': group.file_number})
        else:
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': 0})

    @staticmethod
    def GetGroupList():
        group = GroupTable.query.all()
        group_list = []
        for group_tuple in group:
            group_item = {}
            group_item['group_name'] = group_tuple.group_name
            group_item['file_number'] = group_tuple.file_number
            group_list.append(group_item)

        print(group_list)
        return jsonify(results = group_list)

    @staticmethod
    def AddVector(group_id, vector):
        print(group_id, vector)
        file = FileTable.query.filter(and_(FileTable.group_name == group_id, FileTable.type == 'raw')).first()
        if file:
            if file.row_number >= ROW_LIMIT:
                # create index
                index_filename = file.filename + "_index"
                CreateIndex(group_id, index_filename)

                # create another raw file
                raw_filename = file.seq_no
                InsertVectorIntoRawFile(group_id, raw_filename, vector)
                # insert a record into database
                db.session.add(FileTable(group_id, raw_filename, 'raw', 1))
                db.session.commit()
            else:
                # we still can insert into exist raw file
                InsertVectorIntoRawFile(file.filename, vector)
                # update database
                # FileTable.query.filter_by(FileTable.group_name == group_id).filter_by(FileTable.type == 'raw').update('row_number':file.row_number + 1)
        else:
            # first raw file
            raw_filename = group_id + '_0'
            # create and insert vector into raw file
            InsertVectorIntoRawFile(raw_filename, vector)
            # insert a record into database
            db.session.add(FileTable(group_id, raw_filename, 'raw', 1))
            db.session.commit()

        return jsonify({'code': 0})
    
    @staticmethod
    def SearchVector(group_id, vector, limit):
        # find all files
        # according to difference files get topk of each
        # reduce the topk from them
        # construct response and send back
        return jsonify({'code': 0})
    
    @staticmethod
    def CreateIndex(group_id, filename):
        path = GroupHandler.GetGroupDirectory(group_id) + '/' + filename 
        print(group_id, path)
        return jsonify({'code': 0})

    @staticmethod
    def InsertVectorIntoRawFile(group_id, filename, vector):
        print(sys._getframe().f_code.co_name)
        path = GroupHandler.GetGroupDirectory(group_id) + '/' + filename 

        # if filename exist
        # append
        # if filename not exist
        # create file
        # append
        return filename

