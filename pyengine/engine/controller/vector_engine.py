from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable
from engine.controller.raw_file_handler import RawFileHandler
from engine.controller.group_handler import GroupHandler
from engine.controller.index_file_handler import IndexFileHandler
from engine.settings import ROW_LIMIT
from flask import jsonify
from engine import db
from engine.ingestion import build_index
from engine.controller.scheduler import Scheduler
import sys, os

class VectorEngine(object):
    group_dict = None

    @staticmethod
    def AddGroup(group_id, dimension):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            print('Already create the group: ', group_id)
            return jsonify({'code': 1, 'group_name': group_id, 'file_number': group.file_number})
        else:
            print('To create the group: ', group_id)
            new_group = GroupTable(group_id, dimension)
            GroupHandler.CreateGroupDirectory(group_id)

            # add into database
            db.session.add(new_group)
            db.session.commit()
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': 0})


    @staticmethod
    def GetGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            print('Found the group: ', group_id)
            return jsonify({'code': 0, 'group_name': group_id, 'file_number': group.file_number})
        else:
            print('Not found the group: ', group_id)
            return jsonify({'code': 1, 'group_name': group_id, 'file_number': 0}) # not found


    @staticmethod
    def DeleteGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if(group):
            # old_group = GroupTable(group_id)
            db.session.delete(group)
            db.session.commit()
            GroupHandler.DeleteGroupDirectory(group_id)

            records = FileTable.query.filter(FileTable.group_name == group_id).all()
            for record in records:
                print("record.group_name: ", record.group_name)
                db.session.delete(record)
            db.session.commit()

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
        file = FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').first()
        group = GroupTable.query.filter(GroupTable.group_name == group_id).first()
        if file:
            print('insert into exist file')
            # insert into raw file
            VectorEngine.InsertVectorIntoRawFile(group_id, file.filename, vector)

            # check if the file can be indexed
            if file.row_number + 1 >= ROW_LIMIT:
                raw_data = GetVectorListFromRawFile(group_id)
                d = group.dimension

                # create index
                index_builder = build_index.FactoryIndex()
                index = index_builder().build(d, raw_data)

                # TODO(jinhai): store index into Cache
                index_filename = file.filename + '_index'

                # TODO(jinhai): Update raw_file_name => index_file_name
                FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').update({'row_number':file.row_number + 1, 'type': 'index'})
                pass

            else:
                # we still can insert into exist raw file, update database
                FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').update({'row_number':file.row_number + 1})
                db.session.commit()
                print('Update db for raw file insertion')
                pass

        else:
            print('add a new raw file')
            # first raw file
            raw_filename = group_id + '.raw'
            # create and insert vector into raw file
            VectorEngine.InsertVectorIntoRawFile(group_id, raw_filename, vector)
            # insert a record into database
            db.session.add(FileTable(group_id, raw_filename, 'raw', 1))
            db.session.commit()

        return jsonify({'code': 0})


    @staticmethod
    def SearchVector(group_id, vectors, limit):
        # find all files
        files = FileTable.query.filter(FileTable.group_name == group_id).all()

        raw_keys = [ i.filename for i in files if i.type == 'raw' ]
        index_keys = [ i.filename for i in files if i.type == 'index' ]
        index_map = dict
        index_map['raw'] = raw_keys
        index_map['index'] = index_keys # {raw:[key1, key2], index:[key3, key4]}

        scheduler_instance = Scheduler
        result = scheduler_instance.Search(index_map, vectors, k=limit)

        # according to difference files get topk of each
        # reduce the topk from them
        # construct response and send back
        return jsonify({'code': 0})


    # TODO(linxj): Debug Interface. UnSopport now
    # @staticmethod
    # def CreateIndex(group_id):
    #     # create index
    #     file = FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').first()
    #     path = GroupHandler.GetGroupDirectory(group_id) + '/' + file.filename
    #     print('Going to create index for: ', path)
    #     return jsonify({'code': 0})


    @staticmethod
    def InsertVectorIntoRawFile(group_id, filename, vector):
        # print(sys._getframe().f_code.co_name, group_id, vector)
        # path = GroupHandler.GetGroupDirectory(group_id) + '/' + filename
        if VectorEngine.group_dict is None:
            # print("VectorEngine.group_dict is None")
            VectorEngine.group_dict = dict()
            VectorEngine.group_dict[group_id] = []

        VectorEngine.group_dict[group_id].append(vector)

        print('InsertVectorIntoRawFile: ', VectorEngine.group_dict[group_id])

        # if filename exist
        # append
        # if filename not exist
        # create file
        # append
        return filename


    @staticmethod
    def GetVectorListFromRawFile(group_id, filename="todo"):
        return VectorEngine.group_dict[group_id]

