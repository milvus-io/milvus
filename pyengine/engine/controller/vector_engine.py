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
from engine.ingestion import serialize
from engine.controller.meta_manager import MetaManager
from engine.controller.error_code import ErrorCode
from engine.controller.storage_manager import StorageManager
import sys, os

class VectorEngine(object):
    group_vector_dict = None
    group_vector_id_dict = None
    SUCCESS_CODE = 0
    FAULT_CODE = 1
    GROUP_NOT_EXIST = 2

    @staticmethod
    def AddGroup(group_name, dimension):
        error, group = MetaManager.GetGroup(group_name)
        if(error == ErrorCode.SUCCESS_CODE):
            return ErrorCode.FAULT_CODE, group_name
        else:
            StorageManager.CreateGroup(group_name)
            new_group = GroupTable(group_name, dimension)

            # add into database
            db.session.add(new_group)
            db.session.commit()
            return VectorEngine.SUCCESS_CODE, group_name


    @staticmethod
    def GetGroup(group_id):
        group = GroupTable.query.filter(GroupTable.group_name==group_id).first()
        if group:
            return VectorEngine.SUCCESS_CODE, group_id, group.file_number
        else:
            return VectorEngine.FAULT_CODE, group_id, 0


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

            return VectorEngine.SUCCESS_CODE, group_id, group.file_number
        else:
            return VectorEngine.SUCCESS_CODE, group_id, 0


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
        return VectorEngine.SUCCESS_CODE, group_list


    @staticmethod
    def AddVector(group_id, vectors):
        print(group_id, vectors)
        code, _, _ = VectorEngine.GetGroup(group_id)
        if code == VectorEngine.FAULT_CODE:
            return VectorEngine.GROUP_NOT_EXIST, 'invalid'

        vector_str_list = []
        for vector in vectors:
            file = FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').first()
            group = GroupTable.query.filter(GroupTable.group_name == group_id).first()

            if file:
                print('insert into exist file')
                # create vector id
                vector_id = file.seq_no + 1
                # insert into raw file
                VectorEngine.InsertVectorIntoRawFile(group_id, file.filename, vector, vector_id)

                # check if the file can be indexed
                if file.row_number + 1 >= ROW_LIMIT:
                    raw_vector_array, raw_vector_id_array = VectorEngine.GetVectorListFromRawFile(group_id)
                    d = group.dimension

                    # create index
                    index_builder = build_index.FactoryIndex()
                    index = index_builder().build(d, raw_vector_array, raw_vector_id_array)

                    # TODO(jinhai): store index into Cache
                    index_filename = file.filename + '_index'
                    serialize.write_index(file_name=index_filename, index=index)

                    FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').update({'row_number':file.row_number + 1,
                                                                                                                    'type': 'index',
                                                                                                                    'filename': index_filename,
                                                                                                                    'seq_no': file.seq_no + 1})
                    db.session.commit()
                    VectorEngine.group_dict = None
                else:
                    # we still can insert into exist raw file, update database
                    FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').update({'row_number':file.row_number + 1, 
                                                                                                                    'seq_no': file.seq_no + 1})
                    db.session.commit()
                    print('Update db for raw file insertion')

            else:
                print('add a new raw file')
                # first raw file
                raw_filename = group_id + '.raw'
                # create vector id
                vector_id = 0
                # create and insert vector into raw file
                VectorEngine.InsertVectorIntoRawFile(group_id, raw_filename, vector, vector_id)
                # insert a record into database
                db.session.add(FileTable(group_id, raw_filename, 'raw', 1))
                db.session.commit()

            vector_str_list.append(group_id + '.' + str(vector_id))

        return VectorEngine.SUCCESS_CODE, vector_str_list


    @staticmethod
    def SearchVector(group_id, vector, limit):
        # Check the group exist
        code, _, _ = VectorEngine.GetGroup(group_id)
        if code == VectorEngine.FAULT_CODE:
            return VectorEngine.GROUP_NOT_EXIST, {}

        group = GroupTable.query.filter(GroupTable.group_name == group_id).first()
        # find all files
        files = FileTable.query.filter(FileTable.group_name == group_id).all()
        index_keys = [ i.filename for i in files if i.type == 'index' ]
        index_map = {}
        index_map['index'] = index_keys
        index_map['raw'], index_map['raw_id'] = VectorEngine.GetVectorListFromRawFile(group_id, "fakename") #TODO: pass by key, get from storage
        index_map['dimension'] = group.dimension

        scheduler_instance = Scheduler()
        vectors = []
        vectors.append(vector)
        result = scheduler_instance.search(index_map, vectors, limit)

        vector_ids_str = []
        for int_id in result:
            vector_ids_str.append(group_id + '.' + str(int_id))

        return VectorEngine.SUCCESS_CODE, vector_ids_str


    @staticmethod
    def CreateIndex(group_id):
        # Check the group exist
        code, _, _ = VectorEngine.GetGroup(group_id)
        if code == VectorEngine.FAULT_CODE:
            return VectorEngine.GROUP_NOT_EXIST

        # create index
        file = FileTable.query.filter(FileTable.group_name == group_id).filter(FileTable.type == 'raw').first()
        path = GroupHandler.GetGroupDirectory(group_id) + '/' + file.filename 
        print('Going to create index for: ', path)
        return VectorEngine.SUCCESS_CODE


    @staticmethod
    def InsertVectorIntoRawFile(group_id, filename, vector, vector_id):
        # print(sys._getframe().f_code.co_name, group_id, vector)
        # path = GroupHandler.GetGroupDirectory(group_id) + '/' + filename
        if VectorEngine.group_vector_dict is None:
            # print("VectorEngine.group_vector_dict is None")
            VectorEngine.group_vector_dict = dict()

        if VectorEngine.group_vector_id_dict is None:
            VectorEngine.group_vector_id_dict = dict()

        if not (group_id in VectorEngine.group_vector_dict):
            VectorEngine.group_vector_dict[group_id] = []

        if not (group_id in VectorEngine.group_vector_id_dict):
            VectorEngine.group_vector_id_dict[group_id] = []

        VectorEngine.group_vector_dict[group_id].append(vector)
        VectorEngine.group_vector_id_dict[group_id].append(vector_id)

        # print('InsertVectorIntoRawFile: ', VectorEngine.group_vector_dict[group_id], VectorEngine.group_vector_id_dict[group_id])
        print("cache size: ", len(VectorEngine.group_vector_dict[group_id]))

        return filename


    @staticmethod
    def GetVectorListFromRawFile(group_id, filename="todo"):
        # print("GetVectorListFromRawFile, vectors: ", serialize.to_array(VectorEngine.group_vector_dict[group_id]))
        # print("GetVectorListFromRawFile, vector_ids: ", serialize.to_int_array(VectorEngine.group_vector_id_dict[group_id]))
        return serialize.to_array(VectorEngine.group_vector_dict[group_id]), serialize.to_int_array(VectorEngine.group_vector_id_dict[group_id])

    @staticmethod
    def ClearRawFile(group_id):
        print("VectorEngine.group_vector_dict: ", VectorEngine.group_vector_dict)
        del VectorEngine.group_vector_dict[group_id]
        del VectorEngine.group_vector_id_dict[group_id]
        return VectorEngine.SUCCESS_CODE

