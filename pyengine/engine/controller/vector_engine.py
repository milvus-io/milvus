from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable
from engine.controller.raw_file_handler import RawFileHandler
from engine.controller.group_handler import GroupHandler
from engine.controller.index_file_handler import IndexFileHandler
from engine.settings import ROW_LIMIT
from flask import jsonify
from engine.ingestion import build_index
from engine.controller.scheduler import Scheduler
from engine.ingestion import serialize
from engine.controller.meta_manager import MetaManager
from engine.controller.error_code import ErrorCode
from engine.controller.storage_manager import StorageManager
from datetime import date
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
        if error == ErrorCode.SUCCESS_CODE:
            return ErrorCode.FAULT_CODE, group_name
        else:
            StorageManager.AddGroup(group_name)
            MetaManager.AddGroup(group_name, dimension)
            MetaManager.Sync()
            return ErrorCode.SUCCESS_CODE, group_name


    @staticmethod
    def GetGroup(group_name):
        error, _ = MetaManager.GetGroup(group_name)
        return error, group_name

    @staticmethod
    def DeleteGroup(group_name):
        group = GroupTable.query.filter(GroupTable.group_name==group_name).first()
        if(group):
            MetaManager.DeleteGroup(group)
            StorageManager.DeleteGroup(group_name)
            MetaManager.DeleteGroupFiles(group_name)
            MetaManager.Sync()
            return VectorEngine.SUCCESS_CODE, group_name
        else:
            return VectorEngine.SUCCESS_CODE, group_name


    @staticmethod
    def GetGroupList():
        groups = MetaManager.GetAllGroup()
        group_list = []
        for group_tuple in groups:
            group_item = {}
            group_item['group_name'] = group_tuple.group_name
            group_item['file_number'] = 0
            group_list.append(group_item)

        return VectorEngine.SUCCESS_CODE, group_list

    @staticmethod
    def AddVectorToNewFile(group_name):
        pass

    @staticmethod
    def BuildVectorIndex(group_name, raw_filename, dimension):
        # Build index
        raw_vector_array, raw_vector_id_array = VectorEngine.GetVectorListFromRawFile(group_name)

        # create index
        index_builder = build_index.FactoryIndex()
        index = index_builder().build(dimension, raw_vector_array, raw_vector_id_array)

        # TODO(jinhai): store index into Cache
        index_filename = raw_filename + '_index'
        serialize.write_index(file_name=index_filename, index=index)

        UpdateFile(raw_filename, {'row_number': ROW_LIMIT, 'type': 'index', 'filename': index_filename})

    @staticmethod
    def AddVector(group_name, vectors):
        print(group_name, vectors)
        error, group = MetaManager.GetGroup(group_name)
        if error == VectorEngine.FAULT_CODE:
            return VectorEngine.GROUP_NOT_EXIST, 'invalid'

        # first raw file
        raw_filename = str(group.file_number)
        files = MetaManager.GetAllRawFiles(group_name)

        current_raw_row_number = 0
        current_raw_file = None
        if files != None:
            for file in files:
                if file.filename == raw_filename:
                    current_raw_file = file
                    current_raw_row_number = file.row_number
                    print(raw_filename)
                elif file.type == 'raw':
                    BuildVectorIndex(group_name, file.filename, group.dimension)
                else:
                    pass
        else:
            pass

        vector_str_list = []

        # Verify if the row number + incoming row > limit
        incoming_row_number = len(vectors)

        start_row_index = 0
        total_row_number = group.row_number
        table_row_number = current_raw_row_number
        if current_raw_row_number + incoming_row_number > ROW_LIMIT:
            # Insert into exist raw file
            start_row_index = ROW_LIMIT - current_raw_row_number
            
            for i in range(0, start_row_index, 1):
                total_row_number += 1
                vector_id = total_row_number
                VectorEngine.InsertVectorIntoRawFile(group_name, raw_filename, vectors[i], vector_id)
                ++ table_row_number
                vector_str_list.append(group_name + '.' + str(vector_id))

            BuildVectorIndex(group_name, raw_filename, group.dimension)

            # create new raw file name
            raw_filename = str(group.file_number + 1)
            table_row_number = 0
            current_raw_file = None

        # If no raw file
        if current_raw_file == None:
            # update file table
            MetaManager.CreateRawFile(group_name, raw_filename)

        # 1. update db on file number and row number
        new_group_file_number = group.file_number + 1
        new_group_row_number = int(group.row_number) + incoming_row_number - start_row_index
        MetaManager.UpdateGroup(group_name, {'file_number': new_group_file_number, 'row_number': new_group_row_number})

        # 2. store vector into raw files
        for i in range (start_row_index, incoming_row_number, 1):
            vector_id = total_row_number
            total_row_number += 1
            VectorEngine.InsertVectorIntoRawFile(group_name, raw_filename, vectors[i], vector_id)
            ++ table_row_number
            vector_str_list.append(group_name + '.' + str(vector_id))

        MetaManager.UpdateFile(raw_filename, {'row_number': table_row_number})

        MetaManager.UpdateGroup(group_name, {'row_number': total_row_number})
        # 3. sync
        MetaManager.Sync()
        return VectorEngine.SUCCESS_CODE, vector_str_list


    @staticmethod
    def SearchVector(group_id, vector, limit):
        # Check the group exist
        code, _ = VectorEngine.GetGroup(group_id)
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
        code, _ = VectorEngine.GetGroup(group_id)
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

