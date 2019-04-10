from engine.controller.vector_engine import VectorEngine
from engine.settings import DATABASE_DIRECTORY
from engine.controller.error_code import ErrorCode
from flask import jsonify
import pytest
import os
import numpy as np
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestVectorEngine:
    def setup_class(self):
        self.__vectors = [[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8] for _ in range(10) ]
        self.__vector = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]
        self.__limit = 1


    def teardown_class(self):
        pass

    def test_group(self):
        # Make sure there is no group
        code, group_id = VectorEngine.DeleteGroup('test_group')
        assert code == ErrorCode.SUCCESS_CODE
        assert group_id == 'test_group'

        # Add a group
        code, group_id = VectorEngine.AddGroup('test_group', 8)
        assert code == ErrorCode.SUCCESS_CODE
        assert group_id == 'test_group'

        # Check the group existing
        code, group_id = VectorEngine.GetGroup('test_group')
        assert code == ErrorCode.SUCCESS_CODE
        assert group_id == 'test_group'

        # Check the group list
        code, group_list = VectorEngine.GetGroupList()
        assert code == ErrorCode.SUCCESS_CODE
        assert group_list == [{'group_name': 'test_group', 'file_number': 0}]

        # Add Vector for not exist group
        code, vector_id = VectorEngine.AddVector('not_exist_group', self.__vectors)
        assert code == VectorEngine.GROUP_NOT_EXIST
        assert vector_id == 'invalid'

        # Add vector for exist group
        code, vector_id = VectorEngine.AddVector('test_group', self.__vectors)
        assert code == ErrorCode.SUCCESS_CODE
        print(vector_id)
        assert vector_id == ['test_group.0', 'test_group.1', 'test_group.2', 'test_group.3', 'test_group.4', 'test_group.5', 'test_group.6', 'test_group.7', 'test_group.8', 'test_group.9']

        # Check search vector interface
        code, vector_id = VectorEngine.SearchVector('test_group', self.__vector, self.__limit)
        assert code == ErrorCode.SUCCESS_CODE
        assert vector_id == ['test_group.0']

        # Check create index interface
        code = VectorEngine.CreateIndex('test_group')
        assert code == ErrorCode.SUCCESS_CODE

        # Remove the group
        code, group_id = VectorEngine.DeleteGroup('test_group')
        assert code == ErrorCode.SUCCESS_CODE
        assert group_id == 'test_group'

        # Check the group is disppeared
        code, group_id = VectorEngine.GetGroup('test_group')
        assert code == VectorEngine.FAULT_CODE
        assert group_id == 'test_group'

        # Check SearchVector interface
        code, vector_ids = VectorEngine.SearchVector('test_group', self.__vector, self.__limit)
        assert code == VectorEngine.GROUP_NOT_EXIST
        assert vector_ids == {}

        # Create Index for not exist group id
        code = VectorEngine.CreateIndex('test_group')
        assert code == VectorEngine.GROUP_NOT_EXIST

        # Clear raw file
        code = VectorEngine.ClearRawFile('test_group')
        assert code == ErrorCode.SUCCESS_CODE

    def test_raw_file(self):
        filename = VectorEngine.InsertVectorIntoRawFile('test_group', 'test_group.raw', self.__vector, 0)
        assert filename == 'test_group.raw'

        expected_list = [self.__vector]
        vector_list, vector_id_list = VectorEngine.GetVectorListFromRawFile('test_group', filename)


        print('expected_list: ', expected_list)
        print('vector_list: ', vector_list)
        print('vector_id_list: ', vector_id_list)

        expected_list = np.asarray(expected_list).astype('float32')
        assert np.all(vector_list == expected_list)

        code = VectorEngine.ClearRawFile('test_group')
        assert code == ErrorCode.SUCCESS_CODE




