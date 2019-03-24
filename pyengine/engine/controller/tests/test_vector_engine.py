from engine.controller.vector_engine import VectorEngine
from engine.settings import DATABASE_DIRECTORY
from flask import jsonify
import pytest
import os
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestVectorEngine:
    def setup_class(self):
        self.__vector = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]
        self.__limit = 3


    def teardown_class(self):
        pass

    def test_group(self):
        # Make sure there is no group
        code, group_id, file_number = VectorEngine.DeleteGroup('test_group')
        assert code == VectorEngine.SUCCESS_CODE
        assert group_id == 'test_group'
        assert file_number == 0

        # Add a group
        code, group_id, file_number = VectorEngine.AddGroup('test_group', 8)
        assert code == VectorEngine.SUCCESS_CODE
        assert group_id == 'test_group'
        assert file_number == 0

        # Check the group existing
        code, group_id, file_number = VectorEngine.GetGroup('test_group')
        assert code == VectorEngine.SUCCESS_CODE
        assert group_id == 'test_group'
        assert file_number == 0

        # Check the group list
        code, group_list = VectorEngine.GetGroupList()
        assert code == VectorEngine.SUCCESS_CODE
        assert group_list == [{'group_name': 'test_group', 'file_number': 0}]

        # Add Vector for not exist group
        code = VectorEngine.AddVector('not_exist_group', self.__vector)
        assert code == VectorEngine.GROUP_NOT_EXIST

        # Add vector for exist group
        code = VectorEngine.AddVector('test_group', self.__vector)
        assert code == VectorEngine.SUCCESS_CODE

        # Check search vector interface
        code, vector_id = VectorEngine.SearchVector('test_group', self.__vector, self.__limit)
        assert code == VectorEngine.SUCCESS_CODE
        assert vector_id == 0

        # Check create index interface
        code = VectorEngine.CreateIndex('test_group')
        assert code == VectorEngine.SUCCESS_CODE

        # Remove the group
        code, group_id, file_number = VectorEngine.DeleteGroup('test_group')
        assert code == VectorEngine.SUCCESS_CODE
        assert group_id == 'test_group'
        assert file_number == 0

        # Check the group is disppeared
        code, group_id, file_number = VectorEngine.GetGroup('test_group')
        assert code == VectorEngine.FAULT_CODE
        assert group_id == 'test_group'
        assert file_number == 0

        # Check SearchVector interface
        code = VectorEngine.SearchVector('test_group', self.__vector, self.__limit)
        assert code == VectorEngine.GROUP_NOT_EXIST

        # Create Index for not exist group id
        code = VectorEngine.CreateIndex('test_group')
        assert code == VectorEngine.GROUP_NOT_EXIST

        # Clear raw file
        code = VectorEngine.ClearRawFile('test_group')
        assert code == VectorEngine.SUCCESS_CODE

    def test_raw_file(self):
        filename = VectorEngine.InsertVectorIntoRawFile('test_group', 'test_group.raw', self.__vector)
        assert filename == 'test_group.raw'

        expected_list = [self.__vector]
        vector_list = VectorEngine.GetVectorListFromRawFile('test_group', filename)

        print('expected_list: ', expected_list)
        print('vector_list: ', vector_list)

        assert vector_list == expected_list

        code = VectorEngine.ClearRawFile('test_group')
        assert code == VectorEngine.SUCCESS_CODE




