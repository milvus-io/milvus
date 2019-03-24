from engine.controller.group_handler import GroupHandler
from engine.settings import DATABASE_DIRECTORY
import pytest
import os
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestGroupHandler:
    def test_get_group(self):
        group_path = GroupHandler.GetGroupDirectory('test_group')
        verified_path = DATABASE_DIRECTORY + '/' + 'test_group'
        logger.debug(group_path)
        assert group_path == verified_path

    def test_create_group(self):
        group_path = GroupHandler.CreateGroupDirectory('test_group')
        if os.path.exists(group_path):
            assert True
        else:
            assert False

    def test_delete_group(self):
        group_path = GroupHandler.GetGroupDirectory('test_group')
        if os.path.exists(group_path):
            assert True
            GroupHandler.DeleteGroupDirectory('test_group')
            if os.path.exists(group_path):
                assert False
            else:
                assert True
        else:
            assert False


