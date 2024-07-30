import pytest
from base.testbase import TestBase
from utils.utils import gen_unique_str


@pytest.mark.L0
class TestDatabaseE2E(TestBase):

    def test_database_e2e(self):
        """
        """
        default_db_name = "default"
        test_db_name = gen_unique_str()
        # list databases before create
        rsp = self.database_client.list_databases()
        assert rsp['code'] == 0
        assert default_db_name in rsp['data']
        assert test_db_name not in rsp['data']
        # create database
        rsp = self.database_client.create_database(test_db_name)
        assert rsp['code'] == 0
        # list databases after create
        rsp = self.database_client.list_databases()
        assert rsp['code'] == 0
        assert default_db_name in rsp['data']
        assert test_db_name in rsp['data']
        # describe test database
        rsp = self.database_client.describe_database(test_db_name)
        assert rsp['code'] == 0
        assert rsp['data'] == {}
        # alter test database
        rsp = self.database_client.alter_database(test_db_name, {"database.max.collections": "100"})
        assert rsp['code'] == 0
        # describe test database
        rsp = self.database_client.describe_database(test_db_name)
        assert rsp['code'] == 0
        assert rsp['data']["database.max.collections"] == "100"
        # drop database
        rsp = self.database_client.drop_database(test_db_name)
        assert rsp['code'] == 0
        # list databases after drop
        rsp = self.database_client.list_databases()
        assert rsp['code'] == 0
        assert default_db_name in rsp['data']
        assert test_db_name not in rsp['data']
