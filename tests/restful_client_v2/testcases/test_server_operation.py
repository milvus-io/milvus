import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel


@pytest.mark.tags(CaseLabel.L0)
class TestServerOperation(TestBase):
    """
    Test cases for server operations
    """

    def test_get_server_version_basic(self):
        """
        Test getting basic server version
        """
        rsp = self.server_client.get_version({})
        assert rsp["code"] == 0
        assert "version" in rsp["data"]
        assert isinstance(rsp["data"]["version"], str)
        assert len(rsp["data"]["version"]) > 0

    def test_get_server_version_detail_false(self):
        """
        Test getting basic server version with detail=false
        """
        rsp = self.server_client.get_version({"detail": False})
        assert rsp["code"] == 0
        assert "version" in rsp["data"]
        assert isinstance(rsp["data"]["version"], str)

    def test_get_server_version_detail_true(self):
        """
        Test getting detailed server version with detail=true
        """
        rsp = self.server_client.get_version({"detail": True})
        assert rsp["code"] == 0
        assert "version" in rsp["data"]
        assert "buildTags" in rsp["data"]
        assert "buildTime" in rsp["data"]
        assert "gitCommit" in rsp["data"]
        assert "goVersion" in rsp["data"]
        assert "deployMode" in rsp["data"]

