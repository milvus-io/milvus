from request_handler import Request
import pytest


class VDCRequest:

    def __init__(self, addr):
        self.request = Request()
        self.addr = addr

    def GetDeployList(self, url=None, data=None, headers=None):
        headers = {'Content-type': "application/json",
                   "Authorization": "***"} if headers is None else headers

        return self.request.get(url, json=data, headers=headers)

    def CreateMilvus(self, auth='auth', name="", cloud_id='', region_id='', deploy_type='', headers=None):
        url = self.addr + "/api/v1/cloud/%s/CreateMilvus" % auth
        data = {"Name": name,
                "CloudId": cloud_id,
                "RegionId": region_id,
                "DeployType": deploy_type}
        return self.request.get(url, json=data, headers=headers)


class TestVDC:
    """ sample """
    def setup_class(self):
        pass

    def teardown_class(self):
       pass

    def setup_method(self):
        self.r = VDCRequest("http://localhost")

    def teardown_method(self):
        pass

    @pytest.mark.parametrize("name", ["China", "Canada"])
    def test_1(self, name):
        self.r.CreateMilvus(name)
