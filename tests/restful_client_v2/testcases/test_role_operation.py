from utils.utils import gen_unique_str
from base.testbase import TestBase
import pytest


@pytest.mark.L1
class TestRoleE2E(TestBase):

    def teardown_method(self):
        # because role num is limited, so we need to delete all roles after test
        rsp = self.role_client.role_list()
        all_roles = rsp['data']
        # delete all roles except default roles
        for role in all_roles:
            if role.startswith("role") and role in self.role_client.role_names:
                payload = {
                    "roleName": role
                }
                # revoke privilege from role
                rsp = self.role_client.role_describe(role)
                for d in rsp['data']:
                    payload = {
                        "roleName": role,
                        "objectType": d['objectType'],
                        "objectName": d['objectName'],
                        "privilege": d['privilege']
                    }
                    self.role_client.role_revoke(payload)
                self.role_client.role_drop(payload)

    def test_role_e2e(self):

        # list role before create
        rsp = self.role_client.role_list()
        # create role
        role_name = gen_unique_str("role")
        payload = {
            "roleName": role_name,
        }
        rsp = self.role_client.role_create(payload)
        # list role after create
        rsp = self.role_client.role_list()
        assert role_name in rsp['data']
        # describe role
        rsp = self.role_client.role_describe(role_name)
        assert rsp['code'] == 0
        # grant privilege to role
        payload = {
            "roleName": role_name,
            "objectType": "Global",
            "objectName": "*",
            "privilege": "CreateCollection"
        }
        rsp = self.role_client.role_grant(payload)
        assert rsp['code'] == 0
        # describe role after grant
        rsp = self.role_client.role_describe(role_name)
        privileges = []
        for p in rsp['data']:
            privileges.append(p['privilege'])
        assert "CreateCollection" in privileges
        # revoke privilege from role
        payload = {
            "roleName": role_name,
            "objectType": "Global",
            "objectName": "*",
            "privilege": "CreateCollection"
        }
        rsp = self.role_client.role_revoke(payload)
        # describe role after revoke
        rsp = self.role_client.role_describe(role_name)
        privileges = []
        for p in rsp['data']:
            privileges.append(p['privilege'])
        assert "CreateCollection" not in privileges
        # drop role
        payload = {
            "roleName": role_name
        }
        rsp = self.role_client.role_drop(payload)
        rsp = self.role_client.role_list()
        assert role_name not in rsp['data']

