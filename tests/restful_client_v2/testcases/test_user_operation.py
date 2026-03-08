import time
from utils.utils import gen_collection_name, gen_unique_str
import pytest
from base.testbase import TestBase
from pymilvus import (connections)


class TestUserE2E(TestBase):

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

    @pytest.mark.L0
    def test_user_e2e(self):
        # list user before create

        rsp = self.user_client.user_list()
        # create user
        user_name = gen_unique_str("user")
        password = "1234578"
        payload = {
            "userName": user_name,
            "password": password
        }
        rsp = self.user_client.user_create(payload)
        # list user after create
        rsp = self.user_client.user_list()
        assert user_name in rsp['data']
        # describe user
        rsp = self.user_client.user_describe(user_name)

        # update user password
        new_password = "87654321"
        payload = {
            "userName": user_name,
            "password": password,
            "newPassword": new_password
        }
        rsp = self.user_client.user_password_update(payload)
        assert rsp['code'] == 0
        # drop user
        payload = {
            "userName": user_name
        }
        rsp = self.user_client.user_drop(payload)

        rsp = self.user_client.user_list()
        assert user_name not in rsp['data']

    @pytest.mark.L1
    def test_user_binding_role(self):
        # create user
        user_name = gen_unique_str("user")
        password = "12345678"
        payload = {
            "userName": user_name,
            "password": password
        }
        rsp = self.user_client.user_create(payload)
        # list user after create
        rsp = self.user_client.user_list()
        assert user_name in rsp['data']
        # create role
        role_name = gen_unique_str("role")
        payload = {
            "roleName": role_name,
        }
        rsp = self.role_client.role_create(payload)
        # privilege to role
        payload = {
            "roleName": role_name,
            "objectType": "Global",
            "objectName": "*",
            "privilege": "All"
        }
        rsp = self.role_client.role_grant(payload)
        # bind role to user
        payload = {
            "userName": user_name,
            "roleName": role_name
        }
        rsp = self.user_client.user_grant(payload)
        # describe user roles
        rsp = self.user_client.user_describe(user_name)
        rsp = self.role_client.role_describe(role_name)

        # test user has privilege with pymilvus
        uri = self.user_client.endpoint
        connections.connect(alias="test", uri=f"{uri}", token=f"{user_name}:{password}")
        # wait to make sure user has been updated
        time.sleep(5)

        # create collection with user
        collection_name = gen_collection_name()
        payload = {
            "collectionName": collection_name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": "128"}}
                ]
            }
        }
        self.collection_client.api_key = f"{user_name}:{password}"
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0


@pytest.mark.L1
class TestUserNegative(TestBase):

    def test_create_user_with_short_password(self):
        # list user before create

        rsp = self.user_client.user_list()
        # create user
        user_name = gen_unique_str("user")
        password = "1234"
        payload = {
            "userName": user_name,
            "password": password
        }
        rsp = self.user_client.user_create(payload)
        assert rsp['code'] == 1100

    def test_create_user_twice(self):
        # list user before create

        rsp = self.user_client.user_list()
        # create user
        user_name = gen_unique_str("user")
        password = "12345678"
        payload = {
            "userName": user_name,
            "password": password
        }
        for i in range(2):
            rsp = self.user_client.user_create(payload)
            if i == 0:
                assert rsp['code'] == 0
            else:
                assert rsp['code'] == 65535
                assert "user already exists" in rsp['message']
