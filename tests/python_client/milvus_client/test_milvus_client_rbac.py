import time
import numpy as np

import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *


prefix = "client_rbac"
user_pre = "user"
role_pre = "role"
root_token = "root:Milvus"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacBase(TestMilvusClientV2Base):
    """ Test case of rbac interface """

    def teardown_method(self, method):
        """
        teardown method: drop role and user
        """
        log.info("[utility_teardown_method] Start teardown utility test cases ...")
        client = self._client()

        # drop users
        users, _ = self.list_users(client)
        for user in users:
            if user != ct.default_user:
                self.drop_user(client, user)
        users, _ = self.list_users(client)
        assert len(users) == 1

        # drop roles
        roles, _ = self.list_roles(client)
        for role in roles:
            if role not in ['admin', 'public']:
                res, _ = self.describe_role(client, role)
                if res['privileges']:
                    for privilege in res['privileges']:
                        self.revoke_privilege(client, role, privilege["object_type"],
                                              privilege["privilege"], privilege["object_name"])
                self.drop_role(client, role)
        roles, _ = self.list_roles(client)
        assert len(roles) == 2

        super().teardown_method(method)

    def test_milvus_client_connect_using_token(self, host, port):
        """
        target: test init milvus client using token
        method: init milvus client with only token
        expected: init successfully
        """
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, token=root_token)
        # check success link
        res = self.list_databases(client)[0]
        assert res != []

    def test_milvus_client_connect_using_user_password(self, host, port):
        """
        target: test init milvus client using user and password
        method: init milvus client with user and password
        expected: init successfully
        """
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=ct.default_user,
                                            password=ct.default_password)
        # check success link
        res = self.list_databases(client)[0]
        assert res != []

    def test_milvus_client_create_user(self, host, port):
        """
        target: test milvus client api create_user
        method: create user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        # check
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        res = self.list_databases(client)[0]
        assert res == []

    def test_milvus_client_drop_user(self, host, port):
        """
        target: test milvus client api drop_user
        method: drop user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        # drop user that exists
        self.drop_user(client, user_name=user_name)
        # drop user that not exists
        not_exist_user_name = cf.gen_unique_str(user_pre)
        self.drop_user(client, user_name=not_exist_user_name)

    def test_milvus_client_update_password(self, host, port):
        """
        target: test milvus client api update_password
        method: create a user and update password
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        new_password = cf.gen_str_by_length()
        self.update_password(client, user_name=user_name, old_password=password, new_password=new_password)
        self.close(client)
        # check
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=new_password)
        res = self.list_databases(client)[0]
        assert res == []
        self.close(client)
        self.init_milvus_client(uri=uri, user=user_name, password=password,
                                check_task=CheckTasks.check_auth_failure)

    def test_milvus_client_list_users(self, host, port):
        """
        target: test milvus client api list_users
        method: create a user and list users
        expected: succeed
        """
        client = self._client()
        user_name1 = cf.gen_unique_str(user_pre)
        user_name2 = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name1, password=password)
        self.create_user(client, user_name=user_name2, password=password)
        res = self.list_users(client)[0]
        assert {ct.default_user, user_name1, user_name2}.issubset(set(res)) is True

    def test_milvus_client_describe_user(self, host, port):
        """
        target: test milvus client api describe_user
        method: create a user and describe the user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        # describe one self
        res, _ = self.describe_user(client, user_name=ct.default_user)
        assert res["user_name"] == ct.default_user
        # describe other users
        res, _ = self.describe_user(client, user_name=user_name)
        assert res["user_name"] == user_name
        # describe user that not exists
        user_not_exist = cf.gen_unique_str(user_pre)
        res, _ = self.describe_user(client, user_name=user_not_exist)
        assert res == {}

    def test_milvus_client_create_role(self, host, port):
        """
        target: test milvus client api create_role
        method: create a role
        expected: succeed
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)

    def test_milvus_client_drop_role(self, host, port):
        """
        target: test milvus client api drop_role
        method: create a role and drop
        expected: succeed
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_describe_role(self, host, port):
        """
        target: test milvus client api describe_role
        method: create a role and describe
        expected: succeed
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        # describe a role that exists
        self.describe_role(client, role_name=role_name)

    def test_milvus_client_list_roles(self, host, port):
        """
        target: test milvus client api list_roles
        method: create a role and list roles
        expected: succeed
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        res, _ = self.list_roles(client)
        assert role_name in res

    def test_milvus_client_grant_role(self, host, port):
        """
        target: test milvus client api grant_role
        method: create a role and a user, then grant role to the user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)

    def test_milvus_client_revoke_role(self, host, port):
        """
        target: test milvus client api revoke_role
        method: create a role and a user, then grant role to the user, then revoke
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        # revoke a user that does not exist
        self.revoke_role(client, user_name=user_name, role_name=role_name)
        # revoke a user that exists
        self.grant_role(client, user_name=user_name, role_name=role_name)
        self.revoke_role(client, user_name=user_name, role_name=role_name)

    def test_milvus_client_grant_privilege(self, host, port):
        """
        target: test milvus client api grant_privilege
        method: create a role and a user, then grant role to the user, grant a privilege to the role
        expected: succeed
        """
        # prepare a collection
        client_root = self._client()
        coll_name = cf.gen_unique_str()
        self.create_collection(client_root, coll_name, default_dim, consistency_level="Strong")

        # create a new role and a new user ( no privilege)
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client_root, user_name=user_name, password=password)
        self.create_role(client_root, role_name=role_name)
        self.grant_role(client_root, user_name=user_name, role_name=role_name)

        # check the role has no privilege of drop collection
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        self.drop_collection(client, coll_name, check_task=CheckTasks.check_permission_deny)

        # grant the role with the privilege of drop collection
        self.grant_privilege(client_root, role_name, "Global", "DropCollection", "*")
        time.sleep(10)

        # check the role has privilege of drop collection
        self.drop_collection(client, coll_name)

    @pytest.mark.skip("https://github.com/milvus-io/pymilvus/issues/1908")
    def test_milvus_client_revoke_privilege(self, host, port):
        """
        target: test milvus client api revoke_privilege
        method: create a role and a user, then grant role to the user, grant a privilege to the role, then revoke
        expected: succeed
        """
        # prepare a collection
        client_root = self._client()
        coll_name = cf.gen_unique_str()

        # create a new role and a new user ( no privilege)
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client_root, user_name=user_name, password=password)
        self.create_role(client_root, role_name=role_name)
        self.grant_role(client_root, user_name=user_name, role_name=role_name)
        self.grant_privilege(client_root, role_name, "Global", "CreateCollection", "*")
        time.sleep(10)

        # check the role has privilege of create collection
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        self.create_collection(client, coll_name, default_dim, consistency_level="Strong")

        # revoke the role with the privilege of create collection
        self.revoke_privilege(client_root, role_name, "Global", "CreateCollection", "*")
        time.sleep(10)

        # check the role has no privilege of create collection
        self.create_collection(client, coll_name, default_dim, consistency_level="Strong",
                               check_task=CheckTasks.check_permission_deny)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacInvalid(TestMilvusClientV2Base):

    """ Test case of rbac interface """
    def test_milvus_client_init_token_invalid(self, host, port):
        """
        target: test milvus client api token invalid
        method: init milvus client using a wrong token
        expected: raise exception
        """
        uri = f"http://{host}:{port}"
        wrong_token = root_token + "kk"
        self.init_milvus_client(uri=uri, token=wrong_token, check_task=CheckTasks.check_auth_failure)

    def test_milvus_client_init_username_invalid(self, host, port):
        """
        target: test milvus client api username invalid
        method: init milvus client using a wrong username
        expected: raise exception
        """
        uri = f"http://{host}:{port}"
        invalid_user_name = ct.default_user + "nn"
        self.init_milvus_client(uri=uri, user=invalid_user_name, password=ct.default_password,
                                check_task=CheckTasks.check_auth_failure)

    def test_milvus_client_init_password_invalid(self, host, port):
        """
        target: test milvus client api password invalid
        method: init milvus client using a wrong password
        expected: raise exception
        """
        uri = f"http://{host}:{port}"
        wrong_password = ct.default_password + "kk"
        self.init_milvus_client(uri=uri, user=ct.default_user, password=wrong_password,
                                check_task=CheckTasks.check_auth_failure)

    @pytest.mark.parametrize("invalid_name", ["", "0", "n@me", "h h"])
    def test_milvus_client_create_user_value_invalid(self, host, port, invalid_name):
        """
        target: test milvus client api create_user invalid
        method: create using a wrong username
        expected: raise exception
        """
        client = self._client()
        self.create_user(client, invalid_name, ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1100,
                                      ct.err_msg: "invalid parameter"})

    @pytest.mark.parametrize("invalid_name", [1, [], None, {}])
    def test_milvus_client_create_user_type_invalid(self, host, port, invalid_name):
        """
        target: test milvus client api create_user invalid
        method: create using a wrong username
        expected: raise exception
        """
        client = self._client()
        self.create_user(client, invalid_name, ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1,
                                      ct.err_msg: f"`user` value {invalid_name} is illegal"})

    def test_milvus_client_create_user_exist(self, host, port):
        """
        target: test milvus client api create_user invalid
        method: create using a wrong username
        expected: raise exception
        """
        client = self._client()
        self.create_user(client, "root", ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 65535,
                                      ct.err_msg: "user already exists: root"})

    @pytest.mark.parametrize("invalid_password", ["", "0", "p@ss", "h h", "1+1=2"])
    def test_milvus_client_create_user_password_invalid_value(self, host, port, invalid_password):
        """
        target: test milvus client api create_user invalid
        method: create using a wrong username
        expected: raise exception
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        self.create_user(client, user_name, invalid_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1100,
                                      ct.err_msg: "invalid password"})

    @pytest.mark.parametrize("invalid_password", [1, [], None, {}])
    def test_milvus_client_create_user_password_invalid_type(self, host, port, invalid_password):
        """
        target: test milvus client api create_user invalid
        method: create using a wrong username
        expected: raise exception
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        self.create_user(client, user_name, invalid_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1,
                                      ct.err_msg: f"`password` value {invalid_password} is illegal"})

    def test_milvus_client_update_password_user_not_exist(self, host, port):
        """
        target: test milvus client api update_password
        method: create a user and update password
        expected: raise exception
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        new_password = cf.gen_str_by_length(contain_numbers=True)
        self.update_password(client, user_name=user_name, old_password=password,
                             new_password=new_password,
                             check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 1400,
                                          ct.err_msg: "old password not correct for %s: "
                                                      "not authenticated" % user_name})

    def test_milvus_client_update_password_password_wrong(self, host, port):
        """
        target: test milvus client api update_password
        method: create a user and update password
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        new_password = cf.gen_str_by_length(contain_numbers=True)
        wrong_password = password + 'kk'
        self.update_password(client, user_name=user_name, old_password=wrong_password,
                             new_password=new_password, check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 1400,
                                          ct.err_msg: "old password not correct for %s: "
                                                      "not authenticated" % user_name})

    def test_milvus_client_update_password_new_password_same(self, host, port):
        """
        target: test milvus client api update_password
        method: create a user and update password
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.update_password(client, user_name=user_name, old_password=password, new_password=password)

    @pytest.mark.parametrize("invalid_password", ["", "0", "p@ss", "h h", "1+1=2"])
    def test_milvus_client_update_password_new_password_invalid(self, host, port, invalid_password):
        """
        target: test milvus client api update_password
        method: create a user and update password
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.update_password(client, user_name=user_name, old_password=password,
                             new_password=invalid_password, check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 1100,
                                          ct.err_msg: "invalid password"})

    def test_milvus_client_create_role_exist(self, host, port):
        """
        target: test milvus client api create_role
        method: create a role using invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        # create existed role
        error_msg = f'role [name:"{role_name}"] already exists'
        self.create_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 65535, ct.err_msg: error_msg})

    def test_milvus_client_drop_role_invalid(self, host, port):
        """
        target: test milvus client api drop_role
        method: create a role and drop
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 65535,
                                    ct.err_msg: "not found the role, maybe the role isn't "
                                                "existed or internal system error"})

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_drop_built_in_role(self, host, port, role_name):
        """
        target: test milvus client api drop_role
        method: create a role and drop
        expected: raise exception
        """
        client = self._client()
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 65535,
                                    ct.err_msg: f"the role[{role_name}] is a default role, "
                                                f"which can't be dropped"})

    def test_milvus_client_describe_role_invalid(self, host, port):
        """
        target: test milvus client api describe_role
        method: describe a role using invalid name
        expected: raise exception
        """
        client = self._client()
        # describe a role that does not exist
        role_not_exist = cf.gen_unique_str(role_pre)
        error_msg = "not found the role, maybe the role isn't existed or internal system error"
        self.describe_role(client, role_name=role_not_exist, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535, ct.err_msg: error_msg})

    def test_milvus_client_grant_role_user_not_exist(self, host, port):
        """
        target: test milvus client api grant_role
        method: create a role and a user, then grant role to the user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65536,
                                     ct.err_msg: "not found the user, maybe the user "
                                                 "isn't existed or internal system error"})

    def test_milvus_client_grant_role_role_not_exist(self, host, port):
        """
        target: test milvus client api grant_role
        method: create a role and a user, then grant role to the user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name=role_name,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65536,
                                     ct.err_msg: "not found the role, maybe the role "
                                                 "isn't existed or internal system error"})

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_create_privilege_group_with_privilege_group_name_invalid_type(self, name, host, port):
        """
        target: test milvus client api create privilege group with invalid name
        method: create privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"`privilege_group` value {name} is illegal"
        self.create_privilege_group(client, privilege_group=name, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", ["n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_create_privilege_group_with_privilege_group_name_invalid_value_1(self, name, host, port):
        """
        target: test milvus client api create privilege group with invalid name
        method: create privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"privilege group name {name} can only contain numbers, letters and underscores: invalid parameter"
        self.create_privilege_group(client, privilege_group=name, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_drop_privilege_group_with_privilege_group_name_invalid_type(self, name, host, port):
        """
        target: test milvus client api drop privilege group with invalid name
        method: drop privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"`privilege_group` value {name} is illegal"
        self.drop_privilege_group(client, privilege_group=name, check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_add_privileges_to_group_with_privilege_group_name_invalid_type(self, name, host, port):
        """
        target: test milvus client api add privilege group with invalid name
        method: add privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"`privilege_group` value {name} is illegal"
        self.add_privileges_to_group(client, privilege_group=name, privileges=["Insert"], 
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", ["n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_add_privileges_to_group_with_privilege_group_name_invalid_value(self, name, host, port):
        """
        target: test milvus client api add privilege group with invalid name
        method: add privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"privilege group name {name} can only contain numbers, letters and underscores: invalid parameter"
        self.add_privileges_to_group(client, privilege_group=name, privileges=["Insert"], 
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1100, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_add_privilege_into_not_exist_privilege_group(self, host, port):
        """
        target: test milvus client api add privilege into not exist privilege group
        method: add privilege into not exist privilege group
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        privilege_group_name = "privilege_group_not_exist"
        error_msg = f"there is no privilege group name [{privilege_group_name}] defined in system"
        self.add_privileges_to_group(client, privilege_group=privilege_group_name, privileges=["Insert"], 
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1100, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff", "invalid_privilege"])
    def test_milvus_client_add_privileges_to_group_with_privilege_invalid(self, name, host, port):
        """
        target: test milvus client api add privilege group with invalid privilege type
        method: add privilege group with invalid privilege type
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"`privileges` value {name} is illegal"
        self.add_privileges_to_group(client, privilege_group="privilege_group_1", privileges=name, 
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_remove_privileges_to_group_with_privilege_group_name_invalid(self, name, host, port):
        """
        target: test milvus client api remove privilege group with invalid name
        method: remove privilege group with invalid name
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"{name}"
        self.remove_privileges_from_group(client, privilege_group=name, privileges=["Insert"], 
                                          check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff", "invalid_privilege"])
    def test_milvus_client_remove_privileges_to_group_with_privilege_invalid(self, name, host, port):
        """
        target: test milvus client api remove privilege group with invalid privilege type
        method: remove privilege group with invalid privilege type
        expected: raise exception
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        
        error_msg = f"`privileges` value {name} is illegal"
        self.remove_privileges_from_group(client, privilege_group="privilege_group_1", privileges=name, 
                                          check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_role(client, role_name=role_name)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacAdvance(TestMilvusClientV2Base):
    """ Test case of rbac interface """

    def teardown_method(self, method):
        """
        teardown method: drop role and user
        """
        log.info("[utility_teardown_method] Start teardown utility test cases ...")
        client = self._client()

        # drop users
        users, _ = self.list_users(client)
        for user in users:
            if user != ct.default_user:
                self.drop_user(client, user)
        users, _ = self.list_users(client)
        assert len(users) == 1

        # drop roles
        roles, _ = self.list_roles(client)
        for role in roles:
            if role not in ['admin', 'public']:
                role_info, _ = self.describe_role(client, role)
                if role_info:
                    for privilege in role_info.get("privileges", []):
                        self.revoke_privilege(client, role, privilege["object_type"],
                                              privilege["privilege"], privilege["object_name"])
                self.drop_role(client, role)
        roles, _ = self.list_roles(client)
        assert len(roles) == 2

        super().teardown_method(method)

    @pytest.mark.skip("common.security.authorizationEnabled need to be set to true")
    def test_milvus_client_search_iterator_rbac_mul_db(self):
        """
        target: test search iterator(high level api) normal case about mul db by rbac
        method: create connection, collection, insert and search iterator
        expected: search iterator permission deny after switch to no permission db
        """
        batch_size = 20
        uri = f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        client, _ = self.init_milvus_client(uri=uri, token="root:Milvus")
        my_db = cf.gen_unique_str(prefix)
        self.create_database(client, my_db)
        collection_name = cf.gen_unique_str(prefix)
        self.using_database(client, my_db)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        # 2. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.using_database(client, "default")
        # 3. create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Bounded")
        # 4. insert
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        self.grant_privilege(client, role_name, "Collection", "Search", collection_name, 'default')
        self.grant_privilege(client, role_name, "Collection", "Insert", collection_name, my_db)
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        # 5. search_iterator
        vectors_to_search = rng.random((1, default_dim))
        self.search_iterator(client, collection_name, vectors_to_search, batch_size, use_rbac_mul_db=True, another_db=my_db,
                             check_task=CheckTasks.check_permission_deny)
        client, _ = self.init_milvus_client(uri=uri, token="root:Milvus")
        self.revoke_privilege(client, role_name, "Collection", "Insert", collection_name, my_db)
        self.drop_collection(client, collection_name)
        self.using_database(client, 'default')
        self.drop_collection(client, collection_name)

    def test_milvus_client_drop_role_which_bind_user(self, host, port):
        """
        target: test milvus client api drop_role when role is bound to user
        method: create a role, bind user to the role, drop the role
        expected: drop success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)

        # create user and role
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)

        # bind user to role
        self.grant_role(client, user_name=user_name, role_name=role_name)

        # drop role that has user bound to it
        self.drop_role(client, role_name=role_name)

        # verify role is dropped
        roles, _ = self.list_roles(client)
        assert role_name not in roles

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_add_user_to_default_role(self, role_name, host, port):
        """
        target: test milvus client api add user to default role (admin or public)
        method: create a user, add user to admin role or public role
        expected: add success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)

        # create user
        self.create_user(client, user_name=user_name, password=password)

        # grant role to user (add user to role)
        self.grant_role(client, user_name=user_name, role_name=role_name)

        # grant role to user again to test idempotency
        self.grant_role(client, user_name=user_name, role_name=role_name)

        # verify user has the role
        user_info, _ = self.describe_user(client, user_name=user_name)
        assert user_info["user_name"] == user_name
        assert role_name in user_info.get("roles", [])

    def test_milvus_client_add_root_to_new_role(self, host, port):
        """
        target: test milvus client api add root to new role
        method: create a new role and add root user to the role
        expected: add success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        
        # create new role
        self.create_role(client, role_name=role_name)
        
        # add root user to the new role
        self.grant_role(client, user_name=ct.default_user, role_name=role_name)
        
        # verify root user has the role
        user_info, _ = self.describe_user(client, user_name=ct.default_user)
        assert user_info["user_name"] == ct.default_user
        assert role_name in user_info.get("roles", [])
        
        # drop the role
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_list_collection_grants_by_role_and_object(self, host, port):
        """
        target: test milvus client api list grants by role and object
        method: create a new role, grant role collection privilege, list grants by role and object
        expected: list success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        collection_name = cf.gen_unique_str(prefix)
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # create new role
        self.create_role(client, role_name=role_name)
        
        # grant collection privileges to role
        self.grant_privilege(client, role_name, "Collection", "Search", collection_name)
        self.grant_privilege(client, role_name, "Collection", "Insert", collection_name)
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # describe role to get privileges
        role_info, _ = self.describe_role(client, role_name=role_name)
        
        # verify grants
        privileges = role_info.get("privileges", [])
        assert len(privileges) == 2
        
        privilege_names = [priv["privilege"] for priv in privileges]
        assert "Search" in privilege_names
        assert "Insert" in privilege_names
        
        # verify object type and name
        for priv in privileges:
            assert priv["object_type"] == "Collection"
            assert priv["object_name"] == collection_name
        
        # revoke privileges
        for priv in privileges:
            self.revoke_privilege(client, role_name, priv["object_type"], priv["privilege"], priv["object_name"])
        
        # drop role and collection
        self.drop_role(client, role_name=role_name)
        self.drop_collection(client, collection_name)

    def test_milvus_client_list_global_grants_by_role_and_object(self, host, port):
        """
        target: test milvus client api list grants by role and object for global privileges
        method: create a new role, grant role global privilege, list grants by role and object
        expected: list success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        
        # create new role
        self.create_role(client, role_name=role_name)
        
        # grant global privileges to role
        self.grant_privilege(client, role_name, "Global", "CreateCollection", "*")
        self.grant_privilege(client, role_name, "Global", "All", "*")
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # describe role to get privileges
        role_info, _ = self.describe_role(client, role_name=role_name)
        
        # verify grants
        privileges = role_info.get("privileges", [])
        assert len(privileges) == 2
        
        privilege_names = [priv["privilege"] for priv in privileges]
        assert "CreateCollection" in privilege_names
        assert "All" in privilege_names
        
        # verify object type and name
        for priv in privileges:
            assert priv["object_type"] == "Global"
            assert priv["object_name"] == "*"
        
        # revoke privileges
        for priv in privileges:
            self.revoke_privilege(client, role_name, priv["object_type"], priv["privilege"], priv["object_name"])
        
        # wait for revoke to take effect
        time.sleep(10)
        
        # drop role
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_verify_admin_role_privilege(self, host, port):
        """
        target: test milvus client api verify admin role privilege
        method: create a new user, bind to admin role, crud collection
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        
        # create user
        self.create_user(client, user_name=user_name, password=password)
        # grant admin role to user
        self.grant_role(client, user_name=user_name, role_name="admin")
        # wait for role to take effect
        time.sleep(10)
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # test admin privileges by performing CRUD operations
        # create collection
        self.create_collection(user_client, collection_name, default_dim, consistency_level="Strong")
        
        # insert data
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(user_client, collection_name, rows)
        
        # create index
        index_params, _ = self.prepare_index_params(user_client)
        index_params.add_index(field_name=default_vector_field_name)
        self.create_index(user_client, collection_name, index_params)

        # load collection
        self.load_collection(user_client, collection_name)
        
        # verify collection has data
        self.flush(user_client, collection_name)
        num_entities, _ = self.get_collection_stats(user_client, collection_name)
        assert num_entities['row_count'] == default_nb
        
        # release collection
        self.release_collection(user_client, collection_name)
        
        # drop collection
        self.drop_collection(user_client, collection_name)
        
        # drop user
        self.drop_user(client, user_name=user_name)

    @pytest.mark.parametrize("with_db", [False, True])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_verify_grant_collection_load_privilege(self, host, port, with_db):
        """
        target: test milvus client api verify grant collection load privilege
        method: verify grant collection load privilege
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user and role
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        
        # handle database parameter
        if with_db:
            db_name = cf.gen_unique_str(prefix)
            self.create_database(client, db_name)
            self.using_database(client, db_name)
        else:
            db_name = "default"
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # grant collection load privileges to role
        self.grant_privilege(client, role_name, "Collection", "Load", collection_name, db_name=db_name)
        self.grant_privilege(client, role_name, "Collection", "GetLoadingProgress", collection_name, db_name=db_name)
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # test load privilege
        self.using_database(user_client, db_name)
        self.load_collection(user_client, collection_name)
        
        # cleanup
        self.drop_collection(client, collection_name)
        if with_db:
            self.using_database(client, "default")
            self.drop_database(client, db_name)

    @pytest.mark.parametrize("with_db", [False, True])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_verify_grant_collection_release_privilege(self, host, port, with_db):
        """
        target: test milvus client api verify grant collection release privilege
        method: verify grant collection release privilege
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user and role
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        
        # handle database parameter
        if with_db:
            db_name = cf.gen_unique_str(prefix)
            self.create_database(client, db_name)
            self.using_database(client, db_name)
        else:
            db_name = "default"
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # load collection
        self.load_collection(client, collection_name)
        
        # grant collection release privilege to role
        self.grant_privilege(client, role_name, "Collection", "Release", collection_name, db_name=db_name)
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # test release privilege
        self.using_database(user_client, db_name)
        self.release_collection(user_client, collection_name)
        
        # cleanup
        self.drop_collection(client, collection_name)
        if with_db:
            self.using_database(client, "default")
            self.drop_database(client, db_name)

    @pytest.mark.parametrize("with_db", [False, True])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_verify_grant_collection_insert_privilege(self, host, port, with_db):
        """
        target: test milvus client api verify grant collection insert privilege
        method: verify grant collection insert privilege
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user and role
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        
        # handle database parameter
        if with_db:
            db_name = cf.gen_unique_str(prefix)
            self.create_database(client, db_name)
            self.using_database(client, db_name)
        else:
            db_name = "default"
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # grant collection insert privilege to role
        self.grant_privilege(client, role_name, "Collection", "Insert", collection_name, db_name=db_name)
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # test insert privilege
        self.using_database(user_client, db_name)
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(user_client, collection_name, rows)
        
        # cleanup
        self.drop_collection(client, collection_name)
        if with_db:
            self.using_database(client, "default")
            self.drop_database(client, db_name)

    @pytest.mark.parametrize("with_db", [False, True])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_verify_grant_collection_delete_privilege(self, host, port, with_db):
        """
        target: test milvus client api verify grant collection delete privilege
        method: verify grant collection delete privilege
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user and role
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        
        # handle database parameter
        if with_db:
            db_name = cf.gen_unique_str(prefix)
            self.create_database(client, db_name)
            self.using_database(client, db_name)
        else:
            db_name = "default"
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        # grant collection delete privilege to role
        self.grant_privilege(client, role_name, "Collection", "Delete", collection_name, db_name=db_name)
        
        # wait for privilege to take effect
        time.sleep(10)
        
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # test delete privilege
        self.using_database(user_client, db_name)
        delete_expr = f'{default_primary_key_field_name} == 0'
        self.delete(user_client, collection_name, filter=delete_expr)
        
        # cleanup
        self.drop_collection(client, collection_name)
        if with_db:
            self.using_database(client, "default")
            self.drop_database(client, db_name)

    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_new_user_default_owns_public_role_permission(self, host, port):
        """
        target: test milvus client api new user owns public role privilege
        method: create a role, verify its permission
        expected: verify success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        collection_name = cf.gen_unique_str(prefix)
        collection_name_2 = cf.gen_unique_str(prefix)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user
        self.create_user(client, user_name=user_name, password=password)
        
        # create collection
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        
        # create index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name=default_vector_field_name)
        self.create_index(client, collection_name, index_params)
        
        # create new client with the user credentials
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        
        # wait for user to be created
        time.sleep(10)
        
        # test collection permission deny (new user has no privileges)
        self.load_collection(user_client, collection_name, check_task=CheckTasks.check_permission_deny)
        self.release_collection(user_client, collection_name, check_task=CheckTasks.check_permission_deny)
        
        # test insert permission deny
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(user_client, collection_name, rows, check_task=CheckTasks.check_permission_deny)
        
        # test delete permission deny
        delete_expr = f'{default_primary_key_field_name} == 0'
        self.delete(user_client, collection_name, filter=delete_expr, check_task=CheckTasks.check_permission_deny)
        
        # test search permission deny
        vectors_to_search = rng.random((1, default_dim))
        self.search(user_client, collection_name, vectors_to_search, check_task=CheckTasks.check_permission_deny)
        
        # test query permission deny
        query_expr = f'{default_primary_key_field_name} in [0, 1]'
        self.query(user_client, collection_name, filter=query_expr, check_task=CheckTasks.check_permission_deny)
        
        # test global permission deny
        self.create_collection(user_client, collection_name_2, default_dim, consistency_level="Strong", check_task=CheckTasks.check_permission_deny)
        self.drop_collection(user_client, collection_name, check_task=CheckTasks.check_permission_deny)
        self.create_user(user_client, user_name=collection_name, password=password, check_task=CheckTasks.check_permission_deny)
        self.create_role(user_client, role_name=role_name, check_task=CheckTasks.check_permission_deny)
        self.drop_user(user_client, user_name=user_name, check_task=CheckTasks.check_permission_deny)
        
        # cleanup
        self.drop_collection(client, collection_name)
        self.drop_user(client, user_name=user_name)

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/44843")
    def test_milvus_client_remove_user_from_default_role(self, role_name, host, port):
        """
        target: test milvus client api remove user from default role (admin or public)
        method: create a user, add user to admin role or public role, remove user from role
        expected: remove success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        
        # create user
        self.create_user(client, user_name=user_name, password=password)
        
        # grant role to user (add user to role)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        
        # verify user has the role
        user_info, _ = self.describe_user(client, user_name=user_name)
        assert user_info["user_name"] == user_name
        assert role_name in user_info.get("roles", [])
        
        # revoke role from user (remove user from role)
        self.revoke_role(client, user_name=user_name, role_name=role_name)
        
        # verify user no longer has the role
        user_info, _ = self.describe_user(client, user_name=user_name)
        assert user_info["user_name"] == user_name
        assert role_name not in user_info.get("roles", [])
        
        # cleanup
        self.drop_user(client, user_name=user_name)

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_drop_admin_and_public_role(self, role_name, host, port):
        """
        target: test milvus client api drop admin and public role fail
        method: drop admin and public role fail
        expected: fail to drop
        """
        client = self._client()
        
        # verify role exists
        roles, _ = self.list_roles(client)
        assert role_name in roles
        
        # try to drop default role (should fail)
        error_msg = f"the role[{role_name}] is a default role, which can't be dropped"
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 1401, ct.err_msg: error_msg})
        
        # verify role still exists
        roles, _ = self.list_roles(client)
        assert role_name in roles

    def test_milvus_client_add_user_not_exist_role(self, host, port):
        """
        target: test milvus client api add user to not exist role
        method: create a user, add user to not exist role
        expected: fail to add
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user
        self.create_user(client, user_name=user_name, password=password)
        
        # verify role does not exist
        roles, _ = self.list_roles(client)
        assert role_name not in roles
        
        # try to grant non-existent role to user (should fail)
        error_msg = "not found the role, maybe the role isn't existed or internal system error"
        self.grant_role(client, user_name=user_name, role_name=role_name, check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65535, ct.err_msg: error_msg})
        
        # cleanup
        self.drop_user(client, user_name=user_name)

    def test_milvus_client_remove_user_from_empty_role(self, host, port):
        """
        target: test milvus client api remove not exist user from role
        method: create new role, remove not exist user from unbind role
        expected: fail to remove
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        role_name = cf.gen_unique_str(role_pre)
        
        # create user
        self.create_user(client, user_name=user_name, password=password)
        
        # create role
        self.create_role(client, role_name=role_name)
        
        # verify role does not have the user
        role_info, _ = self.describe_role(client, role_name=role_name)
        role_users = role_info.get("users", [])
        assert user_name not in role_users
        
        # try to revoke user from role (should fail since user is not in the role)
        error_msg = "not found the role, maybe the role isn't existed or internal system error"
        self.revoke_role(client, user_name=user_name, role_name=role_name)
        
        # verify user is still not in the role
        role_info, _ = self.describe_role(client, role_name=role_name)
        role_users = role_info.get("users", [])
        assert user_name not in role_users
        
        # cleanup
        self.drop_role(client, role_name=role_name)
        self.drop_user(client, user_name=user_name)

    def test_milvus_client_list_grant_by_not_exist_role(self, host, port):
        """
        target: test milvus client api list grants by not exist role
        method: list grants by not exist role
        expected: fail to list
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        
        # verify role does not exist
        roles, _ = self.list_roles(client)
        assert role_name not in roles
        
        # try to describe non-existent role (should fail)
        error_msg = "not found the role, maybe the role isn't existed or internal system error"
        self.describe_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535, ct.err_msg: error_msg})




@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacPrefixIsolation(TestMilvusClientV2Base):
    """PR #48053 / Issue #47998: RBAC prefix isolation tests.

    Bug: When two usernames have a prefix relationship (e.g. user1 / user11),
    getRolesByUsername returns empty-string roles due to path.Join stripping
    trailing slash in etcd LoadWithPrefix.
    """

    DIM = 128

    def teardown_method(self, method):
        """Clean up entities created by this test, then call super() for connection cleanup."""
        if not hasattr(self, '_p'):
            super().teardown_method(method)
            return
        log.info(f"[pr48053 teardown] cleaning up {method.__name__}, prefix={self._p}")
        client = self._client()

        # Use force_drop to skip manual revoke of privileges.
        # Order: drop roles (force) → drop users → drop privilege groups
        #        → drop aliases → drop collections → drop databases

        for role in [self._role1, self._role10, self._role1_read]:
            try:
                client.drop_role(role, force_drop=True)
            except Exception:
                pass

        for user in [self._user1, self._user11, self._user1_ro]:
            try:
                client.drop_user(user)
            except Exception:
                pass

        for pg in [self._pg1, self._pg10, self._pg1_ext]:
            try:
                client.drop_privilege_group(pg)
            except Exception:
                pass

        for alias in [self._alias1, self._alias10, self._alias1_bak]:
            try:
                client.drop_alias(alias)
            except Exception:
                pass

        for col in [self._col, self._col_v2, self._collection]:
            try:
                client.drop_collection(col)
            except Exception:
                pass

        for db in [self._db1, self._db10]:
            try:
                client.using_database(db)
                client.drop_collection("inner_col")
            except Exception:
                pass
            try:
                client.using_database("default")
                client.drop_database(db)
            except Exception:
                pass

        super().teardown_method(method)

    def test_prefix_isolation_all(self, host, port):
        """All UpgradeCheck cases (#01~#26) in a single test method.

        Before upgrade (bug present):
          - #03 DescribeUser(user1): FAIL (empty-string role)
          - #23 ListDatabases as user1: FAIL (role entity invalid)
        After upgrade (bug fixed): all pass.
        """
        client = self._client()
        uri = f"http://{host}:{port}"

        # ============================================================
        # Setup: create all test data with random prefix
        # ============================================================
        p = cf.gen_unique_str("pr48053")
        self._p = p

        # Names with prefix collision
        self._user1 = f"{p}_user1"
        self._user11 = f"{p}_user11"
        self._user1_ro = f"{p}_user1_ro"
        self._pw1 = cf.gen_str_by_length(contain_numbers=True)
        self._pw11 = cf.gen_str_by_length(contain_numbers=True)
        self._pw1_ro = cf.gen_str_by_length(contain_numbers=True)

        self._role1 = f"{p}_role1"
        self._role10 = f"{p}_role10"
        self._role1_read = f"{p}_role1_read"

        self._col = f"{p}_col"
        self._col_v2 = f"{p}_col_v2"
        self._collection = f"{p}_collection"

        self._alias1 = f"{p}_alias1"
        self._alias10 = f"{p}_alias10"
        self._alias1_bak = f"{p}_alias1_bak"

        self._db1 = f"{p}_db1"
        self._db10 = f"{p}_db10"

        self._pg1 = f"{p}_pg1"
        self._pg10 = f"{p}_pg10"
        self._pg1_ext = f"{p}_pg1_extended"

        dim = self.DIM

        # Collections
        for col_name in [self._col, self._col_v2, self._collection]:
            schema, _ = self.create_schema(client)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
            self.create_collection(client, col_name, schema=schema, force_teardown=False)
            self.insert(client, col_name, [{"id": i, "vec": np.random.random(dim).tolist()} for i in range(10)])
            idx = client.prepare_index_params()
            idx.add_index(field_name="vec", index_type="FLAT", metric_type="COSINE")
            self.create_index(client, col_name, idx)
            self.load_collection(client, col_name)

        # Aliases
        self.create_alias(client, self._col, self._alias1)
        self.create_alias(client, self._col_v2, self._alias10)
        self.create_alias(client, self._collection, self._alias1_bak)

        # Databases + inner collections
        for db_name in [self._db1, self._db10]:
            self.create_database(client, db_name)
            self.using_database(client, db_name)
            schema, _ = self.create_schema(client)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
            self.create_collection(client, "inner_col", schema=schema, force_teardown=False)
            self.insert(client, "inner_col", [{"id": i, "vec": np.random.random(dim).tolist()} for i in range(10)])
            idx = client.prepare_index_params()
            idx.add_index(field_name="vec", index_type="FLAT", metric_type="COSINE")
            self.create_index(client, "inner_col", idx)
            self.load_collection(client, "inner_col")
            self.using_database(client, "default")

        # Roles
        for role in [self._role1, self._role10, self._role1_read]:
            self.create_role(client, role)

        # Users + bindings
        self.create_user(client, self._user1, self._pw1)
        self.create_user(client, self._user11, self._pw11)
        self.create_user(client, self._user1_ro, self._pw1_ro)
        self.grant_role(client, self._user1, self._role1)
        self.grant_role(client, self._user11, self._role10)
        self.grant_role(client, self._user1_ro, self._role1_read)

        # Privilege groups
        self.create_privilege_group(client, self._pg1)
        self.add_privileges_to_group(client, self._pg1, ["Search", "Query"])
        self.create_privilege_group(client, self._pg10)
        self.add_privileges_to_group(client, self._pg10, ["Insert", "Delete", "Upsert"])
        self.create_privilege_group(client, self._pg1_ext)
        self.add_privileges_to_group(client, self._pg1_ext, ["Search", "Query", "Load", "Release"])

        # Grants (CreateCollection db must be "default" not "*" for #23 to reproduce)
        grants = [
            (self._role1,      "Search",           self._col,        "default"),
            (self._role1,      "Query",            self._col,        "default"),
            (self._role10,     "Insert",           self._col_v2,     "default"),
            (self._role10,     "Search",           self._col_v2,     "default"),
            (self._role1_read, "Search",           self._collection, "default"),
            (self._role1,      "CreateCollection", "*",              "default"),
            (self._role1,      "Search",           "inner_col",      self._db1),
            (self._role10,     "Search",           "inner_col",      self._db10),
            (self._role1_read, self._pg1,          self._col_v2,     "default"),
        ]
        for role, priv, col, db in grants:
            self.grant_privilege_v2(client, role, priv, col, db)

        time.sleep(2)
        log.info(f"[pr48053] setup complete, prefix={p}")

        # ============================================================
        # #01~#10: RBAC metadata integrity
        # ============================================================

        # #01: ListUsers
        users, _ = self.list_users(client)
        for u in [self._user1, self._user11, self._user1_ro]:
            assert u in users, f"#01: {u} not in {users}"

        # #02: ListRoles
        roles, _ = self.list_roles(client)
        for r in [self._role1, self._role10, self._role1_read]:
            assert r in roles, f"#02: {r} not in {roles}"

        # #03: DescribeUser(user1) - BUG POINT
        # Bug #47998: user1 is prefix of user11/user1_ro, returns ['role1', '', '']
        result, _ = self.describe_user(client, self._user1)
        roles_u1 = list(result.get("roles", []))
        assert "" not in roles_u1, f"#03 Bug #47998: empty-string role, roles={roles_u1}"
        assert self._role1 in roles_u1, f"#03: should contain {self._role1}, roles={roles_u1}"

        # #04: DescribeUser(user11)
        result, _ = self.describe_user(client, self._user11)
        roles_u11 = list(result.get("roles", []))
        assert self._role10 in roles_u11 and self._role1 not in roles_u11 and "" not in roles_u11, \
            f"#04: roles={roles_u11}"

        # #05: DescribeUser(user1_ro)
        result, _ = self.describe_user(client, self._user1_ro)
        roles_uro = list(result.get("roles", []))
        assert self._role1_read in roles_uro and self._role1 not in roles_uro and "" not in roles_uro, \
            f"#05: roles={roles_uro}"

        # #06: DescribeRole(role1) no col_v2
        result, _ = self.describe_role(client, self._role1)
        privs = result.get("privileges", [])
        col_v2_privs = [p for p in privs if self._col_v2 in str(p.get("object_name", ""))]
        assert len(col_v2_privs) == 0, f"#06: role1 should not have col_v2: {col_v2_privs}"
        assert len(privs) > 0, "#06: role1 should have privileges"

        # #07: DescribeRole(role10) no col
        result, _ = self.describe_role(client, self._role10)
        privs = result.get("privileges", [])
        col_only = [p for p in privs if p.get("object_name") == self._col]
        assert len(col_only) == 0, f"#07: role10 should not have col: {col_only}"

        # #08: DescribeRole(role1_read) no role1's Query/CreateCollection
        result, _ = self.describe_role(client, self._role1_read)
        privs = result.get("privileges", [])
        has_query = any(p.get("privilege") == "Query" and p.get("object_name") == self._col for p in privs)
        has_create = any(p.get("privilege") == "CreateCollection" for p in privs)
        assert not has_query and not has_create, f"#08: Query={has_query}, CreateCollection={has_create}"

        # #09: ListPrivilegeGroups
        groups, _ = self.list_privilege_groups(client)
        names = [g.get("privilege_group", g.get("group_name", "")) for g in groups]
        for pg in [self._pg1, self._pg10, self._pg1_ext]:
            assert pg in names, f"#09: {pg} not in {names}"

        # #10: DescribeRole(role1_read) has pg1 on col_v2
        result, _ = self.describe_role(client, self._role1_read)
        privs = result.get("privileges", [])
        has_pg1 = any(
            self._pg1 in str(p.get("privilege", "")) and self._col_v2 in str(p.get("object_name", ""))
            for p in privs
        )
        assert has_pg1, f"#10: should have {self._pg1} on {self._col_v2}, privs={privs}"

        # ============================================================
        # #11~#20: Permission isolation
        # Uses init_milvus_client per user + check_task for permission checks
        # ============================================================

        # #11: user1 Search col -> success
        uc1, _ = self.init_milvus_client(uri=uri, user=self._user1, password=self._pw1)
        self.search(uc1, self._col, data=[np.random.random(dim).tolist()])

        # #12: user1 Insert col_v2 -> denied
        self.insert(uc1, self._col_v2, [{"id": 9999, "vec": np.random.random(dim).tolist()}],
                    check_task=CheckTasks.check_permission_deny)

        # #13: user1 Search col_v2 -> denied
        self.search(uc1, self._col_v2, data=[np.random.random(dim).tolist()],
                    check_task=CheckTasks.check_permission_deny)

        # #14: user11 Search col -> denied
        uc11, _ = self.init_milvus_client(uri=uri, user=self._user11, password=self._pw11)
        self.search(uc11, self._col, data=[np.random.random(dim).tolist()],
                    check_task=CheckTasks.check_permission_deny)

        # #15: user11 Query col -> denied
        self.query(uc11, self._col, filter="id >= 0",
                   check_task=CheckTasks.check_permission_deny)

        # #16: user1_ro Insert col -> denied
        uc_ro, _ = self.init_milvus_client(uri=uri, user=self._user1_ro, password=self._pw1_ro)
        self.insert(uc_ro, self._col, [{"id": 9999, "vec": np.random.random(dim).tolist()}],
                    check_task=CheckTasks.check_permission_deny)

        # #17: user1_ro Search col_v2 -> success via pg1
        self.search(uc_ro, self._col_v2, data=[np.random.random(dim).tolist()])

        # #18: user1_ro Insert col_v2 -> denied (pg1 has no Insert)
        self.insert(uc_ro, self._col_v2, [{"id": 9999, "vec": np.random.random(dim).tolist()}],
                    check_task=CheckTasks.check_permission_deny)

        # #19: DescribeRole(role1) grants no col_v2
        result, _ = self.describe_role(client, self._role1)
        col_v2 = [p for p in result.get("privileges", []) if self._col_v2 in str(p)]
        assert len(col_v2) == 0, f"#19: role1 should not have col_v2: {col_v2}"

        # #20: DescribeRole(role10) grants no col
        result, _ = self.describe_role(client, self._role10)
        col_only = [p for p in result.get("privileges", []) if p.get("object_name") == self._col]
        assert len(col_only) == 0, f"#20: role10 should not have col: {col_only}"

        # ============================================================
        # #21~#22: Alias
        # ============================================================

        # #21: ListAliases
        all_aliases = []
        for col_name in [self._col, self._col_v2, self._collection]:
            result, _ = self.list_aliases(client, col_name)
            if isinstance(result, dict):
                all_aliases.extend(result.get("aliases", []))
            elif isinstance(result, list):
                all_aliases.extend(result)
        for alias in [self._alias1, self._alias10, self._alias1_bak]:
            assert alias in all_aliases, f"#21: {alias} not in {all_aliases}"

        # #22: Search via alias1
        result, _ = self.search(client, self._alias1, data=[np.random.random(dim).tolist()])
        assert len(result) > 0, "#22: search via alias1 returned no results"

        # ============================================================
        # #23~#26: Multi-DB
        # ============================================================

        # #23: user1 ListDatabases - BUG POINT
        # Bug #47998: getCurrentUserVisibleDatabases -> getRolesByUsername
        # -> empty role -> SelectGrant error
        user_client, _ = self.init_milvus_client(uri=uri, user=self._user1, password=self._pw1)
        self.list_databases(user_client)

        # #25: user1 Search db1.inner_col -> success
        uc1_db1, _ = self.init_milvus_client(uri=uri, user=self._user1, password=self._pw1,
                                              db_name=self._db1)
        self.search(uc1_db1, "inner_col", data=[np.random.random(dim).tolist()])

        # #26: user1 Search db10.inner_col -> denied
        uc1_db10, _ = self.init_milvus_client(uri=uri, user=self._user1, password=self._pw1,
                                               db_name=self._db10)
        self.search(uc1_db10, "inner_col", data=[np.random.random(dim).tolist()],
                    check_task=CheckTasks.check_permission_deny)

        log.info(f"[pr48053] all checks passed, prefix={p}")
