import copy
import time
import random
import numpy as np

import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks


prefix = "client_rbac"
user_pre = "user"
role_pre = "role"
root_token = "root:Milvus"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


def _teardown_rbac(test_instance):
    """Common teardown: drop all non-default users, roles, privilege groups, and databases"""
    client = test_instance._client()

    # drop users (revoke roles first)
    users, _ = test_instance.list_users(client)
    for user in users:
        if user != ct.default_user:
            user_info, _ = test_instance.describe_user(client, user)
            if user_info and user_info.get("roles"):
                for role in user_info["roles"]:
                    try:
                        test_instance.revoke_role(client, user, role)
                    except Exception:
                        pass
            test_instance.drop_user(client, user)

    # collect all dbs for cross-db privilege revocation
    dbs, _ = test_instance.list_databases(client)

    # drop roles (revoke privileges across all dbs first)
    roles, _ = test_instance.list_roles(client)
    for role in roles:
        if role not in ['admin', 'public']:
            for db in dbs:
                role_info, _ = test_instance.describe_role(client, role, db_name=db)
                if role_info and role_info.get('privileges'):
                    for priv in role_info['privileges']:
                        try:
                            test_instance.revoke_privilege(client, role, priv["object_type"],
                                                           priv["privilege"], priv["object_name"],
                                                           db_name=priv.get("db_name", ""))
                        except Exception:
                            try:
                                test_instance.revoke_privilege_v2(client, role, priv["privilege"],
                                                                   priv.get("object_name", "*"),
                                                                   db_name=priv.get("db_name", "*"))
                            except Exception:
                                pass
            test_instance.drop_role(client, role)

    # drop custom privilege groups
    groups, _ = test_instance.list_privilege_groups(client)
    for g in groups:
        if g.get("privilege_group") not in ct.built_in_privilege_groups:
            test_instance.drop_privilege_group(client, g["privilege_group"])

    # drop databases
    for db in dbs:
        if db != ct.default_db:
            test_instance.using_database(client, db)
            colls, _ = test_instance.list_collections(client)
            for c in colls:
                test_instance.drop_collection(client, c)
            test_instance.using_database(client, "default")
            test_instance.drop_database(client, db)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacBase(TestMilvusClientV2Base):
    """Test case of basic RBAC interface: user, role, privilege CRUD"""

    def teardown_method(self, method):
        log.info("[teardown_method] Start teardown RBAC test cases ...")
        _teardown_rbac(self)
        super().teardown_method(method)

    # ==================== Connection Tests ====================

    def test_milvus_client_connect_using_token(self, host, port):
        """
        target: test init milvus client using token
        method: init milvus client with only token
        expected: init successfully
        """
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, token=root_token)
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
        res = self.list_databases(client)[0]
        assert res != []

    # ==================== User Management Tests ====================

    def test_milvus_client_create_user(self, host, port):
        """
        target: test milvus client api create_user
        method: create user and verify login
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        res = self.list_databases(client)[0]
        assert res == []

    def test_milvus_client_drop_user(self, host, port):
        """
        target: test milvus client api drop_user
        method: drop user that exists and not exists
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        self.drop_user(client, user_name=user_name)
        not_exist_user_name = cf.gen_unique_str(user_pre)
        self.drop_user(client, user_name=not_exist_user_name)

    def test_milvus_client_update_password(self, host, port):
        """
        target: test milvus client api update_password
        method: create a user, update password, verify new password works and old doesn't
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        new_password = cf.gen_str_by_length()
        self.update_password(client, user_name=user_name, old_password=password, new_password=new_password)
        self.close(client)
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
        method: create users and list them
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
        method: describe root, new user, and non-existent user
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length()
        self.create_user(client, user_name=user_name, password=password)
        res, _ = self.describe_user(client, user_name=ct.default_user)
        assert res["user_name"] == ct.default_user
        res, _ = self.describe_user(client, user_name=user_name)
        assert res["user_name"] == user_name
        user_not_exist = cf.gen_unique_str(user_pre)
        res, _ = self.describe_user(client, user_name=user_not_exist)
        assert res == {}

    # ==================== Role Management Tests ====================

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

    # ==================== Role Binding Tests ====================

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
        method: grant role then revoke
        expected: succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.revoke_role(client, user_name=user_name, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        self.revoke_role(client, user_name=user_name, role_name=role_name)

    # ==================== Privilege Tests ====================

    def test_milvus_client_grant_privilege(self, host, port):
        """
        target: test milvus client api grant_privilege
        method: create a role and user, grant privilege, verify it works
        expected: succeed
        """
        client_root = self._client()
        coll_name = cf.gen_unique_str()
        self.create_collection(client_root, coll_name, default_dim, consistency_level="Strong")
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client_root, user_name=user_name, password=password)
        self.create_role(client_root, role_name=role_name)
        self.grant_role(client_root, user_name=user_name, role_name=role_name)
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        self.drop_collection(client, coll_name, check_task=CheckTasks.check_permission_deny)
        self.grant_privilege(client_root, role_name, "Global", "DropCollection", "*")
        time.sleep(10)
        self.drop_collection(client, coll_name)

    def test_milvus_client_revoke_privilege(self, host, port):
        """
        target: test milvus client api revoke_privilege
        method: grant CreateCollection then revoke, verify permission denied
        expected: succeed
        """
        client_root = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client_root, user_name=user_name, password=password)
        self.create_role(client_root, role_name=role_name)
        self.grant_role(client_root, user_name=user_name, role_name=role_name)
        # grant privilege and verify it works
        self.grant_privilege(client_root, role_name, "Global", "CreateCollection", "*")
        self.grant_privilege(client_root, role_name, "Global", "All", "*")
        time.sleep(10)
        uri = f"http://{host}:{port}"
        client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        self.drop_collection(client, coll_name)
        # revoke and verify denied
        self.revoke_privilege(client_root, role_name, "Global", "All", "*")
        self.revoke_privilege(client_root, role_name, "Global", "CreateCollection", "*")
        # reconnect to bypass proxy cache
        time.sleep(10)
        client2, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        coll_name_2 = cf.gen_unique_str(prefix)
        self.create_collection(client2, coll_name_2, default_dim,
                               check_task=CheckTasks.check_permission_deny)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacInvalid(TestMilvusClientV2Base):
    """Test case of RBAC invalid parameters"""

    # ==================== Invalid Connection ====================

    def test_milvus_client_init_token_invalid(self, host, port):
        uri = f"http://{host}:{port}"
        self.init_milvus_client(uri=uri, token=root_token + "kk",
                                check_task=CheckTasks.check_auth_failure)

    def test_milvus_client_init_username_invalid(self, host, port):
        uri = f"http://{host}:{port}"
        self.init_milvus_client(uri=uri, user=ct.default_user + "nn",
                                password=ct.default_password,
                                check_task=CheckTasks.check_auth_failure)

    def test_milvus_client_init_password_invalid(self, host, port):
        uri = f"http://{host}:{port}"
        self.init_milvus_client(uri=uri, user=ct.default_user,
                                password=ct.default_password + "kk",
                                check_task=CheckTasks.check_auth_failure)

    # ==================== Invalid User Operations ====================

    @pytest.mark.parametrize("invalid_name", ["", "0", "n@me", "h h"])
    def test_milvus_client_create_user_value_invalid(self, host, port, invalid_name):
        client = self._client()
        self.create_user(client, invalid_name, ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1100, ct.err_msg: "invalid parameter"})

    @pytest.mark.parametrize("invalid_name", [1, [], None, {}])
    def test_milvus_client_create_user_type_invalid(self, host, port, invalid_name):
        client = self._client()
        self.create_user(client, invalid_name, ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1,
                                      ct.err_msg: f"`user` value {invalid_name} is illegal"})

    def test_milvus_client_create_user_exist(self, host, port):
        client = self._client()
        self.create_user(client, "root", ct.default_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 65535, ct.err_msg: "user already exists: root"})

    @pytest.mark.parametrize("invalid_password", ["", "0", "p@ss", "h h", "1+1=2"])
    def test_milvus_client_create_user_password_invalid_value(self, host, port, invalid_password):
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        self.create_user(client, user_name, invalid_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1100, ct.err_msg: "invalid password"})

    @pytest.mark.parametrize("invalid_password", [1, [], None, {}])
    def test_milvus_client_create_user_password_invalid_type(self, host, port, invalid_password):
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        self.create_user(client, user_name, invalid_password,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1,
                                      ct.err_msg: f"`password` value {invalid_password} is illegal"})

    def test_milvus_client_delete_user_root(self, host, port):
        """
        target: test deleting root user
        method: try to delete root user
        expected: fail with error
        """
        client = self._client()
        self.drop_user(client, user_name=ct.default_user,
                       check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 1401,
                                    ct.err_msg: "root user cannot be deleted"})

    # ==================== Invalid Password Operations ====================

    def test_milvus_client_update_password_user_not_exist(self, host, port):
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
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.update_password(client, user_name=user_name, old_password=password, new_password=password)

    @pytest.mark.parametrize("invalid_password", ["", "0", "p@ss", "h h", "1+1=2"])
    def test_milvus_client_update_password_new_password_invalid(self, host, port, invalid_password):
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.update_password(client, user_name=user_name, old_password=password,
                             new_password=invalid_password, check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 1100, ct.err_msg: "invalid password"})

    # ==================== Invalid Role Operations ====================

    def test_milvus_client_create_role_exist(self, host, port):
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        error_msg = f'role [name:"{role_name}"] already exists'
        self.create_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 65535, ct.err_msg: error_msg})
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("name", ["longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong"
                                      "longlonglonglong",
                                      "n%$#@!", "123n", " ", "''", "test-role", "ff ff", "中文"])
    def test_milvus_client_create_role_invalid_name(self, name, host, port):
        """
        target: create role with invalid name
        method: create role with invalid name
        expected: create fail
        """
        client = self._client()
        self.create_role(client, role_name=name,
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1100, ct.err_msg: "invalid parameter"})

    def test_milvus_client_drop_role_invalid(self, host, port):
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 65535,
                                    ct.err_msg: "not found the role, maybe the role isn't "
                                                "existed or internal system error"})

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_drop_built_in_role(self, host, port, role_name):
        client = self._client()
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 65535,
                                    ct.err_msg: f"the role[{role_name}] is a default role, "
                                                f"which can't be dropped"})

    def test_milvus_client_describe_role_invalid(self, host, port):
        client = self._client()
        role_not_exist = cf.gen_unique_str(role_pre)
        self.describe_role(client, role_name=role_not_exist, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535,
                                        ct.err_msg: "not found the role, maybe the role isn't "
                                                    "existed or internal system error"})

    def test_milvus_client_create_over_max_roles(self, host, port):
        """
        target: test create roles over max num
        method: create more roles than the limit
        expected: raise exception
        """
        client = self._client()
        # 2 original roles: admin, public
        for i in range(ct.max_role_num - 2):
            role_name = f"role_{i}"
            self.create_role(client, role_name=role_name)
        # now at max, creating one more should fail
        self.create_role(client, role_name="role_overflow",
                         check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 35,
                                      ct.err_msg: "unable to create role because the number of roles "
                                                  "has reached the limit"})
        # cleanup
        for i in range(ct.max_role_num - 2):
            self.drop_role(client, role_name=f"role_{i}")

    def test_milvus_client_drop_role_with_bind_privilege(self, host, port):
        """
        target: drop role with bound privilege
        method: create a role, grant privilege, try to drop
        expected: fail to drop
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_privilege(client, role_name, "Collection", "*", "*")
        error = {ct.err_code: 36,
                 ct.err_msg: "fail to drop the role that it has privileges"}
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items=error)
        # cleanup
        self.revoke_privilege(client, role_name, "Collection", "*", "*")
        self.drop_role(client, role_name=role_name)

    # ==================== Invalid Grant/Revoke ====================

    def test_milvus_client_grant_role_user_not_exist(self, host, port):
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65536,
                                     ct.err_msg: "not found the user, maybe the user "
                                                 "isn't existed or internal system error"})
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_grant_role_role_not_exist(self, host, port):
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

    def test_milvus_client_grant_privilege_object_not_exist(self, host, port):
        """
        target: grant privilege with non-existent object type
        method: grant privilege with invalid object type
        expected: fail
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        o_name = cf.gen_unique_str(prefix)
        self.create_role(client, role_name=role_name)
        self.grant_privilege(client, role_name, o_name, "*", "*",
                             check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 65535,
                                          ct.err_msg: "the object entity in the request is nil or invalid"})
        self.drop_role(client, role_name=role_name)

    def test_milvus_client_grant_privilege_privilege_not_exist(self, host, port):
        """
        target: grant privilege with non-existent privilege name
        method: grant privilege with invalid privilege
        expected: fail
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        p_name = cf.gen_unique_str(prefix)
        self.create_role(client, role_name=role_name)
        self.grant_privilege(client, role_name, "Global", p_name, "*",
                             check_task=CheckTasks.err_res,
                             check_items={ct.err_code: 65535,
                                          ct.err_msg: f"not found the privilege name[{p_name}]"})
        self.drop_role(client, role_name=role_name)

    # ==================== Invalid Privilege Group Operations ====================

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_create_privilege_group_invalid_type(self, name, host, port):
        client = self._client()
        self.create_privilege_group(client, privilege_group=name,
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1,
                                                 ct.err_msg: f"`privilege_group` value {name} is illegal"})

    @pytest.mark.parametrize("name", ["n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_create_privilege_group_invalid_value(self, name, host, port):
        client = self._client()
        self.create_privilege_group(client, privilege_group=name,
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100,
                                                 ct.err_msg: "can only contain numbers, letters and underscores"})

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_drop_privilege_group_invalid_type(self, name, host, port):
        client = self._client()
        self.drop_privilege_group(client, privilege_group=name,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 1,
                                               ct.err_msg: f"`privilege_group` value {name} is illegal"})

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_add_privileges_to_group_invalid_type(self, name, host, port):
        client = self._client()
        self.add_privileges_to_group(client, privilege_group=name, privileges=["Insert"],
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1,
                                                  ct.err_msg: f"`privilege_group` value {name} is illegal"})

    @pytest.mark.parametrize("name", ["n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_add_privileges_to_group_invalid_value(self, name, host, port):
        client = self._client()
        self.add_privileges_to_group(client, privilege_group=name, privileges=["Insert"],
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1100,
                                                  ct.err_msg: "can only contain numbers, letters and underscores"})

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff", "invalid_privilege"])
    def test_milvus_client_add_privileges_to_group_privilege_invalid(self, name, host, port):
        client = self._client()
        self.add_privileges_to_group(client, privilege_group="pg_1", privileges=name,
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1,
                                                  ct.err_msg: f"`privileges` value {name} is illegal"})

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff"])
    def test_milvus_client_remove_privileges_from_group_invalid_name(self, name, host, port):
        client = self._client()
        self.remove_privileges_from_group(client, privilege_group=name, privileges=["Insert"],
                                          check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1,
                                                       ct.err_msg: f"{name}"})

    @pytest.mark.parametrize("name", [1, 1.0, "n%$#@!", "test-role", "ff ff", "invalid_privilege"])
    def test_milvus_client_remove_privileges_from_group_privilege_invalid(self, name, host, port):
        client = self._client()
        self.remove_privileges_from_group(client, privilege_group="pg_1", privileges=name,
                                          check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1,
                                                       ct.err_msg: f"`privileges` value {name} is illegal"})

    # ==================== Invalid Grant V2 ====================

    @pytest.mark.parametrize("name", [1, 1.0])
    def test_milvus_client_grant_v2_privilege_invalid_type(self, name, host, port):
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_privilege_v2(client, role_name, privilege=name, collection_name=coll_name,
                                check_task=CheckTasks.err_res,
                                check_items={ct.err_code: 1,
                                             ct.err_msg: f"`privilege` value {name} is illegal"})
        self.drop_role(client, role_name=role_name)
        self.drop_collection(client, coll_name)

    def test_milvus_client_grant_v2_privilege_invalid_value(self, host, port):
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_privilege_v2(client, role_name, privilege="invalid_privilege",
                                collection_name=coll_name,
                                check_task=CheckTasks.err_res,
                                check_items={ct.err_code: 65535,
                                             ct.err_msg: "not found the privilege name[invalid_privilege]"})
        self.drop_role(client, role_name=role_name)
        self.drop_collection(client, coll_name)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacAdvance(TestMilvusClientV2Base):
    """Test case of advanced RBAC scenarios"""

    def teardown_method(self, method):
        log.info("[teardown_method] Start teardown advanced RBAC test cases ...")
        _teardown_rbac(self)
        super().teardown_method(method)

    # ==================== Role Binding Advanced ====================

    def test_milvus_client_drop_role_which_bind_user(self, host, port):
        """
        target: drop role which has user bound to it
        method: create role, bind user, drop role
        expected: drop success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        self.drop_role(client, role_name=role_name)
        roles, _ = self.list_roles(client)
        assert role_name not in roles

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_add_user_to_default_role(self, role_name, host, port):
        """
        target: add user to admin or public role
        method: create user, add to default role, verify
        expected: success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        user_info, _ = self.describe_user(client, user_name=user_name)
        assert role_name in user_info.get("roles", [])

    def test_milvus_client_add_root_to_new_role(self, host, port):
        """
        target: add root user to a new role
        method: create role, add root
        expected: success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=ct.default_user, role_name=role_name)
        user_info, _ = self.describe_user(client, user_name=ct.default_user)
        assert role_name in user_info.get("roles", [])
        self.drop_role(client, role_name=role_name)

    @pytest.mark.parametrize("role_name", ["admin", "public"])
    def test_milvus_client_drop_admin_and_public_role(self, role_name, host, port):
        """
        target: drop admin and public role should fail
        method: try to drop default roles
        expected: fail
        """
        client = self._client()
        self.drop_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                       check_items={ct.err_code: 1401,
                                    ct.err_msg: f"the role[{role_name}] is a default role, "
                                                f"which can't be dropped"})

    def test_milvus_client_add_user_not_exist_role(self, host, port):
        """
        target: add user to non-existent role
        method: grant non-existent role to user
        expected: fail
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        role_name = cf.gen_unique_str(role_pre)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name=role_name,
                        check_task=CheckTasks.err_res,
                        check_items={ct.err_code: 65535,
                                     ct.err_msg: "not found the role"})

    # ==================== Privilege Grant Listing ====================

    def test_milvus_client_list_collection_grants(self, host, port):
        """
        target: list grants by role for collection privileges
        method: create role, grant privileges, describe role
        expected: list success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim, consistency_level="Strong")
        self.create_role(client, role_name=role_name)
        self.grant_privilege(client, role_name, "Collection", "Search", coll_name)
        self.grant_privilege(client, role_name, "Collection", "Insert", coll_name)
        time.sleep(10)
        role_info, _ = self.describe_role(client, role_name=role_name)
        privileges = role_info.get("privileges", [])
        assert len(privileges) == 2
        privilege_names = [p["privilege"] for p in privileges]
        assert "Search" in privilege_names
        assert "Insert" in privilege_names

    def test_milvus_client_list_global_grants(self, host, port):
        """
        target: list grants by role for global privileges
        method: create role, grant global privileges, describe role
        expected: list success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        self.grant_privilege(client, role_name, "Global", "CreateCollection", "*")
        self.grant_privilege(client, role_name, "Global", "All", "*")
        time.sleep(10)
        role_info, _ = self.describe_role(client, role_name=role_name)
        privileges = role_info.get("privileges", [])
        assert len(privileges) == 2
        privilege_names = [p["privilege"] for p in privileges]
        assert "CreateCollection" in privilege_names
        assert "All" in privilege_names

    def test_milvus_client_list_grant_by_not_exist_role(self, host, port):
        """
        target: describe non-existent role
        method: describe role that doesn't exist
        expected: fail
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.describe_role(client, role_name=role_name, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 65535,
                                        ct.err_msg: "not found the role"})

    # ==================== Admin Role ====================

    def test_milvus_client_verify_admin_role_privilege(self, host, port):
        """
        target: verify admin role can perform CRUD
        method: create user with admin role, test collection operations
        expected: all operations succeed
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        coll_name = cf.gen_unique_str(prefix)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name="admin")
        time.sleep(10)
        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        self.create_collection(user_client, coll_name, default_dim, consistency_level="Strong")
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0,
                 default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(user_client, coll_name, rows)
        self.drop_collection(user_client, coll_name)

    def test_milvus_client_admin_role_across_dbs(self, host, port):
        """
        target: test admin role has privileges across all databases
        method: create dbs with collections, verify admin user can access all
        expected: success
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name="admin")
        time.sleep(10)

        db_a = cf.gen_unique_str("db_a")
        db_b = cf.gen_unique_str("db_b")
        self.create_database(client, db_a)
        self.create_database(client, db_b)

        self.using_database(client, db_a)
        coll_a = cf.gen_unique_str("coll_a")
        self.create_collection(client, coll_a, default_dim)

        self.using_database(client, db_b)
        coll_b = cf.gen_unique_str("coll_b")
        self.create_collection(client, coll_b, default_dim)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        self.using_database(user_client, db_a)
        res, _ = self.list_collections(user_client)
        assert coll_a in res

        self.using_database(user_client, db_b)
        res, _ = self.list_collections(user_client)
        assert coll_b in res

    # ==================== Alias RBAC ====================

    def test_milvus_client_alias_rbac(self, host, port):
        """
        target: test RBAC for alias operations
        method: grant alias privileges, verify user can create/drop alias, user2 cannot
        expected: permission enforced correctly
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        user_name2 = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        coll_name = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        self.create_user(client, user_name=user_name, password=password)
        self.create_user(client, user_name=user_name2, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)

        # grant alias privileges
        self.grant_privilege(client, role_name, "Global", "CreateAlias", "*")
        self.grant_privilege(client, role_name, "Global", "DropAlias", "*")
        time.sleep(10)

        self.create_collection(client, coll_name, default_dim)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        self.create_alias(user_client, coll_name, alias_name)
        self.drop_alias(user_client, alias_name)

        user_client2, _ = self.init_milvus_client(uri=uri, user=user_name2, password=password)
        self.create_alias(user_client2, coll_name, alias_name,
                          check_task=CheckTasks.check_permission_deny)

    # ==================== Database-scoped RBAC ====================

    def test_milvus_client_grant_privilege_with_db(self, host, port):
        """
        target: test grant privilege scoped to a specific database
        method: grant privilege in one db, verify it doesn't apply to another db
        expected: privilege valid only in granted db
        """
        client = self._client()
        user_name = cf.gen_unique_str(user_pre)
        role_name = cf.gen_unique_str(role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        db_name = cf.gen_unique_str("db")
        coll_name = cf.gen_unique_str(prefix)

        self.create_user(client, user_name=user_name, password=password)
        self.create_role(client, role_name=role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)

        self.create_database(client, db_name)
        self.using_database(client, db_name)
        self.create_collection(client, coll_name, default_dim, consistency_level="Strong")

        # grant privilege only in the new db
        self.grant_privilege(client, role_name, "Global", "All", "*", db_name=db_name)
        time.sleep(10)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        # verify privilege works in granted db
        self.using_database(user_client, db_name)
        res, _ = self.list_collections(user_client)
        assert coll_name in res

        # verify no privilege in default db
        self.using_database(user_client, "default")
        self.create_collection(user_client, cf.gen_unique_str(), default_dim,
                               check_task=CheckTasks.check_permission_deny)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacPrivilegeGroup(TestMilvusClientV2Base):
    """Test case of RBAC privilege group operations"""

    def teardown_method(self, method):
        log.info("[teardown_method] Start teardown privilege group test cases ...")
        _teardown_rbac(self)
        super().teardown_method(method)

    # ==================== Positive Tests ====================

    def test_milvus_client_create_drop_privilege_groups(self, host, port):
        """
        target: create and drop custom privilege groups
        method: create groups, verify in list, drop and verify removed
        expected: success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        name_1 = "pg_test_1"
        name_2 = "pg_test_2"
        self.create_privilege_group(client, privilege_group=name_1)
        self.create_privilege_group(client, privilege_group=name_2)
        groups, _ = self.list_privilege_groups(client)
        group_names = [g["privilege_group"] for g in groups]
        assert name_1 in group_names
        assert name_2 in group_names
        self.drop_privilege_group(client, privilege_group=name_1)
        self.drop_privilege_group(client, privilege_group=name_2)
        groups, _ = self.list_privilege_groups(client)
        group_names = [g["privilege_group"] for g in groups]
        assert name_1 not in group_names
        assert name_2 not in group_names

    def test_milvus_client_add_remove_privileges_to_group(self, host, port):
        """
        target: add and remove privileges from custom group
        method: create group, add privileges, remove them
        expected: success
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        pg_name = "pg_test"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.add_privileges_to_group(client, privilege_group=pg_name, privileges=["Insert"])
        self.add_privileges_to_group(client, privilege_group=pg_name, privileges=["Search"])
        # add duplicate - should be idempotent
        self.add_privileges_to_group(client, privilege_group=pg_name, privileges=["Insert"])
        self.remove_privileges_from_group(client, privilege_group=pg_name, privileges=["Insert"])
        self.remove_privileges_from_group(client, privilege_group=pg_name, privileges=["Search"])
        # remove again - should be idempotent
        self.remove_privileges_from_group(client, privilege_group=pg_name, privileges=["Insert"])

    def test_milvus_client_list_built_in_privilege_groups(self, host, port):
        """
        target: verify all built-in privilege groups exist
        method: list privilege groups and check built-in groups
        expected: all built-in groups present
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        groups, _ = self.list_privilege_groups(client)
        group_names = [g["privilege_group"] for g in groups]
        for built_in in ct.built_in_privilege_groups:
            assert built_in in group_names, f"Built-in group {built_in} not found"

    def test_milvus_client_drop_not_exist_privilege_group(self, host, port):
        """
        target: drop non-existent privilege group
        method: drop a group that doesn't exist
        expected: no error
        """
        client = self._client()
        self.drop_privilege_group(client, privilege_group="pg_not_exist")

    def test_milvus_client_drop_privilege_group_twice(self, host, port):
        """
        target: drop same privilege group twice
        method: create group, drop twice
        expected: no error
        """
        client = self._client()
        pg_name = "pg_test_twice"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.drop_privilege_group(client, privilege_group=pg_name)
        self.drop_privilege_group(client, privilege_group=pg_name)

    # ==================== Negative Tests ====================

    def test_milvus_client_create_privilege_group_with_built_in_name(self, host, port):
        """
        target: cannot create privilege group with built-in name
        method: try to create with built-in names
        expected: fail
        """
        client = self._client()
        for name in ct.built_in_privilege_groups:
            self.create_privilege_group(client, privilege_group=name,
                                        check_task=CheckTasks.err_res,
                                        check_items={ct.err_code: 1100,
                                                     ct.err_msg: f"privilege group name [{name}] is defined by "
                                                                 f"built in privileges or privilege groups"})

    def test_milvus_client_drop_built_in_privilege_group(self, host, port):
        """
        target: cannot drop built-in privilege groups
        method: try to drop built-in groups
        expected: groups still exist after drop attempt
        """
        client = self._client()
        for name in ct.built_in_privilege_groups:
            self.drop_privilege_group(client, privilege_group=name)
        groups, _ = self.list_privilege_groups(client)
        group_names = [g["privilege_group"] for g in groups]
        assert len([g for g in group_names if g in ct.built_in_privilege_groups]) == len(ct.built_in_privilege_groups)

    def test_milvus_client_drop_privilege_group_granted(self, host, port):
        """
        target: cannot drop privilege group that is granted to a role
        method: create group, grant to role, try to drop
        expected: fail until revoked
        """
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        pg_name = "pg_granted"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.grant_privilege_v2(client, role_name, pg_name, coll_name)
        self.drop_privilege_group(client, privilege_group=pg_name,
                                  check_task=CheckTasks.err_res,
                                  check_items={ct.err_code: 65535,
                                               ct.err_msg: f"privilege group [{pg_name}] is used by role "
                                                           f"[{role_name}]"})
        self.revoke_privilege_v2(client, role_name, pg_name, coll_name)
        self.drop_privilege_group(client, privilege_group=pg_name)

    def test_milvus_client_add_privilege_to_built_in_group(self, host, port):
        """
        target: cannot add privilege to built-in group
        method: try to add privilege to built-in groups
        expected: fail
        """
        client = self._client()
        for name in ct.built_in_privilege_groups:
            self.add_privileges_to_group(client, privilege_group=name, privileges=["Insert"],
                                         check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1100,
                                                      ct.err_msg: f"there is no privilege group name [{name}]"})

    def test_milvus_client_remove_privilege_from_built_in_group(self, host, port):
        """
        target: cannot remove privilege from built-in group
        method: try to remove privilege from built-in groups
        expected: fail
        """
        client = self._client()
        for name in ct.built_in_privilege_groups:
            self.remove_privileges_from_group(client, privilege_group=name, privileges=["Insert"],
                                              check_task=CheckTasks.err_res,
                                              check_items={ct.err_code: 1100,
                                                           ct.err_msg: f"there is no privilege group name [{name}]"})

    def test_milvus_client_add_privilege_to_not_exist_group(self, host, port):
        """
        target: add privilege to non-existent group
        method: try to add privilege to non-existent group
        expected: fail
        """
        client = self._client()
        pg_name = "pg_not_exist"
        self.add_privileges_to_group(client, privilege_group=pg_name, privileges=["Insert"],
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1100,
                                                  ct.err_msg: f"there is no privilege group name [{pg_name}]"})

    def test_milvus_client_remove_privilege_from_not_exist_group(self, host, port):
        """
        target: remove privilege from non-existent group
        method: try to remove privilege from non-existent group
        expected: fail
        """
        client = self._client()
        pg_name = "pg_not_exist"
        self.remove_privileges_from_group(client, privilege_group=pg_name, privileges=["Insert"],
                                          check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1100,
                                                       ct.err_msg: f"there is no privilege group name [{pg_name}]"})

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


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientRbacGrantV2(TestMilvusClientV2Base):
    """Test case of RBAC grant/revoke v2 operations"""

    def teardown_method(self, method):
        log.info("[teardown_method] Start teardown grant v2 test cases ...")
        _teardown_rbac(self)
        super().teardown_method(method)

    # ==================== Grant/Revoke V2 Positive ====================

    def test_milvus_client_grant_revoke_v2_normal(self, host, port):
        """
        target: test grant/revoke v2 normal flow
        method: create custom privilege group, grant to role, verify, revoke
        expected: success
        """
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        pg_name = "pg_v2_normal"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.grant_privilege_v2(client, role_name, pg_name, coll_name)
        # grant again - idempotent
        self.grant_privilege_v2(client, role_name, pg_name, coll_name)
        role_info, _ = self.describe_role(client, role_name=role_name)
        privileges = role_info.get("privileges", [])
        assert any(p["privilege"] == pg_name for p in privileges)
        self.revoke_privilege_v2(client, role_name, pg_name, coll_name)
        # revoke again - idempotent
        self.revoke_privilege_v2(client, role_name, pg_name, coll_name)
        role_info, _ = self.describe_role(client, role_name=role_name)
        privileges = role_info.get("privileges", [])
        assert not any(p["privilege"] == pg_name for p in privileges)

    def test_milvus_client_grant_revoke_v2_another_db(self, host, port):
        """
        target: test grant/revoke v2 with non-default database
        method: grant in another db, verify grant shows in that db only
        expected: success
        """
        client = self._client()
        new_db = cf.gen_unique_str("db")
        self.create_database(client, new_db)
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        pg_name = "pg_v2_db"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.grant_privilege_v2(client, role_name, pg_name, coll_name, db_name=new_db)
        role_info, _ = self.describe_role(client, role_name=role_name, db_name=new_db)
        privileges = role_info.get("privileges", [])
        found = False
        for p in privileges:
            if p["privilege"] == pg_name and p.get("db_name") == new_db:
                found = True
        assert found
        self.revoke_privilege_v2(client, role_name, pg_name, coll_name, db_name=new_db)

    @pytest.mark.parametrize("privilege_group_name", ct.built_in_privilege_groups)
    def test_milvus_client_grant_revoke_v2_built_in_groups(self, host, port, privilege_group_name):
        """
        target: test grant/revoke v2 with built-in privilege groups
        method: grant built-in group to role, verify, revoke
        expected: success
        """
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        collection_name = coll_name
        db_name = "default"
        if privilege_group_name.startswith("Database"):
            collection_name = "*"
        if privilege_group_name.startswith("Cluster"):
            collection_name = "*"
            db_name = "*"
        self.grant_privilege_v2(client, role_name, privilege_group_name, collection_name, db_name=db_name)
        role_info, _ = self.describe_role(client, role_name=role_name)
        privileges = role_info.get("privileges", [])
        assert any(p["privilege"] == privilege_group_name for p in privileges)
        self.revoke_privilege_v2(client, role_name, privilege_group_name, collection_name, db_name=db_name)

    def test_milvus_client_grant_v2_not_exist_collection(self, host, port):
        """
        target: grant v2 with non-existent collection
        method: grant to a collection that doesn't exist
        expected: grant succeeds (collection name is just a string to server)
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        pg_name = "pg_v2_noexist"
        self.create_privilege_group(client, privilege_group=pg_name)
        self.grant_privilege_v2(client, role_name, pg_name, "not_exist_collection")
        self.revoke_privilege_v2(client, role_name, pg_name, "not_exist_collection")

    # ==================== Grant V2 Negative ====================

    def test_milvus_client_grant_v2_database_built_in_invalid_collection(self, host, port):
        """
        target: grant v2 database-level built-in group with specific collection should fail
        method: grant Database* group with a specific collection name
        expected: fail - collection should be *
        """
        client = self._client()
        coll_name = cf.gen_unique_str(prefix)
        self.create_collection(client, coll_name, default_dim)
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        for name in ct.built_in_privilege_groups:
            if not name.startswith("Database"):
                continue
            self.grant_privilege_v2(client, role_name, name, coll_name,
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100,
                                                 ct.err_msg: "collectionName should be * for the database "
                                                             f"level privilege: {name}"})

    def test_milvus_client_grant_v2_cluster_built_in_invalid_collection(self, host, port):
        """
        target: grant v2 cluster-level built-in group with specific db should fail
        method: grant Cluster* group with a specific db name
        expected: fail - db and collection should be *
        """
        client = self._client()
        role_name = cf.gen_unique_str(role_pre)
        self.create_role(client, role_name=role_name)
        for name in ct.built_in_privilege_groups:
            if not name.startswith("Cluster"):
                continue
            self.grant_privilege_v2(client, role_name, name, "*", db_name="default",
                                    check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1100,
                                                 ct.err_msg: "dbName and collectionName should be * for the cluster "
                                                             f"level privilege: {name}"})
