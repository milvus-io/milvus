import time

import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

prefix = "rbac_alias"
user_pre = "user"
role_pre = "role"
default_dim = ct.default_dim
default_limit = ct.default_limit
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name

# RBAC propagation polling config
RBAC_POLL_INTERVAL = 2      # seconds between polls
RBAC_POLL_TIMEOUT = 30      # max seconds to wait for propagation


@pytest.mark.tags(CaseLabel.RBAC)
class TestRbacAliasBase(TestMilvusClientV2Base):
    """Base class for RBAC alias resolution tests.

    Uses resource tracking for concurrency-safe teardown (-n 6 safe).
    Each test only cleans up the users/roles/databases it created.
    """

    def setup_method(self, method):
        super().setup_method(method)
        self._created_users = []
        self._created_roles = []
        self._created_aliases = []
        self._created_databases = []

    def teardown_method(self, method):
        """
        teardown method: only drop resources created by this test case
        """
        log.info("[teardown_method] Start teardown RBAC alias test cases ...")
        client = self._client()

        # drop tracked users
        for user in self._created_users:
            try:
                self.drop_user(client, user)
            except Exception as e:
                log.warning(f"Failed to drop user {user}: {e}")

        # revoke privileges and drop tracked roles
        # Must check grants in ALL tracked databases (cross-db grants are invisible from default)
        # Use check_nothing to avoid assertion errors when role was already dropped by another worker
        for role in self._created_roles:
            try:
                # revoke grants visible in default db
                res, ok = self.describe_role(client, role,
                                             check_task=CheckTasks.check_nothing)
                if ok and res and isinstance(res, dict) and res.get('privileges'):
                    for privilege in res['privileges']:
                        self.revoke_privilege(client, role, privilege["object_type"],
                                              privilege["privilege"], privilege["object_name"])
                # revoke grants in tracked databases (cross-db test)
                for db in self._created_databases:
                    try:
                        self.using_database(client, db)
                        res_db, ok_db = self.describe_role(
                            client, role, check_task=CheckTasks.check_nothing)
                        if ok_db and res_db and isinstance(res_db, dict) and res_db.get('privileges'):
                            for privilege in res_db['privileges']:
                                self.revoke_privilege(client, role, privilege["object_type"],
                                                      privilege["privilege"],
                                                      privilege["object_name"])
                    except Exception:
                        pass
                    finally:
                        try:
                            self.using_database(client, "default")
                        except Exception:
                            pass
                self.drop_role(client, role)
            except Exception as e:
                log.warning(f"Failed to drop role {role}: {e}")

        # drop tracked aliases
        for alias in self._created_aliases:
            try:
                self.drop_alias(client, alias)
            except Exception as e:
                log.warning(f"Failed to drop alias {alias}: {e}")

        # drop tracked databases (drop aliases before collections)
        for db in self._created_databases:
            try:
                self.using_database(client, db)
                collections, _ = self.list_collections(client)
                for coll in collections:
                    try:
                        aliases_res, _ = self.list_aliases(client, coll)
                        alias_list = aliases_res.get('aliases', []) \
                            if isinstance(aliases_res, dict) else aliases_res
                        for a in alias_list:
                            self.drop_alias(client, a)
                    except Exception:
                        pass
                    self.drop_collection(client, coll)
                self.drop_database(client, db)
            except Exception as e:
                log.warning(f"Failed to drop database {db}: {e}")
            finally:
                try:
                    self.using_database(client, "default")
                except Exception:
                    pass

        super().teardown_method(method)

    def _create_user_with_tracking(self, client, user_name, password):
        """Create a user and track for teardown"""
        self.create_user(client, user_name=user_name, password=password)
        self._created_users.append(user_name)

    def _create_role_with_tracking(self, client, role_name):
        """Create a role and track for teardown"""
        self.create_role(client, role_name=role_name)
        self._created_roles.append(role_name)

    def _create_alias_with_tracking(self, client, collection_name, alias_name):
        """Create an alias and track for teardown"""
        self.create_alias(client, collection_name, alias_name)
        self._created_aliases.append(alias_name)

    def _create_database_with_tracking(self, client, db_name):
        """Create a database and track for teardown"""
        self.create_database(client, db_name)
        self._created_databases.append(db_name)

    def _setup_restricted_user_role(self, client):
        """Create a user and role, bind them together, track for teardown.
        Returns (user_name, password, role_name)
        """
        role_name = cf.gen_unique_str(role_pre)
        user_name = cf.gen_unique_str(user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self._create_user_with_tracking(client, user_name, password)
        self._create_role_with_tracking(client, role_name)
        self.grant_role(client, user_name=user_name, role_name=role_name)
        return user_name, password, role_name

    def _insert_data(self, client, collection_name, nb=100):
        """Insert test data into a collection"""
        vectors = cf.gen_vectors(nb, default_dim)
        rows = [{default_primary_key_field_name: i,
                 default_vector_field_name: vectors[i],
                 default_float_field_name: i * 1.0,
                 default_string_field_name: str(i)} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

    def _assert_role_has_privilege_on_collection(self, client, role_name, collection_name,
                                                  expected_privileges, should_exist=True):
        """Check describe_role output for a specific collection's privileges"""
        res, _ = self.describe_role(client, role_name)
        found_privileges = []
        for p in res.get('privileges', []):
            if p.get('object_name') == collection_name:
                found_privileges.append(p.get('privilege'))
        if should_exist:
            for priv in expected_privileges:
                assert priv in found_privileges, \
                    f"Expected privilege '{priv}' on '{collection_name}' for role '{role_name}', " \
                    f"found: {found_privileges}"
        else:
            for priv in expected_privileges:
                assert priv not in found_privileges, \
                    f"Unexpected privilege '{priv}' on '{collection_name}' for role '{role_name}', " \
                    f"found: {found_privileges}"

    def _wait_for_grant_propagated(self, client, role_name, collection_name, expected_privileges,
                                    timeout=RBAC_POLL_TIMEOUT, interval=RBAC_POLL_INTERVAL):
        """Poll describe_role until expected privileges are visible on collection_name."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            res, ok = self.describe_role(client, role_name, check_task=CheckTasks.check_nothing)
            if ok and res and isinstance(res, dict):
                found = {p.get('privilege') for p in res.get('privileges', [])
                         if p.get('object_name') == collection_name}
                if all(priv in found for priv in expected_privileges):
                    return True
            time.sleep(interval)
        log.warning(f"Grant propagation timeout: {expected_privileges} on '{collection_name}' "
                    f"for role '{role_name}' not visible after {timeout}s")
        return False

    def _grant_and_wait(self, client, role_name, privilege, collection_name, **kwargs):
        """Grant privilege, then poll until propagated (replaces fixed sleep)."""
        self.grant_privilege_v2(client, role_name, privilege, collection_name, **kwargs)
        self._wait_for_grant_propagated(client, role_name, collection_name, [privilege])

    def _revoke_and_wait(self, client, role_name, privilege, collection_name, **kwargs):
        """Revoke privilege, then poll until revoked (replaces fixed sleep)."""
        self.revoke_privilege_v2(client, role_name, privilege, collection_name, **kwargs)
        self._wait_for_grant_revoked(client, role_name, collection_name, [privilege])

    def _wait_for_grant_revoked(self, client, role_name, collection_name, revoked_privileges,
                                 timeout=RBAC_POLL_TIMEOUT, interval=RBAC_POLL_INTERVAL):
        """Poll describe_role until revoked privileges are no longer visible."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            res, ok = self.describe_role(client, role_name, check_task=CheckTasks.check_nothing)
            if ok and res and isinstance(res, dict):
                found = {p.get('privilege') for p in res.get('privileges', [])
                         if p.get('object_name') == collection_name}
                if not any(priv in found for priv in revoked_privileges):
                    return True
            time.sleep(interval)
        log.warning(f"Revoke propagation timeout: {revoked_privileges} on '{collection_name}' "
                    f"for role '{role_name}' still visible after {timeout}s")
        return False


@pytest.mark.xdist_group("TestRbacAliasShared")
class TestRbacAliasSharedCollection(TestRbacAliasBase):
    """Tests sharing one collection. Each test creates its own alias/user/role.

    Shared resources (class-scoped):
    - self.collection_name: collection with 100 rows loaded
    - self.collection_b_name: second collection for denial tests

    Per-test resources (method-scoped, cleaned by teardown_method):
    - unique alias(es) via _create_alias_with_tracking
    - unique user/role via _setup_restricted_user_role

    This avoids the RBAC alias cache bug (https://github.com/milvus-io/milvus/issues/48071)
    by ensuring each test uses a fresh alias name — no grant→revoke→re-grant on the same alias.

    When to add tests here:
    - Test only needs Search on a pre-loaded collection via alias
    - Test does NOT modify aliases of other tests, drop collections, or use cross-db operations
    """
    shared_alias = "TestRbacAliasShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestRbacAliasShared" + cf.gen_unique_str("_")
        self.collection_b_name = "TestRbacAliasSharedB" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_collections(self, request):
        client = self._client(alias=self.shared_alias)
        # Main collection
        self.create_collection(client, self.collection_name, default_dim,
                               consistency_level="Strong", force_teardown=False)
        self._insert_data(client, self.collection_name)
        # Second collection (for denial tests, no aliases)
        self.create_collection(client, self.collection_b_name, default_dim,
                               consistency_level="Strong", force_teardown=False)
        self._insert_data(client, self.collection_b_name)

        def teardown():
            c = self._client(alias=self.shared_alias)
            self.drop_collection(c, self.collection_name)
            self.drop_collection(c, self.collection_b_name)
        request.addfinalizer(teardown)

    def test_rbac_alias_access_with_real_collection_grant(self, host, port):
        """
        target: TC-L0-01 — verify alias access works when user has privilege on real collection
        method:
            1. grant Search on shared collection to role
            2. search via per-test alias with restricted user
        expected: search succeeds via alias (alias resolved to real collection)
        note: only Search is supported for alias resolution in privilege interceptor
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", self.collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

    def test_rbac_alias_access_denied_without_real_collection_grant(self, host, port):
        """
        target: TC-L0-02 — verify alias access denied when user has no privilege on real collection
        method:
            1. grant Search only on collection_b (not the one alias points to)
            2. access collection_name via alias
        expected: permission denied
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", self.collection_b_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_alias_search_via_alias(self, host, port):
        """
        target: TC-L1-01 — verify Search works via alias with grant on real collection
        method: grant Search on real collection, search via alias and real name
        expected: search succeeds via alias and real name
        note: only Search is supported for alias resolution in privilege interceptor;
              Query/Insert/Load/Release do NOT resolve aliases for RBAC
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", self.collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)
        self.search(restricted_client, self.collection_name, search_vectors, limit=default_limit)

    def test_rbac_alias_wildcard_privilege(self, host, port):
        """
        target: TC-L1-02 — verify wildcard '*' privilege covers alias access
        method: grant Search on '*' (all collections), access via alias
        expected: search succeeds
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", "*")

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

    def test_rbac_alias_multiple_aliases_same_collection(self, host, port):
        """
        target: TC-L1-03 — verify multiple aliases pointing to same collection have consistent RBAC
        method: grant Search on real collection, search via alias1, alias2, and real name
        expected: search via alias1, alias2, and collection_name all succeed
        """
        client = self._client()
        alias1 = cf.gen_unique_str("alias1")
        alias2 = cf.gen_unique_str("alias2")
        self._create_alias_with_tracking(client, self.collection_name, alias1)
        self._create_alias_with_tracking(client, self.collection_name, alias2)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", self.collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias1, search_vectors, limit=default_limit)
        self.search(restricted_client, alias2, search_vectors, limit=default_limit)
        self.search(restricted_client, self.collection_name, search_vectors, limit=default_limit)

    def test_rbac_alias_v1_grant_also_normalized(self, host, port):
        """
        target: TC-L1-05 — verify v1 grant_privilege on alias name is also normalized to real collection
        method: use v1 grant_privilege with alias name, verify grant stored on real collection
        expected: v1 API also normalizes alias → grant on real collection, search succeeds
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        # v1 API: grant_privilege(client, role, object_type, privilege, object_name)
        self.grant_privilege(client, role_name, "Collection", "Search", alias_name)
        self._wait_for_grant_propagated(client, role_name, self.collection_name, ["Search"])

        # verify grant is stored on real collection (v1 API also normalizes)
        self._assert_role_has_privilege_on_collection(client, role_name, self.collection_name,
                                                       ["Search"], should_exist=True)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)
        self.search(restricted_client, self.collection_name, search_vectors, limit=default_limit)

    def test_rbac_grant_via_alias_normalized_to_real_collection(self, host, port):
        """
        target: TC-L0-06 — verify grant via alias name is normalized to real collection name
        method:
            1. grant_privilege_v2(role, Search, alias)
            2. describe_role → should show grant on collection_name (not alias)
            3. search via collection_name and alias both succeed
        expected: grant stored on real collection name
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        # grant Search using alias name — should be normalized to real collection
        self.grant_privilege_v2(client, role_name, "Search", alias_name)
        self._wait_for_grant_propagated(client, role_name, self.collection_name, ["Search"])

        # describe_role → should show grant on real collection name
        self._assert_role_has_privilege_on_collection(client, role_name, self.collection_name,
                                                       ["Search"], should_exist=True)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, self.collection_name, search_vectors, limit=default_limit)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

    def test_rbac_select_grant_via_alias(self, host, port):
        """
        target: TC-L1-24 — verify describe_role resolves alias when querying grants
        method:
            1. grant Search via alias
            2. describe_role should show grant on collection_name
        expected: grant visible under real collection name
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        role_name = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role_name)

        # grant via alias
        self.grant_privilege_v2(client, role_name, "Search", alias_name)
        self._wait_for_grant_propagated(client, role_name, self.collection_name, ["Search"])

        # describe_role → grant should be on real collection (not alias name)
        self._assert_role_has_privilege_on_collection(client, role_name, self.collection_name,
                                                       ["Search"], should_exist=True)

    def test_rbac_alias_query_via_alias(self, host, port):
        """
        target: TC-L1-27 — verify Query works via alias with grant on real collection
        method: grant Query on real collection, query via alias and real name
        expected: query succeeds via both alias and real name
        bug: privilege interceptor only resolves alias for Search, not Query (#48061)
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Query", self.collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        # query via real name → should succeed
        self.query(restricted_client, self.collection_name,
                   filter=f"{default_primary_key_field_name} >= 0",
                   output_fields=[default_primary_key_field_name], limit=default_limit)
        # query via alias → should also succeed (currently fails: alias not resolved for Query)
        self.query(restricted_client, alias_name,
                   filter=f"{default_primary_key_field_name} >= 0",
                   output_fields=[default_primary_key_field_name], limit=default_limit)

    def test_rbac_alias_insert_via_alias(self, host, port):
        """
        target: TC-L1-28 — verify Insert works via alias with grant on real collection
        method: grant Insert on real collection, insert via alias
        expected: insert succeeds via alias
        bug: privilege interceptor only resolves alias for Search, not Insert (#48061)
        """
        client = self._client()
        alias_name = cf.gen_unique_str("alias")
        self._create_alias_with_tracking(client, self.collection_name, alias_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Insert", self.collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        vectors = cf.gen_vectors(1, default_dim)
        rows = [{default_primary_key_field_name: 99999,
                 default_vector_field_name: vectors[0],
                 default_float_field_name: 99999.0,
                 default_string_field_name: "99999"}]
        # insert via alias → should succeed (currently fails: alias not resolved for Insert)
        self.insert(restricted_client, alias_name, rows)


class TestRbacAliasIndependent(TestRbacAliasBase):
    """Tests that modify collection/alias/grant state — each creates its own resources.

    When to add tests here:
    - Test drops aliases, drops collections, alters aliases, or revokes grants
    - Test uses cross-database operations
    - Test creates aliases via alias (CreateAlias with alias as collection_name)
    """

    def test_rbac_alias_cache_stale_after_grant_revoke_cycle(self, host, port):
        """
        target: TC-L1-26 — verify alias search works after grant-revoke-re-grant on same collection
        method:
            1. create collectionA + aliasX
            2. Round 1: role1 granted Search on collectionA, search via aliasX → OK, then revoke + drop role1
            3. Round 2: role2 granted Search on collectionA, search via aliasX → should OK
        expected: round 2 search via alias succeeds (cache properly invalidated after revoke)
        bug: proxy RBAC alias resolution cache is stale after grant-revoke cycle,
             causing round 2 alias-based search to return PermissionDenied
             while real-name search succeeds with the same grant
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # === Round 1: grant → search via alias → revoke ===
        user1, pwd1, role1 = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role1, "Search", collection_name)

        uri = f"http://{host}:{port}"
        client1, _ = self.init_milvus_client(uri=uri, user=user1, password=pwd1)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(client1, alias_name, search_vectors, limit=default_limit)

        # Teardown round 1: revoke and drop
        self._revoke_and_wait(client, role1, "Search", collection_name)

        # === Round 2: new role + new user, same collection + same alias ===
        user2, pwd2, role2 = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role2, "Search", collection_name)

        client2, _ = self.init_milvus_client(uri=uri, user=user2, password=pwd2)

        # search via real name → should succeed (proves grant is valid)
        self.search(client2, collection_name, search_vectors, limit=default_limit)

        # search via alias → should also succeed (but fails due to stale cache)
        self.search(client2, alias_name, search_vectors, limit=default_limit)

    def test_rbac_alias_cross_database(self, host, port):
        """
        target: TC-L1-07 — verify alias resolution is database-scoped
        method:
            1. create db1.collA and db2.collA, each with alias
            2. grant Search on db1.collA only
            3. search via db1's alias → succeed, db2's alias → denied
        expected: cross-database isolation
        """
        client = self._client()
        db1 = cf.gen_unique_str("db1")
        db2 = cf.gen_unique_str("db2")
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str("alias")

        # create databases and collections
        self._create_database_with_tracking(client, db1)
        self._create_database_with_tracking(client, db2)

        self.using_database(client, db1)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        self.using_database(client, db2)
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # switch back to default db for role/user management
        self.using_database(client, "default")

        # create role with Search on db1.collA only
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self.grant_privilege_v2(client, role_name, "Search", collection_name, db_name=db1)
        # cross-db grant: poll in the granted db context
        self.using_database(client, db1)
        self._wait_for_grant_propagated(client, role_name, collection_name, ["Search"])
        self.using_database(client, "default")

        # connect as restricted user to db1 → search via alias should succeed
        uri = f"http://{host}:{port}"
        client_db1, _ = self.init_milvus_client(uri=uri, user=user_name, password=password,
                                                 db_name=db1)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(client_db1, alias_name, search_vectors, limit=default_limit)

        # connect as restricted user to db2 → search via alias should be denied
        client_db2, _ = self.init_milvus_client(uri=uri, user=user_name, password=password,
                                                 db_name=db2)
        self.search(client_db2, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_alias_cache_invalidation_on_drop_alias(self, host, port):
        """
        target: TC-L1-08 — verify cache invalidation when alias is dropped
        method:
            1. search via alias (fills cache)
            2. drop alias
            3. search via alias again
        expected: search fails after alias is dropped
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # create role with Search on real collection
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_name)

        # connect as restricted user, search via alias → success
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

        # drop alias (admin client)
        self.drop_alias(client, alias_name)

        # search via dropped alias → should fail (alias no longer exists)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_alias_cache_invalidation_on_collection_drop(self, host, port):
        """
        target: TC-L1-09 — verify all alias caches invalidated when collection is dropped
        method:
            1. create collectionA with alias1 and alias2
            2. search via both aliases (fill cache)
            3. drop collectionA
            4. search via alias1 and alias2
        expected: both searches fail after collection drop
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias1 = cf.gen_unique_str("alias1")
        alias2 = cf.gen_unique_str("alias2")

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias1)
        self.create_alias(client, collection_name, alias2)

        # create role with Search on real collection
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_name)

        # connect as restricted user, search via both aliases → fill cache
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias1, search_vectors, limit=default_limit)
        self.search(restricted_client, alias2, search_vectors, limit=default_limit)

        # drop aliases then drop collection (admin)
        self.drop_alias(client, alias1)
        self.drop_alias(client, alias2)
        self.drop_collection(client, collection_name)

        # search via aliases → should fail (aliases and collection dropped)
        self.search(restricted_client, alias1, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)
        self.search(restricted_client, alias2, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_alias_cache_invalidation_on_alter_alias(self, host, port):
        """
        target: TC-L1-11 — verify cache updates when alias is altered to point to different collection
        method:
            1. aliasX → collectionA, search via aliasX (cache fills for collectionA)
            2. alter aliasX → collectionB
            3. search via aliasX
        expected: after alter, search accesses collectionB (cache invalidated and refreshed)
        """
        client = self._client()
        collection_a = cf.gen_collection_name_by_testcase_name()
        collection_b = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_a, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_a, nb=100)
        self.create_collection(client, collection_b, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_b, nb=200)
        self.create_alias(client, collection_a, alias_name)

        # create role with Search on both collections
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self.grant_privilege_v2(client, role_name, "Search", collection_a)
        self.grant_privilege_v2(client, role_name, "Search", collection_b)
        self._wait_for_grant_propagated(client, role_name, collection_a, ["Search"])
        self._wait_for_grant_propagated(client, role_name, collection_b, ["Search"])

        # connect as restricted user
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        # search via alias → accesses collectionA (cache fills)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

        # use admin to verify count before alter (collectionA has 100 rows)
        res_before = self.query(client, collection_a,
                                filter=f"{default_primary_key_field_name} >= 0",
                                output_fields=["count(*)"])

        # alter alias to collectionB (admin)
        self.alter_alias(client, collection_b, alias_name)

        # search via alias → should now access collectionB (cache invalidated)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

        # use admin to verify collectionB has different count (200 rows)
        res_after = self.query(client, collection_b,
                               filter=f"{default_primary_key_field_name} >= 0",
                               output_fields=["count(*)"])
        assert res_before[0][0].get("count(*)") != res_after[0][0].get("count(*)"), \
            "collectionA and collectionB should have different data counts"

    def test_rbac_create_alias_resolves_collection_name(self, host, port):
        """
        target: TC-L1-20 — verify CreateAlias resolves alias in collection_name, and RBAC
               grant on real collection covers access via the new alias
        method:
            1. collectionA has aliasX
            2. CreateAlias(collection_name=aliasX, alias=aliasY)
            3. verify aliasY points to collectionA
            4. grant Search on collectionA, verify search via aliasY succeeds
        expected: aliasX resolved to collectionA, aliasY → collectionA, RBAC works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_x = cf.gen_unique_str("aliasX")
        alias_y = cf.gen_unique_str("aliasY")

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_x)

        # create aliasY using aliasX as collection_name
        self.create_alias(client, alias_x, alias_y)

        # verify aliasY is listed under collectionA
        aliases_res, _ = self.list_aliases(client, collection_name)
        alias_list = aliases_res.get('aliases', []) if isinstance(aliases_res, dict) else aliases_res
        assert alias_y in alias_list, f"aliasY should be listed under real collection, got: {aliases_res}"

        # RBAC verification: grant Search on real collection, access via aliasY
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_name)

        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, alias_y, search_vectors, limit=default_limit)

    def test_rbac_revoke_via_alias(self, host, port):
        """
        target: TC-L1-23 — verify revoke via alias name works
        method:
            1. grant Search on collectionA
            2. revoke Search using aliasX
            3. verify grant is removed
        expected: revoke via alias equals revoke on real collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # create role, grant Search on real collection
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_name)

        # verify grant exists
        self._assert_role_has_privilege_on_collection(client, role_name, collection_name,
                                                       ["Search"], should_exist=True)

        # revoke via alias name (alias normalized to real collection)
        self.revoke_privilege_v2(client, role_name, "Search", alias_name)
        self._wait_for_grant_revoked(client, role_name, collection_name, ["Search"])

        # verify grant removed
        self._assert_role_has_privilege_on_collection(client, role_name, collection_name,
                                                       ["Search"], should_exist=False)

        # verify restricted user denied
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, collection_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_alias_redirect_grant_does_not_follow(self, host, port):
        """
        target: TC-L1-25 — verify alias redirect does not move grant (write-time normalization safety)
        method:
            1. collectionA and collectionB, aliasX → collectionA
            2. grant Search via aliasX → stored on collectionA
            3. alter aliasX → collectionB
            4. search via aliasX → denied (alias now points to B, grant is on A)
            5. search via collectionA → succeed (grant is on A)
        expected: grant stays on real collection, alias redirect doesn't move it
        """
        client = self._client()
        collection_a = cf.gen_collection_name_by_testcase_name()
        collection_b = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_a, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_a)
        self.create_collection(client, collection_b, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_b)
        self.create_alias(client, collection_a, alias_name)

        # create role, grant Search via alias (normalized to collectionA)
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self.grant_privilege_v2(client, role_name, "Search", alias_name)
        self._wait_for_grant_propagated(client, role_name, collection_a, ["Search"])

        # alter alias → collectionB
        self.alter_alias(client, collection_b, alias_name)
        time.sleep(5)

        # connect as restricted user
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)

        # search via aliasX → denied (alias resolves to collectionB, grant on collectionA)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

        # search via collectionA → succeed (grant is on collectionA)
        self.search(restricted_client, collection_a, search_vectors, limit=default_limit)

    def test_rbac_alias_redirect_privilege_escalation(self, host, port):
        """
        target: TC-SEC-01 — verify alias redirect cannot be used for privilege escalation
        method:
            1. create collectionA and collectionB, aliasX → collectionA
            2. grant Search on collectionA only
            3. search via aliasX → succeed
            4. alter aliasX → collectionB
            5. search via aliasX → must be denied
        expected: alias redirect does not grant access to unauthorized collection
        """
        client = self._client()
        collection_a = cf.gen_collection_name_by_testcase_name()
        collection_b = cf.gen_unique_str(prefix)
        alias_name = cf.gen_unique_str("alias")

        self.create_collection(client, collection_a, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_a)
        self.create_collection(client, collection_b, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_b)
        self.create_alias(client, collection_a, alias_name)

        # create role with Search on collectionA only
        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_a)

        # connect as restricted user
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)

        # search via alias → succeed (alias → collectionA, has grant)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit)

        # admin alters alias to collectionB
        self.alter_alias(client, collection_b, alias_name)
        time.sleep(5)

        # search via alias → MUST be denied (alias → collectionB, no grant on B)
        self.search(restricted_client, alias_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

    def test_rbac_old_grant_not_leaked_to_new_collection(self, host, port):
        """
        target: TC-SEC-02 — verify old grants don't leak to newly created same-name collection
        method:
            1. create collectionA, grant Search
            2. drop collectionA (grants should be cleaned)
            3. create new collectionA
            4. search new collectionA with old user
        expected: permission denied (old grant was cleaned on drop)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # create collection and grant
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", collection_name)

        # verify access works
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, collection_name, search_vectors, limit=default_limit)

        # drop collection (grants should be cleaned up)
        self.drop_collection(client, collection_name)

        # create new collection with same name
        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")
        self._insert_data(client, collection_name)
        time.sleep(5)  # wait for RBAC cache to reflect collection drop

        # search with old user on new collection → should be denied
        self.search(restricted_client, collection_name, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)


class TestRbacGrantCleanup(TestRbacAliasBase):
    """Test grant cleanup on collection drop (Sub-feature 3).

    When to add tests here:
    - Test verifies grants are automatically removed when a collection is dropped
    - Test checks exact-match deletion (no prefix collision) and multi-privilege cleanup
    """

    def test_rbac_grant_cleanup_on_collection_drop(self, host, port):
        """
        target: TC-L0-04 — verify grants are cleaned up when collection is dropped
        method:
            1. create collectionA, grant Insert+Search to role1 and Query to role2
            2. drop collectionA
            3. check grants for role1 and role2
        expected: no grants remain for collectionA on any role
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        # create two roles with different privileges on the collection
        role1 = cf.gen_unique_str(role_pre)
        role2 = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role1)
        self._create_role_with_tracking(client, role2)
        self.grant_privilege_v2(client, role1, "Insert", collection_name)
        self.grant_privilege_v2(client, role1, "Search", collection_name)
        self.grant_privilege_v2(client, role2, "Query", collection_name)
        self._wait_for_grant_propagated(client, role1, collection_name, ["Insert", "Search"])
        self._wait_for_grant_propagated(client, role2, collection_name, ["Query"])

        # verify grants exist before drop
        self._assert_role_has_privilege_on_collection(client, role1, collection_name,
                                                       ["Insert", "Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role2, collection_name,
                                                       ["Query"], should_exist=True)

        # drop collection
        self.drop_collection(client, collection_name)

        # verify grants are cleaned up
        self._assert_role_has_privilege_on_collection(client, role1, collection_name,
                                                       ["Insert", "Search"], should_exist=False)
        self._assert_role_has_privilege_on_collection(client, role2, collection_name,
                                                       ["Query"], should_exist=False)

    def test_rbac_grant_cleanup_exact_match(self, host, port):
        """
        target: TC-L1-13 — verify grant cleanup uses exact match, not prefix match
        method:
            1. create col1 and col1_backup with grants
            2. drop col1
            3. check col1 grants cleaned, col1_backup grants intact
        expected: exact match deletion — no prefix collision
        """
        client = self._client()
        col1 = cf.gen_collection_name_by_testcase_name()
        col1_backup = col1 + "_bak"

        self.create_collection(client, col1, default_dim, consistency_level="Strong")
        self.create_collection(client, col1_backup, default_dim, consistency_level="Strong")

        role_name = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role_name)
        self.grant_privilege_v2(client, role_name, "Search", col1)
        self.grant_privilege_v2(client, role_name, "Search", col1_backup)
        self._wait_for_grant_propagated(client, role_name, col1, ["Search"])
        self._wait_for_grant_propagated(client, role_name, col1_backup, ["Search"])

        # verify both have grants
        self._assert_role_has_privilege_on_collection(client, role_name, col1,
                                                       ["Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role_name, col1_backup,
                                                       ["Search"], should_exist=True)

        # drop col1 only
        self.drop_collection(client, col1)

        # col1 grants cleaned, col1_backup grants intact
        self._assert_role_has_privilege_on_collection(client, role_name, col1,
                                                       ["Search"], should_exist=False)
        self._assert_role_has_privilege_on_collection(client, role_name, col1_backup,
                                                       ["Search"], should_exist=True)

    def test_rbac_grant_cleanup_multiple_privilege_types(self, host, port):
        """
        target: TC-L1-14 — verify all privilege types are cleaned up on collection drop
        method:
            1. grant Insert, Search, Query, Load, CreateIndex on a collection
            2. drop collection
            3. verify all grant types are cleaned up
        expected: every privilege type removed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_dim, consistency_level="Strong")

        role_name = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role_name)

        privileges = ["Insert", "Search", "Query", "Load", "CreateIndex"]
        for priv in privileges:
            self.grant_privilege_v2(client, role_name, priv, collection_name)
        self._wait_for_grant_propagated(client, role_name, collection_name, privileges)

        # verify all grants exist
        self._assert_role_has_privilege_on_collection(client, role_name, collection_name,
                                                       privileges, should_exist=True)

        # drop collection
        self.drop_collection(client, collection_name)

        # verify all grants cleaned up
        self._assert_role_has_privilege_on_collection(client, role_name, collection_name,
                                                       privileges, should_exist=False)


class TestRbacGrantMigration(TestRbacAliasBase):
    """Test grant migration on collection rename (Sub-feature 4).

    When to add tests here:
    - Test verifies grants automatically migrate to new name on collection rename
    - Test checks exact-match migration, consecutive renames, GranteeID recomputation
    """

    def test_rbac_grant_migration_on_rename(self, host, port):
        """
        target: TC-L0-05 — verify grants migrate to new name on collection rename
        method:
            1. create old_name, grant Search to role
            2. rename old_name → new_name
            3. check grants on old_name and new_name
            4. verify restricted user can search new_name
        expected: grant migrates to new_name, old_name has no grant
        """
        client = self._client()
        old_name = cf.gen_collection_name_by_testcase_name()
        new_name = cf.gen_unique_str(prefix)

        self.create_collection(client, old_name, default_dim, consistency_level="Strong")
        self._insert_data(client, old_name)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self._grant_and_wait(client, role_name, "Search", old_name)

        # verify grant exists on old_name
        self._assert_role_has_privilege_on_collection(client, role_name, old_name,
                                                       ["Search"], should_exist=True)

        # rename collection
        self.rename_collection(client, old_name, new_name)

        # verify grant migrated to new_name
        self._assert_role_has_privilege_on_collection(client, role_name, new_name,
                                                       ["Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role_name, old_name,
                                                       ["Search"], should_exist=False)

        # verify restricted user can search new_name (grant already migrated, poll for propagation)
        self._wait_for_grant_propagated(client, role_name, new_name, ["Search"])
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, new_name, search_vectors, limit=default_limit)

    def test_rbac_grant_migration_multiple_roles(self, host, port):
        """
        target: TC-L1-16 — verify all roles' grants migrate on rename
        method:
            1. role1 has Insert+Search, role2 has Query on old_name
            2. rename old_name → new_name
            3. check both roles' grants
        expected: all roles' grants migrate correctly
        """
        client = self._client()
        old_name = cf.gen_collection_name_by_testcase_name()
        new_name = cf.gen_unique_str(prefix)

        self.create_collection(client, old_name, default_dim, consistency_level="Strong")

        role1 = cf.gen_unique_str(role_pre)
        role2 = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role1)
        self._create_role_with_tracking(client, role2)
        self.grant_privilege_v2(client, role1, "Insert", old_name)
        self.grant_privilege_v2(client, role1, "Search", old_name)
        self.grant_privilege_v2(client, role2, "Query", old_name)
        self._wait_for_grant_propagated(client, role1, old_name, ["Insert", "Search"])
        self._wait_for_grant_propagated(client, role2, old_name, ["Query"])

        # rename
        self.rename_collection(client, old_name, new_name)

        # verify role1 grants migrated
        self._assert_role_has_privilege_on_collection(client, role1, new_name,
                                                       ["Insert", "Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role1, old_name,
                                                       ["Insert", "Search"], should_exist=False)

        # verify role2 grants migrated
        self._assert_role_has_privilege_on_collection(client, role2, new_name,
                                                       ["Query"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role2, old_name,
                                                       ["Query"], should_exist=False)

    def test_rbac_grant_migration_exact_match(self, host, port):
        """
        target: TC-L1-17 — verify grant migration uses exact match, no prefix collision
        method:
            1. create 'old' and 'old_backup' with grants
            2. rename 'old' → 'new'
            3. check 'new' has old's grants, 'old_backup' unaffected
        expected: exact match migration
        """
        client = self._client()
        old = cf.gen_collection_name_by_testcase_name()
        old_backup = old + "_bak"
        new = cf.gen_unique_str("new")

        self.create_collection(client, old, default_dim, consistency_level="Strong")
        self.create_collection(client, old_backup, default_dim, consistency_level="Strong")

        role_name = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role_name)
        self.grant_privilege_v2(client, role_name, "Search", old)
        self.grant_privilege_v2(client, role_name, "Insert", old_backup)
        self._wait_for_grant_propagated(client, role_name, old, ["Search"])
        self._wait_for_grant_propagated(client, role_name, old_backup, ["Insert"])

        # rename old → new
        self.rename_collection(client, old, new)

        # verify 'new' has old's grant
        self._assert_role_has_privilege_on_collection(client, role_name, new,
                                                       ["Search"], should_exist=True)
        # verify old_backup unaffected
        self._assert_role_has_privilege_on_collection(client, role_name, old_backup,
                                                       ["Insert"], should_exist=True)

    def test_rbac_grant_migration_consecutive_renames(self, host, port):
        """
        target: TC-L1-19 — verify grants follow consecutive renames
        method:
            1. create name_A, grant Search
            2. rename A → B, verify grant on B
            3. rename B → C, verify grant on C only
        expected: grant follows the latest name
        """
        client = self._client()
        name_a = cf.gen_collection_name_by_testcase_name()
        name_b = cf.gen_unique_str(prefix)
        name_c = cf.gen_unique_str(prefix)

        self.create_collection(client, name_a, default_dim, consistency_level="Strong")

        role_name = cf.gen_unique_str(role_pre)
        self._create_role_with_tracking(client, role_name)
        self._grant_and_wait(client, role_name, "Search", name_a)

        # rename A → B
        self.rename_collection(client, name_a, name_b)
        self._assert_role_has_privilege_on_collection(client, role_name, name_b,
                                                       ["Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role_name, name_a,
                                                       ["Search"], should_exist=False)

        # rename B → C
        self.rename_collection(client, name_b, name_c)
        self._assert_role_has_privilege_on_collection(client, role_name, name_c,
                                                       ["Search"], should_exist=True)
        self._assert_role_has_privilege_on_collection(client, role_name, name_b,
                                                       ["Search"], should_exist=False)

    def test_rbac_grant_migration_grantee_id_recomputation(self, host, port):
        """
        target: TC-L1-22 — verify GranteeID recomputation prevents old grant leaking to new same-name collection
        method:
            1. create collA, grant Insert+Search to role1
            2. rename collA → collB (grant migrates, GranteeID recomputed)
            3. verify role1 has privileges on collB
            4. create new collA (same name as original)
            5. verify role1 has NO privileges on new collA
        expected: new same-name collection does not inherit old grants
        """
        client = self._client()
        coll_a = cf.gen_collection_name_by_testcase_name()
        coll_b = cf.gen_unique_str(prefix)

        self.create_collection(client, coll_a, default_dim, consistency_level="Strong")
        self._insert_data(client, coll_a)

        user_name, password, role_name = self._setup_restricted_user_role(client)
        self.grant_privilege_v2(client, role_name, "Insert", coll_a)
        self.grant_privilege_v2(client, role_name, "Search", coll_a)
        self._wait_for_grant_propagated(client, role_name, coll_a, ["Insert", "Search"])

        # rename collA → collB
        self.rename_collection(client, coll_a, coll_b)

        # verify role1 has privileges on collB
        self._assert_role_has_privilege_on_collection(client, role_name, coll_b,
                                                       ["Insert", "Search"], should_exist=True)

        # create new collA (same name as original)
        self.create_collection(client, coll_a, default_dim, consistency_level="Strong")
        self._insert_data(client, coll_a)

        # verify role1 has NO privileges on new collA (GranteeID was recomputed)
        self._assert_role_has_privilege_on_collection(client, role_name, coll_a,
                                                       ["Insert", "Search"], should_exist=False)

        # verify restricted user cannot search new collA
        # grant migrated to coll_b on rename, poll until visible there
        self._wait_for_grant_propagated(client, role_name, coll_b, ["Insert", "Search"])
        uri = f"http://{host}:{port}"
        restricted_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        search_vectors = cf.gen_vectors(1, default_dim)
        self.search(restricted_client, coll_a, search_vectors, limit=default_limit,
                    check_task=CheckTasks.check_permission_deny)

        # but can search collB
        self.search(restricted_client, coll_b, search_vectors, limit=default_limit)
